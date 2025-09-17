CREATE DEFINER=`manage_prestartr_reports_user`@`%` PROCEDURE `sp_refresh_audit_events_all`()
BEGIN
 /* 0) Tenant list from schemas named `AuditLog.Tenant<N>` */
  DROP TEMPORARY TABLE IF EXISTS tmp_target_tenants;
  CREATE TEMPORARY TABLE tmp_target_tenants (
    tenant_id   INT PRIMARY KEY,
    tenant_name VARCHAR(128) NOT NULL,
    schema_name  VARCHAR(128) NOT NULL
  );
  
 INSERT INTO tmp_target_tenants (tenant_id, tenant_name, schema_name)
SELECT
  CAST(REGEXP_SUBSTR(SCHEMA_NAME, '[0-9]+$') AS UNSIGNED)               AS tenant_id,
  CONCAT('tenant ', REGEXP_SUBSTR(SCHEMA_NAME, '[0-9]+$'))               AS tenant_name,
  SCHEMA_NAME                                                           AS schema_name
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME REGEXP '^AuditLog\\.Tenant[0-9]+$'                    -- only numbered schemas
ORDER BY tenant_id;
  
  
  -- If you need 5.7 compatibility, use this instead of the INSERT above:
-- INSERT INTO tmp_target_tenants (tenant_id, tenant_name, schema_name)
-- SELECT
--   CAST(SUBSTRING(SCHEMA_NAME, LENGTH('AuditLog.Tenant')+1) AS UNSIGNED),
--   CONCAT('tenant', SUBSTRING(SCHEMA_NAME, LENGTH('AuditLog.Tenant')+1)),
--   SCHEMA_NAME
-- FROM INFORMATION_SCHEMA.SCHEMATA
-- WHERE SCHEMA_NAME LIKE 'AuditLog.Tenant%'
--   AND SUBSTRING(SCHEMA_NAME, LENGTH('AuditLog.Tenant')+1) REGEXP '^[0-9]+$'
-- ORDER BY 1;

   /* 1) Allow long dynamic SQL */
  SET SESSION group_concat_max_len = 1024*1024;

  /* 2) Build UNION over all tenants’ audit logs
        - Adjust column names if yours differ (CreationDate/UserId/etc.)
        - LEFT JOIN group table in case some types have no group
   */
   SET @sql := (
    SELECT GROUP_CONCAT(
      CONCAT(
        'SELECT ', t.tenant_id, ' AS TenantId, ',
        '       ''', t.tenant_name, ''' AS TenantName, ',
        '       al.Id AS AuditLogId, ',
        '       al.CreationDate AS EventAt, ',
        '       al.UserId AS UserId, ',
        '       ty.Id AS AuditLogTypeId, ty.Name AS AuditLogTypeName, ',
        '       tg.Id AS AuditLogTypeGroupId, tg.Name AS AuditLogTypeGroupName ',
        'FROM `AuditLog.Tenant', t.tenant_id, '`.AuditLogs al ',
        'JOIN `AuditLog.Tenant', t.tenant_id, '`.AuditLogTypes ty ON ty.Id = al.AuditLogTypeId ',
        'LEFT JOIN `AuditLog.Tenant', t.tenant_id, '`.AuditLogTypeGroups tg ON tg.Id = ty.AuditLogTypeGroupId ',
        'WHERE al.CreationDate IS NOT NULL '
      )
      SEPARATOR ' UNION ALL '
    )
    FROM tmp_target_tenants t
  );
  
  /* 3) Materialize into a stage (easy to debug, fast to merge) */
  DROP TEMPORARY TABLE IF EXISTS tmp_stage;
  SET @outer := CONCAT('CREATE TEMPORARY TABLE tmp_stage AS ', @sql);
  PREPARE s FROM @outer; EXECUTE s; DEALLOCATE PREPARE s;

  /* 4) INSERT only new audit rows */
  
  INSERT INTO PrestartrReports.AuditLoginEvents
    (TenantId, TenantName, AuditLogId, EventAt, UserId,
     AuditLogTypeId, AuditLogTypeName, AuditLogTypeGroupId, AuditLogTypeGroupName)
  SELECT st.TenantId, st.TenantName, st.AuditLogId, st.EventAt, st.UserId,
         st.AuditLogTypeId, st.AuditLogTypeName, st.AuditLogTypeGroupId, st.AuditLogTypeGroupName
  FROM tmp_stage st
  LEFT JOIN PrestartrReports.AuditLoginEvents ev
    ON ev.TenantId = st.TenantId AND ev.AuditLogId = st.AuditLogId
  WHERE ev.TenantId IS NULL;
  
  
  /* 5) UPDATE attribute changes (names/groups) for existing rows */
  UPDATE PrestartrReports.AuditLoginEvents ev
  JOIN tmp_stage st
    ON ev.TenantId = st.TenantId AND ev.AuditLogId = st.AuditLogId
  SET ev.TenantName            = st.TenantName,
      ev.EventAt               = st.EventAt,
      ev.UserId                = st.UserId,
      ev.AuditLogTypeId        = st.AuditLogTypeId,
      ev.AuditLogTypeName      = st.AuditLogTypeName,
      ev.AuditLogTypeGroupId   = st.AuditLogTypeGroupId,
      ev.AuditLogTypeGroupName = st.AuditLogTypeGroupName;

  /* 6) Cleanup */
  DROP TEMPORARY TABLE IF EXISTS tmp_stage;
  DROP TEMPORARY TABLE IF EXISTS tmp_target_tenants;
END;