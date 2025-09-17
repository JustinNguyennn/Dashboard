CREATE DEFINER=`manage_prestartr_reports_user`@`%` PROCEDURE `sp_refresh_entity_defect_counts_all`()
BEGIN


  /* 1) Get all Tenant IDs */
  DROP TEMPORARY TABLE IF EXISTS tmp_target_tenants;
  CREATE TEMPORARY TABLE tmp_target_tenants (tenant_id INT PRIMARY KEY);

  INSERT INTO tmp_target_tenants (tenant_id)
  SELECT t.Id
  FROM AtlasTest.Tenants t
  JOIN INFORMATION_SCHEMA.SCHEMATA s
    ON s.SCHEMA_NAME = CONCAT('tenant', t.Id)
  WHERE t.DatabaseType = 1
    AND t.Environment  = 0;

  /* Allow longer GROUP_CONCAT for dynamic SQL */
  SET SESSION group_concat_max_len = 1024 * 1024;

  /* 2) Build UNION for defects (only valid DefectId) */
  SET @sql_defect = (
    SELECT GROUP_CONCAT(
      CONCAT(
        'SELECT ',
        t.tenant_id, ' AS TenantId, ',
        'p.Id AS PlantId, p.Name AS PlantName, ',
        'd.Id AS DefectId, ds.Name AS DefectStatusName, ',
        'd.CreationDate AS CreateDate ',
        'FROM tenant', t.tenant_id, '.Defects d ',
        'JOIN tenant', t.tenant_id, '.Plant p ON d.PlantId = p.Id ',
        'JOIN tenant', t.tenant_id, '.DefectStatuses ds ON d.DefectStatusId = ds.Id ',
        'WHERE d.Id IS NOT NULL '   -- ✅ prevent bad inserts
      )
      SEPARATOR ' UNION ALL '
    )
    FROM tmp_target_tenants t
  );

  /* 3) Insert into EntityDefectCounts */
  SET @outer_defect := CONCAT(
    'INSERT INTO PrestartrReports.EntityDefectCounts ',
    ' (TenantId, TenantName, PlantId, PlantName, DefectId, DefectStatusName, CreateDate) ',
    'SELECT x.TenantId, tn.Name AS TenantName, ',
    '       x.PlantId, x.PlantName, x.DefectId, x.DefectStatusName, x.CreateDate ',
    'FROM (', @sql_defect, ') x ',
    'JOIN AtlasTest.Tenants tn ON tn.Id = x.TenantId ',
    'WHERE x.DefectId IS NOT NULL ' ,  -- ✅ extra safety
    'ON DUPLICATE KEY UPDATE ',
    '  TenantName       = VALUES(TenantName), ',
    '  PlantName        = VALUES(PlantName), ',
    '  DefectStatusName = VALUES(DefectStatusName), ',
    '  CreateDate       = VALUES(CreateDate)'
  );

  PREPARE stmt2 FROM @outer_defect;
  EXECUTE stmt2;
  DEALLOCATE PREPARE stmt2;
END