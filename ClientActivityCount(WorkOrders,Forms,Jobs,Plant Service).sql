CREATE DEFINER=`manage_prestartr_reports_user`@`%` PROCEDURE `sp_refresh_entity_counts_all`()
BEGIN
  /* 1) Get all Tenant: tenant{Id} */
  DROP TEMPORARY TABLE IF EXISTS tmp_target_tenants;
  CREATE TEMPORARY TABLE tmp_target_tenants (tenant_id INT PRIMARY KEY);

  INSERT INTO tmp_target_tenants (tenant_id)
  SELECT t.Id
  FROM AtlasTest.Tenants t
  JOIN INFORMATION_SCHEMA.SCHEMATA s
    ON s.SCHEMA_NAME = CONCAT('tenant', t.Id)
  WHERE t.DatabaseType = 1
    AND t.Environment  = 0;

  /*Allow Long Union String */
  SET SESSION group_concat_max_len = 1024 * 1024;

  /* 2) UNION ALL */
  SET @sql = (
    SELECT GROUP_CONCAT(
      CONCAT(
        'SELECT tenant_id, period_start, metric, cnt FROM (',

        /* FORMS submitted (status=2) */
        ' SELECT ', t.tenant_id, ' AS tenant_id, ',
        '        f.Creationdate AS period_start, ',
        '        ''m_forms'' AS metric, COUNT(*) AS cnt ',
        '   FROM tenant', t.tenant_id, '.Form f ',
        '  WHERE f.FormStatusId = 2 ',
        '  GROUP BY period_start ',

        ' UNION ALL ',

        /* JOBS created */
        ' SELECT ', t.tenant_id, ' AS tenant_id, ',
        '        j.Creationdate AS period_start, ',
        '        ''m_jobs'' AS metric, COUNT(*) AS cnt ',
        '   FROM tenant', t.tenant_id, '.Job j ',
        '  GROUP BY period_start ',

        ' UNION ALL ',

        /* WORK ORDERS created */
        ' SELECT ', t.tenant_id, ' AS tenant_id, ',
        '        wo.Creationdate AS period_start, ',
        '        ''m_wos'' AS metric, COUNT(*) AS cnt ',
        '   FROM tenant', t.tenant_id, '.WorkOrders wo ',
        '  GROUP BY period_start ',

        ' UNION ALL ',

        /* SERVICE RECORDS created */
        ' SELECT ', t.tenant_id, ' AS tenant_id, ',
        '        ps.Creationdate AS period_start, ',
        '        ''m_sr_created'' AS metric, COUNT(*) AS cnt ',
        '   FROM tenant', t.tenant_id, '.PlantService ps ',
        '  GROUP BY period_start ',

        ' UNION ALL ',

        /* SERVICE RECORDS closed */
        ' SELECT ', t.tenant_id, ' AS tenant_id, ',
        '        ps.DateCompleted AS period_start, ',
        '        ''m_sr_closed'' AS metric, COUNT(*) AS cnt ',
        '   FROM tenant', t.tenant_id, '.PlantService ps ',
        '  WHERE ps.DateCompleted IS NOT NULL ',
        '  GROUP BY period_start ',

        ') u'
      )
      SEPARATOR ' UNION ALL '
    )
    FROM tmp_target_tenants t
  );

  /* 3) Pivot + UPSERT  (join AtlasTest.Tenants) */
  SET @outer := CONCAT(
    'INSERT INTO PrestartrReports.EntityCounts ',
    '  (TenantId, TenantName, DayOfCount, NumberOfForms, NumberOfJobs, NumberOfWorkOrders, ',
    '   NumberOfServiceRecordsCreated, NumberOfServiceRecordsClosed) ',
    'SELECT x.tenant_id, tn.Name AS TenantName, x.period_start, ',
    '       SUM(CASE WHEN x.metric = ''m_forms''       THEN x.cnt ELSE 0 END), ',
    '       SUM(CASE WHEN x.metric = ''m_jobs''        THEN x.cnt ELSE 0 END), ',
    '       SUM(CASE WHEN x.metric = ''m_wos''         THEN x.cnt ELSE 0 END), ',
    '       SUM(CASE WHEN x.metric = ''m_sr_created''  THEN x.cnt ELSE 0 END), ',
    '       SUM(CASE WHEN x.metric = ''m_sr_closed''   THEN x.cnt ELSE 0 END) ',
    'FROM (', @sql, ') x ',
    'JOIN AtlasTest.Tenants tn ON tn.Id = x.tenant_id ',
    'GROUP BY x.tenant_id, x.period_start ',
    'ON DUPLICATE KEY UPDATE ',
    '  TenantName                   = VALUES(TenantName), ',
    '  NumberOfForms                = VALUES(NumberOfForms), ',
    '  NumberOfJobs                 = VALUES(NumberOfJobs), ',
    '  NumberOfWorkOrders           = VALUES(NumberOfWorkOrders), ',
    '  NumberOfServiceRecordsCreated= VALUES(NumberOfServiceRecordsCreated), ',
    '  NumberOfServiceRecordsClosed = VALUES(NumberOfServiceRecordsClosed)'
  );

  PREPARE stmt FROM @outer;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
 end