-- ING-001: ORDHDR Row Count
SELECT
  'ING-001' AS TEST_ID,
  'ORDHDR Bronze Row Count (Active + Perm)' AS TEST_NAME,
  'INGESTION' AS TEST_CATEGORY,
  'CRITICAL' AS SEVERITY,
  CASE WHEN a.ct + p.ct > 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
  'Greater than 0' AS EXPECTED_VALUE,
  (a.ct + p.ct)::VARCHAR AS ACTUAL_VALUE,
  '' AS VARIANCE,
  'Active: ' || a.ct || ' | Perm: ' || p.ct AS DETAIL,
  'QA_Ingestion_Fidelity' AS PIPELINE_NAME,
  CURRENT_TIMESTAMP() AS RUN_TIMESTAMP
FROM
  (SELECT COUNT(*) AS ct FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDHDR_Bronze") a,
  (SELECT COUNT(*) AS ct FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDHDR_PERM_Bronze") p

UNION ALL

-- ING-002: ORDLIN Row Count
SELECT
  'ING-002',
  'ORDLIN Bronze Row Count (Active + Perm)',
  'INGESTION', 'CRITICAL',
  CASE WHEN a.ct + p.ct > 0 THEN 'PASS' ELSE 'FAIL' END,
  'Greater than 0',
  (a.ct + p.ct)::VARCHAR,
  '',
  'Active: ' || a.ct || ' | Perm: ' || p.ct,
  'QA_Ingestion_Fidelity',
  CURRENT_TIMESTAMP()
FROM
  (SELECT COUNT(*) AS ct FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDLIN_Bronze") a,
  (SELECT COUNT(*) AS ct FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDLIN_PERM_Bronze") p

UNION ALL

-- ING-003: Null PK ORDHDR
SELECT
  'ING-003',
  'Null Primary Key Check - ORDHDR (ID_ORD)',
  'INGESTION', 'CRITICAL',
  CASE WHEN n.ct = 0 THEN 'PASS' ELSE 'FAIL' END,
  '0',
  n.ct::VARCHAR,
  '',
  'Null ID_ORD rows in CP_ORDHDR_Bronze + CP_ORDHDR_PERM_Bronze',
  'QA_Ingestion_Fidelity',
  CURRENT_TIMESTAMP()
FROM (
  SELECT
    (SELECT COUNT(*) FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDHDR_Bronze" WHERE "ID_ORD" IS NULL)
    + (SELECT COUNT(*) FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDHDR_PERM_Bronze" WHERE "ID_ORD" IS NULL)
    AS ct
) n

UNION ALL

-- ING-004: Null PK ORDLIN
SELECT
  'ING-004',
  'Null Primary Key Check - ORDLIN (ID_ORD + SEQ_LINE_ORD)',
  'INGESTION', 'CRITICAL',
  CASE WHEN n.ct = 0 THEN 'PASS' ELSE 'FAIL' END,
  '0',
  n.ct::VARCHAR,
  '',
  'Null ID_ORD or SEQ_LINE_ORD in CP_ORDLIN_Bronze + CP_ORDLIN_PERM_Bronze',
  'QA_Ingestion_Fidelity',
  CURRENT_TIMESTAMP()
FROM (
  SELECT
    (SELECT COUNT(*) FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDLIN_Bronze" WHERE "ID_ORD" IS NULL OR "SEQ_LINE_ORD" IS NULL)
    + (SELECT COUNT(*) FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDLIN_PERM_Bronze" WHERE "ID_ORD" IS NULL OR "SEQ_LINE_ORD" IS NULL)
    AS ct
) n

UNION ALL

-- ING-005: Duplicate PK ORDHDR
SELECT
  'ING-005',
  'Duplicate Primary Key Check - ORDHDR Active',
  'INGESTION', 'WARNING',
  CASE WHEN d.ct = 0 THEN 'PASS' ELSE 'FAIL' END,
  '0',
  d.ct::VARCHAR,
  '',
  'Duplicate ID_ORD count in CP_ORDHDR_Bronze (pre-dedup expected)',
  'QA_Ingestion_Fidelity',
  CURRENT_TIMESTAMP()
FROM (
  SELECT COUNT(*) AS ct FROM (
    SELECT "ID_ORD" FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDHDR_Bronze"
    GROUP BY "ID_ORD" HAVING COUNT(*) > 1
  )
) d

UNION ALL

-- ING-006: Duplicate PK ORDLIN
SELECT
  'ING-006',
  'Duplicate Primary Key Check - ORDLIN Active',
  'INGESTION', 'WARNING',
  CASE WHEN d.ct = 0 THEN 'PASS' ELSE 'FAIL' END,
  '0',
  d.ct::VARCHAR,
  '',
  'Duplicate ID_ORD+SEQ_LINE_ORD in CP_ORDLIN_Bronze (pre-dedup expected)',
  'QA_Ingestion_Fidelity',
  CURRENT_TIMESTAMP()
FROM (
  SELECT COUNT(*) AS ct FROM (
    SELECT "ID_ORD", "SEQ_LINE_ORD" FROM "BRONZE_DATA"."TCM_BRONZE"."CP_ORDLIN_Bronze"
    GROUP BY "ID_ORD", "SEQ_LINE_ORD" HAVING COUNT(*) > 1
  )
) d
