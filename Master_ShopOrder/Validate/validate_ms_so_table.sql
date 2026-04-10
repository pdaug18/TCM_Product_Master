-- ============================================================================
-- MASTER_SHOP_ORDER VALIDATION (PARALLEL / INDEPENDENT TEST BLOCKS)
-- ============================================================================

-- OBJECT PLACEHOLDERS
-- IDENTIFIER() requires a string variable holding the fully-qualified name.
-- Inner double-quotes preserve the mixed-case _Bronze suffix.
SET SRC_HDR_OBJ  = 'BRONZE_DATA.TCM_BRONZE."SHPORD_HDR_Bronze"';
SET SRC_OPER_OBJ = 'BRONZE_DATA.TCM_BRONZE."SHPORD_OPER_Bronze"';
SET SRC_MATL_OBJ = 'BRONZE_DATA.TCM_BRONZE."SHPORD_MATL_Bronze"';
SET TGT_OBJ      = 'SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_TABLE';


-- ============================================================================
-- T1A_SRC_HDR_NULL_KEYS
-- Intent: Source HDR key columns must be populated.
-- Pass Criteria: No NULL or blank (after TRIM) key values in HDR.
-- ============================================================================
WITH src_hdr AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	'T1A_SRC_HDR_NULL_KEYS' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'NULL/blank business keys in SHPORD_HDR after TRIM normalization.' AS notes
FROM src_hdr
WHERE TRIM(COALESCE(ID_LOC, '')) = ''
   OR TRIM(COALESCE(ID_SO, '')) = ''
   OR SUFX_SO IS NULL;

WITH src_hdr AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	TRIM(ID_LOC) AS ID_LOC,
	TRIM(ID_SO) AS ID_SO,
	SUFX_SO
FROM src_hdr
WHERE TRIM(COALESCE(ID_LOC, '')) = ''
   OR TRIM(COALESCE(ID_SO, '')) = ''
   OR SUFX_SO IS NULL
LIMIT 100;


-- ============================================================================
-- T1B_SRC_OPER_NULL_KEYS
-- Intent: Source OPER key columns must be populated.
-- Pass Criteria: No NULL or blank (after TRIM) key values in OPER.
-- ============================================================================
WITH src_oper AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_OPER_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	'T1B_SRC_OPER_NULL_KEYS' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'NULL/blank business keys in SHPORD_OPER after TRIM normalization.' AS notes
FROM src_oper
WHERE TRIM(COALESCE(ID_LOC, '')) = ''
   OR TRIM(COALESCE(ID_SO, '')) = ''
   OR SUFX_SO IS NULL;

WITH src_oper AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_OPER_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	TRIM(ID_LOC) AS ID_LOC,
	TRIM(ID_SO) AS ID_SO,
	SUFX_SO
FROM src_oper
WHERE TRIM(COALESCE(ID_LOC, '')) = ''
   OR TRIM(COALESCE(ID_SO, '')) = ''
   OR SUFX_SO IS NULL
LIMIT 100;


-- ============================================================================
-- T1C_SRC_MATL_NULL_KEYS
-- Intent: Source MATL key columns must be populated.
-- Pass Criteria: No NULL or blank (after TRIM) key values in MATL.
-- ============================================================================
WITH src_matl AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_MATL_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	'T1C_SRC_MATL_NULL_KEYS' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'NULL/blank business keys in SHPORD_MATL after TRIM normalization.' AS notes
FROM src_matl
WHERE TRIM(COALESCE(ID_LOC, '')) = ''
   OR TRIM(COALESCE(ID_SO, '')) = ''
   OR SUFX_SO IS NULL;

WITH src_matl AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_MATL_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	TRIM(ID_LOC) AS ID_LOC,
	TRIM(ID_SO) AS ID_SO,
	SUFX_SO
FROM src_matl
WHERE TRIM(COALESCE(ID_LOC, '')) = ''
   OR TRIM(COALESCE(ID_SO, '')) = ''
   OR SUFX_SO IS NULL
LIMIT 100;


-- ============================================================================
-- T2A_SRC_HDR_DUP_KEYS
-- Intent: HDR should be unique at business key grain.
-- Pass Criteria: No duplicate (ID_LOC, ID_SO, SUFX_SO) in HDR after TRIM.
-- ============================================================================
WITH src_hdr AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_dups AS (
	SELECT ID_LOC, ID_SO, SUFX_SO, COUNT(*) AS dup_cnt
	FROM src_hdr
	WHERE ID_LOC <> '' AND ID_SO <> '' AND SUFX_SO IS NOT NULL
	GROUP BY ID_LOC, ID_SO, SUFX_SO
	HAVING COUNT(*) > 1
)
SELECT
	'T2A_SRC_HDR_DUP_KEYS' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'Duplicate business keys in SHPORD_HDR.' AS notes
FROM src_dups;

WITH src_hdr AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	ID_LOC,
	ID_SO,
	SUFX_SO,
	COUNT(*) AS dup_cnt
FROM src_hdr
WHERE ID_LOC <> '' AND ID_SO <> '' AND SUFX_SO IS NOT NULL
GROUP BY ID_LOC, ID_SO, SUFX_SO
HAVING COUNT(*) > 1
ORDER BY dup_cnt DESC, ID_LOC, ID_SO, SUFX_SO
LIMIT 100;


-- ============================================================================
-- T2B_SRC_OPER_ORPHAN_KEYS
-- Intent: Every OPER key should map to an HDR key.
-- Pass Criteria: No OPER keys missing in HDR.  --! Failed 15 rows
-- ============================================================================
WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_oper AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_OPER_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_orphans AS (
	SELECT o.ID_LOC, o.ID_SO, o.SUFX_SO
	FROM src_oper o
	LEFT JOIN src_hdr h
	  ON o.ID_LOC = h.ID_LOC
	 AND o.ID_SO = h.ID_SO
	 AND o.SUFX_SO = h.SUFX_SO
	WHERE h.ID_LOC IS NULL
)
SELECT
	'T2B_SRC_OPER_ORPHAN_KEYS' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'OPER business keys with no matching HDR key.' AS notes
FROM src_orphans;

WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_oper AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_OPER_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	o.ID_LOC,
	o.ID_SO,
	o.SUFX_SO
FROM src_oper o
LEFT JOIN src_hdr h
  ON o.ID_LOC = h.ID_LOC
 AND o.ID_SO = h.ID_SO
 AND o.SUFX_SO = h.SUFX_SO
WHERE h.ID_LOC IS NULL
LIMIT 100;


-- ============================================================================
-- T2C_SRC_MATL_ORPHAN_KEYS
-- Intent: Every MATL key should map to an HDR key.
-- Pass Criteria: No MATL keys missing in HDR.  --! 18 rows failed
-- ============================================================================
WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_matl AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_MATL_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_orphans AS (
	SELECT m.ID_LOC, m.ID_SO, m.SUFX_SO
	FROM src_matl m
	LEFT JOIN src_hdr h
	  ON m.ID_LOC = h.ID_LOC
	 AND m.ID_SO = h.ID_SO
	 AND m.SUFX_SO = h.SUFX_SO
	WHERE h.ID_LOC IS NULL
)
SELECT
	'T2C_SRC_MATL_ORPHAN_KEYS' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'MATL business keys with no matching HDR key.' AS notes
FROM src_orphans;

WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_matl AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_MATL_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	m.ID_LOC,
	m.ID_SO,
	m.SUFX_SO
FROM src_matl m
LEFT JOIN src_hdr h
  ON m.ID_LOC = h.ID_LOC
 AND m.ID_SO = h.ID_SO
 AND m.SUFX_SO = h.SUFX_SO
WHERE h.ID_LOC IS NULL
LIMIT 100;


-- ============================================================================
-- T3A_TGT_NULL_KEYS
-- Intent: Target key columns must be populated.
-- Pass Criteria: No NULL or blank key values in MASTER_SHOP_ORDER.
-- ============================================================================
WITH tgt_rows AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	'T3A_TGT_NULL_KEYS' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'NULL/blank business keys in MASTER_SHOP_ORDER.' AS notes
FROM tgt_rows
WHERE TRIM(COALESCE(ID_LOC, '')) = ''
   OR TRIM(COALESCE(ID_SO, '')) = ''
   OR SUFX_SO IS NULL;

WITH tgt_rows AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	TRIM(ID_LOC) AS ID_LOC,
	TRIM(ID_SO) AS ID_SO,
	SUFX_SO
FROM tgt_rows
WHERE TRIM(COALESCE(ID_LOC, '')) = ''
   OR TRIM(COALESCE(ID_SO, '')) = ''
   OR SUFX_SO IS NULL
LIMIT 100;


-- ============================================================================
-- T3B_TGT_DUP_KEYS
-- Intent: Target should be unique at business key grain.
-- Pass Criteria: No duplicate (ID_LOC, ID_SO, SUFX_SO) in target.
-- ============================================================================
WITH tgt_rows AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	-- AND LOAD_DT = :as_of_date
),
tgt_dups AS (
	SELECT ID_LOC, ID_SO, SUFX_SO, COUNT(*) AS dup_cnt
	FROM tgt_rows
	WHERE ID_LOC <> '' AND ID_SO <> '' AND SUFX_SO IS NOT NULL
	GROUP BY ID_LOC, ID_SO, SUFX_SO
	HAVING COUNT(*) > 1
)
SELECT
	'T3B_TGT_DUP_KEYS' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'Duplicate business keys in MASTER_SHOP_ORDER.' AS notes
FROM tgt_dups;

WITH tgt_rows AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	-- AND LOAD_DT = :as_of_date
)
SELECT
	ID_LOC,
	ID_SO,
	SUFX_SO,
	COUNT(*) AS dup_cnt
FROM tgt_rows
WHERE ID_LOC <> '' AND ID_SO <> '' AND SUFX_SO IS NOT NULL
GROUP BY ID_LOC, ID_SO, SUFX_SO
HAVING COUNT(*) > 1
ORDER BY dup_cnt DESC, ID_LOC, ID_SO, SUFX_SO
LIMIT 100;


-- ============================================================================
-- T4A_KEY_COUNT_COMPARE
-- Intent: Distinct key counts should match between HDR and target.
-- Pass Criteria: Distinct business key count delta = 0.
-- ============================================================================
WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
tgt_rows AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
cmp AS (
	SELECT (SELECT COUNT(*) FROM src_hdr) AS src_key_cnt,
		   (SELECT COUNT(*) FROM tgt_rows) AS tgt_key_cnt
)
SELECT
	'T4A_KEY_COUNT_COMPARE' AS test_id,
	'FAIL' AS severity,
	ABS(src_key_cnt - tgt_key_cnt) AS failed_rows,
	IFF(src_key_cnt = tgt_key_cnt, 'PASS', 'FAIL') AS status,
	'Distinct business key count mismatch between SHPORD_HDR and MASTER_SHOP_ORDER.' AS notes
FROM cmp;

WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
tgt_rows AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
)
SELECT
	(SELECT COUNT(*) FROM src_hdr) AS src_key_cnt,
	(SELECT COUNT(*) FROM tgt_rows) AS tgt_key_cnt,
	(SELECT COUNT(*) FROM src_hdr) - (SELECT COUNT(*) FROM tgt_rows) AS key_count_delta
LIMIT 100;


-- ============================================================================
-- T4B_MISSING_IN_TARGET
-- Intent: Every HDR key should exist in target.
-- Pass Criteria: No business keys in HDR missing from target.
-- ============================================================================
WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
tgt_rows AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
src_missing AS (
	SELECT h.ID_LOC, h.ID_SO, h.SUFX_SO
	FROM src_hdr h
	LEFT JOIN tgt_rows t
	  ON h.ID_LOC = t.ID_LOC
	 AND h.ID_SO = t.ID_SO
	 AND h.SUFX_SO = t.SUFX_SO
	WHERE t.ID_LOC IS NULL
)
SELECT
	'T4B_MISSING_IN_TARGET' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'Business keys present in SHPORD_HDR but missing in MASTER_SHOP_ORDER.' AS notes
FROM src_missing;

WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
tgt_rows AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
)
SELECT
	h.ID_LOC,
	h.ID_SO,
	h.SUFX_SO
FROM src_hdr h
LEFT JOIN tgt_rows t
  ON h.ID_LOC = t.ID_LOC
 AND h.ID_SO = t.ID_SO
 AND h.SUFX_SO = t.SUFX_SO
WHERE t.ID_LOC IS NULL
LIMIT 100;


-- ============================================================================
-- T4C_EXTRA_IN_TARGET
-- Intent: Target should not contain keys absent from HDR.
-- Pass Criteria: No business keys in target missing from HDR.
-- ============================================================================
WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
tgt_rows AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
tgt_extra AS (
	SELECT t.ID_LOC, t.ID_SO, t.SUFX_SO
	FROM tgt_rows t
	LEFT JOIN src_hdr h
	  ON t.ID_LOC = h.ID_LOC
	 AND t.ID_SO = h.ID_SO
	 AND t.SUFX_SO = h.SUFX_SO
	WHERE h.ID_LOC IS NULL
)
SELECT
	'T4C_EXTRA_IN_TARGET' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'Business keys present in MASTER_SHOP_ORDER but missing in SHPORD_HDR.' AS notes
FROM tgt_extra;

WITH src_hdr AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
tgt_rows AS (
	SELECT DISTINCT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
)
SELECT
	t.ID_LOC,
	t.ID_SO,
	t.SUFX_SO
FROM tgt_rows t
LEFT JOIN src_hdr h
  ON t.ID_LOC = h.ID_LOC
 AND t.ID_SO = h.ID_SO
 AND t.SUFX_SO = h.SUFX_SO
WHERE h.ID_LOC IS NULL
LIMIT 100;


-- ============================================================================
-- T5A_OPER_LINECOUNT_PROFILE
-- Intent: Profile operation line distribution by business key.
-- Pass Criteria: Informational; WARN if high operation line count keys exist.
-- ============================================================================
WITH src_oper AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_OPER_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
src_oper_prof AS (
	SELECT ID_LOC, ID_SO, SUFX_SO, COUNT(*) AS oper_line_cnt
	FROM src_oper
	GROUP BY ID_LOC, ID_SO, SUFX_SO
)
SELECT
	'T5A_OPER_LINECOUNT_PROFILE' AS test_id,
	'WARN' AS severity,
	COUNT_IF(oper_line_cnt > 100) AS failed_rows,
	IFF(COUNT_IF(oper_line_cnt > 100) = 0, 'PASS', 'FAIL') AS status,
	'Profile warning for keys with OPER line count > 100.' AS notes
FROM src_oper_prof;

WITH src_oper AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_OPER_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
)
SELECT
	ID_LOC,
	ID_SO,
	SUFX_SO,
	COUNT(*) AS oper_line_cnt
FROM src_oper
GROUP BY ID_LOC, ID_SO, SUFX_SO
ORDER BY oper_line_cnt DESC, ID_LOC, ID_SO, SUFX_SO
LIMIT 100;


-- ============================================================================
-- T5B_MATL_LINECOUNT_PROFILE
-- Intent: Profile material line distribution by business key.
-- Pass Criteria: Informational; WARN if high material line count keys exist.
-- ============================================================================
WITH src_matl AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_MATL_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
),
src_matl_prof AS (
	SELECT ID_LOC, ID_SO, SUFX_SO, COUNT(*) AS matl_line_cnt
	FROM src_matl
	GROUP BY ID_LOC, ID_SO, SUFX_SO
)
SELECT
	'T5B_MATL_LINECOUNT_PROFILE' AS test_id,
	'WARN' AS severity,
	COUNT_IF(matl_line_cnt > 100) AS failed_rows,
	IFF(COUNT_IF(matl_line_cnt > 100) = 0, 'PASS', 'FAIL') AS status,
	'Profile warning for keys with MATL line count > 100.' AS notes
FROM src_matl_prof;

WITH src_matl AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_MATL_OBJ)
	WHERE TRIM(COALESCE(ID_LOC, '')) <> ''
	  AND TRIM(COALESCE(ID_SO, '')) <> ''
	  AND SUFX_SO IS NOT NULL
	-- AND LOAD_DT = :as_of_date
)
SELECT
	ID_LOC,
	ID_SO,
	SUFX_SO,
	COUNT(*) AS matl_line_cnt
FROM src_matl
GROUP BY ID_LOC, ID_SO, SUFX_SO
ORDER BY matl_line_cnt DESC, ID_LOC, ID_SO, SUFX_SO
LIMIT 100;


-- ============================================================================
-- VALIDATION_SUMMARY
-- Intent: Single dashboard union of FAIL/WARN controls for governance tracking.
-- Note: Profile tests (T5A/T5B) are excluded from this pass/fail dashboard.
-- ============================================================================
WITH
src_hdr AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_HDR_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_oper AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_OPER_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_matl AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($SRC_MATL_OBJ)
	-- AND LOAD_DT = :as_of_date
),
tgt_rows AS (
	SELECT TRIM(ID_LOC) AS ID_LOC, TRIM(ID_SO) AS ID_SO, SUFX_SO
	FROM IDENTIFIER($TGT_OBJ)
	-- AND LOAD_DT = :as_of_date
),
src_hdr_nn AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM src_hdr
	WHERE ID_LOC <> '' AND ID_SO <> '' AND SUFX_SO IS NOT NULL
),
tgt_nn AS (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM tgt_rows
	WHERE ID_LOC <> '' AND ID_SO <> '' AND SUFX_SO IS NOT NULL
),
src_hdr_dist AS (
	SELECT DISTINCT ID_LOC, ID_SO, SUFX_SO
	FROM src_hdr_nn
),
src_oper_dist AS (
	SELECT DISTINCT ID_LOC, ID_SO, SUFX_SO
	FROM src_oper
	WHERE ID_LOC <> '' AND ID_SO <> '' AND SUFX_SO IS NOT NULL
),
src_matl_dist AS (
	SELECT DISTINCT ID_LOC, ID_SO, SUFX_SO
	FROM src_matl
	WHERE ID_LOC <> '' AND ID_SO <> '' AND SUFX_SO IS NOT NULL
),
tgt_dist AS (
	SELECT DISTINCT ID_LOC, ID_SO, SUFX_SO
	FROM tgt_nn
)
SELECT
	'T1A_SRC_HDR_NULL_KEYS' AS test_id,
	'FAIL' AS severity,
	COUNT(*) AS failed_rows,
	IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS status,
	'NULL/blank business keys in SHPORD_HDR after TRIM normalization.' AS notes
FROM src_hdr
WHERE ID_LOC = '' OR ID_SO = '' OR SUFX_SO IS NULL

UNION ALL

SELECT
	'T1B_SRC_OPER_NULL_KEYS',
	'FAIL',
	COUNT(*),
	IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
	'NULL/blank business keys in SHPORD_OPER after TRIM normalization.'
FROM src_oper
WHERE ID_LOC = '' OR ID_SO = '' OR SUFX_SO IS NULL

UNION ALL

SELECT
	'T1C_SRC_MATL_NULL_KEYS',
	'FAIL',
	COUNT(*),
	IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
	'NULL/blank business keys in SHPORD_MATL after TRIM normalization.'
FROM src_matl
WHERE ID_LOC = '' OR ID_SO = '' OR SUFX_SO IS NULL

UNION ALL

SELECT
	'T2A_SRC_HDR_DUP_KEYS',
	'FAIL',
	COUNT(*),
	IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
	'Duplicate business keys in SHPORD_HDR.'
FROM (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM src_hdr_nn
	GROUP BY ID_LOC, ID_SO, SUFX_SO
	HAVING COUNT(*) > 1
) d

UNION ALL

SELECT
	'T2B_SRC_OPER_ORPHAN_KEYS',
	'FAIL',
	COUNT(*),
	IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
	'OPER business keys with no matching HDR key.'
FROM (
	SELECT o.ID_LOC, o.ID_SO, o.SUFX_SO
	FROM src_oper_dist o
	LEFT JOIN src_hdr_dist h
	  ON o.ID_LOC = h.ID_LOC
	 AND o.ID_SO = h.ID_SO
	 AND o.SUFX_SO = h.SUFX_SO
	WHERE h.ID_LOC IS NULL
) d

UNION ALL

SELECT
	'T2C_SRC_MATL_ORPHAN_KEYS',
	'FAIL',
	COUNT(*),
	IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
	'MATL business keys with no matching HDR key.'
FROM (
	SELECT m.ID_LOC, m.ID_SO, m.SUFX_SO
	FROM src_matl_dist m
	LEFT JOIN src_hdr_dist h
	  ON m.ID_LOC = h.ID_LOC
	 AND m.ID_SO = h.ID_SO
	 AND m.SUFX_SO = h.SUFX_SO
	WHERE h.ID_LOC IS NULL
) d

UNION ALL

SELECT
	'T3A_TGT_NULL_KEYS',
	'FAIL',
	COUNT(*),
	IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
	'NULL/blank business keys in MASTER_SHOP_ORDER.'
FROM tgt_rows
WHERE ID_LOC = '' OR ID_SO = '' OR SUFX_SO IS NULL

UNION ALL

SELECT
	'T3B_TGT_DUP_KEYS',
	'FAIL',
	COUNT(*),
	IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
	'Duplicate business keys in MASTER_SHOP_ORDER.'
FROM (
	SELECT ID_LOC, ID_SO, SUFX_SO
	FROM tgt_nn
	GROUP BY ID_LOC, ID_SO, SUFX_SO
	HAVING COUNT(*) > 1
) d

UNION ALL

SELECT
	'T4A_KEY_COUNT_COMPARE',
	'FAIL',
	ABS((SELECT COUNT(*) FROM src_hdr_dist) - (SELECT COUNT(*) FROM tgt_dist)),
	IFF((SELECT COUNT(*) FROM src_hdr_dist) = (SELECT COUNT(*) FROM tgt_dist), 'PASS', 'FAIL'),
	'Distinct business key count mismatch between SHPORD_HDR and MASTER_SHOP_ORDER.'

UNION ALL

SELECT
	'T4B_MISSING_IN_TARGET',
	'FAIL',
	COUNT(*),
	IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
	'Business keys present in SHPORD_HDR but missing in MASTER_SHOP_ORDER.'
FROM (
	SELECT h.ID_LOC, h.ID_SO, h.SUFX_SO
	FROM src_hdr_dist h
	LEFT JOIN tgt_dist t
	  ON h.ID_LOC = t.ID_LOC
	 AND h.ID_SO = t.ID_SO
	 AND h.SUFX_SO = t.SUFX_SO
	WHERE t.ID_LOC IS NULL
) d

UNION ALL

SELECT
	'T4C_EXTRA_IN_TARGET',
	'FAIL',
	COUNT(*),
	IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
	'Business keys present in MASTER_SHOP_ORDER but missing in SHPORD_HDR.'
FROM (
	SELECT t.ID_LOC, t.ID_SO, t.SUFX_SO
	FROM tgt_dist t
	LEFT JOIN src_hdr_dist h
	  ON t.ID_LOC = h.ID_LOC
	 AND t.ID_SO = h.ID_SO
	 AND t.SUFX_SO = h.SUFX_SO
	WHERE h.ID_LOC IS NULL
) d
ORDER BY test_id;

