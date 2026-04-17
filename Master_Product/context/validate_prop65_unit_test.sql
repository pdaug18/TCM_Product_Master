-- Unit test script for PROP 65 logic validation
-- Purpose:
-- 1) Reproduce the current logic against physical tables (no CTE aliases).
-- 2) Validate the DISTINCT-based rewrite that avoids GROUP BY parser issues.
-- 3) Run basic PASS/FAIL checks for output quality.

-- =====================================================================
-- TEST A (Optional): Current pattern using GROUP BY
-- Note: Run this block only if you want to verify whether your environment
--       still throws: '... neither an aggregate nor in the group by clause'.
-- =====================================================================
/*
SELECT
    TRIM(p.id_item_par) AS id_item_par,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM (
                SELECT
                    TRIM(p2.id_item_par) AS id_item_par,
                    TRIM(p2.id_item_comp) AS id_item_comp,
                    p2.date_eff_end
                FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" p2
                INNER JOIN (
                    SELECT
                        TRIM(px.id_item_comp) AS id_item_comp,
                        MAX(px.date_eff_end) AS max_eff_end
                    FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" px
                    INNER JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ibx
                        ON TRIM(px.id_item_comp) = TRIM(ibx.id_item)
                    WHERE ibx.flag_stat_item = 'A'
                      AND px.date_eff_end > CURRENT_DATE()
                    GROUP BY TRIM(px.id_item_comp)
                ) latest
                    ON TRIM(p2.id_item_comp) = latest.id_item_comp
                   AND p2.date_eff_end = latest.max_eff_end
                INNER JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib2
                    ON TRIM(p2.id_item_comp) = TRIM(ib2.id_item)
                WHERE ib2.flag_stat_item = 'A'
                  AND p2.date_eff_end > CURRENT_DATE()
            ) latest_comp
            INNER JOIN (
                SELECT
                    TRIM(id.id_item) AS id_item,
                    id.descr_addl,
                    id.seq_descr,
                    id."rowid"
                FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze" id
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY TRIM(id.id_item), id.seq_descr
                    ORDER BY id."rowid" DESC
                ) = 1
            ) d
                ON latest_comp.id_item_comp = d.id_item
            WHERE latest_comp.id_item_par = TRIM(p.id_item_par)
              AND d.descr_addl LIKE '%PROP 65%'
        ) THEN 'Y'
        ELSE 'N'
    END AS prop_65
FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" p
INNER JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
    ON TRIM(p.id_item_comp) = TRIM(ib.id_item)
WHERE ib.flag_stat_item = 'A'
  AND p.date_eff_end > CURRENT_DATE()
GROUP BY TRIM(p.id_item_par);
*/

-- =====================================================================
-- TEST B: DISTINCT-based version (recommended)
-- =====================================================================
CREATE OR REPLACE TEMP TABLE UT_PROP65_DISTINCT AS
SELECT DISTINCT
    TRIM(p.id_item_par) AS id_item_par,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM (
                SELECT
                    TRIM(p2.id_item_par) AS id_item_par,
                    TRIM(p2.id_item_comp) AS id_item_comp,
                    p2.date_eff_end
                FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" p2
                INNER JOIN (
                    SELECT
                        TRIM(px.id_item_comp) AS id_item_comp,
                        MAX(px.date_eff_end) AS max_eff_end
                    FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" px
                    INNER JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ibx
                        ON TRIM(px.id_item_comp) = TRIM(ibx.id_item)
                    WHERE ibx.flag_stat_item = 'A'
                      AND px.date_eff_end > CURRENT_DATE()
                    GROUP BY TRIM(px.id_item_comp)
                ) latest
                    ON TRIM(p2.id_item_comp) = latest.id_item_comp
                   AND p2.date_eff_end = latest.max_eff_end
                INNER JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib2
                    ON TRIM(p2.id_item_comp) = TRIM(ib2.id_item)
                WHERE ib2.flag_stat_item = 'A'
                  AND p2.date_eff_end > CURRENT_DATE()
            ) latest_comp
            INNER JOIN (
                SELECT
                    TRIM(id.id_item) AS id_item,
                    id.descr_addl,
                    id.seq_descr,
                    id."rowid"
                FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze" id
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY TRIM(id.id_item), id.seq_descr
                    ORDER BY id."rowid" DESC
                ) = 1
            ) d
                ON latest_comp.id_item_comp = d.id_item
            WHERE latest_comp.id_item_par = TRIM(p.id_item_par)
              AND d.descr_addl LIKE '%PROP 65%'
        ) THEN 'Y'
        ELSE 'N'
    END AS prop_65
FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" p
INNER JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
    ON TRIM(p.id_item_comp) = TRIM(ib.id_item)
WHERE ib.flag_stat_item = 'A'
  AND p.date_eff_end > CURRENT_DATE();

-- Preview sample output
SELECT *
FROM UT_PROP65_DISTINCT
ORDER BY id_item_par
LIMIT 100;

-- =====================================================================
-- Assertions (PASS/FAIL)
-- =====================================================================
SELECT
    'UT_PROP65_01_ROWS_EXIST' AS test_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    COUNT(*) AS observed_value
FROM UT_PROP65_DISTINCT;

SELECT
    'UT_PROP65_02_ONE_ROW_PER_PARENT' AS test_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    COUNT(*) AS observed_value
FROM (
    SELECT id_item_par
    FROM UT_PROP65_DISTINCT
    GROUP BY id_item_par
    HAVING COUNT(*) > 1
) dup;

SELECT
    'UT_PROP65_03_ONLY_Y_OR_N' AS test_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    COUNT(*) AS observed_value
FROM UT_PROP65_DISTINCT
WHERE prop_65 NOT IN ('Y', 'N')
   OR prop_65 IS NULL;
