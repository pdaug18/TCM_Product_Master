SELECT
    "Product ID/SKU",
    COUNT(DISTINCT "ID_PLANNER") AS planner_count,
    LISTAGG(DISTINCT "ID_PLANNER", ', ') WITHIN GROUP (ORDER BY "ID_PLANNER") AS planners
FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE
GROUP BY "Product ID/SKU"
HAVING COUNT(DISTINCT "ID_PLANNER") > 1
ORDER BY planner_count DESC;


-- Items where id_planner resolved to NULL (not in ITMMAS_LOC with M or P)  --! 266 items with null planner, why !?
SELECT "Product ID/SKU", "ID_LOC", "Child Item Status", "ID_PLANNER"
FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE
WHERE "ID_PLANNER" IS NULL
AND "Product ID/SKU" = 'C04UPLG5503'

-- Summary: how many items have vs. lack a planner  --! 266 items with null planner, why !?
SELECT
    CASE WHEN "ID_PLANNER" IS NULL THEN 'NO PLANNER' ELSE 'HAS PLANNER' END AS planner_status,
    COUNT(*) AS item_count
FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE
GROUP BY planner_status;




WITH multi_mfg AS (
    SELECT id_item
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze"
    WHERE flag_source = 'M'
    GROUP BY id_item
    HAVING COUNT(*) > 1
),
has_loc_10 AS (
    SELECT id_item
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze"
    WHERE flag_source = 'M' AND id_loc = '10'
)
SELECT
    mpt."Product ID/SKU",
    mpt."ID_PLANNER"  AS resolved_planner,
    loc10.id_planner  AS loc_10_planner,
    IFF(mpt."ID_PLANNER" = loc10.id_planner, 'PASS', 'FAIL') AS hq_precedence_check
FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE mpt
JOIN multi_mfg mm   ON mpt."Product ID/SKU" = mm.id_item
JOIN has_loc_10 h10 ON mpt."Product ID/SKU" = h10.id_item
JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze" loc10
    ON mpt."Product ID/SKU" = loc10.id_item
   AND loc10.flag_source = 'M'
   AND loc10.id_loc = '10'
HAVING hq_precedence_check = 'FAIL';


-- Items manufactured at multiple locations, none of which is '10'
SELECT
    mpt."Product ID/SKU",
    mpt."ID_PLANNER"  AS resolved_planner,
    STRING_AGG(il.id_loc || ' (' || il.id_planner || ')', ', ')
        WITHIN GROUP (ORDER BY il.id_loc) AS all_mfg_locs_and_planners
FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE mpt
JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze" il
    ON mpt."Product ID/SKU" = il.id_item
   AND il.flag_source = 'M'
WHERE mpt."Product ID/SKU" NOT IN (
    SELECT id_item
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze"
    WHERE flag_source = 'M' AND id_loc = '10'
)
GROUP BY mpt."Product ID/SKU", mpt."ID_PLANNER"
HAVING COUNT(*) > 1
ORDER BY mpt."Product ID/SKU";


-- Items with only P rows (no M row) — should never be NULL if P rows have id_planner populated
SELECT
    mpt."Product ID/SKU",
    mpt."ID_PLANNER",
    IFF(mpt."ID_PLANNER" IS NULL, 'FAIL - NULL PLANNER', 'PASS') AS check_result
FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE mpt
WHERE mpt."Product ID/SKU" NOT IN (
    SELECT id_item FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze" WHERE flag_source = 'M'
)
OR mpt."Product ID/SKU" IN (
    SELECT id_item FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze" WHERE flag_source = 'P'
)
HAVING check_result = 'FAIL - NULL PLANNER';



-- Breakdown of planner assignments — spot unexpected NULLs or concentrations
SELECT
    "ID_PLANNER",
    COUNT(*)                                   AS item_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE
GROUP BY "ID_PLANNER"
ORDER BY item_count DESC;


SELECT
    'Unique planner per item'                    AS test,
    IFF(COUNT(*) = 0, 'PASS', 'FAIL')            AS result,
    COUNT(*)                                     AS failing_rows
FROM (
    SELECT "Product ID/SKU"
    FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE
    GROUP BY "Product ID/SKU"
    HAVING COUNT(DISTINCT "ID_PLANNER") > 1
)
UNION ALL
SELECT
    'No NULL planners for items in ITMMAS_LOC'   AS test,
    IFF(COUNT(*) = 0, 'PASS', 'FAIL')            AS result,
    COUNT(*)                                     AS failing_rows
FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE mpt
WHERE mpt."ID_PLANNER" IS NULL
AND EXISTS (
    SELECT 1 FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze" il
    WHERE il.id_item = mpt."Product ID/SKU"
    AND il.flag_source IN ('M','P')
);


select * 
from BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze"
where id_item = 'C04UPLG5503'