WITH PrimaryDescriptions AS (
    -- Priority 1: Get descriptions from ITMMAS_DESCR_Bronze
    SELECT
        id_item,
        LISTAGG(descr_addl, '') WITHIN GROUP (ORDER BY SEQ_DESCR) AS primary_description
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze"
    WHERE seq_descr BETWEEN 800 AND 810
    GROUP BY id_item
),
FallbackDescriptions AS (
    -- Priority 2: Process DESCR_2 from ITMMAS_BASE_Bronze
    SELECT
        id_item,
        CASE
            -- Priority 2.1.1: If more than 2 comma-separated values, get the first two
            WHEN ARRAY_SIZE(SPLIT(DESCR_2, ',')) > 2 
            THEN SPLIT(DESCR_2, ',')[0] || ',' || SPLIT(DESCR_2, ',')[1]
            -- Otherwise, use DESCR_2 as is
            ELSE DESCR_2
        END AS fallback_description
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze"
    WHERE DESCR_2 IS NOT NULL AND DESCR_2 != '' -- Priority 2.1: Check if DESCR_2 exists
      AND DESCR_1 != 'MISSING PARENT DESCRIPTION' -- Priority 2.1.1: Exclude specified records
)
-- Final SELECT to combine results based on your logic
SELECT
    ib.id_item,
    ib.FLAG_STAT_ITEM AS PARENT_ITEM_STATUS,
    CASE
        -- Priority 1: Use primary description if it exists
        WHEN pd.primary_description IS NOT NULL THEN pd.primary_description
        -- Priority 2: Use fallback description if it exists
        WHEN fd.fallback_description IS NOT NULL THEN fd.fallback_description
        -- Priority 2.2: Final fallback if no description is found
        ELSE 'Description doesn''t exist in any tcm table.'
    END AS "PARENT DESCRIPTION"
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
LEFT JOIN PrimaryDescriptions pd ON ib.id_item = pd.id_item
LEFT JOIN FallbackDescriptions fd ON ib.id_item = fd.id_item
WHERE ib.code_comm = 'PAR'
GROUP BY ib.id_item, ib.FLAG_STAT_ITEM, pd.primary_description, fd.fallback_description
HAVING ib.id_item LIKE '15812-01%';


-- ========= Extract DESCR_2 from Child Items ==========
WITH PrimaryDescriptions AS (
    -- Priority 1: Get descriptions from ITMMAS_DESCR_Bronze
    SELECT
        id_item,
        LISTAGG(descr_addl, '') WITHIN GROUP (ORDER BY SEQ_DESCR) AS primary_description
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze"
    WHERE seq_descr BETWEEN 800 AND 810
    GROUP BY id_item
),
FallbackDescriptions AS (
    -- Priority 2: Get fallback descriptions from the first valid child item
    SELECT 
        parent_id_item,
        fallback_description
    FROM (
        SELECT 
            p.id_item AS parent_id_item,
            CASE
                -- If more than 2 comma-separated values, get the first two
                WHEN ARRAY_SIZE(SPLIT(c.DESCR_2, ',')) > 2 
                THEN SPLIT(c.DESCR_2, ',')[0] || ',' || SPLIT(c.DESCR_2, ',')[1]
                -- Otherwise, use DESCR_2 as is
                ELSE c.DESCR_2
            END AS fallback_description,
            -- Assign a row number to each child within a parent group
            ROW_NUMBER() OVER(PARTITION BY p.id_item ORDER BY c.id_item) as rn
        FROM 
            BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" p
        JOIN 
            BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" c 
            ON c.id_item LIKE p.id_item || '%' AND c.id_item != p.id_item
        WHERE 
            p.code_comm = 'PAR'
            AND c.DESCR_1 != 'PARENT ITEM FOR LABELS'
            AND c.DESCR_2 IS NOT NULL 
            AND c.DESCR_2 != ''
    )
    -- Select only the first child record for each parent
    WHERE rn = 1
)
SELECT
    ib.id_item,
    ib.FLAG_STAT_ITEM AS PARENT_ITEM_STATUS,
    COALESCE(
        pd.primary_description,
        fd.fallback_description,
        'Description doesn''t exist in any tcm table.'
    ) AS "PARENT DESCRIPTION"
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
LEFT JOIN PrimaryDescriptions pd ON ib.id_item = pd.id_item
LEFT JOIN FallbackDescriptions fd ON ib.id_item = fd.parent_id_item
WHERE ib.code_comm = 'PAR';