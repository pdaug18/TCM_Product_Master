    SELECT
        ib.id_item,
        LISTAGG(id.descr_addl, '') 
        WITHIN GROUP (ORDER BY SEQ_DESCR) AS "PARENT DESCRIPTION"
    FROM "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic" ib
    LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_DESCR_Bronze" id
        ON ib.id_item = id.id_item
    WHERE ib.code_comm = 'PAR'
        AND id.seq_descr BETWEEN 800 AND 810
        AND ib."is_deleted" = 0
    GROUP BY ib.id_item
    limit 1000;
    -- HAVING ib.id_item = 'VNT99345';

    -- DF2-AX3-324LS-HY

select ID_ITEM, DESCR_ADDL, SEQ_DESCR 
-- id_item, LISTAGG(descr_addl, ', ')  
from "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_DESCR_Bronze" 
-- group by id_item
-- having 
where id_item = 'VNT99345'; 
-- limit 1000;


-- example: VNT99345
select id_item, "Product Description"
from (
    SELECT 
        ib.id_item,
        ib.DESCR_1 || ' ' || ib.DESCR_2 AS "Product Description"
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
)
where "Product Description" is null;


SELECT
    ib.id_item,
    ib.FLAG_STAT_ITEM AS PARENT_ITEM_STATUS,
    CASE
        WHEN COUNT(id.id_item) > 0 
            THEN LISTAGG(id.descr_addl, '') WITHIN GROUP (ORDER BY id.SEQ_DESCR)
        WHEN ib.DESCR_1 != 'PARENT ITEM FOR LABELS'
            THEN ib.DESCR_2
        ELSE 'MISSING PARENT DESCRIPTION' 
    END AS "PARENT DESCRIPTION"
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
LEFT JOIN (
    SELECT id_item, descr_addl, SEQ_DESCR
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze"
    WHERE seq_descr BETWEEN 800 AND 810
    -- AND id_item = '15812-01%'
) id ON ib.id_item = id.id_item
WHERE ib.code_comm = 'PAR'
GROUP BY ib.id_item, ib.FLAG_STAT_ITEM, ib.DESCR_1, ib.DESCR_2
HAVING ib.id_item LIKE '15812-01%';



select * 
from BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze"
where 
id_item like '174I%'
-- 10861-01, 15812-01%
AND seq_descr BETWEEN 800 AND 810;

select ID_ITEM, DESCR_1, DESCR_2
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze"
where id_item like '174I%'
-- 10861-01, 15812-01%
;

-- ========== Final Query ==========
WITH child_descriptions AS (
    SELECT 
        parent_id_item, 
        child_descr_2
    FROM (
        SELECT 
            p.id_item AS parent_id_item,
            c.DESCR_2 AS child_descr_2,
            ROW_NUMBER() OVER(PARTITION BY p.id_item ORDER BY c.id_item) as rn
        FROM 
            BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" p
        JOIN 
            BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" c 
            ON c.id_item LIKE p.id_item || '%' AND c.id_item != p.id_item
        WHERE 
            p.code_comm = 'PAR'
            AND c.DESCR_1 != 'PARENT ITEM FOR LABELS'
    )
    WHERE rn = 1
)
SELECT
    ib.id_item,
    ib.FLAG_STAT_ITEM AS PARENT_ITEM_STATUS,
    CASE
        WHEN COUNT(id.id_item) > 0 
            THEN LISTAGG(id.descr_addl, '') WITHIN GROUP (ORDER BY id.SEQ_DESCR)
        ELSE COALESCE(cd.child_descr_2, 'MISSING PARENT DESCRIPTION')
    END AS "PARENT DESCRIPTION"
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
LEFT JOIN (
    SELECT id_item, descr_addl, SEQ_DESCR
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze"
    WHERE seq_descr BETWEEN 800 AND 810
) id ON ib.id_item = id.id_item
LEFT JOIN child_descriptions cd ON ib.id_item = cd.parent_id_item
WHERE ib.code_comm = 'PAR'
-- AND id.id_item is null
GROUP BY ib.id_item, ib.FLAG_STAT_ITEM, ib.DESCR_1, ib.DESCR_2, cd.child_descr_2;
-- HAVING ib.id_item LIKE '15812-01%';



--- ========== Final Query V3 ==========

WITH primary_descriptions AS (
    -- 1. Get the primary description from the description table
    SELECT
        id.id_item,
        LISTAGG(id.descr_addl, '') WITHIN GROUP (ORDER BY id.SEQ_DESCR) AS primary_description
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze" id
    WHERE id.seq_descr BETWEEN 800 AND 810
    GROUP BY id.id_item
),
fallback_descriptions AS (
    -- 2. Prepare the fallback description from a related child item
    SELECT 
        p.id_item as parent_id_item,
        c.DESCR_2 as fallback_description
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" p
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" c
        ON c.id_item LIKE p.id_item || '%' -- Find child items
        AND c.id_item != p.id_item         -- Exclude the parent itself
        AND c.DESCR_1 != 'PARENT ITEM FOR LABELS' -- Ensure it's a child record
    WHERE p.code_comm = 'PAR'
    -- Efficiently get only the first valid child description for each parent
    QUALIFY ROW_NUMBER() OVER(PARTITION BY p.id_item ORDER BY c.id_item) = 1
)
-- 3. Combine the results
SELECT
    ib.id_item,
    ib.FLAG_STAT_ITEM AS PARENT_ITEM_STATUS,
    COALESCE(
        pd.primary_description,
        fd.fallback_description,
        'MISSING PARENT DESCRIPTION'
    ) AS "PARENT DESCRIPTION"
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
LEFT JOIN primary_descriptions pd ON ib.id_item = pd.id_item
LEFT JOIN fallback_descriptions fd ON ib.id_item = fd.parent_id_item
WHERE ib.code_comm = 'PAR';
-- AND ib.id_item like '15812-01%';

-- ========== Optimized Fallback Description Query ==========

WITH PotentialChildren AS (
    -- 1. First, select only the rows that can be children.
    -- This reduces the number of rows to be scanned in the main join.
    SELECT id_item, DESCR_2
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze"
    WHERE DESCR_1 != 'PARENT ITEM FOR LABELS'
)
-- 2. Prepare the fallback description from a related child item
SELECT 
    p.id_item as parent_id_item,
    c.DESCR_2 as fallback_description
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" p
-- Use an INNER JOIN as we only need parents with at least one child
JOIN PotentialChildren c
    ON c.id_item LIKE p.id_item || '%' -- This LIKE is the main bottleneck, but now runs on a smaller dataset
    AND c.id_item != p.id_item         -- Exclude the parent itself
WHERE p.code_comm = 'PAR'
-- Efficiently get only the first valid child description for each parent
QUALIFY ROW_NUMBER() OVER(PARTITION BY p.id_item ORDER BY c.id_item) = 1;
