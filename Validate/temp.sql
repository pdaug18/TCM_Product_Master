WITH sku_attributes AS (
    SELECT
        TRIM(ib.id_item) AS "Item_ID_Child_SKU",
        MAX(
            CASE
                WHEN TRIM(av.id_attr) = 'ID_PARENT' THEN TRIM(av.val_string_attr)
                ELSE ''
            END
        ) AS "Item_ID_Parent_SKU"
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze" av
           ON TRIM(ib.id_item) = TRIM(av.id_item)
          AND ib.code_comm = av.code_comm
    -- WHERE ib.code_comm <> 'PAR'
    WHERE ib.code_comm = 'FG'
    GROUP BY TRIM(ib.id_item)
)
SELECT
    COUNT(DISTINCT s."Item_ID_Child_SKU") AS "Distinct_Child_SKU_Count",
    COUNT(DISTINCT NULLIF(TRIM(s."Item_ID_Parent_SKU"), '')) AS "Distinct_Parent_SKU_Count",
    COUNT_IF(NULLIF(TRIM(s."Item_ID_Parent_SKU"), '') IS NULL) AS "Missing_Parent_SKU_Count",
    ROUND(
        COUNT_IF(NULLIF(TRIM(s."Item_ID_Parent_SKU"), '') IS NULL)
        * 100.0
        / NULLIF(COUNT(DISTINCT s."Item_ID_Child_SKU"), 0),
        2
    ) AS "Missing_Parent_SKU_Percent"
FROM sku_attributes s;

