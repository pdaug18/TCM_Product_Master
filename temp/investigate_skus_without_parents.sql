/*
This query investigates why "Parent Item Status" in the MASTER_PRODUCT_TABLE has a large number of NULL values.
It counts the number of SKUs that do not have a corresponding parent ID in the sku_attributes table.
This will help confirm that a significant reason for the NULLs is that many SKUs are not linked to a parent.
*/
WITH sku_attributes AS (    -- 46,686 SKUs without Parent in Snowflake
    SELECT      
        ib.id_item,
        MAX(CASE WHEN av.id_attr = 'ID_PARENT' THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) ID_PARENT"
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze" av
           ON ib.id_item = av.id_item
          AND ib.code_comm = av.code_comm
    WHERE ib.code_comm <> 'PAR'
    --   AND ib."is_deleted" = 0 
    --   AND av."is_deleted" = 0
    GROUP BY ib.id_item
)
SELECT 
    COUNT(*) AS "SKUs_without_Parent"
FROM sku_attributes
WHERE "ATTR (SKU) ID_PARENT" IS NULL OR "ATTR (SKU) ID_PARENT" = '';

WITH sku_attributes AS (    -- 70,223 SKUs without Parent in TCM
    SELECT
        ib.id_item,
        MAX(CASE WHEN av.id_attr = 'ID_PARENT' THEN av.val_string_attr ELSE '' END) AS [ATTR (SKU) ID_PARENT]
    FROM nsa.ITMMAS_BASE ib
    LEFT JOIN nsa.IM_CMCD_ATTR_VALUE av
           ON ib.id_item = av.id_item
          AND ib.code_comm = av.code_comm
    WHERE ib.code_comm <> 'PAR'
    GROUP BY ib.id_item
)
SELECT 
    COUNT(*) AS [SKUs_without_Parent]
FROM sku_attributes
WHERE [ATTR (SKU) ID_PARENT] IS NULL OR [ATTR (SKU) ID_PARENT] = '';


select * from nsa.ITMMAS_BASE
WHERE LTRIM(ID_ITEM) LIKE 'DF2-505RHB-OD';

select DATE_INVC from nsa.CP_INVLIN_HIST
WHERE LTRIM(ID_ITEM) LIKE 'C52JTSRC2MO6X';

select ID_ITEM, DESCR_1, DESCR_2, FLAG_STAT_ITEM from nsa.ITMMAS_BASE
WHERE LTRIM(ID_ITEM) LIKE '160SG%';
/*
| ID_ITEM     | DESCR_1                 | DESCR_2           | FLAG_STAT_ITEM |
|-------------|-------------------------|-------------------|----------------|
| 160SG       | PARENT ITEM FOR LABELS  | DO NOT USE        | O              |
| 160SG-2XRG  | POLO/POLY/SHORT SLEEVE  | GREY, SIZE 2X     | A              |
| 160SG-3XRG  | POLO/POLY/SHORT SLEEVE  | GREY, SIZE 3X     | A              |
| 160SG-4XRG  | POLO/POLY/SHORT SLEEVE  | GREY, SIZE 4X     | A              |
| 160SG-5XRG  | POLO/POLY/SHORT SLEEVE  | GREY, SIZE 5X     | A              |
| 160SG-LGRG  | POLO/POLY/SHORT SLEEVE  | GREY, SIZE LG     | A              |
| 160SG-MDRG  | POLO/POLY/SHORT SLEEVE  | GREY, SIZE MD     | A              |
| 160SG-XLRG  | POLO/POLY/SHORT SLEEVE  | GREY, SIZE XL     | A              |
*/
