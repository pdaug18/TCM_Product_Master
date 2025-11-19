/*
This is the parent table. It contains the core record for a unique item (SKU).
Primary Key: Usually a column like ID_ITEM or ITEM_ID.
*/
SELECT * 
FROM nsa.ITMMAS_BASE;

-- Filter to only 'PAR' commodity code items (This gives a subset of items classified as 'Parent' items)
SELECT * 
FROM nsa.ITMMAS_BASE
WHERE CODE_COMM = 'PAR';



SELECT * 
FROM nsa.ITMMAS_DESCR;
/*
This table holds extended descriptions or language-specific descriptions for the items in the base table.
Connection: It joins to ITMMAS_BASE on the Item ID column (e.g., ID_ITEM).
Relationship: One-to-One or One-to-Many (if multiple languages are supported).
*/

SELECT *
FROM nsa.IM_CMCD_ATTR_VALUE;
/*
This table likely stores specific attribute values associated with the commodity code assigned to an item (e.g., Color = Red, Size = Large).
Connection: It joins to ITMMAS_BASE on the Item ID (ID_ITEM). It might also join via a Commodity Code column (ID_COMM_CODE) found in the base table.
Relationship: One-to-Many (one item can have multiple attribute values).
*/

SELECT 
    base.ID_ITEM,
    base.CODE_COMM,
    descr.DESCR,
    base.FLAG_STAT_ITEM,
    attr.ID_ATTR,
    attr.VAL_STRING_ATTR,
    attr.VAL_NUM_ATTR,
    attr.VAL_DATE_ATTR
FROM 
    nsa.ITMMAS_BASE base
JOIN 
    nsa.ITMMAS_DESCR descr ON base.ID_ITEM = descr.ID_ITEM
LEFT JOIN 
    nsa.IM_CMCD_ATTR_VALUE attr ON base.ID_ITEM = attr.ID_ITEM
SELECT 
    base.ID_ITEM,
    base.CODE_COMM,
    descr.DESCR,
    base.FLAG_STAT_ITEM,
    attr.ID_ATTR,
    attr.VAL_STRING_ATTR,
    attr.VAL_NUM_ATTR,
    attr.VAL_DATE_ATTR
FROM 
    nsa.ITMMAS_BASE base
JOIN 
    nsa.ITMMAS_DESCR descr ON base.ID_ITEM = descr.ID_ITEM
LEFT JOIN 
    nsa.IM_CMCD_ATTR_VALUE attr ON base.ID_ITEM = attr.ID_ITEM;

-- Analyze Parent Item Status distribution mapped from FLAG_STAT_ITEM in ITMMAS_BASE table for 'PAR' commodity code items
WITH parent_sku_attributes AS (
    SELECT
        ib.id_item,
        MAX(CASE WHEN av.id_attr = 'ID_PARENT' THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) ID_PARENT"
    FROM nsa.ITMMAS_BASE ib
    LEFT JOIN nsa.IM_CMCD_ATTR_VALUE av
           ON ib.id_item = av.id_item
          AND ib.code_comm = av.code_comm
    WHERE ib.code_comm <> 'PAR'
    GROUP BY ib.id_item
),
parent_items AS (
    SELECT
        ib.id_item AS parent_id_item,
        ib.FLAG_STAT_ITEM AS parent_item_status
    FROM nsa.ITMMAS_BASE ib
    WHERE ib.code_comm = 'PAR'
),
child_items AS (
    SELECT
        ib.id_item AS child_id_item,
        ib.FLAG_STAT_ITEM AS child_item_status,
        psa."ATTR (SKU) ID_PARENT" AS parent_id_item
    FROM nsa.ITMMAS_BASE ib
    JOIN parent_sku_attributes psa ON ib.id_item = psa.id_item
    WHERE ib.code_comm <> 'PAR'
),
joined_items AS (
    SELECT
        ci.child_id_item,
        ci.child_item_status,
        pi.parent_id_item,
        pi.parent_item_status
    FROM child_items ci
    JOIN parent_items pi ON ci.parent_id_item = pi.parent_id_item
)
SELECT * 
FROM joined_items;
