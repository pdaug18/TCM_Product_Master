select count(*) from BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic__test" ;

/* Distinct FLAG_STAT_ITEM values in ITMMAS_BASE_Bronze with record counts:
┌───────────────┬───────────┬───────────┐
│ FLAG_STAT_ITEM│ sf_count  │ TCM_Count │
├───────────────┼───────────┼───────────┤
│ A             │    67819  │ 67823     │
│               │        1  │ 1         │
│ O             │    96461  │ 96462     │
│ P             │        4  │ 4         │
│ M             │        3  │ 3         │
│ F             │        1  │ 1         │
└───────────────┴───────────┴───────────┘
*/

-- 10 example from all_sales_hist with NULL child Item Status - product ID sku
select "Product ID/SKU", "Product Description" from BRONZE_DATA.TCM_BRONZE."ALL_SALES_HIST"
where "Child Item Status" is null
limit 100;

select 
    DISTINCT "Product ID/SKU" 
from BRONZE_DATA.TCM_BRONZE."ALL_SALES_HIST"
where 
-- "Product ID/SKU" is not null
 "Child Item Status" is null
AND "INVOICE DATE" >= '1/1/2022';

select "Child Item Status", "Parent Item Status" from SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE
where "Product ID/SKU" = 'C52JTSRC2MO6X';
-- 'DF2-505RHB-OD';


select 

select distinct 
    "Child Item Status", count (*) as CHILD_REC_COUNT,
    -- "Parent Item Status", count (*) as PARENT_REC_COUNT
-- FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE
FROM BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST
GROUP BY 
    "Child Item Status"
    -- "Parent Item Status"
ORDER BY CHILD_REC_COUNT desc;

/* child item status values:
┌───────────────────────┬────────────────┐
│ Child Item Status     │ CHILD_REC_COUNT│
├───────────────────────┼────────────────┤
│ O                     │        86759   │ 87693
│ A                     │        62383   │ 67799
│ P                     │            4   │ 4
│ M                     │            3   │ 3
│                       │            1   │ 1
│ F                     │            1   │ 1
└───────────────────────┴────────────────┘

Parent Item Status values:
┌───────────────────────┬────────────────┐
│ Parent Item Status    │PARENT_REC_COUNT│
├───────────────────────┼────────────────┤
| null                  | 128449         | 134712
| O                     | 20592          | 20769
| A                     | 110            | 20
└───────────────────────┴────────────────┘
*/


select distinct PARENT_ITEM_STATUS, count(*) as PARENT_REC_COUNT
from 
(SELECT
        ib.id_item,
        ib.FLAG_STAT_ITEM AS PARENT_ITEM_STATUS,
        LISTAGG(id.descr_addl, '') WITHIN GROUP (ORDER BY SEQ_DESCR) AS "PARENT DESCRIPTION"
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze" id
           ON ib.id_item = id.id_item
    WHERE ib.code_comm = 'PAR'
      AND id.seq_descr BETWEEN 800 AND 810
    --   AND ib."is_deleted" = 0
    --   AND id."is_deleted" = 0
    GROUP BY ib.id_item, ib.FLAG_STAT_ITEM
)
GROUP BY PARENT_ITEM_STATUS
ORDER BY PARENT_REC_COUNT desc;

/* Parent Item Status distribution from Bronze data:
┌───────────────────────┬────────────────┐
│ PARENT_ITEM_STATUS    │PARENT_REC_COUNT│
├───────────────────────┼────────────────┤
│ O                     │           1346 │ 1355
│ A                     │             11 │ 10
└───────────────────────┴────────────────┘
*/




WITH sku_attributes AS (
    SELECT
        ib.id_item,
        MAX(CASE WHEN av.id_attr = 'ID_PARENT' THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) ID_PARENT"
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic" ib
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Dynamic" av
           ON ib.id_item = av.id_item
          AND ib.code_comm = av.code_comm
    WHERE ib.code_comm <> 'PAR'
      AND ib."is_deleted" = 0
      AND av."is_deleted" = 0
    GROUP BY ib.id_item
)
SELECT 
    COUNT(*) AS "SKUs_without_Parent"
FROM sku_attributes
WHERE "ATTR (SKU) ID_PARENT" IS NULL OR "ATTR (SKU) ID_PARENT" = '';
-- SKUs without_Parent in snowflake: 46,686
-- SKUs without_Parent in snowflake (Bronze): 70,442
-- SKUs without_Parent in TCM: 70,223

select count(*) from BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze"; -- 155501     -- 164
select count(*) from BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic__test"
WHERE "is_deleted" = 0;  -- 157918

select count(*) from BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze"; -- 1520324
select count(*) from BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Dynamic"
WHERE "is_deleted" = 0;  -- 1467389


select "Product ID/SKU", "Product Description","Product Name/Parent ID", "Child Item Status", "Parent Item Status"
from SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE
where "Product ID/SKU" ilike '160SG%';




-- ==================================================================
        SELECT
            TRIM(iv.id_item) as id_item,
            iv.flag_vnd_prim,
            TRIM(iv.id_vnd_payto) AS id_vnd_payto,
            TRIM(iv.id_vnd_ordfm) AS id_vnd_ordfm,
            TRIM(iv.id_item_vnd) AS id_item_vnd,
            COALESCE(
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'PRICE_QUOTE')::STRING,
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'QUOTE_PRICE')::STRING,
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'AMT_QUOTE')::STRING,
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'PRICE_VND')::STRING,
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'PRICE')::STRING
            ) AS quote_price,
            iv.date_quote,
            iv.date_expire_quote,
            iv.qty_mult_ord,
            iv.code_um_vnd,
            -- iv.ratio_stk_pur,
            -- iv.comment_user
        FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_VND_Bronze" iv
        where 

    -- ===============================================================
        SELECT
    iv.id_item,
    LISTAGG(vp.name_vnd, ', ') WITHIN GROUP (ORDER BY iv.id_vnd_payto) AS "Item_Secondary_Vendors_Names",
    LISTAGG(iv.id_vnd_payto, ', ') WITHIN GROUP (ORDER BY iv.id_vnd_payto) AS "Item_Secondary_Vendor_IDs",
    LISTAGG(iv.PRICE_VND_FC_1 , ', ') WITHIN GROUP (ORDER BY iv.id_vnd_payto) AS "Item_Secondary_Vendors_Quoted_Prices"
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_VND_Bronze" iv
LEFT JOIN BRONZE_DATA.TCM_BRONZE."VENMAS_PAYTO_Bronze" vp
    ON iv.id_vnd_payto = TRIM(vp.id_vnd)
WHERE iv.flag_vnd_prim = 'S'
GROUP BY iv.id_item