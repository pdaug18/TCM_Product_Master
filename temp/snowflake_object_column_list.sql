-- get the me list of all columns in item_inventory_master
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'MASTER_PRODUCT_TABLE'  
ORDER BY ordinal_position;


-- get me the list of columns in MASTER_PRODUCT_TABLE in silver database
SELECT
    column_name,
    data_type
FROM SILVER_DATA.INFORMATION_SCHEMA.COLUMNS
WHERE table_name   = 'MASTER_PRODUCT_TABLE'
  AND table_schema = 'TCM_SILVER'
ORDER BY ordinal_position;