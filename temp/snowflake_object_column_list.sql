-- get the me list of all columns in item_inventory_master
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'ITEM_INVENTORY_MASTER'  
ORDER BY ordinal_position;