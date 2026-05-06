-- GOLD_DATA	TCM_GOLD	OPEN_ORDERS_GOLD
-- Item_Brand

select distinct "Item_Brand"
from GOLD_DATA.TCM_GOLD.OPEN_ORDERS_GOLD
where "Item_Brand" is not null


-- SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE_SILVER
--Item_Brand
select count(*)
from SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE_SILVER
where "Item_Brand" = 'BASHLIN'


select * 
from SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE_SILVER
where "Item_Brand" = 'BASHLIN'
and "Item_Commodity_Code" ='FG'
and "Item_ID_Parent_SKU" is not null