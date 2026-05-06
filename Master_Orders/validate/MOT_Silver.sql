-- SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE_SILVER

select distinct "Order_ID", count(*) as "Order_Count"
from SILVER_DATA.TCM_SILVER."MASTER_ORDERS_TABLE_SILVER"
where "Record_Source_Lin" = 'ACTIVE' and "Record_Source_Hdr" = 'ACTIVE'
group by "Order_ID"
HAVING "Order_Count" > 1
order by "Order_Count" desc

select count(*)
from SILVER_DATA.TCM_SILVER."MASTER_ORDERS_TABLE_SILVER"
where "Order_ID" = '862188'     -- 1692
and "Record_Source_Lin" = 'ACTIVE' and "Record_Source_Hdr" = 'ACTIVE'   -- 423

select * 
from BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_Bronze"
where ID_ORD = '843520'

select *
from BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_Bronze"
where ID_ORD = '840574' --423

843520

--"Order_ID" = 840574 AND "Record_Source_Lin" = 'ACTIVE'

select *
from BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_Bronze"
where ID_ORD = '843520' --423