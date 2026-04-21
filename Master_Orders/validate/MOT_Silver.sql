select * 
from SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE_SILVER
limit 10;

select distinct "Order_ID", "Order_Line_Sequence_#", count(*) cnt
from SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE_SILVER
where  "Record_Source_Hdr" = 'ACTIVE' 
-- and "Record_Source_Lin" = 'ACTIVE'
group by "Order_ID", "Order_Line_Sequence_#"
having count(*) > 1
order by cnt desc;

select distinct "Record_Source_Hdr", "Record_Source_Lin", count(*) cnt
from SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE_SILVER
group by "Record_Source_Hdr", "Record_Source_Lin"
order by cnt desc;
/*
SRC_Hdr	SRC_Lin	CNT
PERM	PERM	1247251
ACTIVE	PERM	15398
PERM	ACTIVE	7787
ACTIVE	ACTIVE	7779
*/

select count(distinct TRIM(ID_ORD)) -- 2653
from BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_Bronze";

select count(distinct TRIM(ID_ORD), TRIM(SEQ_LINE_ORD)) --7779
from BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_Bronze";

select * 
from BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_Bronze"
where ID_ORD = '856049' and SEQ_LINE_ORD = '5';