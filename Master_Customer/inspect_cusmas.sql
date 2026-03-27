select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CUSMAS_SHIPTO'
order by ordinal_position

select * from [nsa].[CUSMAS_SHIPTO];

select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CUSMAS_SOLDTO'
order by ordinal_position

select * from [nsa].[CUSMAS_SOLDTO];


select top 10 * from [nsa].[CUST_GROUP_CODE]