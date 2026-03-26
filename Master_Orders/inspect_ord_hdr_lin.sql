

select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CP_ORDHDR'
order by ordinal_position

select top 10 * from [nsa].[CP_ORDHDR]

select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CP_ORDLIN'
order by ordinal_position

select top 10 * from [nsa].[CP_ORDLIN]

select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'BOKHST_HDR'
order by ordinal_position

select top 10 * from [nsa].[BOKHST_HDR]

select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'BOKHST_LINE'
order by ordinal_position

select top 10 * from [nsa].[BOKHST_LINE]