-- column list for CP_SHPHDR
select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CP_SHPHDR'
order by ordinal_position


select top 10 * from [nsa].[CP_SHPHDR]
where ID_ORD = '571555'

select top 10 * from [nsa].[BOKHST_LINE]
where ID_ORD = '571555'

select top 10 * from [nsa].[CP_ORDLIN]
where ID_ORD = '571555'

select top 10 * from [nsa].[CP_SHPLIN]
where ID_ORD = '571555'

select top 10 * from [nsa].[CP_INVLIN_HIST]
where ID_ORD = '571555'
















select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CP_SHPLIN'
order by ordinal_position


select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'SHPORD_HDR'
order by ordinal_position

select top 10 * from [nsa].[SHPORD_HDR]

select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'SHPORD_LIN'
order by ordinal_position

select top 10 * from [nsa].[SHPORD_LIN]

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


