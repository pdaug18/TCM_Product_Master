

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
where table_name = 'CP_ORDHDR_PERM'
order by ordinal_position

select * from [nsa].[CP_ORDHDR_PERM]

select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CP_ORDLIN_PERM'
order by ordinal_position

select * from [nsa].[CP_ORDLIN_PERM];



select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CP_ORDHDR_CUSTOM_COMMENTS'
order by ordinal_position

select top 10 * from [nsa].[CP_ORDHDR_CUSTOM_COMMENTS]


select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CP_BILL_LADING_HIST'
order by ordinal_position

select top 10 * from [nsa].[CP_BILL_LADING_HIST_Bronze]
