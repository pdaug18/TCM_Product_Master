-- column list for CP_SHPHDR
select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CP_SHPHDR'
order by ordinal_position


select column_name, data_type, character_maximum_length
from information_schema.columns
where table_name = 'CP_SHPLIN'
order by ordinal_position






