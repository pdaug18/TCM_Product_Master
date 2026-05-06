
select 
    LTRIM(ID_ITEM) as ID_ITEM,
    ID_LOC, 
    ID_LOC_HOME,
    TYPE_LOC,
    FLAG_SOURCE
from nsa.itmmas_loc
where id_item = '100003-06-XLT'
-- 'L05NLNL138-LGXL'
-- find an item that has multiple locations with various flag values

select id_item, id_loc
from nsa.itmmas_loc
where id_loc is null

with base as (
    select
        trim(id_item) as id_item,
        trim(id_loc) as id_loc,
        upper(trim(flag_source)) as flag_source
    from nsa.itmmas_loc
    where id_item is not null
      and id_loc is not null
),
item_loc as (
    select
        id_item,
        id_loc,
        max(case when flag_source = 'M' then 1 else 0 end) as has_m,
        max(case when flag_source = 'P' then 1 else 0 end) as has_p,
        count(*) as row_count   -- count of rows for this item-location combination; meaning number of records with different flag_source values
    from base
    group by id_item, id_loc
) select * from item_loc
ORDER BY row_count desc
,
ranked as (
    select
        id_item,
        id_loc,
        has_m,
        has_p,
        row_count,
        row_number() over (
            partition by id_item
            order by
                has_m desc,
                row_count desc,
                id_loc asc
        ) as rn
    from item_loc
)
select
    id_item as Item_ID,
    max(case when rn = 1 then id_loc end) as Item_Primary_Location,
    listagg(case when rn > 1 then id_loc end, ',')
        within group (order by rn) as Item_Secondary_Locations
from ranked
group by id_item;


select * 
from nsa.itmmas_loc
where id_item = 'L05NLNL138-LGXL'