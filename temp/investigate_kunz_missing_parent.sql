-- Kunz parent SKUs missing

SELECT
    ib.id_item,
    ib.code_comm,
    MAX(CASE WHEN av.id_attr = 'ID_PARENT'   THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) ID_PARENT",
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
LEFT JOIN BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze" av
        ON ib.id_item = av.id_item
        AND ib.code_comm = av.code_comm
WHERE ib.code_comm <> 'PAR'
GROUP BY ib.id_item, ib.code_comm
HAVING "ATTR (SKU) ID_PARENT" = '' AND ib.code_comm != 'FG';
--  HAVING ib.id_item in ('913S-KC-7', '913S-KC-8', '913S-KC-9', '913S-KC-10', '913S-KC-11', '913S-KC-12',
--                        '1005-7CCARL*8', '1005-7CCARL*9', '1007-5BCALR*10', '1007-5CC*9.5', '1007-6*9', '1007-6*10',
--                       '1050-6CCAF*10', '1050-7*9', '1057-6CC*11.5', '1200-5BC*9');
/*
┌─────────────┬────────────────────┐
│ ID_ITEM     │ATTR (SKU) ID_PARENT│
├─────────────┼────────────────────┤
│ 913S-KC-12  │ 913S-KC            │
│1005-7CCARL*8│                    │
└─────────────┴────────────────────┘
*/


select ID_ITEM,
       CODE_COMM,
       DESCR_1,
       DESCR_2
from BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze"
where id_item in ('913S-KC-7', '913S-KC-8', '913S-KC-9', '913S-KC-10', '913S-KC-11', '913S-KC-12',
                      '1005-7CCARL*8', '1005-7CCARL*9', '1007-5BCALR*10', '1007-5CC*9.5', '1007-6*9', '1007-6*10',
                      '1050-6CCAF*10', '1050-7*9', '1057-6CC*11.5', '1200-5BC*9');
/*
┌───────────────┬─────────────────────────────────┐
│ ID_ITEM       │ DESCR_1                         │
├───────────────┼─────────────────────────────────┤
│ 1005-7CCARL*8 │ KUNZ HV PROTECTOR, CR GOAT      │
│ 913S-KC-12    │ KUNZ LV PROTECTOR, PRL GOAT     │
└───────────────┴─────────────────────────────────┘
*/

select  ID_ITEM,
        CODE_COMM,
        ID_ATTR,
        VAL_STRING_ATTR
from BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze"
where ID_ATTR = 'ID_PARENT' 
AND (id_item = '1005-7CCARL*8'
OR  id_item = '913S-KC-12');
/*
┌───────────────┬───────────┬───────────────────┐
│ ID_ITEM       │ ID_ATTR   │ VAL_STRING_ATTR   │
├───────────────┼───────────┼───────────────────┤
│ 1005-7CCARL*8 │ ID_PARENT │ 1005-7CCARL       │
│ 913S-KC-12    │ ID_PARENT │ 913S-KC           │
└───────────────┴───────────┴───────────────────┘
*/

-- Enhanced query to investigate missing ID_PARENT attrs
-- Shows items with empty ATTR (SKU) ID_PARENT, their base code_comm, and any existing ID_PARENT attrs with their code_comm

SELECT
    ib.id_item,
    ib.code_comm AS base_code_comm,
    concat(ib.descr_1, ' || ', ib.descr_2) AS item_descr,
    av.id_attr AS attr_id,
    av.code_comm AS attr_code_comm,
    av.val_string_attr as "ID_PARENT"
FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
LEFT JOIN BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze" av ON ib.id_item = av.id_item AND ib.code_comm = av.code_comm AND av.id_attr = 'ID_PARENT'
WHERE ib.code_comm <> 'PAR'
  AND ib.code_comm != 'FG'
  AND av.id_attr like '%PARENT%'
  AND NOT EXISTS ( 
      SELECT 1
      FROM BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze" av_inner
      WHERE av_inner.id_item = ib.id_item
        AND av_inner.code_comm = ib.code_comm
        AND av_inner.id_attr = 'ID_PARENT'
        AND av_inner.val_string_attr IS NOT NULL
        AND av_inner.val_string_attr <> ''
  )
ORDER BY ib.id_item, av.code_comm;