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
HAVING "ATTR (SKU) ID_PARENT" = '' AND ib.code_comm != 'FG'
AND ib.id_item in ('1200-5BC*9', '1005-7CCARL*9', 'TSC-YARN-0005', '1050-7*9', 'IN CZ60', 'I ZFS2119895', '4005-26', 'I ZLEXXONBK', '3024-07', '4005-25', '1007-6*9', '1050-6CCAF*10', '1007-5CC*9.5','3024-01',
'1007-6*10', '3019-07', '3024-02', 'I ZZEROINC', '1005-7CCARL*8', '3019-02', '3024-13', '3030-73', 'I ZLLINXONBK', '2525-02', 'I ZLNMGC', '2031-06', '1700-64', '3080-01', 'I ZLHAZMAT9X2',
'4005-41', '3094-07', '3065-01', '3030-01', '3019-13', 'I ZLCOMED8X2', '3019-01', '3019-16', 'I ZLCOMED4X1', '3030-55', 'I ZLMORGAN', '3030-04', '3136-01', '2006-19', 'I UEKCLARKW',
'3035-16', 'I ZLLINXONWH', '4005-65', '3136-05', '4005-73', '3000-09', 'I ZLINTEGRITY', '4005-13', '3019-75', '2006-08', '3019-06', 'I ZLPSARROW', 'I ZLCATERP', '3750-01', '3002-65',
'3030-14', '3002-67', '3000-12', '4008-84', '3080-56', 'I ZLTCLP', '3136-03', '3019-73', '3081-14', '1171-13', '3081-56', '1057-6CC*11.5', 'I ZLRICHARDS', '2006-14', '1171-07S', '82',
'I ZLFIELD4X1', 'I ZPGMC3X3', 'I ZLATCO', '4005-57', 'I ZTNGRY40', 'SHR2NGE14XLRG', '3042-B1', '2052-14', '4005-10', '3003-64', '2036-07');
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