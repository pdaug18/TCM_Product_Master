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
HAVING ib.id_item in ('913S-KC-7', '913S-KC-8', '913S-KC-9', '913S-KC-10', '913S-KC-11', '913S-KC-12',
                      '1005-7CCARL*8', '1005-7CCARL*9', '1007-5BCALR*10', '1007-5CC*9.5', '1007-6*9', '1007-6*10',
                      '1050-6CCAF*10', '1050-7*9', '1057-6CC*11.5', '1200-5BC*9');
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
