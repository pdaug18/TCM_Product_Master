    SELECT
        ib.id_item,
        LISTAGG(id.descr_addl, '') 
        WITHIN GROUP (ORDER BY SEQ_DESCR) AS "PARENT DESCRIPTION"
    FROM "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic" ib
    LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_DESCR_Bronze" id
        ON ib.id_item = id.id_item
    WHERE ib.code_comm = 'PAR'
        AND id.seq_descr BETWEEN 800 AND 810
        AND ib."is_deleted" = 0
    GROUP BY ib.id_item
    limit 1000;
    -- HAVING ib.id_item = 'VNT99345';

    -- DF2-AX3-324LS-HY

select ID_ITEM, DESCR_ADDL, SEQ_DESCR 
-- id_item, LISTAGG(descr_addl, ', ')  
from "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_DESCR_Bronze" 
-- group by id_item
-- having 
where id_item = 'VNT99345'; 
-- limit 1000;


-- example: VNT99345