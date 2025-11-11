-- An issue has been spotted where some child products are marked as active despite their parent products being inactive/Obsolete in the Master Product Table. 
-- The status flag is marked as -> FLAG_STAT_ITEM = 'O' for Obsolete products and 'A' for Active products.
-- CODE_COMM = 'PAR' indicates Parent products.

select * from "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic"
WHERE CODE_COMM = 'PAR' limit 10;
select CODE_COMM, count(*) from "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic"
group by CODE_COMM
order by count(*) desc;

describe table "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic";

-- Spot and count the number of records where child products are active while their parent products are obsolete.
select 
    child.ID_ITEM AS CHILD_ID,
    child.FLAG_STAT_ITEM AS CHILD_STATUS,
    parent.ID_ITEM AS PARENT_ID,
    parent.FLAG_STAT_ITEM AS PARENT_STATUS
from "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic" child
join "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic" parent
    on child.PARENT_ID = parent.ID_ITEM
where child.FLAG_STAT_ITEM = 'A' and parent.FLAG_STAT_ITEM = 'O';