select 
    extract(year from lh.date_invc) AS "INVOICE YEAR",
    sum(lh.price_net) AS "Total Price",
    count(distinct id_invc) AS "Total Invoices"
from "BRONZE_DATA"."TCM_BRONZE"."CP_INVLIN_HIST_Bronze" lh
group by extract(year from lh.date_invc);

select * from (
select 
    'View 4' as VIEW,
    extract(year from "INVOICE DATE") AS "INVOICE YEAR",
    count(distinct "InvoiceID") AS "Total Invoices",
    sum("Total Price") AS "Total Price"
from BRONZE_DATA.TCM_BRONZE.INVOICE_HIST_VIEW_4
group by extract(year from "INVOICE DATE")

UNION ALL

select 
    'View 2' as VIEW,
    extract(year from "INVOICE DATE") AS "INVOICE YEAR",
    count(distinct "InvoiceID") AS "Total Invoices",
    sum("Total Price") AS "Total Price"
from BRONZE_DATA.TCM_BRONZE.INVOICE_HIST_VIEW_2
group by extract(year from "INVOICE DATE")
)
order by "INVOICE YEAR"; 

/* 2024 YEAR 
Total Invoices | Total Price
---------------------------
View 2 | 71968 | 163,493,223.5
View 4 | 71968 | 163,491,495.5
===========================
               | $ 1,728.00 
*/

-- QA "InvoiceID" = '1009487' to compare values between views
select * from  (
select 
    'View 4' as VIEW,
    "InvoiceID",
    sum("Total Price") AS "Total Price"
from BRONZE_DATA.TCM_BRONZE.INVOICE_HIST_VIEW_4
where "InvoiceID" = '1009487'
group by "InvoiceID"
UNION ALL
select 
    'View 2' as VIEW,
    "InvoiceID",
    sum("Total Price") AS "Total Price"
from BRONZE_DATA.TCM_BRONZE.INVOICE_HIST_VIEW_2
where "InvoiceID" = '1009487'
group by "InvoiceID"
)
order by "Total Price";

-- QA total numner of SKUs in each view
select 
    'View 4' as VIEW,
    count(distinct "Product ID/SKU") AS "Total SKUs"
from BRONZE_DATA.TCM_BRONZE.INVOICE_HIST_VIEW_4
UNION ALL
select 
    'View 2' as VIEW,
    count(distinct "Product ID/SKU") AS "Total SKUs"
from BRONZE_DATA.TCM_BRONZE.INVOICE_HIST_VIEW_2;