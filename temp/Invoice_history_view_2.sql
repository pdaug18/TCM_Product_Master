create or replace view BRONZE_DATA.TCM_BRONZE.INVOICE_HIST_VIEW_2(
	"OrderID",
	"LineNumber",
	"Ordered Date",
	"Stock Flag",
	"InvoiceID",
	"Freight_Cost",
	"Sales_Tax",
	"Customer Name",
	"Customer ID",
	"Customer Type",
	"CUST/GROUP CODE",
	"CUST/GROUP NAME",
	"Sold to Address",
	"Sold to City",
	"Sold to State",
	"Sold to Zip Code",
	"Sold to Country",
	"Customer Attribute Flag",
	"Ship to Address",
	"Ship to City",
	"Ship to State",
	"Ship to Zip Code",
	"Ship to Customer Name",
	"Ship to #",
	"Ship to Country",
	"TCM Sales Rep ID",
	"Product ID/SKU",
	"Product Description",
	"COST CATEGORY ID",
	"COST CAT DESCR",
	"PRODUCT CATEGORY/VERTICAL",
	"PRDT CAT DESCR",
	"VERTICAL (Calc)",
	"CATEGORY (Calc)",
	"Product Name/Parent ID",
	"PARENT DESCRIPTION",
	"ATTR (SKU) CERT_NUM",
	"ATTR (SKU) COLOR",
	"ATTR (SKU) SIZE",
	"ATTR (SKU) LENGTH",
	"ATTR (SKU) TARIFF_CODE",
	"ATTR (SKU) UPC_CODE",
	"ATTR (SKU) PFAS",
	"ATTR (PAR) BERRY",
	"ATTR (PAR) CARE",
	"ATTR (PAR) HEAT TRANSFER",
	"ATTR (PAR) OTHER",
	"ATTR (PAR) PAD PRINT",
	"ATTR (PAR) PRODUCT CAT",
	"ATTR (PAR) PRODUCT TYPE",
	"ATTR (PAR) TRACKING",
	"ATTR (PAR) Z_BRAND",
	"ATTR (PAR) Z_CATEGORY",
	"ATTR (PAR) Z_GENDER",
	"ATTR (PAR) Z_VERTICAL",
	"PROP 65",
	"INVOICE DATE",
	ALT_KEY,
	"Reorder Level",
	SEQ_SHIPTO,
	"Promise Date",
	"Quantity Shipped",
	"Total Price",
	"Unit Cost",
	"Sales Rep ID",
	"Sales Rep Name",
	"Sales transaction line ID",
	ID_LOC,
	"LOC DESCR",
	"CALENDAR DATE",
	"Booking Type Table"
) as
SELECT
    lh.id_ord AS "OrderID",
    lh.seq_line_ord as "LineNumber",
    lh.date_book_last as "Ordered Date",
    lh.FLAG_STK as"Stock Flag",
    lh.ID_INVC as "InvoiceID",
    hh.amt_frt as "Freight_Cost",
    hh.tax_sls as "Sales_Tax",
    st.name_cust AS "Customer Name",
    st.id_cust AS "Customer ID",
    st.code_cust AS "Customer Type",
    COALESCE(gc.group_code, st.id_cust) AS "CUST/GROUP CODE",
    COALESCE(gc.group_name, st.name_cust) AS "CUST/GROUP NAME",
    st.ADDR_CUST_2 AS "Sold to Address",
    st.city AS "Sold to City",
    st.id_st AS "Sold to State",
    st.ZIP AS "Sold to Zip Code",
    st.country AS "Sold to Country",
    st.code_user_2_ar as "Customer Attribute Flag",
    hh.addr_2 AS "Ship to Address",
    hh.city AS "Ship to City",
    hh.id_st AS "Ship to State",
    hh.zip AS "Ship to Zip Code",
    hh.name_cust_shipto AS "Ship to Customer Name",
    hh.seq_shipto AS "Ship to #",
    hh.country AS "Ship to Country",
    hh.id_slsrep_1 as "TCM Sales Rep ID",
    lh.id_item AS "Product ID/SKU",
    ib.DESCR_1 || ' ' || ib.DESCR_2 AS "Product Description",
    ib.code_cat_cost AS "COST CATEGORY ID",
    COALESCE(ib.code_cat_cost || ' - ' || cc.descr, 'INVALID COST CATEGORY') AS "COST CAT DESCR",
    ib.code_cat_prdt AS "PRODUCT CATEGORY/VERTICAL",
    COALESCE(ib.code_cat_prdt || ' - ' || pc.descr, 'INVALID PRODUCT CATEGORY') AS "PRDT CAT DESCR",
    av2.Vertical AS "VERTICAL (Calc)",
    av3.Z_CATEGORY AS "CATEGORY (Calc)",
    av."ATTR (SKU) ID_PARENT" AS "Product Name/Parent ID",
    COALESCE(id."PARENT DESCRIPTION", 'MISSING DESCRIPTION - UPDATE TCM') AS "PARENT DESCRIPTION",
    av."ATTR (SKU) CERT_NUM",
    av."ATTR (SKU) COLOR",
    av."ATTR (SKU) SIZE",
    av."ATTR (SKU) LENGTH",
    av."ATTR (SKU) TARIFF_CODE",
    av."ATTR (SKU) UPC_CODE",
    av."ATTR (SKU) PFAS",
    attr."ATTR (PAR) BERRY",
    attr."ATTR (PAR) CARE",
    attr."ATTR (PAR) HEAT TRANSFER",
    attr."ATTR (PAR) OTHER",
    attr."ATTR (PAR) PAD PRINT",
    attr."ATTR (PAR) PRODUCT CAT",
    attr."ATTR (PAR) PRODUCT TYPE",
    attr."ATTR (PAR) TRACKING",
    attr."ATTR (PAR) Z_BRAND",
    attr."ATTR (PAR) Z_CATEGORY",
    attr."ATTR (PAR) Z_GENDER",
    attr."ATTR (PAR) Z_VERTICAL",
    prop.prop_65 AS "PROP 65",
    lh.date_invc AS "INVOICE DATE",
    ib.key_alt AS "ALT_KEY",
    r.level_rop AS "Reorder Level",
    hh.seq_shipto as "SEQ_SHIPTO",
    lh.date_prom AS "Promise Date",
    lh.qty_ship AS "Quantity Shipped",
    lh.price_net AS "Total Price",
    CAST(RIGHT(lh.cost_unit_vp,10) AS DECIMAL)/10000 AS "Unit Cost",
    hh.id_slsrep_1 AS "Sales Rep ID",
    sr.name_slsrep AS "Sales Rep Name",
    lh.seq_line_ord AS "Sales transaction line ID",
    lh.id_loc AS "ID_LOC",
    lh.id_loc || ' - ' || tl.DESCR AS "LOC DESCR",
    lh.date_invc AS "CALENDAR DATE",
    'Shipped' as "Booking Type Table"
from
"BRONZE_DATA"."TCM_BRONZE"."CP_INVLIN_HIST_Bronze" lh
LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Bronze" ib ON lh.id_item = ib.id_item
left join "BRONZE_DATA"."TCM_BRONZE"."TABLES_LOC_Bronze" tl on lh.id_loc = tl.ID_LOC
left join "BRONZE_DATA"."TCM_BRONZE"."TABLES_CODE_CAT_COST_Bronze" cc on ib.code_cat_cost = cc.code_cat_cost
left join "BRONZE_DATA"."TCM_BRONZE"."TABLES_CODE_CAT_PRDT_Bronze" pc on ib.code_cat_prdt = pc.code_cat_prdt and pc.code_type_cust is null
left join "BRONZE_DATA"."TCM_BRONZE"."CP_INVHDR_HIST_Bronze" hh on lh.id_invc = hh.id_invc
left join "BRONZE_DATA"."TCM_BRONZE"."TABLES_SLSREP_Bronze" sr on ltrim(hh.id_slsrep_1) = ltrim(sr.id_slsrep)
left join "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_REORD_Bronze" r on lh.id_item = r.id_item and lh.id_loc = r.id_loc_home
left join
(    
    select
        av.id_item,
        max(case when id_attr = 'CERT_NUM' then val_string_attr else '' end) as "ATTR (SKU) CERT_NUM",
        max(case when id_attr = 'COLOR' then val_string_attr else '' end) as "ATTR (SKU) COLOR",
        max(case when id_attr = 'ID_PARENT' then val_string_attr else '' end) as "ATTR (SKU) ID_PARENT",
        max(case when id_attr = 'LENGTH' then val_string_attr else '' end) as "ATTR (SKU) LENGTH",
        max(case when id_attr = 'SIZE' then val_string_attr else '' end) as "ATTR (SKU) SIZE",
        max(case when id_attr = 'TARIFF_CODE' then val_string_attr else '' end) as "ATTR (SKU) TARIFF_CODE",
        max(case when id_attr = 'UPC_CODE' then val_string_attr else '' end) as "ATTR (SKU) UPC_CODE",
        max(case when id_attr = 'PFAS' then val_string_attr else '' end) as "ATTR (SKU) PFAS"
    from
    (
        select
            av.id_item,
            id_attr,
            VAL_STRING_ATTR
        from "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Bronze" ib
        left join "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Bronze" av
        on ib.id_item = av.id_item and ib.code_comm = av.code_comm
        where ib.code_comm <> 'PAR'
    ) av
    group by av.id_item
) av on lh.id_item = av.id_item
left join
(
    select
        id_item as ID_PARENT,
        max("ATTR (PAR) BERRY") as "ATTR (PAR) BERRY",
        max("ATTR (PAR) CARE") as "ATTR (PAR) CARE",
        max("ATTR (PAR) HEAT TRANSFER") as "ATTR (PAR) HEAT TRANSFER",
        max("ATTR (PAR) OTHER") as "ATTR (PAR) OTHER",
        max("ATTR (PAR) PAD PRINT") as "ATTR (PAR) PAD PRINT",
        max("ATTR (PAR) PRODUCT CAT") as "ATTR (PAR) PRODUCT CAT",
        max("ATTR (PAR) PRODUCT TYPE") as "ATTR (PAR) PRODUCT TYPE",
        max("ATTR (PAR) TRACKING") as "ATTR (PAR) TRACKING",
        max("ATTR (PAR) Z_BRAND") as "ATTR (PAR) Z_BRAND",
        max("ATTR (PAR) Z_CATEGORY") as "ATTR (PAR) Z_CATEGORY",
        max("ATTR (PAR) Z_GENDER") as "ATTR (PAR) Z_GENDER",
        max("ATTR (PAR) Z_VERTICAL") as "ATTR (PAR) Z_VERTICAL"
    from(
        select
            id_item,
            case when id_attr = 'BERRY' then val_string_attr else '' end as "ATTR (PAR) BERRY",
            case when id_attr = 'CARE' then val_string_attr else '' end as "ATTR (PAR) CARE",
            case when id_attr = 'HEAT TRANSFER' then val_string_attr else '' end as "ATTR (PAR) HEAT TRANSFER",
            case when id_attr = 'OTHER' then val_string_attr else '' end as "ATTR (PAR) OTHER",
            case when id_attr = 'PAD PRINT' then val_string_attr else '' end as "ATTR (PAR) PAD PRINT",
            case when id_attr = 'PRODUCT CAT' then val_string_attr else '' end as "ATTR (PAR) PRODUCT CAT",
            case when id_attr = 'PRODUCT TYPE' then val_string_attr else '' end as "ATTR (PAR) PRODUCT TYPE",
            case when id_attr = 'TRACKING' then val_string_attr else '' end as "ATTR (PAR) TRACKING",
            case when id_attr = 'Z_BRAND' then val_string_attr else '' end as "ATTR (PAR) Z_BRAND",
            case when id_attr = 'Z_CATEGORY' then val_string_attr else '' end as "ATTR (PAR) Z_CATEGORY",
            case when id_attr = 'Z_GENDER' then val_string_attr else '' end as "ATTR (PAR) Z_GENDER",
            case when id_attr = 'Z_VERTICAL' then val_string_attr else '' end as "ATTR (PAR) Z_VERTICAL"
        from "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Bronze"
        where
            code_comm = 'PAR'
    ) av
    group by id_item
) attr on av."ATTR (SKU) ID_PARENT" = attr.ID_PARENT
left join
(
    SELECT
        ib.id_item,
        LISTAGG(id.descr_addl, ', ') AS "PARENT DESCRIPTION"
    FROM "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Bronze" ib
    LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_DESCR_Bronze" id ON ib.id_item = id.id_item
    WHERE ib.code_comm = 'PAR' AND id.seq_descr BETWEEN 800 AND 810
    GROUP BY ib.id_item
) id on av."ATTR (SKU) ID_PARENT" = id.ID_ITEM
left join
(
    select
        ib2.id_item,
        MAX(case
        when av2.Z_VERTICAL = 'AF' and av2.Z_CATEGORY in ('CL','KT') then 'CLOTHING & KITS'
        when av2.Z_VERTICAL = 'AF' and av2.Z_CATEGORY in ('ES') then 'ELECTRICAL SAFETY'
        when av2.Z_VERTICAL = 'AF' and av2.Z_CATEGORY in ('FSB') then 'FACESHIELDS & BALACLAVAS'
        when av2.Z_VERTICAL = 'AF' and av2.Z_CATEGORY in ('LP') then 'KUNZ LEATHER PROTECTORS'
        when av2.Z_VERTICAL = 'AF' and av2.Z_CATEGORY in ('VG') then 'VOLTAGE RATED GLOVES'
        when av2.Z_VERTICAL = 'AF' and av2.Z_CATEGORY in ('WG') then 'KUNZ WORK GLOVES'
        when av2.Z_VERTICAL = 'FR' and av2.Z_CATEGORY in ('AC') then 'FR ACCESSORIES'
        when av2.Z_VERTICAL = 'FR' and av2.Z_CATEGORY in ('HV') then 'FR HI-VIS'
        when av2.Z_VERTICAL = 'FR' and av2.Z_CATEGORY in ('IDC') then 'FR INFECTIOUS DISEASE CONTROL'
        when av2.Z_VERTICAL = 'FR' and av2.Z_CATEGORY in ('IND') then 'INDUSTRIAL FR UNIFORMS'
        when av2.Z_VERTICAL = 'FR' and av2.Z_CATEGORY in ('MSC') then 'MISC'
        when av2.Z_VERTICAL = 'FR' and av2.Z_CATEGORY in ('FABRC') then 'FR FABRIC'
        when av2.Z_VERTICAL = 'FR' and av2.Z_CATEGORY in ('RW') then 'FR RAINWEAR'
        when av2.Z_VERTICAL = 'FR' and av2.Z_CATEGORY in ('WW') then 'FR WORK WEAR'
        when av2.Z_VERTICAL = 'GV' and av2.Z_CATEGORY in ('FRML') then 'FR MILITARY'
        when av2.Z_VERTICAL = 'GV' and av2.Z_CATEGORY in ('MISC') then 'GOVERNMENT (NON-MILITARY)'
        when av2.Z_VERTICAL = 'GV' and av2.Z_CATEGORY in ('LE') then 'LAW ENFORCEMENT'
        when av2.Z_VERTICAL = 'GV' and av2.Z_CATEGORY in ('FABRC') then 'FR MILITARY FABRIC'
        when av2.Z_VERTICAL = 'GV' and av2.Z_CATEGORY in ('WT') then 'WILD THINGS'
        when av2.Z_VERTICAL IN ('CT','IS') and av2.Z_CATEGORY in ('CR') then 'CRYOGENIC PPE'
        when av2.Z_VERTICAL IN ('CT','IS') and av2.Z_CATEGORY in ('IDC') then 'INFECTIOUS DISEASE CONTROL'
        when av2.Z_VERTICAL IN ('CT','IS') and av2.Z_CATEGORY in ('CP','HV','MISC') then 'MISC'
        when av2.Z_VERTICAL IN ('CT','IS') and av2.Z_CATEGORY in ('MC') then 'MECHANICAL/CUT PROTECTION'
        when av2.Z_VERTICAL IN ('TH') and av2.Z_CATEGORY in ('CL') then 'CLOTHING'
        when av2.Z_VERTICAL IN ('TH') and av2.Z_CATEGORY in ('FSB') then 'FACESHIELDS & BALACLAVAS'
        when av2.Z_VERTICAL IN ('TH') and av2.Z_CATEGORY in ('HP') then 'HAND PROTECTION'
        when av2.Z_VERTICAL IN ('TH') and av2.Z_CATEGORY in ('MAC') then 'MACHINERY PROTECTION'
        when av2.Z_VERTICAL IN ('TH') and av2.Z_CATEGORY in ('FABRC') then 'THERMAL FABRIC'
        when av2.Z_VERTICAL IN ('TH') and av2.Z_CATEGORY in ('MSC') then 'MISC/THERMAL PROTECTION'
        else '#NOT CATEGORIZED' end) as "Z_CATEGORY"
    from "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Bronze" ib2
    left join "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Bronze" av on ib2.id_item = av.id_item and ib2.code_comm = av.code_comm
    left join
    (
        select
            id_item,
            MAX(case when id_attr = 'Z_VERTICAL' then val_string_attr else '' end) as "Z_VERTICAL",
            MAX(case when id_attr = 'Z_CATEGORY' then val_string_attr else '' end) as "Z_CATEGORY"
        from "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Bronze"
        where code_comm = 'PAR' and (id_attr in ('Z_VERTICAL', 'Z_CATEGORY'))
        GROUP BY ID_ITEM
    ) av2 on av.val_string_attr = av2.id_item
    GROUP BY IB2.ID_ITEM
) av3 on ib.id_item = av3.ID_ITEM
left join
(
    select
        ib3.id_item,
        CASE
        WHEN MAX(av2.Z_VERTICAL) = 'AF' THEN 'ARC FLASH PPE'
        WHEN MAX(av2.Z_VERTICAL) = 'CT' OR MAX(av2.Z_VERTICAL) = 'IS' OR COALESCE(MAX(av2.Z_VERTICAL), '') = '' THEN 'INDUSTRIAL PPE'
        WHEN MAX(av2.Z_VERTICAL) = 'FR' THEN 'FR CLOTHING'
        WHEN MAX(av2.Z_VERTICAL) = 'GV' THEN 'MILITARY'
        WHEN MAX(av2.Z_VERTICAL) = 'TH' THEN 'THERMAL'
        WHEN MAX(av2.Z_VERTICAL) = 'UL' AND MAX(av2.Z_CATEGORY) = 'AD' THEN 'AD SPECIALTY'
        WHEN MAX(av2.Z_VERTICAL) = 'UL' AND MAX(av2.Z_CATEGORY) = 'USPS' THEN 'USPS'
        WHEN COALESCE(MAX(av2.Z_VERTICAL), '') = '' THEN 'INDUSTRIAL PPE'
        ELSE 'INDUSTRIAL PPE'
        END AS VERTICAL
    from "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Bronze" ib3
left join "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Bronze" av on ib3.id_item = av.id_item and ib3.code_comm = av.code_comm
left join
(
    select
        id_item
        , case when id_attr = 'Z_VERTICAL' then val_string_attr else '' end as Z_VERTICAL
        , case when id_attr = 'Z_CATEGORY' then val_string_attr else '' end as Z_CATEGORY
    from "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Bronze"
    where code_comm = 'PAR' and (id_attr in ('Z_VERTICAL', 'Z_CATEGORY'))
) av2 on av.val_string_attr = av2.id_item
group by ib3.id_item
) av2 on lh.id_item = av2.id_item
left join "BRONZE_DATA"."TCM_BRONZE"."CUSMAS_SOLDTO_Bronze" st on ltrim(lh.id_cust_soldto) = ltrim(st.id_cust)
LEFT JOIN (
    SELECT 
        p.id_item_par,
        CASE 
            WHEN EXISTS (
                SELECT 1
                FROM (
                    SELECT p2.*
                    FROM "BRONZE_DATA"."TCM_BRONZE"."PRDSTR_Bronze" p2
                    INNER JOIN (
                        SELECT id_item_comp, MAX(date_eff_end) AS max_eff_end
                        FROM "BRONZE_DATA"."TCM_BRONZE"."PRDSTR_Bronze"
                        GROUP BY id_item_comp
                    ) latest ON p2.id_item_comp = latest.id_item_comp AND p2.date_eff_end = latest.max_eff_end
                ) latest_comp
                JOIN "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_DESCR_Bronze" d  ON latest_comp.id_item_comp = d.id_item
                WHERE latest_comp.id_item_par = p.id_item_par AND d.descr_addl LIKE '%PROP 65%'
            ) THEN 'Y'
            ELSE 'N'
        END AS prop_65
    FROM "BRONZE_DATA"."TCM_BRONZE"."PRDSTR_Bronze" p
    GROUP BY p.id_item_par
) prop ON av.id_item = prop.id_item_par
left join
"BRONZE_DATA"."TCM_BRONZE"."CUST_GROUP_CODE_Bronze" gc
on st.code_user_3_ar = gc.group_code
where
lh.date_invc > '1/1/2014'
and st.CODE_CUST <>'IC';