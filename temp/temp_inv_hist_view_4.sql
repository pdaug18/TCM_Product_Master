create or replace view BRONZE_DATA.TCM_BRONZE.INVOICE_HIST_VIEW_4(
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
-- ========================================
-- ITMMAS_BASE CTE - Base item master data
-- ========================================
WITH ITMMAS_BASE AS (
    SELECT 
        ib.id_item,
        ib.key_alt AS "ALT_KEY",
        ib.code_cat_prdt AS "NSA_PRODUCT CATEGORY/VERTICAL",
        ib.code_cat_cost AS "COST CATEGORY",
        ib.DESCR_1 || ' ' || ib.DESCR_2 AS "Product Description",
        ib.code_comm
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic__test" ib
    WHERE ib."is_deleted" = 0
)

-- ========================================
-- SKU ATTRIBUTES CTE - Extract SKU-level attributes
-- ========================================
,sku_attributes AS (
    SELECT
        av.id_item,
        MAX(CASE WHEN id_attr = 'ID_PARENT' THEN val_string_attr ELSE '' END) AS "ATTR (SKU) ID_PARENT",
        MAX(CASE WHEN id_attr = 'SIZE' THEN val_string_attr ELSE '' END) AS "ATTR (SKU) SIZE",
        MAX(CASE WHEN id_attr = 'COLOR' THEN val_string_attr ELSE '' END) AS "ATTR (SKU) COLOR",
        MAX(CASE WHEN id_attr = 'LENGTH' THEN val_string_attr ELSE '' END) AS "ATTR (SKU) LENGTH",
        MAX(CASE WHEN id_attr = 'UPC_CODE' THEN val_string_attr ELSE '' END) AS "ATTR (SKU) UPC_CODE",
        MAX(CASE WHEN id_attr = 'CERT_NUM' THEN val_string_attr ELSE '' END) AS "ATTR (SKU) CERT_NUM",
        MAX(CASE WHEN id_attr = 'TARIFF_CODE' THEN val_string_attr ELSE '' END) AS "ATTR (SKU) TARIFF_CODE",
        MAX(CASE WHEN id_attr = 'PFAS' THEN val_string_attr ELSE '' END) AS "ATTR (SKU) PFAS"
    FROM "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic__test" ib
    LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Dynamic__test" av
        ON ib.id_item = av.id_item AND ib.code_comm = av.code_comm
    WHERE ib.code_comm <> 'PAR'
        AND ib."is_deleted" = 0 
        AND av."is_deleted" = 0
    GROUP BY av.id_item
)

-- ========================================
-- PARENT ATTRIBUTES CTE - Extract parent-level attributes
-- ========================================
,parent_attributes AS (
    SELECT
        id_item,
        MAX(CASE WHEN id_attr = 'PRODUCT CAT' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) PRODUCT CAT",
        MAX(CASE WHEN id_attr = 'Z_BRAND' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) Z_BRAND",
        MAX(CASE WHEN id_attr = 'Z_GENDER' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) Z_GENDER",
        MAX(CASE WHEN id_attr = 'PRODUCT TYPE' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) PRODUCT TYPE",
        MAX(CASE WHEN id_attr = 'Z_CATEGORY' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) Z_CATEGORY",
        MAX(CASE WHEN id_attr = 'Z_VERTICAL' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) Z_VERTICAL",
        MAX(CASE WHEN id_attr = 'BERRY' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) BERRY",
        MAX(CASE WHEN id_attr = 'CARE' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) CARE",
        MAX(CASE WHEN id_attr = 'HEAT TRANSFER' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) HEAT TRANSFER",
        MAX(CASE WHEN id_attr = 'OTHER' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) OTHER",
        MAX(CASE WHEN id_attr = 'PAD PRINT' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) PAD PRINT",
        MAX(CASE WHEN id_attr = 'TRACKING' THEN val_string_attr ELSE '' END) AS "ATTR (PAR) TRACKING"
    FROM "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Dynamic__test"
    WHERE code_comm = 'PAR'
        AND "is_deleted" = 0
    GROUP BY id_item
)

-- ========================================
-- PARENT DESCRIPTIONS CTE - Get parent descriptions
-- ========================================
,parent_descriptions AS (
    SELECT
        ib.id_item,
        LISTAGG(id.descr_addl, ', ') AS "PARENT DESCRIPTION"
    FROM "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic__test" ib
    LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_DESCR_Dynamic__test" id
        ON ib.id_item = id.id_item
    WHERE ib.code_comm = 'PAR'
        AND id.seq_descr BETWEEN 800 AND 810
        AND ib."is_deleted" = 0
        AND id."is_deleted" = 0
    GROUP BY ib.id_item
)

-- ========================================
-- VERTICAL CALCULATION CTE - Calculate VERTICAL based on parent attributes
-- ========================================
,vertical_calc AS (
    SELECT
        ib.id_item,
        CASE
            WHEN MAX(pa.z_vertical) = 'AF' THEN 'ARC FLASH PPE'
            WHEN MAX(pa.z_vertical) = 'CT' OR MAX(pa.z_vertical) = 'IS' OR COALESCE(MAX(pa.z_vertical), '') = '' THEN 'INDUSTRIAL PPE'
            WHEN MAX(pa.z_vertical) = 'FR' THEN 'FR CLOTHING'
            WHEN MAX(pa.z_vertical) = 'GV' THEN 'MILITARY'
            WHEN MAX(pa.z_vertical) = 'TH' THEN 'THERMAL'
            WHEN MAX(pa.z_vertical) = 'UL' AND MAX(pa.z_category) = 'AD' THEN 'AD SPECIALTY'
            WHEN MAX(pa.z_vertical) = 'UL' AND MAX(pa.z_category) = 'USPS' THEN 'USPS'
            ELSE 'INDUSTRIAL PPE'
        END AS vertical
    FROM "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic__test" ib
    LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Dynamic__test" av
        ON ib.id_item = av.id_item AND ib.code_comm = av.code_comm
    LEFT JOIN parent_attributes pa
        ON av.val_string_attr = pa.id_item
    WHERE ib."is_deleted" = 0
        AND av."is_deleted" = 0
    GROUP BY ib.id_item
)

-- ========================================
-- CATEGORY CALCULATION CTE - Calculate CATEGORY based on parent attributes
-- ========================================
,category_calc AS (
    SELECT
        ib.id_item,
        MAX(CASE
            WHEN pa.z_vertical = 'AF' AND pa.z_category IN ('CL','KT') THEN 'CLOTHING & KITS'
            WHEN pa.z_vertical = 'AF' AND pa.z_category IN ('ES') THEN 'ELECTRICAL SAFETY'
            WHEN pa.z_vertical = 'AF' AND pa.z_category IN ('FSB') THEN 'FACESHIELDS & BALACLAVAS'
            WHEN pa.z_vertical = 'AF' AND pa.z_category IN ('LP') THEN 'KUNZ LEATHER PROTECTORS'
            WHEN pa.z_vertical = 'AF' AND pa.z_category IN ('VG') THEN 'VOLTAGE RATED GLOVES'
            WHEN pa.z_vertical = 'AF' AND pa.z_category IN ('WG') THEN 'KUNZ WORK GLOVES'
            WHEN pa.z_vertical = 'FR' AND pa.z_category IN ('AC') THEN 'FR ACCESSORIES'
            WHEN pa.z_vertical = 'FR' AND pa.z_category IN ('HV') THEN 'FR HI-VIS'
            WHEN pa.z_vertical = 'FR' AND pa.z_category IN ('IDC') THEN 'FR INFECTIOUS DISEASE CONTROL'
            WHEN pa.z_vertical = 'FR' AND pa.z_category IN ('IND') THEN 'INDUSTRIAL FR UNIFORMS'
            WHEN pa.z_vertical = 'FR' AND pa.z_category IN ('MSC') THEN 'MISC'
            WHEN pa.z_vertical = 'FR' AND pa.z_category IN ('FABRC') THEN 'FR FABRIC'
            WHEN pa.z_vertical = 'FR' AND pa.z_category IN ('RW') THEN 'FR RAINWEAR'
            WHEN pa.z_vertical = 'FR' AND pa.z_category IN ('WW') THEN 'FR WORK WEAR'
            WHEN pa.z_vertical = 'GV' AND pa.z_category IN ('FRML') THEN 'FR MILITARY'
            WHEN pa.z_vertical = 'GV' AND pa.z_category IN ('MISC') THEN 'GOVERNMENT (NON-MILITARY)'
            WHEN pa.z_vertical = 'GV' AND pa.z_category IN ('LE') THEN 'LAW ENFORCEMENT'
            WHEN pa.z_vertical = 'GV' AND pa.z_category IN ('FABRC') THEN 'FR MILITARY FABRIC'
            WHEN pa.z_vertical = 'GV' AND pa.z_category IN ('WT') THEN 'WILD THINGS'
            WHEN pa.z_vertical IN ('CT','IS') AND pa.z_category IN ('CR') THEN 'CRYOGENIC PPE'
            WHEN pa.z_vertical IN ('CT','IS') AND pa.z_category IN ('IDC') THEN 'INFECTIOUS DISEASE CONTROL'
            WHEN pa.z_vertical IN ('CT','IS') AND pa.z_category IN ('CP','HV','MISC') THEN 'MISC'
            WHEN pa.z_vertical IN ('CT','IS') AND pa.z_category IN ('MC') THEN 'MECHANICAL/CUT PROTECTION'
            WHEN pa.z_vertical IN ('TH') AND pa.z_category IN ('CL') THEN 'CLOTHING'
            WHEN pa.z_vertical IN ('TH') AND pa.z_category IN ('FSB') THEN 'FACESHIELDS & BALACLAVAS'
            WHEN pa.z_vertical IN ('TH') AND pa.z_category IN ('HP') THEN 'HAND PROTECTION'
            WHEN pa.z_vertical IN ('TH') AND pa.z_category IN ('MAC') THEN 'MACHINERY PROTECTION'
            WHEN pa.z_vertical IN ('TH') AND pa.z_category IN ('FABRC') THEN 'THERMAL FABRIC'
            WHEN pa.z_vertical IN ('TH') AND pa.z_category IN ('MSC') THEN 'MISC/THERMAL PROTECTION'
            ELSE '#NOT CATEGORIZED'
        END) AS category
    FROM "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic__test" ib
    LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Dynamic__test" av
        ON ib.id_item = av.id_item AND ib.code_comm = av.code_comm
    LEFT JOIN parent_attributes pa
        ON av.val_string_attr = pa.id_item
    WHERE ib."is_deleted" = 0
        AND av."is_deleted" = 0
    GROUP BY ib.id_item
)

-- ========================================
-- Z_CATEGORY CALCULATION CTE - Fallback Z_CATEGORY calculation using Bronze tables
-- ========================================
,z_category_calc AS (
    SELECT
        ib2.id_item,
        MAX(CASE
            WHEN av2.z_vertical = 'AF' AND av2.z_category IN ('CL','KT') THEN 'CLOTHING & KITS'
            WHEN av2.z_vertical = 'AF' AND av2.z_category IN ('ES') THEN 'ELECTRICAL SAFETY'
            WHEN av2.z_vertical = 'AF' AND av2.z_category IN ('FSB') THEN 'FACESHIELDS & BALACLAVAS'
            WHEN av2.z_vertical = 'AF' AND av2.z_category IN ('LP') THEN 'KUNZ LEATHER PROTECTORS'
            WHEN av2.z_vertical = 'AF' AND av2.z_category IN ('VG') THEN 'VOLTAGE RATED GLOVES'
            WHEN av2.z_vertical = 'AF' AND av2.z_category IN ('WG') THEN 'KUNZ WORK GLOVES'
            WHEN av2.z_vertical = 'FR' AND av2.z_category IN ('AC') THEN 'FR ACCESSORIES'
            WHEN av2.z_vertical = 'FR' AND av2.z_category IN ('HV') THEN 'FR HI-VIS'
            WHEN av2.z_vertical = 'FR' AND av2.z_category IN ('IDC') THEN 'FR INFECTIOUS DISEASE CONTROL'
            WHEN av2.z_vertical = 'FR' AND av2.z_category IN ('IND') THEN 'INDUSTRIAL FR UNIFORMS'
            WHEN av2.z_vertical = 'FR' AND av2.z_category IN ('MSC') THEN 'MISC'
            WHEN av2.z_vertical = 'FR' AND av2.z_category IN ('FABRC') THEN 'FR FABRIC'
            WHEN av2.z_vertical = 'FR' AND av2.z_category IN ('RW') THEN 'FR RAINWEAR'
            WHEN av2.z_vertical = 'FR' AND av2.z_category IN ('WW') THEN 'FR WORK WEAR'
            WHEN av2.z_vertical = 'GV' AND av2.z_category IN ('FRML') THEN 'FR MILITARY'
            WHEN av2.z_vertical = 'GV' AND av2.z_category IN ('MISC') THEN 'GOVERNMENT (NON-MILITARY)'
            WHEN av2.z_vertical = 'GV' AND av2.z_category IN ('LE') THEN 'LAW ENFORCEMENT'
            WHEN av2.z_vertical = 'GV' AND av2.z_category IN ('FABRC') THEN 'FR MILITARY FABRIC'
            WHEN av2.z_vertical = 'GV' AND av2.z_category IN ('WT') THEN 'WILD THINGS'
            WHEN av2.z_vertical IN ('CT','IS') AND av2.z_category IN ('CR') THEN 'CRYOGENIC PPE'
            WHEN av2.z_vertical IN ('CT','IS') AND av2.z_category IN ('IDC') THEN 'INFECTIOUS DISEASE CONTROL'
            WHEN av2.z_vertical IN ('CT','IS') AND av2.z_category IN ('CP','HV','MISC') THEN 'MISC'
            WHEN av2.z_vertical IN ('CT','IS') AND av2.z_category IN ('MC') THEN 'MECHANICAL/CUT PROTECTION'
            WHEN av2.z_vertical IN ('TH') AND av2.z_category IN ('CL') THEN 'CLOTHING'
            WHEN av2.z_vertical IN ('TH') AND av2.z_category IN ('FSB') THEN 'FACESHIELDS & BALACLAVAS'
            WHEN av2.z_vertical IN ('TH') AND av2.z_category IN ('HP') THEN 'HAND PROTECTION'
            WHEN av2.z_vertical IN ('TH') AND av2.z_category IN ('MAC') THEN 'MACHINERY PROTECTION'
            WHEN av2.z_vertical IN ('TH') AND av2.z_category IN ('FABRC') THEN 'THERMAL FABRIC'
            WHEN av2.z_vertical IN ('TH') AND av2.z_category IN ('MSC') THEN 'MISC/THERMAL PROTECTION'
            ELSE '#NOT CATEGORIZED'
        END) AS z_category_calc
    FROM "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic__test" ib2
    LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Bronze" av
        ON ib2.id_item = av.id_item AND ib2.code_comm = av.code_comm
    LEFT JOIN (
        SELECT
            id_item,
            MAX(CASE WHEN id_attr = 'Z_VERTICAL' THEN val_string_attr ELSE '' END) AS z_vertical,
            MAX(CASE WHEN id_attr = 'Z_CATEGORY' THEN val_string_attr ELSE '' END) AS z_category
        FROM "BRONZE_DATA"."TCM_BRONZE"."IM_CMCD_ATTR_VALUE_Bronze"
        WHERE code_comm = 'PAR' AND id_attr IN ('Z_VERTICAL', 'Z_CATEGORY')
        GROUP BY id_item
    ) av2
        ON av.val_string_attr = av2.id_item
    GROUP BY ib2.id_item
)

-- ========================================
-- PROP 65 CALCULATION CTE - Calculate PROP 65 status
-- ========================================
,prop_65_calc AS (
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
                    ) latest
                    ON p2.id_item_comp = latest.id_item_comp
                    AND p2.date_eff_end = latest.max_eff_end
                ) latest_comp
                JOIN "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_DESCR_Bronze" d 
                  ON latest_comp.id_item_comp = d.id_item
                WHERE latest_comp.id_item_par = p.id_item_par
                  AND d.descr_addl LIKE '%PROP 65%'
            ) THEN 'Y'
            ELSE 'N'
        END AS prop_65
    FROM "BRONZE_DATA"."TCM_BRONZE"."PRDSTR_Bronze" p
    GROUP BY p.id_item_par
)

-- ========================================
-- MAIN SELECT - Final view query using CTEs
-- ========================================
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
    av2.vertical AS "VERTICAL (Calc)",
    av3.category AS "CATEGORY (Calc)",
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
LEFT JOIN "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_BASE_Dynamic__test" ib
ON lh.id_item = ib.id_item
left join
"BRONZE_DATA"."TCM_BRONZE"."TABLES_LOC_Bronze" tl
on lh.id_loc = tl.ID_LOC
left join
"BRONZE_DATA"."TCM_BRONZE"."TABLES_CODE_CAT_COST_Bronze" cc
on ib.code_cat_cost = cc.code_cat_cost
left join
"BRONZE_DATA"."TCM_BRONZE"."TABLES_CODE_CAT_PRDT_Bronze" pc
on ib.code_cat_prdt = pc.code_cat_prdt
and pc.code_type_cust is null
left join
"BRONZE_DATA"."TCM_BRONZE"."CP_INVHDR_HIST_Bronze" hh
on lh.id_invc = hh.id_invc
left join
"BRONZE_DATA"."TCM_BRONZE"."TABLES_SLSREP_Bronze" sr
on ltrim(hh.id_slsrep_1) = ltrim(sr.id_slsrep)
left join "BRONZE_DATA"."TCM_BRONZE"."ITMMAS_REORD_Bronze" r
on lh.id_item = r.id_item
and lh.id_loc = r.id_loc_home
left join sku_attributes av
on lh.id_item = av.id_item
left join parent_attributes attr
on av."ATTR (SKU) ID_PARENT" = attr.id_item
left join parent_descriptions id
on av."ATTR (SKU) ID_PARENT" = id.id_item
left join category_calc av3
on ib.id_item = av3.id_item
left join vertical_calc av2
on lh.id_item = av2.id_item
left join
"BRONZE_DATA"."TCM_BRONZE"."CUSMAS_SOLDTO_Bronze" st
on ltrim(lh.id_cust_soldto) = ltrim(st.id_cust)
LEFT JOIN prop_65_calc prop 
ON av."ATTR (SKU) ID_PARENT" = prop.id_item_par
left join
"BRONZE_DATA"."TCM_BRONZE"."CUST_GROUP_CODE_Bronze" gc
on st.code_user_3_ar = gc.group_code
where
lh.date_invc > '1/1/2014'
and st.CODE_CUST <>'IC';