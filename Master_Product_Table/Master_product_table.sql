create or replace dynamic table SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE(
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
	ALT_KEY,
	ID_LOC,
    "STATUS",
	"Booking Type Table"
) target_lag = 'DOWNSTREAM' refresh_mode = AUTO initialize = ON_CREATE warehouse = ELT_DEFAULT
 as
/* ========================================
   ITMMAS_BASE — Base item master (kept)
   ======================================== */
WITH ITMMAS_BASE AS (
    SELECT 
        ib.id_item,
        ib.key_alt AS "ALT_KEY",
        ib.code_cat_prdt AS "NSA_PRODUCT CATEGORY/VERTICAL",
        ib.code_cat_cost AS "COST CATEGORY",
        ib.DESCR_1 || ' ' || ib.DESCR_2 AS "Product Description",
        ib.code_comm,
        ib.id_loc,
        ib.FLAG_STAT_ITEM
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic__test" ib
    WHERE ib.FLAG_STAT_ITEM <> 'O'  -- Exclude Obsolete items 
    AND ib."is_deleted" = 0
),

/* ========================================
   SKU ATTRIBUTES — keep ELSE '' as requested
   ======================================== */
sku_attributes AS (
    SELECT
        ib.id_item,
        MAX(CASE WHEN av.id_attr = 'ID_PARENT'   THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) ID_PARENT",
        MAX(CASE WHEN av.id_attr = 'SIZE'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) SIZE",
        MAX(CASE WHEN av.id_attr = 'COLOR'       THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) COLOR",
        MAX(CASE WHEN av.id_attr = 'LENGTH'      THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) LENGTH",
        MAX(CASE WHEN av.id_attr = 'UPC_CODE'    THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) UPC_CODE",
        MAX(CASE WHEN av.id_attr = 'CERT_NUM'    THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) CERT_NUM",
        MAX(CASE WHEN av.id_attr = 'TARIFF_CODE' THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) TARIFF_CODE",
        MAX(CASE WHEN av.id_attr = 'PFAS'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) PFAS"
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic__test" ib
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Dynamic__test" av
           ON ib.id_item = av.id_item
          AND ib.code_comm = av.code_comm
    WHERE ib.code_comm <> 'PAR'
      AND ib."is_deleted" = 0 
      AND av."is_deleted" = 0
    GROUP BY ib.id_item
),

/* ========================================
   PARENT ATTRIBUTES — keep ELSE '' as requested
   ======================================== */
parent_attributes AS (
    SELECT
        av.id_item AS ID_PARENT,
        MAX(CASE WHEN av.id_attr = 'PRODUCT CAT'  THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) PRODUCT CAT",
        MAX(CASE WHEN av.id_attr = 'Z_BRAND'      THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) Z_BRAND",
        MAX(CASE WHEN av.id_attr = 'Z_GENDER'     THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) Z_GENDER",
        MAX(CASE WHEN av.id_attr = 'PRODUCT TYPE' THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) PRODUCT TYPE",
        MAX(CASE WHEN av.id_attr = 'Z_CATEGORY'   THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) Z_CATEGORY",
        MAX(CASE WHEN av.id_attr = 'Z_VERTICAL'   THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) Z_VERTICAL",
        MAX(CASE WHEN av.id_attr = 'BERRY'        THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) BERRY",
        MAX(CASE WHEN av.id_attr = 'CARE'         THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) CARE",
        MAX(CASE WHEN av.id_attr = 'HEAT TRANSFER'THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) HEAT TRANSFER",
        MAX(CASE WHEN av.id_attr = 'OTHER'        THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) OTHER",
        MAX(CASE WHEN av.id_attr = 'PAD PRINT'    THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) PAD PRINT",
        MAX(CASE WHEN av.id_attr = 'TRACKING'     THEN av.val_string_attr ELSE '' END) AS "ATTR (PAR) TRACKING"
    FROM BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Dynamic__test" av
    WHERE av.code_comm = 'PAR'
      AND av."is_deleted" = 0
    GROUP BY av.id_item
),

/* ========================================
   PARENT DESCRIPTIONS — no ORDER BY per request
   ======================================== */
parent_descriptions AS (
    SELECT
        ib.id_item,
        LISTAGG(id.descr_addl, '') WITHIN GROUP (ORDER BY SEQ_DESCR) AS "PARENT DESCRIPTION"
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic__test" ib
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Dynamic__test" id
           ON ib.id_item = id.id_item
    WHERE ib.code_comm = 'PAR'
      AND ib.FLAG_STAT_ITEM <> 'O'  -- Exclude Obsolete items
      AND id.seq_descr BETWEEN 800 AND 810
      AND ib."is_deleted" = 0
      AND id."is_deleted" = 0
    GROUP BY ib.id_item
),

/* ========================================
   VERTICAL — safe parent join via SKU.ID_PARENT
   ======================================== */
vertical_calc AS (
    SELECT
        s.id_item,
        CASE
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'AF' THEN 'ARC FLASH PPE'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('CT','IS') OR COALESCE(pa."ATTR (PAR) Z_VERTICAL", '') = '' THEN 'INDUSTRIAL PPE'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'FR' THEN 'FR CLOTHING'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'GV' THEN 'MILITARY'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'TH' THEN 'THERMAL'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'UL' AND pa."ATTR (PAR) Z_CATEGORY" = 'AD' THEN 'AD SPECIALTY'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'UL' AND pa."ATTR (PAR) Z_CATEGORY" = 'USPS' THEN 'USPS'
            ELSE 'INDUSTRIAL PPE'
        END AS vertical
    FROM sku_attributes s
    LEFT JOIN parent_attributes pa
           ON s."ATTR (SKU) ID_PARENT" = pa.ID_PARENT
),

/* ========================================
   CATEGORY — safe parent join via SKU.ID_PARENT
   ======================================== */
category_calc AS (
    SELECT
        s.id_item,
        MAX(CASE
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'AF' AND pa."ATTR (PAR) Z_CATEGORY" IN ('CL','KT') THEN 'CLOTHING & KITS'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'AF' AND pa."ATTR (PAR) Z_CATEGORY" IN ('ES') THEN 'ELECTRICAL SAFETY'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'AF' AND pa."ATTR (PAR) Z_CATEGORY" IN ('FSB') THEN 'FACESHIELDS & BALACLAVAS'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'AF' AND pa."ATTR (PAR) Z_CATEGORY" IN ('LP') THEN 'KUNZ LEATHER PROTECTORS'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'AF' AND pa."ATTR (PAR) Z_CATEGORY" IN ('VG') THEN 'VOLTAGE RATED GLOVES'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'AF' AND pa."ATTR (PAR) Z_CATEGORY" IN ('WG') THEN 'KUNZ WORK GLOVES'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'FR' AND pa."ATTR (PAR) Z_CATEGORY" IN ('AC') THEN 'FR ACCESSORIES'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'FR' AND pa."ATTR (PAR) Z_CATEGORY" IN ('HV') THEN 'FR HI-VIS'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'FR' AND pa."ATTR (PAR) Z_CATEGORY" IN ('IDC') THEN 'FR INFECTIOUS DISEASE CONTROL'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'FR' AND pa."ATTR (PAR) Z_CATEGORY" IN ('IND') THEN 'INDUSTRIAL FR UNIFORMS'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'FR' AND pa."ATTR (PAR) Z_CATEGORY" IN ('MSC') THEN 'MISC'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'FR' AND pa."ATTR (PAR) Z_CATEGORY" IN ('FABRC') THEN 'FR FABRIC'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'FR' AND pa."ATTR (PAR) Z_CATEGORY" IN ('RW') THEN 'FR RAINWEAR'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'FR' AND pa."ATTR (PAR) Z_CATEGORY" IN ('WW') THEN 'FR WORK WEAR'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'GV' AND pa."ATTR (PAR) Z_CATEGORY" IN ('FRML') THEN 'FR MILITARY'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'GV' AND pa."ATTR (PAR) Z_CATEGORY" IN ('MISC') THEN 'GOVERNMENT (NON-MILITARY)'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'GV' AND pa."ATTR (PAR) Z_CATEGORY" IN ('LE') THEN 'LAW ENFORCEMENT'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'GV' AND pa."ATTR (PAR) Z_CATEGORY" IN ('FABRC') THEN 'FR MILITARY FABRIC'
            WHEN pa."ATTR (PAR) Z_VERTICAL" = 'GV' AND pa."ATTR (PAR) Z_CATEGORY" IN ('WT') THEN 'WILD THINGS'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('CT','IS') AND pa."ATTR (PAR) Z_CATEGORY" IN ('CR') THEN 'CRYOGENIC PPE'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('CT','IS') AND pa."ATTR (PAR) Z_CATEGORY" IN ('IDC') THEN 'INFECTIOUS DISEASE CONTROL'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('CT','IS') AND pa."ATTR (PAR) Z_CATEGORY" IN ('CP','HV','MISC') THEN 'MISC'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('CT','IS') AND pa."ATTR (PAR) Z_CATEGORY" IN ('MC') THEN 'MECHANICAL/CUT PROTECTION'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('TH') AND pa."ATTR (PAR) Z_CATEGORY" IN ('CL') THEN 'CLOTHING'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('TH') AND pa."ATTR (PAR) Z_CATEGORY" IN ('FSB') THEN 'FACESHIELDS & BALACLAVAS'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('TH') AND pa."ATTR (PAR) Z_CATEGORY" IN ('HP') THEN 'HAND PROTECTION'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('TH') AND pa."ATTR (PAR) Z_CATEGORY" IN ('MAC') THEN 'MACHINERY PROTECTION'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('TH') AND pa."ATTR (PAR) Z_CATEGORY" IN ('FABRC') THEN 'THERMAL FABRIC'
            WHEN pa."ATTR (PAR) Z_VERTICAL" IN ('TH') AND pa."ATTR (PAR) Z_CATEGORY" IN ('MSC') THEN 'MISC/THERMAL PROTECTION'
            ELSE '#NOT CATEGORIZED'
        END) AS category
    FROM sku_attributes s
    LEFT JOIN parent_attributes pa
           ON s."ATTR (SKU) ID_PARENT" = pa.ID_PARENT
    GROUP BY s.id_item
),

/* ========================================
   PROP 65 — unchanged logic
   ======================================== */
prop_65_calc AS (
    SELECT 
        p.id_item_par,
        CASE 
            WHEN EXISTS (
                SELECT 1
                FROM (
                    SELECT p2.*
                    FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" p2
                    INNER JOIN (
                        SELECT id_item_comp, MAX(date_eff_end) AS max_eff_end
                        FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze"
                        GROUP BY id_item_comp
                    ) latest
                        ON p2.id_item_comp = latest.id_item_comp
                       AND p2.date_eff_end = latest.max_eff_end
                ) latest_comp
                JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze" d 
                  ON latest_comp.id_item_comp = d.id_item
                WHERE latest_comp.id_item_par = p.id_item_par
                  AND d.descr_addl LIKE '%PROP 65%'
            ) THEN 'Y'
            ELSE 'N'
        END AS prop_65
    FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" p
    GROUP BY p.id_item_par
)

-- =========================
-- Final SELECT (SKUs only)
-- =========================
SELECT
    b.id_item                                   AS "Product ID/SKU",
    b."Product Description",
    b."COST CATEGORY"                           AS "COST CATEGORY ID",
    COALESCE(b."COST CATEGORY" || ' - ' || cc.descr, 'INVALID COST CATEGORY')                 AS "COST CAT DESCR",
    b."NSA_PRODUCT CATEGORY/VERTICAL"           AS "PRODUCT CATEGORY/VERTICAL",
    COALESCE(b."NSA_PRODUCT CATEGORY/VERTICAL" || ' - ' || pc.descr, 'INVALID PRODUCT CATEGORY') AS "PRDT CAT DESCR",

    v.vertical                                  AS "VERTICAL (Calc)",
    c.category                                  AS "CATEGORY (Calc)",

    s."ATTR (SKU) ID_PARENT"                    AS "Product Name/Parent ID",
    CASE
        WHEN "PRDT CAT DESCR" ILIKE '%FABRIC%' AND pd."PARENT DESCRIPTION" IS NULL
        THEN b."Product Description"
        ELSE COALESCE(pd."PARENT DESCRIPTION", 'MISSING DESCRIPTION - UPDATE TCM')
    END AS "PARENT DESCRIPTION",

    s."ATTR (SKU) CERT_NUM",
    s."ATTR (SKU) COLOR",
    s."ATTR (SKU) SIZE",
    s."ATTR (SKU) LENGTH",
    s."ATTR (SKU) TARIFF_CODE",
    s."ATTR (SKU) UPC_CODE",
    s."ATTR (SKU) PFAS",

    pa."ATTR (PAR) BERRY",
    pa."ATTR (PAR) CARE",
    pa."ATTR (PAR) HEAT TRANSFER",
    pa."ATTR (PAR) OTHER",
    pa."ATTR (PAR) PAD PRINT",
    pa."ATTR (PAR) PRODUCT CAT",
    pa."ATTR (PAR) PRODUCT TYPE",
    pa."ATTR (PAR) TRACKING",
    pa."ATTR (PAR) Z_BRAND",
    pa."ATTR (PAR) Z_CATEGORY",
    pa."ATTR (PAR) Z_GENDER",
    pa."ATTR (PAR) Z_VERTICAL",

    p65.prop_65                                 AS "PROP 65",
    b."ALT_KEY",
    b.id_loc                                    AS "ID_LOC",
    b.FLAG_STAT_ITEM                            AS "STATUS",
    /* placeholder until sourced */
    NULL                                        AS "Booking Type Table"

FROM ITMMAS_BASE b
LEFT JOIN BRONZE_DATA.TCM_BRONZE."TABLES_CODE_CAT_COST_Bronze" cc
       ON b."COST CATEGORY" = cc.code_cat_cost
LEFT JOIN BRONZE_DATA.TCM_BRONZE."TABLES_CODE_CAT_PRDT_Bronze" pc
       ON b."NSA_PRODUCT CATEGORY/VERTICAL" = pc.code_cat_prdt
      AND pc.code_type_cust IS NULL

LEFT JOIN sku_attributes     s   ON b.id_item = s.id_item
LEFT JOIN parent_attributes  pa  ON s."ATTR (SKU) ID_PARENT" = pa.ID_PARENT
LEFT JOIN parent_descriptions pd ON s."ATTR (SKU) ID_PARENT" = pd.id_item
LEFT JOIN category_calc      c   ON b.id_item = c.id_item
LEFT JOIN vertical_calc      v   ON b.id_item = v.id_item
LEFT JOIN prop_65_calc       p65 ON s."ATTR (SKU) ID_PARENT" = p65.id_item_par

WHERE b.code_comm <> 'PAR';


select count(*) from SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE;