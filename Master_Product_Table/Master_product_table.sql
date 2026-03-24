create or replace dynamic table SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE(
	"Product ID/SKU",
	"Product Description",
	"COST CATEGORY ID",
	"COST CAT DESCR",
	"PRODUCT CATEGORY/VERTICAL",
	"PRDT CAT DESCR",
	"COMMODITY CODE",
	RATIO_STK_PUR,
	"VERTICAL (Calc)",
	"CATEGORY (Calc)",
	"Current Cost",
	"Standard Cost",
	"Product Name/Parent ID",
	"PARENT DESCRIPTION",
	"ATTR (SKU) CERT_NUM",
	"ATTR (SKU) COLOR",
	"ATTR (SKU) SIZE",
	"ATTR (SKU) LENGTH",
	"ATTR (SKU) TARIFF_CODE",
	"ATTR (SKU) UPC_CODE",
	"ATTR (SKU) PFAS",
	"ATTR (SKU) CLASS",
	"ATTR (SKU) PPC",
	"ATTR (SKU) PRIOR COMMODITY",
	"ATTR (SKU) RBN_WC",
	"ATTR (SKU) REASON",
	"ATTR (SKU) REPLACEMENT",
	"ATTR (SKU) REQUESTOR",
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
	"Advertised Flag",
	"PROP 65",
	ALT_KEY,
	ID_LOC,
	"Child Item Status",
	"Parent Item Status",
	"Adj_Parent_Item_Status",
	"COST_FB_VA_CRNT",
	"COST_FB_VA_STD",
	"COST_MATL_VA_CRNT", 
	"COST_LABOR_VA_CRNT",
	"COST_MATL_VA_STD",
	"COST_LABOR_VA_STD",
	"COST_OUTP_VA_CRNT",
	"COST_USER_VA_CRNT",
	"COST_OUTP_VA_STD",
	"COST_USER_VA_STD",
	"COST_TOTAL_ACCUM_CRNT",
	"COST_TOTAL_ACCUM_STD",
	"COST_VB_VA_CRNT",
	"COST_VB_VA_STD",
    "ID_LOC_SRC_COST_STD",
    "TYPE_COST",
    "DATE_ACCUM_COST",
    "DATE_CHG_COST_VA",
    "DATE_STD_COST",
    "WGT_ITEM",
    "RATIO_STK_PRICE",
    "CODE_UM_PRICE",
    "CODE_UM_PUR",
    "CODE_UM_STK",
    "CODE_USER_1_IM",
    "CODE_USER_2_IM",
    "CODE_USER_3_IM",
    "DATE_QUOTE",
    "ID_VND_ORDFM",
    "ID_VND_PAYTO",
    "ID_ITEM_VND",
    "CODE_UM_VND",
    "LEVEL_ROP",
    "QTY_MIN_ROP",
    "QTY_MULT_ORD_ROP",
    "ID_LOC_HOME",
    "QTY_ORD_ECON",
    "LT_ROP"
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
        ib.FLAG_STAT_ITEM AS CHILD_ITEM_STATUS,
        ib."RATIO_STK_PUR",
        ib.type_cost,
        ib.wgt_item,
        ib.ratio_stk_price,
        ib.code_um_price,
        ib.code_um_pur,
        ib.code_um_stk,
        ib.code_user_1_im,
        ib.code_user_2_im,
        ib.code_user_3_im
    -- FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib 
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic" ib
    WHERE ib."is_deleted" = 0
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
        MAX(CASE WHEN av.id_attr = 'PFAS'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) PFAS",
        MAX(CASE WHEN av.id_attr = 'CLASS'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) CLASS",
        MAX(CASE WHEN av.id_attr = 'PPC'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) PPC",
        MAX(CASE WHEN av.id_attr = 'PRIOR COMMODITY'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) PRIOR COMMODITY",
        MAX(CASE WHEN av.id_attr = 'RBN_WC'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) RBN_WC",
        MAX(CASE WHEN av.id_attr = 'REASON'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) REASON",
        MAX(CASE WHEN av.id_attr = 'REPLACEMENT'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) REPLACEMENT",
        MAX(CASE WHEN av.id_attr = 'REQUESTOR'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) REQUESTOR",
        
        
    -- FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic" ib
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze" av
    -- LEFT JOIN BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Dynamic" av
           ON ib.id_item = av.id_item
          AND ib.code_comm = av.code_comm
    WHERE ib.code_comm <> 'PAR'
      AND ib."is_deleted" = 0 
    --   AND av."is_deleted" = 0
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
    FROM BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze" av
    -- FROM BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Dynamic" av
    WHERE av.code_comm = 'PAR'
    --   AND av."is_deleted" = 0
    GROUP BY av.id_item
),

/* ========================================
   PARENT DESCRIPTIONS — no ORDER BY per request
   ======================================== */
parent_descriptions AS (
    SELECT
        ib.id_item,
        ib.FLAG_STAT_ITEM AS PARENT_ITEM_STATUS,
        LISTAGG(id.descr_addl, '') WITHIN GROUP (ORDER BY SEQ_DESCR) AS "PARENT DESCRIPTION"
    -- FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Dynamic" ib
    LEFT JOIN (select * from BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze"
                where seq_descr BETWEEN 800 AND 810) id
           ON ib.id_item = id.id_item
    WHERE ib.code_comm = 'PAR' 
      AND ib."is_deleted" = 0
    GROUP BY ib.id_item, ib.FLAG_STAT_ITEM
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
),

/* ========================================
   PRIMARY VENDOR — picks primary vendor per item (flag_vnd_prim = 'P' first)
   ======================================== */
primary_vendor AS (
    SELECT
        iv.id_item,
        iv.date_quote,
        iv.id_vnd_ordfm,
        iv.id_vnd_payto,
        iv.id_item_vnd,
        iv.code_um_vnd
        /*
        ROW_NUMBER() OVER (
            PARTITION BY iv.id_item
            ORDER BY CASE WHEN iv.flag_vnd_prim = 'P' THEN 0 ELSE 1 END, iv.id_vnd_payto
        ) AS rn */
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_VND_Bronze" iv
    WHERE iv.flag_vnd_prim = 'P'
),

/* ========================================
   REORDER PARAMS — per item/location from itmmas_reord
   ======================================== */
reorder_params AS (
    SELECT
        ir.id_item,
        ir.level_rop,
        ir.qty_min_rop,
        ir.qty_mult_ord_rop,
        ir.id_loc_home,
        ir.qty_ord_econ,
        ir.lt_rop
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_REORD_Bronze" ir
),

/*  ========================================
   Adjusted Parent Item Status Logic
   =======================================*/
Adjusted_Parent_Item_Status AS (
    SELECT 
        adj."Product ID/SKU",
        count(*) AS cnt
    FROM (         
        SELECT 
            b.ID_ITEM as "Product ID/SKU",
            pd.PARENT_ITEM_STATUS AS "Parent Item Status",
            b.CHILD_ITEM_STATUS AS "Child Item Status"
        FROM ITMMAS_BASE b
        LEFT JOIN sku_attributes        s   ON b.id_item = s.id_item
        LEFT JOIN parent_descriptions   pd  ON s."ATTR (SKU) ID_PARENT" = pd.id_item
        WHERE b.CHILD_ITEM_STATUS = 'A' AND pd.PARENT_ITEM_STATUS = 'O'
    ) adj
    GROUP BY adj."Product ID/SKU" 
)
        
    SELECT
        b.id_item                                   AS "Product ID/SKU",
        UPPER(b."Product Description") AS "Product Description",
        b."COST CATEGORY"                           AS "COST CATEGORY ID",
        UPPER(COALESCE(b."COST CATEGORY" || ' - ' || cc.descr, 'INVALID COST CATEGORY'))                 AS "COST CAT DESCR",
        UPPER(b."NSA_PRODUCT CATEGORY/VERTICAL")           AS "PRODUCT CATEGORY/VERTICAL",
        UPPER(COALESCE(b."NSA_PRODUCT CATEGORY/VERTICAL" || ' - ' || pc.descr, 'INVALID PRODUCT CATEGORY')) AS "PRDT CAT DESCR",
        b."CODE_COMM" AS "COMMODITY CODE",
        b."RATIO_STK_PUR",
        UPPER(v.vertical)                                  AS "VERTICAL (Calc)",
        UPPER(c.category)                                  AS "CATEGORY (Calc)",
        ic.COST_MATL_ACCUM_CRNT ,
        ic.COST_MATL_ACCUM_STD ,
        ic.COST_FB_VA_CRNT,
        ic.COST_FB_VA_STD,
        ic.COST_MATL_VA_CRNT, 
        ic.COST_LABOR_VA_CRNT,
        ic.COST_MATL_VA_STD,
        ic.COST_LABOR_VA_STD,
        ic.COST_OUTP_VA_CRNT,
        ic.COST_USER_VA_CRNT,
        ic.COST_OUTP_VA_STD,
        ic.COST_USER_VA_STD,
        ic.COST_TOTAL_ACCUM_CRNT,
        ic.COST_TOTAL_ACCUM_STD,
        ic.COST_VB_VA_CRNT,
        ic.COST_VB_VA_STD,
        ic.id_loc_src_cost_std as "ID_LOC_SRC_COST_STD",
        b.type_cost as "TYPE_COST",
        ic.date_accum_cost as "DATE_ACCUM_COST",
        ic.date_chg_cost_va as "DATE_CHG_COST_VA",
        ic.date_std_cost as "DATE_STD_COST",
        b.wgt_item as "WGT_ITEM",
        b.ratio_stk_price as "RATIO_STK_PRICE",
        b.code_um_price as "CODE_UM_PRICE",
        b.code_um_pur as "CODE_UM_PUR",
        b.code_um_stk as "CODE_UM_STK",
        b.code_user_1_im as "CODE_USER_1_IM",
        b.code_user_2_im as "CODE_USER_2_IM",
        b.code_user_3_im as "CODE_USER_3_IM",
        pv.date_quote    AS "DATE_QUOTE",
        pv.id_vnd_ordfm  AS "ID_VND_ORDFM",
        pv.id_vnd_payto  AS "ID_VND_PAYTO",
        pv.id_item_vnd   AS "ID_ITEM_VND",
        pv.code_um_vnd   AS "CODE_UM_VND",
        rp.level_rop         AS "LEVEL_ROP",
        rp.qty_min_rop       AS "QTY_MIN_ROP",
        rp.qty_mult_ord_rop  AS "QTY_MULT_ORD_ROP",
        rp.id_loc_home       AS "ID_LOC_HOME",
        rp.qty_ord_econ      AS "QTY_ORD_ECON",
        rp.lt_rop            AS "LT_ROP",
        
        s."ATTR (SKU) ID_PARENT"                    AS "Product Name/Parent ID",
        UPPER(CASE
            WHEN "PRDT CAT DESCR" ILIKE '%FABRIC%' AND pd."PARENT DESCRIPTION" IS NULL
            THEN b."Product Description"
            ELSE COALESCE(pd."PARENT DESCRIPTION", 'MISSING DESCRIPTION - UPDATE TCM')
        END) AS "PARENT DESCRIPTION",

        UPPER(s."ATTR (SKU) CERT_NUM") AS "ATTR (SKU) CERT_NUM",
        UPPER(s."ATTR (SKU) COLOR") AS "ATTR (SKU) COLOR",
        UPPER(s."ATTR (SKU) SIZE") AS "ATTR (SKU) SIZE",
        UPPER(s."ATTR (SKU) LENGTH") AS "ATTR (SKU) LENGTH",
        UPPER(s."ATTR (SKU) TARIFF_CODE") AS "ATTR (SKU) TARIFF_CODE",
        UPPER(s."ATTR (SKU) UPC_CODE") AS "ATTR (SKU) UPC_CODE",
        UPPER(s."ATTR (SKU) PFAS") AS "ATTR (SKU) PFAS",
        UPPER(s."ATTR (SKU) PFAS") AS "ATTR (SKU) CLASS",
        UPPER(s."ATTR (SKU) PFAS") AS "ATTR (SKU) PPC",
        UPPER(s."ATTR (SKU) PFAS") AS "ATTR (SKU) PRIOR COMMODITY",
        UPPER(s."ATTR (SKU) PFAS") AS "ATTR (SKU) RBN_WC",
        UPPER(s."ATTR (SKU) PFAS") AS "ATTR (SKU) REASON",
        UPPER(s."ATTR (SKU) PFAS") AS "ATTR (SKU) REPLACEMENT",
        UPPER(s."ATTR (SKU) PFAS") AS "ATTR (SKU) REQUESTOR",
        UPPER(pa."ATTR (PAR) BERRY") AS "ATTR (PAR) BERRY",
        UPPER(pa."ATTR (PAR) CARE") AS "ATTR (PAR) CARE",
        UPPER(pa."ATTR (PAR) HEAT TRANSFER") AS "ATTR (PAR) HEAT TRANSFER",
        UPPER(pa."ATTR (PAR) OTHER") AS "ATTR (PAR) OTHER",
        UPPER(pa."ATTR (PAR) PAD PRINT") AS "ATTR (PAR) PAD PRINT",
        UPPER(pa."ATTR (PAR) PRODUCT CAT") AS "ATTR (PAR) PRODUCT CAT",
        UPPER(pa."ATTR (PAR) PRODUCT TYPE") AS "ATTR (PAR) PRODUCT TYPE",
        UPPER(pa."ATTR (PAR) TRACKING") AS "ATTR (PAR) TRACKING",
        UPPER(pa."ATTR (PAR) Z_BRAND") AS "ATTR (PAR) Z_BRAND",
        UPPER(pa."ATTR (PAR) Z_CATEGORY") AS "ATTR (PAR) Z_CATEGORY",
        UPPER(pa."ATTR (PAR) Z_GENDER") AS "ATTR (PAR) Z_GENDER",
        UPPER(pa."ATTR (PAR) Z_VERTICAL") AS "ATTR (PAR) Z_VERTICAL",
        UPPER(stkl.adv) as "Advertised Flag",
        UPPER(p65.prop_65)                                 AS "PROP 65",
        b."ALT_KEY",
        b.id_loc                                    AS "ID_LOC",
        UPPER(b.CHILD_ITEM_STATUS)                        AS "Child Item Status",
        UPPER(pd.PARENT_ITEM_STATUS)                       AS "Parent Item Status",
        /* placeholder until sourced */
        /* Adjusted Parent Item Status via the CTE logic */
        UPPER(CASE 
            WHEN apit.cnt >= 1 THEN 'A'
            ELSE pd.PARENT_ITEM_STATUS
        END) AS "Adj_Parent_Item_Status"

    FROM ITMMAS_BASE b
    -- LEFT JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC" il ON b.id_item = il.id_item
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_COST_Bronze" ic on b.id_item = ic.id_item 
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."TABLES_CODE_CAT_COST_Bronze" cc ON b."COST CATEGORY" = cc.code_cat_cost
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."TABLES_CODE_CAT_PRDT_Bronze" pc ON b."NSA_PRODUCT CATEGORY/VERTICAL" = pc.code_cat_prdt AND pc.code_type_cust IS NULL
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_STK_LIST_Bronze" stkl on b.id_item = stkl.id_item 
    LEFT JOIN sku_attributes     s   ON b.id_item = s.id_item
    LEFT JOIN parent_attributes  pa  ON s."ATTR (SKU) ID_PARENT" = pa.ID_PARENT
    LEFT JOIN parent_descriptions pd ON s."ATTR (SKU) ID_PARENT" = pd.id_item
    LEFT JOIN category_calc      c   ON b.id_item = c.id_item
    LEFT JOIN vertical_calc      v   ON b.id_item = v.id_item
    LEFT JOIN prop_65_calc       p65 ON s."ATTR (SKU) ID_PARENT" = p65.id_item_par
    LEFT JOIN primary_vendor     pv  ON b.id_item = pv.id_item 
    LEFT JOIN reorder_params     rp  ON b.id_item = rp.id_item
    LEFT JOIN Adjusted_Parent_Item_Status apit ON b.id_item = apit."Product ID/SKU" 
    WHERE b.code_comm <> 'PAR';