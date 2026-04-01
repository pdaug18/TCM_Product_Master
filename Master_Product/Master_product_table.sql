create or replace dynamic table SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE
target_lag = 'DOWNSTREAM' 
refresh_mode = AUTO 
initialize = ON_CREATE 
warehouse = ELT_DEFAULT
as
/* ========================================
   ITMMAS_BASE — Base item master (kept)
   ======================================== */
WITH ITMMAS_BASE AS (
    SELECT 
        ib.id_item,
        ib.key_alt AS "Item_ALT Key",
        ib.code_cat_prdt AS "NSA_PRODUCT CATEGORY/VERTICAL",
        ib.code_cat_cost AS "COST CATEGORY",
        ib.DESCR_1 || ' ' || ib.DESCR_2 AS "Item Description_Child SKU",
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
        MAX(CASE WHEN av.id_attr = 'CERT_NUM'    THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) CERT_NUM",
        MAX(CASE WHEN av.id_attr = 'COLOR'       THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) COLOR",
        MAX(CASE WHEN av.id_attr = 'SIZE'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) SIZE",
        MAX(CASE WHEN av.id_attr = 'LENGTH'      THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) LENGTH",
        MAX(CASE WHEN av.id_attr = 'TARIFF_CODE' THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) TARIFF_CODE",
        MAX(CASE WHEN av.id_attr = 'UPC_CODE'    THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) UPC_CODE",
        MAX(CASE WHEN av.id_attr = 'PFAS'        THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) PFAS",
        MAX(CASE WHEN av.id_attr = 'CLASS'       THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) CLASS",
        MAX(CASE WHEN av.id_attr = 'PPC'         THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) PPC",
        MAX(CASE WHEN av.id_attr = 'PRIOR COMMODITY' THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) PRIOR COMMODITY",
        MAX(CASE WHEN av.id_attr = 'RBN_WC'      THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) RBN_WC",
        MAX(CASE WHEN av.id_attr = 'REASON'      THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) REASON",
        MAX(CASE WHEN av.id_attr = 'REPLACEMENT' THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) REPLACEMENT",
        MAX(CASE WHEN av.id_attr = 'REQUESTOR'   THEN av.val_string_attr ELSE '' END) AS "ATTR (SKU) REQUESTOR",
        
        
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
        LISTAGG(id.descr_addl, '') WITHIN GROUP (ORDER BY SEQ_DESCR) AS "Item Description_Parent SKU"
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

/*  ========================================
   Adjusted Parent Item Status Logic
   =======================================*/
Adjusted_Parent_Item_Status AS (
    SELECT 
        adj."Item ID_Child SKU",
        count(*) AS cnt
    FROM (         
        SELECT 
            b.ID_ITEM AS "Item ID_Child SKU",
            pd.PARENT_ITEM_STATUS AS "Item Status_Parent Active Status",
            b.CHILD_ITEM_STATUS AS "Item Status_Child Active Status"
        FROM ITMMAS_BASE b
        LEFT JOIN sku_attributes        s   ON b.id_item = s.id_item
        LEFT JOIN parent_descriptions   pd  ON s."ATTR (SKU) ID_PARENT" = pd.id_item
        WHERE b.CHILD_ITEM_STATUS = 'A' AND pd.PARENT_ITEM_STATUS = 'O'
    ) adj
    GROUP BY adj."Item ID_Child SKU" 
),

/* ========================================
   ITEM PLANNER — single id_planner per id_item
   Ranking priority:
     1. flag_source = 'M' (manufactured) over 'P' (purchased)
     2. Within same flag_source: location '10' (Cleveland HQ) first
     3. Then alphabetically by id_loc for any remaining ties
   ======================================== */
item_planner AS (
    SELECT
        id_item,
        id_loc           AS primary_mfg_loc,
        id_planner
    FROM (
        SELECT
            il.id_item,
            il.id_loc,
            il.id_planner,
            ROW_NUMBER() OVER (
                PARTITION BY il.id_item
                ORDER BY
                    CASE WHEN il.flag_source = 'M' THEN 0 ELSE 1 END,
                    CASE WHEN il.id_loc = '10'     THEN 0 ELSE 1 END,
                    il.id_loc
            ) AS rn
        FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze" il
        WHERE il.flag_source IN ('M', 'P')
    ) ranked
    WHERE rn = 1
)
        
    SELECT
        b.id_item                                   AS "Item ID_Child SKU",
        UPPER(b."Item Description_Child SKU")       AS "Item Description_Child SKU",
        b."COST CATEGORY"                           AS "Item_Cost Category ID",
        UPPER(COALESCE(b."COST CATEGORY" || ' - ' || cc.descr, 'INVALID COST CATEGORY'))                 AS "COST CAT DESCR",
        UPPER(b."NSA_PRODUCT CATEGORY/VERTICAL")    AS "PRODUCT CATEGORY/VERTICAL",
        UPPER(COALESCE(b."NSA_PRODUCT CATEGORY/VERTICAL" || ' - ' || pc.descr, 'INVALID PRODUCT CATEGORY')) AS "PRDT CAT DESCR",
        b."CODE_COMM"                               AS "COMMODITY CODE",
        b."RATIO_STK_PUR",
        UPPER(v.vertical)                           AS "Item_Vertical",
        UPPER(c.category)                           AS "CATEGORY (Calc)",
        ic.COST_MATL_ACCUM_CRNT                     AS "Cost_Material_Accumulated_Current",
        ic.COST_MATL_ACCUM_STD                      AS "Cost_Material_Accumulated_Standard",
        ic.COST_FB_VA_CRNT                          AS "Cost_Freight_Current",
        ic.COST_FB_VA_STD                           AS "Cost_Freight_Standard",
        ic.COST_MATL_VA_CRNT                        AS "Cost_Material_Current",
        ic.COST_MATL_VA_STD                         AS "Cost_Material_Standard",
        ic.COST_LABOR_VA_CRNT                       AS "Cost_Labor_Current",
        ic.COST_LABOR_VA_STD                        AS "Cost_Labor_Standard",
        ic.COST_OUTP_VA_CRNT                        AS "Cost_Outside Service_Current",
        ic.COST_USER_VA_CRNT                        AS "Cost_User Field_Current",
        ic.COST_OUTP_VA_STD                         AS "Cost_Outside Service_Standard",
        ic.COST_USER_VA_STD                         AS "Cost_User Field_Standard",
        ic.COST_TOTAL_ACCUM_CRNT                    AS "Cost_Total_Current",
        ic.COST_TOTAL_ACCUM_STD                     AS "Cost_Total_Standard",
        ic.COST_VB_VA_CRNT                          AS "Cost_Variable Burden_Current",
        ic.COST_VB_VA_STD                           AS "Cost_Variable Burden_Standard",
        CASE 
            WHEN ic.COST_MATL_VA_CRNT IS NOT NULL THEN ic.COST_MATL_VA_CRNT 
            WHEN ic.COST_MATL_VA_STD IS NOT NULL THEN ic.COST_MATL_VA_STD
            ELSE 0
        END AS "Cost_Material_Current_Calculated", 
        CASE 
            WHEN ic.COST_LABOR_VA_CRNT IS NOT NULL THEN ic.COST_LABOR_VA_CRNT 
            WHEN ic.COST_LABOR_VA_CRNT IS NOT NULL THEN ic.COST_LABOR_VA_CRNT
            ELSE 0
        END AS "Cost_Labor_Current_Calculated", 
        CASE 
            WHEN ic.COST_MATL_VA_CRNT IS NOT NULL AND ic.COST_LABOR_VA_CRNT IS NOT NULL THEN ic.COST_MATL_VA_CRNT + ic.COST_LABOR_VA_CRNT
            WHEN ic.COST_MATL_VA_CRNT IS NOT NULL AND ic.COST_LABOR_VA_CRNT IS NULL THEN ic.COST_MATL_VA_CRNT
            WHEN ic.COST_MATL_VA_CRNT IS NULL AND ic.COST_LABOR_VA_CRNT IS NOT NULL THEN ic.COST_LABOR_VA_CRNT
            ELSE 0
        END AS "Cost_Material_Labor_Current_Calculated",
        ic.id_loc_src_cost_std                      AS "Cost_Location_Standard_Cost_Source_Location",
        b.type_cost                                 AS "Cost_Cost_Type",
        ic.date_accum_cost                          AS "Date_Cost_Accumulated",
        ic.date_chg_cost_va                         AS "Date_Cost_Changed",
        ic.date_std_cost                            AS "Date_Cost_Standard",
        b.wgt_item                                  AS "Item_Weight",
        b.ratio_stk_price                           AS "Ratio_Price_to_Stock",
        b.code_um_price                             AS "Unit_of_Measure_Price",
        b.code_um_pur                               AS "Unit_of_Measure_Purchase",
        b.code_um_stk                               AS "Unit_of_Measure_Stock",
        b.code_user_1_im                            AS "CODE_USER_1",
        b.code_user_2_im                            AS "CODE_USER_2",
        b.code_user_3_im                            AS "CODE_USER_3",
        pv.date_quote                               AS "Date_Last_Vendor_Quote",
        pv.id_vnd_ordfm                             AS "Item_Primary_Vendor_Primary_Order_From_ID",
        pv.id_vnd_payto                             AS "Item_Primary_Vendor_Primary_Pay_To_ID",
        pv.id_item_vnd                              AS "Item_Primary_Vendor_Vendor_Item_ID",
        pv.code_um_vnd                              AS "Unit_of_Measure_Vendor_Code",
        s."ATTR (SKU) ID_PARENT"                    AS "Item ID_Parent SKU",
        UPPER(CASE
            WHEN "PRDT CAT DESCR" ILIKE '%FABRIC%' AND pd."Item Description_Parent SKU" IS NULL
            THEN b."Item Description_Child SKU"
            ELSE COALESCE(pd."Item Description_Parent SKU", 'MISSING DESCRIPTION - UPDATE TCM')
        END)                                        AS "Item Description_Parent SKU",

        UPPER(s."ATTR (SKU) CERT_NUM")              AS "Item_Certificate Number",
        UPPER(s."ATTR (SKU) COLOR")                 AS "Item_Color",
        UPPER(s."ATTR (SKU) SIZE")                  AS "Item_Size",
        UPPER(s."ATTR (SKU) LENGTH")                AS "Item_Length",
        UPPER(s."ATTR (SKU) TARIFF_CODE")           AS "Item_Tariff Code",
        UPPER(s."ATTR (SKU) UPC_CODE")              AS "Item_UPC Code",
        UPPER(s."ATTR (SKU) PFAS")                  AS "Item_PFAS",
        UPPER(s."ATTR (SKU) CLASS")                 AS "Item_Class",
        UPPER(s."ATTR (SKU) PPC")                   AS "Item_PPC",
        UPPER(s."ATTR (SKU) PRIOR COMMODITY")       AS "Item_Commodity Code Prior",
        UPPER(s."ATTR (SKU) RBN_WC")                AS "Item_Work Center_Rubin",
        UPPER(s."ATTR (SKU) REASON")                AS "Item Status_Obsolete Reason",
        UPPER(s."ATTR (SKU) REPLACEMENT")           AS "Item_Replaced By",
        UPPER(s."ATTR (SKU) REQUESTOR")             AS "Item Status_Obsolete Requestor",
        
        UPPER(pa."ATTR (PAR) BERRY")                AS "Item_Berry",
        UPPER(pa."ATTR (PAR) CARE")                 AS "Item_Care",
        UPPER(pa."ATTR (PAR) HEAT TRANSFER")        AS "Item_Heat Transfer",
        UPPER(pa."ATTR (PAR) OTHER")                AS "Item_Other",
        UPPER(pa."ATTR (PAR) PAD PRINT")            AS "Item_Pad Print",
        UPPER(pa."ATTR (PAR) PRODUCT CAT")          AS "Item_Product Category",
        UPPER(pa."ATTR (PAR) PRODUCT TYPE")         AS "Item_Product Type",
        UPPER(pa."ATTR (PAR) TRACKING")             AS "Item_Bin Tracking",
        UPPER(pa."ATTR (PAR) Z_BRAND")              AS "Item_Brand",
        UPPER(pa."ATTR (PAR) Z_CATEGORY")           AS "Item_Product Category Code",
        UPPER(pa."ATTR (PAR) Z_GENDER")             AS "Item_Gender",
        UPPER(pa."ATTR (PAR) Z_VERTICAL")           AS "Item_Vertical Code",
        UPPER(stkl.adv)                             AS "Item_Advertised Flag",
        UPPER(p65.prop_65)                          AS "Item_Prop 65",
        b."Item_ALT Key",
        b.id_loc                                    AS "Item_Location ID",
        UPPER(b.CHILD_ITEM_STATUS)                  AS "Item Status_Child Active Status",
        UPPER(pd.PARENT_ITEM_STATUS)                AS "Item Status_Parent Active Status",
        /* placeholder until sourced */
        /* Adjusted Parent Item Status via the CTE logic */
        UPPER(CASE 
            WHEN apit.cnt >= 1 THEN 'A'
            ELSE pd.PARENT_ITEM_STATUS
        END)                                        AS "Adj_Parent_Item_Status",
        ip.id_planner                               AS "ID_PLANNER"
        -- ip.PRIMARY_LOC_FLAG                         AS "PRIMARY_LOC_FLAG"

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
    LEFT JOIN Adjusted_Parent_Item_Status apit ON b.id_item = apit."Item ID_Child SKU"
    LEFT JOIN item_planner        ip  ON b.id_item = ip.id_item
    WHERE b.code_comm <> 'PAR';

