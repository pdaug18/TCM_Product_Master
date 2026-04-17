WITH 
/* ******************************************************
GET ALL BASE TABLES FIRST TO AVOID REPEATING SOURCE LOGIC
********************************************************/
    /* ========================================
    ITMMAS_BASE — Base item master
    CTE Key -> ID_ITEM
    ======================================== */
    ITMMAS_BASE AS (
        SELECT 
            TRIM(ib.id_item) AS id_item,
            ib.descr_1,
            ib.key_alt,
            ib.code_cat_prdt,
            ib.code_cat_cost,
            TRIM(ib.id_user_add) AS id_user_add,
            ib.date_add,
            TRIM(ib.id_user_chg) AS id_user_chg,
            ib.date_chg,
            ib.descr_2,
            ib.code_comm,
            TRIM(ib.id_loc) AS id_loc,
            ib.flag_stat_item,
            ib.ratio_stk_pur,
            ib.wgt_item,
            ib.ratio_stk_price,
            ib.code_um_price,
            ib.code_um_pur,
            ib.code_um_stk,
            ib.code_user_1_im,
            ib.code_user_2_im,
            ib.code_user_3_im,
            ib.type_cost
        FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib 
    ),

    /* ========================================
    IM_CMCD_ATTR_VALUE — attribute value base
    CTE Keys -> ID_ITEM + ID_ATTR (for pivot), CODE_COMM (to separate parent vs SKU attributes)
        group by ID_ITEM, ID_ATTR and get recent record using rowid 
    ======================================== */
    IM_CMCD_ATTR_VALUE AS (
        SELECT
            t.code_comm,
            t.id_item,
            t.id_attr,
            t.id_user_add,
            t.date_add,
            t.id_user_chg,
            t.date_chg,
            t.val_string_attr,
            t."rowid"
        FROM (
            SELECT
                av.code_comm,
                TRIM(av.id_item) AS id_item,
                TRIM(av.id_attr) AS id_attr,
                TRIM(av.id_user_add) AS id_user_add,
                av.date_add,
                TRIM(av.id_user_chg) AS id_user_chg,
                av.date_chg,
                av.val_string_attr,
                av."rowid",
                ROW_NUMBER() OVER (
                    PARTITION BY av.code_comm, TRIM(av.id_item), TRIM(av.id_attr)
                    ORDER BY av."rowid" DESC
                ) AS rn
            FROM BRONZE_DATA.TCM_BRONZE."IM_CMCD_ATTR_VALUE_Bronze" av
        ) t
        WHERE t.rn = 1
    ),

    /* ========================================
    ITMMAS_COST — item cost base
    CTE Key -> ID_ITEM (one row per item as cost data is at item level, but includes multiple cost fields and dates for current vs standard costs and accumulated vs non-accumulated costs)
        Take the MAX/latest rowid per item_id to ensure we are pulling the most recent cost record per item in case there are multiple records.
    ======================================== */
    ITMMAS_COST AS (
        SELECT
            TRIM(ic.id_item) AS id_item,
            TRIM(ic.id_loc_home) AS id_loc_home,
            ic.date_chg_cost_va,
            ic.date_accum_cost,
            ic.date_std_cost,
            ic.hr_labor_va_crnt,
            ic.hr_mach_va_crnt,
            ic.cost_matl_va_crnt,
            ic.cost_labor_va_crnt,
            ic.cost_vb_va_crnt,
            ic.cost_fb_va_crnt,
            ic.cost_outp_va_crnt,
            ic.cost_user_va_crnt,
            ic.hr_labor_va_std,
            ic.hr_mach_va_std,
            ic.cost_matl_va_std,
            ic.cost_labor_va_std,
            ic.cost_vb_va_std,
            ic.cost_fb_va_std,
            ic.cost_outp_va_std,
            ic.cost_user_va_std,
            ic.hr_labor_accum_crnt,
            ic.hr_mach_accum_crnt,
            ic.cost_matl_accum_crnt,
            ic.cost_labor_accum_crnt,
            ic.cost_vb_accum_crnt,
            ic.cost_fb_accum_crnt,
            ic.cost_outp_accum_crnt,
            ic.cost_user_accum_crnt,
            ic.hr_labor_accum_std,
            ic.hr_mach_accum_std,
            ic.cost_matl_accum_std,
            ic.cost_labor_accum_std,
            ic.cost_vb_accum_std,
            ic.cost_fb_accum_std,
            ic.cost_outp_accum_std,
            ic.cost_total_accum_crnt,
            ic.cost_total_accum_std,
            TRIM(ic.id_loc_src_cost_std) AS id_loc_src_cost_std,
            ic.cost_user_accum_std,
            ic.qty_run_std,
            ic.qty_run_std_frzn,
            TRIM(ic.id_user_chg) AS id_user_chg,
            ic.date_chg,
        FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_COST_Bronze" ic
    ),

    /* ========================================
    ITMMAS_DESCR — item descriptions base
        CTE Key -> ID_ITEM (multiple descriptions per item with different SEQ_DESCR values, but will use LISTAGG in parent_descriptions CTE to concatenate into single description field)
         Added ROWID to support picking latest description per item based on most recent effective date via window function in parent_descriptions CTE
         --! where seq_descr BETWEEN 800 AND 810 filter moved to parent_descriptions CTE to ensure we have all descriptions available for window function logic to pick latest description per item based on most recent effective date
    ======================================== */
    ITMMAS_DESCR AS (
        SELECT
            t.id_item,
            t.descr_addl,
            t.seq_descr,
            t."rowid"  --! Deduped to latest row per item/sequence using highest ROWID
        FROM (
            SELECT
                TRIM(id.id_item) AS id_item,
                id.descr_addl,
                id.seq_descr,
                id."rowid",
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(id.id_item), id.seq_descr
                    ORDER BY id."rowid" DESC
                ) AS rn
            FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_DESCR_Bronze" id
        ) t
        WHERE t.rn = 1
    ),

    /* ========================================
    ITMMAS_STK_LIST — stock list base
    ======================================== */
    ITMMAS_STK_LIST AS (
        SELECT
            TRIM(stkl.id_item) AS id_item,
            stkl.adv
        FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_STK_LIST_Bronze" stkl
    ),

    /* ========================================
    ITMMAS_VND — item vendor base
    ======================================== */
    ITMMAS_VND AS (
        SELECT
            TRIM(iv.id_item) as id_item,
            iv.flag_vnd_prim,
            TRIM(iv.id_vnd_payto) AS id_vnd_payto,
            TRIM(iv.id_vnd_ordfm) AS id_vnd_ordfm,
            TRIM(iv.id_item_vnd) AS id_item_vnd,
            COALESCE(
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'PRICE_QUOTE')::STRING,
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'QUOTE_PRICE')::STRING,
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'AMT_QUOTE')::STRING,
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'PRICE_VND')::STRING,
                GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(iv.*), 'PRICE')::STRING
            ) AS quote_price,
            iv.date_quote,
            iv.date_expire_quote,
            iv.qty_mult_ord,
            iv.code_um_vnd,
            -- iv.ratio_stk_pur,
            -- iv.comment_user
        FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_VND_Bronze" iv
    ),

    /* ========================================
    PRDSTR — product structure base
        CTE Keys -> ID_ITEM_COMP (to link to component item), ID_ITEM_PAR (to link to parent item for prop 65 logic), DATE_EFF_END and ROWID to pick latest effective record per component item for prop 65 logic
            --! Added ROWID to support picking latest record per component item based on most recent effective date via window function in prop_65_calc CTE
            -- get only ACTIVE flag_stat_item records only
            -- and getdate() < date_eff_end to only get currently effective records as of today to ensure we are capturing any items that may have been marked obsolete but still have a future effective end date in the system and should still be considered active for prop 65 logic until their effective end date has passed
            -- get active item from itmmas_base 
    ======================================== */
    PRDSTR AS (
        SELECT
            TRIM(p.id_item_par) as id_item_par,
             TRIM(p.id_item_comp) as id_item_comp,
             p.date_eff_end,
             p."rowid"
        FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" p
        LEFT JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
          ON p.id_item_comp = ib.id_item
        WHERE ib.flag_stat_item = 'A' 
          AND p.date_eff_end > CURRENT_DATE()
    ),

    /* ========================================
    TABLES_CODE_CAT_COST — cost category code base
    ======================================== */
    TABLES_CODE_CAT_COST AS (
        SELECT
            TRIM(cc.code_cat_cost) as code_cat_cost,
            cc.descr
        FROM BRONZE_DATA.TCM_BRONZE."TABLES_CODE_CAT_COST_Bronze" cc
    ),

    /* ========================================
    TABLES_CODE_CAT_PRDT — product category code base
    ======================================== */
    TABLES_CODE_CAT_PRDT AS (
        SELECT
            TRIM(pc.code_cat_prdt) AS code_cat_prdt,
            pc.descr,
            TRIM(pc.acct_id_sls) AS acct_id_sls,
            TRIM(pc.acct_loc_sls) AS acct_loc_sls,
            pc.acct_dept_sls,
            TRIM(pc.acct_id_cogs) AS acct_id_cogs,
            TRIM(pc.acct_loc_cogs) AS acct_loc_cogs,
            pc.acct_dept_cogs,
            TRIM(pc.acct_id_inv) AS acct_id_inv,
            TRIM(pc.acct_loc_inv) AS acct_loc_inv,
            -- pc.code_type_cust,
        FROM BRONZE_DATA.TCM_BRONZE."TABLES_CODE_CAT_PRDT_Bronze" pc
        WHERE pc.code_type_cust IS NULL  --! Added filter to only pull product category codes
    ),

/* *****************************************************
Silver Transformations and Logic Below
****************************************************** */
/* ========================================
   SKU ATTRIBUTES — keep ELSE '' as requested
   ======================================== */
sku_attributes AS (
    SELECT
        ib.id_item,
        MAX(COALESCE(av.id_user_add, ''))           AS "Employee_ID_User_Add_CMCD_Attribute",
        MAX(av.date_add)                            AS "Date_Added_CMCD_Attribute",
        MAX(COALESCE(av.id_user_chg, ''))           AS "Employee_ID_User_Change_CMCD_Attribute",
        MAX(av.date_chg)                            AS "Date_Last_Changed_CMCD_Attribute",
        MAX(CASE WHEN av.id_attr = 'ID_PARENT'   THEN av.val_string_attr ELSE '' END) AS "Item_ID_Parent_SKU",
        MAX(CASE WHEN av.id_attr = 'CERT_NUM'    THEN av.val_string_attr ELSE '' END) AS "Item_Certificate_Number",
        MAX(CASE WHEN av.id_attr = 'COLOR'       THEN av.val_string_attr ELSE '' END) AS "Item_Color",
        MAX(CASE WHEN av.id_attr = 'SIZE'        THEN av.val_string_attr ELSE '' END) AS "Item_Size",
        MAX(CASE WHEN av.id_attr = 'LENGTH'      THEN av.val_string_attr ELSE '' END) AS "Item_Length",
        MAX(CASE WHEN av.id_attr = 'TARIFF_CODE' THEN av.val_string_attr ELSE '' END) AS "Item_Item_Tariff_Code",
        MAX(CASE WHEN av.id_attr = 'UPC_CODE'    THEN av.val_string_attr ELSE '' END) AS "Item_UPC_Code",
        MAX(CASE WHEN av.id_attr = 'PFAS'        THEN av.val_string_attr ELSE '' END) AS "Item_PFAS",
        MAX(CASE WHEN av.id_attr = 'CLASS'       THEN av.val_string_attr ELSE '' END) AS "Item_Class",
        MAX(CASE WHEN av.id_attr = 'PPC'         THEN av.val_string_attr ELSE '' END) AS "Item_PPC",
        MAX(CASE WHEN av.id_attr = 'PRICE_LIST'  THEN av.val_string_attr ELSE '' END) AS "Item_Price_List",
        MAX(CASE WHEN av.id_attr = 'PRICE_LIST_DESC' THEN av.val_string_attr ELSE '' END) AS "Item_Price_List_Description",
        MAX(CASE WHEN av.id_attr = 'PRICE_LIST_ID' THEN av.val_string_attr ELSE '' END) AS "Item_Item_Price_List_ID",
        MAX(CASE WHEN av.id_attr = 'PRICE_LIST_PT' THEN av.val_string_attr ELSE '' END) AS "Item_Price_List_PT",
        MAX(CASE WHEN av.id_attr = 'PRIOR COMMODITY' THEN av.val_string_attr ELSE '' END) AS "Item_Commodity_Code_Prior",
        MAX(CASE WHEN av.id_attr = 'RBN_WC'      THEN av.val_string_attr ELSE '' END) AS "Item_Work_Center_Rubin",
        MAX(CASE WHEN av.id_attr = 'REASON'      THEN av.val_string_attr ELSE '' END) AS "Item_Status_Obsolete_Reason",
        MAX(CASE WHEN av.id_attr = 'REPLACEMENT' THEN av.val_string_attr ELSE '' END) AS "Item_Replaced_By",
        MAX(CASE WHEN av.id_attr = 'REQUESTOR'   THEN av.val_string_attr ELSE '' END) AS "Item_Status_Obsolete_Requestor",
        MAX(CASE WHEN av.id_attr = 'SF_XREF'     THEN av.val_string_attr ELSE '' END) AS "Item_SF_Xref",
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
    LEFT JOIN IM_CMCD_ATTR_VALUE av
           ON ib.id_item = av.id_item
          AND ib.code_comm = av.code_comm
    WHERE ib.code_comm <> 'PAR'
    GROUP BY ib.id_item
),

/* ========================================
   PARENT ATTRIBUTES — keep ELSE '' as requested
   ======================================== */
parent_attributes AS (
    SELECT
        av.id_item AS ID_PARENT,
        MAX(CASE WHEN av.id_attr = 'BERRY'        THEN av.val_string_attr ELSE '' END) AS "Item_Berry",
        MAX(CASE WHEN av.id_attr = 'CARE'         THEN av.val_string_attr ELSE '' END) AS "Item_Care",
        MAX(CASE WHEN av.id_attr = 'HEAT TRANSFER' THEN av.val_string_attr ELSE '' END) AS "Item_Heat_Transfer",
        MAX(CASE WHEN av.id_attr = 'OTHER'        THEN av.val_string_attr ELSE '' END) AS "Item_Other",
        MAX(CASE WHEN av.id_attr = 'PAD PRINT'    THEN av.val_string_attr ELSE '' END) AS "Item_Pad_Print",
        MAX(CASE WHEN av.id_attr = 'PRODUCT LINE' THEN av.val_string_attr ELSE '' END) AS "Item_Product_Line",
        MAX(CASE WHEN av.id_attr = 'PRODUCT TYPE' THEN av.val_string_attr ELSE '' END) AS "Item_Product_Type",
        MAX(CASE WHEN av.id_attr = 'PRODUCT_APP'  THEN av.val_string_attr ELSE '' END) AS "Item_Product_Application",
        MAX(CASE WHEN av.id_attr = 'TRACKING'     THEN av.val_string_attr ELSE '' END) AS "Item_Tracking",
        MAX(CASE WHEN av.id_attr = 'Z_BRAND'      THEN av.val_string_attr ELSE '' END) AS "Item_Brand",
        MAX(CASE WHEN av.id_attr = 'Z_CATEGORY'   THEN av.val_string_attr ELSE '' END) AS "Item_Product_Category",
        MAX(CASE WHEN av.id_attr = 'Z_GENDER'     THEN av.val_string_attr ELSE '' END) AS "Item_Gender",
    FROM IM_CMCD_ATTR_VALUE av
    WHERE av.code_comm = 'PAR'
    GROUP BY av.id_item
),

/* ========================================
   PARENT DESCRIPTIONS — no ORDER BY per request
   ======================================== */
parent_descriptions AS (
    SELECT
        ib.id_item,
        ib.FLAG_STAT_ITEM AS PARENT_ITEM_STATUS,
        LISTAGG(id.descr_addl, '') WITHIN GROUP (ORDER BY SEQ_DESCR) AS "Item_Description_Parent_SKU"
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" ib
    LEFT JOIN (
        SELECT *
        FROM ITMMAS_DESCR
        WHERE seq_descr BETWEEN 800 AND 810
    ) id
           ON ib.id_item = id.id_item
    WHERE ib.code_comm = 'PAR' 
    GROUP BY ib.id_item, ib.FLAG_STAT_ITEM
),

/* ========================================
   PROP 65 — unchanged logic
   ======================================== */
prop_65_calc AS (
    SELECT
        parents.id_item_par,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM (
                    SELECT
                        p2.id_item_par,
                        p2.id_item_comp
                    FROM PRDSTR p2
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY p2.id_item_comp
                        ORDER BY p2.date_eff_end DESC, p2."rowid" DESC
                    ) = 1
                ) latest_comp
                JOIN ITMMAS_DESCR d
                  ON latest_comp.id_item_comp = d.id_item
                WHERE latest_comp.id_item_par = parents.id_item_par
                  AND d.descr_addl LIKE '%PROP 65%'
            ) THEN 'Y'
            ELSE 'N'
        END AS prop_65
    FROM (
        SELECT DISTINCT id_item_par
        FROM PRDSTR
    ) parents
),

/* ========================================
   PRIMARY VENDOR — picks primary vendor per item (flag_vnd_prim = 'P' first)
   ======================================== */
primary_vendor AS (
    SELECT
        iv.id_item,
        iv.date_quote,
        iv.date_expire_quote,
        iv.id_vnd_ordfm,
        iv.id_vnd_payto,
        iv.id_item_vnd,
        iv.code_um_vnd
    FROM ITMMAS_VND iv
    WHERE iv.flag_vnd_prim = 'P'
),
/* ========================================
   SECONDARY VENDOR — rollup of secondary vendor attributes (flag_vnd_prim = 'S')
   ======================================== */
secondary_vendor AS (
    SELECT
        iv.id_item,
        LISTAGG(vp.name_vnd, ', ') WITHIN GROUP (ORDER BY iv.id_vnd_payto) AS "Item_Secondary_Vendors_Names",
        LISTAGG(iv.id_vnd_payto, ', ') WITHIN GROUP (ORDER BY iv.id_vnd_payto) AS "Item_Secondary_Vendor_IDs",
        LISTAGG(iv.quote_price, ', ') WITHIN GROUP (ORDER BY iv.id_vnd_payto) AS "Item_Secondary_Vendors_Quoted_Prices"
    FROM ITMMAS_VND iv
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."VENMAS_PAYTO_Bronze" vp
      ON iv.id_vnd_payto = TRIM(vp.id_vnd)
    WHERE iv.flag_vnd_prim = 'S'
    GROUP BY iv.id_item
),
/*  ========================================
   Adjusted Parent Item Status Logic
   =======================================*/
Adjusted_Parent_Item_Status AS (
    SELECT 
        adj."Item_ID_Child_SKU",
        count(*) AS cnt
    FROM (         
        SELECT 
            b.ID_ITEM AS "Item_ID_Child_SKU",
            pd.PARENT_ITEM_STATUS AS "Item Status_Parent Active Status",
            b.flag_stat_item AS "Item Status_Child Active Status"
        FROM ITMMAS_BASE b
        LEFT JOIN sku_attributes        s   ON b.id_item = s.id_item
        LEFT JOIN parent_descriptions   pd  ON s."Item_ID_Parent_SKU" = pd.id_item
        WHERE b.flag_stat_item = 'A' AND pd.PARENT_ITEM_STATUS = 'O'
    ) adj
    GROUP BY adj."Item_ID_Child_SKU" 
),

/* ========================================
   ITEM Primary Location
   ======================================== */
item_prim_loc AS (
    select distinct "Item_ID_Child_SKU",
    "Inventory_Location_ID",
    "Item_Secondary_Location_List"
    from SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER
    group by "Item_ID_Child_SKU", "Inventory_Location_ID", "Item_Secondary_Location_List"
)
        
    SELECT
        b.id_item                                  AS "Item_ID_Child_SKU",
        b.descr_1 || ' ' || b.descr_2              AS "Item_Description_Child_SKU",
        b.code_cat_cost                            AS "Item_Cost_Category_ID",
        b.id_user_add                              AS "Employee_ID_User_Add_ITMMAS_BASE",
        b.date_add                                 AS "Date_Added_ITMMAS_BASE",
        b.id_user_chg                              AS "Employee_ID_User_Change_ITMMAS_BASE",
        b.date_chg                                 AS "Date_Changed_ITMMAS_BASE",
        UPPER(COALESCE(cc.descr, 'INVALID COST CATEGORY')) AS "Item_Cost_Category",
        UPPER(b.code_cat_prdt)                     AS "Item_Vertical_Code",
        UPPER(COALESCE(b.code_cat_prdt || ' - ' || pc.descr, 'Invalid Product Vertical')) AS "Item_Vertical",
        b.code_comm                                AS "Item_Commodity_Code",
        b.ratio_stk_pur                            AS "Ratio_Purchase_to_Stock",
        pc.acct_id_sls                             AS "Item_Accounting_Sales_ID",
        pc.acct_loc_sls                            AS "Item_Accounting_Sales_Location",
        pc.acct_dept_sls                           AS "Item_Accounting_Sales_Department",
        pc.acct_id_cogs                            AS "Item_Accounting_COGS_ID",
        pc.acct_loc_cogs                           AS "Item_Accounting_COGS_Location",
        pc.acct_dept_cogs                          AS "Item_Accounting_COGS_Department",
        pc.acct_id_inv                             AS "Item_Accounting_Invoicing_ID",
        pc.acct_loc_inv                            AS "Item_Accounting_Invoicing_Location",
        ic.hr_labor_va_crnt                        AS "Hours_Labor_Current",
        ic.hr_mach_va_crnt                         AS "Hours_Machine_Current",
        ic.hr_labor_va_std                         AS "Hours_Labor_Standard",
        ic.hr_mach_va_std                          AS "Hours_Machine_Standard",
        ic.hr_labor_accum_crnt                     AS "Hours_Labor_Accumulated_Current",
        ic.hr_mach_accum_crnt                      AS "Hours_Machine_Accumulated_Current",
        ic.hr_labor_accum_std                      AS "Hours_Labor_Accumulated_Standard",
        ic.hr_mach_accum_std                       AS "Hours_Machine_Accumulated_Standard",
        ic.COST_MATL_ACCUM_CRNT                     AS "Cost_Material_Accumulated_Current",
        ic.COST_MATL_ACCUM_STD                      AS "Cost_Material_Accumulated_Standard",
        ic.hr_labor_va_crnt * 60                    AS "Minutes_Labor_Current",
        ic.hr_mach_va_crnt * 60                     AS "Minutes_Machine_Current",
        ic.hr_labor_va_std * 60                     AS "Minutes_Labor_Standard",
        ic.hr_mach_va_std * 60                      AS "Minutes_Machine_Standard",
        ic.hr_labor_accum_crnt * 60                 AS "Minutes_Labor_Accumulated_Current",
        ic.hr_mach_accum_crnt * 60                  AS "Minutes_Machine_Accumulated_Current",
        ic.hr_labor_accum_std * 60                  AS "Minutes_Labor_Accumulated_Standard",
        ic.hr_mach_accum_std * 60                   AS "Minutes_Machine_Accumulated_Standard",
        ic.COST_FB_VA_CRNT                          AS "Cost_Freight_Current",
        ic.COST_FB_VA_STD                           AS "Cost_Freight_Standard",
        ic.COST_MATL_VA_CRNT                        AS "Cost_Material_Current",
        ic.COST_MATL_VA_STD                         AS "Cost_Material_Standard",
        ic.COST_LABOR_VA_CRNT                       AS "Cost_Labor_Current",
        ic.COST_LABOR_VA_STD                        AS "Cost_Labor_Standard",
        ic.COST_VB_ACCUM_CRNT                       AS "Cost_Variable_Burden_Accumulated_Current",
        ic.COST_VB_ACCUM_STD                        AS "Cost_Variable_Burden_Accumulated_Standard",
        ic.COST_FB_ACCUM_CRNT                       AS "Cost_Freight_Accumulated_Current",
        ic.COST_FB_ACCUM_STD                        AS "Cost_Freight_Accumulated_Standard",
        ic.COST_OUTP_VA_CRNT                        AS "Cost_Outside_Service_Current",
        ic.COST_USER_VA_CRNT                        AS "Cost_User_Current",
        ic.COST_OUTP_VA_STD                         AS "Cost_Outside_Service_Standard",
        ic.COST_USER_VA_STD                         AS "Cost_User_Standard",
        ic.COST_LABOR_ACCUM_CRNT                    AS "Cost_Labor_Accumulated_Current",
        ic.COST_LABOR_ACCUM_STD                     AS "Cost_Labor_Accumulated_Standard",
        ic.COST_OUTP_ACCUM_CRNT                     AS "Cost_Outside_Service_Accumulated_Current",
        ic.COST_OUTP_ACCUM_STD                      AS "Cost_Outside_Service_Accumulated_Standard",
        ic.COST_USER_ACCUM_CRNT                     AS "Cost_User_Accumulated_Current",
        ic.COST_USER_ACCUM_STD                      AS "Cost_User_Accumulated_Standard",
        ic.COST_TOTAL_ACCUM_CRNT                    AS "Cost_Total_Current",
        ic.COST_TOTAL_ACCUM_STD                     AS "Cost_Total_Standard",
        ic.COST_VB_VA_CRNT                          AS "Cost_Variable_Burden_Current",
        ic.COST_VB_VA_STD                           AS "Cost_Variable_Burden_Standard",
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
        END AS "Cost_Total_Current_Calculated",
        ic.id_loc_src_cost_std                      AS "Cost_Location_ID_Standard_Cost_Source",
        b.type_cost                                 AS "Item_Cost_Type",
        ic.date_accum_cost                          AS "Date_Cost_Accumulated",
        ic.date_chg_cost_va                         AS "Date_Cost_Changed",
        ic.date_std_cost                            AS "Date_Cost_Standard",
        ic.qty_run_std                              AS "Item_Standard_Run_Quantity",
        ic.qty_run_std_frzn                         AS "Item_Standard_Run_Quantity_Frozen",
        b.wgt_item                                  AS "Item_Weight",
        b.ratio_stk_price                           AS "Ratio_Price_to_Stock",
        b.code_um_price                             AS "Unit_of_Measure_Price",
        b.code_um_pur                               AS "Unit_of_Measure_Purchase",
        b.code_um_stk                               AS "Unit_of_Measure_Stock",
        b.code_user_1_im                            AS "Item_Code_User_1",
        b.code_user_2_im                            AS "Item_Code_User_2",
        b.code_user_3_im                            AS "Item_Code_User_3",
        pv.date_quote                               AS "Date_Vendor_Quote_Received",
        pv.date_expire_quote                        AS "Date_Vendor_Quote_Expired",
        pv.id_vnd_ordfm                             AS "Item_Primary_Vendor_Order_From_ID",
        pv.id_vnd_payto                             AS "Item_Primary_Vendor_Pay_To_ID",
        pv.id_item_vnd                              AS "Item_Primary_Vendor_Item_ID",
        sv."Item_Secondary_Vendors_Names"           AS "Item_Secondary_Vendors_Names",
        sv."Item_Secondary_Vendor_IDs"             AS "Item_Secondary_Vendor_IDs",
        sv."Item_Secondary_Vendors_Quoted_Prices"  AS "Item_Secondary_Vendors_Quoted_Prices",
        s."Item_ID_Parent_SKU"                     AS "Item_ID_Parent_SKU",
        UPPER(CASE
            WHEN UPPER(COALESCE(b.code_cat_prdt || ' - ' || pc.descr, 'INVALID PRODUCT CATEGORY')) ILIKE '%FABRIC%' AND pd."Item_Description_Parent_SKU" IS NULL
            THEN b.descr_1 || ' ' || b.descr_2
            ELSE COALESCE(pd."Item_Description_Parent_SKU", 'MISSING DESCRIPTION - UPDATE TCM')
        END)                                        AS "Item_Description_Parent_SKU",

        UPPER(s."Item_Certificate_Number")          AS "Item_Certificate_Number",
        UPPER(s."Item_Color")                       AS "Item_Color",
        UPPER(s."Item_Size")                        AS "Item_Size",
        UPPER(s."Item_Length")                      AS "Item_Length",
        UPPER(s."Item_Item_Tariff_Code")            AS "Item_Tariff_Code",
        UPPER(s."Item_UPC_Code")                    AS "Item_UPC_Code",
        UPPER(s."Item_PFAS")                        AS "Item_PFAS",
        UPPER(s."Item_Class")                       AS "Item_Class",
        UPPER(s."Item_PPC")                         AS "Item_PPC",
        UPPER(s."Item_Price_List")                  AS "Item_Price_List",
        UPPER(s."Item_Price_List_Description")      AS "Item_Price_List_Description",
        UPPER(s."Item_Item_Price_List_ID")          AS "Item_Price_List_ID",
        UPPER(s."Item_Price_List_PT")               AS "Item_Price_List_PT",
        UPPER(s."Item_Commodity_Code_Prior")        AS "Item_Commodity_Code_Prior",
        UPPER(s."Item_Work_Center_Rubin")           AS "Item_Work_Center_Rubin",
        UPPER(s."Item_Status_Obsolete_Reason")      AS "Item_Status_Obsolete_Reason",
        UPPER(s."Item_Replaced_By")                 AS "Item_Replaced_By",
        UPPER(s."Item_Status_Obsolete_Requestor")   AS "Item_Status_Obsolete_Requestor",
        UPPER(s."Item_SF_Xref")                     AS "Item_SF_Xref",
        s."Employee_ID_User_Add_CMCD_Attribute"     AS "Employee_ID_User_Add_CMCD_Attribute",
        s."Date_Added_CMCD_Attribute"               AS "Date_Added_CMCD_Attribute",
        s."Employee_ID_User_Change_CMCD_Attribute"  AS "Employee_ID_User_Change_CMCD_Attribute",
        s."Date_Last_Changed_CMCD_Attribute"        AS "Date_Last_Changed_CMCD_Attribute",
        
        UPPER(pa."Item_Berry")                     AS "Item_Berry",
        UPPER(pa."Item_Care")                      AS "Item_Care",
        UPPER(pa."Item_Heat_Transfer")             AS "Item_Heat_Transfer",
        UPPER(pa."Item_Other")                     AS "Item_Other",
        UPPER(pa."Item_Pad_Print")                 AS "Item_Pad_Print",
        UPPER(pa."Item_Product_Line")              AS "Item_Product_Line",
        UPPER(pa."Item_Product_Type")              AS "Item_Product_Type",
        UPPER(pa."Item_Product_Application")       AS "Item_Product_Application",
        UPPER(pa."Item_Tracking")                  AS "Item_Tracking",
        UPPER(pa."Item_Brand")                     AS "Item_Brand",
        UPPER(pa."Item_Product_Category")          AS "Item_Product_Category",
        UPPER(pa."Item_Gender")                    AS "Item_Gender",
        UPPER(stkl.adv)                             AS "Item_Advertised_Flag",
        UPPER(p65.prop_65)                          AS "Item_Prop_65",
        b.key_alt                                   AS "Item_ALT_Key",
        ipr."Inventory_Location_ID"                 AS "Item_Primary_Location",
        ipr."Item_Secondary_Location_List"          AS "Item_Secondary_Locations",
        UPPER(b.flag_stat_item)                     AS "Item_Status_Child_Active_Status",
        UPPER(pd.PARENT_ITEM_STATUS)                AS "Item_Status_Parent_Active_Status"

    FROM ITMMAS_BASE b
    LEFT JOIN ITMMAS_COST ic on b.id_item = ic.id_item 
    LEFT JOIN TABLES_CODE_CAT_COST cc ON b.code_cat_cost = cc.code_cat_cost
    LEFT JOIN TABLES_CODE_CAT_PRDT pc ON b.code_cat_prdt = pc.code_cat_prdt
    LEFT JOIN ITMMAS_STK_LIST stkl on b.id_item = stkl.id_item 
    LEFT JOIN sku_attributes     s   ON b.id_item = s.id_item
    LEFT JOIN parent_attributes  pa  ON s."Item_ID_Parent_SKU" = pa.ID_PARENT
    LEFT JOIN parent_descriptions pd ON s."Item_ID_Parent_SKU" = pd.id_item
    LEFT JOIN prop_65_calc       p65 ON s."Item_ID_Parent_SKU" = p65.id_item_par
    LEFT JOIN primary_vendor     pv  ON b.id_item = pv.id_item
    LEFT JOIN secondary_vendor   sv  ON b.id_item = sv.id_item
    LEFT JOIN item_prim_loc      ipr ON b.id_item = ipr."Item_ID_Child_SKU"
    LEFT JOIN Adjusted_Parent_Item_Status apit ON b.id_item = apit."Item_ID_Child_SKU"
    WHERE b.code_comm <> 'PAR'
    ;