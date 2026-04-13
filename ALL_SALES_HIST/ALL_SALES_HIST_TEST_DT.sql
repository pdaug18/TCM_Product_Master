CREATE OR REPLACE DYNAMIC TABLE BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST_TEST_DT
    TARGET_LAG = 'DOWNSTREAM'    -- Adjust to desired freshness (e.g., '30 minutes', 'DOWNSTREAM')
    WAREHOUSE  = ELT_DEFAULT  -- Replace with your actual warehouse name
AS
/* =========================
   KIT COMPONENT STOCK LOGIC
   ========================= */
WITH ACTIVE_KIT_COMPONENTS AS (
    SELECT
        ps.id_item_par,
        ps.id_item_comp AS active_component
    FROM bronze_data.tcm_bronze."PRDSTR_Bronze" ps
    WHERE ps.date_eff_end > CURRENT_DATE()
),
KIT_COMPONENT_STOCK AS (
    SELECT
        akc.id_item_par,
        akc.active_component,
        IFF(stk.id_item IS NOT NULL, 1, 0) AS is_component_stock
    FROM ACTIVE_KIT_COMPONENTS akc
    LEFT JOIN bronze_data.tcm_bronze."ITMMAS_STK_LIST_Bronze" stk
        ON LTRIM(RTRIM(akc.active_component)) = LTRIM(RTRIM(stk.id_item))
),
KIT_STOCK_SUMMARY AS (
    SELECT
        id_item_par,
        MIN(is_component_stock) AS kit_all_components_stock,   -- 1 only if all active components are stock
        COUNT(*) AS active_component_count
    FROM KIT_COMPONENT_STOCK
    GROUP BY id_item_par
),

/* =========================
   YOUR EXISTING BASE CTE
   ========================= */
BASE AS (
    SELECT
        sls."OrderID" AS "OrderID",
        sls."LineNumber" AS "LineNumber",
        sls."Ordered Date" AS "Ordered Date",
        sls."Stock Flag" AS "Item_Stock Flag",
        sls."InvoiceID" AS "InvoiceID",
        sls."Freight_Cost" AS "Freight_Cost",
        sls."Sales_Tax" AS "Sales_Tax",
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
        st.code_user_2_ar AS "Customer Attribute Flag",
        sls."Ship to Address" AS "Ship to Address",
        sls."Ship to City" AS "Ship to City",
        sls."Ship to State" AS "Ship to State",
        sls."Ship to Zip Code" AS "Ship to Zip Code",
        cst.name_cust AS "Ship to Customer Name",
        sls.seq_shipto AS "Ship to #",
        sls."Ship to Country" AS "Ship to Country",
        sls."Sales Rep ID" AS "TCM Sales Rep ID",
        sls."Product ID/SKU" AS "Product ID/SKU",
        mpt."Item_ID_Child_SKU",
        mpt."Item_Description_Child_SKU",
        mpt."Item_Cost_Category_ID",
        mpt."Item_Cost_Category",
        -- mpt."PRODUCT_VERTICAL",
        -- COALESCE(mpt."PRDT CAT DESCR", 'INVALID PRODUCT CATEGORY') AS "PRDT CAT DESCR",
        sls."TCM Historical Vertical",
        mpt."Item_Vertical",
        -- mpt."Item_Product_Category",
        mpt."Item_ID_Parent_SKU",
        COALESCE(mpt."Item_Description_Parent_SKU", 'MISSING DESCRIPTION - UPDATE TCM') AS "PARENT DESCRIPTION",
        mpt."Item_Certificate_Number",
        mpt."Item_Color",
        mpt."Item_Size",
        mpt."Item_Length",
        mpt."Item_Tariff_Code",
        mpt."Item_UPC_Code",
        mpt."Item_PFAS",
        mpt."Item_Class",
        mpt."Item_PPC",
        mpt."Item_Commodity_Code_Prior",
        mpt."Item_Work_Center_Rubin",
        mpt."Item_Status_Obsolete_Reason",
        mpt."Item_Replaced_By",
        mpt."Item_Status_Obsolete_Requestor",
        mpt."Item_Berry",
        mpt."Item_Care",
        mpt."Item_Heat_Transfer",
        mpt."Item_Other",
        mpt."Item_Pad_Print",
        mpt."Item_Product_Category",
        mpt."Item_Product_Type",
        mpt."Item_Bin_Tracking",
        mpt."Item_Brand",
        mpt."Item_Product_Category_Code",
        mpt."Item_Gender",
        -- mpt."Item_Vertical_Code",
        mpt."Item_Advertised_Flag",
        mpt."Cost_Material_Accumulated_Current",
        mpt."Cost_Material_Accumulated_Standard",
        mpt."Cost_Freight_Current",
        mpt."Cost_Freight_Standard",
        mpt."Cost_Material_Current", 
        mpt."Cost_Labor_Current",
        mpt."Cost_Material_Standard",
        mpt."Cost_Labor_Standard",
        mpt."Cost_Outside_Service_Current",
        mpt."Cost_User_Field_Current",
        mpt."Cost_Outside_Service_Standard",
        mpt."Cost_User_Field_Standard",
        mpt."Cost_Total_Current",
        mpt."Cost_Total_Standard",
        mpt."Cost_Variable_Burden_Current",
        mpt."Cost_Variable_Burden_Standard",
        mpt."Cost_Material_Current_Calculated",
        mpt."Cost_Labor_Current_Calculated",
        mpt."Cost_Total_Current_Calculated",
        il.id_planner AS "Item_Planned_Classification",
        mpt."Item_Prop_65",
        sls."INVOICE DATE" AS "INVOICE DATE",
        mpt."Item_ALT_Key",
        mpt."Item_Status_Child_Active_Status",
        mpt."Item_Status_Parent_Active_Status",
        r.level_rop AS "Item_Inventory_Reorder_Point",
        sls."LineNumber" AS "Sales Transaction Line ID",
        sls."Promise Date" AS "Promise Date",

        /* Working Days: Invoice Date - Order Date (exclusive of Order Date) */
        CASE
          WHEN sls."Ordered Date" IS NULL OR sls."INVOICE DATE" IS NULL THEN NULL
          WHEN sls."INVOICE DATE"::DATE < sls."Ordered Date"::DATE THEN NULL
          ELSE (
            SELECT COUNT(*)
            FROM GOLD_DATA.DIM.DIM_CALENDAR c
            WHERE c.CALENDAR_DATE BETWEEN DATEADD(day, 1, sls."Ordered Date"::DATE)
                                      AND sls."INVOICE DATE"::DATE
              AND c.IS_WEEKDAY = TRUE
              AND c.IS_HOLIDAY = FALSE
          )
        END AS "Working Days Invoice Minus Order",

        /* Working Days: Promise Date - Order Date (exclusive of Order Date) */
        CASE
          WHEN sls."Ordered Date" IS NULL OR sls."Promise Date" IS NULL THEN NULL
          WHEN sls."Promise Date"::DATE < sls."Ordered Date"::DATE THEN NULL
          ELSE (
            SELECT COUNT(*)
            FROM GOLD_DATA.DIM.DIM_CALENDAR c
            WHERE c.CALENDAR_DATE BETWEEN DATEADD(day, 1, sls."Ordered Date"::DATE)
                                      AND sls."Promise Date"::DATE
              AND c.IS_WEEKDAY = TRUE
              AND c.IS_HOLIDAY = FALSE
          )
        END AS "Working Days Promise Minus Order",

        sls."Unit Quantity" AS "Unit Quantity",
        sls."Total Price" AS "Total Price",
        sls."Unit Cost" AS "Unit Cost",
        sr.name_slsrep AS "Sales Rep Name",
        sls.id_loc AS "Item_Location ID",
        sls."Shipping Location ID",
        sls.id_loc || ' - ' || tl.DESCR AS "LOC DESCR",
        sls."Calendar Date" AS "CALENDAR DATE",
        sls."Booking Type Table" AS "Booking Type Table"

    FROM GOLD_DATA.TCM_GOLD.BOOKINGS_INVOICE_REPORTING sls
    LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE mpt
        ON sls."Product ID/SKU" = mpt."Item_ID_Child_SKU"
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."TABLES_LOC_Bronze" tl
        ON sls.id_loc = tl.id_loc
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."TABLES_SLSREP_Bronze" sr
        ON LTRIM(sls."Sales Rep ID") = LTRIM(sr.id_slsrep)
    LEFT JOIN (
        SELECT
            id_item,
            id_loc_home,
            level_rop,
            ROW_NUMBER() OVER (
                PARTITION BY id_item, id_loc_home
                ORDER BY "rowid" DESC
            ) AS rn
        FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_REORD_Bronze"
    ) r
        ON sls."Product ID/SKU" = r.id_item
       AND sls.id_loc = r.id_loc_home
       AND r.rn = 1
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Dynamic" il
        ON sls."Product ID/SKU" = il.id_item
       AND sls.id_loc = il.id_loc
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."CUSMAS_SHIPTO_Bronze" cst
        ON sls."Customer ID" = cst.id_cust
       AND sls.seq_shipto = cst.seq_shipto
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."CUSMAS_SOLDTO_Bronze" st
        ON LTRIM(sls."Customer ID") = LTRIM(st.id_cust)
    LEFT JOIN BRONZE_DATA.TCM_BRONZE."CUST_GROUP_CODE_Bronze" gc
        ON st.code_user_3_ar = gc.group_code
    WHERE sls."Calendar Date" > '2014-01-01'::DATE
      AND st.CODE_CUST <> 'IC'
),

/* =========================
   Compute Delivery Group ONCE here
   ========================= */
ENRICHED AS (
    SELECT
        b.*,

        CASE
          WHEN b."Item_Stock Flag" = 'S'
           AND b."Item_Advertised_Flag" = 'Y'
           AND b."Item_Inventory_Reorder_Point" > 1
           AND (b."Working Days Promise Minus Order" <= 6 OR b."Working Days Invoice Minus Order" <= 2)
            THEN 'Advertised In-Stock'

          WHEN (
            (b."Item_Stock Flag" = 'S'
             AND b."Item_Advertised_Flag" = 'Y'
             AND b."Item_Inventory_Reorder_Point" > 1)
            OR b."Item_Planned_Classification" = 'AS'
           )
           AND (b."Working Days Promise Minus Order" > 6 AND b."Working Days Invoice Minus Order" > 2)
            THEN 'Advertised Out-of-Stock'

          WHEN b."Item_Planned_Classification" = 'AS'
           AND (b."Working Days Promise Minus Order" <= 6 OR b."Working Days Invoice Minus Order" <= 2)
            THEN 'In-Stock Altered Stock'

          WHEN b."Item_Stock Flag" = 'S'
           AND b."Item_Inventory_Reorder_Point" = 1
            THEN 'Standard'

          WHEN b."Item_Planned_Classification" = 'KT'
            THEN 'Kits'

          WHEN b."Item_Stock Flag" = 'N'
           AND b."Item_Planned_Classification" NOT IN ('AS','KT')
            THEN 'Made To Order'

         WHEN (
               b."Item_Product_Category_Code" IN ('06', '26', '60')
               OR b."Item_Planned_Classification" = 'RM'
              )
    
           THEN 'Fabrics and Raw Materials'

         WHEN b."Item_Stock Flag" = 'S' 
           THEN 'Other Stock'

         ELSE 'Other'
       END AS "Delivery Group"

    FROM BASE b
)

SELECT
    /* ===== Base columns in exact view order ===== */
    e."OrderID",
    e."LineNumber",
    e."Ordered Date",
    e."Item_Stock Flag" AS "Item_Stock Flag",
    e."InvoiceID",
    e."Freight_Cost",
    e."Sales_Tax",
    e."Customer Name",
    e."Customer ID",
    e."Customer Type",
    e."CUST/GROUP CODE",
    e."CUST/GROUP NAME",
    e."Sold to Address",
    e."Sold to City",
    e."Sold to State",
    e."Sold to Zip Code",
    e."Sold to Country",
    e."Customer Attribute Flag",
    e."Ship to Address",
    e."Ship to City",
    e."Ship to State",
    e."Ship to Zip Code",
    e."Ship to Customer Name",
    e."Ship to #",
    e."Ship to Country",
    e."TCM Sales Rep ID",
    e."Item_ID_Child_SKU" AS "Item_ID_Child_SKU",
    e."Item_Description_Child_SKU" AS "Item_Description_Child_SKU",
    e."Item_Cost_Category_ID",
    e."Item_Cost_Category",
    -- e."Item_Vertical_Code",
    -- e."PRDT CAT DESCR", -- removed from MASTER_PRODUCT_TABLE
    e."TCM Historical Vertical",
    e."Item_Vertical" AS "Item_Vertical",
    -- e."CATEGORY (Calc)", -- removed from MASTER_PRODUCT_TABLE
    e."Item_ID_Parent_SKU" AS "Item_ID_Parent_SKU",
    e."PARENT DESCRIPTION" AS "Item_Description_Parent_SKU",
    e."Item_Certificate_Number" AS "Item_Certificate_Number",
    e."Item_Color" AS "Item_Color",
    e."Item_Size" AS "Item_Size",
    e."Item_Length" AS "Item_Length",
    e."Item_Tariff_Code" AS "Item_Tariff_Code",
    e."Item_UPC_Code" AS "Item_UPC_Code",
    e."Item_PFAS" AS "Item_PFAS",
    e."Item_Class" AS "Item_Class",
    e."Item_PPC" AS "Item_PPC",
    e."Item_Commodity_Code_Prior" AS "Item_Commodity_Code_Prior",
    e."Item_Work_Center_Rubin" AS "Item_Work_Center_Rubin",
    e."Item_Status_Obsolete_Reason" AS "Item_Status_Obsolete_Reason",
    e."Item_Replaced_By" AS "Item_Replaced_By",
    e."Item_Status_Obsolete_Requestor" AS "Item_Status_Obsolete_Requestor",
    e."Item_Berry" AS "Item_Berry",
    e."Item_Care" AS "Item_Care",
    e."Item_Heat_Transfer" AS "Item_Heat_Transfer",
    e."Item_Other" AS "Item_Other",
    e."Item_Pad_Print" AS "Item_Pad_Print",
    e."Item_Product_Category",
    e."Item_Product_Type" AS "Item_Product_Type",
    e."Item_Bin_Tracking" AS "Item_Bin_Tracking",
    e."Item_Brand" AS "Item_Brand",
    e."Item_Product_Category_Code",
    e."Item_Gender" AS "Item_Gender",
    -- e."Item_Vertical_Code", -- duplicate of selected Item_Vertical_Code above
    e."Item_Advertised_Flag" AS "Item_Advertised_Flag",
    e."Cost_Material_Accumulated_Current",
    e."Cost_Material_Accumulated_Standard",
    e."Cost_Freight_Current",
    e."Cost_Freight_Standard",
    e."Cost_Material_Current", 
    e."Cost_Labor_Current",
    e."Cost_Material_Standard",
    e."Cost_Labor_Standard",
    e."Cost_Outside_Service_Current" AS "Cost_Outside_Service_Current",
    e."Cost_User_Field_Current" AS "Cost_User_Field_Current",
    e."Cost_Outside_Service_Standard" AS "Cost_Outside_Service_Standard",
    e."Cost_User_Field_Standard" AS "Cost_User_Field_Standard",
    e."Cost_Total_Current",
    e."Cost_Total_Standard",
    e."Cost_Variable_Burden_Current" AS "Cost_Variable_Burden_Current",
    e."Cost_Variable_Burden_Standard" AS "Cost_Variable_Burden_Standard",
    e."Cost_Material_Current_Calculated",
    e."Cost_Labor_Current_Calculated",
    e."Cost_Total_Current_Calculated",
    e."Item_Planned_Classification" AS "Item_Planned_Classification",
    e."Item_Prop_65" AS "Item_Prop_65",
    e."INVOICE DATE",
    e."Item_ALT_Key" AS "Item_ALT_Key",
    e."Item_Status_Child_Active_Status" AS "Item_Status_Child_Active_Status",
    e."Item_Status_Parent_Active_Status" AS "Item_Status_Parent_Active_Status",
    e."Item_Inventory_Reorder_Point" AS "Item_Inventory_Reorder_Point",
    e."Sales Transaction Line ID",
    e."Promise Date",
    e."Working Days Invoice Minus Order",
    e."Working Days Promise Minus Order",
    e."Unit Quantity",
    e."Total Price",
    e."Unit Cost",
    e."Sales Rep Name",
    e."Item_Location ID" AS "Item_Location ID",
    e."Shipping Location ID",
    e."LOC DESCR",
    e."CALENDAR DATE",
    e."Booking Type Table",
    e."Delivery Group" AS "Item Delivery_Delivery Group",

    /* =======================
       SLA Flags (cumulative)
       ======================= */
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 0 THEN 'Y' ELSE 'N' END AS "Shipped_Same_Day",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 1 THEN 'Y' ELSE 'N' END AS "Shipped_Next_Day",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 2 THEN 'Y' ELSE 'N' END AS "Shipped_2_Days",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 5 THEN 'Y' ELSE 'N' END AS "Shipped_1_Week",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 7 THEN 'Y' ELSE 'N' END AS "Shipped_7_Days",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 10 THEN 'Y' ELSE 'N' END AS "Shipped_10_Days",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 15 THEN 'Y' ELSE 'N' END AS "Shipped_15_Days",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 20 THEN 'Y' ELSE 'N' END AS "Shipped_20_Days",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 25 THEN 'Y' ELSE 'N' END AS "Shipped_25_Days",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 30 THEN 'Y' ELSE 'N' END AS "Shipped_30_Days",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 35 THEN 'Y' ELSE 'N' END AS "Shipped_35_Days",
    CASE WHEN e."Working Days Invoice Minus Order" - 1 <= 40 THEN 'Y' ELSE 'N' END AS "Shipped_40_Days",
    CASE WHEN e."Working Days Invoice Minus Order" IS NOT NULL THEN 'Y' ELSE 'N' END AS "Shipped_Greater_Than_40",

    /* =======================
       Actual lead time bucket
       ======================= */
    CASE
      WHEN e."Working Days Invoice Minus Order" IS NULL THEN NULL
      WHEN e."Working Days Invoice Minus Order" - 1 <= 0 THEN 'Shipped_Same_Day'
      WHEN e."Working Days Invoice Minus Order" - 1 = 1 THEN 'Shipped_Next_Day'
      WHEN e."Working Days Invoice Minus Order" - 1 = 2 THEN 'Shipped_2_Days'
      WHEN e."Working Days Invoice Minus Order" - 1 BETWEEN 3 AND 5 THEN 'Shipped_1_Week'
      WHEN e."Working Days Invoice Minus Order" - 1 BETWEEN 6 AND 7 THEN 'Shipped_7_Days'
      WHEN e."Working Days Invoice Minus Order" - 1 BETWEEN 8 AND 10 THEN 'Shipped_10_Days'
      WHEN e."Working Days Invoice Minus Order" - 1 BETWEEN 11 AND 15 THEN 'Shipped_15_Days'
      WHEN e."Working Days Invoice Minus Order" - 1 BETWEEN 16 AND 20 THEN 'Shipped_20_Days'
      WHEN e."Working Days Invoice Minus Order" - 1 BETWEEN 21 AND 25 THEN 'Shipped_25_Days'
      WHEN e."Working Days Invoice Minus Order" - 1 BETWEEN 26 AND 30 THEN 'Shipped_30_Days'
      WHEN e."Working Days Invoice Minus Order" - 1 BETWEEN 31 AND 35 THEN 'Shipped_35_Days'
      WHEN e."Working Days Invoice Minus Order" - 1 BETWEEN 36 AND 40 THEN 'Shipped_40_Days'
      ELSE 'Shipped_Greater_Than_40'
    END AS "Delivery Lead Time Range",

    /* Delivery Group lands correctly now */
    -- e."Item Delivery_Delivery Group" AS "Delivery Group",

    /* =======================
       Fillable (BUSINESS RULES)
       ======================= */
    CASE
      WHEN e."Delivery Group" IN ('Advertised In-Stock', 'In-Stock Altered Stock')
        THEN 'Fillable'

      WHEN e."Delivery Group" = 'Advertised Out-of-Stock'
        THEN 'Unfillable'

      WHEN e."Delivery Group" = 'Standard' THEN
        CASE
          WHEN e."Working Days Promise Minus Order" <= 10 THEN 'Fillable'
          ELSE 'Unfillable'
        END

    WHEN e."Delivery Group" = 'Other Stock' THEN
        CASE
          WHEN e."Working Days Promise Minus Order" <= 6 THEN 'Fillable'
          ELSE 'Unfillable'
        END

    WHEN e."Delivery Group" = 'Fabrics and Raw Materials' THEN
        CASE
    /* Stock item */
            WHEN e."Item_Stock Flag" = 'S' AND e."Working Days Promise Minus Order" <= 6 THEN 'Fillable'

    /* Not stock item */
            WHEN e."Item_Stock Flag" <> 'S'  AND e."Working Days Promise Minus Order" <= 10  THEN 'Fillable'

            ELSE 'Unfillable'
        END
      WHEN e."Delivery Group" = 'Kits' THEN
        CASE
          /* If no active components are found, default to Unfillable (safer) */
          WHEN COALESCE(kss.active_component_count, 0) = 0 THEN 'Unfillable'

          /* Components are stock + promise <= 6 => Fillable */
          WHEN kss.kit_all_components_stock = 1 AND e."Working Days Promise Minus Order" <= 6 THEN 'Fillable'

          /* Components NOT all stock + promise <= 10 => Fillable */
          WHEN kss.kit_all_components_stock = 0 AND e."Working Days Promise Minus Order" <= 10  THEN 'Fillable'

          ELSE 'Unfillable'
        END
      ELSE 'Unfillable'
    END AS "Fillable",

    /* =======================
       On Time Flag (WORKING DAYS – legacy equivalent)
       ======================= */
    CASE
      WHEN e."INVOICE DATE" IS NULL
        OR e."Promise Date" IS NULL
        THEN NULL

      WHEN e."Delivery Group" IN (
          'Standard',
          'Made To Order',
          'Fabrics and Raw Materials',
          'Other Stock',
          'Advertised Out-of-Stock'
      )
        THEN CASE
          WHEN (
            SELECT COUNT(*)
            FROM GOLD_DATA.DIM.DIM_CALENDAR c
            WHERE c.CALENDAR_DATE BETWEEN DATEADD(day, 1, e."Promise Date"::DATE)
                                      AND e."INVOICE DATE"::DATE
              AND c.IS_WEEKDAY = TRUE
              AND c.IS_HOLIDAY = FALSE
          ) <= 10
            THEN 'Y'
          ELSE 'N'
        END

      ELSE CASE
        WHEN e."INVOICE DATE"::DATE <= e."Promise Date"::DATE THEN 'Y'
        ELSE 'N'
      END
    END AS "On Time Flag"

FROM ENRICHED e
LEFT JOIN KIT_STOCK_SUMMARY kss
  ON LTRIM(RTRIM(e."Item_ID_Child_SKU")) = LTRIM(RTRIM(kss.id_item_par))
;



