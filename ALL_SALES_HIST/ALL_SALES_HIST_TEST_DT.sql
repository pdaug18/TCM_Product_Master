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
        sls."Stock Flag" AS "Stock Flag",
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
        mpt."Product Description",
        mpt."COST CATEGORY ID",
        COALESCE(mpt."COST CAT DESCR", 'INVALID COST CATEGORY') AS "COST CAT DESCR",
        mpt."PRODUCT CATEGORY/VERTICAL",
        COALESCE(mpt."PRDT CAT DESCR", 'INVALID PRODUCT CATEGORY') AS "PRDT CAT DESCR",
        sls."TCM Historical Vertical",
        mpt."VERTICAL (Calc)",
        mpt."CATEGORY (Calc)",
        mpt."Product Name/Parent ID",
        COALESCE(mpt."PARENT DESCRIPTION", 'MISSING DESCRIPTION - UPDATE TCM') AS "PARENT DESCRIPTION",
        mpt."ATTR (SKU) CERT_NUM",
        mpt."ATTR (SKU) COLOR",
        mpt."ATTR (SKU) SIZE",
        mpt."ATTR (SKU) LENGTH",
        mpt."ATTR (SKU) TARIFF_CODE",
        mpt."ATTR (SKU) UPC_CODE",
        mpt."ATTR (SKU) PFAS",
        mpt."ATTR (SKU) CLASS",
        mpt."ATTR (SKU) PPC",
        mpt."ATTR (SKU) PRIOR COMMODITY",
        mpt."ATTR (SKU) RBN_WC",
        mpt."ATTR (SKU) REASON",
        mpt."ATTR (SKU) REPLACEMENT",
        mpt."ATTR (SKU) REQUESTOR",
        mpt."ATTR (PAR) BERRY",
        mpt."ATTR (PAR) CARE",
        mpt."ATTR (PAR) HEAT TRANSFER",
        mpt."ATTR (PAR) OTHER",
        mpt."ATTR (PAR) PAD PRINT",
        mpt."ATTR (PAR) PRODUCT CAT",
        mpt."ATTR (PAR) PRODUCT TYPE",
        mpt."ATTR (PAR) TRACKING",
        mpt."ATTR (PAR) Z_BRAND",
        mpt."ATTR (PAR) Z_CATEGORY",
        mpt."ATTR (PAR) Z_GENDER",
        mpt."ATTR (PAR) Z_VERTICAL",
        mpt."Advertised Flag",
        mpt."Cost_Material_Accumulated_Current",
        mpt."Cost_Material_Accumulated_Standard",
        mpt."Cost_Freight_Current",
        mpt."Cost_Freight_Standard",
        mpt."Cost_Material_Current", 
        mpt."Cost_Labor_Current",
        mpt."Cost_Material_Standard",
        mpt."Cost_Labor_Standard",
        mpt."Cost_Outside_Service_Current",
        mpt."Cost_User_Current",
        mpt."Cost_Outside_Service_Standard",
        mpt."Cost_User_Field_Standard",
        mpt."Cost_Total_Current",
        mpt."Cost_Total_Standard",
        mpt."Cost_Variable_Burden_Current",
        mpt."Cost_Variable_Burden_Standard",
        il.id_planner AS "Planned Classification",
        mpt."PROP 65",
        sls."INVOICE DATE" AS "INVOICE DATE",
        mpt."ALT_KEY" AS ALT_KEY,
        mpt."Child Item Status",
        mpt."Parent Item Status",
        r.level_rop AS "Reorder Level",
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
        sls.id_loc AS ID_LOC,
        sls."Shipping Location ID",
        sls.id_loc || ' - ' || tl.DESCR AS "LOC DESCR",
        sls."Calendar Date" AS "CALENDAR DATE",
        sls."Booking Type Table" AS "Booking Type Table"

    FROM GOLD_DATA.TCM_GOLD.BOOKINGS_INVOICE_REPORTING sls
    LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE mpt
        ON sls."Product ID/SKU" = mpt."Product ID/SKU"
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
          WHEN b."Stock Flag" = 'S'
           AND b."Advertised Flag" = 'Y'
           AND b."Reorder Level" > 1
           AND (b."Working Days Promise Minus Order" <= 6 OR b."Working Days Invoice Minus Order" <= 2)
            THEN 'Advertised In-Stock'

          WHEN (
            (b."Stock Flag" = 'S'
             AND b."Advertised Flag" = 'Y'
             AND b."Reorder Level" > 1)
            OR b."Planned Classification" = 'AS'
           )
           AND (b."Working Days Promise Minus Order" > 6 AND b."Working Days Invoice Minus Order" > 2)
            THEN 'Advertised Out-of-Stock'

          WHEN b."Planned Classification" = 'AS'
           AND (b."Working Days Promise Minus Order" <= 6 OR b."Working Days Invoice Minus Order" <= 2)
            THEN 'In-Stock Altered Stock'

          WHEN b."Stock Flag" = 'S'
           AND b."Reorder Level" = 1
            THEN 'Standard'

          WHEN b."Planned Classification" = 'KT'
            THEN 'Kits'

          WHEN b."Stock Flag" = 'N'
           AND b."Planned Classification" NOT IN ('AS','KT')
            THEN 'Made To Order'

         WHEN (
               b."PRODUCT CATEGORY/VERTICAL" IN ('06', '26', '60')
               OR b."Planned Classification" = 'RM'
              )
    
           THEN 'Fabrics and Raw Materials'

         WHEN b."Stock Flag" = 'S' 
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
    e."Stock Flag" AS "Item_Stock Flag",
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
    e."Product ID/SKU" AS "Item ID_Child SKU",
    e."Product Description" AS "Item Description_Child SKU",
    e."COST CATEGORY ID" AS "Item_Cost Category ID",
    e."COST CAT DESCR" AS "Item_Cost Category",
    e."PRODUCT CATEGORY/VERTICAL" AS "Item_Product Category Code",
    e."PRDT CAT DESCR" AS "Item_Product Category",
    e."TCM Historical Vertical",
    e."VERTICAL (Calc)" AS "Item_Vertical",
    e."CATEGORY (Calc)",
    e."Product Name/Parent ID" AS "Item ID_Parent SKU",
    e."PARENT DESCRIPTION" AS "Item Description_Parent SKU",
    e."ATTR (SKU) CERT_NUM" AS "Item_Certificate Number",
    e."ATTR (SKU) COLOR" AS "Item_Color",
    e."ATTR (SKU) SIZE" AS "Item_Size",
    e."ATTR (SKU) LENGTH" AS "Item_Length",
    e."ATTR (SKU) TARIFF_CODE" AS "Item_Tariff Code",
    e."ATTR (SKU) UPC_CODE" AS "Item_UPC Code",
    e."ATTR (SKU) PFAS" AS "Item_PFAS",
    e."ATTR (SKU) CLASS" AS "Item_Class",
    e."ATTR (SKU) PPC" AS "Item_PPC",
    e."ATTR (SKU) PRIOR COMMODITY" AS "Item_Commodity Code Prior",
    e."ATTR (SKU) RBN_WC" AS "Item_Work Center_Rubin",
    e."ATTR (SKU) REASON" AS "Item Status_Obsolete Reason",
    e."ATTR (SKU) REPLACEMENT" AS "Item_Replaced By",
    e."ATTR (SKU) REQUESTOR" AS "Item Status_Obsolete Requestor",
    e."ATTR (PAR) BERRY" AS "Item_Berry",
    e."ATTR (PAR) CARE" AS "Item_Care",
    e."ATTR (PAR) HEAT TRANSFER" AS "Item_Heat Transfer",
    e."ATTR (PAR) OTHER" AS "Item_Other",
    e."ATTR (PAR) PAD PRINT" AS "Item_Pad Print",
    e."ATTR (PAR) PRODUCT CAT",
    e."ATTR (PAR) PRODUCT TYPE" AS "Item_Product Type",
    e."ATTR (PAR) TRACKING" AS "Item_Bin Tracking",
    e."ATTR (PAR) Z_BRAND" AS "Item_Brand",
    e."ATTR (PAR) Z_CATEGORY",
    e."ATTR (PAR) Z_GENDER" AS "Item_Gender",
    e."ATTR (PAR) Z_VERTICAL",
    e."Advertised Flag" AS "Item_Advertised Flag",
    e."Cost_Material_Accumulated_Current",
    e."Cost_Material_Accumulated_Standard",
    e."Cost_Freight_Current",
    e."Cost_Freight_Standard",
    e."Cost_Material_Current", 
    e."Cost_Labor_Current",
    e."Cost_Material_Standard",
    e."Cost_Labor_Standard",
    e."Cost_Outside_Service_Current" AS "Cost_Outside Service_Current",
    e."Cost_User_Current" AS "Cost_User Field_Current",
    e."Cost_Outside_Service_Standard" AS "Cost_Outside Service_Standard",
    e."Cost_User_Field_Standard" AS "Cost_User Field_Standard",
    e."Cost_Total_Current",
    e."Cost_Total_Standard",
    e."Cost_Variable_Burden_Current" AS "Cost_Variable Burden_Current",
    e."Cost_Variable_Burden_Standard" AS "Cost_Variable Burden_Standard",
    e."Planned Classification" AS "Item_Planned Classification",
    e."PROP 65" AS "Item_Prop 65",
    e."INVOICE DATE",
    e."ALT_KEY" AS "Item_ALT Key",
    e."Child Item Status" AS "Item Status_Child Active Status",
    e."Parent Item Status" AS "Item Status_Parent Active Status",
    e."Reorder Level" AS "Item Inventory_Reorder Point",
    e."Sales Transaction Line ID",
    e."Promise Date",
    e."Working Days Invoice Minus Order",
    e."Working Days Promise Minus Order",
    e."Unit Quantity",
    e."Total Price",
    e."Unit Cost",
    e."Sales Rep Name",
    e."ID_LOC" AS "Item_Location ID",
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
    -- e."Delivery Group" AS "Delivery Group",

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
            WHEN e."Stock Flag" = 'S' AND e."Working Days Promise Minus Order" <= 6 THEN 'Fillable'

    /* Not stock item */
            WHEN e."Stock Flag" <> 'S'  AND e."Working Days Promise Minus Order" <= 10  THEN 'Fillable'

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
  ON LTRIM(RTRIM(e."Product ID/SKU")) = LTRIM(RTRIM(kss.id_item_par))
;
