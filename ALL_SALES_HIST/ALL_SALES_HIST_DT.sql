/*
Dynamic Table conversion of BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST view.
Sources data from the Gold layer:
    FROM GOLD_DATA.TCM_GOLD.BOOKINGS_INVOICE_REPORTING

All columns are straight pass-through (no conditional computation),
so the final SELECT uses * from the BASE CTE.
*/
CREATE OR REPLACE DYNAMIC TABLE BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST_DT
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE  = COMPUTE_WH
AS

WITH BASE AS (
    SELECT
        sls."OrderID"                                                           AS "OrderID",
        sls."LineNumber"                                                        AS "LineNumber",
        sls."Ordered Date"                                                      AS "Ordered Date",
        sls."Stock Flag"                                                        AS "Stock Flag",
        sls."InvoiceID"                                                         AS "InvoiceID",
        sls."Freight_Cost"                                                      AS "Freight_Cost",
        sls."Sales_Tax"                                                         AS "Sales_Tax",
        st.name_cust                                                            AS "Customer Name",
        st.id_cust                                                              AS "Customer ID",
        st.code_cust                                                            AS "Customer Type",
        COALESCE(gc.group_code, st.id_cust)                                     AS "CUST/GROUP CODE",
        COALESCE(gc.group_name, st.name_cust)                                   AS "CUST/GROUP NAME",
        st.ADDR_CUST_2                                                          AS "Sold to Address",
        st.city                                                                 AS "Sold to City",
        st.id_st                                                                AS "Sold to State",
        st.ZIP                                                                  AS "Sold to Zip Code",
        st.country                                                              AS "Sold to Country",
        st.code_user_2_ar                                                       AS "Customer Attribute Flag",
        sls."Ship to Address"                                                   AS "Ship to Address",
        sls."Ship to City"                                                      AS "Ship to City",
        sls."Ship to State"                                                     AS "Ship to State",
        sls."Ship to Zip Code"                                                  AS "Ship to Zip Code",
        cst.name_cust                                                           AS "Ship to Customer Name",
        sls.seq_shipto                                                          AS "Ship to #",
        sls."Ship to Country"                                                   AS "Ship to Country",
        sls."Sales Rep ID"                                                      AS "TCM Sales Rep ID",
        sls."Product ID/SKU"                                                    AS "Product ID/SKU",
        mpt."Product Description",
        mpt."COST CATEGORY ID",
        COALESCE(mpt."COST CAT DESCR", 'INVALID COST CATEGORY')                 AS "COST CAT DESCR",
        mpt."PRODUCT CATEGORY/VERTICAL",
        COALESCE(mpt."PRDT CAT DESCR", 'INVALID PRODUCT CATEGORY')              AS "PRDT CAT DESCR",
        sls."TCM Historical Vertical",
        mpt."VERTICAL (Calc)",
        mpt."CATEGORY (Calc)",
        mpt."Product Name/Parent ID",
        COALESCE(mpt."PARENT DESCRIPTION", 'MISSING DESCRIPTION - UPDATE TCM')  AS "PARENT DESCRIPTION",
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
        il.id_planner                                                           AS "Planned Classification",
        mpt."PROP 65",
        sls."INVOICE DATE"                                                      AS "INVOICE DATE",
        mpt."ALT_KEY",
        mpt."Child Item Status",
        mpt."Parent Item Status",
        r.level_rop                                                             AS "Reorder Level",
        sls."LineNumber"                                                        AS "Sales Transaction Line ID",
        sls."Promise Date"                                                      AS "Promise Date",
        sls."Unit Quantity"                                                     AS "Unit Quantity",
        sls."Total Price"                                                       AS "Total Price",
        sls."Unit Cost"                                                         AS "Unit Cost",
        sr.name_slsrep                                                          AS "Sales Rep Name",
        sls.id_loc                                                              AS "ID_LOC",
        sls.id_loc || ' - ' || tl.DESCR                                         AS "LOC DESCR",
        sls."Calendar Date"                                                     AS "CALENDAR DATE",
        sls."Booking Type Table"                                                AS "Booking Type Table"

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
            ROW_NUMBER() OVER (PARTITION BY id_item ORDER BY "rowid" DESC) AS rn
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
)

SELECT
    "OrderID",
    "LineNumber",
    "Ordered Date",
    "Stock Flag" AS "Item_Stock Flag",
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
    "Product ID/SKU" AS "Item ID_Child SKU",
    "Product Description" AS "Item Description_Child SKU",
    "COST CATEGORY ID" AS "Item_Cost Category ID",
    "COST CAT DESCR" AS "Item_Cost Category",
    "PRODUCT CATEGORY/VERTICAL" AS "Item_Product Category Code",
    "PRDT CAT DESCR" AS "Item_Product Category",
    "TCM Historical Vertical",
    "VERTICAL (Calc)" AS "Item_Vertical",
    "CATEGORY (Calc)",
    "Product Name/Parent ID" AS "Item ID_Parent SKU",
    "PARENT DESCRIPTION" AS "Item Description_Parent SKU",
    "ATTR (SKU) CERT_NUM" AS "Item_Certificate Number",
    "ATTR (SKU) COLOR" AS "Item_Color",
    "ATTR (SKU) SIZE" AS "Item_Size",
    "ATTR (SKU) LENGTH" AS "Item_Length",
    "ATTR (SKU) TARIFF_CODE" AS "Item_Tariff Code",
    "ATTR (SKU) UPC_CODE" AS "Item_UPC Code",
    "ATTR (SKU) PFAS" AS "Item_PFAS",
    "ATTR (SKU) CLASS" AS "Item_Class",
    "ATTR (SKU) PPC" AS "Item_PPC",
    "ATTR (SKU) PRIOR COMMODITY" AS "Item_Commodity Code Prior",
    "ATTR (SKU) RBN_WC" AS "Item_Work Center_Rubin",
    "ATTR (SKU) REASON" AS "Item Status_Obsolete Reason",
    "ATTR (SKU) REPLACEMENT" AS "Item_Replaced By",
    "ATTR (SKU) REQUESTOR" AS "Item Status_Obsolete Requestor",
    "ATTR (PAR) BERRY" AS "Item_Berry",
    "ATTR (PAR) CARE" AS "Item_Care",
    "ATTR (PAR) HEAT TRANSFER" AS "Item_Heat Transfer",
    "ATTR (PAR) OTHER" AS "Item_Other",
    "ATTR (PAR) PAD PRINT" AS "Item_Pad Print",
    "ATTR (PAR) PRODUCT CAT",
    "ATTR (PAR) PRODUCT TYPE" AS "Item_Product Type",
    "ATTR (PAR) TRACKING" AS "Item_Bin Tracking",
    "ATTR (PAR) Z_BRAND" AS "Item_Brand",
    "ATTR (PAR) Z_CATEGORY",
    "ATTR (PAR) Z_GENDER" AS "Item_Gender",
    "ATTR (PAR) Z_VERTICAL",
    "Advertised Flag" AS "Item_Advertised Flag",
    "Cost_Material_Accumulated_Current",
    "Cost_Material_Accumulated_Standard",
    "Cost_Freight_Current",
    "Cost_Freight_Standard",
    "Cost_Material_Current", 
    "Cost_Labor_Current",
    "Cost_Material_Standard",
    "Cost_Labor_Standard",
    "Cost_Outside_Service_Current" AS "Cost_Outside Service_Current",
    "Cost_User_Current" AS "Cost_User Field_Current",
    "Cost_Outside_Service_Standard" AS "Cost_Outside Service_Standard",
    "Cost_User_Field_Standard" AS "Cost_User Field_Standard",
    "Cost_Total_Current",
    "Cost_Total_Standard",
    "Cost_Variable_Burden_Current" AS "Cost_Variable Burden_Current",
    "Cost_Variable_Burden_Standard" AS "Cost_Variable Burden_Standard",
    "Planned Classification" AS "Item_Planned Classification",
    "PROP 65" AS "Item_Prop 65",
    "INVOICE DATE",
    "ALT_KEY" AS "Item_ALT Key",
    "Child Item Status" AS "Item Status_Child Active Status",
    "Parent Item Status" AS "Item Status_Parent Active Status",
    "Reorder Level" AS "Item Inventory_Reorder Point",
    "Sales Transaction Line ID",
    "Promise Date",
    "Unit Quantity",
    "Total Price",
    "Unit Cost",
    "Sales Rep Name",
    "ID_LOC" AS "Item_Location ID",
    "LOC DESCR",
    "CALENDAR DATE",
    "Booking Type Table"
FROM BASE
;
