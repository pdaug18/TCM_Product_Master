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
        sls."Stock Flag" AS "Item_Stock Flag",
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
        sls."Product ID/SKU" AS "Item ID_Child SKU",
        mpt."Item Description_Child SKU",
        mpt."Item_Cost Category ID",
        COALESCE(mpt."Item_Cost_Category", 'INVALID COST CATEGORY')                 AS "Item_Cost Category",
        mpt."Item_Vertical_Code",
        COALESCE(mpt."Item_Vertical", 'INVALID PRODUCT CATEGORY')              AS "Item_Vertical",
        sls."TCM Historical Vertical",
        mpt."Item_Vertical",
        mpt."CATEGORY (Calc)",
        mpt."Item ID_Parent SKU",
        COALESCE(mpt."Item Description_Parent SKU", 'MISSING DESCRIPTION - UPDATE TCM')  AS "Item Description_Parent SKU",
        mpt."Item_Certificate Number",
        mpt."Item_Color",
        mpt."Item_Size",
        mpt."Item_Length",
        mpt."Item_Tariff Code",
        mpt."Item_UPC Code",
        mpt."Item_PFAS",
        mpt."Item_Class",
        mpt."Item_PPC",
        mpt."Item_Commodity Code Prior",
        mpt."Item_Work Center_Rubin",
        mpt."Item Status_Obsolete Reason",
        mpt."Item_Replaced By",
        mpt."Item Status_Obsolete Requestor",
        mpt."Item_Berry",
        mpt."Item_Care",
        mpt."Item_Heat Transfer",
        mpt."Item_Other",
        mpt."Item_Pad Print",
        mpt."Item_Product Category",
        mpt."Item_Product Type",
        mpt."Item_Bin Tracking",
        mpt."Item_Brand",
        mpt."Item_Product Category Code",
        mpt."Item_Gender",
        mpt."Item_Vertical Code",
        mpt."Item_Advertised Flag",
        mpt."Cost_Material_Accumulated_Current",
        mpt."Cost_Material_Accumulated_Standard",
        mpt."Cost_Freight_Current",
        mpt."Cost_Freight_Standard",
        mpt."Cost_Material_Current", 
        mpt."Cost_Labor_Current",
        mpt."Cost_Material_Standard",
        mpt."Cost_Labor_Standard",
        mpt."Cost_Outside Service_Current",
        mpt."Cost_User Field_Current",
        mpt."Cost_Outside Service_Standard",
        mpt."Cost_User Field_Standard",
        mpt."Cost_Total_Current",
        mpt."Cost_Total_Standard",
        mpt."Cost_Variable Burden_Current",
        mpt."Cost_Variable Burden_Standard",
        mpt."Cost_Material_Current_Calculated",
        mpt."Cost_Labor_Current_Calculated",
        mpt."Cost_Total_Current_Calculated",
        il.id_planner                                                           AS "Item_Planned Classification",
        mpt."Item_Prop 65",
        sls."INVOICE DATE"                                                      AS "INVOICE DATE",
        mpt."Item_ALT Key",
        mpt."Item Status_Child Active Status",
        mpt."Item Status_Parent Active Status",
        r.level_rop                                                             AS "Item Inventory_Reorder Point",
        sls."LineNumber"                                                        AS "Sales Transaction Line ID",
        sls."Promise Date"                                                      AS "Promise Date",
        sls."Unit Quantity"                                                     AS "Unit Quantity",
        sls."Total Price"                                                       AS "Total Price",
        sls."Unit Cost"                                                         AS "Unit Cost",
        sr.name_slsrep                                                          AS "Sales Rep Name",
        sls.id_loc AS "Item_Location ID",
        sls.id_loc || ' - ' || tl.DESCR                                         AS "LOC DESCR",
        sls."Calendar Date"                                                     AS "CALENDAR DATE",
        sls."Booking Type Table"                                                AS "Booking Type Table"

    FROM GOLD_DATA.TCM_GOLD.BOOKINGS_INVOICE_REPORTING sls
    LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE mpt
        ON sls."Product ID/SKU" = mpt."Item ID_Child SKU"
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

SELECT * FROM BASE;




