/*
Dynamic Table conversion of BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST view.
Sources data from the Gold layer:
    FROM GOLD_DATA.TCM_GOLD.BOOKINGS_INVOICE_REPORTING

All columns are straight pass-through (no conditional computation),
so the final SELECT uses * from the BASE CTE.
*/
CREATE OR REPLACE DYNAMIC TABLE BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST
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
        -- il.id_planner                                                           AS "Planned Classification",
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
    -- LEFT JOIN BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Dynamic" il
    --     ON sls."Product ID/SKU" = il.id_item
    --    AND sls.id_loc = il.id_loc
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

SELECT * FROM BASE
;
