CREATE OR REPLACE DYNAMIC TABLE SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
    TARGET_LAG   = 'DOWNSTREAM'
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    WAREHOUSE    = ELT_DEFAULT
AS

/* ============================================================
   SHP_HDR — Shipment header fields (one row per shipment)
   Source: BRONZE_DATA.TCM_BRONZE.CP_SHPHDR_Bronze
   ============================================================ */
WITH SHP_HDR AS (
    SELECT
        h.ID_ORD,
        h.ID_SHIP,

        -- Customer
        h.ID_CUST_SOLDTO,
        h.SEQ_SHIPTO,
        h.ID_CUST_BILLTO,
        h.NAME_CUST,
        h.NAME_CUST_SHIPTO,
        h.ID_PO_CUST,

        -- Order Classification
        h.TYPE_ORD_CP,
        h.CODE_STAT_ORD,

        -- Sales
        h.ID_SLSREP_1,
        h.ID_SLSREP_2,
        h.ID_SLSREP_3,
        h.PCT_SPLIT_COMMSN_1,
        h.PCT_SPLIT_COMMSN_2,
        h.PCT_SPLIT_COMMSN_3,
        h.PCT_COMMSN,

        -- Ship Date
        h.DATE_SHIP,
        h.DATE_ORD,
        h.DATE_ADD                  AS SHP_DATE_CREATED,

        -- Ship-from / Shipping
        h.ID_LOC_SHIPFM,
        h.CODE_SHIP_VIA_CP,
        h.DESCR_SHIP_VIA,

        -- Ship-to Address
        h.ADDR_1,
        h.ADDR_2,
        h.CITY,
        h.ID_ST,
        h.ZIP,
        h.COUNTRY,

        -- Terms
        h.CODE_TRMS_CP,
        h.DESCR_TRMS,
        h.PCT_DISC_TRMS,
        h.PCT_DISC_ORD_1,
        h.PCT_DISC_ORD_2,
        h.PCT_DISC_ORD_3,

        -- Financials (header-level)
        h.AMT_ORD_TOTAL,
        h.COST_TOTAL,
        h.AMT_FRT,
        h.TAX_SLS,
        h.AMT_CHRG_MISC,
        h.AMT_FEE_RESTOCK,

        -- Invoice
        h.ID_INVC,
        h.FLAG_INVC,

        -- Weight / Carton
        h.WGT_TOTAL,
        h.QTY_CARTON_TOTAL,

        -- BOL
        h.ID_SHIP_BOL,
        h.FLAG_BOL,
        h.DATE_BOL_LAST,

        -- Confirmation
        h.CODE_STAT_CONFIRM,

        -- Territory / Reference
        h.ID_TERR,
        h.ID_QUOTE,
        h.ID_JOB

    FROM BRONZE_DATA.TCM_BRONZE."cp_shphdr_perm_Bronze" h
),

/* ============================================================
   SHP_LIN — Shipment line detail (one row per shipment line)
   Source: BRONZE_DATA.TCM_BRONZE.CP_SHPLIN_Bronze
   ============================================================ */
SHP_LIN AS (
    SELECT
        l.ID_ORD,
        l.ID_SHIP,
        l.SEQ_LINE_ORD,

        -- Item
        l.ID_ITEM,      -- Child SKU
        l.ID_LOC,
        TRIM(COALESCE(l.DESCR_1, '') || ' ' || COALESCE(l.DESCR_2, ''))  AS LINE_ITEM_DESCRIPTION,

        -- Category
        l.CODE_CAT_PRDT,
        l.CODE_CAT_COST,

        -- Quantities
        l.QTY_SHIP,
        l.QTY_OPEN,
        l.QTY_BO,
        l.QTY_ALLOC,
        l.QTY_CARTON,
        l.QTY_CARTON_PER,

        -- Pricing
        l.PRICE_LIST_VP,
        l.PRICE_SELL_VP,
        l.PRICE_SELL_NET_VP,
        l.COST_UNIT_VP,
        l.PRICE_NET,

        -- Dates (line-level)
        l.DATE_RQST,
        l.DATE_PROM,
        l.DATE_BOOK_LAST            AS LINE_DATE_BOOK_LAST,
        l.DATE_PICK_LAST            AS LINE_DATE_PICK_LAST,
        l.DATE_BOL_LAST             AS LINE_DATE_BOL_LAST,
        l.DATE_CHG_LAST             AS LINE_DATE_CHG_LAST,

        -- Unit of Measure
        l.CODE_UM_ORD,
        l.CODE_UM_PRICE,
        l.RATIO_STK_PRICE,

        -- Weight
        l.WGT_ITEM,
        l.WGT_SHIP_TOTAL,

        -- Flags
        l.FLAG_STK,
        l.FLAG_BO,
        l.FLAG_CONFIRM_SHIP,
        l.FLAG_INVC                 AS LINE_FLAG_INVC,
        l.FLAG_BOL                  AS LINE_FLAG_BOL,
        l.FLAG_PICK                 AS LINE_FLAG_PICK,
        l.FLAG_POST,

        -- Shop Order Linkage (FK → MASTER_SHOPORDER_TABLE)
        l.ID_LOC_SO,
        l.ID_SO,
        l.SUFX_SO,

        -- Estimate / Quote
        l.ID_EST,
        l.ID_QUOTE                  AS LINE_ID_QUOTE,

        -- Freight
        l.CODE_FRT

    FROM BRONZE_DATA.TCM_BRONZE."cp_shplin_perm_Bronze" l
),

/* ============================================================
   BOL_HIST — Bill of Lading history (one row per shipment)
   Deduped to latest per (ID_ORD, ID_SHIP); metadata columns excluded
   Source: BRONZE_DATA.TCM_BRONZE.CP_BILL_LADING_HIST_bronze
   ============================================================ */
BOL_HIST AS (
    SELECT
        b.ID_ORD,
        b.ID_SHIP,

        -- Vehicle / Carrier
        b.ID_VHCL,
        b.ID_CARRIER,
        b.NAME_CARRIER,
        b.ACCT_SHIP_VIA,
        b.CODE_SHIP_VIA_CP          AS BOL_CODE_SHIP_VIA_CP,

        -- BOL Identifiers
        b.ID_PRO_BOL,
        b.SEQ_STOP_BOL,
        b.PNT_ORG,

        -- Location / Invoice
        b.ID_LOC                    AS BOL_ID_LOC,
        b.ID_BATCH_INVC,
        b.ID_INVC                   AS BOL_ID_INVC,

        -- Weight / Volume / Quantities
        b.WGT_SHIP_TOTAL            AS BOL_WGT_SHIP_TOTAL,
        b.WGT_CONTENT,
        b.QTY_CUBES,
        b.QTY_CARTON                AS BOL_QTY_CARTON,
        b.QTY_PALLETS,
        b.VOL_CONT,
        b.QTY_CONT_1,
        b.VOL_SHIP_NET,

        -- Freight / COD
        b.FLAG_COL_PPD_FRT,
        b.CODE_COL_PPD,
        b.DESCR_COL_PPD,
        b.AMT_COD,
        b.AMT_COD_FC,
        b.AMT_COD_FEE,
        b.AMT_COD_FEE_FC,

        -- Ship-to Address (BOL-level)
        b.ADDR_1                    AS BOL_ADDR_1,
        b.ADDR_2                    AS BOL_ADDR_2,
        b.ADDR_3                    AS BOL_ADDR_3,
        b.ADDR_4                    AS BOL_ADDR_4,

        -- Third-Party Address
        b.ADDR_1_THIRD,
        b.ADDR_2_THIRD,
        b.CITY_THIRD,
        b.ID_ST_THIRD,
        b.COUNTRY_THIRD,
        b.PROV_THIRD,
        b.ZIP_THIRD,

        -- Delivery Dates
        b.DATE_DELIV_EARLIEST,
        b.DATE_DELIV_LATEST,

        -- Customer Reference
        b.REF_CUST,

        -- Flags
        b.FLAG_SHIP_COMP,

        -- Audit
        b.DATE_ADD                  AS BOL_DATE_CREATED,
        b.TIME_ADD                  AS BOL_TIME_ADD,
        b.ID_USER_ADD               AS BOL_ID_USER_ADD,
        b.DATE_CHG                  AS BOL_DATE_CHANGED,
        b.TIME_CHG                  AS BOL_TIME_CHG,
        b.ID_USER_CHG               AS BOL_ID_USER_CHG

    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ID_ORD, ID_SHIP
                ORDER BY "rowid" DESC NULLS LAST, "rowversion" DESC NULLS LAST
            ) AS RN
        FROM BRONZE_DATA.TCM_BRONZE."CP_BILL_LADING_HIST_Bronze"
    ) b
    WHERE b.RN = 1
)

/* ============================================================
   FINAL SELECT — Shipment-line grain master table
   Header fields denormalized onto every line
   ============================================================ */
SELECT
    -- ── Shipment Key ──────────────────────────────────────
        l.ID_ORD as "Order_ID",
        l.ID_SHIP as "Shipment_ID",
        l.SEQ_LINE_ORD as "Shipment_Line_Sequence_#",

    -- ── Customer ──────────────────────────────────────────
        -- h.ID_CUST_SOLDTO,
        -- h.SEQ_SHIPTO,
        -- h.ID_CUST_BILLTO,
        -- h.NAME_CUST,
        -- h.NAME_CUST_SHIPTO,
        -- h.ID_PO_CUST,

    -- ── Order Classification ──────────────────────────────
        h.TYPE_ORD_CP as "Order_Type",
        h.CODE_STAT_ORD as "Order_Status",

    -- ── Sales Rep ─────────────────────────────────────────
        -- h.ID_SLSREP_1,
        -- h.ID_SLSREP_2,
        -- h.ID_SLSREP_3,
        -- h.PCT_SPLIT_COMMSN_1,
        -- h.PCT_SPLIT_COMMSN_2,
        -- h.PCT_SPLIT_COMMSN_3,
        -- h.PCT_COMMSN,

    -- ── Item (line-level) ─────────────────────────────────
        l.ID_ITEM as "Item ID_Child SKU",
        l.ID_LOC as "Shipping_Location_ID", --! Is this where the item is shipping from or to? Verify and rename accordingly.
        -- l.LINE_ITEM_DESCRIPTION,
        -- l.CODE_CAT_PRDT,
        -- l.CODE_CAT_COST,

    -- ── Quantities ────────────────────────────────────────
        l.QTY_SHIP as "Quantity_Shipped",
        -- l.QTY_OPEN,
        -- l.QTY_BO,
        -- l.QTY_ALLOC,
        l.QTY_CARTON as "Quantity_Carton",
        l.QTY_CARTON_PER as "Quantity_Per_Carton",

    -- -- ── Pricing ───────────────────────────────────────────
        -- l.PRICE_LIST_VP,
        -- l.PRICE_SELL_VP,
        -- l.PRICE_SELL_NET_VP,
        -- l.COST_UNIT_VP,
        -- l.PRICE_NET,

    -- ── Dates (header-level) ──────────────────────────────
        h.DATE_SHIP as "Date_Shipped",
        h.DATE_ORD as "Date_Order",
        h.SHP_DATE_CREATED as "Date_Shipment_Created",

    -- ── Dates (line-level) ────────────────────────────────
        -- l.DATE_RQST,
        -- l.DATE_PROM,
        -- l.LINE_DATE_BOOK_LAST,
        l.LINE_DATE_PICK_LAST as "Date_Last_Picking_Line",
        l.LINE_DATE_BOL_LAST as "Date_Last_Bill_of_Lading_Line",
        l.LINE_DATE_CHG_LAST as "Date_Last_Changed_Line",

    -- ── Shipping ──────────────────────────────────────────
        h.ID_LOC_SHIPFM as "Shipping_Location_ID_Ship-From",    --! Is this the same as l.ID_LOC? Verify and rename accordingly.
        h.CODE_SHIP_VIA_CP as "Ship_Via_Code",
        h.DESCR_SHIP_VIA as "Ship_Via_Description",
        h.ADDR_1 as "Shipment_Address_Label_Ship-To",
        h.ADDR_2 as "Shipment_Address_Street_Ship-To",
        h.CITY as "Shipment_Address_City_Ship-To",
        h.ID_ST as "Shipment_Address_State_Ship-To",
        h.ZIP as "Shipment_Address_Zip_Ship-To",
        h.COUNTRY as "Shipment_Address_Country_Ship-To",

    -- -- ── Terms / Discounts ─────────────────────────────────
        -- h.CODE_TRMS_CP,
        -- h.DESCR_TRMS,
        -- h.PCT_DISC_TRMS,
        -- h.PCT_DISC_ORD_1,
        -- h.PCT_DISC_ORD_2,
        -- h.PCT_DISC_ORD_3,

    -- ── Financials (header-level) ─────────────────────────
        -- h.AMT_ORD_TOTAL,
        -- h.COST_TOTAL,
        h.AMT_FRT as "Freight_Amount",
        -- h.TAX_SLS,
        h.AMT_CHRG_MISC as "Misc_Charge_Amount",
        -- h.AMT_FEE_RESTOCK,

    -- ── Invoice ───────────────────────────────────────────
        h.ID_INVC as "Invoice_ID",
        h.FLAG_INVC as "Invoice_Flag",
        l.LINE_FLAG_INVC as "Invoice_Line_Flag",

    -- ── Weight ────────────────────────────────────────────
        h.WGT_TOTAL as "Shipment_Weight_Total",
        h.QTY_CARTON_TOTAL as "Qty_Carton_Total",
        l.WGT_ITEM as "Shipment_Weight_Item",
        l.WGT_SHIP_TOTAL as "Shipment_Weight_Line_Total",

    -- ── BOL / Confirmation ────────────────────────────────
        h.ID_SHIP_BOL as "Bill_of_Lading_ID",
        h.FLAG_BOL as "Bill_of_Lading_Flag",
        h.DATE_BOL_LAST as "Date_Last_Bill_of_Lading_Header",
        h.CODE_STAT_CONFIRM as "Shipment_Confirmation_Status_Code",
        l.FLAG_CONFIRM_SHIP as "Shipment_Confirmation_Line_Flag",
        l.LINE_FLAG_BOL as "Bill_of_Lading_Line_Flag",
        l.LINE_FLAG_PICK as "Picking_Line_Flag",
        l.FLAG_POST as "Post_Flag",

    -- ── Unit of Measure ───────────────────────────────────
        -- l.CODE_UM_ORD,
        -- l.CODE_UM_PRICE,
        -- l.RATIO_STK_PRICE,

    -- ── Flags ─────────────────────────────────────────────
        -- l.FLAG_STK,
        -- l.FLAG_BO,

    -- ── Shop Order Linkage (FK → MASTER_SHOPORDER_TABLE) ──
        -- l.ID_LOC_SO,
        -- l.ID_SO,
        -- l.SUFX_SO,

    -- ── Freight ───────────────────────────────────────────
        -- l.CODE_FRT,

    -- ── Reference ─────────────────────────────────────────
        -- h.ID_TERR,
        -- h.ID_QUOTE,
        -- h.ID_JOB,
        -- l.ID_EST,
        -- l.LINE_ID_QUOTE,

    -- ── Bill of Lading ────────────────────────────────────
        b.ID_VHCL as "Vehicle_ID",
        b.ID_CARRIER as "Carrier_ID",
        b.NAME_CARRIER as "Carrier_Name",
        b.ACCT_SHIP_VIA as "Ship_Via_Account",
        b.BOL_CODE_SHIP_VIA_CP as "Bill_of_Lading_Ship_Via_Code",
        b.ID_PRO_BOL as "Bill_of_Lading_Pro_Number",
        b.SEQ_STOP_BOL as "Bill_of_Lading_Stop_Sequence",
        b.PNT_ORG as "Point_of_Origin",
        b.BOL_ID_LOC as "Bill_of_Lading_Location_ID",
        b.ID_BATCH_INVC as "Invoice_Batch_ID",
        b.BOL_ID_INVC as "Invoice_Bill_of_Lading_ID",
        -- b.BOL_WGT_SHIP_TOTAL as "BOL_Shipment_Total_Weight", --! Redundant with l.WGT_SHIP_TOTAL; verify and remove one or the other.
        b.WGT_CONTENT as "Shipment_Weight_Of_Contents",
        b.QTY_CUBES as "Shipment_Quantity_Of_Cubic_Feet",
        -- b.BOL_QTY_CARTON as "BOL_Quantity_Carton",
        b.QTY_PALLETS as "Quantity_Pallets",
        b.VOL_CONT as "Content_Volume",
        b.QTY_CONT_1 as "Quantity_Container_1",
        b.VOL_SHIP_NET as "Shipment_Volume_Net",
        b.FLAG_COL_PPD_FRT as "Freight_Collect_Pre-Paid_Flag",
        b.CODE_COL_PPD as "Pre-Paid_Collect_Code",
        b.DESCR_COL_PPD as "Pre-Paid_Collect_Description",
        b.AMT_COD as "Cash_On_Delivery_Amount",      
        b.AMT_COD_FC as "Cash_On_Delivery_Foreign_Currency",   
        b.AMT_COD_FEE as "Cash_On_Delivery_Fee_Amount",
        b.AMT_COD_FEE_FC as "Cash_On_Delivery_Fee_Foreign_Currency",
        b.BOL_ADDR_1 as "Bill_Of_Lading_Address_1",
        b.BOL_ADDR_2 as "Bill_Of_Lading_Address_2",
        b.BOL_ADDR_3 as "Bill_Of_Lading_Address_3",
        b.BOL_ADDR_4 as "Bill_Of_Lading_Address_4",
        -- b.ADDR_1_THIRD as "BOL_Third_Party_Address_1",
        -- b.ADDR_2_THIRD as "BOL_Third_Party_Address_2",
        -- b.CITY_THIRD as "BOL_Third_Party_City",
        -- b.ID_ST_THIRD as "BOL_Third_Party_State",
        -- b.COUNTRY_THIRD as "BOL_Third_Party_Country",
        -- b.PROV_THIRD as "BOL_Third_Party_Province",
        -- b.ZIP_THIRD as "BOL_Third_Party_Zip",
        b.DATE_DELIV_EARLIEST as "Date_Delivery_Earliest",
        b.DATE_DELIV_LATEST as "Date_Delivery_Latest",
        b.REF_CUST as "Shipment_Customer_Reference",
        b.FLAG_SHIP_COMP as "Shipment_Completion_Flag",
        b.BOL_DATE_CREATED as "Date_Bill_Of_Lading_Created",
        b.BOL_TIME_ADD as "Date_Bill_Of_Lading_Time_Added",
        b.BOL_ID_USER_ADD as "Bill_Of_Lading_User_Added",
        b.BOL_DATE_CHANGED as "Date_Bill_Of_Lading_Changed",
        b.BOL_TIME_CHG as "Date_Bill_Of_Lading_Time_Changed",
        b.BOL_ID_USER_CHG as "Bill_Of_Lading_User_Change"

FROM SHP_LIN l
INNER JOIN SHP_HDR h
    ON l.ID_ORD  = h.ID_ORD
   AND l.ID_SHIP = h.ID_SHIP
LEFT JOIN BOL_HIST b
    ON l.ID_ORD  = b.ID_ORD
   AND l.ID_SHIP = b.ID_SHIP;
