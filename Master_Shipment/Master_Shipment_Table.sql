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

    FROM BRONZE_DATA.TCM_BRONZE."CP_SHPHDR_Bronze" h
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
        l.ID_ITEM,
        l.ID_LOC,
        -- TRIM(COALESCE(l.DESCR_1, '') || ' ' || COALESCE(l.DESCR_2, ''))  AS LINE_ITEM_DESCRIPTION,

        -- Category
        -- l.CODE_CAT_PRDT,
        -- l.CODE_CAT_COST,

        -- Quantities
        -- l.QTY_SHIP,
        -- l.QTY_OPEN,
        -- l.QTY_BO,
        -- l.QTY_ALLOC,
        -- l.QTY_CARTON,
        -- l.QTY_CARTON_PER,

        -- -- Pricing
        -- l.PRICE_LIST_VP,
        -- l.PRICE_SELL_VP,
        -- l.PRICE_SELL_NET_VP,
        -- l.COST_UNIT_VP,
        -- l.PRICE_NET,

        -- Dates (line-level)
        -- l.DATE_RQST,
        -- l.DATE_PROM,
        -- l.DATE_BOOK_LAST            AS LINE_DATE_BOOK_LAST,
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

    FROM BRONZE_DATA.TCM_BRONZE."CP_SHPLIN_Bronze" l
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
    l.ID_ORD,
    l.ID_SHIP,
    l.SEQ_LINE_ORD,

    -- ── Customer ──────────────────────────────────────────
    h.ID_CUST_SOLDTO,
    h.SEQ_SHIPTO,
    h.ID_CUST_BILLTO,
    h.NAME_CUST,
    h.NAME_CUST_SHIPTO,
    h.ID_PO_CUST,

    -- ── Order Classification ──────────────────────────────
    h.TYPE_ORD_CP,
    h.CODE_STAT_ORD,

    -- ── Sales Rep ─────────────────────────────────────────
    h.ID_SLSREP_1,
    h.ID_SLSREP_2,
    h.ID_SLSREP_3,
    h.PCT_SPLIT_COMMSN_1,
    h.PCT_SPLIT_COMMSN_2,
    h.PCT_SPLIT_COMMSN_3,
    h.PCT_COMMSN,

    -- ── Item (line-level) ─────────────────────────────────
    l.ID_ITEM,
    l.ID_LOC,
    -- l.LINE_ITEM_DESCRIPTION,
    -- l.CODE_CAT_PRDT,
    -- l.CODE_CAT_COST,

    -- ── Quantities ────────────────────────────────────────
    -- l.QTY_SHIP,
    -- l.QTY_OPEN,
    -- l.QTY_BO,
    -- l.QTY_ALLOC,
    -- l.QTY_CARTON,
    -- l.QTY_CARTON_PER,

    -- -- ── Pricing ───────────────────────────────────────────
    -- l.PRICE_LIST_VP,
    -- l.PRICE_SELL_VP,
    -- l.PRICE_SELL_NET_VP,
    -- l.COST_UNIT_VP,
    -- l.PRICE_NET,

    -- ── Dates (header-level) ──────────────────────────────
    h.DATE_SHIP,
    h.DATE_ORD,
    h.SHP_DATE_CREATED,

    -- ── Dates (line-level) ────────────────────────────────
    -- l.DATE_RQST,
    -- l.DATE_PROM,
    -- l.LINE_DATE_BOOK_LAST,
    l.LINE_DATE_PICK_LAST,
    l.LINE_DATE_BOL_LAST,
    l.LINE_DATE_CHG_LAST,

    -- ── Shipping ──────────────────────────────────────────
    h.ID_LOC_SHIPFM,
    h.CODE_SHIP_VIA_CP,
    h.DESCR_SHIP_VIA,
    h.ADDR_1,
    h.ADDR_2,
    h.CITY,
    h.ID_ST,
    h.ZIP,
    h.COUNTRY,

    -- ── Terms / Discounts ─────────────────────────────────
    h.CODE_TRMS_CP,
    h.DESCR_TRMS,
    h.PCT_DISC_TRMS,
    h.PCT_DISC_ORD_1,
    h.PCT_DISC_ORD_2,
    h.PCT_DISC_ORD_3,

    -- ── Financials (header-level) ─────────────────────────
    h.AMT_ORD_TOTAL,
    h.COST_TOTAL,
    h.AMT_FRT,
    h.TAX_SLS,
    h.AMT_CHRG_MISC,
    h.AMT_FEE_RESTOCK,

    -- ── Invoice ───────────────────────────────────────────
    h.ID_INVC,
    h.FLAG_INVC,
    l.LINE_FLAG_INVC,

    -- ── Weight ────────────────────────────────────────────
    h.WGT_TOTAL,
    h.QTY_CARTON_TOTAL,
    l.WGT_ITEM,
    l.WGT_SHIP_TOTAL,

    -- ── BOL / Confirmation ────────────────────────────────
    h.ID_SHIP_BOL,
    h.FLAG_BOL,
    h.DATE_BOL_LAST,
    h.CODE_STAT_CONFIRM,
    l.FLAG_CONFIRM_SHIP,
    l.LINE_FLAG_BOL,
    l.LINE_FLAG_PICK,
    l.FLAG_POST,

    -- ── Unit of Measure ───────────────────────────────────
    l.CODE_UM_ORD,
    l.CODE_UM_PRICE,
    l.RATIO_STK_PRICE,

    -- ── Flags ─────────────────────────────────────────────
    l.FLAG_STK,
    l.FLAG_BO,

    -- ── Shop Order Linkage (FK → MASTER_SHOPORDER_TABLE) ──
    l.ID_LOC_SO,
    l.ID_SO,
    l.SUFX_SO,

    -- ── Freight ───────────────────────────────────────────
    l.CODE_FRT,

    -- ── Reference ─────────────────────────────────────────
    h.ID_TERR,
    h.ID_QUOTE,
    h.ID_JOB,
    l.ID_EST,
    l.LINE_ID_QUOTE,

    -- ── Bill of Lading ────────────────────────────────────
    b.ID_VHCL,
    b.ID_CARRIER,
    b.NAME_CARRIER,
    b.ACCT_SHIP_VIA,
    b.BOL_CODE_SHIP_VIA_CP,
    b.ID_PRO_BOL,
    b.SEQ_STOP_BOL,
    b.PNT_ORG,
    b.BOL_ID_LOC,
    b.ID_BATCH_INVC,
    b.BOL_ID_INVC,
    b.BOL_WGT_SHIP_TOTAL,
    b.WGT_CONTENT,
    b.QTY_CUBES,
    b.BOL_QTY_CARTON,
    b.QTY_PALLETS,
    b.VOL_CONT,
    b.QTY_CONT_1,
    b.VOL_SHIP_NET,
    b.FLAG_COL_PPD_FRT,
    b.CODE_COL_PPD,
    b.DESCR_COL_PPD,
    b.AMT_COD,
    b.AMT_COD_FC,
    b.AMT_COD_FEE,
    b.AMT_COD_FEE_FC,
    b.BOL_ADDR_1,
    b.BOL_ADDR_2,
    b.BOL_ADDR_3,
    b.BOL_ADDR_4,
    b.ADDR_1_THIRD,
    b.ADDR_2_THIRD,
    b.CITY_THIRD,
    b.ID_ST_THIRD,
    b.COUNTRY_THIRD,
    b.PROV_THIRD,
    b.ZIP_THIRD,
    b.DATE_DELIV_EARLIEST,
    b.DATE_DELIV_LATEST,
    b.REF_CUST,
    b.FLAG_SHIP_COMP,
    b.BOL_DATE_CREATED,
    b.BOL_TIME_ADD,
    b.BOL_ID_USER_ADD,
    b.BOL_DATE_CHANGED,
    b.BOL_TIME_CHG,
    b.BOL_ID_USER_CHG

FROM SHP_LIN l
INNER JOIN SHP_HDR h
    ON l.ID_ORD  = h.ID_ORD
   AND l.ID_SHIP = h.ID_SHIP
LEFT JOIN BOL_HIST b
    ON l.ID_ORD  = b.ID_ORD
   AND l.ID_SHIP = b.ID_SHIP;
