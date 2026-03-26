CREATE OR REPLACE DYNAMIC TABLE SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
    TARGET_LAG   = 'DOWNSTREAM'
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    WAREHOUSE    = ELT_DEFAULT
AS

/* ============================================================
   ORD_HDR — Order header fields (one row per order)
   Source: BRONZE_DATA.TCM_BRONZE.CP_ORDHDR_Bronze
   ============================================================ */
WITH ORD_HDR AS (
    SELECT
        h.ID_ORD,

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

        -- Dates
        h.DATE_ORD,
        h.DATE_ADD                  AS ORD_DATE_CREATED,
        h.DATE_BOOK_LAST,
        h.DATE_SHIP_LAST            AS HDR_DATE_SHIP_LAST,
        h.DATE_INVC_LAST            AS HDR_DATE_INVC_LAST,

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

        -- Financials (header-level totals)
        h.AMT_ORD_TOTAL,
        h.COST_TOTAL,
        h.AMT_FRT,
        h.TAX_SLS,

        -- Territory
        h.ID_TERR,

        -- Reference
        h.ID_QUOTE,
        h.ID_JOB

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_Bronze" h
),

/* ============================================================
   ORD_LIN — Order line detail (one row per order + line)
   Source: BRONZE_DATA.TCM_BRONZE.CP_ORDLIN_Bronze
   ============================================================ */
ORD_LIN AS (
    SELECT
        l.ID_ORD,
        l.SEQ_LINE_ORD,

        -- Item
        l.ID_ITEM,
        l.ID_LOC,
        TRIM(COALESCE(l.DESCR_1, '') || ' ' || COALESCE(l.DESCR_2, ''))  AS LINE_ITEM_DESCRIPTION,

        -- Category
        l.CODE_CAT_PRDT,
        l.CODE_CAT_COST,

        -- Quantities
        l.QTY_ORG,
        l.QTY_OPEN,
        l.QTY_BO,
        l.QTY_BOOK,
        l.QTY_REL,
        l.QTY_ALLOC,
        l.QTY_SHIP_TOTAL,
        l.QTY_SHIP_LAST,

        -- Pricing (stored as varchar in source — kept as-is for now)
        l.PRICE_LIST_VP,
        l.PRICE_SELL_VP,
        l.PRICE_SELL_NET_VP,
        l.COST_UNIT_VP,
        l.PRICE_NET,

        -- Dates (line-level)
        l.DATE_RQST,
        l.DATE_PROM,
        l.DATE_BOOK_LAST            AS LINE_DATE_BOOK_LAST,
        l.DATE_SHIP_LAST            AS LINE_DATE_SHIP_LAST,
        l.DATE_INVC_LAST            AS LINE_DATE_INVC_LAST,
        l.DATE_REL,

        -- Unit of Measure
        l.CODE_UM_ORD,
        l.CODE_UM_PRICE,
        l.RATIO_STK_PRICE,

        -- Flags
        l.FLAG_STK,
        l.FLAG_BO,
        l.FLAG_PRIOR_LINE_ORD,

        -- Shop Order Linkage (FK to MASTER_SHOPORDER_TABLE)
        l.ID_LOC_SO,
        l.ID_SO,
        l.SUFX_SO,

        -- Estimate / Quote
        l.ID_EST,
        l.ID_QUOTE                  AS LINE_ID_QUOTE,

        -- Weight
        l.WGT_ITEM

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_Bronze" l
)

/* ============================================================
   FINAL SELECT — Order-line grain master table
   Header fields denormalized onto every line
   ============================================================ */
SELECT
    -- ── Order Key ─────────────────────────────────────────
    l.ID_ORD,
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
    l.LINE_ITEM_DESCRIPTION,
    l.CODE_CAT_PRDT,
    l.CODE_CAT_COST,

    -- ── Quantities ────────────────────────────────────────
    l.QTY_ORG,
    l.QTY_OPEN,
    l.QTY_BO,
    l.QTY_BOOK,
    l.QTY_REL,
    l.QTY_ALLOC,
    l.QTY_SHIP_TOTAL,
    l.QTY_SHIP_LAST,
    l.QTY_ORG - l.QTY_SHIP_TOTAL                   AS QTY_REMAINING,

    -- ── Pricing ───────────────────────────────────────────
    l.PRICE_LIST_VP,
    l.PRICE_SELL_VP,
    l.PRICE_SELL_NET_VP,
    l.COST_UNIT_VP,
    l.PRICE_NET,

    -- ── Dates (header-level) ──────────────────────────────
    h.DATE_ORD,
    h.ORD_DATE_CREATED,
    h.DATE_BOOK_LAST,

    -- ── Dates (line-level) ────────────────────────────────
    l.DATE_RQST,
    l.DATE_PROM,
    l.LINE_DATE_BOOK_LAST,
    l.LINE_DATE_SHIP_LAST,
    l.LINE_DATE_INVC_LAST,
    l.DATE_REL,

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

    -- ── Unit of Measure ───────────────────────────────────
    l.CODE_UM_ORD,
    l.CODE_UM_PRICE,
    l.RATIO_STK_PRICE,

    -- ── Flags ─────────────────────────────────────────────
    l.FLAG_STK,
    l.FLAG_BO,
    l.FLAG_PRIOR_LINE_ORD,

    -- ── Shop Order Linkage (FK → MASTER_SHOPORDER_TABLE) ──
    l.ID_LOC_SO,
    l.ID_SO,
    l.SUFX_SO,

    -- ── Reference ─────────────────────────────────────────
    h.ID_TERR,
    h.ID_QUOTE,
    h.ID_JOB,
    l.ID_EST,
    l.LINE_ID_QUOTE,
    l.WGT_ITEM

FROM ORD_LIN l
INNER JOIN ORD_HDR h
    ON l.ID_ORD = h.ID_ORD;
