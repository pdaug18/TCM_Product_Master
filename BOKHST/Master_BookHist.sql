CREATE OR REPLACE DYNAMIC TABLE SILVER_DATA.TCM_SILVER.MASTER_BOOKING_HISTORY_TABLE
    TARGET_LAG   = 'DOWNSTREAM'
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    WAREHOUSE    = ELT_DEFAULT
AS

/* ============================================================
   VALID_ORDERS — Filter to orders with bookings since 2019,
   excluding RMA entries and REWORK items
   ============================================================ */
WITH VALID_ORDERS AS (
    SELECT DISTINCT bl.ID_ORD
    FROM BRONZE_DATA.TCM_BRONZE."BOKHST_LINE_Bronze" bl
    WHERE bl.DATE_BOOK_LAST >= '2019-01-01'::DATE
      AND bl.ID_USER_ADD <> 'RMA'
      AND bl.ID_ITEM     <> 'REWORK'
),

/* ============================================================
   BK_LINE — Booking history line detail (item-level actuals)
   Filtered to valid orders only
   Source: BRONZE_DATA.TCM_BRONZE.BOKHST_LINE_Bronze
   ============================================================ */
BK_LINE AS (
    SELECT
        -- Join Keys (to HDR)
        l.ID_ORD,
        l.ID_CUST,
        l.SEQ_SHIPTO,

        -- Line Key
        l.SEQ_LINE_ORD,
        l.CONCAT_ORD_REL,
        l.TYPE_REC_CP_HIST,
        l.SEQ_REC,

        -- Shipment FK
        l.ID_SHIP,
        l.VER_BO,

        -- Item Detail
        l.ID_ITEM,
        l.ID_LOC,
        TRIM(COALESCE(l.DESCR_1, '') || ' ' || COALESCE(l.DESCR_2, ''))  AS LINE_ITEM_DESCRIPTION,
        l.CODE_CAT_PRDT,
        l.CODE_UM_PRICE,

        -- Quantities
        l.QTY_SHIP,
        l.QTY_OPEN,
        l.QTY_BO,

        -- Actuals (revenue & cost)
        l.SLS,
        l.COST,
        l.PRICE_LIST,

        -- Dates (line-level)
        l.DATE_RQST,
        l.DATE_PROM,
        l.DATE_BOOK_LAST,
        l.DATE_TRX,

        -- Activity Flags (event classification)
        l.FLAG_ACT_QTY,
        l.FLAG_ACT_PRICE,
        l.FLAG_ACT_DISC_ORD,
        l.FLAG_ACT_DISC_LINE,
        l.FLAG_ACT_MARKUP,
        l.FLAG_ACT_CANCEL_BO,
        l.FLAG_ACT_OVRSHIP,
        l.FLAG_ACT_CANCEL,
        l.FLAG_ACT_ADD,
        l.FLAG_ACT_UNUSED,

        -- Other
        l.FLAG_DROP_SHIP,
        l.WGT_ITEM,
        l.ID_EST,
        l.AMT_FEE_RESTOCK           AS LINE_AMT_FEE_RESTOCK

    FROM BRONZE_DATA.TCM_BRONZE."BOKHST_LINE_Bronze" l
    INNER JOIN VALID_ORDERS vo
        ON l.ID_ORD = vo.ID_ORD
)select * from BK_LINE limit 100;
,

/* ============================================================
   BK_HDR_RANKED — Booking header, deduped to 1 row per
   (ID_ORD, ID_CUST, SEQ_SHIPTO) using most recent DATE_ORD.
   Excludes inter-company (IC) and inter-plant (IP) customers.
   Snowflake equivalent of SQL Server CROSS APPLY TOP 1.
   Source: BRONZE_DATA.TCM_BRONZE.BOKHST_HDR_Bronze
   ============================================================ */
BK_HDR_RANKED AS (
    SELECT
        h.ID_ORD,
        h.ID_CUST,
        h.SEQ_SHIPTO,

        -- Invoice
        h.ID_INVC,
        h.DATE_INVC,
        h.AMT_INVC,
        h.AMT_INVC_TAXBL,

        -- Customer
        h.NAME_CUST_SOLDTO,
        h.ID_CUST_BILLTO,
        h.ID_PO_CUST,
        h.CODE_CUST_1,

        -- Order Classification
        h.TYPE_ORD_CP,
        h.CODE_TRMS_CP,

        -- Sales Rep
        h.ID_SLSREP_1,
        h.ID_SLSREP_2,
        h.ID_SLSREP_3,
        h.PCT_SPLIT_COMMSN_1,
        h.PCT_SPLIT_COMMSN_2,
        h.PCT_SPLIT_COMMSN_3,

        -- Dates
        h.DATE_ORD,
        h.DATE_USER,
        h.DATE_ADD                  AS BK_DATE_CREATED,

        -- Financials (header-level)
        h.AMT_FRT,
        h.TAX_SLS,
        h.AMT_CHRG_MISC,
        h.COST_TOTAL                AS HDR_COST_TOTAL,
        h.AMT_FEE_RESTOCK,

        -- Discounts
        h.PCT_DISC_ORD_1,
        h.PCT_DISC_ORD_2,
        h.PCT_DISC_ORD_3,

        -- Geography
        h.CITY,
        h.ID_ST,
        h.ZIP,
        h.COUNTRY,

        -- Reference
        h.ID_TERR,
        h.ID_QUOTE,
        h.ID_JOB,
        h.CODE_USER,

        ROW_NUMBER() OVER (
            PARTITION BY h.ID_ORD, h.ID_CUST, h.SEQ_SHIPTO
            ORDER BY h.DATE_ORD DESC
        ) AS RN

    FROM BRONZE_DATA.TCM_BRONZE."BOKHST_HDR_Bronze" h
    WHERE h.CODE_CUST_1 NOT IN ('IC', 'IP')
),

BK_HDR AS (
    SELECT * EXCLUDE (RN)
    FROM BK_HDR_RANKED
    WHERE RN = 1
)

/* ============================================================
   FINAL SELECT — Transaction-event grain booking history
   HDR deduped via ROW_NUMBER (most recent DATE_ORD per order+cust)
   LINE filtered to valid orders (post-2019, no RMA, no REWORK)
   ============================================================ */
SELECT
    -- ── Order Key ─────────────────────────────────────────
    l.ID_ORD,
    l.SEQ_LINE_ORD,
    l.CONCAT_ORD_REL,
    l.TYPE_REC_CP_HIST,
    l.SEQ_REC,

    -- ── Customer ──────────────────────────────────────────
    h.ID_CUST,
    h.SEQ_SHIPTO,
    h.NAME_CUST_SOLDTO,
    h.ID_CUST_BILLTO,
    h.ID_PO_CUST,
    h.CODE_CUST_1,

    -- ── Invoice ───────────────────────────────────────────
    h.ID_INVC,
    h.DATE_INVC,
    h.AMT_INVC,
    h.AMT_INVC_TAXBL,

    -- ── Order Classification ──────────────────────────────
    h.TYPE_ORD_CP,
    h.CODE_TRMS_CP,

    -- ── Sales Rep ─────────────────────────────────────────
    h.ID_SLSREP_1,
    h.ID_SLSREP_2,
    h.ID_SLSREP_3,
    h.PCT_SPLIT_COMMSN_1,
    h.PCT_SPLIT_COMMSN_2,
    h.PCT_SPLIT_COMMSN_3,

    -- ── Item (line-level) ─────────────────────────────────
    l.ID_ITEM,
    l.ID_LOC,
    l.LINE_ITEM_DESCRIPTION,
    l.CODE_CAT_PRDT,
    l.CODE_UM_PRICE,

    -- ── Quantities ────────────────────────────────────────
    l.QTY_SHIP,
    l.QTY_OPEN,
    l.QTY_BO,

    -- ── Revenue & Cost Actuals ────────────────────────────
    l.SLS,
    l.COST,
    l.PRICE_LIST,
    l.SLS - l.COST                                  AS MARGIN,

    -- ── Dates (header-level) ──────────────────────────────
    h.DATE_ORD,
    h.DATE_USER,
    h.BK_DATE_CREATED,

    -- ── Dates (line-level) ────────────────────────────────
    l.DATE_RQST,
    l.DATE_PROM,
    l.DATE_BOOK_LAST,
    l.DATE_TRX,
    l.DATE_BOOK_LAST                                AS CALENDAR_DATE,

    -- ── Activity Flags (event classification) ─────────────
    l.FLAG_ACT_QTY,
    l.FLAG_ACT_PRICE,
    l.FLAG_ACT_DISC_ORD,
    l.FLAG_ACT_DISC_LINE,
    l.FLAG_ACT_MARKUP,
    l.FLAG_ACT_CANCEL_BO,
    l.FLAG_ACT_OVRSHIP,
    l.FLAG_ACT_CANCEL,
    l.FLAG_ACT_ADD,
    l.FLAG_ACT_UNUSED,

    -- ── Financials (header-level) ─────────────────────────
    h.AMT_FRT,
    h.TAX_SLS,
    h.AMT_CHRG_MISC,
    h.HDR_COST_TOTAL,
    h.AMT_FEE_RESTOCK,

    -- ── Discounts ─────────────────────────────────────────
    h.PCT_DISC_ORD_1,
    h.PCT_DISC_ORD_2,
    h.PCT_DISC_ORD_3,

    -- ── Shipping / Geography ──────────────────────────────
    h.CITY,
    h.ID_ST,
    h.ZIP,
    h.COUNTRY,
    l.FLAG_DROP_SHIP,
    l.WGT_ITEM,

    -- ── Shipment FK ───────────────────────────────────────
    l.ID_SHIP,
    l.VER_BO,

    -- ── Reference ─────────────────────────────────────────
    h.ID_TERR,
    h.ID_QUOTE,
    h.ID_JOB,
    h.CODE_USER,
    l.ID_EST,
    l.LINE_AMT_FEE_RESTOCK

FROM BK_LINE l
INNER JOIN BK_HDR h
    ON  l.ID_ORD     = h.ID_ORD
   AND l.ID_CUST    = h.ID_CUST
   AND l.SEQ_SHIPTO = h.SEQ_SHIPTO;
