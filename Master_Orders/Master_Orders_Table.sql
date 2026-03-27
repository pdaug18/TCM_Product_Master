CREATE OR REPLACE DYNAMIC TABLE SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
    TARGET_LAG   = 'DOWNSTREAM'
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    WAREHOUSE    = ELT_DEFAULT
AS
/* ============================================================
   ORD_HDR — Order header fields (one row per order)
   Sources: CP_ORDHDR_Bronze (active) ∪ CP_ORDHDR_PERM_Bronze (closed)
   ============================================================ */
WITH ORD_HDR AS (
    SELECT
        h.ID_ORD,
        'ACTIVE'                    AS HDR_SOURCE_TABLE,

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
        h.ID_JOB,

        -- User
        h.ID_USER_ADD

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_Bronze" h

    UNION ALL

    SELECT
        p.ID_ORD,
        'PERM'                      AS HDR_SOURCE_TABLE,

        p.ID_CUST_SOLDTO,
        p.SEQ_SHIPTO,
        p.ID_CUST_BILLTO,
        p.NAME_CUST,
        p.NAME_CUST_SHIPTO,
        p.ID_PO_CUST,

        p.TYPE_ORD_CP,
        p.CODE_STAT_ORD,

        p.ID_SLSREP_1,
        p.ID_SLSREP_2,
        p.ID_SLSREP_3,
        p.PCT_SPLIT_COMMSN_1,
        p.PCT_SPLIT_COMMSN_2,
        p.PCT_SPLIT_COMMSN_3,
        p.PCT_COMMSN,

        p.DATE_ORD,
        p.DATE_ADD                  AS ORD_DATE_CREATED,
        p.DATE_BOOK_LAST,
        p.DATE_SHIP_LAST            AS HDR_DATE_SHIP_LAST,
        p.DATE_INVC_LAST            AS HDR_DATE_INVC_LAST,

        p.ID_LOC_SHIPFM,
        p.CODE_SHIP_VIA_CP,
        p.DESCR_SHIP_VIA,

        p.ADDR_1,
        p.ADDR_2,
        p.CITY,
        p.ID_ST,
        p.ZIP,
        p.COUNTRY,

        p.CODE_TRMS_CP,
        p.DESCR_TRMS,
        p.PCT_DISC_TRMS,
        p.PCT_DISC_ORD_1,
        p.PCT_DISC_ORD_2,
        p.PCT_DISC_ORD_3,

        p.AMT_ORD_TOTAL,
        p.COST_TOTAL,
        p.AMT_FRT,
        p.TAX_SLS,

        p.ID_TERR,

        p.ID_QUOTE,
        p.ID_JOB,

        p.ID_USER_ADD

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_PERM_Bronze" p
),

/* ============================================================
   ORD_LIN — Order line detail (one row per order + line)
   Sources: CP_ORDLIN_Bronze (active) ∪ CP_ORDLIN_PERM_Bronze (closed)
   ============================================================ */
ORD_LIN AS (
    SELECT
        l.ID_ORD,
        l.SEQ_LINE_ORD,
        'ACTIVE'                    AS LIN_SOURCE_TABLE,

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
        l.WGT_ITEM,

        -- User
        l.ID_USER_ADD

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_Bronze" l

    UNION ALL

    SELECT
        p.ID_ORD,
        p.SEQ_LINE_ORD,
        'PERM'                      AS LIN_SOURCE_TABLE,

        p.ID_ITEM,
        p.ID_LOC,
        TRIM(COALESCE(p.DESCR_1, '') || ' ' || COALESCE(p.DESCR_2, ''))  AS LINE_ITEM_DESCRIPTION,

        p.CODE_CAT_PRDT,
        p.CODE_CAT_COST,

        p.QTY_ORG,
        p.QTY_OPEN,
        p.QTY_BO,
        p.QTY_BOOK,
        p.QTY_REL,
        p.QTY_ALLOC,
        p.QTY_SHIP_TOTAL,
        p.QTY_SHIP_LAST,

        p.PRICE_LIST_VP,
        p.PRICE_SELL_VP,
        p.PRICE_SELL_NET_VP,
        p.COST_UNIT_VP,
        p.PRICE_NET,

        p.DATE_RQST,
        p.DATE_PROM,
        p.DATE_BOOK_LAST            AS LINE_DATE_BOOK_LAST,
        p.DATE_SHIP_LAST            AS LINE_DATE_SHIP_LAST,
        p.DATE_INVC_LAST            AS LINE_DATE_INVC_LAST,
        p.DATE_REL,

        p.CODE_UM_ORD,
        p.CODE_UM_PRICE,
        p.RATIO_STK_PRICE,

        p.FLAG_STK,
        p.FLAG_BO,
        p.FLAG_PRIOR_LINE_ORD,

        p.ID_LOC_SO,
        p.ID_SO,
        p.SUFX_SO,

        p.ID_EST,
        p.ID_QUOTE                  AS LINE_ID_QUOTE,

        p.WGT_ITEM,

        p.ID_USER_ADD

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_PERM_Bronze" p
),

/* ============================================================
   ORD_COMMENTS — Custom comments / ship-date overrides
   Deduped to latest per ID_ORD; soft-deletes excluded
   Source: BRONZE_DATA.TCM_BRONZE.CP_ORDHDR_CUSTOM_COMMENTS_Bronze
   ============================================================ */
ORD_COMMENTS AS (
    SELECT
        c.ID_ORD,
        c.DATE_EST_SHIP,
        c.DATE_OLD_SHIP,
        c.COMMENT                   AS ORD_COMMENT,
        c.LATE_CODE
    FROM (
        SELECT
            ID_ORD,
            DATE_EST_SHIP,
            DATE_OLD_SHIP,
            COMMENT,
            LATE_CODE,
            ROW_NUMBER() OVER (
                PARTITION BY ID_ORD
                ORDER BY DATE_CHG DESC NULLS LAST
            ) AS RN
        FROM BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_CUSTOM_COMMENTS_Bronze"
        WHERE COALESCE(FLAG_DEL, '') <> 'D'
    ) c
    WHERE c.RN = 1
)

/* ============================================================
   FINAL SELECT — Order-line grain master table
   Header fields denormalized onto every line
   ============================================================ */
SELECT
    -- ── Order Key ─────────────────────────────────────────
    l.ID_ORD,
    l.SEQ_LINE_ORD,

    -- ── Source Tables ─────────────────────────────────────
    h.HDR_SOURCE_TABLE,
    l.LIN_SOURCE_TABLE,

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
    l.WGT_ITEM,

    -- ── Comments / Ship Dates ─────────────────────────────
    c.DATE_EST_SHIP,
    c.DATE_OLD_SHIP,
    c.ORD_COMMENT,
    c.LATE_CODE,

    -- ── User ──────────────────────────────────────────────
    h.ID_USER_ADD

FROM ORD_LIN l
INNER JOIN ORD_HDR h
    ON l.ID_ORD = h.ID_ORD
LEFT JOIN ORD_COMMENTS c
    ON l.ID_ORD = c.ID_ORD;
