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

        -- Customer Type Codes
        h.CODE_CUST_1,
        h.CODE_CUST_2,
        h.CODE_CUST_3,

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

        p.CODE_CUST_1,
        p.CODE_CUST_2,
        p.CODE_CUST_3,

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
        l.ID_ITEM_CUST,
        l.ID_CONFIG,
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

        -- Commission
        l.AMT_COMMSN,

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

        -- Backorder
        l.VER_BO,

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
        p.ID_ITEM_CUST,
        p.ID_CONFIG,
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

        p.AMT_COMMSN,

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

        p.VER_BO,

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
),

/* ============================================================
   SLSREP — Sales representative name lookup
   Source: BRONZE_DATA.TCM_BRONZE.TABLES_SLSREP_Bronze
   ============================================================ */
SLSREP AS (
    SELECT
        sr.ID_SLSREP,
        sr.NAME_SLSREP
    FROM BRONZE_DATA.TCM_BRONZE."TABLES_SLSREP_Bronze" sr
),

/* ============================================================
   LOC_DESC — Location description lookup
   Source: BRONZE_DATA.TCM_BRONZE.TABLES_LOC_Bronze
   ============================================================ */
LOC_DESC AS (
    SELECT
        loc.ID_LOC,
        loc.DESCR                   AS LOC_DESCRIPTION
    FROM BRONZE_DATA.TCM_BRONZE."TABLES_LOC_Bronze" loc
),

/* ============================================================
   PROD_CAT_CUST — Product category description (customer-type-specific)
   Source: BRONZE_DATA.TCM_BRONZE.TABLES_CODE_CAT_PRDT_Bronze
   ============================================================ */
PROD_CAT_CUST AS (
    SELECT
        pc.CODE_CAT_PRDT,
        pc.CODE_TYPE_CUST,
        pc.DESCR                    AS PROD_CAT_DESCR
    FROM BRONZE_DATA.TCM_BRONZE."TABLES_CODE_CAT_PRDT_Bronze" pc
    WHERE pc.CODE_TYPE_CUST <> ' '
),

/* ============================================================
   PROD_CAT_DFLT — Product category description (default / generic)
   Source: BRONZE_DATA.TCM_BRONZE.TABLES_CODE_CAT_PRDT_Bronze
   ============================================================ */
PROD_CAT_DFLT AS (
    SELECT
        pd.CODE_CAT_PRDT,
        pd.DESCR                    AS PROD_CAT_DESCR
    FROM BRONZE_DATA.TCM_BRONZE."TABLES_CODE_CAT_PRDT_Bronze" pd
    WHERE pd.CODE_TYPE_CUST = ' '
)

/* ============================================================
   FINAL SELECT — Order-line grain master table
   Header fields denormalized onto every line
   Reference lookups: sales rep name, location, product category
   VP pricing decoded to numeric + open-value calculations
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

    -- ── Customer Type Codes ───────────────────────────────
    h.CODE_CUST_1,
    h.CODE_CUST_2,
    h.CODE_CUST_3,

    -- ── Sales Rep ─────────────────────────────────────────
    h.ID_SLSREP_1,
    sr.NAME_SLSREP                                  AS SLSREP_1_NAME,
    h.ID_SLSREP_2,
    h.ID_SLSREP_3,
    h.PCT_SPLIT_COMMSN_1,
    h.PCT_SPLIT_COMMSN_2,
    h.PCT_SPLIT_COMMSN_3,
    h.PCT_COMMSN,

    -- ── Item (line-level) ─────────────────────────────────
    l.ID_ITEM,
    l.ID_ITEM_CUST,
    l.ID_CONFIG,
    l.ID_LOC,
    ld.LOC_DESCRIPTION,
    l.LINE_ITEM_DESCRIPTION,
    l.CODE_CAT_PRDT,
    l.CODE_CAT_COST,
    COALESCE(pcc.PROD_CAT_DESCR, pcd.PROD_CAT_DESCR)
                                                    AS PROD_CAT_DESCR,
    l.CODE_CAT_PRDT || h.CODE_CUST_1               AS CONCAT_PROD_CAT,

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

    -- ── Pricing (raw VP varchar — retained for audit) ─────
    l.PRICE_LIST_VP,
    l.PRICE_SELL_VP,
    l.PRICE_SELL_NET_VP,
    l.COST_UNIT_VP,
    l.PRICE_NET,

    -- ── Pricing (decoded numeric) ─────────────────────────
    --   VP format: RIGHT(field, 10) gives mantissa; /10000 scales
    --   TRY_CAST handles non-numeric VP values gracefully (→ NULL)
    TRY_CAST(RIGHT(l.COST_UNIT_VP, 10) AS DECIMAL(18,6))       / 10000   AS COST_UNIT,
    TRY_CAST(RIGHT(l.PRICE_LIST_VP, 10) AS DECIMAL(18,6))      / 10000   AS PRICE_LIST,
    TRY_CAST(RIGHT(l.PRICE_SELL_VP, 10) AS DECIMAL(18,6))      / 10000   AS PRICE_SELL,
    TRY_CAST(RIGHT(l.PRICE_SELL_NET_VP, 10) AS DECIMAL(18,6))  / 10000   AS PRICE_SELL_NET,

    -- ── Open-Value Calculations ───────────────────────────
    l.QTY_OPEN * (TRY_CAST(RIGHT(l.COST_UNIT_VP, 10) AS DECIMAL(18,6))      / 10000)   AS OPEN_COST,
    l.QTY_OPEN * (TRY_CAST(RIGHT(l.PRICE_SELL_NET_VP, 10) AS DECIMAL(18,6)) / 10000)   AS OPEN_NET_AMT,
    l.QTY_OPEN * (TRY_CAST(RIGHT(l.PRICE_LIST_VP, 10) AS DECIMAL(18,6))     / 10000)   AS OPEN_LIST_AMT,
    l.QTY_OPEN * (TRY_CAST(RIGHT(l.PRICE_SELL_NET_VP, 10) AS DECIMAL(18,6)) / 10000)   AS OPEN_SELL_AMT, 
    l.QTY_OPEN * (TRY_CAST(RIGHT(l.COST_UNIT_VP, 10) AS DECIMAL(18,6))      / 10000)   AS OPEN_MARGIN,

    -- ── Commission ────────────────────────────────────────
    l.AMT_COMMSN,

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

    -- ── Promise Date Dimensions ───────────────────────────
    YEAR(l.DATE_PROM)                               AS DATE_PROM_YEAR,
    QUARTER(l.DATE_PROM)                            AS DATE_PROM_QUARTER,
    MONTH(l.DATE_PROM)                              AS DATE_PROM_MONTH,
    YEAR(l.DATE_PROM) * 100 + MONTH(l.DATE_PROM)   AS DATE_PROM_YEARMONTH,

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

    -- ── Backorder ─────────────────────────────────────────
    l.VER_BO,

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
    ON l.ID_ORD = c.ID_ORD
LEFT JOIN SLSREP sr
    ON h.ID_SLSREP_1 = sr.ID_SLSREP
LEFT JOIN LOC_DESC ld
    ON l.ID_LOC = ld.ID_LOC
LEFT JOIN PROD_CAT_CUST pcc
    ON l.CODE_CAT_PRDT = pcc.CODE_CAT_PRDT
   AND h.CODE_CUST_1   = pcc.CODE_TYPE_CUST
LEFT JOIN PROD_CAT_DFLT pcd
    ON l.CODE_CAT_PRDT = pcd.CODE_CAT_PRDT;
