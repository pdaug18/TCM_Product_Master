CREATE OR REPLACE DYNAMIC TABLE SILVER_DATA.TCM_SILVER.MASTER_CUSTOMER_TABLE
    TARGET_LAG   = 'DOWNSTREAM'
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    WAREHOUSE    = ELT_DEFAULT
AS

/* ============================================================
   CUST_SOLDTO — Sold-to customer master (one row per customer)
   Source: BRONZE_DATA.TCM_BRONZE.CUSMAS_SOLDTO_Bronze
   ============================================================ */
WITH CUST_SOLDTO AS (
    SELECT
        LTRIM(st.ID_CUST)           AS ID_CUST,

        -- Identity
        st.NAME_CUST,
        st.NAME_SORT,
        st.NAME_CONTACT_CUST,
        st.PHONE,
        st.PHONE_FAX,

        -- Classification
        st.CODE_CUST,
        st.CODE_USER_1_AR,
        st.CODE_USER_2_AR,
        st.CODE_USER_3_AR,

        -- Billing
        st.ID_CUST_BILLTO,
        st.CODE_TRMS_CP,
        st.CODE_BO,
        st.CODE_COL_PPD,

        -- Credit
        st.RATING_CR,
        st.CODE_STAT_CR,
        st.AMT_CR_LIMIT,
        st.AMT_CR_MAX,

        -- Address
        st.ADDR_CUST_1,
        st.ADDR_CUST_2,
        st.ADDR_CUST_3,
        st.ADDR_CUST_4,
        st.CITY,
        st.ID_ST,
        st.ZIP,
        st.PROV,
        st.COUNTRY,

        -- AR Metrics
        st.BAL_AR,
        st.QTY_INVC_YTD,
        st.QTY_PYMT_YTD,
        st.AMT_PYMT_YTD,
        st.AMT_PYMT_MTD,
        st.AMT_DSO,
        st.AMT_DDSO,

        -- Aging
        st.AMT_AGE_1,
        st.AMT_AGE_2,
        st.AMT_AGE_3,
        st.AMT_AGE_4,
        st.AMT_AGE_5,
        st.AMT_AGE_FUTURE,
        st.CODE_AGE,
        st.DATE_AGE,

        -- Last Activity
        st.DATE_ORD_LAST,
        st.DATE_INVC_LAST,
        st.DATE_PYMT_LAST,
        st.AMT_ORD_LAST,
        st.AMT_INVC_LAST,
        st.AMT_PYMT_LAST,

        -- Largest Transactions
        st.AMT_ORD_LGST,
        st.AMT_INVC_LGST,
        st.AMT_PYMT_LGST,

        -- Audit
        st.DATE_ADD                 AS CUST_DATE_CREATED,
        st.ID_USER_ADD

    FROM BRONZE_DATA.TCM_BRONZE."CUSMAS_SOLDTO_Bronze" st
),

/* ============================================================
   CUST_SHIPTO — Ship-to addresses (one row per customer + ship-to)
   Source: BRONZE_DATA.TCM_BRONZE.CUSMAS_SHIPTO_Bronze
   ============================================================ */
CUST_SHIPTO AS (
    SELECT
        LTRIM(sh.ID_CUST)           AS ID_CUST,
        sh.SEQ_SHIPTO,

        -- Ship-to Identity
        sh.NAME_CUST                AS SHIPTO_NAME,
        sh.NAME_SORT                AS SHIPTO_NAME_SORT,
        sh.NAME_CONTACT_CUST        AS SHIPTO_CONTACT,
        sh.PHONE                    AS SHIPTO_PHONE,

        -- Ship-to Address
        sh.ADDR_CUST_1              AS SHIPTO_ADDR_1,
        sh.ADDR_CUST_2              AS SHIPTO_ADDR_2,
        sh.ADDR_CUST_3              AS SHIPTO_ADDR_3,
        sh.ADDR_CUST_4              AS SHIPTO_ADDR_4,
        sh.CITY                     AS SHIPTO_CITY,
        sh.ID_ST                    AS SHIPTO_ST,
        sh.ZIP                      AS SHIPTO_ZIP,
        sh.PROV                     AS SHIPTO_PROV,
        sh.COUNTRY                  AS SHIPTO_COUNTRY,

        -- Shipping Config
        sh.ID_LOC_CUST,
        sh.CODE_SHIP_VIA_CP         AS SHIPTO_SHIP_VIA,
        sh.ID_TERR                  AS SHIPTO_TERR,
        sh.ID_SLSREP                AS SHIPTO_SLSREP,
        sh.COMMENT_INVC,

        -- Tax
        sh.ID_EXMT_TAX,
        sh.FLAG_TAX_SHIP,

        -- Ship-to Sales Metrics
        sh.SLS_YR_LAST              AS SHIPTO_SLS_YR_LAST,
        sh.SLS_MTD                  AS SHIPTO_SLS_MTD,
        sh.SLS_YTD                  AS SHIPTO_SLS_YTD,
        sh.COST_YR_LAST             AS SHIPTO_COST_YR_LAST,
        sh.COST_MTD                 AS SHIPTO_COST_MTD,
        sh.COST_YTD                 AS SHIPTO_COST_YTD,

        -- Currency
        sh.CODE_CRNCY               AS SHIPTO_CODE_CRNCY

    FROM BRONZE_DATA.TCM_BRONZE."CUSMAS_SHIPTO_Bronze" sh
),

/* ============================================================
   CUST_GROUP — Customer group code mapping
   Source: BRONZE_DATA.TCM_BRONZE.CUST_GROUP_CODE_Bronze
   ============================================================ */
CUST_GROUP AS (
    SELECT
        g.GROUP_CODE,
        g.GROUP_NAME
    FROM BRONZE_DATA.TCM_BRONZE."CUST_GROUP_CODE_Bronze" g
)

/* ============================================================
   FINAL SELECT — Ship-to grain customer master
   Sold-to fields denormalized onto every ship-to row
   Group code joined via CODE_USER_3_AR
   ============================================================ */
SELECT
    -- ── Customer Key ──────────────────────────────────────
    st.ID_CUST,
    sh.SEQ_SHIPTO,

    -- ── Group Classification ──────────────────────────────
    COALESCE(g.GROUP_CODE, st.ID_CUST)              AS CUST_GROUP_CODE,
    COALESCE(g.GROUP_NAME, st.NAME_CUST)            AS CUST_GROUP_NAME,

    -- ── Sold-to Identity ──────────────────────────────────
    st.NAME_CUST,
    st.NAME_SORT,
    st.NAME_CONTACT_CUST,
    st.PHONE,
    st.PHONE_FAX,

    -- ── Customer Classification ───────────────────────────
    st.CODE_CUST,
    st.CODE_USER_1_AR,
    st.CODE_USER_2_AR,
    st.CODE_USER_3_AR,

    -- ── Billing ───────────────────────────────────────────
    st.ID_CUST_BILLTO,
    st.CODE_TRMS_CP,
    st.CODE_BO,
    st.CODE_COL_PPD,

    -- ── Credit ────────────────────────────────────────────
    st.RATING_CR,
    st.CODE_STAT_CR,
    st.AMT_CR_LIMIT,
    st.AMT_CR_MAX,

    -- ── Sold-to Address ───────────────────────────────────
    st.ADDR_CUST_1,
    st.ADDR_CUST_2,
    st.ADDR_CUST_3,
    st.ADDR_CUST_4,
    st.CITY,
    st.ID_ST,
    st.ZIP,
    st.PROV,
    st.COUNTRY,

    -- ── AR Metrics ────────────────────────────────────────
    st.BAL_AR,
    st.QTY_INVC_YTD,
    st.QTY_PYMT_YTD,
    st.AMT_PYMT_YTD,
    st.AMT_PYMT_MTD,
    st.AMT_DSO,
    st.AMT_DDSO,

    -- ── Aging ─────────────────────────────────────────────
    st.AMT_AGE_1,
    st.AMT_AGE_2,
    st.AMT_AGE_3,
    st.AMT_AGE_4,
    st.AMT_AGE_5,
    st.AMT_AGE_FUTURE,
    st.CODE_AGE,
    st.DATE_AGE,

    -- ── Last Activity ─────────────────────────────────────
    st.DATE_ORD_LAST,
    st.DATE_INVC_LAST,
    st.DATE_PYMT_LAST,
    st.AMT_ORD_LAST,
    st.AMT_INVC_LAST,
    st.AMT_PYMT_LAST,

    -- ── Largest Transactions ──────────────────────────────
    st.AMT_ORD_LGST,
    st.AMT_INVC_LGST,
    st.AMT_PYMT_LGST,

    -- ── Ship-to Identity ──────────────────────────────────
    sh.SHIPTO_NAME,
    sh.SHIPTO_NAME_SORT,
    sh.SHIPTO_CONTACT,
    sh.SHIPTO_PHONE,

    -- ── Ship-to Address ───────────────────────────────────
    sh.SHIPTO_ADDR_1,
    sh.SHIPTO_ADDR_2,
    sh.SHIPTO_ADDR_3,
    sh.SHIPTO_ADDR_4,
    sh.SHIPTO_CITY,
    sh.SHIPTO_ST,
    sh.SHIPTO_ZIP,
    sh.SHIPTO_PROV,
    sh.SHIPTO_COUNTRY,

    -- ── Shipping Config ───────────────────────────────────
    sh.ID_LOC_CUST,
    sh.SHIPTO_SHIP_VIA,
    sh.SHIPTO_TERR,
    sh.SHIPTO_SLSREP,
    sh.COMMENT_INVC,

    -- ── Tax ───────────────────────────────────────────────
    sh.ID_EXMT_TAX,
    sh.FLAG_TAX_SHIP,

    -- ── Ship-to Sales Metrics ─────────────────────────────
    sh.SHIPTO_SLS_YR_LAST,
    sh.SHIPTO_SLS_MTD,
    sh.SHIPTO_SLS_YTD,
    sh.SHIPTO_COST_YR_LAST,
    sh.SHIPTO_COST_MTD,
    sh.SHIPTO_COST_YTD,
    sh.SHIPTO_CODE_CRNCY,

    -- ── Audit ─────────────────────────────────────────────
    st.CUST_DATE_CREATED,
    st.ID_USER_ADD

FROM CUST_SHIPTO sh
INNER JOIN CUST_SOLDTO st
    ON sh.ID_CUST = st.ID_CUST
LEFT JOIN CUST_GROUP g
    ON st.CODE_USER_3_AR = g.GROUP_CODE;
