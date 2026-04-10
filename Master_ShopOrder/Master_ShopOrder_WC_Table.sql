CREATE OR REPLACE DYNAMIC TABLE SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
    TARGET_LAG   = 'DOWNSTREAM'
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    WAREHOUSE    = ELT_DEFAULT
AS

/* ============================================================
   SO_HDR — Core shop order header fields
   Grain: 1 row per (ID_LOC, ID_SO, SUFX_SO)
   Source: BRONZE_DATA.TCM_BRONZE."SHPORD_HDR_Bronze"
   ============================================================ */
WITH SO_HDR AS (
    SELECT
        -- Business Key
        h.ID_LOC,
        h.ID_SO,
        h.SUFX_SO,

        -- Item Identifiers
        h.ID_ITEM_PAR,
        h.DESCR_ITEM_1,
        h.DESCR_ITEM_2,

        -- Order Classification
        h.TYPE_ORD,
        h.CODE_CAT_COST,

        -- Status / Control Flags
        h.STAT_REC_SO,
        h.FLAG_CMPL_SO,
        h.FLAG_REL,
        h.FLAG_EXPEDITED_SO,
        h.FLAG_STAT_ARCHIVE,

        -- Quantities
        h.QTY_ORD,
        h.QTY_CMPL,

        -- Dates (SHOP_ORDER_INFO-aligned column names)
        h.DATE_ADD,
        h.DATE_DUE_ORD,
        h.DATE_START_OPER_1ST,
        h.TIME_START_OPER_1ST,

        -- Dates (enriched aliases for additional context)
        h.DATE_START_ORD            AS SO_DATE_START_ORDERED,
        h.DATE_START_PLAN           AS SO_DATE_START_PLANNED,
        h.DATE_DUE_PLAN             AS SO_DATE_DUE_PLANNED,
        h.DATE_CMPL                 AS SO_DATE_COMPLETED,
        h.DATE_CHG_LAST             AS SO_DATE_LAST_CHANGED,

        -- Reference / Linkage
        h.ID_BUYER,
        h.ID_PROJ,
        h.ID_EST,
        h.ID_DRAW,
        h.ID_REV_DRAW

    FROM BRONZE_DATA.TCM_BRONZE."SHPORD_HDR_Bronze" AS h
),

/* ============================================================
   SO_OPER_DETAIL — Operation-level rows (drives grain change)
   Grain: 1 row per (ID_SO, SUFX_SO, ID_OPER)
   Source: BRONZE_DATA.TCM_BRONZE."SHPORD_OPER_Bronze"
   ============================================================ */
SO_OPER_DETAIL AS (
    SELECT
        o.ID_SO,
        o.SUFX_SO,
        o.ID_OPER,
        o.ID_WC                         AS SOPERWC,
        o.STAT_REC_OPER,
        o.HR_LABOR_SF,
        o.QTY_CMPL                      AS O_QTY_CMPL,
        o.QTY_ORD                       AS O_QTY_ORD
    FROM BRONZE_DATA.TCM_BRONZE."SHPORD_OPER_Bronze" AS o
),

/* ============================================================
   SO_APPROVE — First (earliest) approval event per shop order
   Dedup: MIN() per shop order to take earliest approval record
   Source: BRONZE_DATA.TCM_BRONZE."SHPORD_APPROVE_Bronze"
   ============================================================ */
SO_APPROVE AS (
    SELECT
        CONCAT(LTRIM(ID_SO), SUFX_SO)   AS SA_SO_AND_SUFX,
        MIN(DATETIME_APPROVED)          AS DATETIME_APPROVED,
        MIN(TIME_APPROVED)              AS TIME_APPROVED,
        MIN(DATE_APPROVED)              AS DATE_APPROVED
    FROM BRONZE_DATA.TCM_BRONZE."SHPORD_APPROVE_Bronze"
    GROUP BY CONCAT(LTRIM(ID_SO), SUFX_SO)
),

/* ============================================================
   SO_JOB — Primary job linkage per shop order
   Deduplication: ROW_NUMBER() keeps first row by SEQ_JOB ASC
   (handles shop order splits — takes the primary job assignment)
   Source: BRONZE_DATA.TCM_BRONZE."SHPORD_JOB_Bronze"
   ============================================================ */
-- SO_JOB AS (
    -- SELECT
    --     ID_LOC,
    --     ID_SO,
    --     SUFX_SO,
    --     ID_JOB,
    --     SEQ_JOB,
    --     ID_CUST         AS JOB_ID_CUST,
    --     ID_ORD,
    --     SEQ_LINE_ORD,
    --     DATE_NEED       AS JOB_DATE_NEED,
    --     DATE_PROM       AS JOB_DATE_PROM,
    --     PCT_SPLIT_JOB,
    --     CODE_CAT_COST   AS JOB_CODE_CAT_COST,
    --     FLAG_POST_JOB
    -- FROM (
    --     SELECT
    --         j.*,
    --         ROW_NUMBER() OVER (
    --             PARTITION BY j.ID_LOC, j.ID_SO, j.SUFX_SO
    --             ORDER BY j.SEQ_JOB ASC
    --         ) AS RN
    --     FROM BRONZE_DATA.TCM_BRONZE."SHPORD_JOB_Bronze" AS j
    -- ) ranked
    -- WHERE RN = 1
-- ),

/* ============================================================
   RTE_OPER — Routing operation work-centre override
   Source: BRONZE_DATA.TCM_BRONZE."ROUTMS_OPER_Bronze"
   ============================================================ */
RTE_OPER AS (
    SELECT
        r.ID_ITEM,
        r.ID_OPER,
        r.ID_RTE,
        r.ID_WC                         AS RTOWC,
        r.HR_MACH_SR                    AS RTO_HR_MACH_SR
    FROM BRONZE_DATA.TCM_BRONZE."ROUTMS_OPER_Bronze" AS r
),

/* ============================================================
   PREPROD — Shop orders currently on the production floor
   Source: BRONZE_DATA.TCM_BRONZE."OP_JOB_CARDS_ON_FLOOR_Bronze"
   ============================================================ */
PREPROD AS (
    SELECT DISTINCT
        LTRIM(ID_SO)                    AS SOFROMPREPROD,
        SUFX_SO
    FROM BRONZE_DATA.TCM_BRONZE."OP_JOB_CARDS_ON_FLOOR_Bronze"
),

/* ============================================================
   SO_OPER_AGG — Operation metrics rolled up to header level
   Grain: 1 row per (ID_LOC, ID_SO, SUFX_SO)
   Source: BRONZE_DATA.TCM_BRONZE."SHPORD_OPER_Bronze"
   ============================================================ */
SO_OPER_AGG AS (
    SELECT
        ID_LOC,
        ID_SO,
        SUFX_SO,
        COUNT(*)                        AS oper_count,
        SUM(HR_LABOR_SF)                AS oper_labor_hrs_planned,
        SUM(HR_LABOR_ACTUAL_SF)         AS oper_labor_hrs_actual,
        SUM(HR_MACH_SF)                 AS oper_machine_hrs_planned,
        SUM(HR_MACH_ACTUAL_SF)          AS oper_machine_hrs_actual,
        MIN(DATE_START_OPER)            AS oper_earliest_start_date,
        MAX(DATE_DUE_OPER)              AS oper_latest_due_date
    FROM BRONZE_DATA.TCM_BRONZE."SHPORD_OPER_Bronze"
    GROUP BY
        ID_LOC,
        ID_SO,
        SUFX_SO
),

/* ============================================================
   SO_MATL_AGG — Material/component metrics rolled up to header level
   Grain: 1 row per (ID_LOC, ID_SO, SUFX_SO)
   Source: BRONZE_DATA.TCM_BRONZE."SHPORD_MATL_Bronze"
   ============================================================ */
SO_MATL_AGG AS (
    SELECT
        ID_LOC,
        ID_SO,
        SUFX_SO,
        COUNT(*)                        AS matl_line_count,
        SUM(QTY_PER)                    AS matl_total_qty_per,
        SUM(QTY_ALLOC)                  AS matl_total_qty_allocated,
        SUM(QTY_ISS)                    AS matl_total_qty_issued
    FROM BRONZE_DATA.TCM_BRONZE."SHPORD_MATL_Bronze"
    GROUP BY
        ID_LOC,
        ID_SO,
        SUFX_SO
)

/* ============================================================
   FINAL SELECT — Operation-grain master table
   Grain: 1 row per (ID_LOC, ID_SO, SUFX_SO, ID_OPER)
   Oper/matl aggregates denormalized onto each operation row
   ============================================================ */
SELECT
    -- ── Business Key ──────────────────────────────────────
    h.ID_LOC                                        AS "Shop_Order_Location_ID",
    LTRIM(h.ID_SO)                                  AS "Shop_Order_ID",
    h.SUFX_SO,                                      AS "Shop_Order_ID_Suffix",

    -- ── Item ──────────────────────────────────────────────
    h.ID_ITEM_PAR                                  AS "Item ID_Parent SKU",
    h.DESCR_ITEM_1,
    h.DESCR_ITEM_2,

    -- ── Work Centre Resolution (SHPORD_OPER + ROUTMS_OPER) ──
    CASE
        WHEN rto.RTOWC IS NULL THEN od.SOPERWC
        ELSE rto.RTOWC
    END                                             AS SOWC,
    od.SOPERWC,
    rto.RTOWC,
    rto.ID_RTE,
    CASE
        WHEN rto.RTOWC IS NULL THEN od.HR_LABOR_SF
        ELSE rto.RTO_HR_MACH_SR
    END                                             AS MACH_SR_LABOR_SF,
    od.HR_LABOR_SF,
    rto.RTO_HR_MACH_SR,

    -- ── Operation Detail ──────────────────────────────────
    od.ID_OPER,
    od.STAT_REC_OPER,
    od.O_QTY_CMPL,
    od.O_QTY_ORD,

    -- ── Order Classification ───────────────────────────────
    h.TYPE_ORD,
    h.CODE_CAT_COST,

    -- ── Status / Control ──────────────────────────────────
    h.STAT_REC_SO,
    h.FLAG_CMPL_SO,
    h.FLAG_REL,
    h.FLAG_EXPEDITED_SO,
    h.FLAG_STAT_ARCHIVE,

    -- ── Header Quantities ─────────────────────────────────
    h.QTY_ORD,
    h.QTY_CMPL,
    h.QTY_ORD - h.QTY_CMPL                         AS QTY_REMAINING,

    -- ── Header Dates (SHOP_ORDER_INFO-aligned) ────────────
    h.DATE_ADD,
    h.DATE_DUE_ORD,
    h.DATE_START_OPER_1ST,
    h.TIME_START_OPER_1ST,

    -- ── Header Dates (enriched) ───────────────────────────
    h.SO_DATE_START_ORDERED,
    h.SO_DATE_START_PLANNED,
    h.SO_DATE_DUE_PLANNED,
    h.SO_DATE_COMPLETED,
    h.SO_DATE_LAST_CHANGED,

    -- ── Approval ──────────────────────────────────────────
    sap.SA_SO_AND_SUFX,
    sap.DATE_APPROVED,
    sap.DATETIME_APPROVED,
    sap.TIME_APPROVED,

    -- ── Reference / Linkage ───────────────────────────────
    h.ID_BUYER,
    h.ID_PROJ,
    h.ID_EST,
    h.ID_DRAW,
    h.ID_REV_DRAW,


    -- ── Pre-Production Floor ──────────────────────────────
    sop.SOFROMPREPROD,

    -- ── Operation Aggregates (SHPORD_OPER) ────────────────
    o.oper_count,
    o.oper_labor_hrs_planned,
    o.oper_labor_hrs_actual,
    o.oper_machine_hrs_planned,
    o.oper_machine_hrs_actual,
    o.oper_earliest_start_date,
    o.oper_latest_due_date,

    -- ── Material Aggregates (SHPORD_MATL) ─────────────────
    m.matl_line_count,
    m.matl_total_qty_per,
    m.matl_total_qty_allocated,
    m.matl_total_qty_issued,

    -- ── Work Centre Efficiency Metrics ────────────────────
    wceff.SUM_ACTUAL_MINS,
    wceff.SUM_EARNED_MINS,
    wceff.SUM_AVAIL_MINS,
    wceff.SUM_INDIR_MINS,
    wceff.SUM_SAMPLE_MINS,
    wceff.WORK_CENTER_LOCATION,
    wceff.EMPLOYEE_LOCATION,
    wceff.WORK_CENTER_DESCRIPTION,

    -- ── Work Centre Staffing ──────────────────────────────
    wceff.FIRST_SHIFT_FT,
    wceff.SECOND_SHIFT_FT,
    wceff.SECOND_SHIFT_PT,
    wceff.INACTIVE,
    wceff.FIRST_SHIFT_FT_EMPLOYEES,
    wceff.SECOND_SHIFT_FT_EMPLOYEES,
    wceff.SECOND_SHIFT_PT_EMPLOYEES,
    wceff.FIRST_SHIFT_TEAMS,
    wceff.SECOND_SHIFT_TEAMS

-- SO_HDR: Shop order header (1 row per shop order)
FROM SO_HDR h

-- Core join: changes grain to 1 row per operation
INNER JOIN SO_OPER_DETAIL od
    ON  h.ID_SO    = od.ID_SO
    AND h.SUFX_SO  = od.SUFX_SO

-- ROUTMS_OPER: Brings in work centre overrides at the operation level (1 row per operation, but may be null if no override exists)
LEFT JOIN RTE_OPER rto
    ON  h.ID_ITEM_PAR = rto.ID_ITEM
    AND od.ID_OPER    = rto.ID_OPER

-- SO_APPROVE: Brings in approval dates (1 row per shop order, but may be null if no approval record exists)
-- Approval events (earliest per shop order)
LEFT JOIN SO_APPROVE sap
    ON  CONCAT(LTRIM(h.ID_SO), h.SUFX_SO) = sap.SA_SO_AND_SUFX


-- Pre-production floor indicator
-- PREPOD: Identifies shop orders currently on the production floor (1 row per shop order, but may be null if not currently on the floor)
LEFT JOIN PREPROD sop
    ON  LTRIM(h.ID_SO) = sop.SOFROMPREPROD
    AND h.SUFX_SO      = sop.SUFX_SO


-- Operation aggregates (denormalized onto each operation row)
-- SO_OPER_AGG: Brings in operation-level metrics rolled up to the shop order header level (1 row per shop order, but may be null if no operations exist for the order)
LEFT JOIN SO_OPER_AGG o
    ON  h.ID_LOC   = o.ID_LOC
    AND h.ID_SO    = o.ID_SO
    AND h.SUFX_SO  = o.SUFX_SO

-- Material aggregates
-- SO_MATL_AGG: Brings in material/component metrics rolled up to the shop order header level (1 row per shop order, but may be null if no materials/components exist for the order)
LEFT JOIN SO_MATL_AGG m
    ON  h.ID_LOC   = m.ID_LOC
    AND h.ID_SO    = m.ID_SO
    AND h.SUFX_SO  = m.SUFX_SO

-- Work centre efficiency & staffing
-- WORK_CENTER_MINUTES_EMPLOYEE_SUMMARY: Brings in work centre efficiency metrics and staffing details (1 row per work centre, but may be null if no matching work centre record exists)
LEFT JOIN SILVER_DATA.TCM_SILVER.WORK_CENTER_MINUTES_EMPLOYEE_SUMMARY wceff
    ON  TRIM(CASE WHEN rto.RTOWC IS NULL THEN od.SOPERWC ELSE rto.RTOWC END) = TRIM(wceff.WORK_CENTER)
    AND TRIM(h.ID_LOC)                                                        = TRIM(wceff.WORK_CENTER_LOCATION)

WHERE
    -- Active shop order statuses
    h.STAT_REC_SO       IN ('A', 'R', 'S', 'U')
    -- Active operation statuses
    AND od.STAT_REC_OPER  IN ('P', 'R', 'A', 'C')
    -- Only standard routing or unrouted operations
    AND (rto.ID_RTE = 'TSS' OR rto.ID_RTE IS NULL)
    -- Exclude pre-production and sample orders
    AND h.ID_SO NOT LIKE 'PROD%'
    AND h.ID_SO NOT LIKE 'SAMPLE%'
    -- Limit to orders added since 2020
    AND h.DATE_ADD >= '2020-01-01'
    -- Exclude administrative / overhead work centres
    AND CASE WHEN rto.RTOWC IS NULL THEN od.SOPERWC ELSE rto.RTOWC END
        NOT IN ('900', '950', '1100', '1200', '1300', '1400', '7699', '7950', '8000', '7990');
