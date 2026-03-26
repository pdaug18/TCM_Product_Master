CREATE OR REPLACE DYNAMIC TABLE SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_TABLE
    TARGET_LAG   = 'DOWNSTREAM'
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    WAREHOUSE    = ELT_DEFAULT
AS

/* ============================================================
   SO_HDR — Core shop order header fields
   Grain: 1 row per (ID_LOC, ID_SO, SUFX_SO)
   Source: BRONZE_DATA.TCM_BRONZE.SHPORD_HDR
   ============================================================ */
WITH SO_HDR AS (
    SELECT
        -- Business Key
        h.ID_LOC,
        h.ID_SO,
        h.SUFX_SO,

        -- Item Identifiers
        h.ID_ITEM_PAR,
        TRIM(COALESCE(h.DESCR_ITEM_1, '') || ' ' || COALESCE(h.DESCR_ITEM_2, '')) AS SO_ITEM_DESCRIPTION,

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

        -- Dates
        h.DATE_ADD                  AS SO_DATE_CREATED,
        h.DATE_START_ORD            AS SO_DATE_START_ORDERED,
        h.DATE_DUE_ORD              AS SO_DATE_DUE_ORDERED,
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

    FROM BRONZE_DATA.TCM_BRONZE."SHPORD_HDR_Bronze" as h
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
    --     -- FROM BRONZE_DATA.TCM_BRONZE."SHPORD_JOB_Bronze" AS j
    --     FROM nsa.SHPORD_JOB AS j
    -- ) ranked
    -- WHERE RN = 1
-- ),

/* ============================================================
   SO_OPER_AGG — Operation metrics rolled up to header level
   Grain: 1 row per (ID_LOC, ID_SO, SUFX_SO)
   Source: BRONZE_DATA.TCM_BRONZE.SHPORD_OPER
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
   FINAL SELECT — Header-grain master table
   All OPER / MATL data aggregated; JOB deduped to primary row
   ============================================================ */
SELECT
    -- ── Business Key ──────────────────────────────────────
    h.ID_LOC,
    h.ID_SO,
    h.SUFX_SO,

    -- ── Item ──────────────────────────────────────────────
    h.ID_ITEM_PAR,
    h.SO_ITEM_DESCRIPTION,

    -- ── Order Classification ───────────────────────────────
    h.TYPE_ORD,
    h.CODE_CAT_COST,

    -- ── Status / Control ──────────────────────────────────
    h.STAT_REC_SO,
    h.FLAG_CMPL_SO,
    h.FLAG_REL,
    h.FLAG_EXPEDITED_SO,
    h.FLAG_STAT_ARCHIVE,

    -- ── Quantities ────────────────────────────────────────
    h.QTY_ORD,
    h.QTY_CMPL,
    h.QTY_ORD - h.QTY_CMPL                     AS QTY_REMAINING,

    -- ── Header Dates ──────────────────────────────────────
    h.SO_DATE_CREATED,
    h.SO_DATE_START_ORDERED,
    h.SO_DATE_DUE_ORDERED,
    h.SO_DATE_START_PLANNED,
    h.SO_DATE_DUE_PLANNED,
    h.SO_DATE_COMPLETED,
    h.SO_DATE_LAST_CHANGED,

    -- ── Reference / Linkage ───────────────────────────────
    h.ID_BUYER,
    h.ID_PROJ,
    h.ID_EST,
    h.ID_DRAW,
    h.ID_REV_DRAW,

    -- ── Job / Order Linkage (SHPORD_JOB) ──────────────────
    -- j.ID_JOB,
    -- j.SEQ_JOB,
    -- j.JOB_ID_CUST,
    -- j.ID_ORD,
    -- j.SEQ_LINE_ORD,
    -- j.JOB_DATE_NEED,
    -- j.JOB_DATE_PROM,
    -- j.PCT_SPLIT_JOB,
    -- j.JOB_CODE_CAT_COST,
    -- j.FLAG_POST_JOB,

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
    m.matl_total_qty_issued

FROM SO_HDR h
-- LEFT JOIN SO_JOB      j ON  h.ID_LOC   = j.ID_LOC
--                         AND h.ID_SO    = j.ID_SO
--                         AND h.SUFX_SO  = j.SUFX_SO
LEFT JOIN SO_OPER_AGG o ON  h.ID_LOC   = o.ID_LOC
                        AND h.ID_SO    = o.ID_SO
                        AND h.SUFX_SO  = o.SUFX_SO
LEFT JOIN SO_MATL_AGG m ON  h.ID_LOC   = m.ID_LOC
                        AND h.ID_SO    = m.ID_SO
                        AND h.SUFX_SO  = m.SUFX_SO;
