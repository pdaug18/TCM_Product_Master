-- ============================================================
-- Validation: Open Orders with Prices
-- Report:     rpt_NSA_Open_Orders_with_Prices2
-- Source:     SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
-- Purpose:    Match TOTAL_ORDERS and TOTAL_LINE_ITEMS against
--             the report snapshot cached 2026-04-06 @ 09:12 AM
-- ============================================================
-- Snapshot Targets:
--   Total # of Orders:      298
--   Total # of Line Items:  983
--   Total Open Value:       $700,035.64
-- ============================================================
--
-- REPORT PARAMETER → FILTER MAPPING
-- ──────────────────────────────────────────────────────────────────────────────
--  Report Control               Snapshot Value       Filter / Note
-- ──────────────────────────────────────────────────────────────────────────────
--  Starting Add Date            20260301             ORD_DATE_CREATED >= '2026-03-01'
--  Ending Add Date              20260331             ORD_DATE_CREATED <  '2026-04-01'
--  Starting TCM Due Date        20260301             DATE_RQST >= '2026-03-01'
--  Ending TCM Due Date          20260331             DATE_RQST <  '2026-04-01'
--  Starting Prod Cat            0                    CODE_CAT_PRDT >= '0'
--  Ending Prod Cat              Z                    CODE_CAT_PRDT <= 'Z'
--  Starting Item #              0                    ID_ITEM >= '0'
--  Ending Item #                Z                    ID_ITEM <= 'Z'
--  Starting Customer Sold to #  1                    ID_CUST_SOLDTO >= '1'
--  Ending Customer Sold to #    Z                    ID_CUST_SOLDTO <= 'Z'
--  Ack. End Date Cutoff         12/31/2999           APPROX via DATE_PROM (DATE_ACKN_LAST not
--                                                    projected in silver table); value = far future
--                                                    so this adds no effective restriction
--  Minimum Order Total Value $  0                    AMT_ORD_TOTAL >= 0 (no effective restriction)
--  Order Num                    NULL (checked)       No ID_ORD restriction
--  Carrier                      ALL                  No CODE_SHIP_VIA_CP restriction
--  Main Material                NULL (checked)       No material restriction
--  Mfg / Purch                  Purchased,Manufact   Both P and M included → no net restriction
--                                                    (FLAG_SOURCE not in silver table; see commented
--                                                     subquery in Section 1 for future tightening)
--  SO Created?                  Both                 No ID_SO restriction
--  Open lines                   (implicit)           QTY_OPEN > 0
-- ──────────────────────────────────────────────────────────────────────────────


/* ============================================================
   SECTION 1 — FINAL VALIDATION COUNT
   Run this to compare directly against the snapshot targets.
   Expected: TOTAL_ORDERS = 298 | TOTAL_LINE_ITEMS = 983
             TOTAL_OPEN_NET_VALUE ≈ $700,035.64
   ============================================================ */
SELECT
    COUNT(DISTINCT ID_ORD)       AS TOTAL_ORDERS,
    COUNT(*)                     AS TOTAL_LINE_ITEMS,
    ROUND(SUM(OPEN_NET_AMT), 2)  AS TOTAL_OPEN_NET_VALUE
FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
WHERE 1 = 1

    -- Open lines only
    AND QTY_OPEN > 0

    -- [Starting Add Date / Ending Add Date]
    AND ORD_DATE_CREATED >= '2026-03-01'::DATE
    AND ORD_DATE_CREATED <  '2026-04-01'::DATE
    -- ORD_DATE_CREATED BETWEEN '2026-03-01'::DATE AND ORD_DATE_CREATED <  '2026-04-01'::DATE

    -- [Starting TCM Due Date / Ending TCM Due Date]
    AND DATE_RQST >= '2026-03-01'::DATE
    AND DATE_RQST <  '2026-04-01'::DATE

    -- [Starting Prod Cat = 0 / Ending Prod Cat = Z]
    AND CODE_CAT_PRDT >= '0'
    AND CODE_CAT_PRDT <= 'Z'

    -- [Starting Item # = 0 / Ending Item # = Z]
    AND ID_ITEM >= '0'
    AND ID_ITEM <= 'Z'

    -- [Starting Customer Sold to # = 1 / Ending Customer Sold to # = Z]
    AND ID_CUST_SOLDTO >= '1'
    AND ID_CUST_SOLDTO <= 'Z'

    -- [Minimum Order Total Value $ = 0] — no effective restriction
    AND AMT_ORD_TOTAL >= 0

    -- [Ack. End Date Cutoff = 12/31/2999] — APPROXIMATION (DATE_ACKN_LAST not in silver table)
    --   Using DATE_PROM as the closest proxy; far-future cutoff = no rows excluded.
    --   To use the exact field, add DATE_ACKN_LAST to MASTER_ORDERS_TABLE projection first.
    AND DATE_PROM <= '2999-12-31'::DATE

    -- [Mfg / Purch = Purchased, Manufactured] — both P & M → no net restriction
    --   Uncomment below to restrict to a specific source type when FLAG_SOURCE join is available:
    -- AND EXISTS (
    --     SELECT 1
    --     FROM SILVER_DATA.TCM_SILVER.MASTER_ITEM_INVENTORY mii
    --     WHERE mii.ID_ITEM    = MASTER_ORDERS_TABLE.ID_ITEM
    --       AND mii.FLAG_SOURCE IN ('P', 'M')   -- P=Purchased, M=Manufactured
    -- )

    -- [Carrier = ALL]      → no CODE_SHIP_VIA_CP restriction
    -- [Order Num = NULL]   → no ID_ORD restriction
    -- [Main Material = NULL] → no material restriction
    -- [SO Created = Both]  → no ID_SO restriction
;


/* ============================================================
   SECTION 2 — STAGED DIAGNOSTICS (FILTER FUNNEL)
   Run this to isolate where counts diverge from the snapshot.
   Each stage adds one additional filter on top of the previous.
   ============================================================ */
SELECT
    stage,
    COUNT(DISTINCT ID_ORD)       AS TOTAL_ORDERS,
    COUNT(*)                     AS TOTAL_LINE_ITEMS,
    ROUND(SUM(OPEN_NET_AMT), 2)  AS TOTAL_OPEN_NET_VALUE
FROM (

    -- Stage 1: Entire silver table (baseline — no filters)
    SELECT '1_ALL_ROWS'::VARCHAR AS stage, ID_ORD, OPEN_NET_AMT
    FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE

    UNION ALL

    -- Stage 2: Open lines only  [QTY_OPEN > 0]
    SELECT '2_OPEN_LINES', ID_ORD, OPEN_NET_AMT
    FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
    WHERE QTY_OPEN > 0

    UNION ALL

    -- Stage 3: + Add Date range  [ORD_DATE_CREATED 2026-03-01 – 2026-03-31]
    SELECT '3_ADD_DATE', ID_ORD, OPEN_NET_AMT
    FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
    WHERE QTY_OPEN > 0
      AND ORD_DATE_CREATED >= '2026-03-01'::DATE
      AND ORD_DATE_CREATED <  '2026-04-01'::DATE

    UNION ALL

    -- Stage 4: + TCM Due Date range  [DATE_RQST 2026-03-01 – 2026-03-31]
    SELECT '4_DUE_DATE', ID_ORD, OPEN_NET_AMT
    FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
    WHERE QTY_OPEN > 0
      AND ORD_DATE_CREATED >= '2026-03-01'::DATE
      AND ORD_DATE_CREATED <  '2026-04-01'::DATE
      AND DATE_RQST >= '2026-03-01'::DATE
      AND DATE_RQST <  '2026-04-01'::DATE

    UNION ALL

    -- Stage 5: + Range filters  [Prod Cat 0-Z / Item 0-Z / Sold-to 1-Z / Min Total >= 0]
    SELECT '5_RANGE_FILTERS', ID_ORD, OPEN_NET_AMT
    FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
    WHERE QTY_OPEN > 0
      AND ORD_DATE_CREATED >= '2026-03-01'::DATE
      AND ORD_DATE_CREATED <  '2026-04-01'::DATE
      AND DATE_RQST >= '2026-03-01'::DATE
      AND DATE_RQST <  '2026-04-01'::DATE
      AND CODE_CAT_PRDT  >= '0' AND CODE_CAT_PRDT  <= 'Z'
      AND ID_ITEM        >= '0' AND ID_ITEM        <= 'Z'
      AND ID_CUST_SOLDTO >= '1' AND ID_CUST_SOLDTO <= 'Z'
      AND AMT_ORD_TOTAL  >= 0

    UNION ALL

    -- Stage 6: + Ack End Date Cutoff approximation  [DATE_PROM proxy for 12/31/2999]
    SELECT '6_ACK_CUTOFF_APPROX', ID_ORD, OPEN_NET_AMT
    FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
    WHERE QTY_OPEN > 0
      AND ORD_DATE_CREATED >= '2026-03-01'::DATE
      AND ORD_DATE_CREATED <  '2026-04-01'::DATE
      AND DATE_RQST >= '2026-03-01'::DATE
      AND DATE_RQST <  '2026-04-01'::DATE
      AND CODE_CAT_PRDT  >= '0' AND CODE_CAT_PRDT  <= 'Z'
      AND ID_ITEM        >= '0' AND ID_ITEM        <= 'Z'
      AND ID_CUST_SOLDTO >= '1' AND ID_CUST_SOLDTO <= 'Z'
      AND AMT_ORD_TOTAL  >= 0
      AND DATE_PROM <= '2999-12-31'::DATE

) funnel
GROUP BY stage
ORDER BY stage
;