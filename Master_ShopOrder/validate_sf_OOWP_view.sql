select * from GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
where alt_stk is not null or alt_stk != ''
/*
DATAREFRESHTIMESTAMP	OPEN_NET_AMT	ID_CUST_SOLDTO	SHIP_COMPLETE_FLAG	DATE_PICK_LAST	WorkingDaysSinceLastPicked	FLAG_PICK	FLAG_ACKN	AMT_ORD_TOTAL	ID_SLSREP_1	DESCR_SHIP_VIA	DATE_RQST	DATE_PROM	DATE_ORD	DATE_CALC_START	DATE_CALC_END	ALT_STK	FLAG_MO	ID_ITEM	ID_ITEM_COMP	STOCK_STATUS	ID_ORD	ID_USER_ADD	Date_Order_Created	SEQ_LINE_ORD	ID_SO_ODBC	Item_Vertical	CODE_USER_1	ID_REV_DRAW	ID_CARRIER	QTY_OPEN	FLAG_STAT_ITEM	OL_FLAG_STK	IL_FLAG_STK	RBN_WC	QTY_REL	QTY_START	QTY_PRESEW	QTY_REL_PND	QTY_START_PND	SBNB	QTY_ONHD	QTY_ALLOC	QTY_ONORD	BIN_PRIM	FLAG_SOURCE	FLAG_TRACK_BIN	LEVEL_ROP	STK_TEST	CODE_UM_PRICE	NAME_CUST	STAT_REC_SO	ID_SO	QTY_SHIP_TOTAL	ID_SHIP	CODE_STAT_ORD	ID_PO_CUST	NUM_SHIPMENTS	NUM_INVCS	COUNTER
2026-04-08 05:50:39.226 -0700	0	901987	Y	2025-01-09 00:00:00.000			A	0	50	UPS GRD    R	2999-01-01 00:00:00.000	2025-01-10 00:00:00.000	2025-01-08 00:00:00.000	2025-01-09 00:00:00.000	2025-01-10 00:00:00.000		N	H61RK	null	3-FABRIC	774073	DB3	2025-01-08 00:00:00.000	1		INDUSTRIAL PPE	RK	null	DT-FC-SHIP4	0	A	S	S		1800	null	null	0	0	5653	384	0	1800	2-3G-01-03-H	Manufactured	0	1824	1		SAFEWERKS	null	null	3	938096	A	P002277	1	1	1
 */

 select * from SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
where o.LINE_COMMENT_NOTE is not null
and o.ID_ORD = '774073';

/*
Validation checklist for OPEN_ORDERS_WITH_PRICES
Run each test case and mark PASS/FAIL.

Recommended scope: filter to recent data during iterative testing.
Adjust the date below as needed.

How to use this file:
- Each TC_xx query validates one part of the view against its source table logic.
- For mismatch-count queries, the expected result is usually 0.
- For distribution / aggregate queries, review the output for reasonableness and compare to source-side totals.
- For duplicate checks, investigate any row returned unless the duplication is expected from component expansion.
*/

/* ============================================================
   Global sample window
   ============================================================ */
-- What this tests:
-- Establishes the sample population you are validating.
-- Why it matters:
-- All subsequent checks should be interpreted against the same date window.
-- TC_00: Baseline rowcount in sample window
SELECT
	COUNT(*) AS row_count_sample
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
WHERE "Date_Order_Created" >= '2025-01-01';

/* ============================================================
   SOURCE: MASTER_ORDERS_TABLE
   ============================================================ */
-- What this section tests:
-- Verifies columns and business flags that originate from MASTER_ORDERS_TABLE.
-- Why it matters:
-- This confirms the base order-line grain is intact and direct carry-through fields were not altered by downstream joins.

-- What this tests:
-- Confirms the view still maps back to the source order-line keys.
-- Why it matters:
-- If this join is weak, nearly every downstream validation becomes unreliable.
-- TC_01: Key mapping coverage (ID_ORD + SEQ_LINE_ORD)
SELECT
	COUNT(*) AS matched_rows
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
INNER JOIN SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
	ON v.ID_ORD = o.ID_ORD
   AND v.SEQ_LINE_ORD = o.SEQ_LINE_ORD
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Checks direct 1:1 field parity for fields expected to flow straight from MASTER_ORDERS_TABLE.
-- Why it matters:
-- Any mismatch here usually indicates bad joins, aliasing mistakes, or unintended transformations.
-- TC_02: Direct field parity mismatches
SELECT
	COUNT_IF(NVL(v.OPEN_NET_AMT, 0) <> NVL(o.OPEN_NET_AMT, 0)) AS mm_open_net_amt,
	COUNT_IF(NVL(v.AMT_ORD_TOTAL, 0) <> NVL(o.AMT_ORD_TOTAL, 0)) AS mm_amt_ord_total,
	COUNT_IF(NVL(v.ID_SO_ODBC, '') <> NVL(o.ID_SO, '')) AS mm_id_so,
	COUNT_IF(NVL(v.CODE_STAT_ORD, '') <> NVL(o.CODE_STAT_ORD, '')) AS mm_code_stat_ord,
	COUNT_IF(NVL(v.ID_PO_CUST, '') <> NVL(o.ID_PO_CUST, '')) AS mm_id_po_cust
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
INNER JOIN SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
	ON v.ID_ORD = o.ID_ORD
   AND v.SEQ_LINE_ORD = o.SEQ_LINE_ORD
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Validates the output remapping of numeric order flags into display values P and A.
-- Why it matters:
-- These are easy to break during refactors because the view transforms the raw source values.
-- TC_03: FLAG_PICK and FLAG_ACKN transformation checks
SELECT
	COUNT_IF(TO_VARCHAR(o.FLAG_PICK) = '2' AND NVL(v.FLAG_PICK, '') <> 'P') AS bad_flag_pick_expected_p,
	COUNT_IF(TO_VARCHAR(o.FLAG_PICK) <> '2' AND NVL(v.FLAG_PICK, '') <> '') AS bad_flag_pick_expected_blank,
	COUNT_IF(TO_VARCHAR(o.FLAG_ACKN) = '2' AND NVL(v.FLAG_ACKN, '') <> 'A') AS bad_flag_ackn_expected_a,
	COUNT_IF(TO_VARCHAR(o.FLAG_ACKN) <> '2' AND NVL(v.FLAG_ACKN, '') <> '') AS bad_flag_ackn_expected_blank
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
INNER JOIN SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
	ON v.ID_ORD = o.ID_ORD
   AND v.SEQ_LINE_ORD = o.SEQ_LINE_ORD
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Recomputes ship_complete_flag from LINE_COMMENT_NOTE using the source-table rules.
-- Why it matters:
-- This validates a derived text-based business rule rather than a direct source column copy.
-- TC_04: ship_complete_flag logic check
SELECT
	COUNT_IF(
		o.LINE_COMMENT_NOTE ILIKE '%SHIP%COMPLETE%'
		AND o.LINE_COMMENT_NOTE NOT ILIKE '%LINE%'
		AND NVL(v.SHIP_COMPLETE_FLAG, '') <> 'Y'
	) AS bad_ship_complete_expected_y,
	COUNT_IF(
		NOT (o.LINE_COMMENT_NOTE ILIKE '%SHIP%COMPLETE%'
		AND o.LINE_COMMENT_NOTE NOT ILIKE '%LINE%')
		AND NVL(v.SHIP_COMPLETE_FLAG, '') = 'Y'
	) AS bad_ship_complete_unexpected_y
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
INNER JOIN SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
	ON v.ID_ORD = o.ID_ORD
   AND v.SEQ_LINE_ORD = o.SEQ_LINE_ORD
WHERE v."Date_Order_Created" >= '2025-01-01';

/* ============================================================
   SOURCE: MASTER_SHOPORDER_WC_TABLE
   ============================================================ */
-- What this section tests:
-- Validates shop-order-derived attributes and qty staging logic that now source from MASTER_SHOPORDER_WC_TABLE.
-- Why it matters:
-- Recent refactoring moved qty logic away from bronze SHPORD tables into this master table.

-- What this tests:
-- Detects whether the view produces more than one row per order-line key.
-- Why it matters:
-- Unexpected duplicates usually mean one of the joins is multiplying rows.
-- TC_05: Duplicate detector at order-line grain
SELECT
	ID_ORD,
	SEQ_LINE_ORD,
	COUNT(*) AS row_count
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
WHERE "Date_Order_Created" >= '2025-01-01'
GROUP BY ID_ORD, SEQ_LINE_ORD
HAVING COUNT(*) > 1
ORDER BY row_count DESC, ID_ORD, SEQ_LINE_ORD;

-- What this tests:
-- Compares specific wc-sourced fields in the view to the shop-order master.
-- Why it matters:
-- Confirms the current join path to MASTER_SHOPORDER_WC_TABLE is returning the intended values.
-- TC_06: wc sourced fields parity spot-check counts
SELECT
	COUNT_IF(NVL(v.ID_REV_DRAW, '') <> NVL(wc.ID_REV_DRAW, '')) AS mm_id_rev_draw,
	COUNT_IF(NVL(v.STAT_REC_SO, '') <> NVL(wc.STAT_REC_SO, '')) AS mm_stat_rec_so
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE wc
	ON TRIM(v.ID_SO_ODBC) = TRIM(wc."ShopOrder#")
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Summarizes the source-side WPS and PCC quantities used to drive Qty_Start and Qty_presew.
-- Why it matters:
-- This gives a baseline to compare against the view’s rolled-up qty behavior.
-- TC_07: Qty pre-sew components from WC source (WPS / PCC) aggregate reconciliation
WITH wps AS (
	SELECT ID_ITEM_PAR, SUM(QTY_REMAINING) AS qty
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
	WHERE STAT_REC_SO = 'S'
	GROUP BY ID_ITEM_PAR
),
pcc AS (
	SELECT ID_ITEM_PAR, SUM(QTY_REMAINING) AS qty
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
	WHERE STAT_REC_SO = 'S'
	  AND ID_OPER = 3999
	  AND STAT_REC_OPER = 'C'
	GROUP BY ID_ITEM_PAR
)
SELECT
	(SELECT SUM(qty) FROM wps) AS sum_wps_qty,
	(SELECT SUM(qty) FROM pcc) AS sum_pcc_qty;

/* ============================================================
   SOURCE: ITEM_INVENTORY_MASTER
   ============================================================ */
-- What this section tests:
-- Verifies inventory and stocking columns sourced through ITEM_INVENTORY_MASTER, including location-aware joins.
-- Why it matters:
-- These columns are sensitive to item/location join quality and component fallback behavior.

-- What this tests:
-- Measures how often the inventory join fails to populate key inventory columns.
-- Why it matters:
-- High null rates can indicate join mismatches on item or location.
-- TC_08: Location-aware inventory join coverage
SELECT
	COUNT(*) AS total_rows,
	COUNT_IF(v.QTY_ONHD IS NULL) AS null_qty_onhd,
	COUNT_IF(v.QTY_ALLOC IS NULL) AS null_qty_alloc,
	COUNT_IF(v.QTY_ONORD IS NULL) AS null_qty_onord
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Checks parity of direct inventory columns between the view and ITEM_INVENTORY_MASTER.
-- Why it matters:
-- These should generally match exactly unless the component-aware fallback intentionally changes the result.
-- TC_09: Inventory parity mismatch counts
SELECT
	COUNT_IF(NVL(v.QTY_ONHD, 0) <> NVL(inv."Qty_On_Hand", 0)) AS mm_qty_onhd,
	COUNT_IF(NVL(v.QTY_ALLOC, 0) <> NVL(inv."Qty_Allocated", 0)) AS mm_qty_alloc,
	COUNT_IF(NVL(v.QTY_ONORD, 0) <> NVL(inv."Qty_On_Order", 0)) AS mm_qty_onord,
	COUNT_IF(NVL(v.LEVEL_ROP, 0) <> NVL(inv."Item_Inventory_Reorder_Point", 0)) AS mm_level_rop
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
	ON v.ID_ORD = o.ID_ORD AND v.SEQ_LINE_ORD = o.SEQ_LINE_ORD
LEFT JOIN SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER inv
	ON o.ID_ITEM = inv."Product_ID_SKU"
   AND o.ID_LOC = inv."Location_ID"
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Recomputes stk_test from reorder point and stock flag logic.
-- Why it matters:
-- This confirms the derived inventory status rule still matches the implemented formula.
-- TC_10: stk_test formula validation
SELECT
	COUNT_IF(
		v.STK_TEST <>
		CASE
			WHEN NVL(v.LEVEL_ROP, 0) > 1 AND NVL(v.IL_FLAG_STK, v.OL_FLAG_STK) = 'S' THEN 1
			ELSE 0
		END
	) AS bad_stk_test
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Shows how often BIN_PRIM is blank in the final output.
-- Why it matters:
-- Helps identify missing component/item inventory rows or incomplete bin setup in the source.
-- TC_11: BIN_PRIM populated rate
SELECT
	COUNT(*) AS total_rows,
	COUNT_IF(NVL(v.BIN_PRIM, '') = '') AS blank_bin_prim
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
WHERE v."Date_Order_Created" >= '2025-01-01';

/* ============================================================
   SOURCE: MASTER_PRODUCT_TABLE
   ============================================================ */
-- What this section tests:
-- Validates product attributes sourced from MASTER_PRODUCT_TABLE, including the PRDSTR FG commodity logic dependency.
-- Why it matters:
-- This confirms the view is consuming the curated product master rather than raw bronze attributes.

-- What this tests:
-- Compares product attributes in the view with the corresponding product master fields.
-- Why it matters:
-- These should be clean 1:1 mappings and are a strong indicator that the product join is stable.
-- TC_12: Product attribute parity checks
SELECT
	COUNT_IF(NVL(v.ITEM_VERTICAL, '') <> NVL(mp."Item_Vertical", '')) AS mm_item_vertical,
	COUNT_IF(NVL(v.CODE_USER_1, '') <> NVL(mp."CODE_USER_1", '')) AS mm_code_user_1,
	COUNT_IF(NVL(v.FLAG_STAT_ITEM, '') <> NVL(mp."Item Status_Child Active Status", '')) AS mm_flag_stat_item,
	COUNT_IF(NVL(v.RBN_WC, '') <> NVL(mp."Item_Work Center_Rubin", '')) AS mm_rbn_wc,
	COUNT_IF(NVL(v.CODE_UM_PRICE, '') <> NVL(mp."Unit_of_Measure_Price", '')) AS mm_code_um_price
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE mp
	ON v.ID_ITEM = mp."Item ID_Child SKU"
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Shows the output mix of derived STOCK_STATUS values.
-- Why it matters:
-- Useful for anomaly detection after logic changes, especially if one bucket suddenly spikes or disappears.
-- TC_13: STOCK_STATUS output distribution
SELECT
	STOCK_STATUS,
	COUNT(*) AS rows
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
WHERE "Date_Order_Created" >= '2025-01-01'
GROUP BY STOCK_STATUS
ORDER BY rows DESC;

/* ============================================================
   SOURCE: MASTER_SHIPMENT_TABLE
   ============================================================ */
-- What this section tests:
-- Validates shipment-line, order-level shipment counts, and SBNB logic against MASTER_SHIPMENT_TABLE.
-- Why it matters:
-- Shipment joins are a common source of row inflation and count mismatches.

-- What this tests:
-- Recomputes ship-line max values used in the SHIP_LINE CTE.
-- Why it matters:
-- Confirms the view returns the correct representative shipment/carrier per order line.
-- TC_14: SHIP_LINE parity (MAX ship and carrier per order line)
WITH src AS (
	SELECT
		ID_ORD,
		SEQ_LINE_ORD,
		MAX(ID_SHIP) AS max_id_ship,
		MAX(ID_CARRIER) AS max_id_carrier
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
	GROUP BY ID_ORD, SEQ_LINE_ORD
)
SELECT
	COUNT_IF(NVL(v.ID_SHIP, -1) <> NVL(src.max_id_ship, -1)) AS mm_id_ship,
	COUNT_IF(NVL(v.ID_CARRIER, '') <> NVL(src.max_id_carrier, '')) AS mm_id_carrier
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
LEFT JOIN src
	ON v.ID_ORD = src.ID_ORD
   AND v.SEQ_LINE_ORD = src.SEQ_LINE_ORD
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Recomputes distinct shipment and invoice counts per order.
-- Why it matters:
-- Confirms order-level shipment rollups are not distorted by downstream joins.
-- TC_15: SHIP_ORD parity (distinct shipment and invoice counts)
WITH src AS (
	SELECT
		ID_ORD,
		COUNT(DISTINCT ID_SHIP) AS num_shipments,
		COUNT(DISTINCT ID_INVC) AS num_invcs
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
	GROUP BY ID_ORD
)
SELECT
	COUNT_IF(NVL(v.NUM_SHIPMENTS, 0) <> NVL(src.num_shipments, 0)) AS mm_num_shipments,
	COUNT_IF(NVL(v.NUM_INVCS, 0) <> NVL(src.num_invcs, 0)) AS mm_num_invcs
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
LEFT JOIN src
	ON v.ID_ORD = src.ID_ORD
WHERE v."Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Sanity-checks SBNB output and gives a place to compare against the source-side unconfirmed shipment totals.
-- Why it matters:
-- SBNB is component-aware and location-aware, so it is important to watch for negative or obviously inflated values.
-- TC_16: SBNB parity by component-aware item + location
WITH src AS (
	SELECT
		ID_ITEM,
		ID_LOC,
		SUM(QTY_SHIP) AS sbnb
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
	WHERE COALESCE(FLAG_CONFIRM_SHIP, 0) <> 1
	GROUP BY ID_ITEM, ID_LOC
)
SELECT
	COUNT_IF(NVL(v.SBNB, 0) < 0) AS bad_negative_sbnb,
	COUNT(*) AS checked_rows
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES v
WHERE v."Date_Order_Created" >= '2025-01-01';

/* ============================================================
   SOURCE: DIM_CALENDAR
   ============================================================ */
-- What this section tests:
-- Validates business-day-derived fields that depend on GOLD_DATA.DIM.DIM_CALENDAR.
-- Why it matters:
-- Calendar logic is easy to get subtly wrong even when the SQL compiles and returns values.

-- What this tests:
-- Ensures working-day count only appears when the pick flag logic says it should.
-- Why it matters:
-- This confirms the display logic and source flag interpretation are aligned.
-- TC_17: WorkingDaysSinceLastPicked populated only when FLAG_PICK='P'
SELECT
	COUNT_IF(NVL(FLAG_PICK, '') = 'P' AND NVL("WorkingDaysSinceLastPicked", '') = '') AS bad_missing_working_days,
	COUNT_IF(NVL(FLAG_PICK, '') <> 'P' AND NVL("WorkingDaysSinceLastPicked", '') <> '') AS bad_unexpected_working_days
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
WHERE "Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Simple sanity check that the calculated start date is not after the calculated end date.
-- Why it matters:
-- Catches calendar branch errors without needing to inspect every CASE branch manually.
-- TC_18: DATE_CALC_START <= DATE_CALC_END sanity
SELECT
	COUNT_IF(CAST(DATE_CALC_START AS DATE) > CAST(DATE_CALC_END AS DATE)) AS bad_date_window
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
WHERE "Date_Order_Created" >= '2025-01-01'
  AND DATE_CALC_START IS NOT NULL
  AND DATE_CALC_END IS NOT NULL;

/* ============================================================
   SOURCE: PRDSTR_Bronze (component mapping)
   ============================================================ */
-- What this section tests:
-- Measures how many rows are actually using component expansion from PRDSTR.
-- Why it matters:
-- Helps you know whether component-aware logic is materially affecting the output population.
-- TC_19: Component-matched row population
SELECT
	COUNT(*) AS component_rows
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
WHERE ID_ITEM_COMP IS NOT NULL
  AND "Date_Order_Created" >= '2025-01-01';

/* ============================================================
   Cross-table end-to-end checks
   ============================================================ */
-- What this section tests:
-- High-level completeness and aggregate reasonableness across the final output.
-- Why it matters:
-- These checks catch issues that may not appear in a single-source parity test.

-- What this tests:
-- Null leakage in critical output fields.
-- Why it matters:
-- Critical nulls often indicate broken joins or incomplete CASE handling.
-- TC_20: Null check on critical fields
SELECT
	COUNT_IF(ID_ORD IS NULL) AS null_id_ord,
	COUNT_IF(SEQ_LINE_ORD IS NULL) AS null_seq_line_ord,
	COUNT_IF(ID_ITEM IS NULL) AS null_id_item,
	COUNT_IF(STOCK_STATUS IS NULL) AS null_stock_status,
	COUNT_IF(QTY_START IS NULL) AS null_qty_start,
	COUNT_IF(QTY_PRESEW IS NULL) AS null_qty_presew,
	COUNT_IF(QTY_REL_PND IS NULL) AS null_qty_rel_pnd,
	COUNT_IF(QTY_START_PND IS NULL) AS null_qty_start_pnd
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
WHERE "Date_Order_Created" >= '2025-01-01';

-- What this tests:
-- Aggregate totals for the most important derived qty columns.
-- Why it matters:
-- Use this to compare before/after refactors and against source-side totals.
-- TC_21: Aggregate sanity for key qty metrics
SELECT
	SUM(QTY_START) AS sum_qty_start,
	SUM(QTY_PRESEW) AS sum_qty_presew,
	SUM(QTY_REL_PND) AS sum_qty_rel_pnd,
	SUM(QTY_START_PND) AS sum_qty_start_pnd,
	SUM(SBNB) AS sum_sbnb
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
WHERE "Date_Order_Created" >= '2025-01-01';