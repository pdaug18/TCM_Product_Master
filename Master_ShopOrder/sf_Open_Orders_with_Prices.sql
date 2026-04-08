
CREATE OR REPLACE VIEW GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES AS
/*

Gap list (not present in current master_* outputs) with OOwP_temp source lineage:


1) STAT_REC_SO_display --! Not needed yet
	- Source: sh.STAT_REC_SO/DATE_START_OPER_1ST (SHPORD_HDR), so3999/so9999.STAT_REC_OPER (SHPORD_OPER), ilPAR.ID_PLANNER (ITMMAS_LOC).
	- Logic: business CASE that remaps SO status to R/W/D under operation/planner conditions.
2) CREDIT_STATUS --! Table missing from any MASTERS table.
	- Source: cs.STATUS_CREDIT from nsa.CP_CREDIT_STS (joined on ID_ORD with TYPE_REC = 0). lookinto customer master
	- Logic: CASE 0 -> 'H', 1 -> 'R'.

Notes:
- STOCK_STATUS is implemented in this view.
- Qty_Start, Qty_presew, Qty_Rel_PND, and Qty_Start_PND are implemented in this view.
- SBNB is implemented in this view.
- BIN_PRIM and stk_test are implemented in this view.
- The remaining fields above are intentionally excluded from this draft.
- This view uses only SILVER master tables and available/derivable attributes.
*/
WITH SHIP_LINE AS (
	SELECT
		ID_ORD,
		SEQ_LINE_ORD,
		MAX(ID_SHIP) AS ID_SHIP,
		MAX(ID_CARRIER) AS ID_CARRIER
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
	GROUP BY
		ID_ORD,
		SEQ_LINE_ORD
),
SHIP_ORD AS (
	SELECT
		ID_ORD,
		COUNT(DISTINCT ID_SHIP) AS NUM_SHIPMENTS,
		COUNT(DISTINCT ID_INVC) AS NUM_INVCS
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
	GROUP BY ID_ORD
),
PRDSTR_FG AS (
	SELECT
		ps.ID_ITEM_PAR,
		ps.ID_ITEM_COMP,
		ps.DATE_EFF_START,
		ps.DATE_EFF_END
	FROM BRONZE_DATA.TCM_BRONZE."PRDSTR_Bronze" ps
	INNER JOIN SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE mp2
		ON ps.ID_ITEM_COMP = mp2."Item ID_Child SKU"
	WHERE mp2."COMMODITY CODE" = 'FG'
),
WORKING_DAYS AS (
    SELECT
        o.ID_ORD,
        o.SEQ_LINE_ORD,
        COUNT(*) AS WORKING_DAYS_SINCE_LAST_PICKED
    FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
    INNER JOIN GOLD_DATA.DIM.DIM_CALENDAR d
        ON d.CALENDAR_DATE > CAST(o.DATE_PICK_LAST AS DATE)
        AND d.CALENDAR_DATE <= CURRENT_DATE()
        AND d.IS_WEEKDAY = TRUE
        AND COALESCE(d.IS_HOLIDAY, FALSE) = FALSE
    WHERE TO_VARCHAR(o.FLAG_PICK) = '2'
        AND o.DATE_PICK_LAST IS NOT NULL
    GROUP BY
        o.ID_ORD,
        o.SEQ_LINE_ORD
),
/* ── AddWorkDays(3, DATE_ORD) ─────────────────────────────────────────────
   Replaces nsa.AddWorkDays(3, oh.DATE_ORD): returns the 3rd business day
   after each distinct order date, using GOLD_DATA.DIM.DIM_CALENDAR.
   ─────────────────────────────────────────────────────────────────────── */
WORKDAY_3_AFTER_ORD AS (
	SELECT
		base_date,
		MIN(CALENDAR_DATE) AS result_date
	FROM (
		SELECT
			CAST(o.DATE_ORD AS DATE) AS base_date,
			d.CALENDAR_DATE,
			ROW_NUMBER() OVER (PARTITION BY CAST(o.DATE_ORD AS DATE) ORDER BY d.CALENDAR_DATE) AS rn
		FROM (SELECT DISTINCT DATE_ORD FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE WHERE DATE_ORD IS NOT NULL) o
		INNER JOIN GOLD_DATA.DIM.DIM_CALENDAR d
			ON d.CALENDAR_DATE > CAST(o.DATE_ORD AS DATE)
			AND d.IS_WEEKDAY = TRUE
			AND COALESCE(d.IS_HOLIDAY, FALSE) = FALSE
	) ranked
	WHERE rn = 3
	GROUP BY base_date
),
/* ── AddWorkDays(1, DATE_ADD) ─────────────────────────────────────────────
   Replaces nsa.AddWorkDays(1, ol.DATE_ADD): returns the 1st business day
   after each distinct line-add date, using GOLD_DATA.DIM.DIM_CALENDAR.
   ─────────────────────────────────────────────────────────────────────── */
WORKDAY_1_AFTER_ADD AS (
	SELECT
		base_date,
		MIN(CALENDAR_DATE) AS result_date
	FROM (
		SELECT
			CAST(o.DATE_ADD AS DATE) AS base_date,
			d.CALENDAR_DATE,
			ROW_NUMBER() OVER (PARTITION BY CAST(o.DATE_ADD AS DATE) ORDER BY d.CALENDAR_DATE) AS rn
		FROM (SELECT DISTINCT DATE_ADD FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE WHERE DATE_ADD IS NOT NULL) o
		INNER JOIN GOLD_DATA.DIM.DIM_CALENDAR d
			ON d.CALENDAR_DATE > CAST(o.DATE_ADD AS DATE)
			AND d.IS_WEEKDAY = TRUE
			AND COALESCE(d.IS_HOLIDAY, FALSE) = FALSE
	) ranked
	WHERE rn = 1
	GROUP BY base_date
),
/* ── AddWorkDays(10, DATE_PROM) ───────────────────────────────────────────
   Replaces nsa.AddWorkDays(10, ol.DATE_PROM): returns the 10th business day
   after each distinct promise date, using GOLD_DATA.DIM.DIM_CALENDAR.
   ─────────────────────────────────────────────────────────────────────── */
WORKDAY_10_AFTER_PROM AS (
	SELECT
		base_date,
		MIN(CALENDAR_DATE) AS result_date
	FROM (
		SELECT
			CAST(o.DATE_PROM AS DATE) AS base_date,
			d.CALENDAR_DATE,
			ROW_NUMBER() OVER (PARTITION BY CAST(o.DATE_PROM AS DATE) ORDER BY d.CALENDAR_DATE) AS rn
		FROM (SELECT DISTINCT DATE_PROM FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE WHERE DATE_PROM IS NOT NULL) o
		INNER JOIN GOLD_DATA.DIM.DIM_CALENDAR d
			ON d.CALENDAR_DATE > CAST(o.DATE_PROM AS DATE)
			AND d.IS_WEEKDAY = TRUE
			AND COALESCE(d.IS_HOLIDAY, FALSE) = FALSE
	) ranked
	WHERE rn = 10
	GROUP BY base_date
),
/* ── SBNB ────────────────────────────────────────────────────────────────
   Unconfirmed shipped qty by item and location.
   Replaces tSBNB subquery from nsa.CP_SHPLIN.
   Logic: SUM(QTY_SHIP) where FLAG_CONFIRM_SHIP <> 1, by ID_ITEM and ID_LOC.
   ─────────────────────────────────────────────────────────────────────── */
SBNB AS (
	SELECT
		ID_ITEM,
		ID_LOC,
		SUM(QTY_SHIP) AS SBNB
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
	WHERE COALESCE(FLAG_CONFIRM_SHIP, 0) <> 1
	GROUP BY ID_ITEM, ID_LOC
),
/* ── WPS ──────────────────────────────────────────────────────────────────
   Shop orders in 'S' (Start) status.
   Drives Qty_Start and Qty_presew numerator.
   Source: MASTER_SHOPORDER_WC_TABLE where STAT_REC_SO = 'S'
   ─────────────────────────────────────────────────────────────────────── */
WPS AS (
	SELECT
		ID_ITEM_PAR,
		SUM(QTY_REMAINING) AS SUM_QTY_ONORD
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
	WHERE STAT_REC_SO = 'S'
	GROUP BY ID_ITEM_PAR
),
/* ── PCC ──────────────────────────────────────────────────────────────────
   Shop orders in 'S' status that have completed operation 3999 (pre-cut).
   Drives the "presew" portion of Qty_Start / Qty_presew.
   Source: MASTER_SHOPORDER_WC_TABLE (ID_OPER = 3999, STAT_REC_OPER = 'C')
   ─────────────────────────────────────────────────────────────────────── */
PCC AS (
	SELECT
		ID_ITEM_PAR,
		MIN(STAT_REC_SO) AS STAT_REC_SO,
		SUM(QTY_REMAINING) AS SUM_QTY_ONORD
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
	WHERE STAT_REC_SO = 'S'
		AND ID_OPER = 3999
		AND STAT_REC_OPER = 'C'
	GROUP BY ID_ITEM_PAR
),
/* ── WPR_PND ──────────────────────────────────────────────────────────────
   Pending released shop orders (ID_ITEM_PAR ending in '#').
   Drives Qty_Rel_PND. Join key strips the trailing '#'.
   Source: MASTER_SHOPORDER_WC_TABLE where STAT_REC_SO = 'R' and ID_ITEM_PAR like '%#'
   ─────────────────────────────────────────────────────────────────────── */
WPR_PND AS (
	SELECT
		ID_ITEM_PAR,
		REPLACE(ID_ITEM_PAR, '#', '') AS ID_ITEM_PAR_NP,
		SUM(QTY_REMAINING) AS SUM_QTY_ONORD
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
	WHERE STAT_REC_SO = 'R'
		AND ID_ITEM_PAR LIKE '%#'
	GROUP BY ID_ITEM_PAR
),
/* ── WPS_PND ──────────────────────────────────────────────────────────────
   Pending started shop orders (ID_ITEM_PAR ending in '#').
   Drives Qty_Start_PND. Join key strips the trailing '#'.
   Source: MASTER_SHOPORDER_WC_TABLE where STAT_REC_SO = 'S' and ID_ITEM_PAR like '%#'
   ─────────────────────────────────────────────────────────────────────── */
WPS_PND AS (
	SELECT
		ID_ITEM_PAR,
		REPLACE(ID_ITEM_PAR, '#', '') AS ID_ITEM_PAR_NP,
		SUM(QTY_REMAINING) AS SUM_QTY_ONORD
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
	WHERE STAT_REC_SO = 'S'
		AND ID_ITEM_PAR LIKE '%#'
	GROUP BY ID_ITEM_PAR
),
/* ── FLAG_CACHE ──────────────────────────────────────────────────────────
   Pre-compute flag conversions to avoid redundant TO_VARCHAR calls.
   ─────────────────────────────────────────────────────────────────────── */
FLAG_CACHE AS (
	SELECT
		ID_ORD,
		SEQ_LINE_ORD,
		FLAG_PICK,
		FLAG_ACKN,
		TO_VARCHAR(FLAG_PICK) AS flag_pick_str,
		TO_VARCHAR(FLAG_ACKN) AS flag_ackn_str
	FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
),
/* ── QTY_STAGING ─────────────────────────────────────────────────────────
   Pre-calculate presew qty once (shared between Qty_Start and Qty_presew).
   ─────────────────────────────────────────────────────────────────────── */
QTY_STAGING AS (
	SELECT
		wps.ID_ITEM_PAR,
		COALESCE(wps.SUM_QTY_ONORD, 0) AS wps_qty,
		COALESCE(
			CASE WHEN pcc.STAT_REC_SO IS NULL THEN wps.SUM_QTY_ONORD ELSE pcc.SUM_QTY_ONORD END,
			0
		) AS presew_qty
	FROM WPS wps
	LEFT JOIN PCC pcc ON pcc.ID_ITEM_PAR = wps.ID_ITEM_PAR
)
SELECT
	CURRENT_TIMESTAMP() AS dataRefreshTimeStamp,
	o.OPEN_NET_AMT AS open_net_amt,
	o.ID_CUST_SOLDTO,
	-- ship_complete_flag: derived from LINE_COMMENT_NOTE (equivalent of CP_COMMENT.NOTE in OOwP)
	-- Original logic: distinct ID_ORD where NOTE ilike '%SHIP%COMPLETE%' and note not ilike '%LINE%'
	CASE
		WHEN o.LINE_COMMENT_NOTE ILIKE '%SHIP%COMPLETE%'
		 AND o.LINE_COMMENT_NOTE NOT ILIKE '%LINE%'
		THEN 'Y'
	END AS ship_complete_flag,
	o.DATE_PICK_LAST,
	CASE
		WHEN fc.flag_pick_str = '2' THEN TO_VARCHAR(COALESCE(wd.WORKING_DAYS_SINCE_LAST_PICKED, 0))
		ELSE ''
	END AS "WorkingDaysSinceLastPicked",
	CASE
		WHEN fc.flag_pick_str = '2' THEN 'P'
		ELSE ''
	END AS FLAG_PICK,
    CASE
		WHEN fc.flag_ackn_str = '2' THEN 'A'
		ELSE ''
	END AS FLAG_ACKN,
	o.AMT_ORD_TOTAL AS amt_ord_total,
	o.ID_SLSREP_1,
	o.DESCR_SHIP_VIA,
	o.DATE_RQST,
	o.DATE_PROM,
	o.DATE_ORD,
	-- DATE_CALC_START: Snowflake equivalent of OOwP nsa.AddWorkDays CASE logic
	-- Branch 1: same request/promise date            → use DATE_RQST
	-- Branch 2: customer 102340                      → use DATE_PROM as-is
	-- Branch 3: stock/planner item AND fast-track ord → use AddWorkDays(1, DATE_ADD)
	-- Default:                                          use DATE_PROM
	CASE
		WHEN o.DATE_RQST = o.DATE_PROM
			THEN o.DATE_RQST
		WHEN o.ID_CUST_SOLDTO = '102340'
			THEN o.DATE_PROM
		WHEN (o.FLAG_STK = 'S' OR inv."Item_Planned_Classification" IN ('AS','1A','KT','A '))
			 AND w3.result_date >= CAST(o.DATE_PROM AS DATE)
			THEN w1.result_date
		ELSE o.DATE_PROM
	END AS DATE_CALC_START,
	-- DATE_CALC_END: Snowflake equivalent of OOwP AddWorkDays(10) CASE logic
	CASE
		WHEN o.DATE_RQST = o.DATE_PROM
			THEN o.DATE_RQST
		WHEN o.ID_CUST_SOLDTO = '102340'
			THEN w10.result_date
		WHEN (o.FLAG_STK = 'S' OR inv."Item_Planned_Classification" IN ('AS','1A','KT','A '))
			 AND w3.result_date >= CAST(o.DATE_PROM AS DATE)
			THEN o.DATE_PROM
		ELSE w10.result_date
	END AS DATE_CALC_END,
	CASE
		WHEN wc.ID_BUYER = 'AS' AND inv."Qty_On_Hand" IS NOT NULL AND inv."Item_Inventory_Reorder_Point" > 1 THEN 'AS'
		WHEN wc.ID_BUYER = '1A' AND inv."Qty_On_Hand" IS NOT NULL AND inv."Item_Inventory_Reorder_Point" > 1 THEN 'AS'
		WHEN wc.ID_BUYER = 'KT' AND inv."Qty_On_Hand" IS NOT NULL AND inv."Item_Inventory_Reorder_Point" > 1 THEN 'KT'
		ELSE ''
	END AS alt_stk,
	CASE
		WHEN o.ORD_COMMENT ILIKE '%#MO%' THEN 'Y'
		ELSE 'N'
	END AS FLAG_MO,
	o.ID_ITEM,
	ps.ID_ITEM_COMP,
	CASE
		-- Component-aware stock check for FG-style parent items.
		WHEN i."COMMODITY CODE" = 'FG'
			 AND COALESCE(inv_comp."Item_Stock_Flag", inv."Item_Stock_Flag", o.FLAG_STK) = 'S'
			THEN '3-FABRIC'
		WHEN COALESCE(inv."Item_Stock_Flag", o.FLAG_STK) = 'S'
			THEN '1-STOCK'
		ELSE '2-MTO'
	END AS STOCK_STATUS,
	o.ID_ORD,
	o.ID_USER_ADD,
	o.DATE_ADD as "Date_Order_Created",
	o.SEQ_LINE_ORD,
	o.ID_SO AS id_so_odbc,
	i."Item_Vertical",
	-- o.CODE_CAT_PRDT,
	i."CODE_USER_1",
	wc.ID_REV_DRAW,
	sl.ID_CARRIER,
	o.QTY_OPEN,
	i."Item Status_Child Active Status" AS FLAG_STAT_ITEM,
	o.FLAG_STK AS OL_FLAG_STK,
	inv."Item_Stock_Flag" AS IL_FLAG_STK,
	i."Item_Work Center_Rubin" AS RBN_WC,
	inv."Qty_Released" AS Qty_Rel,
	-- Qty_Start: WPS qty not yet past pre-cut (operation 3999).
	-- Sourced from pre-calculated QTY_STAGING CTE.
	qty_stg.wps_qty - qty_stg.presew_qty AS Qty_Start,
	-- Qty_presew: qty in 'S' state that has completed operation 3999 (pre-cut done, awaiting sew).
	qty_stg.presew_qty AS Qty_presew,
	-- Qty_Rel_PND / Qty_Start_PND: pending shop orders whose ID_ITEM_PAR ends with '#'.
	COALESCE(wpr_pnd.SUM_QTY_ONORD, 0) AS Qty_Rel_PND,
	COALESCE(wps_pnd.SUM_QTY_ONORD, 0) AS Qty_Start_PND,
	-- SBNB: unconfirmed shipped qty (component-aware: use comp item + order loc when present).
	COALESCE(sbnb.SBNB, 0) AS SBNB,
	inv."Qty_On_Hand" AS QTY_ONHD,
	inv."Qty_Allocated" AS QTY_ALLOC,
	inv."Qty_On_Order" AS QTY_ONORD,
	COALESCE(inv_comp."Primary_Bin", inv."Primary_Bin") AS BIN_PRIM,
	inv."Item_Source_Flag" AS FLAG_SOURCE,
	inv."Item_Bin_Tracking" AS FLAG_TRACK_BIN,
	inv."Item_Inventory_Reorder_Point" AS LEVEL_ROP,
	CASE
		WHEN COALESCE(inv_comp."Item_Inventory_Reorder_Point", inv."Item_Inventory_Reorder_Point", 0) > 1
			 AND COALESCE(inv_comp."Item_Stock_Flag", inv."Item_Stock_Flag", o.FLAG_STK) = 'S'
			THEN 1
		ELSE 0
	END AS stk_test,
	i."Unit_of_Measure_Price" AS CODE_UM_PRICE,
	o.NAME_CUST,
	wc.STAT_REC_SO,
	wc."ShopOrder#" AS ID_SO,
	o.QTY_SHIP_TOTAL,
	sl.ID_SHIP,
	o.CODE_STAT_ORD,
	o.ID_PO_CUST,
	so.NUM_SHIPMENTS,
	so.NUM_INVCS,
	1 AS COUNTER
	-- o.ID_LOC
FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
LEFT JOIN FLAG_CACHE fc ON o.ID_ORD = fc.ID_ORD AND o.SEQ_LINE_ORD = fc.SEQ_LINE_ORD
LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE wc ON TRIM(o.ID_SO) = TRIM(wc."ShopOrder#") 
-- AND o.SUFX_SO = wc.SUFX_SO
LEFT JOIN SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER inv ON o.ID_ITEM = inv."Product_ID_SKU" AND o.ID_LOC = inv."Location_ID"
LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE i ON o.ID_ITEM = i."Item ID_Child SKU"
LEFT JOIN SHIP_LINE sl ON o.ID_ORD = sl.ID_ORD AND o.SEQ_LINE_ORD = sl.SEQ_LINE_ORD
LEFT JOIN SHIP_ORD so ON o.ID_ORD = so.ID_ORD
LEFT JOIN WORKING_DAYS wd        ON o.ID_ORD = wd.ID_ORD AND o.SEQ_LINE_ORD = wd.SEQ_LINE_ORD
LEFT JOIN WORKDAY_3_AFTER_ORD w3 ON CAST(o.DATE_ORD AS DATE) = w3.base_date
LEFT JOIN WORKDAY_1_AFTER_ADD w1 ON CAST(o.DATE_ADD AS DATE) = w1.base_date
LEFT JOIN WORKDAY_10_AFTER_PROM w10 ON CAST(o.DATE_PROM AS DATE) = w10.base_date
LEFT JOIN PRDSTR_FG ps
	ON o.ID_ITEM = ps.ID_ITEM_PAR
	AND o.FLAG_STK = 'N'
	AND (
		ps.ID_ITEM_COMP = REPLACE(o.ID_ITEM, '*', '')
		OR ps.ID_ITEM_COMP ILIKE 'PNT%UI%'
		OR ps.ID_ITEM_COMP ILIKE 'SPX%'
		OR ps.ID_ITEM_COMP ILIKE 'TCG%'
		OR ps.ID_ITEM_COMP ILIKE 'SHRDR3%'
		OR ps.ID_ITEM_COMP ILIKE 'C54WFLS%'
		OR ps.ID_ITEM_COMP ILIKE 'C54VYLS%'
		OR ps.ID_ITEM_COMP ILIKE 'DF2-CM-618-JN-DN-%'
		OR ps.ID_ITEM_COMP ILIKE 'HYDROJACK%'
		OR wc.ID_BUYER = 'AS'
	)
	AND (
		(CURRENT_DATE() BETWEEN CAST(ps.DATE_EFF_START AS DATE) AND CAST(ps.DATE_EFF_END AS DATE))
		OR ps.DATE_EFF_START IS NULL
		OR ps.DATE_EFF_END IS NULL
	)
LEFT JOIN SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER inv_comp
	ON REPLACE(ps.ID_ITEM_COMP, '*', '') = inv_comp."Product_ID_SKU"
/* ── SBNB: component-aware — use comp item when present, same ID_LOC as order ── */
LEFT JOIN SBNB
	ON SBNB.ID_ITEM = COALESCE(ps.ID_ITEM_COMP, o.ID_ITEM)
	AND SBNB.ID_LOC = o.ID_LOC
/* ── Shop-order qty aggregates (component-aware: use comp item when present) ── */
LEFT JOIN WPS
	ON WPS.ID_ITEM_PAR = COALESCE(ps.ID_ITEM_COMP, o.ID_ITEM)
LEFT JOIN QTY_STAGING qty_stg
	ON qty_stg.ID_ITEM_PAR = COALESCE(ps.ID_ITEM_COMP, o.ID_ITEM)
LEFT JOIN PCC
	ON PCC.ID_ITEM_PAR = COALESCE(ps.ID_ITEM_COMP, o.ID_ITEM)
LEFT JOIN WPR_PND
	ON WPR_PND.ID_ITEM_PAR_NP = COALESCE(ps.ID_ITEM_COMP, o.ID_ITEM)
LEFT JOIN WPS_PND
	ON WPS_PND.ID_ITEM_PAR_NP = COALESCE(ps.ID_ITEM_COMP, o.ID_ITEM);
