CREATE OR REPLACE VIEW GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES AS
/*

Gap list (not present in current master_* outputs) with OOwP_temp source lineage:

1) STAT_REC_SO_display --! Not needed yet
	- Source: sh.STAT_REC_SO/DATE_START_OPER_1ST (SHPORD_HDR), so3999/so9999.STAT_REC_OPER (SHPORD_OPER), ilPAR.ID_PLANNER (ITMMAS_LOC).
	- Logic: business CASE that remaps SO status to R/W/D under operation/planner conditions.
2) CREDIT_STATUS --! Table missing from any MASTERS table.
	- Source: cs.STATUS_CREDIT from nsa.CP_CREDIT_STS (joined on ID_ORD with TYPE_REC = 0). lookinto customer master
	- Logic: CASE 0 -> 'H', 1 -> 'R'.
3) COMPONENT_LEVEL_DETAILS --! Removed as component-level details aren't needed.
	- Removed output field: ID_ITEM_COMP.
	- Removed CTE/source path: PRDSTR_FG (PRDSTR_Bronze component path).
	- Removed joins: PRDSTR/component matching and ITEM_INVENTORY_MASTER inv_comp fallback join.
	- Removed behavior: component-aware Qty joins (COALESCE(ps.ID_ITEM_COMP, o.ID_ITEM)).
	- Removed behavior: component fallback for BIN_PRIM, STOCK_STATUS, IL_FLAG_STK, LEVEL_ROP, FLAG_SOURCE, FLAG_TRACK_BIN.

Notes:
- STOCK_STATUS is implemented in this view.
- Qty_Rel, Qty_Start, Qty_presew, Qty_Rel_PND, and Qty_Start_PND are implemented in this view.
- SBNB is implemented in this view.
- BIN_PRIM and stk_test are implemented in this view.
- The remaining fields above are intentionally excluded from this draft.
- This view uses only SILVER master tables and available/derivable attributes.
*/
-- ORDERS_BASE: safety dedupe by business key, prioritizing ACTIVE-like records.
WITH ORDERS_BASE AS (
	SELECT *
	FROM (
		SELECT
			o.*,
			ROW_NUMBER() OVER (
				PARTITION BY o.ID_ITEM, o.ID_ORD, o.SEQ_LINE_ORD
				ORDER BY
					CASE WHEN o.CODE_STAT_ORD IN ('O', 'B', 'S', 'P', 'R', 'A') THEN 0 ELSE 1 END,
					CASE WHEN o.QTY_OPEN > 0 THEN 0 ELSE 1 END,
					COALESCE(o.DATE_ADD, TO_DATE('1900-01-01')) DESC,
					COALESCE(o.DATE_ORD, TO_DATE('1900-01-01')) DESC
			) AS rn
		FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
	) ranked
	WHERE rn = 1
),
-- SHIP_LINE: line-grain shipment rollup used for latest ship/carrier and shipped qty offsets.
SHIP_LINE AS (
	SELECT
		ID_ORD,
		SEQ_LINE_ORD,
		MAX(ID_SHIP) AS ID_SHIP,
		MAX(ID_CARRIER) AS ID_CARRIER,
		SUM(QTY_SHIP) AS QTY_SHIP
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
	GROUP BY
		ID_ORD,
		SEQ_LINE_ORD
),
-- SHIP_ORD: order-grain shipment/invoice counters used for fulfillment visibility columns.
SHIP_ORD AS (
	SELECT
		ID_ORD,
		COUNT(DISTINCT ID_SHIP) AS NUM_SHIPMENTS,
		COUNT(DISTINCT ID_INVC) AS NUM_INVCS
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
	GROUP BY ID_ORD
),
-- WORKING_DAYS: computes business-day age since last pick for currently pick-flagged lines.
WORKING_DAYS AS (
    SELECT
        o.ID_ORD,
        o.SEQ_LINE_ORD,
        COUNT(*) AS WORKING_DAYS_SINCE_LAST_PICKED
	FROM ORDERS_BASE o
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
-- WORKDAY_3_AFTER_ORD: helper calendar lookup for 3rd business day after order date.
WORKDAY_3_AFTER_ORD AS (
	SELECT
		base_date,
		MIN(CALENDAR_DATE) AS result_date
	FROM (
		SELECT
			CAST(o.DATE_ORD AS DATE) AS base_date,
			d.CALENDAR_DATE,
			ROW_NUMBER() OVER (PARTITION BY CAST(o.DATE_ORD AS DATE) ORDER BY d.CALENDAR_DATE) AS rn
		FROM (SELECT DISTINCT DATE_ORD FROM ORDERS_BASE WHERE DATE_ORD IS NOT NULL) o
		INNER JOIN GOLD_DATA.DIM.DIM_CALENDAR d
			ON d.CALENDAR_DATE > CAST(o.DATE_ORD AS DATE)
			AND d.IS_WEEKDAY = TRUE
			AND COALESCE(d.IS_HOLIDAY, FALSE) = FALSE
	) ranked
	WHERE rn = 3
	GROUP BY base_date
),
-- WORKDAY_1_AFTER_ADD: helper calendar lookup for 1st business day after line add date.
WORKDAY_1_AFTER_ADD AS (
	SELECT
		base_date,
		MIN(CALENDAR_DATE) AS result_date
	FROM (
		SELECT
			CAST(o.DATE_ADD AS DATE) AS base_date,
			d.CALENDAR_DATE,
			ROW_NUMBER() OVER (PARTITION BY CAST(o.DATE_ADD AS DATE) ORDER BY d.CALENDAR_DATE) AS rn
		FROM (SELECT DISTINCT DATE_ADD FROM ORDERS_BASE WHERE DATE_ADD IS NOT NULL) o
		INNER JOIN GOLD_DATA.DIM.DIM_CALENDAR d
			ON d.CALENDAR_DATE > CAST(o.DATE_ADD AS DATE)
			AND d.IS_WEEKDAY = TRUE
			AND COALESCE(d.IS_HOLIDAY, FALSE) = FALSE
	) ranked
	WHERE rn = 1
	GROUP BY base_date
),
-- WORKDAY_10_AFTER_PROM: helper calendar lookup for 10th business day after promise date.
WORKDAY_10_AFTER_PROM AS (
	SELECT
		base_date,
		MIN(CALENDAR_DATE) AS result_date
	FROM (
		SELECT
			CAST(o.DATE_PROM AS DATE) AS base_date,
			d.CALENDAR_DATE,
			ROW_NUMBER() OVER (PARTITION BY CAST(o.DATE_PROM AS DATE) ORDER BY d.CALENDAR_DATE) AS rn
		FROM (SELECT DISTINCT DATE_PROM FROM ORDERS_BASE WHERE DATE_PROM IS NOT NULL) o
		INNER JOIN GOLD_DATA.DIM.DIM_CALENDAR d
			ON d.CALENDAR_DATE > CAST(o.DATE_PROM AS DATE)
			AND d.IS_WEEKDAY = TRUE
			AND COALESCE(d.IS_HOLIDAY, FALSE) = FALSE
	) ranked
	WHERE rn = 10
	GROUP BY base_date
),
-- SBNB: unconfirmed shipped quantity by item/location (used to adjust available inventory views).
SBNB AS (
	SELECT
		ID_ITEM,
		ID_LOC,
		SUM(QTY_SHIP) AS SBNB
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE
	WHERE COALESCE(FLAG_CONFIRM_SHIP, 0) <> 1
	GROUP BY ID_ITEM, ID_LOC
),
-- SHOPORDER_HDR: header-grain shop orders (deduped from operation-level WC rows) used by downstream SO qty and lookup logic.
SHOPORDER_HDR AS (
	SELECT DISTINCT
		SHOP_ORDER_LOCATION,
		"ShopOrder#",
		SUFX_SO,
		ID_ITEM_PAR,
		STAT_REC_SO,
		QTY_REMAINING,
		ID_BUYER,
		ID_REV_DRAW
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
),
-- WC_LOOKUP: one deterministic row per (ShopOrder#, location) for final join cardinality control.
WC_LOOKUP AS (
	SELECT
		SHOP_ORDER_LOCATION,
		"ShopOrder#",
		ID_ITEM_PAR,
		STAT_REC_SO,
		ID_BUYER,
		ID_REV_DRAW
	FROM (
		SELECT
			SHOP_ORDER_LOCATION,
			"ShopOrder#",
			SUFX_SO,
			ID_ITEM_PAR,
			STAT_REC_SO,
			ID_BUYER,
			ID_REV_DRAW,
			ROW_NUMBER() OVER (
				PARTITION BY SHOP_ORDER_LOCATION, "ShopOrder#"
				ORDER BY SUFX_SO ASC
			) AS rn
		FROM SHOPORDER_HDR
		WHERE SUFX_SO = 0
	) ranked
	WHERE rn = 1
),
-- PCC_ORDERS: suffix-level keys where operation 3999 is complete; these keys classify started qty as presew.
PCC_ORDERS AS (
	SELECT DISTINCT
		SHOP_ORDER_LOCATION,
		"ShopOrder#",
		SUFX_SO
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
	WHERE ID_OPER = 3999
		AND STAT_REC_OPER = 'C'
),
-- SO_QTY_BY_PARENT: released and started SO qty summarized once by parent item/location.
SO_QTY_BY_PARENT AS (
	SELECT
		ID_ITEM_PAR,
		SHOP_ORDER_LOCATION,
		SUM(CASE WHEN STAT_REC_SO = 'R' THEN QTY_REMAINING ELSE 0 END) AS QTY_REL,
		SUM(CASE WHEN STAT_REC_SO = 'S' THEN QTY_REMAINING ELSE 0 END) AS QTY_START
	FROM SHOPORDER_HDR
	GROUP BY ID_ITEM_PAR, SHOP_ORDER_LOCATION
),
-- SO_QTY_PENDING_BY_PARENT: pending (#-suffix) released and started qty keyed by normalized parent item/location.
SO_QTY_PENDING_BY_PARENT AS (
	SELECT
		REPLACE(ID_ITEM_PAR, '#', '') AS ID_ITEM_PAR_NP,
		SHOP_ORDER_LOCATION,
		SUM(CASE WHEN STAT_REC_SO = 'R' THEN QTY_REMAINING ELSE 0 END) AS QTY_REL_PND,
		SUM(CASE WHEN STAT_REC_SO = 'S' THEN QTY_REMAINING ELSE 0 END) AS QTY_START_PND
	FROM SHOPORDER_HDR
	WHERE ID_ITEM_PAR LIKE '%#'
	GROUP BY REPLACE(ID_ITEM_PAR, '#', ''), SHOP_ORDER_LOCATION
),
-- PCC: subset of WPS where operation 3999 is complete; feeds presew split.
PCC AS (
	SELECT
		h.ID_ITEM_PAR,
		h.SHOP_ORDER_LOCATION,
		MIN(h.STAT_REC_SO) AS STAT_REC_SO,
		SUM(h.QTY_REMAINING) AS SUM_QTY_ONORD
	FROM SHOPORDER_HDR h
	INNER JOIN PCC_ORDERS p
		ON h.SHOP_ORDER_LOCATION = p.SHOP_ORDER_LOCATION
		AND h."ShopOrder#" = p."ShopOrder#"
		AND h.SUFX_SO = p.SUFX_SO
	WHERE h.STAT_REC_SO = 'S'
	GROUP BY h.ID_ITEM_PAR, h.SHOP_ORDER_LOCATION
),
-- QTY_STAGING: centralizes started vs presew split so Qty_Start and Qty_presew remain internally consistent.
QTY_STAGING AS (
	SELECT
		soq.ID_ITEM_PAR,
		soq.SHOP_ORDER_LOCATION,
		COALESCE(soq.QTY_START, 0) AS qty_start,
		COALESCE(
			CASE WHEN pcc.STAT_REC_SO IS NULL THEN soq.QTY_START ELSE pcc.SUM_QTY_ONORD END,
			0
		) AS presew_qty
	FROM SO_QTY_BY_PARENT soq
	LEFT JOIN PCC pcc
		ON pcc.ID_ITEM_PAR = soq.ID_ITEM_PAR
		AND pcc.SHOP_ORDER_LOCATION = soq.SHOP_ORDER_LOCATION
),
-- PRODUCT_DIM: enforce one product row per item before joining order lines.
PRODUCT_DIM AS (
	SELECT
		"Item ID_Child SKU",
		"COMMODITY CODE",
		"Item_Vertical",
		"CODE_USER_1",
		"Item Status_Child Active Status",
		"Unit_of_Measure_Price",
		"Item_Work Center_Rubin"
	FROM (
		SELECT
			i.*,
			ROW_NUMBER() OVER (
				PARTITION BY i."Item ID_Child SKU"
				ORDER BY
					CASE WHEN i."Item Status_Child Active Status" = 'A' THEN 0 ELSE 1 END,
					CASE WHEN i."COMMODITY CODE" IS NULL THEN 1 ELSE 0 END,
					i."Item ID_Child SKU"
			) AS rn
		FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE i
	) ranked
	WHERE rn = 1
)
SELECT
	CURRENT_TIMESTAMP() AS dataRefreshTimeStamp,
	o.OPEN_NET_AMT AS open_net_amt,
	o.ID_CUST_SOLDTO,
	CASE
		WHEN o.LINE_COMMENT_NOTE ILIKE '%SHIP%COMPLETE%'
		 AND o.LINE_COMMENT_NOTE NOT ILIKE '%LINE%'
		THEN 'Y'
	END AS ship_complete_flag,
	o.DATE_PICK_LAST,
	CASE
		WHEN TO_VARCHAR(o.FLAG_PICK) = '2' THEN TO_VARCHAR(COALESCE(wd.WORKING_DAYS_SINCE_LAST_PICKED, 0))
		ELSE ''
	END AS "WorkingDaysSinceLastPicked",
	CASE
		WHEN TO_VARCHAR(o.FLAG_PICK) = '2' THEN 'P'
		ELSE ''
	END AS FLAG_PICK,
    CASE
		WHEN TO_VARCHAR(o.FLAG_ACKN) = '2' THEN 'A'
		ELSE ''
	END AS FLAG_ACKN,
	o.AMT_ORD_TOTAL AS amt_ord_total,
	o.ID_SLSREP_1,
	sl.ID_CARRIER,
	o.DESCR_SHIP_VIA,
	o.DATE_RQST as DR,
	o.DATE_PROM as DP,
	o.DATE_ORD as DO,
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
    inv."Item_Planned_Classification",
	-- '"id_ord"', '"seq_line_ord"', '"id_item"',
    o.ID_ITEM,
	o.ID_ORD,
	o.SEQ_LINE_ORD,
	CASE
		WHEN wc.ID_BUYER = 'AS' AND inv."Qty_On_Hand" IS NOT NULL AND COALESCE(inv."Item_Inventory_Reorder_Point", 0) > 1 THEN 'AS'
		WHEN wc.ID_BUYER = '1A' AND inv."Qty_On_Hand" IS NOT NULL AND COALESCE(inv."Item_Inventory_Reorder_Point", 0) > 1 THEN 'AS'
		WHEN wc.ID_BUYER = 'KT' AND inv."Qty_On_Hand" IS NOT NULL AND COALESCE(inv."Item_Inventory_Reorder_Point", 0) > 1 THEN 'KT'
		ELSE ''
	END AS alt_stk,
	o.ID_USER_ADD,
	o.DATE_ADD as "Date_Order_Created",
	o.ID_SO AS id_so_odbc,
    inv."Location_ID",
	o.ID_LOC,
	CASE
		WHEN o.ORD_COMMENT ILIKE '%#MO%' THEN 'Y'
		ELSE 'N'
	END AS FLAG_MO,
	CASE
		WHEN i."COMMODITY CODE" LIKE 'RM%'
			OR i."COMMODITY CODE" = 'FAB'
			OR i."COMMODITY CODE" LIKE 'DF%'
			THEN '3-FABRIC'
		WHEN COALESCE(inv."Item_Stock_Flag", o.FLAG_STK) = 'S'
			 AND COALESCE(inv."Item_Inventory_Reorder_Point", 0) > 1
			THEN '1-STOCK'
		ELSE '2-MTO'
	END AS STOCK_STATUS,
	i."Item_Vertical",
	i."CODE_USER_1",
	(o.QTY_OPEN - COALESCE(sl.QTY_SHIP, 0)) AS QTY_OPEN,
    i."Item Status_Child Active Status" AS FLAG_STAT_ITEM,
	o.FLAG_STK AS OL_FLAG_STK,
	inv."Item_Stock_Flag" AS IL_FLAG_STK,
	COALESCE(soq.QTY_REL, 0) AS Qty_Rel,
	COALESCE(qty_stg.qty_start, 0) - COALESCE(qty_stg.presew_qty, 0) AS Qty_Start,
	COALESCE(qty_stg.presew_qty, 0) AS Qty_presew,
	COALESCE(soq_pnd.QTY_REL_PND, 0) AS Qty_Rel_PND,
	COALESCE(soq_pnd.QTY_START_PND, 0) AS Qty_Start_PND,
	COALESCE(sbnb.SBNB, 0) AS SBNB,
	inv."Primary_Bin" AS BIN_PRIM,
	inv."Item_Inventory_Reorder_Point" AS LEVEL_ROP,
    CASE
		WHEN COALESCE(inv."Item_Inventory_Reorder_Point", 0) > 1
			 AND COALESCE(inv."Item_Stock_Flag", o.FLAG_STK) = 'S'
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
	(COALESCE(inv."Qty_On_Hand", 0) - COALESCE(sbnb.SBNB, 0)) AS QTY_ONHD,
	(COALESCE(inv."Qty_Allocated", 0) - COALESCE(sbnb.SBNB, 0)) AS QTY_ALLOC,
	COALESCE(inv."Qty_On_Order", 0) AS QTY_ONORD,
	inv."Item_Source_Flag" AS FLAG_SOURCE,
	inv."Item_Bin_Tracking" AS FLAG_TRACK_BIN,
	o.ID_PO_CUST,
	so.NUM_SHIPMENTS,
	so.NUM_INVCS,
	1 AS COUNTER,
	wc.ID_REV_DRAW,
	i."Item_Work Center_Rubin" AS RBN_WC
FROM ORDERS_BASE o
LEFT JOIN WC_LOOKUP wc ON TRIM(o.ID_SO) = TRIM(wc."ShopOrder#") AND wc.SHOP_ORDER_LOCATION = o.ID_LOC
LEFT JOIN SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER inv ON o.ID_ITEM = inv."Product_ID_SKU" AND o.ID_LOC = inv."Location_ID"
LEFT JOIN PRODUCT_DIM i ON o.ID_ITEM = i."Item ID_Child SKU"
LEFT JOIN SHIP_LINE sl ON o.ID_ORD = sl.ID_ORD AND o.SEQ_LINE_ORD = sl.SEQ_LINE_ORD
LEFT JOIN SHIP_ORD so ON o.ID_ORD = so.ID_ORD
LEFT JOIN WORKING_DAYS wd ON o.ID_ORD = wd.ID_ORD AND o.SEQ_LINE_ORD = wd.SEQ_LINE_ORD
LEFT JOIN WORKDAY_3_AFTER_ORD w3 ON CAST(o.DATE_ORD AS DATE) = w3.base_date
LEFT JOIN WORKDAY_1_AFTER_ADD w1 ON CAST(o.DATE_ADD AS DATE) = w1.base_date
LEFT JOIN WORKDAY_10_AFTER_PROM w10 ON CAST(o.DATE_PROM AS DATE) = w10.base_date
LEFT JOIN SBNB ON SBNB.ID_ITEM = o.ID_ITEM AND SBNB.ID_LOC = o.ID_LOC
LEFT JOIN SO_QTY_BY_PARENT soq ON soq.ID_ITEM_PAR = o.ID_ITEM AND soq.SHOP_ORDER_LOCATION = o.ID_LOC
LEFT JOIN QTY_STAGING qty_stg ON qty_stg.ID_ITEM_PAR = o.ID_ITEM AND qty_stg.SHOP_ORDER_LOCATION = o.ID_LOC
LEFT JOIN SO_QTY_PENDING_BY_PARENT soq_pnd ON soq_pnd.ID_ITEM_PAR_NP = o.ID_ITEM AND soq_pnd.SHOP_ORDER_LOCATION = o.ID_LOC;