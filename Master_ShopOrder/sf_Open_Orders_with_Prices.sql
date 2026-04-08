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
-- SHIP_LINE: line-grain shipment rollup used for latest ship/carrier and shipped qty offsets.
WITH SHIP_LINE AS (
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
		FROM (SELECT DISTINCT DATE_ORD FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE WHERE DATE_ORD IS NOT NULL) o
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
		FROM (SELECT DISTINCT DATE_ADD FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE WHERE DATE_ADD IS NOT NULL) o
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
		FROM (SELECT DISTINCT DATE_PROM FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE WHERE DATE_PROM IS NOT NULL) o
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
-- SHOPORDER_HDR: de-duplicates shop-order WC data down to header-like rows to avoid operation double counting.
SHOPORDER_HDR AS (
	SELECT DISTINCT
		SHOP_ORDER_LOCATION,
		"ShopOrder#",
		SUFX_SO,
		ID_ITEM_PAR,
		STAT_REC_SO,
		QTY_REMAINING
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
),
-- PCC_ORDERS: identifies shop orders with completed operation 3999 for presew logic.
PCC_ORDERS AS (
	SELECT DISTINCT
		SHOP_ORDER_LOCATION,
		"ShopOrder#",
		SUFX_SO
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE
	WHERE ID_OPER = 3999
		AND STAT_REC_OPER = 'C'
),
-- WPR: released shop-order qty by parent item/location, used for Qty_Rel.
WPR AS (
	SELECT
		ID_ITEM_PAR,
		SHOP_ORDER_LOCATION,
		SUM(QTY_REMAINING) AS SUM_QTY_ONORD
	FROM SHOPORDER_HDR
	WHERE STAT_REC_SO = 'R'
	GROUP BY ID_ITEM_PAR, SHOP_ORDER_LOCATION
),
-- WPS: started shop-order qty by parent item/location, base for Qty_Start and Qty_presew.
WPS AS (
	SELECT
		ID_ITEM_PAR,
		SHOP_ORDER_LOCATION,
		SUM(QTY_REMAINING) AS SUM_QTY_ONORD
	FROM SHOPORDER_HDR
	WHERE STAT_REC_SO = 'S'
	GROUP BY ID_ITEM_PAR, SHOP_ORDER_LOCATION
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
-- WPR_PND: pending released qty (# items) by parent item/location for Qty_Rel_PND.
WPR_PND AS (
	SELECT
		ID_ITEM_PAR,
		SHOP_ORDER_LOCATION,
		REPLACE(ID_ITEM_PAR, '#', '') AS ID_ITEM_PAR_NP,
		SUM(QTY_REMAINING) AS SUM_QTY_ONORD
	FROM SHOPORDER_HDR
	WHERE STAT_REC_SO = 'R'
		AND ID_ITEM_PAR LIKE '%#'
	GROUP BY ID_ITEM_PAR, SHOP_ORDER_LOCATION
),
-- WPS_PND: pending started qty (# items) by parent item/location for Qty_Start_PND.
WPS_PND AS (
	SELECT
		ID_ITEM_PAR,
		SHOP_ORDER_LOCATION,
		REPLACE(ID_ITEM_PAR, '#', '') AS ID_ITEM_PAR_NP,
		SUM(QTY_REMAINING) AS SUM_QTY_ONORD
	FROM SHOPORDER_HDR
	WHERE STAT_REC_SO = 'S'
		AND ID_ITEM_PAR LIKE '%#'
	GROUP BY ID_ITEM_PAR, SHOP_ORDER_LOCATION
),
-- QTY_STAGING: centralizes shared WPS/PCC math so Qty_Start and Qty_presew stay consistent.
QTY_STAGING AS (
	SELECT
		wps.ID_ITEM_PAR,
		wps.SHOP_ORDER_LOCATION,
		COALESCE(wps.SUM_QTY_ONORD, 0) AS wps_qty,
		COALESCE(
			CASE WHEN pcc.STAT_REC_SO IS NULL THEN wps.SUM_QTY_ONORD ELSE pcc.SUM_QTY_ONORD END,
			0
		) AS presew_qty
	FROM WPS wps
	LEFT JOIN PCC pcc
		ON pcc.ID_ITEM_PAR = wps.ID_ITEM_PAR
		AND pcc.SHOP_ORDER_LOCATION = wps.SHOP_ORDER_LOCATION
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
	COALESCE(wpr.SUM_QTY_ONORD, 0) AS Qty_Rel,
	COALESCE(qty_stg.wps_qty, 0) - COALESCE(qty_stg.presew_qty, 0) AS Qty_Start,
	COALESCE(qty_stg.presew_qty, 0) AS Qty_presew,
	COALESCE(wpr_pnd.SUM_QTY_ONORD, 0) AS Qty_Rel_PND,
	COALESCE(wps_pnd.SUM_QTY_ONORD, 0) AS Qty_Start_PND,
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
FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE wc ON TRIM(o.ID_SO) = TRIM(wc."ShopOrder#")
LEFT JOIN SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER inv ON o.ID_ITEM = inv."Product_ID_SKU" AND o.ID_LOC = inv."Location_ID"
LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE i ON o.ID_ITEM = i."Item ID_Child SKU"
LEFT JOIN SHIP_LINE sl ON o.ID_ORD = sl.ID_ORD AND o.SEQ_LINE_ORD = sl.SEQ_LINE_ORD
LEFT JOIN SHIP_ORD so ON o.ID_ORD = so.ID_ORD
LEFT JOIN WORKING_DAYS wd ON o.ID_ORD = wd.ID_ORD AND o.SEQ_LINE_ORD = wd.SEQ_LINE_ORD
LEFT JOIN WORKDAY_3_AFTER_ORD w3 ON CAST(o.DATE_ORD AS DATE) = w3.base_date
LEFT JOIN WORKDAY_1_AFTER_ADD w1 ON CAST(o.DATE_ADD AS DATE) = w1.base_date
LEFT JOIN WORKDAY_10_AFTER_PROM w10 ON CAST(o.DATE_PROM AS DATE) = w10.base_date
LEFT JOIN SBNB ON SBNB.ID_ITEM = o.ID_ITEM AND SBNB.ID_LOC = o.ID_LOC
LEFT JOIN WPR ON WPR.ID_ITEM_PAR = o.ID_ITEM AND WPR.SHOP_ORDER_LOCATION = o.ID_LOC
LEFT JOIN QTY_STAGING qty_stg ON qty_stg.ID_ITEM_PAR = o.ID_ITEM AND qty_stg.SHOP_ORDER_LOCATION = o.ID_LOC
LEFT JOIN WPR_PND ON WPR_PND.ID_ITEM_PAR_NP = o.ID_ITEM AND WPR_PND.SHOP_ORDER_LOCATION = o.ID_LOC
LEFT JOIN WPS_PND ON WPS_PND.ID_ITEM_PAR_NP = o.ID_ITEM AND WPS_PND.SHOP_ORDER_LOCATION = o.ID_LOC;