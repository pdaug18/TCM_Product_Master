CREATE OR REPLACE VIEW SILVER_DATA.TCM_SILVER.GOLD_OPEN_ORDERS_WITH_PRICES AS
/*

Gap list (not present in current master_* outputs) with OOwP_temp source lineage:
1) WorkingDaysSinceLastPicked
	- Source: ol.DATE_PICK_LAST and ol.FLAG_PICK from nsa.CP_ORDLIN.
	- Logic: CASE when ol.flag_pick = 2 then CAST(nsa.WORKINGDAYSBETWEEN(ol.DATE_PICK_LAST, GETDATE()) as varchar) else ''.
3) flag_ack	  Note: ship_complete_flag is now derived from MASTER_ORDERS_TABLE.LINE_COMMENT_NOTE; see SELECT below.	
    - Source: oh.FLAG_ACKN from nsa.CP_ORDHDR.
	- Logic: CASE when oh.flag_ackn = 2 then 'A' else ''.
4) DATE_CALC_START
	- Source: ol.DATE_RQST/DATE_PROM/DATE_ADD/FLAG_STK (CP_ORDLIN), oh.ID_CUST_SOLDTO/DATE_ORD (CP_ORDHDR), ilPAR.ID_PLANNER (ITMMAS_LOC).
	- Logic: business CASE using nsa.AddWorkDays with customer and planner/stock exceptions.
5) DATE_CALC_END --! Use DIM calendar in DIM > Calendar 
	- Source: same inputs as DATE_CALC_START.
	- Logic: business CASE using nsa.AddWorkDays(10, ...) with same exception branches.
6) id_item_comp
	- Source: ps.ID_ITEM_COMP from ps subquery (nsa.PRDSTR joined to nsa.ITMMAS_BASE with code_comm = 'FG').
7) alt_stk; lookinto shoporder master
	- Source: sh.ID_BUYER (nsa.SHPORD_HDR), il.QTY_ONHD (nsa.ITMMAS_LOC), ir.LEVEL_ROP (nsa.ITMMAS_REORD).
	- Logic: CASE returns AS/KT/'' by buyer and stocking checks.
8) FLAG_MO
	- Source: MO subquery using nsa.CP_ORDHDR_CUSTOM_COMMENTS (FLAG_DEL <> 'D').
	- Logic: CASE when comment like '%#MO%' then 'Y' else 'N'.
9) STOCK_STATUS look into item master and item inventory master
	- Source: ib.CODE_COMM (ITMMAS_BASE), il.FLAG_STK (ITMMAS_LOC), ir.LEVEL_ROP (ITMMAS_REORD), chk.KIT_AS_flag_stk (derived kit subquery).
	- Logic: CASE => '3-FABRIC' / '1-STOCK' / '2-MTO'.
10) Qty_Start
	- Source: WPS and PCC subqueries from nsa.SHPORD_HDR, plus nsa.SHPORD_OPER (ID_OPER = 3999, STAT_REC_OPER = 'C').
	- Logic: isnull(WPS.SUM_QTY_ONORD,0) - isnull(CASE when PCC.stat_rec_so is null then WPS.SUM_QTY_ONORD else PCC.SUM_QTY_ONORD end,0).
11) Qty_presew
	- Source: same WPS/PCC pipeline as Qty_Start.
	- Logic: isnull(CASE when PCC.stat_rec_so is null then WPS.SUM_QTY_ONORD else PCC.SUM_QTY_ONORD end,0).
12) Qty_Rel_PND
	- Source: WPR_PND subquery from nsa.SHPORD_HDR where STAT_REC_SO = 'R' and ID_ITEM_PAR like '%#'.
13) Qty_Start_PND
	- Source: WPS_PND subquery from nsa.SHPORD_HDR where STAT_REC_SO = 'S' and ID_ITEM_PAR like '%#'.
14) SBNB    look into shipment master table
	- Source: tSBNB subquery from nsa.CP_SHPLIN.
	- Logic: sum(QTY_SHIP) by ID_ITEM, ID_LOC where FLAG_CONFIRM_SHIP <> 1.
15) BIN_PRIM look into item_iventory master
	- Source: il.BIN_PRIM from nsa.ITMMAS_LOC (component-aware join path in OOwP).
16) stk_test    item_invetory master
	- Source: ir.LEVEL_ROP (ITMMAS_REORD) and il.FLAG_STK (ITMMAS_LOC).
	- Logic: CASE when LEVEL_ROP > 1 and flag_stk = 'S' then 1 else 0.
17) STAT_REC_SO_display --! Not needed yet
	- Source: sh.STAT_REC_SO/DATE_START_OPER_1ST (SHPORD_HDR), so3999/so9999.STAT_REC_OPER (SHPORD_OPER), ilPAR.ID_PLANNER (ITMMAS_LOC).
	- Logic: business CASE that remaps SO status to R/W/D under operation/planner conditions.
18) CREDIT_STATUS 
	- Source: cs.STATUS_CREDIT from nsa.CP_CREDIT_STS (joined on ID_ORD with TYPE_REC = 0). lookinto customer master
	- Logic: CASE 0 -> 'H', 1 -> 'R'.

Notes:
- The fields above are intentionally excluded from this draft.
- This view uses only SILVER master tables and available/derivable attributes.
*/
WITH SHIP_LINE AS (
	SELECT
		ID_ORD,
		SEQ_LINE_ORD,
		MAX(ID_SHIP) AS ID_SHIP,
		MAX(LINE_DATE_PICK_LAST) AS DATE_PICK_LAST,
		MAX(LINE_FLAG_PICK) AS FLAG_PICK,
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
)
SELECT DISTINCT
	CURRENT_TIMESTAMP() AS dataRefreshTimeStamp,
	o.OPEN_NET_AMT AS open_net_amt,
	o.ID_CUST_SOLDTO AS cust_soldto,
	-- ship_complete_flag: derived from LINE_COMMENT_NOTE (equivalent of CP_COMMENT.NOTE in OOwP)
	-- Original logic: distinct ID_ORD where NOTE ilike '%SHIP%COMPLETE%' and note not ilike '%LINE%'
	CASE
		WHEN o.LINE_COMMENT_NOTE ILIKE '%SHIP%COMPLETE%'
		 AND o.LINE_COMMENT_NOTE NOT ILIKE '%LINE%'
		THEN 'Y'
	END AS ship_complete_flag,
	sl.DATE_PICK_LAST AS date_pick_last,
	sl.FLAG_PICK AS flag_pick,
	o.AMT_ORD_TOTAL AS amt_ord_total,
	o.ID_SLSREP_1,
	o.DESCR_SHIP_VIA,
	o.DATE_RQST AS DR,
	o.DATE_PROM AS DP,
	o.DATE_ORD AS DO,
	o.ID_ITEM,
	o.ID_ORD,
	o.ID_USER_ADD,
	o.ORD_DATE_CREATED AS DATE_ADD,
	o.SEQ_LINE_ORD,
	o.ID_SO AS id_so_odbc,
	i."Item_Vertical" AS VERTICAL,
	o.CODE_CAT_PRDT,
	i."CODE_USER_1" AS CODE_USER_1_IM,
	wc.ID_REV_DRAW,
	sl.ID_CARRIER,
	o.QTY_OPEN,
	i."Item Status_Child Active Status" AS FLAG_STAT_ITEM,
	o.FLAG_STK AS OL_FLAG_STK,
	inv."Item_Stock_Flag" AS IL_FLAG_STK,
	o.FLAG_STK,
	i."Item_Work Center_Rubin" AS RBN_WC,
	inv."Qty_Released" AS Qty_Rel,
	inv."Qty_On_Hand" AS QTY_ONHD,
	inv."Qty_Allocated" AS QTY_ALLOC,
	inv."Qty_On_Order" AS QTY_ONORD,
	inv."Item_Source_Flag" AS FLAG_SOURCE,
	inv."Item_Bin_Tracking" AS FLAG_TRACK_BIN,
	inv."Item_Inventory_Reorder_Point" AS LEVEL_ROP,
	i."Unit_of_Measure_Price" AS CODE_UM_PRICE,
	o.NAME_CUST AS name_cust_soldto,
	o.ID_SLSREP_1 AS id_slsrep,
	wc.STAT_REC_SO,
	wc."ShopOrder#" AS ID_SO,
	o.QTY_SHIP_TOTAL,
	sl.ID_SHIP,
	o.CODE_STAT_ORD,
	o.ID_PO_CUST,
	so.NUM_SHIPMENTS,
	so.NUM_INVCS,
	1 AS COUNTER,
	o.ID_LOC
FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE wc ON TRIM(o.ID_SO) = TRIM(wc."ShopOrder#") 
-- AND o.SUFX_SO = wc.SUFX_SO
LEFT JOIN SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER inv ON o.ID_ITEM = inv."Product_ID_SKU" 
-- AND o.ID_LOC = inv."Location_ID"
LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE i ON o.ID_ITEM = i."Item ID_Child SKU"
LEFT JOIN SHIP_LINE sl ON o.ID_ORD = sl.ID_ORD AND o.SEQ_LINE_ORD = sl.SEQ_LINE_ORD
LEFT JOIN SHIP_ORD so ON o.ID_ORD = so.ID_ORD;
