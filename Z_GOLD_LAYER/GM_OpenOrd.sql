CREATE OR REPLACE DYNAMIC TABLE GOLD_DATA.TCM_GOLD.OPEN_ORDERS_temp
	TARGET_LAG   = 'DOWNSTREAM'
	REFRESH_MODE = AUTO
	INITIALIZE   = ON_CREATE
	WAREHOUSE    = ELT_DEFAULT
AS
/* ============================================================
   GOLD_MASTER — Open Orders (Order-Line Grain)
   Priority source: SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE

   Open-order definition:
   - "Open Order Quantity" > 0
   - "Line_Source_Table" = 'ACTIVE'

   OOwP carryover in this version:
   - WorkingDaysSinceLastPicked (business-day count only)

   Duplicate governance:
   - MASTER_ORDERS_TABLE remains canonical where overlaps exist.
   - Alternative source values are retained as *_REVIEW columns.
   ============================================================ */
WITH open_orders_base AS (
	SELECT
		o.*,
		(o."Original Order Quantity" - o."Total Shipped Quantity") AS qty_remaining_calc
	FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
	WHERE o."Open Order Quantity" > 0
	  AND o."Line_Source_Table" = 'ACTIVE'
),

product_dim AS (
	SELECT
		p."Item ID_Child SKU"               AS product_id_sku,
		p."Item Description_Child SKU"      AS prd_item_description,
		p."PRODUCT CATEGORY/VERTICAL"       AS prd_product_category_code,
		p."Item_Cost Category ID"           AS prd_cost_category_code,
		p."Item_Vertical"                   AS prd_vertical,
		p."CATEGORY (Calc)"                 AS prd_category,
		p."Item ID_Parent SKU"              AS parent_product_id,
		p."Item Description_Parent SKU"     AS parent_item_description,
		p."ID_PLANNER"                      AS prd_planner,
		p."Item Status_Child Active Status" AS child_item_status,
		p."Adj_Parent_Item_Status"          AS adjusted_parent_item_status
	FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE p
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY p."Item ID_Child SKU"
		ORDER BY
			CASE WHEN COALESCE(p."Item Status_Child Active Status", '') = 'A' THEN 0 ELSE 1 END,
			CASE WHEN COALESCE(p."Item_Location ID", '') = '10' THEN 0 ELSE 1 END,
			COALESCE(p."Item_Location ID", '')
	) = 1
),

inventory_dim AS (
	SELECT
		i."Product_ID_SKU"                      AS product_id_sku,
		i."Location_ID"                         AS location_id,
		i."Item_Description"                    AS inv_item_description,
		i."Item_Source_Flag"                    AS inv_item_source_flag,
		i."Primary_Source"                      AS inv_primary_source,
		i."Item_Planned_Classification"         AS inv_planner,
		i."Qty_On_Hand"                         AS inv_qty_on_hand,
		i."Qty_Allocated"                       AS inv_qty_allocated,
		i."Qty_On_Order"                        AS inv_qty_on_order,
		i."Primary_Bin"                         AS inv_primary_bin,
		i."Item_Stock_Flag"                     AS inv_item_stock_flag,
		i."Item_Bin_Tracking"                   AS inv_item_bin_tracking,
		i."Item_Fulfillment_Type"               AS inv_fulfillment_type,
		i."Item_Inventory_Reorder_Point"        AS inv_reorder_point,
		i."Item_Inventory_Reorder_Point_Minimum" AS inv_reorder_point_minimum,
		i."Item_Order_Quantity_Econ"            AS inv_order_quantity_econ,
		i."Qty_Released"                        AS inv_qty_released
	FROM SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER i
),

vendor_dim AS (
	SELECT
		LTRIM(v.id_item)                 AS product_id_sku,
		v.primary_vendor_id,
		v.primary_vendor_name,
		v.secondary_vendor_ids,
		v.secondary_vendor_names
	FROM SILVER_DATA.TCM_SILVER.MASTER_ITEM_VENDOR_TABLE v
),

shipment_totals AS (
	SELECT
		s.ID_ORD,
		s.SEQ_LINE_ORD,
		COUNT(DISTINCT s.ID_SHIP)        AS shipment_count,
		SUM(COALESCE(s.QTY_SHIP, 0))     AS shipment_qty_total,
		MAX(s.DATE_SHIP)                 AS last_ship_date,
		MAX(s.ID_CARRIER)                AS last_carrier_id
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE s
	GROUP BY
		s.ID_ORD,
		s.SEQ_LINE_ORD
),

shipment_latest AS (
	SELECT
		s.ID_ORD,
		s.SEQ_LINE_ORD,
		s.ID_SHIP,
		s.ID_INVC,
		s.DATE_SHIP,
		s.ID_CARRIER
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE s
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY s.ID_ORD, s.SEQ_LINE_ORD
		ORDER BY s.DATE_SHIP DESC NULLS LAST, s.ID_SHIP DESC NULLS LAST
	) = 1
),

shipment_line AS (
	SELECT
		t.ID_ORD,
		t.SEQ_LINE_ORD,
		t.shipment_count,
		t.shipment_qty_total,
		t.last_ship_date,
		COALESCE(l.ID_SHIP, '')          AS latest_shipment_id,
		COALESCE(l.ID_INVC, '')          AS latest_invoice_id,
		COALESCE(l.ID_CARRIER, t.last_carrier_id) AS latest_carrier_id
	FROM shipment_totals t
	LEFT JOIN shipment_latest l
		ON t.ID_ORD = l.ID_ORD
	   AND t.SEQ_LINE_ORD = l.SEQ_LINE_ORD
),

shop_wc_pick AS (
	SELECT
		w.SHOP_ORDER_LOCATION,
		w."ShopOrder#",
		w.SUFX_SO,
		w.STAT_REC_SO,
		w.QTY_ORD,
		w.QTY_CMPL,
		w.QTY_REMAINING,
		w.DATE_DUE_ORD,
		w.SO_DATE_COMPLETED,
		w.CODE_CAT_COST,
		TRIM(COALESCE(w.DESCR_ITEM_1, '') || ' ' || COALESCE(w.DESCR_ITEM_2, '')) AS shop_item_description
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE w
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY w.SHOP_ORDER_LOCATION, w."ShopOrder#", w.SUFX_SO
		ORDER BY w.SO_DATE_LAST_CHANGED DESC NULLS LAST, w.ID_OPER ASC
	) = 1
),

working_days AS (
	SELECT
		o."Order ID"                     AS order_id,
		o."Order_Sequence Line Number"   AS seq_line_ord,
		COUNT(*)                          AS working_days_since_last_picked
	FROM open_orders_base o
	INNER JOIN GOLD_DATA.DIM.DIM_CALENDAR d
		ON d.CALENDAR_DATE > CAST(o."Date_Order_Last_Picked" AS DATE)
	   AND d.CALENDAR_DATE <= CURRENT_DATE()
	   AND d.IS_WEEKDAY = TRUE
	   AND COALESCE(d.IS_HOLIDAY, FALSE) = FALSE
	WHERE TO_VARCHAR(o."Order_Pick_Flag") = '2'
	  AND o."Date_Order_Last_Picked" IS NOT NULL
	GROUP BY
		o."Order ID",
		o."Order_Sequence Line Number"
),

assembled AS (
	SELECT
		o."Order ID"                                 AS order_id,
		o."Order_Sequence Line Number"               AS order_sequence_line_number,
		o."Item ID_Child SKU"                        AS product_id_sku,
		o."Item Location"                            AS location_id,

		/* Canonical columns (orders-first priority) */
		o."Item_Product_Category_Code"               AS canonical_product_category_code,
		COALESCE(
			i.inv_item_description,
			p.prd_item_description,
			sw.shop_item_description
		)                                             AS canonical_item_description,
		COALESCE(
			p.prd_cost_category_code,
			sw.CODE_CAT_COST
		)                                             AS canonical_cost_category_code,
		COALESCE(
			i.inv_planner,
			p.prd_planner
		)                                             AS canonical_item_planner,

		o."Line_Source_Table",
		o."Customer_ID_Sold-To",
		o."Customer_Name",
		o."Customer Purchase_Order_ID",
		o."Order_Type",
		o."Order_Status_Code",
		o."Date_Order",
		o."Date_Order_Created",
		o."Date_Line_Requested",
		o."Date_Line_Promised",
		o."Date_Order_Last_Picked",
		o."Location ID_Ship-From",
		o."Shipping_Method_Description",
		o."ID_Sales_Rep_1",

		o."Original Order Quantity",
		o."Open Order Quantity",
		o."Backordered Quantity",
		o."Booked Quantity",
		o."Released Quantity",
		o."Allocated Quantity",
		o."Total Shipped Quantity",
		o.qty_remaining_calc,

		o.COST_UNIT,
		o.PRICE_LIST,
		o.PRICE_SELL,
		o.PRICE_SELL_NET,
		o.OPEN_COST,
		o.OPEN_LIST_AMT,
		o.OPEN_SELL_AMT,
		o.OPEN_NET_AMT,

		o."Order_Pick_Flag",
		o."Order_Line_Stock_Flag",
		o."Order_Backorder_Flag",
		o."Order_Acknowledgement_Flag",
		o.ID_LOC_SO,
		o.ID_SO,
		o.SUFX_SO,

		p.parent_product_id,
		p.parent_item_description,
		p.prd_vertical,
		p.prd_category,
		p.child_item_status,
		p.adjusted_parent_item_status,

		i.inv_item_source_flag,
		i.inv_primary_source,
		i.inv_qty_on_hand,
		i.inv_qty_allocated,
		i.inv_qty_on_order,
		i.inv_primary_bin,
		i.inv_item_stock_flag,
		i.inv_item_bin_tracking,
		i.inv_fulfillment_type,
		i.inv_reorder_point,
		i.inv_reorder_point_minimum,
		i.inv_order_quantity_econ,
		i.inv_qty_released,

		v.primary_vendor_id,
		v.primary_vendor_name,
		v.secondary_vendor_ids,
		v.secondary_vendor_names,

		s.shipment_count,
		s.shipment_qty_total,
		s.last_ship_date,
		s.latest_shipment_id,
		s.latest_invoice_id,
		s.latest_carrier_id,

		sw.STAT_REC_SO                                 AS shop_order_status,
		sw.QTY_ORD                                     AS shop_order_qty,
		sw.QTY_CMPL                                    AS shop_completed_qty,
		sw.QTY_REMAINING                               AS shop_remaining_qty,
		sw.DATE_DUE_ORD                                AS shop_due_date,
		sw.SO_DATE_COMPLETED                           AS shop_completed_date,

		COALESCE(wd.working_days_since_last_picked, 0) AS working_days_since_last_picked,

		/* Overlap review columns */
		p.prd_product_category_code                    AS product_category_product_review,
		p.prd_cost_category_code                       AS cost_category_product_review,
		sw.CODE_CAT_COST                               AS cost_category_shoporder_review,
		i.inv_item_description                         AS item_description_inventory_review,
		p.prd_item_description                         AS item_description_product_review,
		sw.shop_item_description                       AS item_description_shoporder_review,
		i.inv_planner                                  AS planner_inventory_review,
		p.prd_planner                                  AS planner_product_review
	FROM open_orders_base o
	LEFT JOIN product_dim p
		ON LTRIM(o."Item ID_Child SKU") = LTRIM(p.product_id_sku)
	LEFT JOIN inventory_dim i
		ON LTRIM(o."Item ID_Child SKU") = LTRIM(i.product_id_sku)
	   AND o."Item Location" = i.location_id
	LEFT JOIN vendor_dim v
		ON LTRIM(o."Item ID_Child SKU") = LTRIM(v.product_id_sku)
	LEFT JOIN shipment_line s
		ON o."Order ID" = s.ID_ORD
	   AND o."Order_Sequence Line Number" = s.SEQ_LINE_ORD
	LEFT JOIN shop_wc_pick sw
		ON o."Item Location" = sw.SHOP_ORDER_LOCATION
	   AND TRIM(o.ID_SO) = TRIM(sw."ShopOrder#")
	   AND o.SUFX_SO = sw.SUFX_SO
	LEFT JOIN working_days wd
		ON o."Order ID" = wd.order_id
	   AND o."Order_Sequence Line Number" = wd.seq_line_ord
)

SELECT
	a.order_id                                    AS "Order ID",
	a.order_sequence_line_number                  AS "Order_Sequence Line Number",
	a.product_id_sku                              AS "Item ID_Child SKU",
	a.location_id                                 AS "Item Location",
	a."Line_Source_Table"                        AS "Line_Source_Table",

	a."Customer_ID_Sold-To"                      AS "Customer_ID_Sold-To",
	a."Customer_Name"                            AS "Customer_Name",
	a."Customer Purchase_Order_ID"               AS "Customer Purchase_Order_ID",
	a."Order_Type"                               AS "Order_Type",
	a."Order_Status_Code"                        AS "Order_Status_Code",
	a."Date_Order"                               AS "Date_Order",
	a."Date_Order_Created"                       AS "Date_Order_Created",
	a."Date_Line_Requested"                      AS "Date_Line_Requested",
	a."Date_Line_Promised"                       AS "Date_Line_Promised",
	a."Date_Order_Last_Picked"                   AS "Date_Order_Last_Picked",
	a."Location ID_Ship-From"                    AS "Location ID_Ship-From",
	a."Shipping_Method_Description"              AS "Shipping_Method_Description",
	a."ID_Sales_Rep_1"                           AS "ID_Sales_Rep_1",

	a.canonical_item_description                  AS "Item Description",
	a.canonical_product_category_code             AS "Product Category Code",
	a.canonical_cost_category_code                AS "Cost Category Code",
	a.canonical_item_planner                      AS "Item Planner",

	a."Original Order Quantity"                  AS "Original Order Quantity",
	a."Open Order Quantity"                      AS "Open Order Quantity",
	a."Backordered Quantity"                     AS "Backordered Quantity",
	a."Booked Quantity"                          AS "Booked Quantity",
	a."Released Quantity"                        AS "Released Quantity",
	a."Allocated Quantity"                       AS "Allocated Quantity",
	a."Total Shipped Quantity"                   AS "Total Shipped Quantity",
	a.qty_remaining_calc                          AS "Qty Remaining",

	a.COST_UNIT                                   AS "Cost Unit",
	a.PRICE_LIST                                  AS "Price List",
	a.PRICE_SELL                                  AS "Price Sell",
	a.PRICE_SELL_NET                              AS "Price Sell Net",
	a.OPEN_COST                                   AS "Open Cost",
	a.OPEN_LIST_AMT                               AS "Open List Amount",
	a.OPEN_SELL_AMT                               AS "Open Sell Amount",
	a.OPEN_NET_AMT                                AS "Open Net Amount",

	a."Order_Pick_Flag"                          AS "Order_Pick_Flag",
	a.working_days_since_last_picked              AS "WorkingDaysSinceLastPicked",
	a."Order_Line_Stock_Flag"                    AS "Order_Line_Stock_Flag",
	a."Order_Backorder_Flag"                     AS "Order_Backorder_Flag",
	a."Order_Acknowledgement_Flag"               AS "Order_Acknowledgement_Flag",

	a.parent_product_id                           AS "Parent Product ID",
	a.parent_item_description                     AS "Parent Item Description",
	a.prd_vertical                                AS "Vertical",
	a.prd_category                                AS "Category",
	a.child_item_status                           AS "Child Item Status",
	a.adjusted_parent_item_status                 AS "Adjusted Parent Item Status",

	a.inv_item_source_flag                        AS "Item Source Flag",
	a.inv_primary_source                          AS "Primary Source",
	a.inv_qty_on_hand                             AS "Inv Qty On Hand",
	a.inv_qty_allocated                           AS "Inv Qty Allocated",
	a.inv_qty_on_order                            AS "Inv Qty On Order",
	a.inv_primary_bin                             AS "Inv Primary Bin",
	a.inv_item_stock_flag                         AS "Inv Item Stock Flag",
	a.inv_item_bin_tracking                       AS "Inv Item Bin Tracking",
	a.inv_fulfillment_type                        AS "Inv Fulfillment Type",
	a.inv_reorder_point                           AS "Inv Reorder Point",
	a.inv_reorder_point_minimum                   AS "Inv Reorder Point Minimum",
	a.inv_order_quantity_econ                     AS "Inv Economic Order Quantity",
	a.inv_qty_released                            AS "Inv Qty Released",

	a.primary_vendor_id                           AS "Primary Vendor ID",
	a.primary_vendor_name                         AS "Primary Vendor Name",
	a.secondary_vendor_ids                        AS "Secondary Vendor IDs",
	a.secondary_vendor_names                      AS "Secondary Vendor Names",

	a.shipment_count                              AS "Shipment Count",
	a.shipment_qty_total                          AS "Shipment Qty Total",
	a.last_ship_date                              AS "Last Ship Date",
	a.latest_shipment_id                          AS "Latest Shipment ID",
	a.latest_invoice_id                           AS "Latest Invoice ID",
	a.latest_carrier_id                           AS "Latest Carrier ID",

	a.shop_order_status                           AS "Shop Order Status",
	a.shop_order_qty                              AS "Shop Order Qty",
	a.shop_completed_qty                          AS "Shop Completed Qty",
	a.shop_remaining_qty                          AS "Shop Remaining Qty",
	a.shop_due_date                               AS "Shop Due Date",
	a.shop_completed_date                         AS "Shop Completed Date",

	/* Overlap review columns */
	a.item_description_inventory_review           AS "Item Description Inventory_REVIEW",
	a.item_description_product_review             AS "Item Description Product_REVIEW",
	a.item_description_shoporder_review           AS "Item Description ShopOrder_REVIEW",
	a.product_category_product_review             AS "Product Category Product_REVIEW",
	a.cost_category_product_review                AS "Cost Category Product_REVIEW",
	a.cost_category_shoporder_review              AS "Cost Category ShopOrder_REVIEW",
	a.planner_inventory_review                    AS "Item Planner Inventory_REVIEW",
	a.planner_product_review                      AS "Item Planner Product_REVIEW"
FROM assembled a;
