CREATE OR REPLACE DYNAMIC TABLE GOLD_DATA.TCM_GOLD.GOLD_MASTER
	TARGET_LAG   = 'DOWNSTREAM'
	REFRESH_MODE = AUTO
	INITIALIZE   = ON_CREATE
	WAREHOUSE    = ELT_DEFAULT
AS
/* ============================================================
   GOLD_MASTER — Item + Location Gold Layer
   Sources:
   - SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER (base grain)
   - SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE (item attributes)
   - SILVER_DATA.TCM_SILVER.MASTER_ITEM_VENDOR_TABLE (vendor attributes)
   - SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE (12-month item-location rollups)
   - SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE (12-month item-location rollups)
   - SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE (12-month item-location rollups)

   Duplicate governance:
   - Canonical values use source priority:
	 Inventory > Product > Orders > Shipment > ShopOrder > Vendor
   - Overlapping alternatives retained with _REVIEW aliases.
   ============================================================ */
WITH base_inventory AS (
	SELECT
		i."Product_ID_SKU"                          AS product_id_sku,
		i."Location_ID"                             AS location_id,
		i."Item_Description"                        AS inv_item_description,
		i."Item_Source_Flag"                        AS inv_item_source_flag,
		i."Primary_Source"                          AS inv_primary_source,
		i."NSA_Manufactured"                        AS inv_nsa_manufactured,
		i."Qty_On_Hand"                             AS qty_on_hand,
		i."Qty_Allocated"                           AS qty_allocated,
		i."Qty_On_Order"                            AS qty_on_order,
		i."Primary_Bin"                             AS primary_bin,
		i."Item_Stock_Flag"                         AS item_stock_flag,
		i."Item_Bin_Tracking"                       AS item_bin_tracking,
		i."Item_Controlled_Noncontrolled_Flag"      AS item_control_flag,
		i."Item_Fulfillment_Type"                   AS item_fulfillment_type,
		i."Item_Order_Policy_Flag"                  AS item_order_policy_flag,
		i."Item_Planned_Classification"             AS inv_item_planner,
		i."Item_Primary_Location_Type"              AS item_primary_location_type,
		i."Item_Routing_Number"                     AS item_routing_number,
		i."Item_Shop_Floor_Auto_Issue_Flag"         AS item_shop_floor_auto_issue_flag,
		i."Item_Home_Location_Code"                 AS item_home_location_code,
		i."Item_Inventory_Reorder_Point"            AS reorder_point,
		i."Item_Inventory_Reorder_Point_Minimum"    AS reorder_point_minimum,
		i."Item_Inventory_Reorder_Point_Mult"       AS reorder_point_multiple,
		i."Item_Order_Quantity_Econ"                AS order_quantity_economic,
		i."Item_Reorder_Point_Lead_Time"            AS reorder_point_lead_time,
		i."Qty_Cut"                                 AS qty_cut,
		i."Qty_Released"                            AS qty_released,
		i."Source_Location_Match_Flag"              AS source_location_match_flag
	FROM SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER i
),

product_dim AS (
	SELECT
		p."Item ID_Child SKU"                       AS product_id_sku,
		p."Item Description_Child SKU"              AS prd_item_description,
		p."PRODUCT CATEGORY/VERTICAL"               AS prd_product_category_code,
		p."Item_Cost Category ID"                   AS prd_cost_category_code,
		p."Item ID_Parent SKU"                      AS parent_product_id,
		p."Item Description_Parent SKU"             AS parent_item_description,
		p."Item_Vertical"                           AS vertical,
		p."CATEGORY (Calc)"                         AS category,
		p."ID_PLANNER"                              AS prd_item_planner,
		p."Item Status_Child Active Status"         AS child_item_status,
		p."Adj_Parent_Item_Status"                  AS adjusted_parent_item_status
	FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE p
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY p."Item ID_Child SKU"
		ORDER BY CASE WHEN COALESCE(p."Item_Location ID", '') = '10' THEN 0 ELSE 1 END,
				 COALESCE(p."Item_Location ID", '')
	) = 1
),

vendor_dim AS (
	SELECT
		LTRIM(v.id_item)                             AS product_id_sku,
		v.primary_vendor_id,
		v.primary_vendor_name,
		v.secondary_vendor_ids,
		v.secondary_vendor_names
	FROM SILVER_DATA.TCM_SILVER.MASTER_ITEM_VENDOR_TABLE v
),

orders_12m AS (
	SELECT
		o."Item ID_Child SKU"                       AS product_id_sku,
		o."Item Location"                           AS location_id,
		COUNT(*)                                     AS order_line_count_12m,
		COUNT(DISTINCT o."Order ID")                AS order_count_12m,
		COUNT(DISTINCT o."Customer_ID_Sold-To")     AS customer_count_12m,
		SUM(COALESCE(o."Original Order Quantity", 0))   AS order_qty_original_12m,
		SUM(COALESCE(o."Open Order Quantity", 0))       AS order_qty_open_12m,
		SUM(COALESCE(o."Backordered Quantity", 0))      AS order_qty_backordered_12m,
		SUM(COALESCE(o."Total Shipped Quantity", 0))    AS order_qty_shipped_12m,
		MAX(o."Date_Order")                         AS last_order_date_12m,
		MAX(o."Date_Line_Promised")                 AS last_promised_date_12m,
		MAX(o."Item_Product_Category_Code")         AS ord_product_category_code_review
	FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
	WHERE o."Date_Order" >= DATEADD(MONTH, -12, CURRENT_DATE())
	GROUP BY
		o."Item ID_Child SKU",
		o."Item Location"
),

shipment_12m AS (
	SELECT
		s.ID_ITEM                                    AS product_id_sku,
		s.ID_LOC                                     AS location_id,
		COUNT(*)                                     AS shipment_line_count_12m,
		COUNT(DISTINCT s.ID_SHIP)                    AS shipment_count_12m,
		SUM(COALESCE(s.QTY_SHIP, 0))                 AS shipment_qty_12m,
		SUM(COALESCE(s.QTY_CARTON, 0))               AS shipment_carton_qty_12m,
		SUM(COALESCE(s.AMT_FRT, 0))                  AS freight_amount_12m,
		MAX(s.DATE_SHIP)                             AS last_shipment_date_12m
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE s
	WHERE s.DATE_SHIP >= DATEADD(MONTH, -12, CURRENT_DATE())
	GROUP BY
		s.ID_ITEM,
		s.ID_LOC
),

shop_wc_12m AS (
	SELECT
		w.ID_ITEM_PAR                                AS product_id_sku,
		w.SHOP_ORDER_LOCATION                        AS location_id,
		COUNT(DISTINCT w.SH_SO_AND_SUFX)             AS shop_order_count_12m,
		COUNT(*)                                     AS shop_operation_count_12m,
		SUM(COALESCE(w.QTY_ORD, 0))                  AS shop_qty_ordered_12m,
		SUM(COALESCE(w.QTY_CMPL, 0))                 AS shop_qty_completed_12m,
		SUM(COALESCE(w.QTY_REMAINING, 0))            AS shop_qty_remaining_12m,
		MAX(w.SO_DATE_COMPLETED)                     AS last_shoporder_completed_date_12m,
		MAX(w.CODE_CAT_COST)                         AS shop_cost_category_code_review,
		MAX(TRIM(COALESCE(w.DESCR_ITEM_1, '') || ' ' || COALESCE(w.DESCR_ITEM_2, ''))) AS shop_item_description_review
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_WC_TABLE w
	WHERE w.DATE_ADD >= DATEADD(MONTH, -12, CURRENT_DATE())
	GROUP BY
		w.ID_ITEM_PAR,
		w.SHOP_ORDER_LOCATION
),

assembled AS (
	SELECT
		b.product_id_sku,
		b.location_id,

		/* Canonical (coalesced) business attributes */
		COALESCE(
			b.inv_item_description,
			p.prd_item_description,
			sw.shop_item_description_review
		)                                            AS canonical_item_description,

		COALESCE(
			p.prd_product_category_code,
			o.ord_product_category_code_review
		)                                            AS canonical_product_category_code,

		COALESCE(
			p.prd_cost_category_code,
			sw.shop_cost_category_code_review
		)                                            AS canonical_cost_category_code,

		COALESCE(
			b.inv_item_planner,
			p.prd_item_planner
		)                                            AS canonical_item_planner,

		b.inv_item_source_flag,
		b.inv_primary_source,
		b.inv_nsa_manufactured,

		p.parent_product_id,
		p.parent_item_description,
		p.vertical,
		p.category,
		p.child_item_status,
		p.adjusted_parent_item_status,

		v.primary_vendor_id,
		v.primary_vendor_name,
		v.secondary_vendor_ids,
		v.secondary_vendor_names,

		b.qty_on_hand,
		b.qty_allocated,
		b.qty_on_order,
		b.qty_cut,
		b.qty_released,
		b.primary_bin,
		b.item_stock_flag,
		b.item_bin_tracking,
		b.item_control_flag,
		b.item_fulfillment_type,
		b.item_order_policy_flag,
		b.item_primary_location_type,
		b.item_routing_number,
		b.item_shop_floor_auto_issue_flag,
		b.item_home_location_code,
		b.reorder_point,
		b.reorder_point_minimum,
		b.reorder_point_multiple,
		b.order_quantity_economic,
		b.reorder_point_lead_time,
		b.source_location_match_flag,

		o.order_line_count_12m,
		o.order_count_12m,
		o.customer_count_12m,
		o.order_qty_original_12m,
		o.order_qty_open_12m,
		o.order_qty_backordered_12m,
		o.order_qty_shipped_12m,
		o.last_order_date_12m,
		o.last_promised_date_12m,

		s.shipment_line_count_12m,
		s.shipment_count_12m,
		s.shipment_qty_12m,
		s.shipment_carton_qty_12m,
		s.freight_amount_12m,
		s.last_shipment_date_12m,

		sw.shop_order_count_12m,
		sw.shop_operation_count_12m,
		sw.shop_qty_ordered_12m,
		sw.shop_qty_completed_12m,
		sw.shop_qty_remaining_12m,
		sw.last_shoporder_completed_date_12m,

		/* Overlap review fields */
		b.inv_item_description                       AS item_description_inventory_review,
		p.prd_item_description                       AS item_description_product_review,
		sw.shop_item_description_review              AS item_description_shoporder_review,
		p.prd_product_category_code                  AS product_category_product_review,
		o.ord_product_category_code_review           AS product_category_orders_review,
		p.prd_cost_category_code                     AS cost_category_product_review,
		sw.shop_cost_category_code_review            AS cost_category_shoporder_review,
		b.inv_item_planner                           AS planner_inventory_review,
		p.prd_item_planner                           AS planner_product_review,

		/* Source-presence flags */
		IFF(o.product_id_sku IS NOT NULL, 'Y', 'N') AS has_orders_12m,
		IFF(s.product_id_sku IS NOT NULL, 'Y', 'N') AS has_shipments_12m,
		IFF(sw.product_id_sku IS NOT NULL, 'Y', 'N') AS has_shop_wc_12m,

		/* Coalesce lineage markers */
		CASE
			WHEN b.inv_item_description IS NOT NULL THEN 'INVENTORY'
			WHEN p.prd_item_description IS NOT NULL THEN 'PRODUCT'
			WHEN sw.shop_item_description_review IS NOT NULL THEN 'SHOPORDER'
			ELSE 'UNAVAILABLE'
		END                                          AS item_description_source,

		CASE
			WHEN p.prd_product_category_code IS NOT NULL THEN 'PRODUCT'
			WHEN o.ord_product_category_code_review IS NOT NULL THEN 'ORDERS'
			ELSE 'UNAVAILABLE'
		END                                          AS product_category_source,

		CASE
			WHEN p.prd_cost_category_code IS NOT NULL THEN 'PRODUCT'
			WHEN sw.shop_cost_category_code_review IS NOT NULL THEN 'SHOPORDER'
			ELSE 'UNAVAILABLE'
		END                                          AS cost_category_source,

		CASE
			WHEN b.inv_item_planner IS NOT NULL THEN 'INVENTORY'
			WHEN p.prd_item_planner IS NOT NULL THEN 'PRODUCT'
			ELSE 'UNAVAILABLE'
		END                                          AS planner_source

	FROM base_inventory b
	LEFT JOIN product_dim p
		ON b.product_id_sku = p.product_id_sku
	LEFT JOIN vendor_dim v
		ON b.product_id_sku = v.product_id_sku
	LEFT JOIN orders_12m o
		ON b.product_id_sku = o.product_id_sku
	   AND b.location_id = o.location_id
	LEFT JOIN shipment_12m s
		ON b.product_id_sku = s.product_id_sku
	   AND b.location_id = s.location_id
	LEFT JOIN shop_wc_12m sw
		ON b.product_id_sku = sw.product_id_sku
	   AND b.location_id = sw.location_id
)

SELECT
	/* Technical keys */
	a.product_id_sku,
	a.location_id,

	/* Canonical business columns */
	a.canonical_item_description                    AS "Item Description",
	a.canonical_product_category_code               AS "Product Category Code",
	a.canonical_cost_category_code                  AS "Cost Category Code",
	a.canonical_item_planner                        AS "Item Planner",
	a.inv_item_source_flag                          AS "Item Source Flag",
	a.inv_primary_source                            AS "Primary Source",
	a.inv_nsa_manufactured                          AS "NSA Manufactured",
	a.parent_product_id                             AS "Parent Product ID",
	a.parent_item_description                       AS "Parent Item Description",
	a.vertical                                      AS "Vertical",
	a.category                                      AS "Category",
	a.child_item_status                             AS "Child Item Status",
	a.adjusted_parent_item_status                   AS "Adjusted Parent Item Status",
	a.primary_vendor_id                             AS "Primary Vendor ID",
	a.primary_vendor_name                           AS "Primary Vendor Name",
	a.secondary_vendor_ids                          AS "Secondary Vendor IDs",
	a.secondary_vendor_names                        AS "Secondary Vendor Names",

	/* Inventory and planning */
	a.qty_on_hand                                   AS "Qty On Hand",
	a.qty_allocated                                 AS "Qty Allocated",
	a.qty_on_order                                  AS "Qty On Order",
	a.qty_cut                                       AS "Qty Cut",
	a.qty_released                                  AS "Qty Released",
	a.primary_bin                                   AS "Primary Bin",
	a.item_stock_flag                               AS "Item Stock Flag",
	a.item_bin_tracking                             AS "Item Bin Tracking",
	a.item_control_flag                             AS "Item Control Flag",
	a.item_fulfillment_type                         AS "Item Fulfillment Type",
	a.item_order_policy_flag                        AS "Item Order Policy Flag",
	a.item_primary_location_type                    AS "Item Primary Location Type",
	a.item_routing_number                           AS "Item Routing Number",
	a.item_shop_floor_auto_issue_flag               AS "Shop Floor Auto Issue Flag",
	a.item_home_location_code                       AS "Home Location Code",
	a.reorder_point                                 AS "Reorder Point",
	a.reorder_point_minimum                         AS "Reorder Point Minimum",
	a.reorder_point_multiple                        AS "Reorder Point Multiple",
	a.order_quantity_economic                       AS "Economic Order Quantity",
	a.reorder_point_lead_time                       AS "Reorder Point Lead Time",
	a.source_location_match_flag                    AS "Source Location Match Flag",

	/* 12-month order metrics */
	a.order_line_count_12m                          AS "Order Lines 12M",
	a.order_count_12m                               AS "Orders 12M",
	a.customer_count_12m                            AS "Customers 12M",
	a.order_qty_original_12m                        AS "Order Qty Original 12M",
	a.order_qty_open_12m                            AS "Order Qty Open 12M",
	a.order_qty_backordered_12m                     AS "Order Qty Backordered 12M",
	a.order_qty_shipped_12m                         AS "Order Qty Shipped 12M",
	a.last_order_date_12m                           AS "Last Order Date 12M",
	a.last_promised_date_12m                        AS "Last Promised Date 12M",

	/* 12-month shipment metrics */
	a.shipment_line_count_12m                       AS "Shipment Lines 12M",
	a.shipment_count_12m                            AS "Shipments 12M",
	a.shipment_qty_12m                              AS "Shipment Qty 12M",
	a.shipment_carton_qty_12m                       AS "Shipment Carton Qty 12M",
	a.freight_amount_12m                            AS "Freight Amount 12M",
	a.last_shipment_date_12m                        AS "Last Shipment Date 12M",

	/* 12-month shop-order metrics */
	a.shop_order_count_12m                          AS "Shop Orders 12M",
	a.shop_operation_count_12m                      AS "Shop Operations 12M",
	a.shop_qty_ordered_12m                          AS "Shop Qty Ordered 12M",
	a.shop_qty_completed_12m                        AS "Shop Qty Completed 12M",
	a.shop_qty_remaining_12m                        AS "Shop Qty Remaining 12M",
	a.last_shoporder_completed_date_12m             AS "Last Shop Order Completed Date 12M",

	/* Review columns for overlapping attributes */
	a.item_description_inventory_review             AS "Item Description Inventory_REVIEW",
	a.item_description_product_review               AS "Item Description Product_REVIEW",
	a.item_description_shoporder_review             AS "Item Description ShopOrder_REVIEW",
	a.product_category_product_review               AS "Product Category Product_REVIEW",
	a.product_category_orders_review                AS "Product Category Orders_REVIEW",
	a.cost_category_product_review                  AS "Cost Category Product_REVIEW",
	a.cost_category_shoporder_review                AS "Cost Category ShopOrder_REVIEW",
	a.planner_inventory_review                      AS "Item Planner Inventory_REVIEW",
	a.planner_product_review                        AS "Item Planner Product_REVIEW",

	/* Lineage and coverage flags */
	a.has_orders_12m                                AS "Has Orders 12M",
	a.has_shipments_12m                             AS "Has Shipments 12M",
	a.has_shop_wc_12m                               AS "Has Shop WC 12M",
	a.item_description_source                        AS "Item Description Source",
	a.product_category_source                        AS "Product Category Source",
	a.cost_category_source                           AS "Cost Category Source",
	a.planner_source                                 AS "Item Planner Source"
FROM assembled a;
