CREATE OR REPLACE DYNAMIC TABLE GOLD_DATA.TCM_GOLD.OPENORDERS_DT
	TARGET_LAG   = 'DOWNSTREAM'
	REFRESH_MODE = AUTO
	INITIALIZE   = ON_CREATE
	WAREHOUSE    = ELT_DEFAULT
AS
/* ============================================================
   OPENORDERS_DT — Open Orders Gold Layer (v1)

   Base:
   - SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
   - Active source only: "Line_Source_Table" = 'ACTIVE'

   Required left joins:
   - MASTER_PRODUCT_TABLE (all fields)
   - ITEM_INVENTORY_MASTER (all fields)
   - CUSTOMER_MASTER_SILVER (all fields; Sold-To+Ship-To preferred, Sold-To fallback)
   - MASTER_SHIPMENT_TABLE aggregated by order id:
	   count(distinct shipment id), count(distinct invoice id)
   - MASTER_SHOPORDER_TABLE:
	   one selected most-recent shop order (ID + suffix) with S-O designation columns
   ============================================================ */
WITH ORDERS_BASE AS (
	SELECT
		o.*
	FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE o
	WHERE o."Line_Source_Table" = 'ACTIVE'
),

PRODUCT_DIM AS (
	SELECT
		p.*
	FROM SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE p
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY p."Item_ID_Child_SKU"
		ORDER BY
			CASE WHEN COALESCE(p."Item_Status_Child_Active_Status", '') = 'A' THEN 0 ELSE 1 END,
			CASE WHEN COALESCE(p."Item_Primary_Location", '') = '10' THEN 0 ELSE 1 END,
			COALESCE(p."Item_Primary_Location", ''),
			COALESCE(p."Item_ID_Parent_SKU", '')
	) = 1
),

INVENTORY_DIM AS (
	SELECT
		i.*
	FROM SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER i
),

CUSTOMER_MATCH AS (
	SELECT
		o."Order ID"                  AS CM_ORDER_ID,
		o."Order_Sequence Line Number" AS CM_ORDER_LINE,
		c.*,
		ROW_NUMBER() OVER (
			PARTITION BY o."Order ID", o."Order_Sequence Line Number"
			ORDER BY
				CASE
					WHEN TRIM(COALESCE(o."Customer_End_User_Ship_To_Sequence_#", ''))
					   = TRIM(COALESCE(c."Customer_ID_Ship-To", '')) THEN 0
					ELSE 1
				END,
				CASE WHEN c."Customer_ID_Ship-To" IS NULL THEN 1 ELSE 0 END,
				COALESCE(c."Customer_ID_Ship-To", '')
		) AS RN
	FROM ORDERS_BASE o
	LEFT JOIN SILVER_DATA.TCM_SILVER.CUSTOMER_MASTER_SILVER c
		ON TRIM(COALESCE(o."Customer_ID_Sold-To", '')) = TRIM(COALESCE(c."Customer_ID_Sold-To", ''))
),

SHIPMENT_BY_ORDER AS (
	SELECT
		s."Order_ID"                              AS "Order ID",
		COUNT(DISTINCT s."Shipment_ID")          AS "Shipment_Count_Distinct",
		COUNT(DISTINCT s."Invoice_ID")           AS "Invoice_Count_Distinct"
	FROM SILVER_DATA.TCM_SILVER.MASTER_SHIPMENT_TABLE s
	GROUP BY s."Order_ID"
),

SHOPORDER_PICK AS (
	SELECT
		o."Order ID",
		o."Order_Sequence Line Number",
		so.ID_SO,
		so.SUFX_SO,
		ROW_NUMBER() OVER (
			PARTITION BY o."Order ID", o."Order_Sequence Line Number"
			ORDER BY
				COALESCE(so.SO_DATE_LAST_CHANGED, TO_DATE('1900-01-01')) DESC,
				COALESCE(so.SO_DATE_CREATED, TO_DATE('1900-01-01')) DESC,
				COALESCE(so.SO_DATE_START_ORDERED, TO_DATE('1900-01-01')) DESC,
				COALESCE(so.SUFX_SO, 0) DESC,
				COALESCE(so.ID_SO, '') DESC
		) AS RN
	FROM ORDERS_BASE o
	LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_SHOPORDER_TABLE so
		ON TRIM(COALESCE(o."Shop_Order_Location_ID", '')) = TRIM(COALESCE(so.ID_LOC, ''))
	   AND TRIM(COALESCE(o."Shop_Order_ID", '')) = TRIM(COALESCE(so.ID_SO, ''))
)

SELECT
	o.*,

	-- Product: all fields except duplicate item key
	p.* EXCLUDE ("Item_ID_Child_SKU"),

	-- Inventory: all fields except duplicate item/location/tracking keys
	i.* EXCLUDE ("Item_ID_Child_SKU", "Inventory_Location_ID", "Item_Bin_Tracking"),

	-- Customer: all fields except CTE metadata columns and duplicate Sold-To key
	c.* EXCLUDE (CM_ORDER_ID, CM_ORDER_LINE, RN, "Customer_ID_Sold-To"),

	-- Shipment aggregate by order
	sb."Shipment_Count_Distinct",
	sb."Invoice_Count_Distinct",

	-- Shop order (single selected row) with S-O designation fields
	sp.ID_SO                                     AS "S-O Shop_Order_ID",
	sp.SUFX_SO                                   AS "S-O Shop_Order_ID_Suffix"

FROM ORDERS_BASE o
LEFT JOIN PRODUCT_DIM p
	ON TRIM(COALESCE(o."Item ID_Child SKU", '')) = TRIM(COALESCE(p."Item_ID_Child_SKU", ''))
LEFT JOIN INVENTORY_DIM i
	ON TRIM(COALESCE(o."Item ID_Child SKU", '')) = TRIM(COALESCE(i."Item_ID_Child_SKU", ''))
   AND TRIM(COALESCE(o."Item Location", '')) = TRIM(COALESCE(i."Inventory_Location_ID", ''))
LEFT JOIN CUSTOMER_MATCH c
	ON o."Order ID" = c.CM_ORDER_ID
	   AND o."Order_Sequence Line Number" = c.CM_ORDER_LINE
   AND c.RN = 1
LEFT JOIN SHIPMENT_BY_ORDER sb
	ON o."Order ID" = sb."Order ID"
LEFT JOIN SHOPORDER_PICK sp
	ON o."Order ID" = sp."Order ID"
   AND o."Order_Sequence Line Number" = sp."Order_Sequence Line Number"
   AND sp.RN = 1
;
