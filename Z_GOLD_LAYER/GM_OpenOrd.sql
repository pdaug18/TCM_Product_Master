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
		PARTITION BY p."Item ID_Child SKU"
		ORDER BY
			CASE WHEN COALESCE(p."Item Status_Child Active Status", '') = 'A' THEN 0 ELSE 1 END,
			CASE WHEN COALESCE(p."Item_Location ID", '') = '10' THEN 0 ELSE 1 END,
			COALESCE(p."Item_Location ID", ''),
			COALESCE(p."Item ID_Parent SKU", '')
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
					   = TRIM(COALESCE(c."Customer_Shipto_Num", '')) THEN 0
					ELSE 1
				END,
				CASE WHEN c."Customer_Shipto_Num" IS NULL THEN 1 ELSE 0 END,
				COALESCE(c."Customer_Shipto_Num", '')
		) AS RN
	FROM ORDERS_BASE o
	LEFT JOIN SILVER_DATA.TCM_SILVER.CUSTOMER_MASTER_SILVER c
		ON TRIM(COALESCE(o."Customer_ID_Sold-To", '')) = TRIM(COALESCE(c."Customer_Id", ''))
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
	p.* EXCLUDE ("Item ID_Child SKU"),

	-- Inventory: all fields except duplicate item/location keys
	i.* EXCLUDE ("Product_ID_SKU", "Location_ID"),

	-- Customer: all fields except duplicate customer name
	c.* EXCLUDE (CM_ORDER_ID, CM_ORDER_LINE, RN, "Customer_Name"),

	-- Shipment aggregate by order
	sb."Shipment_Count_Distinct",
	sb."Invoice_Count_Distinct",

	-- Shop order (single selected row) with S-O designation fields
	sp.ID_SO                                     AS "S-O Shop_Order_ID",
	sp.SUFX_SO                                   AS "S-O Shop_Order_ID_Suffix"

FROM ORDERS_BASE o
LEFT JOIN PRODUCT_DIM p
	ON TRIM(COALESCE(o."Item ID_Child SKU", '')) = TRIM(COALESCE(p."Item ID_Child SKU", ''))
LEFT JOIN INVENTORY_DIM i
	ON TRIM(COALESCE(o."Item ID_Child SKU", '')) = TRIM(COALESCE(i."Product_ID_SKU", ''))
   AND TRIM(COALESCE(o."Item Location", '')) = TRIM(COALESCE(i."Location_ID", ''))
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
