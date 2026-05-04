CREATE OR REPLACE DYNAMIC TABLE GOLD_DATA.TCM_GOLD.OPEN_ORDERS_GOLD_DT
    TARGET_LAG   = 'DOWNSTREAM'
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    WAREHOUSE    = ELT_DEFAULT
AS
/*
Open Orders Gold - Order Grain
- One row per Order_ID
- Driven by active/open orders from MASTER_ORDERS_TABLE_SILVER
- Joins to Product, Inventory, Shipment, Invoice, Shop Order, and Customer silver tables
*/
WITH orders_base AS (
    SELECT
        /* Trim all ID-like columns at base level for stable joins */
        TRIM(o.Order_ID) AS Order_ID,
        TRIM(o.Item_ID_Child_SKU) AS Item_ID_Child_SKU,
        TRIM(o.Shop_Order_ID) AS Shop_Order_ID,
        TRIM(o.Shop_Order_Location_ID) AS Shop_Order_Location_ID,
        TRIM(o.Customer_ID_Sold_To) AS Customer_ID_Sold_To,
        TRIM(o.Customer_ID_Bill_To) AS Customer_ID_Bill_To,

        o."Order_Line_Sequence_#" AS Order_Line_Sequence,
        o.Order_Type,
        o.Order_Status_Code,
        o.Date_Ordered,
        o.Date_Order_Requested,
        o.Date_Order_Promised,
        o.Date_Order_Last_Acknowledged,
        o.Open_Order_Quantity,
        o.Order_Total_Amount,
        o.Net_Price_At_Order,
        o.Line_Source_Table
    FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE_SILVER o
    WHERE COALESCE(o.Line_Source_Table, 'ACTIVE') = 'ACTIVE'
      AND COALESCE(o.Open_Order_Quantity, 0) > 0
      AND COALESCE(o.Order_Status_Code, 'O') IN ('O', 'A', 'B', 'R', 'S', 'P')
),
orders_line_agg AS (
    SELECT
        Order_ID,
        SUM(COALESCE(Open_Order_Quantity, 0)) AS Open_Order_Quantity,
        SUM(COALESCE(Order_Total_Amount, 0)) AS Order_Total_Amount,
        AVG(COALESCE(Net_Price_At_Order, 0)) AS Net_Price_At_Order
    FROM orders_base
    GROUP BY Order_ID
),
orders_dedup AS (
    SELECT
        b.*
    FROM orders_base b
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.Order_ID
        ORDER BY b.Date_Order_Requested ASC NULLS LAST, b.Order_Line_Sequence ASC
    ) = 1
),
product_rep AS (
    SELECT
        d.Order_ID,
        d.Item_ID_Child_SKU,
        p.Item_ID_Parent_SKU,
        p.Item_Description_Child_SKU,
        p.Item_Description_Parent_SKU,
        p.Item_Vertical,
        p.Item_Product_Line,
        p.Item_Product_Type,
        p.Item_Brand
    FROM orders_dedup d
    LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE_SILVER p
        ON TRIM(COALESCE(d.Item_ID_Child_SKU, '')) = TRIM(COALESCE(p.Item_ID_Child_SKU, ''))
),
order_items AS (
    SELECT DISTINCT
        ob.Order_ID,
        ob.Item_ID_Child_SKU
    FROM orders_base ob
),
inventory_agg AS (
    SELECT
        oi.Order_ID,

        SUM(COALESCE(i.Inventory_Allocated_Quantity, 0)) AS Inventory_Allocated_Quantity_Total,
        SUM(COALESCE(i.Inventory_Inspected_Quantity, 0)) AS Inventory_Inspected_Quantity_Total,
        SUM(COALESCE(i.Inventory_On_Hand_Quantity, 0)) AS Inventory_On_Hand_Quantity_Total,
        SUM(COALESCE(i.Inventory_On_Order_Quantity, 0)) AS Inventory_On_Order_Quantity_Total,
        SUM(COALESCE(i.Inventory_Quantity_In_Transit, 0)) AS Inventory_In_Transit_Quantity,
        SUM(COALESCE(i.Inventory_Released_Quantity, 0)) AS Released_Quantity_Total,
        SUM(COALESCE(i.Inventory_Start_Quantity, 0)) AS Started_Quantity_Total,
        SUM(COALESCE(i.Inventory_Pending_Release_Quantity, 0)) AS Pending_Release_Quantity_Total,
        SUM(COALESCE(i.Inventory_Pending_Start_Quantity, 0)) AS Pending_Start_Quantity_Total,

        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'P' THEN COALESCE(i.Inventory_Allocated_Quantity, 0) ELSE 0 END) AS Inventory_Allocated_Quantity_at_Primary_Location,
        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'S' THEN COALESCE(i.Inventory_Allocated_Quantity, 0) ELSE 0 END) AS Inventory_Allocated_Quantity_at_Secondary_Locations,

        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'P' THEN COALESCE(i.Inventory_Inspected_Quantity, 0) ELSE 0 END) AS Inventory_Inspected_Quantity_at_Primary_Location,
        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'S' THEN COALESCE(i.Inventory_Inspected_Quantity, 0) ELSE 0 END) AS Inventory_Inspected_Quantity_at_Secondary_Locations,

        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'P' THEN COALESCE(i.Inventory_On_Hand_Quantity, 0) ELSE 0 END) AS Inventory_On_Hand_Quantity_at_Primary_Location,
        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'S' THEN COALESCE(i.Inventory_On_Hand_Quantity, 0) ELSE 0 END) AS Inventory_On_Hand_Quantity_at_Secondary_Locations,

        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'P' THEN COALESCE(i.Inventory_On_Order_Quantity, 0) ELSE 0 END) AS Inventory_On_Order_Quantity_at_Primary_Location,
        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'S' THEN COALESCE(i.Inventory_On_Order_Quantity, 0) ELSE 0 END) AS Inventory_On_Order_Quantity_at_Secondary_Locations,

        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'P' THEN COALESCE(i.Inventory_Released_Quantity, 0) ELSE 0 END) AS Released_Quantity_at_Primary_Location,
        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'S' THEN COALESCE(i.Inventory_Released_Quantity, 0) ELSE 0 END) AS Released_Quantity_at_Secondary_Locations,

        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'P' THEN COALESCE(i.Inventory_Start_Quantity, 0) ELSE 0 END) AS Started_Quantity_at_Primary_Location,
        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'S' THEN COALESCE(i.Inventory_Start_Quantity, 0) ELSE 0 END) AS Started_Quantity_at_Secondary_Locations,

        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'P' THEN COALESCE(i.Inventory_Pending_Release_Quantity, 0) ELSE 0 END) AS Pending_Release_Quantity_at_Primary_Location,
        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'S' THEN COALESCE(i.Inventory_Pending_Release_Quantity, 0) ELSE 0 END) AS Pending_Release_Quantity_at_Secondary_Locations,

        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'P' THEN COALESCE(i.Inventory_Pending_Start_Quantity, 0) ELSE 0 END) AS Pending_Start_Quantity_at_Primary_Location,
        SUM(CASE WHEN i.Inventory_Primary_Location_Flag = 'S' THEN COALESCE(i.Inventory_Pending_Start_Quantity, 0) ELSE 0 END) AS Pending_Start_Quantity_at_Secondary_Locations
    FROM order_items oi
    LEFT JOIN SILVER_DATA.TCM_SILVER.MASTER_INVENTORY_SILVER i
        ON TRIM(COALESCE(oi.Item_ID_Child_SKU, '')) = TRIM(COALESCE(i.Item_ID_Child_SKU, ''))
    GROUP BY oi.Order_ID
),
shipment_agg AS (
    SELECT
        TRIM(sh.Order_ID) AS Order_ID,
        COUNT(DISTINCT sh.Shipment_ID) AS Shipment_Count,
        SUM(COALESCE(sh.Shipped_Quantity, 0)) AS Shipped_Quantity,
        MAX(sh.Date_Shipped) AS Date_Shipped_Last
    FROM SILVER_DATA.TCM_SILVER.SHIPMENT_MASTER_SILVER sh
    GROUP BY TRIM(sh.Order_ID)
),
invoice_agg AS (
    SELECT
        TRIM(iv.Order_ID) AS Order_ID,
        COUNT(DISTINCT iv.Invoice_ID) AS Invoice_Count,
        SUM(COALESCE(iv.Quantity_Shipped, 0)) AS Invoice_Unit_Quantity,
        SUM(COALESCE(iv.Total_Price, 0)) AS Invoice_Value
    FROM SILVER_DATA.TCM_SILVER.INVOICE_MASTER_SILVER iv
    GROUP BY TRIM(iv.Order_ID)
),
shop_order_keys AS (
    SELECT DISTINCT
        ob.Order_ID,
        ob.Shop_Order_ID,
        ob.Shop_Order_Location_ID
    FROM orders_base ob
    WHERE COALESCE(ob.Shop_Order_ID, '') <> ''
),
shop_order_pick AS (
    SELECT
        sok.Order_ID,
        so.Shop_Order_ID,
        so.Shop_Order_SUFX_ID,
        so.Shop_Order_Process_Status,
        so.Shop_Order_Location_ID
    FROM shop_order_keys sok
    LEFT JOIN SILVER_DATA.TCM_SILVER.SHOP_ORDER_MASTER_SILVER so
        ON TRIM(COALESCE(sok.Shop_Order_ID, '')) = TRIM(COALESCE(so.Shop_Order_ID, ''))
       AND TRIM(COALESCE(sok.Shop_Order_Location_ID, '')) = TRIM(COALESCE(so.Shop_Order_Location_ID, ''))
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sok.Order_ID
        ORDER BY COALESCE(so.Shop_Order_SUFX_ID, 0) DESC
    ) = 1
),
customer_dim AS (
    SELECT
        d.Order_ID,
        d.Customer_ID_Sold_To,
        c.Customer_Name_Sold_To,
        c.Customer_Credit_Status
    FROM orders_dedup d
    LEFT JOIN SILVER_DATA.TCM_SILVER.CUSTOMER_MASTER_SILVER c
        ON TRIM(COALESCE(d.Customer_ID_Sold_To, '')) = TRIM(COALESCE(c.Customer_ID_Sold_To, ''))
)
SELECT
    d.Order_ID AS Order_ID,
    d.Order_Type AS Order_Type,
    d.Order_Status_Code AS Order_Status_Code,
    d.Date_Ordered AS Date_Ordered,
    d.Date_Order_Requested AS Date_Order_Requested,
    d.Date_Order_Promised AS Date_Order_Promised,
    d.Date_Order_Last_Acknowledged AS Date_Order_Last_Acknowledged,

    ola.Open_Order_Quantity AS Open_Order_Quantity,
    ola.Order_Total_Amount AS Order_Total_Amount,
    ola.Net_Price_At_Order AS Net_Price_At_Order,

    cd.Customer_ID_Sold_To AS Customer_ID_Sold_To,
    cd.Customer_Name_Sold_To AS Customer_Name_Sold_To,
    cd.Customer_Credit_Status AS Customer_Credit_Status,

    pr.Item_ID_Child_SKU AS Item_ID_Child_SKU,
    pr.Item_ID_Parent_SKU AS Item_ID_Parent_SKU,
    pr.Item_Description_Child_SKU AS Item_Description_Child_SKU,
    pr.Item_Description_Parent_SKU AS Item_Description_Parent_SKU,
    pr.Item_Vertical AS Item_Vertical,
    pr.Item_Product_Line AS Item_Product_Line,
    pr.Item_Product_Type AS Item_Product_Type,
    pr.Item_Brand AS Item_Brand,

    ia.Inventory_Allocated_Quantity_Total AS Inventory_Allocated_Quantity_Total,
    ia.Inventory_Inspected_Quantity_Total AS Inventory_Inspected_Quantity_Total,
    ia.Inventory_On_Hand_Quantity_Total AS Inventory_On_Hand_Quantity_Total,
    ia.Inventory_On_Order_Quantity_Total AS Inventory_On_Order_Quantity_Total,
    ia.Inventory_In_Transit_Quantity AS Inventory_In_Transit_Quantity,
    ia.Released_Quantity_Total AS Released_Quantity_Total,
    ia.Started_Quantity_Total AS Started_Quantity_Total,
    ia.Pending_Release_Quantity_Total AS Pending_Release_Quantity_Total,
    ia.Pending_Start_Quantity_Total AS Pending_Start_Quantity_Total,

    ia.Inventory_Allocated_Quantity_at_Primary_Location AS Inventory_Allocated_Quantity_at_Primary_Location,
    ia.Inventory_Allocated_Quantity_at_Secondary_Locations AS Inventory_Allocated_Quantity_at_Secondary_Locations,
    ia.Inventory_Inspected_Quantity_at_Primary_Location AS Inventory_Inspected_Quantity_at_Primary_Location,
    ia.Inventory_Inspected_Quantity_at_Secondary_Locations AS Inventory_Inspected_Quantity_at_Secondary_Locations,
    ia.Inventory_On_Hand_Quantity_at_Primary_Location AS Inventory_On_Hand_Quantity_at_Primary_Location,
    ia.Inventory_On_Hand_Quantity_at_Secondary_Locations AS Inventory_On_Hand_Quantity_at_Secondary_Locations,
    ia.Inventory_On_Order_Quantity_at_Primary_Location AS Inventory_On_Order_Quantity_at_Primary_Location,
    ia.Inventory_On_Order_Quantity_at_Secondary_Locations AS Inventory_On_Order_Quantity_at_Secondary_Locations,
    ia.Released_Quantity_at_Primary_Location AS Released_Quantity_at_Primary_Location,
    ia.Released_Quantity_at_Secondary_Locations AS Released_Quantity_at_Secondary_Locations,
    ia.Started_Quantity_at_Primary_Location AS Started_Quantity_at_Primary_Location,
    ia.Started_Quantity_at_Secondary_Locations AS Started_Quantity_at_Secondary_Locations,
    ia.Pending_Release_Quantity_at_Primary_Location AS Pending_Release_Quantity_at_Primary_Location,
    ia.Pending_Release_Quantity_at_Secondary_Locations AS Pending_Release_Quantity_at_Secondary_Locations,
    ia.Pending_Start_Quantity_at_Primary_Location AS Pending_Start_Quantity_at_Primary_Location,
    ia.Pending_Start_Quantity_at_Secondary_Locations AS Pending_Start_Quantity_at_Secondary_Locations,

    sa.Shipment_Count AS Shipment_Count,
    sa.Shipped_Quantity AS Shipped_Quantity,
    sa.Date_Shipped_Last AS Date_Shipped_Last,

    iva.Invoice_Count AS Invoice_Count,
    iva.Invoice_Unit_Quantity AS Invoice_Unit_Quantity,
    iva.Invoice_Value AS Invoice_Value,

    sop.Shop_Order_ID AS Shop_Order_ID,
    sop.Shop_Order_SUFX_ID AS Shop_Order_SUFX_ID,
    sop.Shop_Order_Process_Status AS Shop_Order_Process_Status,
    sop.Shop_Order_Location_ID AS Shop_Order_Location_ID
FROM orders_dedup d
LEFT JOIN orders_line_agg ola
    ON d.Order_ID = ola.Order_ID
LEFT JOIN product_rep pr
    ON d.Order_ID = pr.Order_ID
LEFT JOIN inventory_agg ia
    ON d.Order_ID = ia.Order_ID
LEFT JOIN shipment_agg sa
    ON d.Order_ID = sa.Order_ID
LEFT JOIN invoice_agg iva
    ON d.Order_ID = iva.Order_ID
LEFT JOIN shop_order_pick sop
    ON d.Order_ID = sop.Order_ID
LEFT JOIN customer_dim cd
    ON d.Order_ID = cd.Order_ID
;

CREATE OR REPLACE VIEW GOLD_DATA.TCM_GOLD.VW_OPEN_ORDERS_GOLD AS
SELECT *
FROM GOLD_DATA.TCM_GOLD.OPEN_ORDERS_GOLD_DT;
