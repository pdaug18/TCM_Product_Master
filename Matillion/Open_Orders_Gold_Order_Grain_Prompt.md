# Matillion Prompt - Open Orders Gold (Order Grain)

Use this prompt in Matillion AI / Copilot to generate the transformation SQL.

---

Create a Snowflake Gold-layer transformation for Open Orders at ORDER grain.

## Objective
Build a Gold table/view where each row represents one active/open order.
The driving grain is:
- Order_ID

Only include orders considered open/active.

## Source Tables (Silver)
Use the following Silver tables and aliases:
- SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE_SILVER as o
- SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE_SILVER as p
- SILVER_DATA.TCM_SILVER.MASTER_INVENTORY_SILVER as i
- SILVER_DATA.TCM_SILVER.INVOICE_MASTER_SILVER as iv
- SILVER_DATA.TCM_SILVER.SHIPMENT_MASTER_SILVER as sh
- SILVER_DATA.TCM_SILVER.SHOP_ORDER_MASTER_SILVER as so
- SILVER_DATA.TCM_SILVER.CUSTOMER_MASTER_SILVER as c

## Required Join Logic
1. Base CTE = orders (o)
- Keep only active/open orders using available status/source columns in o.
- Deduplicate to one row per Order_ID at the end.

2. Product join
- Join p by Item_ID_Child_SKU.
- If order has multiple lines with different items, aggregate item-level attributes to order level using deterministic rules:
  - representative item = item from earliest requested line, then lowest line sequence.

3. Inventory join
- Join i by Item_ID_Child_SKU.
- Derive order-level inventory rollups by summing across all items on the order:
  - Inventory_Allocated_Quantity_Total
  - Inventory_Inspected_Quantity_Total
  - Inventory_On_Hand_Quantity_Total
  - Inventory_On_Order_Quantity_Total
  - Inventory_In_Transit_Quantity
  - Released_Quantity_Total
  - Started_Quantity_Total
  - Pending_Release_Quantity_Total
  - Pending_Start_Quantity_Total
- Also derive primary vs secondary location splits using Inventory_Primary_Location_Flag:
  - ..._at_Primary_Location = SUM(CASE WHEN flag='P' THEN qty ELSE 0 END)
  - ..._at_Secondary_Locations = SUM(CASE WHEN flag='S' THEN qty ELSE 0 END)

4. Invoice join
- Join iv by Order_ID.
- Aggregate to order level:
  - Invoice_Count = COUNT(DISTINCT Invoice_ID)
  - Invoice_Unit_Quantity = SUM(Quantity_Shipped)
  - Invoice_Value = SUM(Total_Price)

5. Shipment join
- Join sh by Order_ID.
- Aggregate to order level:
  - Shipment_Count = COUNT(DISTINCT Shipment_ID)
  - Shipped_Quantity = SUM(Shipped_Quantity)
  - Date_Shipped_Last = MAX(Date_Shipped)

6. Shop order join
- Join so by Shop_Order_ID and Shop_Order_Location_ID when available.
- If multiple shop orders per sales order, pick most recent by available date/change timestamp.

7. Customer join
- Join c by Customer_ID_Sold-To.
- Bring Sold-To attributes and credit status.

## Data Quality / Key Handling Rules
- TRIM all ID-like columns in base CTE before downstream joins.
- For all join keys, use TRIM(COALESCE(col, '')) on both sides.
- Preserve null-safe LEFT JOIN behavior.
- Do not duplicate orders in final output.

## Output Columns
Use Title_Case with underscore naming in final SELECT aliases.

### Core Order Fields
- Order_ID
- Order_Type
- Order_Status_Code
- Date_Ordered
- Date_Order_Requested
- Date_Order_Promised
- Date_Order_Last_Acknowledged
- Open_Order_Quantity
- Order_Total_Amount
- Net_Price_At_Order

### Customer Fields
- Customer_ID_Sold_To
- Customer_Name_Sold_To
- Customer_Credit_Status

### Product Representative Fields
- Item_ID_Child_SKU
- Item_ID_Parent_SKU
- Item_Description_Child_SKU
- Item_Description_Parent_SKU
- Item_Vertical
- Item_Product_Line
- Item_Product_Type
- Item_Brand

### Inventory Rollup Fields (Order Level)
- Inventory_Allocated_Quantity_Total
- Inventory_Inspected_Quantity_Total
- Inventory_On_Hand_Quantity_Total
- Inventory_On_Order_Quantity_Total
- Inventory_In_Transit_Quantity
- Released_Quantity_Total
- Started_Quantity_Total
- Pending_Release_Quantity_Total
- Pending_Start_Quantity_Total

### Primary/Secondary Split Fields
- Inventory_Allocated_Quantity_at_Primary_Location
- Inventory_Allocated_Quantity_at_Secondary_Locations
- Inventory_Inspected_Quantity_at_Primary_Location
- Inventory_Inspected_Quantity_at_Secondary_Locations
- Inventory_On_Hand_Quantity_at_Primary_Location
- Inventory_On_Hand_Quantity_at_Secondary_Locations
- Inventory_On_Order_Quantity_at_Primary_Location
- Inventory_On_Order_Quantity_at_Secondary_Locations
- Released_Quantity_at_Primary_Location
- Released_Quantity_at_Secondary_Locations
- Started_Quantity_at_Primary_Location
- Started_Quantity_at_Secondary_Locations
- Pending_Release_Quantity_at_Primary_Location
- Pending_Release_Quantity_at_Secondary_Locations
- Pending_Start_Quantity_at_Primary_Location
- Pending_Start_Quantity_at_Secondary_Locations

### Shipment / Invoice / Shop Order Fields
- Shipment_Count
- Shipped_Quantity
- Date_Shipped_Last
- Invoice_Count
- Invoice_Unit_Quantity
- Invoice_Value
- Shop_Order_ID
- Shop_Order_SUFX_ID
- Shop_Order_Process_Status
- Shop_Order_Location_ID

## Technical Requirements
- Snowflake SQL only.
- Use CTE structure:
  - orders_base
  - orders_dedup
  - product_rep
  - inventory_agg
  - shipment_agg
  - invoice_agg
  - shop_order_pick
  - customer_dim
  - final_select
- Final output must be exactly one row per Order_ID.
- Include a WHERE filter that keeps only active/open orders.
- Add clear comments for each CTE.

## Delivery
Return:
1. Full CREATE OR REPLACE DYNAMIC TABLE statement (TARGET_LAG='DOWNSTREAM', REFRESH_MODE=AUTO, INITIALIZE=ON_CREATE, WAREHOUSE=ELT_DEFAULT)
2. Optional CREATE OR REPLACE VIEW wrapper selecting from the dynamic table.
