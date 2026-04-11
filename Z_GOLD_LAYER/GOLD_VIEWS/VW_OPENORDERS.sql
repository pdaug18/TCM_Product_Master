CREATE OR REPLACE VIEW GOLD_DATA.TCM_GOLD.VW_OPENORDERS AS
/* ============================================================
   VW_OPENORDERS — Clean business view over OPENORDERS_DT
   Source: GOLD_DATA.TCM_GOLD.OPENORDERS_DT
   ============================================================ */
SELECT
    /* ---------------------------
       Order Keys
       --------------------------- */
    o."Order ID",
    o."Order_Sequence Line Number",
    o."Line_Source_Table",

    /* ---------------------------
       Customer
       --------------------------- */
    o."Customer_ID_Sold-To",
    o."Customer_End_User_Ship_To_Sequence_#",
    o."Customer_ID_Bill-To",
    o."Customer_Name_Sold-To",
    o."Customer_Name_Ship-To",
    -- o."Customer_Market", --! verify column exists in MASTER_ORDERS_TABLE or CUSTOMER_MASTER_SILVER
    -- o."Customer_Segment", --! verify column exists in MASTER_ORDERS_TABLE or CUSTOMER_MASTER_SILVER
    o."Customer_Credit_Status",
    o."Customer_Credit_Terms_Code",

    /* ---------------------------
       Item / Product
       --------------------------- */
   o."Item ID_Child SKU" AS "Item_ID_Child_SKU",
    o."Item_ID_Parent_SKU",
    o."Item_Description_Child_SKU",
    o."Item_Description_Parent_SKU",
    o."Item Location",
    o."Item_Vertical",
    -- o."CATEGORY (Calc)", --! CATEGORY (Calc) removed from Master_product_table (no Needed Master Field mapping)
    o."Item_Status_Child_Active_Status",
   o."Item_Status_Parent_Active_Status" AS "Item_Status_Parent_Active_Status_Adjusted",

    /* ---------------------------
       Order Status, Dates, Qty
       --------------------------- */
    o."Order_Type",
    o."Order_Status_Code",
    o."Date_Order",
    o."Date_Order_Created",
    o."Date_Line_Requested",
    o."Date_Line_Promised",
    o."Original Order Quantity",
    o."Open Order Quantity",
    o."Backordered Quantity",
    o."Booked Quantity",
    o."Released Quantity",
    o."Allocated Quantity",
    o."Total Shipped Quantity",

    /* ---------------------------
       Pricing
       --------------------------- */
    o."List_Price_at_Order",
    o."Sell_Price_at_Order",
    o."Net_Sell_Price_at_Order",
    o."Unit_Cost_at_Order",
    o."Open_List_Price_At_Order",
    o."Open_Net_Sell_At_Order",
    o."Open_Cost_At_Order",

    /* ---------------------------
       Inventory
       --------------------------- */
    o."Item_Primary_Source_by_Location",
    o."Item_Sourcing_Type_Flag",
    o."Inventory_Quantity_On_Hand",
    o."Inventory_Quantity_Allocated",
    o."Inventory_Quantity_On_Order",
    o."Inventory_Quantity_Cut",
    o."Inventory_Quantity_Released",

    /* ---------------------------
       Shipment rollups
       --------------------------- */
    o."Shipment_Count_Distinct",
    o."Invoice_Count_Distinct",

    /* ---------------------------
       Shop Order (v1 fields)
       --------------------------- */
    o."S-O Shop_Order_ID",
    o."S-O Shop_Order_ID_Suffix"

FROM GOLD_DATA.TCM_GOLD.OPENORDERS_DT o;
