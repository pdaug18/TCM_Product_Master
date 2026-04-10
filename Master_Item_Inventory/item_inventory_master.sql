/*
This script creates and replaces a dynamic table named item_inventory_master in Snowflake.
The table includes finished goods, their quantity metrics, and a derived primary source.

Logic Breakdown:
1.  FinishedGoods CTE: 
        Filters for items that are finished goods based on their cost category and commission code in the BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" table.
2.  RankedVendors CTE: 
        Ranks all vendors for each item, prioritizing the primary vendor.
3.  VendorSources CTE:
        Pivots the ranked vendor data to provide primary and secondary vendor information in a single row per item.
4.  ManufacturingLocations CTE: 
        Ranks manufacturing locations for each item, prioritizing location '10' (CLE).
5.  ItemSourceData CTE: 
        Joins the location information with finished goods, vendor sources, and manufacturing location data.
6.  Final SELECT: 
        Constructs the final report for the dynamic table.
*/

CREATE OR REPLACE DYNAMIC TABLE SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER
TARGET_LAG = 'DOWNSTREAM' -- This ensures the table is refreshed based on changes in the source data
WAREHOUSE = 'COMPUTE_WH'
AS
WITH
LocationNames AS (
    SELECT '10' as id_loc, 'CLE' as loc_name UNION ALL
    SELECT '20', 'CHI' UNION ALL
    SELECT '50', 'ARK' UNION ALL
    SELECT '90', 'SAT' UNION ALL
    SELECT '130', 'DR'
),
FinishedGoods AS (
    SELECT
        il.id_item,
        il.id_loc,
        CASE 
            WHEN fg.descr_2 IS NOT NULL THEN CONCAT(fg.descr_1, ' || ', fg.descr_2)
            ELSE fg.descr_1
        END AS item_description,
        il.BIN_PRIM,
        il.flag_source,
        il.flag_stk,
        il.flag_track_bin,
        il.flag_cntrl,
        il.flag_fulfill_type,
        il.flag_plcy_ord,
        il.id_planner,
        il.type_loc,
        il.id_rte,
        il.flag_iss_auto_sf,
        il.QTY_ONHD,
        il.QTY_ALLOC,
        il.QTY_ONORD
    FROM
        BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze" il
    INNER JOIN
        BRONZE_DATA.TCM_BRONZE."ITMMAS_BASE_Bronze" fg ON il.id_item = fg.id_item AND fg.CODE_CAT_COST = '05' AND fg.code_comm <> 'PAR' -- ensure that the item is a FG and not a part
    WHERE
        il.flag_source IN ('P', 'M') -- Consider only Manufactured or Purchased items
        AND il.id_loc IN ('10', '20', '50', '90', '130') -- Filter for relevant locations
),

RankedVendors AS (
    SELECT
        iv.id_item,
        iv.id_vnd_payto,
        vp.name_vnd,
        ROW_NUMBER() OVER (PARTITION BY iv.id_item ORDER BY CASE WHEN iv.flag_vnd_prim = 'P' THEN 0 ELSE 1 END, iv.id_vnd_payto) as rn
    FROM
        BRONZE_DATA.TCM_BRONZE."ITMMAS_VND_Bronze" iv
    LEFT JOIN
        BRONZE_DATA.TCM_BRONZE."VENMAS_PAYTO_Bronze" vp ON LTRIM(iv.id_vnd_payto) = LTRIM(vp.id_vnd)
),

VendorSources AS (
    SELECT
        id_item,
        MAX(CASE WHEN rn = 1 THEN name_vnd END) as primary_vendor_name,
        MAX(CASE WHEN rn = 1 THEN id_vnd_payto END) as primary_vendor_id,
        MAX(CASE WHEN rn = 2 THEN name_vnd END) as secondary_vendor_name,
        MAX(CASE WHEN rn = 2 THEN id_vnd_payto END) as secondary_vendor_id
    FROM
        RankedVendors
    GROUP BY
        id_item
),

ManufacturingLocations AS (
    SELECT
        id_item,
        id_loc,
        ROW_NUMBER() OVER (PARTITION BY id_item ORDER BY CASE WHEN id_loc = '10' THEN 0 ELSE 1 END, id_loc) as rn
    FROM
        BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze"
    WHERE
        flag_source = 'M'
),

ShopOrderData AS (
    -- Qty_Rel is the quantity in the queue for manufacturing.
    -- Qty_Cut is the quantity actively being manufactured.
    -- Pending versions represent qts for SO where the id_item ends with '#' which indicates a pending SO that has not yet been released to the shop floor.
    SELECT
        id_item_par,
        id_loc,
        SUM(CASE WHEN stat_rec_so = 'R' AND NOT ENDSWITH(id_item_par, '#') THEN qty_onord ELSE 0 END) AS Qty_Rel,
        SUM(CASE WHEN stat_rec_so = 'S' AND NOT ENDSWITH(id_item_par, '#') THEN qty_onord ELSE 0 END) AS Qty_Start,
        SUM(CASE WHEN stat_rec_so = 'R' AND ENDSWITH(id_item_par, '#') THEN qty_onord ELSE 0 END) AS Qty_Rel_PND,
        SUM(CASE WHEN stat_rec_so = 'S' AND ENDSWITH(id_item_par, '#') THEN qty_onord ELSE 0 END) AS Qty_Start_PND
    FROM BRONZE_DATA.TCM_BRONZE."SHPORD_HDR_Bronze"
    WHERE stat_rec_so IN ('R', 'S')
    GROUP BY id_item_par, id_loc
),

InHouseManufacturedItems AS (
    SELECT DISTINCT id_item
    FROM FinishedGoods
    WHERE flag_source = 'M'
),

ReorderParams AS (
    SELECT
        ir.id_item,
        ir.id_loc_home,
        ir.level_rop,
        ir.qty_min_rop,
        ir.qty_mult_ord_rop,
        ir.qty_ord_econ,
        ir.lt_rop
    FROM BRONZE_DATA.TCM_BRONZE."ITMMAS_REORD_Bronze" ir
),

ItemSourceData AS (
    SELECT
        fg.id_item,
        fg.id_loc,
        fg.item_description,
        fg.flag_source,
        fg.flag_stk,
        fg.flag_track_bin,
        fg.flag_cntrl,
        fg.flag_fulfill_type,
        fg.flag_plcy_ord,
        fg.id_planner,
        fg.type_loc,
        fg.id_rte,
        fg.flag_iss_auto_sf,
        fg.QTY_ONHD,
        fg.QTY_ALLOC,
        fg.QTY_ONORD,
        fg.BIN_PRIM,
        vs.primary_vendor_name,
        vs.primary_vendor_id,
        vs.secondary_vendor_name,
        vs.secondary_vendor_id,
        ml.id_loc as primary_mfg_loc,
        CASE WHEN imi.id_item IS NOT NULL THEN 'Y' ELSE 'N' END AS NSA_Manufactured,
        sod.Qty_Rel,
        sod.Qty_Start,
        sod.Qty_Rel_PND,
        sod.Qty_Start_PND,
        CASE
            WHEN fg.flag_source = 'P' THEN 'Y'
            WHEN sod.Qty_Rel_PND > 0 OR sod.Qty_Start_PND > 0 THEN 'Y'
            ELSE 'N'
        END as flag_show,
        rp.id_loc_home,
        rp.level_rop,
        rp.qty_min_rop,
        rp.qty_mult_ord_rop,
        rp.qty_ord_econ,
        rp.lt_rop
    FROM
        FinishedGoods fg
    LEFT JOIN
        VendorSources vs ON fg.id_item = vs.id_item
    LEFT JOIN
        ManufacturingLocations ml ON fg.id_item = ml.id_item AND ml.rn = 1
    LEFT JOIN
        InHouseManufacturedItems imi ON fg.id_item = imi.id_item
    LEFT JOIN
        ShopOrderData sod ON fg.id_item = sod.id_item_par AND fg.id_loc = sod.id_loc
    LEFT JOIN
        ReorderParams rp ON fg.id_item = rp.id_item AND fg.id_loc = rp.id_loc_home
    
),
FinalData AS (
SELECT
    isd.id_item,
    isd.item_description,
    isd.id_loc,

    CASE 
        WHEN isd.FLAG_SOURCE = 'M' THEN 'Manufactured'
        WHEN isd.FLAG_SOURCE = 'P' THEN 'Purchased'
        ELSE 'Other'
    END AS flag_source,
    CASE
        WHEN isd.primary_mfg_loc = '10' THEN 'CLE'
        WHEN isd.primary_mfg_loc = '20' THEN 'CHI'
        WHEN isd.primary_mfg_loc = '50' THEN 'ARK'
        WHEN isd.primary_mfg_loc = '90' THEN 'SAT'
        WHEN isd.primary_mfg_loc = '130' THEN 'DR'
        WHEN LTRIM(isd.primary_vendor_id) = '10602' THEN 'CLE'
        WHEN LTRIM(isd.primary_vendor_id) = 'D79008' THEN 'CHI'
        WHEN LTRIM(isd.primary_vendor_id) = '10932'  THEN 'SAT'
        WHEN LTRIM(isd.primary_vendor_id) = 'D79010'  THEN 'ARK'
        WHEN LTRIM(isd.primary_vendor_id) = 'NSA130'  THEN 'DR'
        WHEN isd.primary_vendor_name IS NOT NULL THEN isd.primary_vendor_name
        ELSE 'Vendor needs to be setup'
    END AS primary_source,
    isd.NSA_Manufactured,
    isd.QTY_ONHD,
    isd.QTY_ALLOC,
    isd.QTY_ONORD,
    isd.BIN_PRIM,
    isd.flag_stk,
    isd.flag_track_bin,
    isd.flag_cntrl,
    isd.flag_fulfill_type,
    isd.flag_plcy_ord,
    isd.id_planner,
    isd.type_loc,
    isd.id_rte,
    isd.flag_iss_auto_sf,
    isd.id_loc_home,
    isd.level_rop,
    isd.qty_min_rop,
    isd.qty_mult_ord_rop,
    isd.qty_ord_econ,
    isd.lt_rop,
    CASE
        WHEN isd.flag_stk IN ('S', 'M') OR isd.flag_show = 'Y' THEN isd.Qty_Start + isd.Qty_Start_PND
        ELSE 0
    END AS Qty_Cut,
    CASE
        WHEN isd.flag_stk IN ('S', 'M') OR isd.flag_show = 'Y' THEN isd.Qty_Rel + isd.Qty_Rel_PND
        ELSE 0
    END AS Qty_Rel
FROM
    ItemSourceData isd
)
SELECT 
    fd.id_item                  AS "Product_ID_SKU",
    fd.item_description         AS "Item_Description",
    fd.id_loc                   AS "Location_ID",
    fd.flag_source              AS "Item_Source_Flag",
    fd.primary_source           AS "Primary_Source",
    fd.NSA_Manufactured         AS "NSA_Manufactured",
    fd.QTY_ONHD                 AS "Qty_On_Hand",
    fd.QTY_ALLOC                AS "Qty_Allocated",
    fd.QTY_ONORD                AS "Qty_On_Order",
    fd.BIN_PRIM                AS  "Primary_Bin",
    fd.flag_stk                 AS "Item_Stock_Flag",
    fd.flag_track_bin           AS "Item_Bin_Tracking",
    fd.flag_cntrl               AS "Item_Controlled_Noncontrolled_Flag",
    fd.flag_fulfill_type        AS "Item_Fulfillment_Type",
    fd.flag_plcy_ord            AS "Item_Order_Policy_Flag",
    fd.id_planner               AS "Item_Planned_Classification",
    fd.type_loc                 AS "Item_Primary_Location_Type",
    fd.id_rte                   AS "Item_Routing_Number",
    fd.flag_iss_auto_sf         AS "Item_Shop_Floor_Auto_Issue_Flag",
    fd.id_loc_home              AS "Item_Home_Location_Code",
    fd.level_rop                AS "Item_Inventory_Reorder_Point",
    fd.qty_min_rop              AS "Item_Inventory_Reorder_Point_Minimum",
    fd.qty_mult_ord_rop         AS "Item_Inventory_Reorder_Point_Mult",
    fd.qty_ord_econ             AS "Item_Order_Quantity_Econ",
    fd.lt_rop                   AS "Item_Reorder_Point_Lead_Time",
    fd.Qty_Cut                  AS "Qty_Cut",
    fd.Qty_Rel                  AS "Qty_Released",
    CASE 
        WHEN fd.primary_source = ln.loc_name THEN 'P'
        ELSE 'S'
    END                         AS "Source_Location_Match_Flag"
FROM FinalData fd
LEFT JOIN LocationNames ln ON fd.id_loc = ln.id_loc
order by fd.id_item, fd.id_loc;

