create or replace dynamic table SILVER_DATA.TCM_SILVER.ITEM_VENDOR_MASTER_DT
TARGET_LAG = '1 day'
WAREHOUSE = ELT_TCM_TRANSFORM
as

WITH base AS (
    SELECT
        LTRIM(ib.id_item) AS id_item,
        iv.FLAG_VND_PRIM,
        iv.ID_VND_ORDFM,
        vof.ID_VND,
        vof.NAME_VND
    FROM bronze_data.tcm_bronze."ITMMAS_BASE_Dynamic" ib
    LEFT JOIN bronze_data.tcm_bronze."ITMMAS_VND_Bronze" iv
        ON LTRIM(ib.id_item) = LTRIM(iv.ID_ITEM)
    LEFT JOIN bronze_data.tcm_bronze."VENMAS_ORDFM_Bronze" vof
        ON LTRIM(iv.ID_VND_ORDFM) = LTRIM(vof.ID_VND_ORDFM)
    WHERE ib.FLAG_STAT_ITEM = 'A'
      AND ib.CODE_CAT_COST = '05'
),
rollup AS (
    SELECT
        id_item,

        /* --- presence checks --- */
        MAX(CASE WHEN FLAG_VND_PRIM IS NOT NULL THEN 1 ELSE 0 END) AS exists_in_itmmas_vnd,
        MAX(CASE WHEN FLAG_VND_PRIM = 'P' THEN 1 ELSE 0 END)       AS has_primary,
        MAX(CASE WHEN FLAG_VND_PRIM = 'S' THEN 1 ELSE 0 END)       AS has_secondary,

        /* --- primary quality flags --- */
        MAX(CASE
              WHEN FLAG_VND_PRIM = 'P'
               AND ID_VND_ORDFM IS NOT NULL
               AND ID_VND IS NULL
              THEN 1 ELSE 0 END) AS primary_missing_in_vendor_master,

        MAX(CASE
              WHEN FLAG_VND_PRIM = 'P'
               AND ID_VND IS NOT NULL
               AND (NAME_VND IS NULL OR LTRIM(RTRIM(NAME_VND)) = '')
              THEN 1 ELSE 0 END) AS primary_name_needs_updated,

        MAX(CASE WHEN FLAG_VND_PRIM = 'P' THEN ID_VND END)   AS primary_vendor_id,
        MAX(CASE WHEN FLAG_VND_PRIM = 'P' THEN NAME_VND END) AS primary_vendor_name_raw,

        /* --- secondary outputs --- */
        listagg(CASE WHEN FLAG_VND_PRIM = 'S' THEN ID_VND END, ', ') AS secondary_vendor_ids,

        listagg(
            CASE
                WHEN FLAG_VND_PRIM = 'S'
                 AND ID_VND_ORDFM IS NOT NULL
                 AND ID_VND IS NULL
                    THEN 'Vendor not setup'
                WHEN FLAG_VND_PRIM = 'S'
                 AND ID_VND IS NOT NULL
                 AND (NAME_VND IS NULL OR LTRIM(RTRIM(NAME_VND)) = '')
                    THEN 'Vendor Name Needs Updated'
                WHEN FLAG_VND_PRIM = 'S'
                    THEN NAME_VND
            END,
            ', '
        ) AS secondary_vendor_names_raw

    FROM base
    GROUP BY id_item
)

SELECT
    id_item,

    /* ---------- Primary output ---------- */
    primary_vendor_id,

    CASE
        WHEN exists_in_itmmas_vnd = 0
            THEN 'Item Vendor not setup'
        WHEN has_primary = 0
            THEN 'No Primary Vendor'
        WHEN primary_missing_in_vendor_master = 1
            THEN 'Vendor not setup'
        WHEN primary_name_needs_updated = 1
            THEN 'Vendor Name Needs Updated'
        ELSE primary_vendor_name_raw
    END AS primary_vendor_name,

    /* ---------- Secondary output ---------- */
    secondary_vendor_ids,

    CASE
        WHEN has_secondary = 0
            THEN 'No Secondary Vendor'
        ELSE secondary_vendor_names_raw
    END AS secondary_vendor_names

FROM rollup;