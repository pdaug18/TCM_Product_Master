-- check if id_item_comp is always null or blank in shpord_hdr
SELECT
    ID_ITEM_COMP,
    COUNT(*) AS COUNT
FROM nsa.SHPORD_HDR
WHERE ID_ITEM_COMP IS NOT NULL AND TRIM(ID_ITEM_COMP) != ''
GROUP BY ID_ITEM_COMP
ORDER BY COUNT DESC;