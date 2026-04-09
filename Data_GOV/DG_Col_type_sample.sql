EXECUTE IMMEDIATE $$
DECLARE
    db_name STRING DEFAULT 'GOLD_DATA';
    schema_name STRING DEFAULT 'TCM_GOLD';
    object_name STRING DEFAULT 'OPEN_ORDERS_WITH_PRICES';
    sample_limit NUMBER DEFAULT 10;
    use_db_sql STRING;
    profile_sql STRING;
    final_sql STRING;
    rs RESULTSET;
BEGIN
    -- Ensure INFORMATION_SCHEMA resolves against the requested database.
    use_db_sql := 'USE DATABASE "' || REPLACE(db_name, '"', '""') || '"';
    EXECUTE IMMEDIATE use_db_sql;

    SELECT LISTAGG(
            'SELECT ' ||
            '''' || REPLACE(COLUMN_NAME, '''', '''''') || ''' AS COLUMN_NAME, ' ||
            '''' || REPLACE(DATA_TYPE, '''', '''''') || ''' AS DATA_TYPE, ' ||
            'TO_VARCHAR("' || REPLACE(COLUMN_NAME, '"', '""') || '") AS SAMPLE_VALUE ' ||
            'FROM "' || REPLACE(:db_name, '"', '""') || '"."' || REPLACE(:schema_name, '"', '""') || '"."' || REPLACE(:object_name, '"', '""') || '" ' ||
            'WHERE "' || REPLACE(COLUMN_NAME, '"', '""') || '" IS NOT NULL ' ||
            'QUALIFY ROW_NUMBER() OVER (ORDER BY "' || REPLACE(COLUMN_NAME, '"', '""') || '") <= ' || TO_VARCHAR(:sample_limit)
            , ' UNION ALL '
    ) WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    INTO :profile_sql
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = UPPER(:schema_name)
        AND TABLE_NAME   = UPPER(:object_name);

    final_sql := COALESCE(
            profile_sql,
            'SELECT ''Object not found, unsupported type, or no non-null column values found.'' AS COLUMN_NAME, NULL::STRING AS DATA_TYPE, NULL::STRING AS SAMPLE_VALUE'
    );

    rs := (EXECUTE IMMEDIATE final_sql);
    RETURN TABLE(rs);
END;
$$;


-- =============================================================
-- Columns Names and Types for Master_ShopOrder.OPEN_ORDERS_WITH_PRICES
-- Source: GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES
-- =============================================================
SELECT
    COLUMN_NAME,
    DATA_TYPE  
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'TCM_GOLD'
  AND TABLE_NAME   = 'OPEN_ORDERS_WITH_PRICES'