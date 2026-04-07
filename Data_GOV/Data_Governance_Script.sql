-- Reusable Snowflake column-level governance profile
-- Works for tables and views. Dynamic tables are covered when exposed via INFORMATION_SCHEMA.TABLES.
-- Replace the parameter placeholders or SET from an upstream procedure/task.

-- SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER

SET DB_NAME = 'SILVER_DATA';        -- Set to the database containing the object
SET SCHEMA_NAME = 'TCM_SILVER';  -- Set to the schema containing the object
SET OBJECT_NAME = 'ITEM_INVENTORY_MASTER';  -- Set to the table or view name to profile

-- Profiling controls: for large objects, set PROFILE_ON_SAMPLE = TRUE to reduce scan cost.
SET PROFILE_ON_SAMPLE = FALSE;  -- Set to TRUE to enable sampling for profiling (uses SAMPLE BERNOULLI)
SET PROFILE_SAMPLE_PCT = 10;    -- Percentage of data to sample when PROFILE_ON_SAMPLE is TRUE (between 0 and 100)
SET SAMPLE_VALUE_COUNT = 5;     -- Number of distinct sample values to retrieve for each column (only for non-numeric types)


SET OBJECT_FQN = (
	SELECT
		'"' || REPLACE($DB_NAME, '"', '""') || '"."'
		|| REPLACE($SCHEMA_NAME, '"', '""') || '"."'
		|| REPLACE($OBJECT_NAME, '"', '""') || '"'
);

SET INFO_SCHEMA_FQN = (
	SELECT '"' || REPLACE($DB_NAME, '"', '""') || '"."INFORMATION_SCHEMA"'
);

SET INFO_SCHEMA_TABLES_FQN = (
	SELECT $INFO_SCHEMA_FQN || '.TABLES'
);

SET INFO_SCHEMA_VIEWS_FQN = (
	SELECT $INFO_SCHEMA_FQN || '.VIEWS'
);

SET INFO_SCHEMA_COLUMNS_FQN = (
	SELECT $INFO_SCHEMA_FQN || '.COLUMNS'
);

SET INFO_SCHEMA_TABLE_CONSTRAINTS_FQN = (
	SELECT $INFO_SCHEMA_FQN || '.TABLE_CONSTRAINTS'
);


CREATE OR REPLACE TEMP TABLE SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_BASE AS
WITH object_meta AS (
	SELECT
		table_catalog AS database_name,
		table_schema AS schema_name,
		table_name AS object_name,
		table_type AS object_type,
		last_altered
	FROM IDENTIFIER($INFO_SCHEMA_TABLES_FQN)
	WHERE UPPER(table_schema) = UPPER($SCHEMA_NAME)
	  AND UPPER(table_name) = UPPER($OBJECT_NAME)

	UNION ALL

	SELECT
		table_catalog AS database_name,
		table_schema AS schema_name,
		table_name AS object_name,
		'VIEW' AS object_type,
		last_altered
	FROM IDENTIFIER($INFO_SCHEMA_VIEWS_FQN)
	WHERE UPPER(table_schema) = UPPER($SCHEMA_NAME)
	  AND UPPER(table_name) = UPPER($OBJECT_NAME)
),
column_meta AS (
	SELECT
		table_catalog AS database_name,
		table_schema AS schema_name,
		table_name AS object_name,
		column_name,
		ordinal_position,
		data_type,
		numeric_precision,
		numeric_scale,
		character_maximum_length,
		datetime_precision,
		is_nullable,
		column_default,
		comment AS column_comment,
		COALESCE(is_identity, IFF(identity_start IS NOT NULL, 'YES', 'NO')) AS is_identity
	FROM IDENTIFIER($INFO_SCHEMA_COLUMNS_FQN)
	WHERE UPPER(table_schema) = UPPER($SCHEMA_NAME)
	  AND UPPER(table_name) = UPPER($OBJECT_NAME)
)
SELECT
	cm.database_name,
	cm.schema_name,
	cm.object_name,
	om.object_type,
	cm.column_name,
	cm.ordinal_position,
	CASE
		WHEN UPPER(cm.data_type) IN ('NUMBER', 'DECIMAL', 'NUMERIC')
			THEN cm.data_type || '(' || COALESCE(TO_VARCHAR(cm.numeric_precision), '38') || ',' || COALESCE(TO_VARCHAR(cm.numeric_scale), '0') || ')'
		WHEN UPPER(cm.data_type) IN ('VARCHAR', 'CHAR', 'CHARACTER', 'STRING', 'TEXT', 'BINARY', 'VARBINARY')
			 AND cm.character_maximum_length IS NOT NULL
			THEN cm.data_type || '(' || cm.character_maximum_length || ')'
		WHEN UPPER(cm.data_type) IN ('TIME', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ')
			 AND cm.datetime_precision IS NOT NULL
			THEN cm.data_type || '(' || cm.datetime_precision || ')'
		ELSE cm.data_type
	END AS data_type_full,
	cm.numeric_precision,
	cm.numeric_scale,
	cm.character_maximum_length,
	cm.is_nullable,
	cm.column_default,
	cm.column_comment,
	cm.is_identity,
	'NO' AS is_primary_key,
	'NO' AS is_foreign_key,
	om.last_altered
FROM column_meta cm
LEFT JOIN object_meta om
	ON cm.database_name = om.database_name
   AND cm.schema_name = om.schema_name
   AND cm.object_name = om.object_name;

CREATE OR REPLACE TEMP TABLE SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_SECURITY AS
WITH mask_refs AS (
	SELECT
		UPPER(ref_entity_name) AS object_name,
		UPPER(ref_column_name) AS column_name,
		MAX(policy_name) AS masking_policy_name
	FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
	WHERE policy_kind = 'MASKING_POLICY'
	  AND ref_entity_domain = 'COLUMN'
	  AND UPPER(ref_entity_name) = UPPER($OBJECT_NAME)
	GROUP BY 1, 2
),
tag_refs AS (
	SELECT
		UPPER(object_name) AS object_name,
		UPPER(column_name) AS column_name,
		LISTAGG(tag_name || '=' || COALESCE(tag_value, ''), '; ')
			WITHIN GROUP (ORDER BY tag_name) AS tags
	FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
	WHERE domain = 'COLUMN'
	  AND UPPER(object_database) = UPPER($DB_NAME)
	  AND UPPER(object_schema) = UPPER($SCHEMA_NAME)
	  AND UPPER(object_name) = UPPER($OBJECT_NAME)
	GROUP BY 1, 2
)
SELECT
	UPPER($OBJECT_NAME) AS object_name,
	COALESCE(m.column_name, t.column_name) AS column_name,
	IFF(m.masking_policy_name IS NOT NULL, 'YES', 'NO') AS is_masked,
	m.masking_policy_name,
	t.tags
FROM mask_refs m
FULL OUTER JOIN tag_refs t
	ON m.object_name = t.object_name
   AND m.column_name = t.column_name;

BEGIN
	LET PROFILING_SQL STRING;

	SELECT
		'CREATE OR REPLACE TEMP TABLE SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_PROFILE AS '
		|| LISTAGG(sql_fragment, ' UNION ALL ') WITHIN GROUP (ORDER BY ordinal_position)
	INTO :PROFILING_SQL
	FROM (
		WITH c AS (
			SELECT
				column_name,
				ordinal_position,
				data_type_full,
				'"' || REPLACE(column_name, '"', '""') || '"' AS col_ref,
				UPPER(SPLIT_PART(data_type_full, '(', 1)) AS data_type_base
			FROM SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_BASE
			ORDER BY ordinal_position
		),
		q AS (
			SELECT
				ordinal_position,
				'SELECT '
				|| '''' || REPLACE(column_name, '''', '''''') || '''' || ' AS column_name, '
				|| ordinal_position || ' AS ordinal_position, '
				|| CASE
					WHEN data_type_base IN ('NUMBER', 'DECIMAL', 'NUMERIC', 'FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE', 'DOUBLE PRECISION', 'REAL', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT', 'DATE', 'TIME', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'BOOLEAN')
						THEN 'TO_VARCHAR(MIN(' || col_ref || '))'
					ELSE 'NULL'
				   END || ' AS min_value, '
				|| CASE
					WHEN data_type_base IN ('NUMBER', 'DECIMAL', 'NUMERIC', 'FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE', 'DOUBLE PRECISION', 'REAL', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT', 'DATE', 'TIME', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'BOOLEAN')
						THEN 'TO_VARCHAR(MAX(' || col_ref || '))'
					ELSE 'NULL'
				   END || ' AS max_value, '
				|| CASE
					WHEN data_type_base IN ('VARCHAR', 'CHAR', 'CHARACTER', 'STRING', 'TEXT')
						THEN 'MIN(IFF(NULLIF(TRIM(' || col_ref || '), '''') IS NULL, NULL, LENGTH(' || col_ref || ')))'
					ELSE 'NULL'
				   END || ' AS min_length, '
				|| CASE
					WHEN data_type_base IN ('VARCHAR', 'CHAR', 'CHARACTER', 'STRING', 'TEXT')
						THEN 'MAX(IFF(NULLIF(TRIM(' || col_ref || '), '''') IS NULL, NULL, LENGTH(' || col_ref || ')))'
					ELSE 'NULL'
				   END || ' AS max_length, '
				|| 'COUNT(DISTINCT IFF(' || col_ref || ' IS NULL, NULL, ' || col_ref || ')) AS distinct_count, '
				|| '(SELECT LISTAGG(sample_v, '', '') WITHIN GROUP (ORDER BY sample_v) FROM ('
				|| 'SELECT DISTINCT TO_VARCHAR(' || col_ref || ') AS sample_v '
				|| 'FROM ' || $OBJECT_FQN
				|| IFF($PROFILE_ON_SAMPLE, ' SAMPLE BERNOULLI (' || $PROFILE_SAMPLE_PCT || ') ', ' ')
				|| 'WHERE ' || col_ref || ' IS NOT NULL '
				|| 'AND NULLIF(TRIM(TO_VARCHAR(' || col_ref || ')), '''') IS NOT NULL '
				|| 'ORDER BY HASH(sample_v) '
				|| 'LIMIT ' || $SAMPLE_VALUE_COUNT
				|| ')) AS sample_values '
				|| 'FROM ' || $OBJECT_FQN
				|| IFF($PROFILE_ON_SAMPLE, ' SAMPLE BERNOULLI (' || $PROFILE_SAMPLE_PCT || ') ', '')
				AS sql_fragment
			FROM c
		)
		SELECT sql_fragment, ordinal_position
		FROM q
	);

	EXECUTE IMMEDIATE :PROFILING_SQL;
END;

SELECT
	b.database_name,
	b.schema_name,
	b.object_name,
	b.object_type,
	b.column_name,
	b.ordinal_position,
	b.data_type_full,
	b.numeric_precision,
	b.numeric_scale,
	b.character_maximum_length,
	b.is_nullable,
	b.column_default,
	p.min_length,
	p.max_length,
	p.min_value,
	p.max_value,
	p.distinct_count,
	p.sample_values,
	b.is_primary_key,
	b.is_foreign_key,
	b.is_identity,
	b.column_comment,
	COALESCE(s.is_masked, 'NO') AS is_masked,
	s.masking_policy_name,
	s.tags,
	b.last_altered
FROM SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_BASE b
LEFT JOIN SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_PROFILE p
	ON b.column_name = p.column_name
   AND b.ordinal_position = p.ordinal_position
LEFT JOIN SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_SECURITY s
	ON UPPER(b.object_name) = s.object_name
   AND UPPER(b.column_name) = s.column_name
ORDER BY b.ordinal_position;
