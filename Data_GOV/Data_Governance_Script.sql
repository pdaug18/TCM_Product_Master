-- Reusable Snowflake column-level governance profile
-- Works for tables and views. Dynamic tables are covered when exposed via INFORMATION_SCHEMA.TABLES.
-- Replace the parameter placeholders or SET from an upstream procedure/task.

/*
SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE_SILVER
 */

SET DB_NAME = 'SILVER_DATA';        -- Set to the database containing the object
SET SCHEMA_NAME = 'TCM_SILVER';  -- Set to the schema containing the object
SET OBJECT_NAME = 'MASTER_PRODUCT_TABLE_SILVER';  -- Set to the table or view name to profile

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

SET INFO_SCHEMA_FQN = (SELECT '"' || REPLACE($DB_NAME, '"', '""') || '"."INFORMATION_SCHEMA"');

SET INFO_SCHEMA_TABLES_FQN = (SELECT $INFO_SCHEMA_FQN || '.TABLES');

SET INFO_SCHEMA_VIEWS_FQN = (SELECT $INFO_SCHEMA_FQN || '.VIEWS');

SET INFO_SCHEMA_COLUMNS_FQN = (SELECT $INFO_SCHEMA_FQN || '.COLUMNS');

SET INFO_SCHEMA_TABLE_CONSTRAINTS_FQN = (SELECT $INFO_SCHEMA_FQN || '.TABLE_CONSTRAINTS');


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
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY UPPER(table_catalog), UPPER(table_schema), UPPER(table_name)
		ORDER BY last_altered DESC NULLS LAST
	) = 1
),
column_meta AS (
	SELECT
		table_catalog AS database_name,
		table_schema AS schema_name,
		table_name,
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
	cm.Table_Name,
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
	END AS "DATA_TYPE",
	-- cm.numeric_precision,
	-- cm.numeric_scale,
	cm.character_maximum_length as char_max_length,
	-- cm.is_nullable,
	-- cm.column_default,
	-- cm.column_comment,
	-- cm.is_identity,
	-- 'NO' AS is_primary_key,
	-- 'NO' AS is_foreign_key,
	-- om.last_altered
FROM column_meta cm
LEFT JOIN object_meta om
	ON cm.database_name = om.database_name
   AND cm.schema_name = om.schema_name
   AND cm.Table_Name = om.object_name
QUALIFY ROW_NUMBER() OVER (
	PARTITION BY UPPER(cm.database_name), UPPER(cm.schema_name), UPPER(cm.table_name), UPPER(cm.column_name), cm.ordinal_position
	ORDER BY om.last_altered DESC NULLS LAST
) = 1;

-- Commented out: all s.* output columns (is_masked, masking_policy_name, tags) are disabled above.
-- Uncomment this block and the LEFT JOIN below to re-enable security profiling.
/*
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
*/

BEGIN
	LET PROFILING_SQL STRING;

	SELECT
		'CREATE OR REPLACE TEMP TABLE SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_PROFILE AS '
		|| COALESCE(
			NULLIF(TRIM(LISTAGG(sql_fragment, ' UNION ALL ') WITHIN GROUP (ORDER BY ordinal_position)), ''),
			'SELECT NULL::STRING AS column_name, NULL::NUMBER AS ordinal_position, NULL::STRING AS min_value, NULL::STRING AS max_value, NULL::NUMBER AS min_length, NULL::NUMBER AS max_length, NULL::NUMBER AS distinct_count, NULL::NUMBER(10,2) AS "NULL_%", NULL::STRING AS sample_values WHERE 1=0'
		)
	INTO :PROFILING_SQL
	FROM (
		WITH c AS (
			SELECT
				column_name,
				ordinal_position,
				"DATA_TYPE",
				'"' || REPLACE(column_name, '"', '""') || '"' AS col_ref,
				UPPER(SPLIT_PART("DATA_TYPE", '(', 1)) AS data_type_base
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
				|| 'COUNT(DISTINCT NULLIF(TRIM(TO_VARCHAR(' || col_ref || ')), '''')) AS distinct_count, '
				|| 'ROUND((COUNT(*) - COUNT(NULLIF(TRIM(TO_VARCHAR(' || col_ref || ')), ''''))) / NULLIF(COUNT(*), 0) * 100, 2) AS "NULL_%", '
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
	b.Table_Name, 
	-- b.object_type,
	b.column_name,
	-- b.ordinal_position,
	b."DATA_TYPE",
	-- b.numeric_precision,
	-- b.numeric_scale,
	b.char_max_length,
	-- b.is_nullable,
	-- b.column_default,
	-- p.min_length,
	-- p.max_length,
	p.min_value,
	p.max_value,
	p.distinct_count,
	p."NULL_%",
	p.sample_values
	-- b.is_primary_key,
	-- b.is_foreign_key,
	-- b.is_identity,
	-- b.column_comment,
	-- COALESCE(s.is_masked, 'NO') AS is_masked,
	-- s.masking_policy_name,
	-- s.tags,
	-- b.last_altered
FROM SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_BASE b
LEFT JOIN SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_PROFILE p
	ON b.column_name = p.column_name
   AND b.ordinal_position = p.ordinal_position
-- LEFT JOIN SANDBOX_RND.DATA_GOVERNANCE_PROFILING.TMP_GOV_COLUMN_SECURITY s
-- 	ON UPPER(b.Table_Name) = s.object_name
--    AND UPPER(b.column_name) = s.column_name
ORDER BY b.ordinal_position;

