from configparser import ConfigParser
from pathlib import Path

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, when, coalesce, abs as sf_abs,
    concat_ws, upper, trim, to_varchar, current_timestamp
)


CONFIG_SECTION = "connections.snowpark"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "snowflake_connect.config"


def load_snowflake_connection_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    parser = ConfigParser()
    read_files = parser.read(config_path)
    if not read_files:
        raise FileNotFoundError(f"Snowflake config file not found: {config_path}")

    if not parser.has_section(CONFIG_SECTION):
        raise KeyError(f"Missing config section [{CONFIG_SECTION}] in {config_path}")

    connection_config = {}
    for key, value in parser.items(CONFIG_SECTION):
        normalized_value = value.strip()
        if normalized_value == "":
            continue

        if normalized_value.lower() == "true":
            connection_config[key] = True
        elif normalized_value.lower() == "false":
            connection_config[key] = False
        else:
            connection_config[key] = normalized_value

    return connection_config


def create_session(config_path: Path = DEFAULT_CONFIG_PATH) -> Session:
    return Session.builder.configs(load_snowflake_connection_config(config_path)).create()


def _normalize_identifier(name: str) -> str:
    return name.strip('"').upper()


def _build_column_lookup(df) -> dict:
    return {_normalize_identifier(column_name): column_name for column_name in df.schema.names}
 
def main(session: Session):
 
    # ------------------------------------------------------------------
    # CONFIG
    # ------------------------------------------------------------------
    OLD_TABLE = "BRONZE_DATA.TCM_BRONZE.OOWP"
    NEW_TABLE = "GOLD_DATA.TCM_GOLD.OPEN_ORDERS_WITH_PRICES"
 
    QA_SCHEMA = "GOLD_DATA.QA"
    DETAIL_TABLE = f"{QA_SCHEMA}.OPEN_ORDER_QA_DETAIL"
    SUMMARY_TABLE = f"{QA_SCHEMA}.OPEN_ORDER_QA_SUMMARY"
    DUP_TABLE = f"{QA_SCHEMA}.OPEN_ORDER_QA_DUPLICATES"
    RUN_LOG_TABLE = f"{QA_SCHEMA}.OPEN_ORDER_QA_RUN_LOG"
 
    PRICE_TOLERANCE = 0.01
    DATE_AS_STRING_FORMAT = "YYYY-MM-DD HH24:MI:SS"
 
    # ------------------------------------------------------------------
    # FIELD MAP
    # old_col : new_col
    # ------------------------------------------------------------------
    field_map = {
        "dataRefreshTimeStamp": "DATAREFRESHTIMESTAMP",
        "open_net_amt": "OPEN_NET_AMT",
        "cust_soldto": "ID_CUST_SOLDTO",
        "ship_complete_flag": "SHIP_COMPLETE_FLAG",
        "WorkingDaysSinceLastPicked": "WorkingDaysSinceLastPicked",
        "flag_pick": "FLAG_PICK",
        "flag_ack": "FLAG_ACKN",
        "amt_ord_total": "AMT_ORD_TOTAL",
        "id_slsrep_1": "ID_SLSREP_1",
        "ID_CARRIER": "ID_CARRIER",
        "DESCR_SHIP_VIA": "DESCR_SHIP_VIA",
        "DR": "DATE_RQST",
        "DP": "DATE_PROM",
        "DO": "DATE_ORD",
        "DATE_CALC_START": "DATE_CALC_START",
        "DATE_CALC_END": "DATE_CALC_END",
        "alt_stk": "ALT_STK",
        "FLAG_MO": "FLAG_MO",
        "id_item": "ID_ITEM",
        "id_ord": "ID_ORD",
        "id_user_add": "ID_USER_ADD",
        "date_add": "Date_Order_Created",
        "seq_line_ord": "SEQ_LINE_ORD",
        "id_so_odbc": "ID_SO_ODBC",
        "VERTICAL": "Item_Vertical",
        "code_user_1_im": "CODE_USER_1",
        "id_rev_draw": "ID_REV_DRAW",
        "qty_open": "QTY_OPEN",
        "flag_stat_item": "FLAG_STAT_ITEM",
        "ol_FLAG_STK": "OL_FLAG_STK",
        "flag_stk": "IL_FLAG_STK",
        "Qty_Rel": "QTY_REL",
        "Qty_Start": "QTY_START",
        "Qty_presew": "QTY_PRESEW",
        "Qty_Rel_PND": "QTY_REL_PND",
        "Qty_Start_PND": "QTY_START_PND",
        "SBNB": "SBNB",
        "QTY_ONHD": "QTY_ONHD",
        "QTY_ALLOC": "QTY_ALLOC",
        "QTY_ONORD": "QTY_ONORD",
        "BIN_PRIM": "BIN_PRIM",
        "LEVEL_ROP": "LEVEL_ROP",
        "stk_test": "STK_TEST",
        "code_um_price": "CODE_UM_PRICE",
        "name_cust_soldto": "NAME_CUST",
        "stat_rec_SO": "STAT_REC_SO",
        "id_SO": "ID_SO",
        "qty_ship_total": "QTY_SHIP_TOTAL",
        "id_ship": "ID_SHIP",
        "code_stat_ord": "CODE_STAT_ORD",
        "id_po_cust": "ID_PO_CUST",
        "NUM_SHIPMENTS": "NUM_SHIPMENTS",
        "NUM_INVCS": "NUM_INVCS",
        "COUNTER": "COUNTER"
    }
 
    # ------------------------------------------------------------------
    # BUSINESS KEY
    # ------------------------------------------------------------------
    key_old = ["id_ord", "seq_line_ord", "id_item"]
    key_new = ["ID_ORD", "SEQ_LINE_ORD", "ID_ITEM"]
 
    # ------------------------------------------------------------------
    # CREATE SCHEMA
    # ------------------------------------------------------------------
    session.sql(f"create schema if not exists {QA_SCHEMA}").collect()
 
    # ------------------------------------------------------------------
    # SOURCE DATA
    # ------------------------------------------------------------------
    old_df = session.table(OLD_TABLE).alias("o")
    new_df = session.table(NEW_TABLE).alias("n")

    old_lookup = _build_column_lookup(old_df)
    new_lookup = _build_column_lookup(new_df)

    missing_old_keys = [column_name for column_name in key_old if _normalize_identifier(column_name) not in old_lookup]
    missing_new_keys = [column_name for column_name in key_new if _normalize_identifier(column_name) not in new_lookup]
    if missing_old_keys or missing_new_keys:
        raise ValueError(
            "Missing key columns required for validation. "
            f"OLD_TABLE missing: {missing_old_keys or 'none'}; "
            f"NEW_TABLE missing: {missing_new_keys or 'none'}"
        )

    resolved_key_old = [old_lookup[_normalize_identifier(column_name)] for column_name in key_old]
    resolved_key_new = [new_lookup[_normalize_identifier(column_name)] for column_name in key_new]

    old_key_ord, old_key_line, old_key_item = resolved_key_old
    new_key_ord, new_key_line, new_key_item = resolved_key_new

    active_field_pairs = []
    skipped_field_map = {}
    for old_column, new_column in field_map.items():
        old_actual = old_lookup.get(_normalize_identifier(old_column))
        new_actual = new_lookup.get(_normalize_identifier(new_column))

        if old_actual and new_actual:
            active_field_pairs.append((old_column, new_column, old_actual, new_actual))
        else:
            skipped_field_map[old_column] = new_column

    if not active_field_pairs:
        raise ValueError(
            "No comparable columns were found between the validation field map and source tables. "
            f"Skipped mappings: {skipped_field_map}"
        )
 
    # ------------------------------------------------------------------
    # RUN METADATA
    # ------------------------------------------------------------------
    run_ts = session.sql("select current_timestamp() as ts").collect()[0]["TS"]
 
    # ------------------------------------------------------------------
    # DUPLICATE CHECKS
    # ------------------------------------------------------------------
    old_dup = (
        session.table(OLD_TABLE)
        .group_by([col(c) for c in resolved_key_old])
        .count()
        .filter(col("COUNT") > 1)
        .select(
            lit("OLD").alias("TABLE_SIDE"),
            col(old_key_ord).alias("ID_ORD"),
            col(old_key_line).alias("SEQ_LINE_ORD"),
            col(old_key_item).alias("ID_ITEM"),
            col("COUNT").alias("DUP_COUNT"),
            lit(str(run_ts)).alias("RUN_TS")
        )
    )
 
    new_dup = (
        session.table(NEW_TABLE)
        .group_by([col(c) for c in resolved_key_new])
        .count()
        .filter(col("COUNT") > 1)
        .select(
            lit("NEW").alias("TABLE_SIDE"),
            col(new_key_ord).alias("ID_ORD"),
            col(new_key_line).alias("SEQ_LINE_ORD"),
            col(new_key_item).alias("ID_ITEM"),
            col("COUNT").alias("DUP_COUNT"),
            lit(str(run_ts)).alias("RUN_TS")
        )
    )
 
    dup_df = old_dup.union_all(new_dup)
    dup_df.write.mode("overwrite").save_as_table(DUP_TABLE)
 
    # ------------------------------------------------------------------
    # NORMALIZE JOIN KEYS
    # ------------------------------------------------------------------
    old_norm = old_df.select(
        col(old_key_ord).alias("K_ID_ORD"),
        col(old_key_line).alias("K_SEQ_LINE_ORD"),
        col(old_key_item).alias("K_ID_ITEM"),
        lit(1).alias("OLD_ROW_PRESENT"),
        *[col(old_actual).alias(f"O__{old_column}") for old_column, _, old_actual, _ in active_field_pairs]
    )
 
    new_norm = new_df.select(
        col(new_key_ord).alias("K_ID_ORD"),
        col(new_key_line).alias("K_SEQ_LINE_ORD"),
        col(new_key_item).alias("K_ID_ITEM"),
        lit(1).alias("NEW_ROW_PRESENT"),
        *[col(new_actual).alias(f"N__{new_column}") for _, new_column, _, new_actual in active_field_pairs]
    )
 
    joined = old_norm.join(
        new_norm,
        on=["K_ID_ORD", "K_SEQ_LINE_ORD", "K_ID_ITEM"],
        how="full"
    )
 
    # ------------------------------------------------------------------
    # HELPER COMPARISON EXPRESSIONS
    # ------------------------------------------------------------------
    def normalize_str(c):
        return upper(trim(coalesce(c.cast("string"), lit(""))))
 
    def normalize_num(c):
        return coalesce(c.cast("double"), lit(0.0))
 
    def normalize_ts(c):
        return coalesce(c.cast("string"), lit(""))
 
    # ------------------------------------------------------------------
    # BUILD DETAIL ROWS
    # one row per field mismatch per business key
    # ------------------------------------------------------------------
    detail_frames = []
 
    numeric_fields = {
        "open_net_amt", "amt_ord_total", "qty_open", "Qty_Rel", "Qty_Start",
        "Qty_presew", "Qty_Rel_PND", "Qty_Start_PND", "SBNB", "QTY_ONHD",
        "QTY_ALLOC", "QTY_ONORD", "LEVEL_ROP", "stk_test",
        "qty_ship_total", "NUM_SHIPMENTS", "NUM_INVCS", "COUNTER"
    }
 
    timestamp_fields = {
        "dataRefreshTimeStamp", "DR", "DP", "DO", "DATE_CALC_START",
        "DATE_CALC_END", "date_add"
    }
 
    for old_col, new_col, _, _ in active_field_pairs:
        o = col(f"O__{old_col}")
        n = col(f"N__{new_col}")
 
        if old_col in numeric_fields:
            mismatch_condition = sf_abs(normalize_num(o) - normalize_num(n)) > PRICE_TOLERANCE
            old_val = o.cast("string")
            new_val = n.cast("string")
        elif old_col in timestamp_fields:
            mismatch_condition = normalize_ts(o) != normalize_ts(n)
            old_val = o.cast("string")
            new_val = n.cast("string")
        else:
            mismatch_condition = normalize_str(o) != normalize_str(n)
            old_val = o.cast("string")
            new_val = n.cast("string")
 
        df = joined.filter(mismatch_condition).select(
            lit(str(run_ts)).alias("RUN_TS"),
            col("K_ID_ORD").alias("ID_ORD"),
            col("K_SEQ_LINE_ORD").alias("SEQ_LINE_ORD"),
            col("K_ID_ITEM").alias("ID_ITEM"),
            lit(old_col).alias("OLD_COLUMN"),
            lit(new_col).alias("NEW_COLUMN"),
            old_val.alias("OLD_VALUE"),
            new_val.alias("NEW_VALUE"),
            lit("VALUE_MISMATCH").alias("ISSUE_TYPE")
        )
 
        detail_frames.append(df)
 
    # Missing records: old only
    missing_in_new = joined.filter(
        col("K_ID_ORD").is_not_null() &
        col("OLD_ROW_PRESENT").is_not_null() &
        col("NEW_ROW_PRESENT").is_null()
    ).select(
        lit(str(run_ts)).alias("RUN_TS"),
        col("K_ID_ORD").alias("ID_ORD"),
        col("K_SEQ_LINE_ORD").alias("SEQ_LINE_ORD"),
        col("K_ID_ITEM").alias("ID_ITEM"),
        lit(None).cast("string").alias("OLD_COLUMN"),
        lit(None).cast("string").alias("NEW_COLUMN"),
        lit("ROW_PRESENT").alias("OLD_VALUE"),
        lit("ROW_MISSING").alias("NEW_VALUE"),
        lit("MISSING_IN_NEW").alias("ISSUE_TYPE")
    )
 
    # Missing records: new only
    missing_in_old = joined.filter(
        col("K_ID_ORD").is_not_null() &
        col("NEW_ROW_PRESENT").is_not_null() &
        col("OLD_ROW_PRESENT").is_null()
    ).select(
        lit(str(run_ts)).alias("RUN_TS"),
        col("K_ID_ORD").alias("ID_ORD"),
        col("K_SEQ_LINE_ORD").alias("SEQ_LINE_ORD"),
        col("K_ID_ITEM").alias("ID_ITEM"),
        lit(None).cast("string").alias("OLD_COLUMN"),
        lit(None).cast("string").alias("NEW_COLUMN"),
        lit("ROW_MISSING").alias("OLD_VALUE"),
        lit("ROW_PRESENT").alias("NEW_VALUE"),
        lit("MISSING_IN_OLD").alias("ISSUE_TYPE")
    )
 
    detail_df = detail_frames[0]
    for d in detail_frames[1:]:
        detail_df = detail_df.union_all(d)
 
    detail_df = detail_df.union_all(missing_in_new).union_all(missing_in_old)
    detail_df.write.mode("overwrite").save_as_table(DETAIL_TABLE)
 
    # ------------------------------------------------------------------
    # SUMMARY
    # ------------------------------------------------------------------
    summary_df = session.table(DETAIL_TABLE).group_by(
        col("ISSUE_TYPE"), col("OLD_COLUMN"), col("NEW_COLUMN")
    ).count().select(
        lit(str(run_ts)).alias("RUN_TS"),
        col("ISSUE_TYPE"),
        col("OLD_COLUMN"),
        col("NEW_COLUMN"),
        col("COUNT").alias("ISSUE_COUNT")
    )
 
    summary_df.write.mode("overwrite").save_as_table(SUMMARY_TABLE)
 
    # ------------------------------------------------------------------
    # RUN LOG
    # ------------------------------------------------------------------
    old_count = session.table(OLD_TABLE).count()
    new_count = session.table(NEW_TABLE).count()
    detail_count = session.table(DETAIL_TABLE).count()
    dup_count = session.table(DUP_TABLE).count()
 
    run_log_df = session.create_dataframe(
        [[
            str(run_ts),
            OLD_TABLE,
            NEW_TABLE,
            old_count,
            new_count,
            detail_count,
            dup_count
        ]],
        schema=[
            "RUN_TS",
            "OLD_TABLE",
            "NEW_TABLE",
            "OLD_ROW_COUNT",
            "NEW_ROW_COUNT",
            "DETAIL_ISSUE_COUNT",
            "DUPLICATE_KEY_COUNT"
        ]
    )
 
    run_log_df.write.mode("overwrite").save_as_table(RUN_LOG_TABLE)
 
    return session.table(SUMMARY_TABLE)


if __name__ == "__main__":
    session = create_session()
    try:
        result = main(session)
        result.show()
    finally:
        session.close()