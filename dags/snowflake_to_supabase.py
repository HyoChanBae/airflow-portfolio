"""Batch migration from Snowflake tables to Supabase PostgreSQL.

Configuration is read from a manual DAG run's ``conf.tables`` value first and
then from the ``snowflake_to_supabase_tables`` Airflow Variable.  Keeping the
configuration outside this file makes the DAG reusable without putting
credentials or environment-specific table names in source control.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from psycopg2 import sql
from psycopg2.extras import Json, execute_values


LOGGER = logging.getLogger(__name__)
CONFIG_VARIABLE = "snowflake_to_supabase_tables"
SNOWFLAKE_CONN_ID = "snowflake_conn3"
SUPABASE_CONN_ID = "supabase_postgres"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
VALID_WRITE_MODES = {"append", "replace", "truncate", "upsert"}
JSON_TYPES = {"ARRAY", "OBJECT", "VARIANT", "VECTOR"}
TIMESTAMP_WITH_TIME_ZONE_TYPES = {"TIMESTAMP_TZ", "TIMESTAMP_LTZ"}
TIMESTAMP_WITHOUT_TIME_ZONE_TYPES = {"DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ"}


@dataclass(frozen=True)
class TableConfig:
    source_database: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    write_mode: str
    batch_size: int
    primary_key_columns: tuple[str, ...]
    lowercase_columns: bool
    source_case_sensitive: bool


def _validate_identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{field_name} must be a simple SQL identifier containing only "
            "letters, numbers, '_', or '$': {value!r}"
        )
    return value


def _split_qualified_name(value: Any, expected_parts: int, field_name: str) -> list[str]:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a dot-separated string")
    parts = value.split(".")
    if len(parts) != expected_parts:
        raise ValueError(f"{field_name} must have {expected_parts} parts: {value!r}")
    return [_validate_identifier(part, field_name) for part in parts]


def _parse_table_config(raw: Any) -> TableConfig:
    if not isinstance(raw, dict):
        raise ValueError(f"Each table configuration must be an object, got {raw!r}")

    source_database, source_schema, source_table = _split_qualified_name(
        raw.get("source"), 3, "source"
    )
    target_schema, target_table = _split_qualified_name(
        raw.get("target"), 2, "target"
    )
    write_mode = str(raw.get("write_mode", "replace")).lower()
    if write_mode not in VALID_WRITE_MODES:
        raise ValueError(
            f"write_mode must be one of {sorted(VALID_WRITE_MODES)}, got {write_mode!r}"
        )

    batch_size = raw.get("batch_size", 5_000)
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise ValueError("batch_size must be an integer")
    if not 1 <= batch_size <= 100_000:
        raise ValueError("batch_size must be between 1 and 100000")

    primary_keys = raw.get("primary_key_columns", [])
    if not isinstance(primary_keys, list):
        raise ValueError("primary_key_columns must be an array")
    primary_keys = tuple(
        _validate_identifier(column, "primary_key_columns") for column in primary_keys
    )
    if write_mode == "upsert" and not primary_keys:
        raise ValueError("upsert mode requires primary_key_columns")

    lowercase_columns = raw.get("lowercase_columns", True)
    source_case_sensitive = raw.get("source_case_sensitive", False)
    if not isinstance(lowercase_columns, bool) or not isinstance(source_case_sensitive, bool):
        raise ValueError("lowercase_columns and source_case_sensitive must be booleans")

    return TableConfig(
        source_database=source_database,
        source_schema=source_schema,
        source_table=source_table,
        target_schema=target_schema,
        target_table=target_table,
        write_mode=write_mode,
        batch_size=batch_size,
        primary_key_columns=primary_keys,
        lowercase_columns=lowercase_columns,
        source_case_sensitive=source_case_sensitive,
    )


def _quote_snowflake_identifier(identifier: str) -> str:
    # Identifiers have already passed _validate_identifier, so no user-provided
    # quote can escape this boundary.
    return f'"{identifier}"'


def _snowflake_type_to_postgres(column: dict[str, Any]) -> str:
    data_type = column["data_type"].upper()
    precision = column["numeric_precision"]
    scale = column["numeric_scale"]

    if data_type in {"NUMBER", "DECIMAL", "NUMERIC"}:
        if precision is not None and scale is not None and 0 <= scale <= precision <= 1000:
            return f"NUMERIC({precision},{scale})"
        return "NUMERIC"
    if data_type in {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"}:
        return "DOUBLE PRECISION"
    if data_type in {"BOOLEAN"}:
        return "BOOLEAN"
    if data_type == "DATE":
        return "DATE"
    if data_type == "TIME":
        return "TIME"
    if data_type in TIMESTAMP_WITH_TIME_ZONE_TYPES:
        return "TIMESTAMPTZ"
    if data_type in TIMESTAMP_WITHOUT_TIME_ZONE_TYPES:
        return "TIMESTAMP"
    if data_type in {"BINARY", "VARBINARY"}:
        return "BYTEA"
    if data_type in JSON_TYPES:
        return "JSONB"
    # Snowflake VARCHAR/TEXT lengths can exceed PostgreSQL's practical varchar
    # limit. TEXT also avoids losing data when schemas differ slightly.
    return "TEXT"


def _load_configuration() -> list[TableConfig]:
    context = get_current_context()
    dag_run = context.get("dag_run")
    run_conf = dag_run.conf if dag_run else {}
    raw_tables = run_conf.get("tables") if isinstance(run_conf, dict) else None
    if raw_tables is None:
        raw_tables = Variable.get(CONFIG_VARIABLE, default_var=None, deserialize_json=True)

    if not isinstance(raw_tables, list) or not raw_tables:
        raise ValueError(
            f"Provide a non-empty conf.tables array when triggering the DAG, or set "
            f"the {CONFIG_VARIABLE!r} Airflow Variable"
        )
    return [_parse_table_config(item) for item in raw_tables]


def _get_snowflake_columns(cursor: Any, config: TableConfig) -> list[dict[str, Any]]:
    database = (
        config.source_database
        if config.source_case_sensitive
        else config.source_database.upper()
    )
    schema = config.source_schema if config.source_case_sensitive else config.source_schema.upper()
    table = config.source_table if config.source_case_sensitive else config.source_table.upper()
    metadata_query = f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM {_quote_snowflake_identifier(database)}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    cursor.execute(metadata_query, (schema, table))
    columns = [
        {
            "name": row[0],
            "data_type": row[1],
            "numeric_precision": row[2],
            "numeric_scale": row[3],
            "nullable": row[4] == "YES",
        }
        for row in cursor.fetchall()
    ]
    if not columns:
        raise ValueError(
            f"Snowflake table {database}.{schema}.{table} was not found or has no columns"
        )
    return columns


def _target_column_names(
    source_columns: list[dict[str, Any]], lowercase: bool
) -> list[str]:
    names = [column["name"].lower() if lowercase else column["name"] for column in source_columns]
    if len(set(names)) != len(names):
        raise ValueError("Column names collide after lowercase_columns conversion")
    for name in names:
        _validate_identifier(name, "Snowflake column")
        if len(name.encode("utf-8")) > 63:
            raise ValueError(f"PostgreSQL identifier is longer than 63 bytes: {name!r}")
    return names


def _prepare_target_table(
    cursor: Any,
    config: TableConfig,
    source_columns: list[dict[str, Any]],
    target_columns: list[str],
) -> None:
    table_identifier = sql.Identifier(config.target_schema, config.target_table)
    cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(config.target_schema)))

    if config.write_mode == "replace":
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_identifier))

    definitions = []
    for source_column, target_name in zip(source_columns, target_columns):
        nullable_sql = sql.SQL("") if source_column["nullable"] else sql.SQL(" NOT NULL")
        definitions.append(
            sql.SQL("{} {}{}").format(
                sql.Identifier(target_name),
                sql.SQL(_snowflake_type_to_postgres(source_column)),
                nullable_sql,
            )
        )

    target_primary_keys = [
        key.lower() if config.lowercase_columns else key for key in config.primary_key_columns
    ]
    missing_keys = set(target_primary_keys).difference(target_columns)
    if missing_keys:
        raise ValueError(f"Primary key columns are absent from source: {sorted(missing_keys)}")
    if target_primary_keys:
        definitions.append(
            sql.SQL("PRIMARY KEY ({})").format(
                sql.SQL(", ").join(sql.Identifier(key) for key in target_primary_keys)
            )
        )

    cursor.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            table_identifier, sql.SQL(", ").join(definitions)
        )
    )
    if config.write_mode == "truncate":
        cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(table_identifier))


def _build_insert_statement(
    connection: Any, config: TableConfig, target_columns: list[str]
) -> str:
    statement = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        sql.Identifier(config.target_schema, config.target_table),
        sql.SQL(", ").join(sql.Identifier(column) for column in target_columns),
    )
    if config.write_mode == "upsert":
        primary_keys = [
            key.lower() if config.lowercase_columns else key
            for key in config.primary_key_columns
        ]
        update_columns = [column for column in target_columns if column not in primary_keys]
        conflict_target = sql.SQL(", ").join(sql.Identifier(key) for key in primary_keys)
        if update_columns:
            assignments = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(column), sql.Identifier(column)
                )
                for column in update_columns
            )
            statement += sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                conflict_target, assignments
            )
        else:
            statement += sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(conflict_target)
    return statement.as_string(connection)


def _adapt_row(row: tuple[Any, ...], json_indexes: set[int]) -> tuple[Any, ...]:
    values = list(row)
    for index in json_indexes:
        value = values[index]
        if value is None:
            continue
        if isinstance(value, str):
            value = json.loads(value)
        values[index] = Json(value)
    return tuple(values)


def _build_snowflake_select_list(source_columns: list[dict[str, Any]]) -> str:
    """Return a projection that avoids Python connector timestamp decoding bugs.

    Some connector/result-format combinations expose nanosecond timestamps as a
    seconds-since-epoch value and Snowflake rejects the result before fetchmany
    can read it. Formatting timestamps server-side preserves their declared
    semantics, and PostgreSQL then coerces the ISO text into the target timestamp
    column during INSERT.
    """
    expressions = []
    for column in source_columns:
        column_name = _quote_snowflake_identifier(column["name"])
        data_type = column["data_type"].upper()
        if data_type in TIMESTAMP_WITH_TIME_ZONE_TYPES:
            expressions.append(
                f"TO_VARCHAR({column_name}, "
                f"'YYYY-MM-DD\"T\"HH24:MI:SS.FF9 TZH:TZM') AS {column_name}"
            )
        elif data_type in TIMESTAMP_WITHOUT_TIME_ZONE_TYPES:
            expressions.append(
                f"TO_VARCHAR({column_name}, "
                f"'YYYY-MM-DD\"T\"HH24:MI:SS.FF9') AS {column_name}"
            )
        else:
            expressions.append(column_name)
    return ", ".join(expressions)


def _migrate_table(
    snowflake_cursor: Any,
    postgres_connection: Any,
    postgres_cursor: Any,
    config: TableConfig,
) -> int:
    source_columns = _get_snowflake_columns(snowflake_cursor, config)
    LOGGER.info(
        "Snowflake source column types: %s",
        [(column["name"], column["data_type"]) for column in source_columns],
    )
    target_columns = _target_column_names(source_columns, config.lowercase_columns)
    _prepare_target_table(postgres_cursor, config, source_columns, target_columns)
    insert_statement = _build_insert_statement(postgres_connection, config, target_columns)

    source_parts = [
        config.source_database,
        config.source_schema,
        config.source_table,
    ]
    if not config.source_case_sensitive:
        source_parts = [part.upper() for part in source_parts]
    source_name = ".".join(_quote_snowflake_identifier(part) for part in source_parts)
    select_list = _build_snowflake_select_list(source_columns)
    timestamp_columns = [
        column["name"]
        for column in source_columns
        if column["data_type"].upper()
        in TIMESTAMP_WITH_TIME_ZONE_TYPES | TIMESTAMP_WITHOUT_TIME_ZONE_TYPES
    ]
    if timestamp_columns:
        LOGGER.info(
            "Formatting Snowflake timestamp columns as ISO text: %s",
            timestamp_columns,
        )
    snowflake_cursor.execute(f"SELECT {select_list} FROM {source_name}")

    json_indexes = {
        index
        for index, column in enumerate(source_columns)
        if column["data_type"].upper() in JSON_TYPES
    }
    migrated_rows = 0
    while True:
        rows = snowflake_cursor.fetchmany(config.batch_size)
        if not rows:
            break
        adapted_rows = [_adapt_row(row, json_indexes) for row in rows]
        execute_values(
            postgres_cursor,
            insert_statement,
            adapted_rows,
            page_size=config.batch_size,
        )
        migrated_rows += len(rows)
        LOGGER.info(
            "Migrated %s rows so far: %s.%s -> %s.%s",
            migrated_rows,
            config.source_schema,
            config.source_table,
            config.target_schema,
            config.target_table,
        )

    if config.write_mode in {"replace", "truncate"}:
        postgres_cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(config.target_schema, config.target_table)
            )
        )
        target_count = postgres_cursor.fetchone()[0]
        if target_count != migrated_rows:
            raise RuntimeError(
                f"Row-count validation failed for {config.target_schema}.{config.target_table}: "
                f"read {migrated_rows}, found {target_count}"
            )
    return migrated_rows


@dag(
    dag_id="snowflake_to_supabase",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["migration", "snowflake", "supabase", "postgres"],
)
def snowflake_to_supabase():
    # A retry after a previously committed append table could duplicate rows.
    # Keep retries disabled; failed runs can be resumed deliberately after the
    # operator checks which table completed in the task log.
    @task(retries=0)
    def migrate() -> None:
        table_configs = _load_configuration()
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        postgres_hook = PostgresHook(postgres_conn_id=SUPABASE_CONN_ID)
        snowflake_connection = snowflake_hook.get_conn()
        postgres_connection = postgres_hook.get_conn()
        postgres_connection.autocommit = False

        try:
            with snowflake_connection.cursor() as snowflake_cursor:
                with postgres_connection.cursor() as postgres_cursor:
                    for table_config in table_configs:
                        LOGGER.info("Starting migration with config: %s", table_config)
                        row_count = _migrate_table(
                            snowflake_cursor,
                            postgres_connection,
                            postgres_cursor,
                            table_config,
                        )
                        # Commit each table independently so a later table failure does
                        # not roll back already completed tables.
                        postgres_connection.commit()
                        LOGGER.info(
                            "Completed %s.%s with %s source rows",
                            table_config.target_schema,
                            table_config.target_table,
                            row_count,
                        )
        except Exception:
            postgres_connection.rollback()
            raise
        finally:
            postgres_connection.close()
            snowflake_connection.close()

    migrate()


snowflake_to_supabase()
