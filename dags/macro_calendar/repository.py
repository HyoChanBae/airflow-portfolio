import re
from collections.abc import Iterable

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


DEFAULT_TABLE = "DEMO_RAW_DB.RAW.MACRO_RELEASE_CALENDAR"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*){2}$")


def _validate_table_name(table_name: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(table_name):
        raise ValueError("Snowflake table name must be a three-part identifier")
    return table_name


def _deduplicate(events: Iterable[dict]) -> list[dict]:
    return list({event["event_key"]: event for event in events}.values())


def merge_calendar_events(
    events: Iterable[dict],
    snowflake_conn_id: str = "snowflake_conn3",
    table_name: str = DEFAULT_TABLE,
) -> dict:
    table_name = _validate_table_name(table_name)
    received_events = list(events)
    unique_events = _deduplicate(received_events)
    if not unique_events:
        raise ValueError("No calendar events were provided for Snowflake merge")

    merge_sql = f"""
        MERGE INTO {table_name} AS T
        USING (
            SELECT
                %s::VARCHAR AS EVENT_KEY,
                %s::VARCHAR AS SOURCE_EVENT_ID,
                %s::VARCHAR AS INDICATOR_CODE,
                %s::VARCHAR AS INDICATOR_NAME,
                %s::VARCHAR AS CATEGORY,
                %s::VARCHAR AS SOURCE,
                %s::VARCHAR AS REFERENCE_PERIOD,
                TO_TIMESTAMP_TZ(%s) AS RELEASE_AT_UTC,
                %s::NUMBER(1, 0) AS IMPORTANCE,
                %s::VARCHAR AS EVENT_URL,
                %s::VARCHAR AS STATUS,
                %s::VARCHAR AS RAW_TITLE
        ) AS S
        ON T.EVENT_KEY = S.EVENT_KEY
        WHEN MATCHED THEN UPDATE SET
            T.SOURCE_EVENT_ID = S.SOURCE_EVENT_ID,
            T.INDICATOR_CODE = S.INDICATOR_CODE,
            T.INDICATOR_NAME = S.INDICATOR_NAME,
            T.CATEGORY = S.CATEGORY,
            T.SOURCE = S.SOURCE,
            T.REFERENCE_PERIOD = S.REFERENCE_PERIOD,
            T.RELEASE_AT_UTC = S.RELEASE_AT_UTC,
            T.IMPORTANCE = S.IMPORTANCE,
            T.EVENT_URL = S.EVENT_URL,
            T.STATUS = S.STATUS,
            T.RAW_TITLE = S.RAW_TITLE,
            T.LAST_COLLECTED_AT = CURRENT_TIMESTAMP(),
            T.UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            EVENT_KEY,
            SOURCE_EVENT_ID,
            INDICATOR_CODE,
            INDICATOR_NAME,
            CATEGORY,
            SOURCE,
            REFERENCE_PERIOD,
            RELEASE_AT_UTC,
            IMPORTANCE,
            EVENT_URL,
            STATUS,
            RAW_TITLE
        ) VALUES (
            S.EVENT_KEY,
            S.SOURCE_EVENT_ID,
            S.INDICATOR_CODE,
            S.INDICATOR_NAME,
            S.CATEGORY,
            S.SOURCE,
            S.REFERENCE_PERIOD,
            S.RELEASE_AT_UTC,
            S.IMPORTANCE,
            S.EVENT_URL,
            S.STATUS,
            S.RAW_TITLE
        )
    """

    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    connection = hook.get_conn()
    cursor = connection.cursor()
    affected_rows = 0

    try:
        connection.autocommit(False)
        for event in unique_events:
            parameters = (
                event["event_key"],
                event["source_event_id"],
                event["indicator_code"],
                event["indicator_name"],
                event["category"],
                event["source"],
                event.get("reference_period"),
                event["release_at_utc"],
                event["importance"],
                event.get("event_url"),
                event.get("status", "SCHEDULED"),
                event["raw_title"],
            )
            cursor.execute(merge_sql, parameters)
            affected_rows += max(cursor.rowcount, 0)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

    return {
        "received": len(received_events),
        "merged": len(unique_events),
        "affected_rows": affected_rows,
        "table": table_name,
    }
