from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from macro_calendar.collectors.bls import collect_bls_events
from macro_calendar.repository import DEFAULT_TABLE, merge_calendar_events


@dag(
    dag_id="macro_calendar_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="0 7,19 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["macro", "calendar", "snowflake"],
)
def macro_calendar_dag():
    @task
    def collect_bls() -> list[dict]:
        user_agent = Variable.get(
            "macro_calendar_bls_user_agent",
            default_var="airflow-macro-calendar/1.0",
        )
        return collect_bls_events(user_agent=user_agent)

    @task
    def merge_into_snowflake(events: list[dict]) -> dict:
        table_name = Variable.get(
            "macro_calendar_snowflake_table",
            default_var=DEFAULT_TABLE,
        )
        return merge_calendar_events(events=events, table_name=table_name)

    @task
    def validate_result(result: dict) -> None:
        if result["merged"] <= 0:
            raise ValueError("No BLS calendar events were merged")
        print(
            "Macro calendar merge completed: "
            f"received={result['received']}, "
            f"merged={result['merged']}, "
            f"affected_rows={result['affected_rows']}, "
            f"table={result['table']}"
        )

    validate_result(merge_into_snowflake(collect_bls()))


macro_calendar_dag()
