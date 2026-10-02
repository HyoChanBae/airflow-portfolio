import os

import pendulum
from airflow.decorators import dag, task
from supabase import Client, create_client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_KEY must be set in the Airflow environment"
        )

    return create_client(
        SUPABASE_URL,
        SUPABASE_KEY,
    )


def test_connection() -> int:
    response = (
        get_supabase_client()
        .schema("public")
        .table("stock_master")
        .select("short_code")
        .limit(1)
        .execute()
    )
    return len(response.data)


@dag(
    dag_id="test_supabase_client",
    start_date=pendulum.datetime(
        2026, 1, 1,
        tz="Asia/Seoul"
    ),
    schedule=None,
    catchup=False,
    tags=["supabase", "connection-test"],
)
def test_supabase_client():

    @task
    def check_connection():
        row_count = test_connection()

        print(
            "Supabase stock_master connection OK: "
            f"returned_rows={row_count}"
        )

    check_connection()


test_supabase_client()
