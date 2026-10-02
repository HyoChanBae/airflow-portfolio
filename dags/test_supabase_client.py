import os

import pendulum
import requests
from airflow.decorators import dag, task
from dotenv import load_dotenv
from supabase import Client, create_client


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_CLIENT: Client | None = (
    create_client(SUPABASE_URL, SUPABASE_KEY)
    if SUPABASE_URL and SUPABASE_KEY
    else None
)


def get_supabase_client() -> Client:
    if SUPABASE_CLIENT is None:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_KEY must be set in the Airflow environment"
        )
    return SUPABASE_CLIENT


def test_connection() -> int:
    get_supabase_client()
    response = requests.get(
        f"{SUPABASE_URL.rstrip('/')}/rest/v1/",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        },
        timeout=15,
    )
    response.raise_for_status()
    return response.status_code


@dag(
    dag_id="test_supabase_client",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["supabase", "connection-test"],
)
def test_supabase_client():
    @task
    def check_connection() -> None:
        status_code = test_connection()
        print(f"Supabase connection OK: status_code={status_code}")

    check_connection()


test_supabase_client()
