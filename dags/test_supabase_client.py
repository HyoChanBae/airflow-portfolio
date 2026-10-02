import os

import pendulum
import requests
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
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/"

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=15,
    )

    print("SUPABASE_URL =", SUPABASE_URL)
    print("KEY_PREFIX =", SUPABASE_KEY[:15] if SUPABASE_KEY else None)
    print("STATUS =", response.status_code)
    print("BODY =", response.text[:1000])

    response.raise_for_status()

    return response.status_code


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
        status_code = test_connection()

        print(
            f"Supabase connection OK: "
            f"status_code={status_code}"
        )

    check_connection()


test_supabase_client()