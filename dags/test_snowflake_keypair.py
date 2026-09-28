from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


def test_snowflake():
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_conn3"
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA()
        """)

        result = cursor.fetchone()

        print("CURRENT_USER      :", result[0])
        print("CURRENT_ROLE      :", result[1])
        print("CURRENT_WAREHOUSE :", result[2])
        print("CURRENT_DATABASE  :", result[3])
        print("CURRENT_SCHEMA    :", result[4])

    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="test_snowflake_keypair",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "test"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_connection",
        python_callable=test_snowflake,
    )
