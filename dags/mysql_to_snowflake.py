from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["connection-test"],
)
def connection_test():

    @task
    def test_mysql():
        hook = MySqlHook(mysql_conn_id="mysql_test")
        conn = hook.get_conn()

        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        print(f"MySQL connection OK: {result}")

    @task
    def test_snowflake():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_test")
        conn = hook.get_conn()

        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()

        print(f"Snowflake connection OK: {result}")


    test_mysql() >> test_snowflake()


connection_test()