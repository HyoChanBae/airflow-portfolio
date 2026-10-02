import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="test_supabase_postgres",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["postgres", "supabase", "connection-test"],
)
def test_supabase_postgres():
    @task
    def test_connection() -> None:
        hook = PostgresHook(postgres_conn_id="supabase_postgres")

        with hook.get_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        1 AS connection_test,
                        CURRENT_DATABASE() AS database_name,
                        CURRENT_USER AS database_user,
                        CURRENT_TIMESTAMP AS server_time
                    """
                )
                result = cursor.fetchone()

        if result is None or result[0] != 1:
            raise RuntimeError("Supabase PostgreSQL connection test failed")

        print("Supabase PostgreSQL connection OK")
        print(f"Database: {result[1]}")
        print(f"User: {result[2]}")
        print(f"Server time: {result[3]}")

    test_connection()


test_supabase_postgres()
