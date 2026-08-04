from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mysql", "snowflake"],
)
def mysql_to_snowflake_customers():

    @task
    def extract_from_mysql():
        """1. MySQL에서 customers 테이블 데이터 Extract"""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")
        
        sql = "SELECT * FROM customers"
        
        with mysql_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                print(f"Extracted {len(rows)} rows from MySQL.")
                return rows

    @task
    def load_to_snowflake(rows):
        """2. Snowflake의 CUSTOMERS 테이블로 Load"""
        if not rows:
            print("No data to load.")
            return

        sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        
        # Snowflake의 target 테이블명 (Database.Schema.Table)
        target_table = "DEMO_RAW_DB.RAW.CUSTOMERS"
        
        # executemany 방식으로 튜플/리스트 데이터를 한 번에 Insert
        with sf_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # 테이블 컬럼 개수/순서에 맞게 %s 개수를 맞춰주세요 (예: 컬럼이 5개인 경우)
                # target 테이블의 컬럼 수를 파악하여 %s 개수를 맞춰주시면 됩니다.
                placeholders = ",".join(["%s"] * len(rows[0]))
                insert_sql = f"INSERT INTO {target_table} VALUES ({placeholders})"
                
                cursor.executemany(insert_sql, rows)
                print(f"Successfully inserted {len(rows)} rows into Snowflake.")

    # Task 실행 흐름 정의
    customer_data = extract_from_mysql()
    load_to_snowflake(customer_data)


mysql_to_snowflake_customers()