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
        """MySQL에서 테이블 스키마(컬럼명/타입)와 데이터 동시 추출"""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")
        
        sql = "SELECT * FROM customers"
        
        with mysql_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                # 컬럼명 추출
                columns = [desc[0] for desc in cursor.description]
                
                print(f"Extracted {len(rows)} rows from MySQL with columns: {columns}")
                return {"columns": columns, "rows": rows}

    @task
    def load_to_snowflake(data):
        """Snowflake에 테이블이 없으면 자동 생성 후 데이터 Load"""
        rows = data["rows"]
        columns = data["columns"]
        
        if not rows:
            print("No data to load.")
            return

        sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        target_table = "DEMO_RAW_DB.RAW.CUSTOMERS"
        
        with sf_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # 1. 테이블이 없을 경우를 대비해 CREATE TABLE IF NOT EXISTS 실행
                # Snowflake에서는 VARCHAR 타입으로 유연하게 받아두는 것이 가장 안전합니다.
                cols_schema = ", ".join([f'"{col.upper()}" VARCHAR' for col in columns])
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ({cols_schema})"
                
                print(f"Executing: {create_table_sql}")
                cursor.execute(create_table_sql)
                
                # 2. Insert 구문 생성 및 실행
                placeholders = ", ".join(["%s"] * len(columns))
                col_names = ", ".join([f'"{col.upper()}"' for col in columns])
                insert_sql = f"INSERT INTO {target_table} ({col_names}) VALUES ({placeholders})"
                
                cursor.executemany(insert_sql, rows)
                print(f"Successfully loaded {len(rows)} rows into {target_table}.")

    # Task 흐름 연결
    extracted_data = extract_from_mysql()
    load_to_snowflake(extracted_data)


mysql_to_snowflake_customers()