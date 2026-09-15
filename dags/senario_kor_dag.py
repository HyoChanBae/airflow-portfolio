import pendulum
from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator

from datetime import datetime


@dag(
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    schedule="30 9,11,14 * * *",
    catchup=False,
    tags=["senario_trade"]
)
def senario_kor_dag():

    run_agent = SimpleHttpOperator(
        task_id="trade_kor_agent",

        http_conn_id="trading_agent_api",

        endpoint="senario-batch-kor",

        method="POST",

        headers={
            "Content-Type": "application/json"
        },


        response_check=lambda response:
            response.status_code == 200
    )

senario_kor_dag()
