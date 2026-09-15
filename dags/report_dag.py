import pendulum
from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator

from datetime import datetime


@dag(
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    #schedule="0 9 * * 1-5",
    #schedule="*/10 * * * *",
    schedule="0 8,17,20 * * *",
    catchup=False,
    tags=["report"]
)
def report_dag():

    run_agent = SimpleHttpOperator(
        task_id="run_report_agent",

        http_conn_id="trading_agent_api",

        endpoint="report-batch",

        method="POST",

        headers={
            "Content-Type": "application/json"
        },


        response_check=lambda response:
            response.status_code == 200
    )


report_dag()
