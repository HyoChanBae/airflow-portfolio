from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator

from datetime import datetime


@dag(
    start_date=datetime(2026, 8, 1),
    schedule="0 9 * * 1-5",
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