import json

import pendulum
from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator


@dag(
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    schedule="0 8,17,20,22 * * *",
    catchup=False,
    tags=["macro"],
)
def macro_dag():

    run_agent = SimpleHttpOperator(
        task_id="run_macro_agent",
        http_conn_id="trading_agent_api",
        endpoint="macro-batch",
        method="POST",
        headers={
            "Content-Type": "application/json"
        },
        data=json.dumps({}),
        response_check=lambda response:
            response.status_code == 200,
    )


macro_dag()
