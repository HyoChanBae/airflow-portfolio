import pendulum
from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator


@dag(
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    schedule="30 9,11,14,23,01,04 * * *",
    catchup=False,
    tags=["senario_trade"],
)
def senario_buy4_dag():

    run_agent = SimpleHttpOperator(
        task_id="senario_batch_buy4",
        http_conn_id="trading_agent_api",
        endpoint="senario-batch-buy4",
        method="POST",
        headers={
            "Content-Type": "application/json"
        },
        response_check=lambda response: response.status_code == 200,
    )


senario_buy4_dag()
