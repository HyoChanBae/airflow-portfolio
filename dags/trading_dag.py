import json  # 1. json 모듈 추가
from datetime import datetime
from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator


@dag(
    start_date=datetime(2026, 8, 1),
    schedule="0 9 * * 1-5",
    catchup=False,
    tags=["trading"],
)
def trading_dag():

    run_agent = SimpleHttpOperator(
        task_id="run_trading_agent",
        http_conn_id="trading_agent_api",
        endpoint="run-batch",
        method="POST",
        headers={"Content-Type": "application/json"},
        # 2. json.dumps()로 감싸서 JSON 문자열로 변환하여 전송
        data=json.dumps({"symbols": ["005930", "000660"]}),
        response_check=lambda response: response.status_code == 200,
    )


trading_dag()
