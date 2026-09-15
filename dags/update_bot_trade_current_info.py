import yfinance as yf

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum


@dag(
    dag_id="update_bot_trade_current_info",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Seoul"),
    schedule="0 8,17,20 * * *",
    catchup=False,
)
def update_bot_trade_current_info():

    @task
    def update_current_info():

        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn2"
        )

        # 1. BOT_TRADE에서 종목코드 조회
        rows = hook.get_records("""
            SELECT DISTINCT SYMBOL
            FROM DEMO_RAW_DB.RAW.BOT_TRADE
            WHERE SYMBOL IS NOT NULL
        """)

        symbols = [row[0] for row in rows]

        for symbol in symbols:

            try:
                ticker = yf.Ticker(symbol)

                # 종목명
                info = ticker.info
                symbol_name = (
                    info.get("shortName")
                    or info.get("longName")
                    or symbol
                )

                # 현재가
                current_price = ticker.fast_info["last_price"]

                # 2. Snowflake MERGE
                sql = """
                    MERGE INTO DEMO_RAW_DB.RAW.BOT_TRADE_CURRENT_INFO T
                    USING (
                        SELECT
                            %s AS SYMBOL,
                            %s AS SYMBOL_NAME,
                            %s AS CURRENT_PRICE
                    ) S
                    ON T.SYMBOL = S.SYMBOL

                    WHEN MATCHED THEN
                        UPDATE SET
                            T.SYMBOL_NAME = S.SYMBOL_NAME,
                            T.CURRENT_PRICE = S.CURRENT_PRICE,
                            T.UPDATE_DT = CURRENT_TIMESTAMP()

                    WHEN NOT MATCHED THEN
                        INSERT (
                            SYMBOL,
                            SYMBOL_NAME,
                            CURRENT_PRICE,
                            UPDATE_DT,
                            CREATE_DT
                        )
                        VALUES (
                            S.SYMBOL,
                            S.SYMBOL_NAME,
                            S.CURRENT_PRICE,
                            CURRENT_TIMESTAMP(),
                            CURRENT_TIMESTAMP()
                        )
                """

                hook.run(
                    sql,
                    parameters=(
                        symbol,
                        symbol_name,
                        float(current_price)
                    )
                )

                print(
                    f"{symbol} / "
                    f"{symbol_name} / "
                    f"{current_price}"
                )

            except Exception as e:
                print(
                    f"{symbol} 처리 실패: {e}"
                )

    update_current_info()


update_bot_trade_current_info()
