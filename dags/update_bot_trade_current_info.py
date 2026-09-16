import yfinance as yf
import pendulum

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_yfinance_symbol_and_ticker(symbol: str):
    """
    DB의 SYMBOL을 받아서 Yahoo Finance용 symbol과 ticker 객체 반환

    미국 종목:
        AAPL -> AAPL

    한국 종목:
        005930 -> 005930.KS 먼저 시도
        실패하면 005930.KQ 시도
    """

    symbol = str(symbol).strip()

    # ==========================================
    # 이미 Yahoo Finance 형식인 경우
    # ==========================================
    if symbol.endswith(".KS") or symbol.endswith(".KQ"):
        ticker = yf.Ticker(symbol)

        try:
            price = ticker.fast_info["last_price"]

            if price is not None:
                return symbol, ticker

        except Exception:
            pass

        raise ValueError(
            f"Yahoo Finance에서 종목을 찾을 수 없습니다: {symbol}"
        )

    # ==========================================
    # 한국 종목
    # 6자리 숫자 종목코드
    # ==========================================
    if symbol.isdigit() and len(symbol) == 6:

        # --------------------------------------
        # 1. KOSPI 시도
        # --------------------------------------
        ks_symbol = f"{symbol}.KS"

        try:
            ks_ticker = yf.Ticker(ks_symbol)

            ks_price = ks_ticker.fast_info["last_price"]

            if ks_price is not None:
                print(
                    f"[MARKET DETECT] "
                    f"{symbol} -> KOSPI ({ks_symbol})"
                )

                return ks_symbol, ks_ticker

        except Exception as e:
            print(
                f"[KOSPI CHECK FAIL] "
                f"{ks_symbol} / {e}"
            )

        # --------------------------------------
        # 2. KOSDAQ 시도
        # --------------------------------------
        kq_symbol = f"{symbol}.KQ"

        try:
            kq_ticker = yf.Ticker(kq_symbol)

            kq_price = kq_ticker.fast_info["last_price"]

            if kq_price is not None:
                print(
                    f"[MARKET DETECT] "
                    f"{symbol} -> KOSDAQ ({kq_symbol})"
                )

                return kq_symbol, kq_ticker

        except Exception as e:
            print(
                f"[KOSDAQ CHECK FAIL] "
                f"{kq_symbol} / {e}"
            )

        raise ValueError(
            f"KOSPI/KOSDAQ 모두 조회 실패: {symbol}"
        )

    # ==========================================
    # 미국 / 해외 종목
    # ==========================================
    ticker = yf.Ticker(symbol)

    try:
        price = ticker.fast_info["last_price"]

        if price is not None:
            return symbol, ticker

    except Exception as e:
        raise ValueError(
            f"Yahoo Finance 조회 실패: {symbol} / {e}"
        )

    raise ValueError(
        f"현재가를 가져올 수 없습니다: {symbol}"
    )

    # 지정 시간 별 0은 분 schedule="0 8,17,20 * * *",
    # 2시간 마다 schedule="0 */2 * * *",
@dag(
    dag_id="update_bot_trade_current_info",
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="Asia/Seoul"
    ),
    schedule="0 */2 * * 1-6", #2시간마다 일요일 제외
    catchup=False,
    tags=[
        "stock",
        "yfinance",
        "snowflake"
    ]
)
def update_bot_trade_current_info():

    @task
    def update_current_info():

        # ==========================================
        # Snowflake 연결
        # ==========================================
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn2"
        )

        # ==========================================
        # BOT_TRADE에서 종목 목록 조회
        # ==========================================
        rows = hook.get_records(
            """
            SELECT * FROM (
                SELECT DISTINCT SYMBOL
                FROM DEMO_RAW_DB.RAW.BOT_TRADE
                WHERE SYMBOL IS NOT NULL
                UNION ALL 
                SELECT DISTINCT SYMBOL
                FROM DEMO_RAW_DB.RAW.BOT_TRADE_SENARIO
                WHERE SYMBOL IS NOT NULL    
			)
			GROUP BY SYMBOL
            """
        )

        symbols = [
            str(row[0]).strip()
            for row in rows
            if row[0] is not None
        ]

        print("==========================================")
        print("BOT_TRADE SYMBOL 조회")
        print(symbols)
        print(f"총 종목 수: {len(symbols)}")
        print("==========================================")

        success_count = 0
        fail_count = 0

        # ==========================================
        # 종목별 Yahoo Finance 조회
        # ==========================================
        for symbol in symbols:

            try:

                print("")
                print("------------------------------------------")
                print(f"[START] SYMBOL = {symbol}")

                # ==================================
                # Yahoo Finance Symbol 자동 판별
                # ==================================
                yf_symbol, ticker = (
                    get_yfinance_symbol_and_ticker(
                        symbol
                    )
                )

                print(
                    f"[YFINANCE SYMBOL] "
                    f"{symbol} -> {yf_symbol}"
                )

                # ==================================
                # 종목명
                # ==================================
                try:

                    info = ticker.info

                    symbol_name = (
                        info.get("shortName")
                        or info.get("longName")
                        or symbol
                    )

                except Exception as e:

                    print(
                        f"[WARNING] "
                        f"종목명 조회 실패: "
                        f"{symbol} / {e}"
                    )

                    symbol_name = symbol

                # ==================================
                # 현재가
                # ==================================
                current_price = (
                    ticker.fast_info["last_price"]
                )

                if current_price is None:
                    raise ValueError(
                        "현재가가 None입니다."
                    )

                current_price = float(
                    current_price
                )

                print(
                    f"[SYMBOL]        {symbol}"
                )

                print(
                    f"[YF SYMBOL]     {yf_symbol}"
                )

                print(
                    f"[SYMBOL NAME]   {symbol_name}"
                )

                print(
                    f"[CURRENT PRICE] {current_price}"
                )

                # ==================================
                # Snowflake MERGE
                # ==================================
                sql = """
                    MERGE INTO
                        DEMO_RAW_DB.RAW.BOT_TRADE_CURRENT_INFO T

                    USING (
                        SELECT
                            %s AS SYMBOL,
                            %s AS SYMBOL_NAME,
                            %s AS CURRENT_PRICE
                    ) S

                    ON T.SYMBOL = S.SYMBOL

                    WHEN MATCHED THEN
                        UPDATE SET
                            T.SYMBOL_NAME =
                                S.SYMBOL_NAME,

                            T.CURRENT_PRICE =
                                S.CURRENT_PRICE,

                            T.UPDATE_DT =
                                CONVERT_TIMEZONE(
                                    'Asia/Seoul',
                                    CURRENT_TIMESTAMP()
                                )::TIMESTAMP_NTZ

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
                            CONVERT_TIMEZONE(
                                'Asia/Seoul',
                                CURRENT_TIMESTAMP()
                            )::TIMESTAMP_NTZ,
                            CONVERT_TIMEZONE(
                                'Asia/Seoul',
                                CURRENT_TIMESTAMP()
                            )::TIMESTAMP_NTZ
                        )
                """

                hook.run(
                    sql,
                    parameters=(
                        symbol,
                        symbol_name,
                        current_price
                    )
                )

                success_count += 1

                print(
                    f"[SUCCESS] "
                    f"{symbol} / "
                    f"{yf_symbol} / "
                    f"{symbol_name} / "
                    f"{current_price}"
                )

            except Exception as e:

                fail_count += 1

                print(
                    f"[ERROR] "
                    f"{symbol} 처리 실패: {e}"
                )

        # ==========================================
        # 최종 결과
        # ==========================================
        print("")
        print("==========================================")
        print("BOT_TRADE_CURRENT_INFO 업데이트 완료")
        print(f"성공: {success_count}")
        print(f"실패: {fail_count}")
        print("==========================================")


    update_current_info()


update_bot_trade_current_info()