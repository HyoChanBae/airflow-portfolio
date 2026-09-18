import yfinance as yf
import pendulum

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def is_korean_symbol(symbol: str) -> bool:
    """6자리 숫자 종목코드 또는 Yahoo 한국 거래소 접미사."""
    symbol = str(symbol).strip().upper()

    if symbol.endswith(".KS") or symbol.endswith(".KQ"):
        return True

    return symbol.isdigit() and len(symbol) == 6


def normalize_short_code(code: str) -> str:
    """
    STOCK_MASTER.SHORT_CODE / BOT_TRADE.SYMBOL 을
    6자리 종목코드로 정규화.

    예:
        067630, A067630, 067630.KS, 67630 -> 067630
    """
    code = str(code).strip().upper()

    if code.endswith(".KS") or code.endswith(".KQ"):
        code = code.rsplit(".", 1)[0]

    if code.startswith("A") and len(code) == 7 and code[1:].isdigit():
        code = code[1:]

    if code.isdigit():
        return code.zfill(6)

    return code


def load_stock_name_map(hook: SnowflakeHook) -> dict:
    """SHORT_CODE -> STOCK_NAME 맵."""
    rows = hook.get_records(
        """
        SELECT
            SHORT_CODE,
            STOCK_NAME
        FROM DEMO_RAW_DB.RAW.STOCK_MASTER
        WHERE SHORT_CODE IS NOT NULL
          AND STOCK_NAME IS NOT NULL
        """
    )

    stock_name_map = {}

    for short_code, stock_name in rows:
        if short_code is None or stock_name is None:
            continue

        key = normalize_short_code(short_code)
        name = str(stock_name).strip()

        if key and name:
            stock_name_map[key] = name

    print("==========================================")
    print("STOCK_MASTER 종목명 맵 로드")
    print(f"총 종목 수: {len(stock_name_map)}")
    print("==========================================")

    return stock_name_map


def get_yfinance_symbol_name(ticker, symbol: str) -> str:
    """미국/해외 종목용 Yahoo Finance 종목명. 식별자 나열 값은 버린다."""
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
        return symbol

    if not symbol_name:
        return symbol

    symbol_name = str(symbol_name).strip()

    # Yahoo가 회사명 대신 "067630.KS,0P0000N5S4,872655"
    # 처럼 식별자를 콤마로 이어 붙인 값을 주는 경우가 있다.
    if "," in symbol_name or symbol_name.upper().startswith(
        str(symbol).strip().upper()
    ):
        print(
            f"[WARNING] "
            f"Yahoo 종목명이 식별자 형식입니다: "
            f"{symbol_name}"
        )
        return symbol

    return symbol_name


def get_valid_kr_equity_ticker(yf_symbol: str):
    """
    Yahoo 한국 티커가 실제 주식인지 확인.

    last_price 만 보면 067630.KS 처럼
    MUTUALFUND 오염 시세(11250)도 통과한다.
    """
    ticker = yf.Ticker(yf_symbol)

    try:
        fast_info = ticker.fast_info
        price = fast_info["last_price"]
    except Exception as e:
        print(
            f"[YF CHECK FAIL] "
            f"{yf_symbol} / {e}"
        )
        return None

    if price is None:
        print(
            f"[YF CHECK FAIL] "
            f"{yf_symbol} / last_price is None"
        )
        return None

    try:
        quote_type = str(
            fast_info["quote_type"] or ""
        ).upper()
    except Exception:
        quote_type = ""

    if quote_type and quote_type != "EQUITY":
        print(
            f"[INVALID QUOTE] "
            f"{yf_symbol} "
            f"quote_type={quote_type} "
            f"last_price={price}"
        )
        return None

    return ticker


def get_yfinance_symbol_and_ticker(symbol: str):
    """
    DB의 SYMBOL을 받아서 Yahoo Finance용 symbol과 ticker 객체 반환

    미국 종목:
        AAPL -> AAPL

    한국 종목:
        005930 -> 005930.KS 먼저 시도
        유효한 주식이 아니면 005930.KQ 시도
    """

    symbol = str(symbol).strip()

    # ==========================================
    # 이미 Yahoo Finance 형식인 경우
    # ==========================================
    if symbol.endswith(".KS") or symbol.endswith(".KQ"):
        ticker = get_valid_kr_equity_ticker(symbol)

        if ticker is not None:
            return symbol, ticker

        alt_suffix = (
            ".KQ" if symbol.endswith(".KS") else ".KS"
        )
        alt_symbol = (
            f"{symbol.rsplit('.', 1)[0]}{alt_suffix}"
        )
        alt_ticker = get_valid_kr_equity_ticker(
            alt_symbol
        )

        if alt_ticker is not None:
            print(
                f"[MARKET FALLBACK] "
                f"{symbol} -> {alt_symbol}"
            )
            return alt_symbol, alt_ticker

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
        ks_ticker = get_valid_kr_equity_ticker(
            ks_symbol
        )

        if ks_ticker is not None:
            print(
                f"[MARKET DETECT] "
                f"{symbol} -> KOSPI ({ks_symbol})"
            )
            return ks_symbol, ks_ticker

        print(
            f"[KOSPI CHECK FAIL] "
            f"{ks_symbol} -> KOSDAQ 시도"
        )

        # --------------------------------------
        # 2. KOSDAQ 시도
        # --------------------------------------
        kq_symbol = f"{symbol}.KQ"
        kq_ticker = get_valid_kr_equity_ticker(
            kq_symbol
        )

        if kq_ticker is not None:
            print(
                f"[MARKET DETECT] "
                f"{symbol} -> KOSDAQ ({kq_symbol})"
            )
            return kq_symbol, kq_ticker

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
    schedule="*/30 * * * 1-6", #30분마다 일요일 제외
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

        stock_name_map = load_stock_name_map(hook)

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
                # 국내: STOCK_MASTER.STOCK_NAME
                # 해외: Yahoo Finance shortName
                # ==================================
                if is_korean_symbol(symbol):
                    kr_code = normalize_short_code(symbol)
                    symbol_name = stock_name_map.get(kr_code)

                    if symbol_name:
                        print(
                            f"[STOCK MASTER] "
                            f"{kr_code} -> {symbol_name}"
                        )
                    else:
                        print(
                            f"[WARNING] "
                            f"STOCK_MASTER에 없음: {kr_code}"
                        )
                        symbol_name = get_yfinance_symbol_name(
                            ticker,
                            symbol
                        )
                else:
                    symbol_name = get_yfinance_symbol_name(
                        ticker,
                        symbol
                    )

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
