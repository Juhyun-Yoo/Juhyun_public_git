import pymysql
import yaml
import time
import main_api
import main_s
import kis_auth as ka
import pandas as pd
import requests
from datetime import datetime, timedelta, time as dt_time
from pytz import timezone
import warnings

warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")

# 🔹 config.yaml에서 DB 설정 읽기
with open("config//config.yaml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

# 🔹 DB 연결 함수
def connect_db():
    return pymysql.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"],
        database="usa_stock",
        port=config["port"],
        charset="utf8mb4",
        autocommit=True
    )

# 🔹 뉴욕 시간 설정
NYT = timezone("America/New_York")

# 🔹 데이터 리샘플링 간격 설정 (e.g. '15min', '5min', '1min')
RESAMPLE_INTERVAL = 15

# 🔹 API로부터 1분 데이터 가져오기
def get_minute_data(cnt, to=None):
    df = main_api.get_overseas_price_quot_inquire_time_itemchartprice(
        div="02", excd="AMS", itm_no="SOXL", nmin='1', pinc="0", nrec=str(cnt), keyb=to
    )
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"xymd":"date", "xhms":"time", "last":"close", "evol":"volume"})
    df['datetime'] = pd.to_datetime(df['date'] + df['time'].str.zfill(6), format='%Y%m%d%H%M%S')
    return df[['datetime', 'open', 'high', 'low', 'close', 'volume']].sort_values('datetime')


# 🔹 다량의 데이터를 한번에 가져오는 함수 (최대 2400개)
def min_massdata(min_interval='1', cnt=20):
    formatted_time = ""
    df_combined = pd.DataFrame()

    for i in range(cnt):
        if i == 0:
            rt_data = main_api.get_overseas_price_quot_inquire_time_itemchartprice(
                div="02", excd="AMS", itm_no="SOXL", nmin=min_interval, pinc="1"
            )
        else:
            rt_data = main_api.get_overseas_price_quot_inquire_time_itemchartprice(
                div="02", excd="AMS", itm_no="SOXL", nmin=min_interval, 
                pinc="1", next_value="1", keyb=formatted_time
            )

        df = rt_data.rename(columns={"xymd":"date", "xhms":"time", "last":"close", "evol":"volume"})
        df['datetime'] = pd.to_datetime(df['date'] + df['time'].str.zfill(6), format='%Y%m%d%H%M%S')
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']].sort_values('datetime')

        if df.empty:
            print("❌ 더 이상 가져올 데이터가 없습니다.")
            break

        formatted_time = df.iloc[0]['datetime'].strftime('%Y%m%d%H%M%S')

        # 기존 데이터와 새로운 데이터 합치기 (시간 순 정렬)
        df_combined = pd.concat([df_combined, df]).drop_duplicates(subset='datetime').sort_values('datetime')
        time.sleep(0.5)

    return df_combined


# 🔹 데이터 저장 (Bulk Insert 방식으로 성능 향상)
def save_to_db(df):
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO SOXL_minute_data (time, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), volume=VALUES(volume);
            """
            data_tuples = [
                (
                    row['datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                    row['open'], row['high'], row['low'], row['close'], row['volume']
                )
                for _, row in df.iterrows()
            ]
            cursor.executemany(sql, data_tuples)
        print(f"✅ {len(df)}건 데이터 DB 저장 완료")
    except Exception as e:
        print("❌ DB 저장 오류:", e)
    finally:
        conn.close()

# 🔹 DISCORD 알림 메시지 발송 로직 사용
def send_message(message):
    url = config['DISCORD_WEBHOOK_URL']
    data = {"content": message}
    response = requests.post(url, json=data)
    if response.status_code == 204:
        print(f"📢 DISCORD 메시지 전송 완료: {message}")
    else:
        print(f"❌ DISCORD 메시지 전송 실패: {response.status_code}, {response.text}")

# 🔹 데이터 분석
def data_analysis():
    conn = connect_db()
    try:
        df = pd.read_sql("SELECT * FROM SOXL_minute_data ORDER BY time DESC LIMIT 2400", conn)
        if not df.empty:
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time')

            aggregated_data = df.resample(RESAMPLE_INTERVAL, on='time').agg({
                'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
            }).dropna().reset_index()

            main_s.plot_candlestick(aggregated_data, show_rsi=False, show_macd=True, show_bollinger=False, show_volume=False, show_hedging_band=True)

    except Exception as e:
        print("❌ 분석 오류:", e)
    finally:
        conn.close()

# 🔹 개선된 데이터 분석 로직 (1분 단위 수 제한 추가)
# - resample_interval: 리샘플링 간격 (ex. 15분봉)
# - limit: 리샘플링된 봉의 최대 개수 (ex. 1200개 15분봉)
# - raw_limit: 원본 1분봉 데이터의 최대 개수 (ex. 최근 30000개 1분봉)
def data_analysis_improved(mode, resample_interval=15, limit=2400, raw_limit=2400):
    interval = resample_interval * 60

    sql = f"""
    SELECT
        interval_start,
        MIN(actual_time) AS actual_start_time,
        MAX(actual_time) AS actual_end_time,
        SUBSTRING_INDEX(GROUP_CONCAT(open ORDER BY actual_time ASC), ',', 1) AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY actual_time DESC), ',', 1) AS close,
        SUM(volume) AS volume
    FROM (
        SELECT
            FROM_UNIXTIME(UNIX_TIMESTAMP(`time`) - MOD(UNIX_TIMESTAMP(`time`), {interval})) AS interval_start,
            `time` AS actual_time,
            open, high, low, close, volume
        FROM (
            SELECT * FROM SOXL_minute_data
            ORDER BY time DESC
            LIMIT {raw_limit}
        ) AS recent_data
    ) AS sub
    GROUP BY interval_start
    ORDER BY interval_start DESC
    LIMIT {limit};
    """

    conn = connect_db()
    try:
        df = pd.read_sql(sql, conn)
        if not df.empty:
            df['interval_start'] = pd.to_datetime(df['interval_start'])
            df = df.sort_values('interval_start')
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

            df = main_s.plot_candlestick(
                df.rename(columns={'interval_start':'time'}),
                show_rsi=False, show_macd=True,
                show_bollinger=True, show_volume=False,
                show_hedging_band=True,
                mode = mode
            )
        if mode == 1 or mode == 2:
            signal = calculate_trading_signal(df)
            execute_trade(signal)
        else:
            None

    except Exception as e:
        print("❌ 분석 오류:", e)
    finally:
        conn.close()

# 🔹 마지막 저장 시간 조회
def get_last_time():
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT time FROM SOXL_minute_data ORDER BY time DESC LIMIT 1")
            result = cursor.fetchone()
            if result:
                return result[0]
    except Exception as e:
        print("❌ DB 조회 오류:", e)
    finally:
        conn.close()
    return None

def fill_missing_data():
    while True:
        last_time = get_last_time()
        now = datetime.now(NYT).replace(second=0, microsecond=0)

        if last_time is None:
            print("📌 DB 비어있음, 2400개 데이터 로딩 시작")
            df = min_massdata('1', cnt=20)
        else:
            last_time = pd.to_datetime(last_time)
            missing_minutes = int((now - NYT.localize(last_time)).total_seconds() // 60)

            if missing_minutes <= 0:
                print("📌 더 이상 누락된 데이터가 없습니다.")
                break
            else:
                print(f"📌 {missing_minutes}분 데이터 누락, 보완 로딩 시작")
                df = get_minute_data(min(missing_minutes, 200), to=None)

                if df.empty:
                    print("❌ 조회된 추가 데이터가 없습니다.")
                    break

                # last_time이 None이 아닌 경우만 필터링 처리
                df = df[df['datetime'] > last_time]

        if df.empty:
            print("📌 추가로 저장할 신규 데이터가 없습니다.")
            break

        save_to_db(df)
        time.sleep(1)  # API 호출 제한 방지

    print("✅ 모든 누락 데이터가 성공적으로 채워졌습니다.")

    
# 🔹 실시간 전략 시그널 계산 깡통
def calculate_trading_signal(df):
    if df.empty:
        print("❌ DataFrame이 비어있습니다.")
        return None

    try:
        # 현재 뉴욕 시간 (초, 마이크로초 제거)
        now = datetime.now(NYT).replace(second=0, microsecond=0)

        # 인덱스 기준으로 마지막 시간
        last_time = pd.to_datetime(df.index[-1])

        # 마지막 행
        last_row = df.iloc[-1]

        if last_time == now:
            if last_row.get('buy_signal', False):
                return "buy"
            elif last_row.get('sell_signal', False):
                return "sell"
    except Exception as e:
        print("❌ 시그널 계산 오류:", e)

    return None

# 🔹 정규장 시작/종료 감지 및 알림
market_open_sent = False
market_close_sent = False

def data_collection_thread(mode):
    global market_open_sent, market_close_sent
    while True:
        now = datetime.now(NYT)

        if dt_time(9, 30) <= now.time() < dt_time(16, 0):
            if not market_open_sent:
                send_message("🟢 정규장이 시작되었습니다.")
                market_open_sent = True
                market_close_sent = False

            if now.second == 3:
                df = get_minute_data(1)
                if not df.empty:
                    save_to_db(df)

                ####    데이터 분석     ####
                data_analysis_improved(mode, resample_interval=RESAMPLE_INTERVAL)

                
            time.sleep(0.5)

        elif now.time() >= dt_time(16, 0):
            if not market_close_sent:
                send_message("🔴 정규장이 종료되었습니다.")
                market_close_sent = True
                market_open_sent = False
            time.sleep(60)

        else:
            time.sleep(1)


# 🔹 자동 거래 주문 기능 깡통
def execute_trade(signal):
    if signal:
        main_api.get_overseas_inquire_present_balance(svr='vps',dv="02", dvsn="01", natn="000", mkt="00", inqr_dvsn="00")
        print(f"🚨 {signal} 신호 발생! 실제 거래 로직을 구현해주세요.")
    main_api.get_overseas_inquire_present_balance(svr='vps',dv="02", dvsn="01", natn="000", mkt="00", inqr_dvsn="00")
    print('A')

# 🔹 모드별 실행
def run_mode(mode):
    svr = 'my_prod' if mode == 1 else 'vps'
    ka.auth(svr)

    if mode in [1, 2]:
        fill_missing_data()
        data_collection_thread(mode)

    elif mode == 3:
        print("🧪 모드 3: 전략 개발 및 테스트 모드 실행")
        data_analysis_improved(mode, resample_interval=RESAMPLE_INTERVAL)

if __name__ == "__main__":
    mode = 3
    run_mode(mode)
