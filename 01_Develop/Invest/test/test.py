import pymysql
import yaml
import time
import threading
import test_api
import test_s
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
RESAMPLE_INTERVAL = '15min'

# 🔹 API로부터 데이터 받아오기
def get_minute_data(cnt, to=None):
    df = test_api.get_overseas_price_quot_inquire_time_itemchartprice(
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
            rt_data = test_api.get_overseas_price_quot_inquire_time_itemchartprice(
                div="02", excd="AMS", itm_no="SOXL", nmin=min_interval, pinc="1"
            )
        else:
            rt_data = test_api.get_overseas_price_quot_inquire_time_itemchartprice(
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

        # 기존 데이터와 새로운 데이터 합치기
        df_combined = pd.concat([df_combined, df]).drop_duplicates(subset='datetime')
        time.sleep(0.5)

    return df_combined


# 🔹 DB에 데이터 저장
def save_to_db(df):
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            for idx, row in df.iterrows():
                sql = """
                INSERT INTO SOXL_minute_data (time, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), volume=VALUES(volume);
                """
                cursor.execute(sql, (
                    row['datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                    row['open'], row['high'], row['low'], row['close'], row['volume']
                ))
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

# 🔹 실제 주문 로직 깡통
def execute_trade(signal):
    send_message(f"{signal} 신호 발생! 실제 주문 로직을 구현해주세요.")

# 🔹 데이터 분석
def data_analysis():
    conn = connect_db()
    try:
        df = pd.read_sql("SELECT * FROM SOXL_minute_data ORDER BY time DESC LIMIT 1500", conn)
        if not df.empty:
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time')

            aggregated_data = df.resample(RESAMPLE_INTERVAL, on='time').agg({
                'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
            }).dropna().reset_index()

            test_s.plot_candlestick(aggregated_data, show_rsi=False, show_macd=True, show_bollinger=True, show_volume=False)

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

# 🔹 누락 데이터 채우기 (개선 버전)
def fill_missing_data():
    while True:
        last_time = get_last_time()
        now = datetime.now(NYT).replace(second=0, microsecond=0)

        if last_time is None:
            print("📌 DB 비어있음, 2400개 데이터 로딩 시작")
            df = min_massdata('1', cnt=20)
        else:
            last_time = NYT.localize(pd.to_datetime(last_time))
            missing_minutes = int((now - last_time).total_seconds() // 60)

            if missing_minutes <= 0:
                print("📌 더 이상 누락된 데이터가 없습니다.")
                break  # 반복 종료
            else:
                print(f"📌 {missing_minutes}분 데이터 누락, 보완 로딩 시작")
                df = get_minute_data(min(missing_minutes, 200))  # 최대 200개씩 점진적으로 보완

        if df.empty:
            print("❌ 조회된 추가 데이터가 없습니다.")
            break

        save_to_db(df)
        time.sleep(1)  # API 호출 제한 방지

    print("✅ 모든 누락 데이터가 성공적으로 채워졌습니다.")

# 🔹 데이터 수집 스레드
def data_collection_thread():
    while True:
        now = datetime.now(NYT)
        if dt_time(9, 20) <= now.time() < dt_time(16, 0):
            if now.second == 3:
                df = get_minute_data(1)
                if not df.empty:
                    save_to_db(df)
                time.sleep(1)
            time.sleep(0.5)
        else:
            time.sleep(60)

# 🔹 데이터 분석 스레드
def data_analysis_thread():
    while True:
        now = datetime.now(NYT)
        if dt_time(9, 30) <= now.time() <= dt_time(16, 0):
            data_analysis()
        time.sleep(60)

# 🔹 모드별 실행
def run_mode(mode):
    svr = 'my_prod' if mode == 1 else 'vps'
    ka.auth(svr)

    fill_missing_data()

    if mode in [1, 2]:
        threading.Thread(target=data_collection_thread, daemon=True).start()
        threading.Thread(target=data_analysis_thread, daemon=True).start()
        while True:
            time.sleep(1)

    elif mode == 3:
        print("🧪 모드 3: 전략 개발 및 테스트 모드 실행")
        data_analysis()

if __name__ == "__main__":
    mode = 2
    run_mode(mode)
