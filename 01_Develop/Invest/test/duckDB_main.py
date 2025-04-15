import duckdb
import yaml
import time
import main_api
import main_s
import kis_auth as ka
import pandas as pd
import requests
from datetime import datetime, timedelta, time as dt_time
from pytz import timezone
import os
import warnings
import math

warnings.filterwarnings("ignore")

# DuckDB 경로
DUCKDB_PATH = os.path.expanduser("G:/내 드라이브/trading/soxl.duckdb")
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

# 시간대
NYT = timezone("America/New_York")
RESAMPLE_INTERVAL = 15  # 15분봉

# config.yaml 불러오기
with open("config/config.yaml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

# DB 연결
def connect_db():
    return duckdb.connect(DUCKDB_PATH)

# DuckDB 초기 테이블 생성
def init_duckdb_schema():
    con = connect_db()
    con.execute("""
    CREATE TABLE IF NOT EXISTS SOXL_minute_data (
        time TIMESTAMP PRIMARY KEY,
        open DECIMAL(18,8),
        high DECIMAL(18,8),
        low DECIMAL(18,8),
        close DECIMAL(18,8),
        volume DECIMAL(18,8)
    )
    """)
    con.close()

# 1분 데이터 수집
def get_minute_data(cnt, to=None):
    df = main_api.get_overseas_price_quot_inquire_time_itemchartprice(
        div="02", excd="AMS", itm_no="SOXL", nmin='1', pinc="0", nrec=str(cnt), keyb=to
    )
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"xymd": "date", "xhms": "time", "last": "close", "evol": "volume"})
    df['datetime'] = pd.to_datetime(df['date'] + df['time'].str.zfill(6), format='%Y%m%d%H%M%S')
    return df[['datetime', 'open', 'high', 'low', 'close', 'volume']].sort_values('datetime')

# 다량의 데이터를 한번에 가져오는 함수 (최대 2400개)
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

        df_combined = pd.concat([df_combined, df]).drop_duplicates(subset='datetime').sort_values('datetime')
        time.sleep(0.5)

    return df_combined

def fetch_missing_data(missing_minutes, min_interval='1'):
    formatted_time = ""
    df_combined = pd.DataFrame()
    max_per_call = 120  # ✅ API가 한 번에 가져올 수 있는 최대 분량
    required_calls = math.ceil(missing_minutes / max_per_call)

    for i in range(required_calls):
        if df_combined.empty:
            rt_data = main_api.get_overseas_price_quot_inquire_time_itemchartprice(
                div="02", excd="AMS", itm_no="SOXL", nmin=min_interval, pinc="1"
            )
        else:
            rt_data = main_api.get_overseas_price_quot_inquire_time_itemchartprice(
                div="02", excd="AMS", itm_no="SOXL", nmin=min_interval, 
                pinc="1", next_value="1", keyb=formatted_time
            )

        df = rt_data.rename(columns={"xymd": "date", "xhms": "time", "last": "close", "evol": "volume"})
        df['datetime'] = pd.to_datetime(df['date'] + df['time'].str.zfill(6), format='%Y%m%d%H%M%S')
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']].sort_values('datetime')

        if df.empty:
            print("❌ 더 이상 가져올 데이터가 없습니다.")
            break

        formatted_time = df.iloc[0]['datetime'].strftime('%Y%m%d%H%M%S')

        before_count = len(df_combined)
        df_combined = pd.concat([df_combined, df]).drop_duplicates(subset='datetime').sort_values('datetime')
        collected = len(df_combined)
        newly_added = collected - before_count

        print(f"📥 {newly_added}건 수집 (누적 {collected}/{missing_minutes})")
        time.sleep(0.5)

    return df_combined

# DuckDB 저장
def save_to_db(df):
    if df.empty:
        return
    con = connect_db()
    try:
        con.executemany("""
            INSERT OR REPLACE INTO SOXL_minute_data (time, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            (
                row['datetime'].to_pydatetime(),
                row['open'], row['high'], row['low'], row['close'], row['volume']
            ) for _, row in df.iterrows()
        ])
        print(f"✅ {len(df)}건 DuckDB 저장 완료")
    except Exception as e:
        print("❌ DuckDB 저장 오류:", e)
    finally:
        con.close()

# 마지막 저장 시간 조회
def get_last_time():
    con = connect_db()
    try:
        result = con.execute("SELECT MAX(time) FROM SOXL_minute_data").fetchone()
        return result[0] if result else None
    except Exception as e:
        print("❌ DuckDB 조회 오류:", e)
    finally:
        con.close()

def fill_missing_data():
    while True:
        last_time = get_last_time()
        now = datetime.now(NYT).replace(second=0, microsecond=0)
        print(f"📌 [디버그] DB 기준 마지막 저장 시각: {last_time}")

        if last_time is None:
            print("📌 DB 비어있음, 데이터 로딩 시작")
            df = fetch_missing_data(2400)
        else:
            last_time = pd.to_datetime(last_time)
            missing_minutes = int((now - NYT.localize(last_time)).total_seconds() // 60)

            if missing_minutes <= 0:
                print("📌 누락 데이터 없음")
                break
            else:
                print(f"📌 {missing_minutes}분 필요 → 보완 시작")
                df = fetch_missing_data(missing_minutes)
                print(f"📌 [디버그] fetch_missing_data() 수집된 총 데이터 수: {len(df)}")

                df = df[df['datetime'] > last_time]
                print(f"📌 [디버그] last_time 이후 데이터 수: {len(df)}")

        if df.empty:
            print("📌 추가 데이터 없음")
            break

        save_to_db(df)
        time.sleep(1)


# 개선된 데이터 분석 로직
def data_analysis_improved(mode, resample_interval=15, limit=2400, raw_limit=2400):
    con = connect_db()
    try:
        df = con.execute(f"""
            SELECT time AS time, open, high, low, close, volume FROM (
                SELECT * FROM SOXL_minute_data
                ORDER BY time DESC
                LIMIT {raw_limit}
            )
            ORDER BY time ASC
        """).df()

        if not df.empty:
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time')

            df_resampled = df.resample(f'{resample_interval}min', on='time').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna().reset_index()

            if mode == 3:
                df_resampled.to_csv('data.csv')

            df_resampled = main_s.plot_candlestick(
                df_resampled.rename(columns={'interval_start':'time'}),
                show_rsi=False, show_macd=True,
                show_bollinger=True, show_volume=False,
                show_hedging_band=True,
                mode=mode
            )

            if mode in [1, 2]:
                signal = calculate_trading_signal(df_resampled)
                execute_trade(signal)

    except Exception as e:
        print("❌ 분석 오류:", e)
    finally:
        con.close()

# Discord 메시지
def send_message(message):
    url = config['DISCORD_WEBHOOK_URL']
    data = {"content": message}
    try:
        response = requests.post(url, json=data)
        if response.status_code == 204:
            print(f"📢 DISCORD 메시지: {message}")
        else:
            print(f"❌ 메시지 실패: {response.status_code}")
    except Exception as e:
        print(f"❌ 메시지 전송 오류: {e}")

# 전략 시그널 예시
def calculate_trading_signal(df):
    if df.empty:
        return None

    try:
        now = datetime.now(NYT).replace(second=0, microsecond=0)
        last_time = pd.to_datetime(df.index[-1]) if not isinstance(df.index, pd.RangeIndex) else df['interval_start'].iloc[-1]
        last_row = df.iloc[-1]

        if last_time == now:
            if last_row.get('buy_signal', False):
                return "buy"
            elif last_row.get('sell_signal', False):
                return "sell"
    except Exception as e:
        print("❌ 시그널 계산 오류:", e)
    return None

# 거래 실행
def execute_trade(signal):
    if signal:
        main_api.get_overseas_inquire_present_balance(svr='vps', dv="02", dvsn="01", natn="000", mkt="00", inqr_dvsn="00")
        last_price = main_api.get_overseas_price_quot_price_detail(excd="AMS", itm_no="SOXL")
        print(f"🚨 {signal} 신호 발생! 실제 거래 로직을 구현해주세요.")
        send_message(f"🚨 현재가 {last_price} 신호 발생! 실제 거래 로직을 구현해주세요.")

        #잔고조회
        main_api.get_overseas_inquire_present_balance(svr='vps', dv="03", dvsn="01", natn="000", mkt="00", inqr_dvsn="00")

        #구매로직
        main_api.get_overseas_order(svr='vps',ord_dv="buy", excg_cd="AMEX", itm_no="SOXL", qty=1, unpr=1)

    
    print('매매 신호가 없습니다.')

# 정규장 수집 루프
def data_collection_thread(mode):
    global market_open_sent, market_close_sent
    market_open_sent = False
    market_close_sent = False
    preopen_sent = False
    svr = 'my_prod' if mode == 1 else 'vps'  # 서버 정보 기억

    while True:
        now = datetime.now(NYT)
        # 🔁 매 루프마다 토큰 유효성 체크

        if dt_time(9, 20) <= now.time() < dt_time(9, 29):
            if not preopen_sent:
                try:
                    send_message("🟢 정규장이 시작 10분 전입니다. 모니터링을 시작합니다.")
                    ka.auth(svr)
                except:
                    send_message("토큰 갱신에 실패 했습니다. 확인 바랍니다.")
                fill_missing_data()
                preopen_sent = True

            fill_missing_data()

        if dt_time(9, 30) <= now.time() < dt_time(16, 0):
            if not market_open_sent:
                send_message("🟢 정규장이 시작되었습니다.")
                main_api.get_overseas_inquire_present_balance(svr='vps', dv="02", dvsn="01", natn="000", mkt="00", inqr_dvsn="00")
                market_open_sent = True
                market_close_sent = False

            if now.second == 3:
                fill_missing_data()
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

# 모드 실행
def run_mode(mode):
    svr = 'my_prod' if mode == 1 else 'vps'
    ka.auth(svr)
    init_duckdb_schema()

    if mode in [1, 2]:
        data_collection_thread(mode)
    elif mode == 3:
        fill_missing_data()
        print("🧪 모드 3: 전략 개발 및 테스트 모드 실행")
        data_analysis_improved(mode, resample_interval=RESAMPLE_INTERVAL)
        
        

if __name__ == "__main__":
    mode = 3
    run_mode(mode)