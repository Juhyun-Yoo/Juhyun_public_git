import json
import pprint
import test_api, test_s
import kis_auth as ka
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
import numpy as np
import time

# 전역 변수: 1분 데이터를 저장할 DataFrame와 기준 시작 시간
global_minute_data = pd.DataFrame()
global_start_time = None

class Time:
    """
    미국 주식 거래 시간을 관리하는 클래스.
    """
    
    def __init__(self):
        """
        현재 뉴욕 시간을 기준으로 미국 주식 시장 주요 시간을 설정.
        """
        self.t_now_usa = self._get_ny_time()
        self.t_open_usa = self._set_time(9, 30)    # 개장
        self.t_start_usa = self._set_time(9, 45)   # 매매 시작 (개장 후 15분)
        self.t_sell_usa = self._set_time(15, 45)   # 매매 종료 (폐장 15분 전)
        self.t_exit_usa = self._set_time(16, 0)    # 폐장

    @staticmethod
    def _get_ny_time():
        """현재 미국 뉴욕 시간을 반환."""
        return datetime.now(timezone('America/New_York'))

    def _set_time(self, hour, minute):
        """지정된 시간으로 설정."""
        return self._get_ny_time().replace(hour=hour, minute=minute, second=0, microsecond=0)


def check_balance(broker):
    """
    계좌 잔고를 조회하여 총 평가 금액을 반환.

    :param broker: 거래 API 객체
    :return: 총 평가 금액 (int)
    """
    try:
        balance = broker.fetch_present_balance()
        return balance["output3"]["tot_asst_amt"]
    except KeyError:
        print("⚠️ 잔고 조회 실패: 응답 데이터 구조가 예상과 다릅니다.")
        return None
    except Exception as e:
        print(f"⚠️ 오류 발생: {e}")
        return None


def data_formatting(data):
    """
    rt_data를 DataFrame으로 변환 후 필요한 컬럼만 선택하여 가공.
    date와 time을 합쳐 YYYY-MM-DD HH:MM 형식으로 변환하고,
    volume과 eamt 컬럼을 숫자형으로 변환.
    
    :param data: 원본 데이터 (JSON 또는 딕셔너리 형태)
    :return: 필터링된 DataFrame
    """
    # 📌 rt_data를 DataFrame으로 변환
    df_rt = pd.DataFrame(data)

    # 📌 필요한 컬럼만 선택 후 컬럼명 변경
    df_rt_filtered = df_rt[["xymd", "xhms", "open", "high", "low", "last", "evol", "eamt"]].rename(
        columns={
            "xymd": "date",
            "xhms": "time",
            "last": "close",
            "evol": "volume",
        }
    )

    # 📌 date와 time을 YYYY-MM-DD HH:MM 형태로 변환하여 time 컬럼에 저장
    df_rt_filtered["time"] = (
        df_rt_filtered["date"].astype(str).str[:4] + "-" +  # YYYY
        df_rt_filtered["date"].astype(str).str[4:6] + "-" +  # MM
        df_rt_filtered["date"].astype(str).str[6:] + " " +   # DD
        df_rt_filtered["time"].astype(str).str.zfill(6).str[:2] + ":" +  # HH
        df_rt_filtered["time"].astype(str).str.zfill(6).str[2:4]  # MM
    )

    # 📌 date 컬럼 삭제
    df_rt_filtered = df_rt_filtered.drop(columns=["date"])

    # 📌 volume과 eamt 컬럼을 숫자형으로 변환 (합계 계산을 위해)
    df_rt_filtered["volume"] = pd.to_numeric(df_rt_filtered["volume"], errors='coerce')
    df_rt_filtered["eamt"] = pd.to_numeric(df_rt_filtered["eamt"], errors='coerce')

    return df_rt_filtered


def min_data(min_interval):
    """
    1분 단위의 데이터를 가져오는 함수.
    """
    rt_data = test_api.get_overseas_price_quot_inquire_time_itemchartprice(
                div="02", excd="AMS", itm_no="SOXL", nmin=min_interval, pinc="1"
            )
    df = data_formatting(rt_data)
    return df


def min_massdata(min_interval, cnt):
    """
    다량의 1분 데이터를 한번에 받아오는 함수.
    """
    formatted_time = ""
    df_combined = pd.DataFrame()  # 데이터를 누적 저장할 빈 DataFrame 생성

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

        df = data_formatting(rt_data)  # API에서 받아온 데이터 정리

        if df.empty:  # 데이터가 비어 있으면 중단
            print("No more data available.")
            break

        last_time = df.iloc[-1]['time']  # 마지막 인덱스의 time 값
        formatted_time = pd.to_datetime(last_time).strftime('%Y%m%d%H%M%S')

        # 기존 데이터와 새로운 데이터 합치기
        df_combined = pd.concat([df_combined, df]).drop_duplicates(subset='time')
        time.sleep(0.5)
    return df_combined  # 누적된 데이터를 반환


def aggregate_rolling(df, agg_minutes, start_time=None):
    """
    1분 데이터(DataFrame)를 받아, 주어진 agg_minutes 간격(예: 15, 30, 60분)으로 집계합니다.
    집계 기준은 start_time부터의 시간 차이를 이용합니다.
    
    Parameters:
        df (DataFrame): 'time', 'open', 'high', 'low', 'close', 'volume', 'eamt' 컬럼을 가진 데이터.
        agg_minutes (int): 집계 간격 (분 단위).
        start_time (datetime, optional): 집계의 기준 시작 시간. 지정하지 않으면 df의 첫 시간 사용.
        
    Returns:
        DataFrame: 집계된 데이터
    """
    # time 컬럼을 datetime 타입으로 변환 후 정렬
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values('time')
    
    if start_time is None:
        start_time = df['time'].iloc[0]
        
    # 기준 시각으로부터 각 데이터의 시간 차이를 분 단위로 계산
    df['diff_minutes'] = (df['time'] - start_time).dt.total_seconds() / 60
    # 주어진 agg_minutes 간격으로 그룹 인덱스 생성 (예: 0~14분: 그룹 0, 15~29분: 그룹 1, ...)
    df['group'] = (df['diff_minutes'] // agg_minutes).astype(int)
    
    # 집계: open은 그룹 내 첫값, high는 최대, low는 최소, close는 마지막, volume과 eamt는 합계
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'eamt': 'sum'
    }
    grouped = df.groupby('group').agg(agg_dict).reset_index()
    
    # 각 그룹의 대표 시간은 start_time + (그룹 인덱스 * agg_minutes) 분으로 설정
    grouped['time'] = grouped['group'].apply(lambda x: start_time + pd.Timedelta(minutes=x * agg_minutes))
    
    # 최종적으로 필요한 컬럼 순서 지정
    grouped = grouped[['time', 'open', 'high', 'low', 'close', 'volume', 'eamt']]
    grouped = grouped.sort_values('time').reset_index(drop=True)
    
    return grouped


def run_mode(mode):
    """
    선택한 실행 모드에 따라 작업을 수행.
    
    :param mode: 실행 모드 (1: 실제 매매, 2: 모의투자, 3: 전략 개발)
    """
    global global_minute_data, global_start_time  # 전역 변수 사용
    
    try:
        if mode == '1':
            print("🟢 실제 매매 모드 (R) 실행")
            # TODO: 실제 매매 관련 코드 추가
            
        elif mode == '2':
            print("🟡 모의투자 모드 (V) 실행")
            ka.auth(svr='vps')

            
            # 1️⃣ 초기 실행: 다량의 1분 데이터를 받아옴 (예: cnt=10)
            min_interval = '1'  # 1분 데이터
            cnt = 10
            print("초기 1분 데이터 수집 중 (min_massdata)...")
            global_minute_data = min_massdata(min_interval, cnt)
            
            if global_minute_data.empty:
                print("⚠️ 초기 데이터 수집 실패")
                return
            
            # 기준 시작 시간은 초기 데이터의 첫 time
            global_minute_data['time'] = pd.to_datetime(global_minute_data['time'])
            global_start_time = global_minute_data.iloc[0]['time']
            
            # 집계 간격 설정 (원하는 간격: 예를 들어 15, 30, 60분 등)
            agg_minutes = 15  # 여기서 원하는 값으로 변경 가능
            
            # 초기 1분 데이터를 집계하여 파일로 저장
            aggregated_data = aggregate_rolling(global_minute_data, agg_minutes, start_time=global_start_time)
            aggregated_data.to_csv("aggregated_data.csv", index=False)
            print("초기 집계 데이터 저장 완료 (aggregated_data.csv)")
            df = test_s.plot_candlestick(aggregated_data, show_rsi=False, show_macd=True, show_bollinger=False)  #MA #RSI #MACD #BB #CCI
            # 2️⃣ 이후, 1분마다 새 데이터를 받아 rolling 방식으로 집계 데이터 갱신
            while True:
                print("1분 대기 중...")
                time.sleep(60)  # 1분 대기 (실제 환경에서는 60초)
                print("새로운 1분 데이터 수집 중 (min_data)...")
                new_data = min_data(min_interval)
                if new_data.empty:
                    print("새로운 데이터가 없습니다. 다음 주기로 넘어갑니다.")
                    continue

                # 새로운 데이터의 'time' 컬럼을 datetime으로 변환 후 추가
                new_data['time'] = pd.to_datetime(new_data['time'])
                
                # 기존 1분 데이터와 합치고 중복 제거 후 정렬
                global_minute_data = pd.concat([global_minute_data, new_data]).drop_duplicates(subset='time').sort_values('time')
                
                # rolling 방식으로 전체 1분 데이터를 집계
                aggregated_data = aggregate_rolling(global_minute_data, agg_minutes, start_time=global_start_time)
                aggregated_data.to_csv("aggregated_data.csv", index=False)
                print("집계 데이터 업데이트 완료:")
                df = test_s.plot_candlestick(aggregated_data, show_rsi=False, show_macd=True, show_bollinger=False)  #MA #RSI #MACD #BB #CCI
                print(aggregated_data.tail())

        elif mode == '3':
            print("🔵 전략 개발 모드 (T) 실행")
            ka.auth(svr='vps')  # 한투 API 인증

            # 해외 주식 분봉 데이터 조회 (SOXL, 분봉)
            min_interval = '15'
            cnt = 3
            df = min_data(min_interval)

            #df = min_massdata(min_interval, cnt)

            df.to_csv("data.csv", index=False)
            df = test_s.plot_candlestick(df, show_rsi=False, show_macd=True, show_bollinger=False, show_volume = True)  #MA #RSI #MACD #BB #CCI 
            #rt_data = test_api.get_overseas_price_quot_inquire_daily_chartprice(
            #    div="N", itm_no="AAPL", inqr_strt_dt="20250101", inqr_end_dt="", period="D"
            #)

            # 📊 원본 데이터 출력
            #print("📊 해외 주식 데이터 조회 결과:")
            # 📂 CSV 파일로 저장
                
        else:
            print("❌ 유효하지 않은 모드입니다.")
            
    except Exception as e:
        print(f"⚠️ 실행 중 오류 발생: {e}")


def main():
    """
    사용자 입력을 받아 실행할 모드를 결정.
    """
    valid_modes = {"1", "2", "3"}
    
    while True:
        mode = input("👉 실행할 모드를 선택하세요 (1: 실제 매매, 2: 모의투자, 3: 전략 개발): ").strip()
        
        if mode in valid_modes:
            run_mode(mode)
            break  # 정상적인 입력 시 루프 종료
        else:
            print("⚠️ 올바른 값을 입력하세요. (1, 2, 3)")


if __name__ == "__main__":
    main()
