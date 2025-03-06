import json
import pprint
import test_api, test_s
import kis_auth as ka
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
import numpy as np
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
    date와 time을 합쳐 YYYY-MM-DD HH:MM 형식으로 변환.
    
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

    return df_rt_filtered

def min_data(min_interval):
    rt_data = test_api.get_overseas_price_quot_inquire_time_itemchartprice(
                div="02", excd="AMS", itm_no="SOXL", nmin=min_interval, pinc="1"
            )
    df = data_formatting(rt_data)
    return df

def convert_to_dataframe(data):
    if "output2" in data:
        # output2 데이터를 데이터프레임으로 변환
        df = pd.DataFrame(data["output2"])
        
        # 필요한 열만 선택 및 시간 데이터 처리
        df = df[['tymd', 'xhms', 'open', 'high', 'low', 'last', 'evol', 'eamt']]
        df['datetime'] = pd.to_datetime(df['tymd'] + df['xhms'], format='%Y%m%d%H%M%S')
        
        # 데이터프레임 정리 (시간 순서대로 정렬)
        df = df.sort_values(by='datetime').reset_index(drop=True)
        
        # 필요 없는 열 삭제
        df.drop(columns=['tymd', 'xhms'], inplace=True)
        
        return df
    else:
        return pd.DataFrame()

def get_next_keyb(output2, nmin):
    last_record = output2[-1]
    last_time_str = last_record["xymd"] + last_record["xhms"]  # YYYYMMDDHHMMSS 형태의 문자열
    last_time = datetime.strptime(last_time_str, "%Y%m%d%H%M%S")  # 문자열을 datetime 객체로 변환
    next_keyb_time = last_time - timedelta(minutes=nmin)  # nmin 값만큼 이전 시간 계산
    return next_keyb_time.strftime("%Y%m%d%H%M%S")  # 다시 문자열로 변환하여 반환

def run_mode(mode):
    """
    선택한 실행 모드에 따라 작업을 수행.

    :param mode: 실행 모드 (1: 실제 매매, 2: 모의투자, 3: 전략 개발)
    """
    try:
        if mode == '1':
            print("🟢 실제 매매 모드 (R) 실행")
            # TODO: 실제 매매 관련 코드 추가
        elif mode == '2':
            print("🟡 모의투자 모드 (V) 실행")
            ka.auth(svr='vps')
            nmin = '15'
            period = '4'
            first_call = test_api.get_overseas_price_quot_inquire_time_itemchartprice(
                div="02", excd="AMS", itm_no="SOXL", nmin=nmin, pinc="1", keyb=keyb
            )
            if not first_call:
                return
            
            # 첫 조회 데이터 변환 및 저장
            df = convert_to_dataframe(first_call)
            all_data = pd.concat([all_data, df], ignore_index=True)

            # 다음 조회를 위한 변수 초기화
            next_value = first_call["output1"]["next"]
            keyb = get_next_keyb(first_call["output2"], nmin)  # nmin에 따라 1분 또는 n분 전 시간 계산
            
            for _ in range(period - 1):
                # 다음 조회 실행
                next_call = test_api.get_overseas_price_quot_inquire_time_itemchartprice(
                    div="02", excd="AMS", itm_no="SOXL", nmin=nmin, pinc="1", next_value=next_value, keyb=keyb
                )
                if not next_call:
                    break
                
                # 다음 조회 데이터 변환 및 저장
                df = convert_to_dataframe(next_call)
                all_data = pd.concat([all_data, df], ignore_index=True)
                
                # 다음 조회를 위한 keyb 및 next 값 갱신
                next_value = next_call["output1"]["next"]
                keyb = get_next_keyb(next_call["output2"], nmin)  # nmin에 따라 갱신된 keyb 값
                
            # 결과 데이터프레임을 시간순으로 정렬하여 저장
            all_data = all_data.sort_values(by='datetime').reset_index(drop=True).drop_duplicates() # 중복 제거
            all_data.to_csv(f'fetched_data.csv', index=False)  # CSV 파일로 저장
            print(f"데이터가 CSV 파일로 저장되었습니다.")
        elif mode == '3':
            print("🔵 전략 개발 모드 (T) 실행")
            ka.auth()  # 한투 API 인증

            # 해외 주식 분봉 데이터 조회 (SOXL, 분봉)
            min_interval = '15'
            df = min_data(min_interval)
            df.to_csv("data.csv", index=False)
            df = test_s.plot_candlestick_with_macd(df, show_rsi=True, show_macd=True, show_bollinger=False)  #MA #RSI #MACD #BB #CCI 
            rt_data = test_api.get_overseas_price_quot_inquire_daily_chartprice(
                div="N", itm_no="AAPL", inqr_strt_dt="20250101", inqr_end_dt="", period="D"
            )

            # 📊 원본 데이터 출력
            #print("📊 해외 주식 데이터 조회 결과:")
            # 📂 CSV 파일로 저장
            
            #print(df)
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
