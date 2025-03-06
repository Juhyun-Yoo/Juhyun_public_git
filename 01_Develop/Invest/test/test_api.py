import kis_auth as kis
import pandas as pd
from collections import namedtuple
from datetime import datetime
from pandas import DataFrame
##############################################################################################
# [해외주식] 기본시세 > 해외주식분봉조회
# 해외주식분봉조회 API입니다.
# 실전계좌의 경우, 최근 120건까지 확인 가능합니다.
#
# ※ 해외주식 분봉은 정규장만 제공됩니다.
#
# [미국주식시세 이용시 유의사항]
# ■ 무료 실시간 시세(0분 지연) 제공
# ※ 무료(매수/매도 각 10호가) : 나스닥 마켓센터에서 거래되는 호가 및 호가 잔량 정보
# ■ 무료 실시간 시세 서비스는 유료 실시간 시세 서비스 대비 평균 50% 수준에 해당하는 정보이므로
# 현재가/호가/순간체결량/차트 등에서 일시적·부분적 차이가 있을 수 있습니다.
# ■ 무료∙유료 모두 미국에 상장된 종목(뉴욕, 나스닥, 아멕스 등)의 시세를 제공하며, 동일한 시스템을 사용하여 주문∙체결됩니다.
# 단, 무료∙유료의 기반 데이터 차이로 호가 및 체결 데이터는 차이가 발생할 수 있고, 이로 인해 발생하는 손실에 대해서 당사가 책임지지 않습니다.
# ■ 무료 실시간 시세 서비스의 시가, 저가, 고가, 종가는 유료 실시간 시세 서비스와 다를 수 있으며,
# 종목별 과거 데이터(거래량, 시가, 종가, 고가, 차트 데이터 등)는 장 종료 후(오후 12시경) 유료 실시간 시세 서비스 데이터와 동일하게 업데이트됩니다.
# (출처: 한국투자증권 외화증권 거래설명서 - https://www.truefriend.com/main/customer/guide/Guide.jsp?&cmd=TF04ag010002¤tPage=1&num=64)
##############################################################################################
# 해외주식 해외주식분봉조회 시세 Object를 DataFrame 으로 반환
# Input: None (Option) 상세 Input값 변경이 필요한 경우 API문서 참조
# Output: DataFrame (Option) output
def get_overseas_price_quot_inquire_time_itemchartprice(div="02", excd="", itm_no="", nmin="", pinc="0", tr_cont="", dataframe=None, next_value = "0", keyb=""):
    url = '/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice'
    tr_id = "HHDFS76950200" # 해외주식 해외주식분봉조회

    params = {
        "AUTH": "",         # 시장 분류 코드 	J : 주식/ETF/ETN, W: ELW
        "EXCD": excd,       # 거래소코드 	HKS : 홍콩,NYS : 뉴욕,NAS : 나스닥,AMS : 아멕스,TSE : 도쿄,SHS : 상해,SZS : 심천,SHI : 상해지수
                            #           SZI : 심천지수,HSX : 호치민,HNX : 하노이,BAY : 뉴욕(주간),BAQ : 나스닥(주간),BAA : 아멕스(주간)
        "SYMB": itm_no,     # 종목코드(ex. TSLA)
        "NMIN": nmin,       # 분갭 분단위(1: 1분봉, 2: 2분봉, ...)
        "PINC": pinc,       # 전일포함여부(0:당일 1:전일포함)
        "NEXT": next_value,         # (사용안함)다음여부
        "NREC": "120",      # 요청갯수 레코드요청갯수 (최대 120)
        "FILL": "",         # (사용안함)미체결채움구분
        "KEYB": keyb          # (사용안함)NEXT KEY BUFF
    }
    res = kis._url_fetch(url, tr_id, tr_cont, params)

    # Assuming 'output' is a dictionary that you want to convert to a DataFrame
    if div == "02":
        current_data = pd.DataFrame(res.getBody().output2)
    else:
        current_data = pd.DataFrame(res.getBody().output1, index=[0])

    dataframe = current_data

    return dataframe

###########################################################################
# [해외주식] 기본시세 > 해외주식 종목/지수/환율기간별시세(일/주/월/년) → 기본정보
###########################################################################
# 해외주식 종목/지수/환율기간별시세(일/주/월/년) API입니다.
# 해외지수 당일 시세의 경우 지연시세 or 종가시세가 제공됩니다.
# ※ 해당 API로 미국주식 조회 시, 다우30, 나스닥100, S&P500 종목만 조회 가능합니다.
#   더 많은 미국주식 종목 시세를 이용할 시에는, 해외주식기간별시세 API 사용 부탁드립니다.
###########################################################################
def get_overseas_price_quot_inquire_daily_price(div="N", itm_no="", inqr_strt_dt="", inqr_end_dt="", period="D", tr_cont="", dataframe=None):
    url = '/uapi/overseas-price/v1/quotations/inquire-daily-chartprice'
    tr_id = "FHKST03030100" # 해외주식 종목/지수/환율기간별시세(일/주/월/년)

    if inqr_strt_dt is None:
        inqr_strt_dt = datetime.today().strftime("%Y%m%d")   # 시작일자 값이 없으면 현재일자
    if inqr_end_dt is None:
        inqr_end_dt = datetime.today().strftime("%Y%m%d")    # 종료일자 값이 없으면 현재일자

    params = {
        "FID_COND_MRKT_DIV_CODE": div,             # 시장 분류 코드 N: 해외지수, X 환율, I: 국채, S:금선물
        "FID_INPUT_ISCD": itm_no,                  # 종목번호 ※ 해외주식 마스터 코드 참조 (포럼 > FAQ > 종목정보 다운로드 > 해외주식)
                                                   # ※ 해당 API로 미국주식 조회 시, 다우30, 나스닥100, S&P500 종목만 조회 가능합니다. 더 많은 미국주식 종목 시세를 이용할 시에는, 해외주식기간별시세 API 사용 부탁드립니다.
        "FID_INPUT_DATE_1": inqr_strt_dt,          # 시작일자(YYYYMMDD)
        "FID_INPUT_DATE_2": inqr_end_dt,           # 종료일자(YYYYMMDD)
        "FID_PERIOD_DIV_CODE": period              # 기간분류코드 D:일, W:주, M:월, Y:년
    }
    res = kis._url_fetch(url, tr_id, tr_cont, params)

    # Assuming 'output' is a dictionary that you want to convert to a DataFrame
    current_data = pd.DataFrame(res.getBody().output1, index=[0])

    dataframe = current_data

    return dataframe


##############################################################################################
# [해외주식] 기본시세 > 해외주식 종목/지수/환율기간별시세(일/주/월/년) → 일자별정보 (최대 30일까지 조회)
##############################################################################################
def get_overseas_price_quot_inquire_daily_chartprice(div="N", itm_no="", inqr_strt_dt="", inqr_end_dt="", period="D", tr_cont="", dataframe=None):
    url = '/uapi/overseas-price/v1/quotations/inquire-daily-chartprice'
    tr_id = "FHKST03030100" # 해외주식 종목/지수/환율기간별시세(일/주/월/년)

    if inqr_strt_dt is None:
        inqr_strt_dt = datetime.today().strftime("%Y%m%d")   # 시작일자 값이 없으면 현재일자
    if inqr_end_dt is None:
        inqr_end_dt = datetime.today().strftime("%Y%m%d")    # 종료일자 값이 없으면 현재일자

    params = {
        "FID_COND_MRKT_DIV_CODE": div,             # 시장 분류 코드 N: 해외지수, X 환율, I: 국채, S:금선물
        "FID_INPUT_ISCD": itm_no,                  # 종목번호 ※ 해외주식 마스터 코드 참조 (포럼 > FAQ > 종목정보 다운로드 > 해외주식)
                                                   # ※ 해당 API로 미국주식 조회 시, 다우30, 나스닥100, S&P500 종목만 조회 가능합니다. 더 많은 미국주식 종목 시세를 이용할 시에는, 해외주식기간별시세 API 사용 부탁드립니다.
        "FID_INPUT_DATE_1": inqr_strt_dt,          # 시작일자(YYYYMMDD)
        "FID_INPUT_DATE_2": inqr_end_dt,           # 종료일자(YYYYMMDD)
        "FID_PERIOD_DIV_CODE": period              # 기간분류코드 D:일, W:주, M:월, Y:년
    }
    res = kis._url_fetch(url, tr_id, tr_cont, params)

    # Assuming 'output' is a dictionary that you want to convert to a DataFrame
    current_data = pd.DataFrame(res.getBody().output2)

    dataframe = current_data

    return dataframe