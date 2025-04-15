import kis_auth as kis
import pandas as pd
from collections import namedtuple
from datetime import datetime, timedelta
from pandas import DataFrame
import mojito

##############################################################################################
# [해외주식] 주문/계좌 > 해외주식 주문[v1_해외주식-001]
#
# * 모의투자의 경우, 모든 해외 종목 매매가 지원되지 않습니다. 일부 종목만 매매 가능한 점 유의 부탁드립니다.
#
# * 해외주식 서비스 신청 후 이용 가능합니다. (아래 링크 3번 해외증권 거래신청 참고)
# https://securities.koreainvestment.com/main/bond/research/_static/TF03ca010001.jsp
#
# * 해외 거래소 운영시간 외 API 호출 시 애러가 발생하오니 운영시간을 확인해주세요.
# * 해외 거래소 운영시간(한국시간 기준)
# 1) 미국 : 23:30 ~ 06:00 (썸머타임 적용 시 22:30 ~ 05:00)
# 2) 일본 : (오전) 09:00 ~ 11:30, (오후) 12:30 ~ 15:00
# 3) 상해 : 10:30 ~ 16:00
# 4) 홍콩 : (오전) 10:30 ~ 13:00, (오후) 14:00 ~ 17:00
#
# ※ POST API의 경우 BODY값의 key값들을 대문자로 작성하셔야 합니다.
#    (EX. "CANO" : "12345678", "ACNT_PRDT_CD": "01",...)
#
# ※ 종목코드 마스터파일 파이썬 정제코드는 한국투자증권 Github 참고 부탁드립니다.
#    https://github.com/koreainvestment/open-trading-api/tree/main/stocks_info
##############################################################################################
# Input: None (Option) 상세 Input값 변경이 필요한 경우 API문서 참조
# Output: DataFrame (Option) output API 문서 참조 등
def get_overseas_order(svr, ord_dv="", excg_cd="", itm_no="", qty=0, unpr=0, tr_cont="", FK100="", NK100="", dataframe=None):  # 국내주식주문 > 주식주문(현금)
    url = '/uapi/overseas-stock/v1/trading/order'

    if ord_dv == "buy":
        if excg_cd in ("NASD","NYSE","AMEX"):
            if svr == 'vps':
                tr_id = "VTTT1002U"
            elif svr == 'my_prod':
                tr_id = "TTTT1002U"  # 미국 매수 주문 [모의투자] VTTT1002U
        else:
            print("해외거래소코드 확인요망!!!")
            return None
        
    elif ord_dv == "sell":
        if excg_cd in ("NASD", "NYSE", "AMEX"):
            if svr == 'vps':
                tr_id = "VTTT1006U"
            elif svr == 'my_prod':
                tr_id = "TTTT1006U"  # 미국 매도 주문 [모의투자] VTTT1006U
        else:
            print("해외거래소코드 확인요망!!!")
            return None
    else:
        print("매수/매도 구분 확인요망!")
        return None

    if itm_no == "":
        print("주문종목번호(상품번호) 확인요망!!!")
        return None

    if qty == 0:
        print("주문수량 확인요망!!!")
        return None

    if unpr == 0:
        print("해외주문단가 확인요망!!!")
        return None

    if ord_dv == "buy":
        sll_type = ""
    elif ord_dv == "sell":
        sll_type = "00"
    else:
        print("매수/매도 구분 확인요망!!!")
        return None

    params = {
        "CANO": kis.getTREnv().my_acct,         # 종합계좌번호 8자리
        "ACNT_PRDT_CD": kis.getTREnv().my_prod, # 계좌상품코드 2자리
        "OVRS_EXCG_CD": excg_cd,                # 해외거래소코드
                                                # NASD:나스닥,NYSE:뉴욕,AMEX:아멕스,SEHK:홍콩,SHAA:중국상해,SZAA:중국심천,TKSE:일본,HASE:베트남하노이,VNSE:호치민
        "PDNO": itm_no,                         # 종목코드
        "ORD_DVSN": "00",                       # 주문구분 00:지정가, 01:시장가, 02:조건부지정가  나머지주문구분 API 문서 참조
        "ORD_QTY": str(int(qty)),               # 주문주식수
        "OVRS_ORD_UNPR": str(int(unpr)),        # 해외주문단가
        "SLL_TYPE": sll_type,                   # 판매유형
        "ORD_SVR_DVSN_CD": "0"                  # 주문서버구분코드l
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params, postFlag=True)
    if str(res.getBody().rt_cd) == "0":
        current_data = pd.DataFrame(res.getBody().output, index=[0])
        dataframe = current_data
    else:
        print(res.getBody().msg_cd + "," + res.getBody().msg1)
        #print(res.getErrorCode() + "," + res.getErrorMessage())
        dataframe = None

    return dataframe

##############################################################################################
# [해외주식] 주문/계좌 > 해외주식 체결기준현재잔고[v1_해외주식-008]
# 해외주식 잔고를 체결 기준으로 확인하는 API 입니다.
#
# HTS(eFriend Plus) [0839] 해외 체결기준잔고 화면을 API로 구현한 사항으로 화면을 함께 보시면 기능 이해가 쉽습니다.
#
# (※모의계좌의 경우 output3(외화평가총액 등 확인 가능)만 정상 출력됩니다.
# 잔고 확인을 원하실 경우에는 해외주식 잔고[v1_해외주식-006] API 사용을 부탁드립니다.)
#
# * 해외주식 서비스 신청 후 이용 가능합니다. (아래 링크 3번 해외증권 거래신청 참고)
# https://securities.koreainvestment.com/main/bond/research/_static/TF03ca010001.jsp
#
# 해외주식 체결기준현재잔고 유의사항
# 1. 해외증권 체결기준 잔고현황을 조회하는 화면입니다.
# 2. 온라인국가는 수수료(국내/해외)가 반영된 최종 정산금액으로 잔고가 변동되며, 결제작업 지연등으로 인해 조회시간은 차이가 발생할 수 있습니다.
#    - 아시아 온라인국가 : 매매일 익일    08:40 ~ 08:45분 경
#    - 미국 온라인국가   : 당일 장 종료후 08:40 ~ 08:45분 경
#   ※ 단, 애프터연장 참여 신청계좌는 10:30 ~ 10:35분 경(Summer Time : 09:30 ~ 09:35분 경)에 최종 정산금액으로 변동됩니다.
# 3. 미국 현재가 항목은 주간시세 및 애프터시세는 반영하지 않으며, 정규장 마감 후에는 종가로 조회됩니다.
# 4. 온라인국가를 제외한 국가의 현재가는 실시간 시세가 아니므로 주문화면의 잔고 평가금액 등과 차이가 발생할 수 있습니다.
# 5. 해외주식 담보대출 매도상환 체결내역은 해당 잔고화면에 반영되지 않습니다.
#    결제가 완료된 이후 외화잔고에 포함되어 반영되오니 참고하여 주시기 바랍니다.
# 6. 외화평가금액은 당일 최초고시환율이 적용된 금액으로 실제 환전금액과는 차이가 있습니다.
# 7. 미국은 메인 시스템이 아닌 별도 시스템을 통해 거래되므로, 18시 10~15분 이후 발생하는 미국 매매내역은 해당 화면에 실시간으로 반영되지 않으니 하단 내용을 참고하여 안내하여 주시기 바랍니다.
#    [외화잔고 및 해외 유가증권 현황 조회]
#    - 일반/통합증거금 계좌 : 미국장 종료 + 30분 후 부터 조회 가능
#                             단, 통합증거금 계좌에 한해 주문금액은 외화잔고 항목에 실시간 반영되며, 해외 유가증권 현황은 반영되지
#                             않아 해외 유가증권 평가금액이 과다 또는 과소 평가될 수 있습니다.
#    - 애프터연장 신청계좌  : 실시간 반영
#                             단, 시스템정산작업시간(23:40~00:10) 및 거래량이 많은 경우 메인시스템에 반영되는 시간으로 인해 차이가
#                             발생할 수 있습니다.
#    ※ 배치작업시간에 따라 시간은 변동될 수 있습니다.
##############################################################################################
# 해외주식 체결기준현재잔고 List를 DataFrame 으로 반환
# Input: None (Option) 상세 Input값 변경이 필요한 경우 API문서 참조
# Output: DataFrame (Option) output API 문서 참조 등
def get_overseas_inquire_present_balance(svr, dv="03", dvsn="01", natn="000", mkt="00", inqr_dvsn="00", tr_cont="", FK100="", NK100="", dataframe=None):
    url = '/uapi/overseas-stock/v1/trading/inquire-present-balance'
    if svr == 'vps':
        tr_id = "VTRP6504R"
    elif svr == 'my_prod':
        tr_id = "CTRP6504R"   # 모의투자 VTRP6504R

    t_cnt = 0

    params = {
        "CANO": kis.getTREnv().my_acct,         # 종합계좌번호 8자리
        "ACNT_PRDT_CD": kis.getTREnv().my_prod, # 계좌상품코드 2자리
        "WCRC_FRCR_DVSN_CD": dvsn,              # 원화외화구분코드 01 : 원화, 02 : 외화
        "NATN_CD": natn,                        # 국가코드 000 전체, 840 미국, 344 홍콩, 156 중국, 392 일본, 704 베트남
        "TR_MKET_CD": mkt,                      # 거래시장코드 00:전체 (API문서 참조)
        "INQR_DVSN_CD": inqr_dvsn               # 00 : 전체,01 : 일반해외주식,02 : 미니스탁
    }


    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if str(res.getBody().rt_cd) == "0":
        if dv == "01":
            current_data = pd.DataFrame(res.getBody().output1)
        elif dv == "02":
            current_data = pd.DataFrame(res.getBody().output2)
        else:
            current_data = pd.DataFrame(res.getBody().output3, index=[0])
        dataframe = current_data
    else:
        print(res.getBody().msg_cd + "," + res.getBody().msg1)
        #print(res.getErrorCode() + "," + res.getErrorMessage())
        dataframe = None

    return dataframe
##############################################################################################
# [해외주식] 주문/계좌 > 해외주식 매수가능금액조회[v1_해외주식-014]
# 해외주식 매수가능금액조회 API입니다.
#
# * 해외주식 서비스 신청 후 이용 가능합니다. (아래 링크 3번 해외증권 거래신청 참고)
# https://securities.koreainvestment.com/main/bond/research/_static/TF03ca010001.jsp
##############################################################################################
# 해외주식 매수가능금액조회 List를 DataFrame 으로 반환
# Input: None (Option) 상세 Input값 변경이 필요한 경우 API문서 참조
# Output: DataFrame (Option) output API 문서 참조 등
def get_overseas_inquire_psamount(svr, dv="03", dvsn="01", natn="000", mkt="00", inqr_dvsn="00", tr_cont="", FK100="", NK100="", dataframe=None):
    url = '/uapi/overseas-stock/v1/trading/inquire-psamount'
    if svr == 'vps':
        tr_id = "VTTT1002U"
    elif svr == 'my_prod':
        tr_id = "JTTT1006U"   # 모의투자 VTTS3007R

    t_cnt = 0

    params = {
        "CANO": kis.getTREnv().my_acct,         # 종합계좌번호 8자리
        "ACNT_PRDT_CD": kis.getTREnv().my_prod, # 계좌상품코드 2자리
        "OVRS_EXCG_CD": dvsn,              # 원화외화구분코드 01 : 원화, 02 : 외화
        "OVRS_ORD_UNPR": natn,                        # 국가코드 000 전체, 840 미국, 344 홍콩, 156 중국, 392 일본, 704 베트남
        "ITEM_CD": inqr_dvsn               # 00 : 전체,01 : 일반해외주식,02 : 미니스탁
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if str(res.getBody().rt_cd) == "0":
        current_data = pd.DataFrame(res.getBody().output, index=[0])
        dataframe = current_data
    else:
        print(res.getBody().msg_cd + "," + res.getBody().msg1)
        #print(res.getErrorCode() + "," + res.getErrorMessage())
        dataframe = None

    return dataframe

##############################################################################################
# [해외주식] 기본시세 > 해외주식 현재가상세
# 개요
# 해외주식 현재가상세 API입니다.
# 해당 API를 활용하여 해외주식 종목의 매매단위(vnit), 호가단위(e_hogau), PER, PBR, EPS, BPS 등의 데이터를 확인하실 수 있습니다.
# 해외주식 시세는 무료시세(지연시세)만이 제공되며, API로는 유료시세(실시간시세)를 받아보실 수 없습니다.
# ※ 지연시세 지연시간 : 미국 - 실시간무료(0분지연) / 홍콩, 베트남, 중국 - 15분지연 / 일본 - 20분지연
#    미국의 경우 0분지연시세로 제공되나, 장중 당일 시가는 상이할 수 있으며, 익일 정정 표시됩니다.
# ※ 추후 HTS(efriend Plus) [7781] 시세신청(실시간) 화면에서 유료 서비스 신청 시 실시간 시세 수신할 수 있도록 변경 예정
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
# 해외주식 현재가상세 시세 Object를 DataFrame 으로 반환
# Input: None (Option) 상세 Input값 변경이 필요한 경우 API문서 참조
# Output: DataFrame (Option) output
def get_overseas_price_quot_price_detail(excd="", itm_no="", tr_cont="", dataframe=None):
    url = '/uapi/overseas-price/v1/quotations/price-detail'
    tr_id = "HHDFS76200200" # 해외주식 현재가상세

    params = {
        "AUTH": "",         # 시장 분류 코드 	J : 주식/ETF/ETN, W: ELW
        "EXCD": excd,       # 	종목번호 (6자리) ETN의 경우, Q로 시작 (EX. Q500001)
        "SYMB": itm_no      # 종목번호 (6자리) ETN의 경우, Q로 시작 (EX. Q500001)
    }
    res = kis._url_fetch(url, tr_id, tr_cont, params)

    # Assuming 'output' is a dictionary that you want to convert to a DataFrame
    current_data = pd.DataFrame(res.getBody().output, index=[0])  # getBody() kis_auth.py 존재
    last_price = float(res.getBody().output['last']) 
    dataframe = current_data

    return last_price

##############################################################################################
# [해외주식] 주문/계좌 > 해외주식 잔고_현황 [v1_해외주식-006]
# 해외주식 잔고를 조회하는 API 입니다.
# 한국투자 HTS(eFriend Plus) > [7600] 해외주식 종합주문 화면의 좌측 하단 '실시간잔고' 기능을 API로 개발한 사항으로, 해당 화면을 참고하시면 기능을 이해하기 쉽습니다.
# 다만 미국주간거래 가능종목에 대해서는 frcr_evlu_pfls_amt(외화평가손익금액), evlu_pfls_rt(평가손익율), ovrs_stck_evlu_amt(해외주식평가금액), now_pric2(현재가격2) 값이 HTS와는 상이하게 표출될 수 있습니다.
# (주간시간 시간대에 HTS는 주간시세로 노출, API로는 야간시세로 노출)
#
# 실전계좌의 경우, 한 번의 호출에 최대 100건까지 확인 가능하며, 이후의 값은 연속조회를 통해 확인하실 수 있습니다.
#
# * 해외주식 서비스 신청 후 이용 가능합니다. (아래 링크 3번 해외증권 거래신청 참고)
# https://securities.koreainvestment.com/main/bond/research/_static/TF03ca010001.jsp
#
# * 미니스탁 잔고는 해당 API로 확인이 불가합니다.
##############################################################################################
# 해외주식 잔고 List를 DataFrame 으로 반환
# Input: None (Option) 상세 Input값 변경이 필요한 경우 API문서 참조
# Output: DataFrame (Option) output API 문서 참조 등
def get_overseas_inquire_balance(svr, excg_cd="", crcy_cd="", tr_cont="", FK100="", NK100="", dataframe=None):
    url = '/uapi/overseas-stock/v1/trading/inquire-balance'
    if svr == 'vps':
        tr_id = "VTTS3012R"
    elif svr == 'my_prod':
        tr_id = "TTTS3012R"   # 모의투자 VTTS3012R

    t_cnt = 0

    params = {
        "CANO": kis.getTREnv().my_acct,         # 종합계좌번호 8자리
        "ACNT_PRDT_CD": kis.getTREnv().my_prod, # 계좌상품코드 2자리
        "OVRS_EXCG_CD": excg_cd,                # 해외거래소코드 NASD:나스닥,NYSE:뉴욕,AMEX:아멕스,SEHK:홍콩,SHAA:중국상해,SZAA:중국심천,TKSE:일본,HASE:베트남하노이,VNSE:호치민
        "TR_CRCY_CD": crcy_cd,                  # 거래통화코드 USD : 미국달러,HKD : 홍콩달러,CNY : 중국위안화,JPY : 일본엔화,VND : 베트남동
        "CTX_AREA_FK200": FK100,                # 공란 : 최초 조회시 이전 조회 Output CTX_AREA_FK100 값 : 다음페이지 조회시(2번째부터)
        "CTX_AREA_NK200": NK100                 # 공란 : 최초 조회시 이전 조회 Output CTX_AREA_NK100 값 : 다음페이지 조회시(2번째부터)
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if str(res.getBody().rt_cd) == "0":
        current_data = pd.DataFrame(res.getBody().output2, index=[0])
        dataframe = current_data
    else:
        print(res.getBody().msg_cd + "," + res.getBody().msg1)
        #print(res.getErrorCode() + "," + res.getErrorMessage())
        dataframe = None

    return dataframe


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
def get_overseas_price_quot_inquire_time_itemchartprice(div="02", excd="", itm_no="", nmin="", pinc="0", tr_cont="", dataframe=None, next_value = "0",nrec = "120", keyb=""):
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
        "NREC": nrec,      # 요청갯수 레코드요청갯수 (최대 120)
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