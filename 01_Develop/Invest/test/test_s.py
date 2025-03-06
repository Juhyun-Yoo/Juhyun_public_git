import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf

def plot_candlestick_with_macd(df, show_rsi=True, show_macd=True, show_bollinger=True):
    # ✅ 날짜 데이터를 변환하고 정렬
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(by='time')
    df.set_index('time', inplace=True)

    # ✅ 데이터 타입 변환
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

    # ✅ mplfinance용 컬럼명 변경
    df = df[['open', 'high', 'low', 'close', 'volume']]
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    # ✅ 매수 및 매도 신호 컬럼 추가 (기본값 False)
    df['buy_signal'] = False
    df['sell_signal'] = False

    # ✅ 최고점 추적 변수
    highest_price = 0
    holding = False  # 현재 포지션 상태 (True: 매수 상태, False: 대기)
    last_signal = None  # 마지막으로 발생한 신호 ('buy' 또는 'sell')

    apds = []
    
    # ✅ 종가선을 추가
    apds.append(mpf.make_addplot(df['Close'], color='black', linestyle='solid', width=1.2, label="Close Price"))

    if show_macd:
        # ✅ MACD 계산
        short_ema = df['Close'].ewm(span=8, adjust=False).mean()
        long_ema = df['Close'].ewm(span=21, adjust=False).mean()
        df['MACD'] = short_ema - long_ema
        df['Signal'] = df['MACD'].ewm(span=5, adjust=False).mean()
        df['Histogram'] = df['MACD'] - df['Signal']

        # ✅ MACD + 스탑로스 적용
        for i in range(1, len(df)):
            current_price = df['Close'].iloc[i]

            # ✅ 매수 신호 발생 (MACD 골든크로스)
            if df['MACD'].iloc[i-1] < df['Signal'].iloc[i-1] and df['MACD'].iloc[i] > df['Signal'].iloc[i]:
                df.at[df.index[i], 'buy_signal'] = True
                highest_price = current_price  # 매수 시 최고점 초기화
                holding = True  # 매수 상태로 변경

            # ✅ 매도 신호 발생 (MACD 데드크로스)
            if holding and df['MACD'].iloc[i-1] > df['Signal'].iloc[i-1] and df['MACD'].iloc[i] < df['Signal'].iloc[i]:
                df.at[df.index[i], 'sell_signal'] = True
                holding = False  # 매도 후 상태 초기화 (추가 매도 신호 방지)

            # ✅ 보유 중일 때, 최고점 갱신
            if holding:
                highest_price = max(highest_price, current_price)

                # ✅ 최고점 대비 2% 이상 하락하면 매도 신호 발생 (스탑로스)
                if current_price <= highest_price * 0.97:
                    df.at[df.index[i], 'sell_signal'] = True
                    holding = False  # 매도 후 상태 초기화 (추가 매도 신호 방지)

        apds.extend([
            mpf.make_addplot(df['MACD'], panel=0, color='purple', width=0.8, secondary_y=True, label="MACD Line"),
            mpf.make_addplot(df['Signal'], panel=0, color='orange', width=0.8, secondary_y=True, label="MACD Signal"),
            mpf.make_addplot(df['Histogram'], panel=0, type='bar', color='gray', alpha=0.3, secondary_y=True, label="MACD Histogram"),
        ])
    
    if show_rsi:
        # ✅ RSI(14) 계산
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))

        # ✅ RSI 기반 매매 신호 (연속 신호 방지)
        for i in range(len(df)):
            if last_signal != 'buy' and df['RSI'].iloc[i] <= 20:
                df.at[df.index[i], 'buy_signal'] = True
                last_signal = 'buy'
            elif last_signal != 'sell' and df['RSI'].iloc[i] >= 80:
                df.at[df.index[i], 'sell_signal'] = True
                last_signal = 'sell'

        apds.append(mpf.make_addplot(df['RSI'], panel=1, color='blue', width=0.8, ylabel='RSI'))
    
    if show_bollinger:
        # ✅ 볼린저 밴드 계산
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['UpperBB'] = df['MA20'] + (df['Close'].rolling(window=20).std() * 2)
        df['LowerBB'] = df['MA20'] - (df['Close'].rolling(window=20).std() * 2)
        
        apds.extend([
            mpf.make_addplot(df['UpperBB'], color='red', linestyle='dashed', width=1, label='Upper BB'),
            mpf.make_addplot(df['LowerBB'], color='blue', linestyle='dashed', width=1, label='Lower BB')
        ])
    
    # ✅ 매수 및 매도 신호를 저장할 컬럼 추가 (신호가 없는 경우 NaN)
    df['buy_marker'] = np.where(df['buy_signal'], df['Close'], np.nan)
    df['sell_marker'] = np.where(df['sell_signal'], df['Close'], np.nan)

    # ✅ 매수/매도 신호를 캔들차트 위에 표시
    apds.append(mpf.make_addplot(df['buy_marker'], scatter=True, marker='^', color='green', markersize=100, label="Buy Signal"))
    apds.append(mpf.make_addplot(df['sell_marker'], scatter=True, marker='v', color='red', markersize=100, label="Sell Signal"))

    # ✅ 캔들차트 출력
    fig, axes = mpf.plot(df, type='candle', volume=True, style='charles', 
                         title='Candle chart & MACD/RSI/Bollinger signals',
                         ylabel='Price', ylabel_lower='Volume', addplot=apds,
                         figsize=(14, 8), returnfig=True)

    # ✅ 범례를 그래프 바깥 오른쪽에 표시
    ax_price = axes[0]
    handles, labels = ax_price.get_legend_handles_labels()
    ax_price.legend(handles, labels, loc="upper left", bbox_to_anchor=(1.1, 1), fontsize=10)

    # ✅ 차트 표시
    plt.show()
    
    return df  # 🚀 매매 신호가 포함된 데이터프레임 반환
