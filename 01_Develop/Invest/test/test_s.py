import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf
from pytz import timezone

def is_regular_trading_hours(time):
    """미국 주식 정규 거래 시간(뉴욕 시간 기준 09:30 ~ 16:00) 내인지 확인하는 함수."""
    ny_time = time.tz_convert('America/New_York')
    return ny_time.time() >= pd.Timestamp("09:30").time() and ny_time.time() <= pd.Timestamp("16:00").time()

def plot_candlestick(df, show_rsi=True, show_macd=True, show_bollinger=True, show_volume=True):
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(by='time')
    df.set_index('time', inplace=True)
    
    df.index = df.index.tz_localize('America/New_York', ambiguous='NaT', nonexistent='shift_forward')
    
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    df = df[['open', 'high', 'low', 'close', 'volume']]
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    df['buy_signal'] = False
    df['sell_signal'] = False

    highest_price = 0
    holding = False
    first_buy_signal_occurred = False
    last_trading_time = pd.Timestamp("15:50", tz="America/New_York").time()

    apds = [mpf.make_addplot(df['Close'], color='black', linestyle='solid', width=1.2, label="Close Price")]

    # ✅ 거래량 기반 전략 추가 (OBV + 거래량 스파이크 감지)
    if show_volume:
        df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
        df['Volume Spike'] = df['Volume'] > df['Volume'].rolling(window=20).mean() * 1.5  # ✅ 평균 대비 1.5배 이상 증가 시 스파이크
        
        apds.append(mpf.make_addplot(df['OBV'], panel=2, color='brown', width=0.8, ylabel="OBV"))
        
    if show_macd:
        short_ema = df['Close'].ewm(span=12, adjust=False).mean()
        long_ema = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = short_ema - long_ema
        df['Signal'] = df['MACD'].ewm(span=3, adjust=False).mean()
        
        for i in range(1, len(df)):
            current_price = df['Close'].iloc[i]
            current_time = df.index[i]

            if not is_regular_trading_hours(current_time):
                continue

            ny_time = current_time.tz_convert('America/New_York')

            # ✅ MACD 골든크로스 매수 
            if df['MACD'].iloc[i-1] < df['Signal'].iloc[i-1] and df['MACD'].iloc[i] > df['Signal'].iloc[i]:
                df.at[df.index[i], 'buy_signal'] = True
                highest_price = current_price
                holding = True
                first_buy_signal_occurred = True

            # ✅ MACD 데드크로스 매도
            elif df['MACD'].iloc[i-1] > df['Signal'].iloc[i-1] and df['MACD'].iloc[i] < df['Signal'].iloc[i]:
                df.at[df.index[i], 'sell_signal'] = True
                holding = False

            # ✅ 15:50 이후 강제 매도
            elif holding and ny_time.time() >= last_trading_time and not df['sell_signal'].any():
                df.at[df.index[i], 'sell_signal'] = True
                holding = False  

            if holding:
                highest_price = max(highest_price, current_price)

        apds.extend([
            mpf.make_addplot(df['MACD'], panel=0, color='purple', width=0.8, secondary_y=True, label="MACD Line"),
            mpf.make_addplot(df['Signal'], panel=0, color='orange', width=0.8, secondary_y=True, label="MACD Signal"),
        ])
    
    if show_rsi:
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))

        for i in range(len(df)):
            if df['RSI'].iloc[i] <= 20 and is_regular_trading_hours(df.index[i]):
                df.at[df.index[i], 'buy_signal'] = True
            elif df['RSI'].iloc[i] >= 80 and is_regular_trading_hours(df.index[i]):
                df.at[df.index[i], 'sell_signal'] = True

        apds.append(mpf.make_addplot(df['RSI'], panel=1, color='blue', width=0.8, ylabel='RSI'))
    
    if show_bollinger:
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['UpperBB'] = df['MA20'] + (df['Close'].rolling(window=20).std() * 2)
        df['LowerBB'] = df['MA20'] - (df['Close'].rolling(window=20).std() * 2)
        
        apds.extend([
            mpf.make_addplot(df['UpperBB'], color='red', linestyle='dashed', width=1, label='Upper BB'),
            mpf.make_addplot(df['LowerBB'], color='blue', linestyle='dashed', width=1, label='Lower BB')
        ])
    
    df['buy_marker'] = np.where(df['buy_signal'], df['Close'], np.nan)
    df['sell_marker'] = np.where(df['sell_signal'], df['Close'], np.nan)

    apds.append(mpf.make_addplot(df['buy_marker'], scatter=True, marker='^', color='green', markersize=100, label="Buy Signal"))
    apds.append(mpf.make_addplot(df['sell_marker'], scatter=True, marker='v', color='red', markersize=100, label="Sell Signal"))

    fig, axes = mpf.plot(df, type='candle', volume=True, style='charles', 
                         title='Candle chart & MACD/RSI/Bollinger signals',
                         ylabel='Price', ylabel_lower='Volume', addplot=apds,
                         figsize=(14, 8), returnfig=True)

    ax_price = axes[0]
    handles, labels = ax_price.get_legend_handles_labels()
    ax_price.legend(handles, labels, loc="upper left", bbox_to_anchor=(1.1, 1), fontsize=10)

    plt.show()
    
    return df
