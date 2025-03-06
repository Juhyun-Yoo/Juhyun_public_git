import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf

def plot_candlestick_with_macd(df, show_bollinger=True, show_macd=True, show_ma=True):
    # Convert and sort datetime
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(by='time')
    df.set_index('time', inplace=True)

    # Convert column types
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

    # Rename columns for mplfinance
    df = df[['open', 'high', 'low', 'close', 'volume']]
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    # Initialize buy and sell signal columns
    df['buy_signal'] = np.nan
    df['sell_signal'] = np.nan

    apds = []
    position = 0  # 0: No position, 1: Holding

    # ✅ 종가선을 추가
    apds.append(mpf.make_addplot(df['Close'], color='black', linestyle='solid', width=1.2, label="Close Price"))

    if show_bollinger:
        # Calculate Bollinger Bands
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['STD20'] = df['Close'].rolling(window=20).std()
        df['Upper'] = df['MA20'] + (df['STD20'] * 3)
        df['Lower'] = df['MA20'] - (df['STD20'] * 3)

        apds.extend([
            mpf.make_addplot(df['Upper'], color='darkred', linestyle='dotted', width=0.8, label="Bollinger Upper"),
            mpf.make_addplot(df['Lower'], color='darkblue', linestyle='dotted', width=0.8, label="Bollinger Lower"),
            mpf.make_addplot(df['MA20'], color='darkgreen', linestyle='solid', width=0.8, label="Bollinger MA20")
        ])

    if show_macd:
        # Calculate MACD
        short_ema = df['Close'].ewm(span=8, adjust=False).mean()
        long_ema = df['Close'].ewm(span=21, adjust=False).mean()
        df['MACD'] = short_ema - long_ema
        df['Signal'] = df['MACD'].ewm(span=5, adjust=False).mean()
        df['Histogram'] = df['MACD'] - df['Signal']

        # ✅ MACD 교차점 찾기
        df['macd_cross'] = np.nan
        for i in range(1, len(df)):
            # MACD가 Signal을 위로 돌파 (매수 신호)
            if df['MACD'].iloc[i-1] < df['Signal'].iloc[i-1] and df['MACD'].iloc[i] > df['Signal'].iloc[i]:
                df.at[df.index[i], 'macd_cross'] = df['Close'].iloc[i]  # 녹색 점
            # MACD가 Signal을 아래로 돌파 (매도 신호)
            elif df['MACD'].iloc[i-1] > df['Signal'].iloc[i-1] and df['MACD'].iloc[i] < df['Signal'].iloc[i]:
                df.at[df.index[i], 'macd_cross'] = df['Close'].iloc[i]  # 빨간 점

        apds.extend([
            mpf.make_addplot(df['MACD'], panel=0, color='purple', width=0.8, secondary_y=True, label="MACD Line"),
            mpf.make_addplot(df['Signal'], panel=0, color='orange', width=0.8, secondary_y=True, label="MACD Signal"),
            mpf.make_addplot(df['Histogram'], panel=0, type='bar', color='gray', alpha=0.3, secondary_y=True, label="MACD Histogram"),
            
            # ✅ MACD 교차점을 캔들차트 위에 점으로 표시
            mpf.make_addplot(df['macd_cross'], scatter=True, marker='o', color='green', markersize=100, label="MACD Buy Signal", secondary_y=False)
        ])

    if show_ma:
        # Calculate Moving Averages (8, 13)
        df["MA_8"] = df["Close"].rolling(window=8).mean()
        df["MA_13"] = df["Close"].rolling(window=13).mean()
        df.dropna(inplace=True)

        apds.extend([
            mpf.make_addplot(df['MA_8'], color='cyan', linestyle='dashed', width=0.8, label="MA 8"),
            mpf.make_addplot(df['MA_13'], color='magenta', linestyle='dashed', width=0.8, label="MA 13")
        ])

    # ✅ 캔들차트 출력
    fig, axes = mpf.plot(df, type='candle', volume=True, style='charles', 
                         title='Candlestick Chart with MACD Cross Signals',
                         ylabel='Price', ylabel_lower='Volume', addplot=apds,
                         figsize=(14, 8), returnfig=True)

    # ✅ 범례를 그래프 바깥 오른쪽에 표시
    ax_price = axes[0]
    handles, labels = ax_price.get_legend_handles_labels()
    ax_price.legend(handles, labels, loc="upper left", bbox_to_anchor=(1.1, 1), fontsize=10)

    # Show Chart
    plt.show()
    
    return df
