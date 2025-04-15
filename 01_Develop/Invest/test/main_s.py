import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf
from pytz import timezone


def is_regular_trading_hours(time):
    """미국 정규장 시간인지 확인"""
    ny_time = time.tz_convert('America/New_York')
    return pd.Timestamp("09:30").time() <= ny_time.time() <= pd.Timestamp("16:00").time()


def apply_macd(df):
    """MACD 시그널 생성"""
    short_ema = df['close'].ewm(span=12, adjust=False).mean()
    long_ema = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = short_ema - long_ema
    df['Signal'] = df['MACD'].ewm(span=3, adjust=False).mean()

    highest_price = 0
    holding = False
    last_trading_time = pd.Timestamp("15:50", tz="America/New_York").time()

    for i in range(1, len(df)):
        current_price = df['close'].iloc[i]
        current_time = df.index[i]
        ny_time = current_time.tz_convert('America/New_York')

        if not is_regular_trading_hours(current_time):
            continue

        if df['MACD'].iloc[i-1] < df['Signal'].iloc[i-1] and df['MACD'].iloc[i] > df['Signal'].iloc[i]:
            df.at[df.index[i], 'buy_signal'] = True
            highest_price = current_price
            holding = True

        elif df['MACD'].iloc[i-1] > df['Signal'].iloc[i-1] and df['MACD'].iloc[i] < df['Signal'].iloc[i]:
            df.at[df.index[i], 'sell_signal'] = True
            holding = False

        elif holding and ny_time.time() >= last_trading_time and not df['sell_signal'].any():
            df.at[df.index[i], 'sell_signal'] = True
            holding = False

        if holding:
            highest_price = max(highest_price, current_price)

    return df


def apply_rsi(df):
    """RSI 시그널 생성"""
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    for i in range(len(df)):
        if df['RSI'].iloc[i] <= 20 and is_regular_trading_hours(df.index[i]):
            df.at[df.index[i], 'buy_signal'] = True
        elif df['RSI'].iloc[i] >= 80 and is_regular_trading_hours(df.index[i]):
            df.at[df.index[i], 'sell_signal'] = True

    return df


def apply_bollinger(df):
    """Bollinger Band 계산"""
    df['MA20'] = df['close'].rolling(window=20).mean()
    df['UpperBB'] = df['MA20'] + df['close'].rolling(20).std() * 2
    df['lowerBB'] = df['MA20'] - df['close'].rolling(20).std() * 2
    return df


def apply_hedging_band(df):
    """Hedging Band 계산 (Keltner + Bollinger + Donchian)"""
    kc_ema = df['close'].ewm(span=21, adjust=False).mean()
    kc_atr = (df['high'] - df['low']).rolling(window=14).mean()
    kc_upper = kc_ema + 2 * kc_atr
    kc_lower = kc_ema - 2 * kc_atr

    bb_sma = df['close'].rolling(window=21).mean()
    bb_std = df['close'].rolling(window=21).std()
    bb_upper = bb_sma + 2 * bb_std
    bb_lower = bb_sma - 2 * bb_std

    dc_upper = df['high'].rolling(window=21).max()
    dc_lower = df['low'].rolling(window=21).min()

    df['Hedging_Upper'] = pd.concat([kc_upper, bb_upper, dc_upper], axis=1).max(axis=1)
    df['Hedging_Lower'] = pd.concat([kc_lower, bb_lower, dc_lower], axis=1).min(axis=1)
    df['Hedging_Center'] = (kc_ema + bb_sma + (dc_upper + dc_lower) / 2) / 3

    return df


def plot_candlestick(
    df, 
    show_rsi=True, 
    show_macd=True, 
    show_bollinger=True, 
    show_volume=True, 
    show_hedging_band=True, 
    mode=3
):
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    df.index = df.index.tz_localize('America/New_York', ambiguous='NaT', nonexistent='shift_forward')
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    df['buy_signal'] = False
    df['sell_signal'] = False

    apds = []

    if show_macd:
        df = apply_macd(df)
        apds.append(mpf.make_addplot(df['MACD'], panel=0, color='purple', secondary_y=True, label='MACD'))
        apds.append(mpf.make_addplot(df['Signal'], panel=0, color='orange', secondary_y=True, label='Signal'))

    if show_rsi:
        df = apply_rsi(df)
        apds.append(mpf.make_addplot(df['RSI'], panel=1, color='blue', ylabel='RSI'))

    if show_bollinger:
        df = apply_bollinger(df)
        apds.append(mpf.make_addplot(df['UpperBB'], color='red', linestyle='dashed'))
        apds.append(mpf.make_addplot(df['lowerBB'], color='blue', linestyle='dashed'))

    if show_hedging_band:
        df = apply_hedging_band(df)
        apds.append(mpf.make_addplot(df['Hedging_Upper'], color='red', linestyle='--', width=1.2))
        apds.append(mpf.make_addplot(df['Hedging_Lower'], color='red', linestyle='--', width=1.2))
        apds.append(mpf.make_addplot(df['Hedging_Center'], color='blue', width=1.4))

    df['buy_marker'] = np.where(df['buy_signal'], df['close'], np.nan)
    df['sell_marker'] = np.where(df['sell_signal'], df['close'], np.nan)

    if mode == 3:
        apds.append(mpf.make_addplot(df['buy_marker'], scatter=True, marker='^', color='green', markersize=100))
        apds.append(mpf.make_addplot(df['sell_marker'], scatter=True, marker='v', color='red', markersize=100))

        fig, axes = mpf.plot(
            df, type='candle', volume=True, style='charles',
            title='Candlestick with Indicators', ylabel='Price', ylabel_lower='Volume',
            addplot=apds, figsize=(14, 8), returnfig=True
        )
        ax_price = axes[0]
        handles, labels = ax_price.get_legend_handles_labels()
        ax_price.legend(handles, labels, loc="upper left", bbox_to_anchor=(1.1, 1))
        plt.show()

    return df
