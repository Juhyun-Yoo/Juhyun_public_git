import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 크록스 주가 데이터 다운로드
ticker = 'U'
start_date = '2023-01-01'
data = yf.download(ticker, start=start_date)

# 볼린저 밴드와 MFI 전략 계산 (10일 이동평균 사용)
data['MA10'] = data['Close'].rolling(window=10).mean()
data['stddev'] = data['Close'].rolling(window=10).std()
data['upper'] = data['MA10'] + (data['stddev'] * 2)
data['lower'] = data['MA10'] - (data['stddev'] * 2)
data['PB'] = (data['Close'] - data['lower']) / (data['upper'] - data['lower'])
data['TP'] = (data['High'] + data['Low'] + data['Close']) / 3
data['PMF'] = 0
data['NMF'] = 0

for i in range(len(data.Close) - 1):
    if data.TP.values[i] < data.TP.values[i + 1]:
        data.PMF.values[i + 1] = data.TP.values[i + 1] * data.Volume.values[i + 1]
        data.NMF.values[i + 1] = 0
    else:
        data.NMF.values[i + 1] = data.TP.values[i + 1] * data.Volume.values[i + 1]
        data.PMF.values[i + 1] = 0

data['MFR'] = data.PMF.rolling(window=10).sum() / data.NMF.rolling(window=10).sum()
data['MFI10'] = 100 - 100 / (1 + data['MFR'])
data = data[9:]

# 그래프 표시
plt.figure(figsize=(9, 8))
plt.subplot(2, 1, 1)
plt.title(f'{ticker} Bollinger Band(10 day, 2 std) - Trend Following')
plt.plot(data.index, data['Close'], color='#0000ff', label='Close')
plt.plot(data.index, data['upper'], 'r--', label='Upper band')
plt.plot(data.index, data['MA10'], 'k--', label='Moving average 10')
plt.plot(data.index, data['lower'], 'c--', label='Lower band')
plt.fill_between(data.index, data['upper'], data['lower'], color='0.9')

for i in range(len(data.Close)):
    if data.PB.values[i] > 0.8 and data.MFI10.values[i] > 80:
        plt.plot(data.index.values[i], data.Close.values[i], 'r^')
    elif data.PB.values[i] < 0.2 and data.MFI10.values[i] < 20:
        plt.plot(data.index.values[i], data.Close.values[i], 'bv')

plt.legend(loc='best')

plt.subplot(2, 1, 2)
plt.plot(data.index, data['PB'] * 100, 'b', label='%B x 100')
plt.plot(data.index, data['MFI10'], 'g--', label='MFI(10 day)')
plt.yticks([-20, 0, 20, 40, 60, 80, 100, 120])

for i in range(len(data.Close)):
    if data.PB.values[i] > 0.8 and data.MFI10.values[i] > 80:
        plt.plot(data.index.values[i], 0, 'r^')
    elif data.PB.values[i] < 0.2 and data.MFI10.values[i] < 20:
        plt.plot(data.index.values[i], 0, 'bv')

plt.grid(True)
plt.legend(loc='best')
plt.show()