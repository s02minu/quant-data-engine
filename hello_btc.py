import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf

# Downloading the OHLCV from yfinance
data = yf.download(
    tickers= 'BTC-USD',
    start='2023-01-01',
    interval='1d',
    group_by='ticker',
    prepost=False,
    auto_adjust=True,
    repair=False,
)

df = pd.DataFrame(data)
print(df.head())
print(df.shape[0])

# Change the name o the columns for simplicity
df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

# Highest close price and date it occurred
highest_close = df["Close"].max()
date_close = df["Close"].idxmax()

# Plot the Price and date in a line chart
y = df['Close']


plt.figure(figure = (14,5))
plt.plot(df.index, y)
plt.savefig("btc_close.png", dpi=300, bbox_inches='tight', transparent=True)

plt.xlabel('Date')
plt.ylabel('Close Price')
plt.title('BTC-USD Daily Close')
plt.show()





