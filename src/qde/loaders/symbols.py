"""
A simple dictionary
mapping the symbol, so no need for guessing how the ticker string is structured on each exchange.
One canonical symbol for the  project.
"""

SYMBOL_MAP = {
    "yfinance": {
        "BTCUSDT": "BTC-USD",
        "ETHUSDT": "ETH-USD",
        "SOLUSDT": "SOL-USD",
        "SPY": "SPY",      # S&P 500 ETF
        "QQQ": "QQQ",      # Nasdaq 100 ETF
        "GLD": "GLD",      # Gold ETF
        "TLT": "TLT",      # 20+ Year Treasury Bond ETF
        "DX-Y.NYB": "DX-Y.NYB",  # US Dollar Index (DXY)
    },
    "binance": {
        "BTCUSDT": "BTCUSDT",  # identity — no change needed
        "ETHUSDT": "ETHUSDT",
    },

}