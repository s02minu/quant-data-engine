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
    },
    "binance": {
        "BTCUSDT": "BTCUSDT",  # identity — no change needed
        "ETHUSDT": "ETHUSDT",
    }
}