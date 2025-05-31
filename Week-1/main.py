import requests
import sqlite3
import csv
import time
from datetime import datetime, timedelta, timezone
from tqdm import tqdm

db_path = "btc_options_week.db"
csv_path = "btc_options_candles.csv"
days_lookback = 14
interval = "1d"
days_forward = 7
strike_padding = 15000
strike_gap = 100
api_base = "https://api.delta.exchange/v2"
reference_day = datetime(2025, 5, 25, tzinfo=timezone.utc)

tickers_response = requests.get(f"{api_base}/tickers")
tickers_response.raise_for_status()
tickers_data = tickers_response.json()["result"]
btc_price = None
for item in tickers_data:
    if item["symbol"] == "BTCUSDT":
        btc_price = float(item["spot_price"])
        break
if btc_price is None:
    raise Exception("BTCUSDT ticker not found")

products_response = requests.get(f"{api_base}/products")
products_response.raise_for_status()
product_list = products_response.json()["result"]

option_symbols = set()

for shift in tqdm(range(days_forward), desc="Processing days"):
    current_day = reference_day + timedelta(days=shift)
    expiry_day = current_day + timedelta(days=3)
    expiry_code = expiry_day.strftime("%d%m%y")
    current_key = current_day.strftime("%Y%m%d")

    low_bound = int((btc_price - strike_padding) // strike_gap) * strike_gap
    high_bound = int((btc_price + strike_padding) // strike_gap) * strike_gap
    valid_strikes = set(range(low_bound, high_bound + 1, strike_gap))

    collected_entries = []

    for p in product_list:
        if p["underlying_asset"]["symbol"] != "BTC":
            continue
        if p["contract_type"] not in ["call_options", "put_options"]:
            continue
        if not p["symbol"].endswith(expiry_code):
            continue
        strike = p.get("strike_price")
        if strike is None or int(strike) not in valid_strikes:
            continue
        vol = float(p.get("volume", 0.0))
        kind = "call" if p["contract_type"] == "call_options" else "put"
        collected_entries.append((p["symbol"], int(strike), expiry_code, kind, vol))
        option_symbols.add(p["symbol"])

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS options_{current_key} (
            symbol TEXT,
            strike_price INTEGER,
            expiry TEXT,
            option_type TEXT,
            volume REAL
        )
    """)
    cur.executemany(f"""
        INSERT INTO options_{current_key} (symbol, strike_price, expiry, option_type, volume)
        VALUES (?, ?, ?, ?, ?)
    """, collected_entries)
    conn.commit()
    conn.close()

end_unix = int(time.time())
start_unix = end_unix - days_lookback * 86400
candles_endpoint = f"{api_base}/history/candles"

with open(csv_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["symbol", "time", "open", "high", "low", "close", "volume"])
    writer.writeheader()

    for sym in tqdm(option_symbols, desc="Downloading candles"):
        params = {
            "symbol": sym,
            "resolution": interval,
            "start": start_unix,
            "end": end_unix
        }
        r = requests.get(candles_endpoint, params=params)
        result = r.json()
        if not (result.get("success") and result.get("result")):
            continue
        for row in result["result"]:
            writer.writerow({
                "symbol": sym,
                "time": datetime.fromtimestamp(row["time"], timezone.utc).isoformat(),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"]
            })
