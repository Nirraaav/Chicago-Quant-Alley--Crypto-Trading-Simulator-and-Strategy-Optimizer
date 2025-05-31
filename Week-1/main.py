import requests
import sqlite3
import csv
import time
from datetime import datetime, timedelta, timezone
from tqdm import tqdm

DB_NAME = "btc_options_week.db"
CANDLES_CSV = "btc_options_candles.csv"
DAYS_BACK = 14
RESOLUTION = "1d"
DAYS_AHEAD = 7
STRIKE_SPREAD = 15000
STRIKE_STEP = 100
BASE_URL = "https://api.delta.exchange/v2"
BASE_DATE = datetime(2025, 5, 25, tzinfo=timezone.utc)

def get_btc_price():
    url = f"{BASE_URL}/tickers"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()['result']
    for ticker in data:
        if ticker['symbol'] == "BTCUSDT":
            return float(ticker['spot_price'])
    raise Exception("BTCUSDT ticker not found")

def fetch_option_products():
    url = f"{BASE_URL}/products"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()['result']

def store_options_in_db(date_str, data_rows):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS options_{date_str} (
            symbol TEXT,
            strike_price INTEGER,
            expiry TEXT,
            option_type TEXT,
            volume REAL
        )
    ''')
    cursor.executemany(f'''
        INSERT INTO options_{date_str} (symbol, strike_price, expiry, option_type, volume)
        VALUES (?, ?, ?, ?, ?)
    ''', data_rows)
    conn.commit()
    conn.close()

def fetch_and_store_candles(symbols):
    end_time = int(time.time())
    start_time = end_time - DAYS_BACK * 24 * 60 * 60
    candles_url = f"{BASE_URL}/history/candles"
    with open(CANDLES_CSV, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["symbol", "time", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        for symbol in tqdm(symbols, desc="Fetching candles"):
            params = {
                "symbol": symbol,
                "resolution": RESOLUTION,
                "start": start_time,
                "end": end_time
            }
            response = requests.get(candles_url, params=params)
            data = response.json()
            if not (data.get("success") and data.get("result")):
                continue
            for candle in data["result"]:
                writer.writerow({
                    "symbol": symbol,
                    "time": datetime.fromtimestamp(candle["time"], timezone.utc).isoformat(),
                    "open": candle["open"],
                    "high": candle["high"],
                    "low": candle["low"],
                    "close": candle["close"],
                    "volume": candle["volume"]
                })

btc_price = get_btc_price()
all_products = fetch_option_products()
all_expiring_symbols = set()

for i in tqdm(range(DAYS_AHEAD), desc="Processing days"):
    current_date = BASE_DATE + timedelta(days=i)
    expiry_date = current_date + timedelta(days=3)
    expiry_str = expiry_date.strftime("%d%m%y")
    date_str = current_date.strftime("%Y%m%d")

    lower = int((btc_price - STRIKE_SPREAD) // STRIKE_STEP) * STRIKE_STEP
    upper = int((btc_price + STRIKE_SPREAD) // STRIKE_STEP) * STRIKE_STEP
    strike_range = set(range(lower, upper + 1, STRIKE_STEP))

    collected = []

    for product in all_products:
        if product['underlying_asset']['symbol'] != "BTC":
            continue
        if product['contract_type'] not in ['put_options', 'call_options']:
            continue
        symbol = product['symbol']
        if not symbol.endswith(expiry_str):
            continue
        strike_price = product.get('strike_price')
        if strike_price is None or int(strike_price) not in strike_range:
            continue
        volume = float(product.get('volume', 0.0))
        option_type = "call" if product['contract_type'] == "call_options" else "put"
        collected.append((symbol, int(strike_price), expiry_str, option_type, volume))
        all_expiring_symbols.add(symbol)

    store_options_in_db(date_str, collected)
print("here")
fetch_and_store_candles(list(all_expiring_symbols))
