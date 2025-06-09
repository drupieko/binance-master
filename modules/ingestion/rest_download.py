import os
import sqlite3
import time
from binance.client import Client
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment
BASE = os.path.dirname(__file__)
load_dotenv(os.path.join(BASE, '..', '..', '.env'))

# Init Binance client
client = Client(os.getenv('BINANCE_API_KEY'), os.getenv('BINANCE_API_SECRET'))

# Exclude large-cap perpetual USDT futures
exclude = {
    'BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT',
    'ADAUSDT','TRXUSDT','STETHUSDT','DOGEUSDT'
}
info    = client.futures_exchange_info()
symbols = [
    s['symbol'] for s in info['symbols']
    if s['contractType']=='PERPETUAL'
    and s['symbol'].endswith('USDT')
    and s['symbol'] not in exclude
]

# Prepare SQLite
db_dir = os.path.join(BASE, '..', '..', 'data')
os.makedirs(db_dir, exist_ok=True)
DB = os.path.join(db_dir, 'raw.db')
conn = sqlite3.connect(DB)
cur  = conn.cursor()

# Main OHLCV table
cur.execute('''
CREATE TABLE IF NOT EXISTS futures_1m (
    symbol TEXT,
    timestamp INTEGER,
    open REAL, high REAL, low REAL, close REAL, volume REAL,
    PRIMARY KEY(symbol, timestamp)
)''')

# Progress checkpoint table
cur.execute('''
CREATE TABLE IF NOT EXISTS futures_backfill_progress (
    symbol TEXT PRIMARY KEY,
    last_timestamp INTEGER
)''')

conn.commit()

# Backfill range and limits
end_ts   = datetime.utcnow()
start_ts = end_ts - timedelta(days=5*365)
limit    = 1000
requests = 0

for sym in symbols:
    print(f"→ {sym}")

    # Check last timestamp from checkpoint
    cur.execute('SELECT last_timestamp FROM futures_backfill_progress WHERE symbol = ?', (sym,))
    row = cur.fetchone()
    if row:
        start_ms = row[0] + 1
    else:
        start_ms = int(start_ts.timestamp() * 1000)

    rows = []

    while start_ms < int(end_ts.timestamp() * 1000):
        try:
            batch = client.futures_klines(
                symbol=sym,
                interval=Client.KLINE_INTERVAL_1MINUTE,
                startTime=start_ms,
                limit=limit
            )
        except Exception as e:
            print(f"  Error for {sym} at {start_ms}: {e}")
            time.sleep(5)
            continue

        if not batch:
            break

        for k in batch:
            ts, o, h, l, c, v, *_ = k
            rows.append((sym, ts, float(o), float(h), float(l), float(c), float(v)))
        start_ms = batch[-1][0] + 1

        # Insert rows and update checkpoint
        if rows:
            cur.executemany(
                'INSERT OR REPLACE INTO futures_1m VALUES (?,?,?,?,?,?,?)',
                rows
            )
            cur.execute(
                'INSERT OR REPLACE INTO futures_backfill_progress VALUES (?, ?)',
                (sym, rows[-1][1])
            )
            conn.commit()
            rows.clear()

        requests += 1
        if requests >= 1000:
            print("  Rate limit reached → sleeping 60s")
            time.sleep(60)
            requests = 0
        else:
            time.sleep(0.06)

        print(f"  downloaded up to {datetime.utcfromtimestamp(start_ms/1000)}")

print("✓ 5-year historical download complete.")
conn.close()
