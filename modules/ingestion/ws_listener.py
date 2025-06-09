# modules/ingestion/ws_listener.py

import os
import sqlite3
from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager
from datetime import datetime

# ─── Load environment ────────────────────────────────────────────────────────
BASE = os.path.dirname(__file__)
load_dotenv(os.path.join(BASE, '..', '..', '.env'))
API_KEY    = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')

# ─── Exclude large-cap perpetual USDT futures ────────────────────────────────
exclude = {
    'BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT',
    'ADAUSDT','TRXUSDT','STETHUSDT','DOGEUSDT'
}
info = Client(API_KEY, API_SECRET).futures_exchange_info()
symbols = [
    s['symbol'] for s in info['symbols']
    if s['contractType']=='PERPETUAL'
    and s['symbol'].endswith('USDT')
    and s['symbol'] not in exclude
]

# ─── SQLite DB setup ────────────────────────────────────────────────────────
DB = os.path.join(BASE, '..', '..', 'data', 'raw.db')
os.makedirs(os.path.dirname(DB), exist_ok=True)
conn = sqlite3.connect(DB, check_same_thread=False)
cur  = conn.cursor()
cur.execute('''
  CREATE TABLE IF NOT EXISTS futures_1m (
    symbol TEXT,
    timestamp INTEGER,
    open REAL, high REAL, low REAL, close REAL, volume REAL,
    PRIMARY KEY(symbol, timestamp)
  )
''')
conn.commit()

# ─── Message handler ────────────────────────────────────────────────────────
def handle_kline(msg):
    # heartbeat to show we're alive
    print("⚡ received raw kline message")
    if msg.get('e') != 'kline':
        return
    k   = msg['k']
    sym = msg['s']
    ts, o, h, l, c, v = (
        k['t'],
        float(k['o']), float(k['h']), float(k['l']),
        float(k['c']), float(k['v'])
    )
    cur.execute(
        'INSERT OR REPLACE INTO futures_1m VALUES (?,?,?,?,?,?,?)',
        (sym, ts, o, h, l, c, v)
    )
    conn.commit()
    print(f"← {sym} @ {datetime.utcfromtimestamp(ts/1000)}")

# ─── Main ──────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # bump queue size to avoid overflow when multiplexing lots of symbols
    twm = ThreadedWebsocketManager(
        api_key=API_KEY,
        api_secret=API_SECRET,
        max_queue_size=1000
    )
    twm.start()

    print(f"Subscribing to {len(symbols)} symbols, e.g.: {symbols[:5]} …")

    # ── Multiplex all 1m kline streams over ONE WebSocket ───────────────
    streams = [f"{sym.lower()}@kline_1m" for sym in symbols]
    twm.start_multiplex_socket(
        callback=handle_kline,
        streams=streams
    )

    print("Listening for live 1m bars (multiplex)… Press Ctrl+C to exit.")
    try:
        twm.join()       # blocks here, handles threads cleanly
    except KeyboardInterrupt:
        twm.stop()       # graceful shutdown
        conn.close()
