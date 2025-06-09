# modules/ingestion/ws_listener.py

import os, sqlite3, time
from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager
from datetime import datetime

# Load env
BASE = os.path.dirname(__file__)
load_dotenv(os.path.join(BASE, '..', '..', '.env'))
API_KEY    = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')

# Exclude large caps
exclude = {
    'BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT',
    'ADAUSDT','TRXUSDT','STETHUSDT','DOGEUSDT'
}
info    = Client(API_KEY, API_SECRET).futures_exchange_info()
symbols = [
    s['symbol'] for s in info['symbols']
    if s['contractType']=='PERPETUAL'
    and s['symbol'].endswith('USDT')
    and s['symbol'] not in exclude
]

# SQLite setup
DB   = os.path.join(BASE, '..', '..', 'data', 'raw.db')
conn = sqlite3.connect(DB, check_same_thread=False)
cur  = conn.cursor()
cur.execute('''
CREATE TABLE IF NOT EXISTS futures_1m (
    symbol TEXT, timestamp INTEGER,
    open REAL, high REAL, low REAL, close REAL, volume REAL,
    PRIMARY KEY(symbol,timestamp)
)''')
conn.commit()

def handle_kline(msg):
    if msg.get('e') != 'kline': 
        return
    k   = msg['k']
    sym = msg['s']
    ts  = k['t']
    o,h,l,c,v = map(float, (k['o'], k['h'], k['l'], k['c'], k['v']))
    cur.execute(
        'INSERT OR REPLACE INTO futures_1m VALUES (?,?,?,?,?,?,?)',
        (sym, ts, o, h, l, c, v)
    )
    conn.commit()
    print(f"← {sym} {datetime.utcfromtimestamp(ts/1000)}")

if __name__ == '__main__':
    twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm.start()
    for sym in symbols:
        # Correct subscription method for futures 1m kline streams:
        twm.start_kline_futures_socket(callback=handle_kline, symbol=sym, interval='1m')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        twm.stop()
        conn.close()
