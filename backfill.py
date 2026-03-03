import time
import yfinance as yf
import psycopg2
from datetime import datetime, timezone

TIMESCALE_DSN = "host=timescaledb port=5432 dbname=stockdb user=stock password=stock123"

WATCHLIST = {
    "Technology": ["AAPL", "MSFT", "GOOGL", "NVDA", "META"],
    "Finance":    ["JPM",  "BAC",  "GS",    "MS",   "WFC"],
    "Healthcare": ["JNJ",  "PFE",  "UNH",   "ABBV", "MRK"],
    "Energy":     ["XOM",  "CVX",  "COP",   "SLB",  "EOG"],
}
SYMBOL_TO_SECTOR = {sym: sec for sec, syms in WATCHLIST.items() for sym in syms}
ALL_SYMBOLS = [sym for syms in WATCHLIST.values() for sym in syms]

conn = psycopg2.connect(TIMESCALE_DSN)
cur = conn.cursor()

for sym in ALL_SYMBOLS:
    try:
        hist = yf.Ticker(sym).history(period='5d', interval='1m')
        hist = hist[hist['Volume'] > 0]
        for ts, row in hist.iterrows():
            cur.execute(
                """INSERT INTO silver.ohlcv_1min
                   (symbol, sector, bar_time, open, high, low, close, volume, tick_count, processed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (symbol, bar_time) DO NOTHING""",
                (sym, SYMBOL_TO_SECTOR[sym], ts,
                 float(row.Open), float(row.High), float(row.Low), float(row.Close),
                 int(row.Volume), 1, datetime.now(timezone.utc).isoformat())
            )
        print(f'{sym}: {len(hist)} bars inserted')
    except Exception as e:
        print(f'{sym}: ERROR - {e}')
    time.sleep(3)  # avoid Yahoo Finance rate limiting

conn.commit()
cur.close()
conn.close()
print('Backfill done!')
