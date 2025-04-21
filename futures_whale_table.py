import streamlit as st
import requests
import asyncio
import websockets
import threading
import json
import pandas as pd
import time
from queue import Queue
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

# Configuration
DEFAULT_THRESHOLD = 100000  # USDT
BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="
EXCLUDE_BASES = {"btc", "eth", "sol"}

# Streamlit UI
st.set_page_config(page_title="Binance Futures Whale Detector", layout="wide")
st.title("🐋 Binance Futures Whale Buy/Sell Detector")
threshold = st.sidebar.number_input(
    "Whale Threshold (USDT)", value=DEFAULT_THRESHOLD, step=10000
)
max_rows = st.sidebar.number_input("Max rows to display", value=100, step=10)

# Function to reliably fetch symbols with retry
def fetch_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    while True:
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            if 'symbols' in data:
                syms = []
                for s in data['symbols']:
                    base = s['symbol'][:-4].lower()
                    if (
                        s.get('contractType') == 'PERPETUAL'
                        and s.get('quoteAsset') == 'USDT'
                        and base not in EXCLUDE_BASES
                    ):
                        syms.append(s['symbol'].lower())
                return syms
            else:
                print(f"⚠️ Unexpected response structure, retrying in 5s: {data}")
        except Exception as e:
            print(f"⚠️ Error fetching symbols, retrying in 5s: {e}")
        time.sleep(5)

# Initialize WebSocket listener only once
def _init_stream():
    st.session_state.ws_queue = Queue()
    st.session_state.ws_started = True
    st.session_state.symbols_data = {}

    symbols = fetch_symbols()
    symbols_upper = [sym.upper() for sym in symbols]

    # Fetch 24h stats once with retry
    change24h = {}
    stat_url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    while True:
        try:
            stats = requests.get(stat_url, timeout=10).json()
            change24h = {
                item['symbol']: float(item.get('priceChangePercent', 0))
                for item in stats
                if item.get('symbol') in symbols_upper
            }
            break
        except Exception as e:
            print(f"⚠️ Error fetching 24h stats, retrying in 5s: {e}")
            time.sleep(5)

    streams = "/".join(f"{sym}@aggTrade" for sym in symbols)
    ws_url = BINANCE_WS_BASE + streams

    last_price = {}
    counts = {}

    def ws_worker(queue, threshold):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def handler():
            async with websockets.connect(ws_url, ping_interval=None) as ws:
                while True:
                    try:
                        msg = await ws.recv()
                        data = json.loads(msg).get('data', {})
                        symbol = data.get('s')
                        price = float(data.get('p', 0))
                        qty = float(data.get('q', 0))
                        is_buy = not data.get('m', True)
                        usdt_val = price * qty

                        if usdt_val >= threshold and symbol:
                            pct24 = change24h.get(symbol, 0.0)
                            prev = last_price.get(symbol)
                            pct_now = (price - prev) / prev * 100 if prev else 0.0
                            last_price[symbol] = price
                            cnt = counts.get(symbol, 0) + 1
                            counts[symbol] = cnt
                            ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
                            ts = ist.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' IST'

                            record = {
                                "Timestamp": ts,
                                "Pair": symbol,
                                "Type": "BUY" if is_buy else "SELL",
                                "USDT Amount": f"{usdt_val:,.2f}",
                                "Price": price,
                                "Qty": qty,
                                "Δ Since Last (%)": f"{pct_now:+.2f}%",
                                "Δ 24 h (%)": f"{pct24:+.2f}%",
                                "Count": cnt
                            }
                            queue.put(record)
                    except Exception as e:
                        print(f"⚠️ WebSocket receive error: {e}")
                        await asyncio.sleep(1)

        loop.run_until_complete(handler())

    threading.Thread(
        target=ws_worker,
        args=(st.session_state.ws_queue, threshold),
        daemon=True
    ).start()

# Only initialize once
if 'ws_started' not in st.session_state:
    _init_stream()

# Auto‑refresh every 500 ms
st_autorefresh(interval=500)

# Process incoming records
incoming = []
while not st.session_state.ws_queue.empty():
    incoming.append(st.session_state.ws_queue.get())

# Update unique symbol data
for rec in incoming:
    st.session_state.symbols_data[rec['Pair']] = rec

# Prepare and display sorted data
all_records = list(st.session_state.symbols_data.values())
sorted_records = sorted(all_records, key=lambda x: x['Count'], reverse=True)
display_records = sorted_records[:max_rows]

if display_records:
    df = pd.DataFrame(display_records)
    styled = df.style.applymap(
        lambda v: 'color: green' if v == 'BUY' else 'color: red',
        subset=['Type']
    )
    st.dataframe(styled)
else:
    st.info("Waiting for whale trades...")
