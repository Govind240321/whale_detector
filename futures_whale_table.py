import streamlit as st
import requests
import asyncio
import websockets
import threading
import json
import pandas as pd
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
threshold = st.sidebar.number_input("Whale Threshold (USDT)", value=DEFAULT_THRESHOLD, step=10000)
max_rows = st.sidebar.number_input("Max rows to display", value=100, step=10)

# Initialize WebSocket listener only once
def _init_stream():
    st.session_state.ws_queue = Queue()
    st.session_state.ws_started = True
    st.session_state.symbols_data = {}  # store unique symbol records

    def ws_worker(queue, threshold):
        # Fetch all USDT perpetual symbols, excluding certain bases
        def get_symbols():
            info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()
            syms = []
            for s in info['symbols']:
                base = s['symbol'][:-4].lower()
                if (s['contractType'] == 'PERPETUAL'
                        and s['quoteAsset'] == 'USDT'
                        and base not in EXCLUDE_BASES):
                    syms.append(s['symbol'].lower())
            return syms

        symbols = get_symbols()
        symbols_upper = [sym.upper() for sym in symbols]

        # Fetch 24h stats once
        stats = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr").json()
        change24h = {
            item['symbol']: float(item['priceChangePercent'])
            for item in stats
            if item['symbol'] in symbols_upper
        }

        streams = "/".join(f"{sym}@aggTrade" for sym in symbols)
        url = BINANCE_WS_BASE + streams

        last_price = {}
        counts = {}

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def handler():
            async with websockets.connect(url, ping_interval=None) as ws:
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)['data']
                    symbol = data['s']
                    price = float(data['p'])
                    qty = float(data['q'])
                    is_buy = not data['m']
                    usdt_val = price * qty

                    if usdt_val >= threshold:
                        # 24h change static
                        pct24 = change24h.get(symbol, 0.0)
                        # change since last
                        prev = last_price.get(symbol)
                        pct_now = (price - prev) / prev * 100 if prev else 0.0
                        last_price[symbol] = price
                        # count increment
                        cnt = counts.get(symbol, 0) + 1
                        counts[symbol] = cnt
                        # IST timestamp
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

# Prepare data for display
all_records = list(st.session_state.symbols_data.values())
# Sort by count desc
sorted_records = sorted(all_records, key=lambda x: x['Count'], reverse=True)
# Limit rows
display_records = sorted_records[:max_rows]

# Display table with colored Type column
if display_records:
    df = pd.DataFrame(display_records)
    styled = df.style.applymap(
        lambda v: 'color: green' if v == 'BUY' else 'color: red',
        subset=['Type']
    )
    st.dataframe(styled)
else:
    st.info("Waiting for whale trades...")
