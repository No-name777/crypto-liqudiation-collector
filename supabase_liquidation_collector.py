import websocket
import json
import threading
import time
import requests
from datetime import datetime

SUPABASE_URL = "https://dlunenowfovxasecdnes.supabase.co"
SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRsdW5lbm93Zm92eGFzZWNkbmVzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDk4Njc1OTMsImV4cCI6MjA2NTQ0MzU5M30.8uzPKPM4Kj13fJ2CaMi4-4ZTwhakWucsnqI0fXjbSLM"
SUPABASE_TABLE = "liquidations"
SUPABASE_HEADERS = {
    "apikey": SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Content-Type": "application/json"
}

def insert_liquidation(data):
    print("📥 insert_liquidation 진입:", data)
    payload = {
        "exchange": data["exchange"],
        "symbol": data["symbol"],
        "side": data["side"],
        "price": data["price"],
        "quantity": data["quantity"],
        "created_at": datetime.utcnow().isoformat() + "Z"
    }
    response = requests.post(f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}", headers=SUPABASE_HEADERS, json=payload)
    print("📦 요청 페이로드:", payload)
    print("📨 Supabase 응답 코드:", response.status_code)
    print("📨 Supabase 응답 내용:", response.text)
    if response.status_code != 201:
        print(f"❌ {data['exchange']} 저장 실패:", response.text)

# Binance
def listen_binance():
    def on_message(ws, message):
        try:
            raw = json.loads(message)
            data_list = raw if isinstance(raw, list) else [raw]
            for data in data_list:
                print("🧾 Binance 수신:", data)
                liq = {
                    "exchange": "Binance",
                    "symbol": data["o"]["s"],
                    "side": "LONG" if data["o"]["S"] == "BUY" else "SHORT",
                    "price": float(data["o"]["p"]),
                    "quantity": float(data["o"]["q"]),
                }
                insert_liquidation(liq)
                print("💥 Binance:", liq)
        except Exception as e:
            print("❌ Binance 파싱 실패:", e)

    def run():
        url = "wss://fstream.binance.com/ws/!forceOrder@arr"
        ws = websocket.WebSocketApp(url, on_message=on_message)
        ws.run_forever()

    threading.Thread(target=run, daemon=True).start()

# Bybit
def listen_bybit():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if "data" in data and data["topic"] == "liquidation":
                for d in data["data"]:
                    liq = {
                        "exchange": "Bybit",
                        "symbol": d["symbol"],
                        "side": "LONG" if d["side"] == "Buy" else "SHORT",
                        "price": float(d["price"]),
                        "quantity": float(d["size"]),
                    }
                    insert_liquidation(liq)
                    print("💥 Bybit:", liq)
        except Exception as e:
            print("❌ Bybit 파싱 실패:", e)

    def run():
        url = "wss://stream.bybit.com/v5/public/linear"
        ws = websocket.WebSocketApp(
            url,
            on_open=lambda ws: ws.send(json.dumps({
                "op": "subscribe",
                "args": ["liquidation.BTCUSDT"]
            })),
            on_message=on_message
        )
        ws.run_forever()

    threading.Thread(target=run, daemon=True).start()

# OKX
def listen_okx():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if "arg" in data and "data" in data:
                for d in data["data"]:
                    liq = {
                        "exchange": "OKX",
                        "symbol": d["instId"],
                        "side": "LONG" if d["side"] == "buy" else "SHORT",
                        "price": float(d["price"]),
                        "quantity": float(d["sz"]),
                    }
                    insert_liquidation(liq)
                    print("💥 OKX:", liq)
        except Exception as e:
            print("❌ OKX 파싱 실패:", e)

    def run():
        url = "wss://ws.okx.com:8443/ws/v5/public"
        ws = websocket.WebSocketApp(
            url,
            on_open=lambda ws: ws.send(json.dumps({
                "op": "subscribe",
                "args": [{"channel": "liquidation", "instType": "SWAP"}]
            })),
            on_message=on_message
        )
        ws.run_forever()

    threading.Thread(target=run, daemon=True).start()

# HTX (Huobi)
def listen_htx():
    import gzip
    def on_message(ws, message):
        try:
            msg = gzip.decompress(message).decode()
            data = json.loads(msg)
            if "data" in data and "topic" in data:
                for d in data["data"]:
                    liq = {
                        "exchange": "HTX",
                        "symbol": data["topic"].split(".")[1],
                        "side": "LONG" if d["direction"] == "buy" else "SHORT",
                        "price": float(d["price"]),
                        "quantity": float(d["amount"]),
                    }
                    insert_liquidation(liq)
                    print("💥 HTX:", liq)
        except Exception as e:
            print("❌ HTX 파싱 실패:", e)

    def on_open(ws):
        sub = {
            "sub": "public.BTC-USDT.liquidation_orders",
            "id": "id1"
        }
        ws.send(json.dumps(sub))

    def run():
        url = "wss://api.hbdm.com/linear-swap-ws"
        ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message)
        ws.run_forever()

    threading.Thread(target=run, daemon=True).start()

# Deribit
def listen_deribit():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if "params" in data and "data" in data["params"]:
                d = data["params"]["data"]
                liq = {
                    "exchange": "Deribit",
                    "symbol": d["instrument_name"],
                    "side": "SHORT" if d.get("direction") == "sell" else "LONG",
                    "price": float(d["price"]),
                    "quantity": float(d["amount"]),
                }
                insert_liquidation(liq)
                print("💥 Deribit:", liq)
        except Exception as e:
            print("❌ Deribit 파싱 실패:", e)

    def on_open(ws):
        ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": 42,
            "method": "public/subscribe",
            "params": {
                "channels": ["liquidation.BTC-PERPETUAL.raw"]
            }
        }))

    def run():
        url = "wss://www.deribit.com/ws/api/v2"
        ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message)
        ws.run_forever()

    threading.Thread(target=run, daemon=True).start()

# 실행
if __name__ == "__main__":
    print("🛰️ 실시간 청산 데이터 수집 시작...")
    listen_binance()
    listen_bybit()
    listen_okx()
    listen_htx()
    listen_deribit()
    while True:
        time.sleep(1)
