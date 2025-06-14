import websocket
import json
import threading
import time
import requests
from datetime import datetime, timezone
import gzip

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
        "value": data.get("value"),
        "timestamp": data.get("timestamp"),
        "created_at": datetime.utcnow().isoformat()
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
            data_list = json.loads(message)
            if not isinstance(data_list, list):
                data_list = [data_list]
            for data in data_list:
                o = data["o"]
                price = float(o["p"])
                quantity = float(o["q"])
                liq = {
                    "exchange": "Binance",
                    "symbol": o["s"],
                    "side": "LONG" if o["S"] == "BUY" else "SHORT",
                    "price": price,
                    "quantity": quantity,
                    "value": price * quantity,
                    "timestamp": int(data["E"])
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
            if "data" in data:
                for d in data["data"]:
                    price = float(d["price"])
                    quantity = float(d["size"])
                    liq = {
                        "exchange": "Bybit",
                        "symbol": d["symbol"],
                        "side": "LONG" if d["side"] == "Buy" else "SHORT",
                        "price": price,
                        "quantity": quantity,
                        "value": price * quantity,
                        "timestamp": int(d["ts"])
                    }
                    insert_liquidation(liq)
                    print("💥 Bybit:", liq)
        except Exception as e:
            print("❌ Bybit 파싱 실패:", e)

    def run():
        url = "wss://stream.bybit.com/realtime"
        ws = websocket.WebSocketApp(url,
            on_message=on_message,
            on_open=lambda ws: ws.send(json.dumps({
                "op": "subscribe",
                "args": ["liquidation"]
            }))
        )
        ws.run_forever()

    threading.Thread(target=run, daemon=True).start()

# OKX
def listen_okx():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            for d in data.get("data", []):
                price = float(d["px"])
                quantity = float(d["sz"])
                liq = {
                    "exchange": "OKX",
                    "symbol": d["instId"],
                    "side": "LONG" if d["side"] == "buy" else "SHORT",
                    "price": price,
                    "quantity": quantity,
                    "value": price * quantity,
                    "timestamp": int(d["ts"])
                }
                insert_liquidation(liq)
                print("💥 OKX:", liq)
        except Exception as e:
            print("❌ OKX 파싱 실패:", e)

    def run():
        url = "wss://ws.okx.com:8443/ws/v5/public"
        ws = websocket.WebSocketApp(url,
            on_message=on_message,
            on_open=lambda ws: ws.send(json.dumps({
                "op": "subscribe",
                "args": [{"channel": "liquidation", "instType": "FUTURES"}]
            }))
        )
        ws.run_forever()

    threading.Thread(target=run, daemon=True).start()

# HTX
def listen_htx():
    def on_message(ws, message):
        try:
            msg = gzip.decompress(message).decode()
            data = json.loads(msg)
            if "data" in data:
                for d in data["data"]:
                    price = float(d["price"])
                    quantity = float(d["amount"])
                    liq = {
                        "exchange": "HTX",
                        "symbol": d["symbol"].upper(),
                        "side": "LONG" if d["direction"] == "buy" else "SHORT",
                        "price": price,
                        "quantity": quantity,
                        "value": price * quantity,
                        "timestamp": int(d["created_at"])
                    }
                    insert_liquidation(liq)
                    print("💥 HTX:", liq)
            elif "ping" in data:
                ws.send(json.dumps({"pong": data["ping"]}))
        except Exception as e:
            print("❌ HTX 파싱 실패:", e)

    def on_open(ws):
        ws.send(json.dumps({
            "sub": "public.*.liquidation.orders",
            "id": "id1"
        }))

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
            for d in data.get("params", {}).get("data", []):
                price = float(d["price"])
                quantity = float(d["amount"])
                liq = {
                    "exchange": "Deribit",
                    "symbol": d["instrument_name"],
                    "side": "LONG" if d["direction"] == "buy" else "SHORT",
                    "price": price,
                    "quantity": quantity,
                    "value": price * quantity,
                    "timestamp": int(d["timestamp"])
                }
                insert_liquidation(liq)
                print("💥 Deribit:", liq)
        except Exception as e:
            print("❌ Deribit 파싱 실패:", e)

    def on_open(ws):
        ws.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 1,
            "params": {
                "channels": ["liquidations.BTC-PERPETUAL.raw"]
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
