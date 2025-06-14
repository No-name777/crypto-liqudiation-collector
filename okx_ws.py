import websocket
import json
import threading
from supabase_liquidation_collector import insert_liquidation

def listen_okx():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            for liq_data in data.get("data", []):
                price = float(liq_data["px"])
                quantity = float(liq_data["sz"])
                liq = {
                    "exchange": "OKX",
                    "symbol": liq_data["instId"],
                    "side": "LONG" if liq_data["side"] == "buy" else "SHORT",
                    "price": price,
                    "quantity": quantity,
                    "value": price * quantity,  # 💰 USD 청산 금액
                    "timestamp": int(liq_data["ts"])  # Unix ms
                }
                insert_liquidation(liq)
                print("💥 OKX 청산:", liq)
        except Exception as e:
            print("❌ OKX 파싱 실패:", e)

    def run():
        url = "wss://ws.okx.com:8443/ws/v5/public"
        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_open=lambda ws: ws.send(json.dumps({
                "op": "subscribe",
                "args": [{"channel": "liquidation", "instType": "FUTURES"}]
            }))
        )
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
