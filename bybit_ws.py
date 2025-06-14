import websocket
import json
import threading
from supabase_liquidation_collector import insert_liquidation

def listen_bybit():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            liq_data = data["data"]
            price = float(liq_data["price"])
            quantity = float(liq_data["qty"])
            liq = {
                "exchange": "Bybit",
                "symbol": liq_data["symbol"],
                "side": "LONG" if liq_data["side"] == "Buy" else "SHORT",
                "price": price,
                "quantity": quantity,
                "value": price * quantity,  # 💰 USD 청산 금액
                "timestamp": int(liq_data["time"])  # Unix time in ms
            }
            insert_liquidation(liq)
            print("💥 Bybit 청산:", liq)
        except Exception as e:
            print("❌ Bybit 파싱 실패:", e)

    def run():
        url = "wss://stream.bybit.com/realtime"
        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_open=lambda ws: ws.send(json.dumps({
                "op": "subscribe",
                "args": ["liquidation"]
            }))
        )
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
