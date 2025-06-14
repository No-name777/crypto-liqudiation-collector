import websocket
import json
import threading
from supabase_liquidation_collector import insert_liquidation

def listen_binance():
    def on_message(ws, message):
        try:
            data = json.loads(message)[0]
            liq = {
                "exchange": "Binance",
                "symbol": data["o"]["s"],
                "side": "LONG" if data["o"]["S"] == "BUY" else "SHORT",
                "price": float(data["o"]["p"]),
                "quantity": float(data["o"]["q"]),
            }
            insert_liquidation(liq)
            print("💥 Binance 청산:", liq)
        except Exception as e:
            print("❌ Binance 파싱 실패:", e)

    def run():
        url = "wss://fstream.binance.com/ws/!forceOrder@arr"
        ws = websocket.WebSocketApp(url, on_message=on_message)
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()