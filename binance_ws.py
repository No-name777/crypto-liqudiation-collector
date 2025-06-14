import websocket
import json
import threading
from supabase_liquidation_collector import insert_liquidation

def listen_binance():
    def on_message(ws, message):
        try:
            print("📦 Binance 원시 메시지:", message)
            data = json.loads(message)[0]
            price = float(data["o"]["p"])
            quantity = float(data["o"]["q"])
            liq = {
                "exchange": "Binance",
                "symbol": data["o"]["s"],
                "side": "LONG" if data["o"]["S"] == "BUY" else "SHORT",
                "price": price,
                "quantity": quantity,
                "value": price * quantity,  # 💰 USD 청산 금액 추가
                "timestamp": int(data["E"]),  # 이벤트 발생 시점 (ms 단위)
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
