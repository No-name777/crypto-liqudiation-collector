import websocket
import json
import threading
from supabase_liquidation_collector import insert_liquidation

def listen_htx():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            tick = data.get("tick", {})
            liq = {
                "exchange": "HTX",
                "symbol": data["ch"].split(".")[1],
                "side": "LONG" if tick.get("direction") == "buy" else "SHORT",
                "price": float(tick["price"]),
                "quantity": float(tick["amount"]),
            }
            insert_liquidation(liq)
            print("💥 HTX 청산:", liq)
        except Exception as e:
            print("❌ HTX 파싱 실패:", e)

    def run():
        url = "wss://api.hbdm.com/linear-swap-ws"
        ws = websocket.WebSocketApp(url,
                                    on_message=on_message,
                                    on_open=lambda ws: ws.send(json.dumps({
                                        "sub": "public.BTC-USDT.liquidation_orders", "id": "id1"})))
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()