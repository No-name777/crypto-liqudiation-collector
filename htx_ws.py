import websocket
import json
import gzip
import threading
import time
from supabase_liquidation_collector import insert_liquidation

def listen_htx():
    def on_message(ws, message):
        try:
            decompressed_data = gzip.decompress(message).decode("utf-8")
            data = json.loads(decompressed_data)
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
                        "value": price * quantity,  # 💰 USD 청산 금액
                        "timestamp": int(d["created_at"])  # Unix ms
                    }
                    insert_liquidation(liq)
                    print("💥 HTX 청산:", liq)
            elif "ping" in data:
                ws.send(json.dumps({"pong": data["ping"]}))
        except Exception as e:
            print("❌ HTX 파싱 실패:", e)

    def on_open(ws):
        sub_msg = {
            "sub": "public.*.liquidation.orders",
            "id": "id1"
        }
        ws.send(json.dumps(sub_msg))

    def run():
        url = "wss://api.hbdm.com/linear-swap-ws"
        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message
        )
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
