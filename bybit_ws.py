import websocket, json, threading, time

def listen_bybit_liquidations(db):
    def on_message(ws, message):
        try:
            data = json.loads(message)
            for item in data.get("data", []):
                liq = {
                    "exchange": "Bybit",
                    "price": float(item["price"]),
                    "quantity": float(item["qty"]),
                    "timestamp": int(time.time() * 1000)
                }
                db.bybit.insert_one(liq)
                print(f"💥 BYBIT 청산: {liq}")
        except Exception as e:
            print(f"❌ BYBIT 파싱 실패: {e}")

    def run():
        ws = websocket.WebSocketApp(
            "wss://stream.bybit.com/v5/public/linear",
            on_message=on_message
        )
        ws.on_open = lambda ws: ws.send(json.dumps({
            "op": "subscribe",
            "args": ["publicTrade.BTCUSDT"]
        }))
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
    while True:
        time.sleep(1)