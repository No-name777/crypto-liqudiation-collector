import websocket, json, threading, time

def listen_okx_liquidations(db):
    def on_message(ws, message):
        try:
            data = json.loads(message)
            for liq in data.get("data", []):
                entry = {
                    "exchange": "OKX",
                    "price": float(liq["px"]),
                    "quantity": float(liq["sz"]),
                    "timestamp": int(time.time() * 1000)
                }
                db.okx.insert_one(entry)
                print(f"💥 OKX 청산: {entry}")
        except Exception as e:
            print(f"❌ OKX 파싱 실패: {e}")

    def run():
        ws = websocket.WebSocketApp(
            "wss://ws.okx.com:8443/ws/v5/public",
            on_message=on_message
        )
        ws.on_open = lambda ws: ws.send(json.dumps({
            "op": "subscribe",
            "args": [{"channel": "liquidation-orders", "instType": "FUTURES"}]
        }))
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
    while True:
        time.sleep(1)