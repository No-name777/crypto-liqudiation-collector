import websocket, json, threading, time

def listen_deribit_liquidations(db):
    def on_message(ws, message):
        try:
            data = json.loads(message)
            for ev in data.get("params", {}).get("data", []):
                liq = {
                    "exchange": "Deribit",
                    "price": float(ev["price"]),
                    "quantity": float(ev["amount"]),
                    "timestamp": int(time.time() * 1000)
                }
                db.deribit.insert_one(liq)
                print(f"💥 DERIBIT 청산: {liq}")
        except Exception as e:
            print(f"❌ DERIBIT 파싱 실패: {e}")

    def run():
        ws = websocket.WebSocketApp(
            "wss://www.deribit.com/ws/api/v2",
            on_message=on_message
        )
        ws.on_open = lambda ws: ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/subscribe",
            "params": {"channels": ["liquidations.BTC-PERPETUAL.raw"]}
        }))
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
    while True:
        time.sleep(1)