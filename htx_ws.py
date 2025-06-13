import websocket, json, threading, time

def listen_htx_liquidations(db):
    def on_message(ws, message):
        try:
            data = json.loads(message)
            liq = {
                "exchange": "HTX",
                "price": float(data["data"][0]["price"]),
                "quantity": float(data["data"][0]["amount"]),
                "timestamp": int(time.time() * 1000)
            }
            db.htx.insert_one(liq)
            print(f"💥 HTX 청산: {liq}")
        except Exception as e:
            print(f"❌ HTX 파싱 실패: {e}")

    def run():
        ws = websocket.WebSocketApp(
            "wss://api.hbdm.com/linear-swap-ws",
            on_message=on_message
        )
        ws.on_open = lambda ws: ws.send(json.dumps({
            "sub": "public.BTC-USDT.liquidation_orders",
            "id": "id1"
        }))
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
    while True:
        time.sleep(1)