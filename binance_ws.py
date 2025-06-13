import websocket, json, threading, time

def on_message(ws, message):
    try:
        data = json.loads(message)
        print("📩 수신 데이터:", data)  # 이 줄 추가
        liq = {
            "exchange": "Binance",
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "timestamp": int(float(data["T"]))
        }
        db.binance.insert_one(liq)
        print(f"💥 청산: {liq}")
except Exception as e:
    print(f"❌ [binance] 파싱 실패: {e}")


    def run():
        ws = websocket.WebSocketApp(
            "wss://fstream.binance.com/ws/!forceOrder@arr",
            on_message=on_message
        )
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
    while True:
        time.sleep(1)
