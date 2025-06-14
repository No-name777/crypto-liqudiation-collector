import websocket
import json
import threading
from supabase_liquidation_collector import insert_liquidation

def listen_deribit():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            for liq_data in data.get("params", {}).get("data", []):
                liq = {
                    "exchange": "Deribit",
                    "symbol": liq_data["instrument_name"],
                    "side": "LONG" if liq_data["direction"] == "buy" else "SHORT",
                    "price": float(liq_data["price"]),
                    "quantity": float(liq_data["amount"]),
                }
                insert_liquidation(liq)
                print("💥 Deribit 청산:", liq)
        except Exception as e:
            print("❌ Deribit 파싱 실패:", e)

    def run():
        url = "wss://www.deribit.com/ws/api/v2"
        ws = websocket.WebSocketApp(url,
                                    on_message=on_message,
                                    on_open=lambda ws: ws.send(json.dumps({
                                        "jsonrpc": "2.0",
                                        "method": "public/subscribe",
                                        "id": 1,
                                        "params": {
                                            "channels": ["liquidations.BTC-PERPETUAL.raw"]
                                        }
                                    })))
        ws.run_forever()

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()