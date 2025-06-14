from binance_ws import listen_binance
from bybit_ws import listen_bybit
from okx_ws import listen_okx
from htx_ws import listen_htx
from deribit_ws import listen_deribit
import time

if __name__ == "__main__":
    print("🛰️ 실시간 청산 데이터 수집 시작...")
    listen_binance()
    listen_bybit()
    listen_okx()
    listen_htx()
    listen_deribit()
    while True:
        time.sleep(1)