import time
from pymongo import MongoClient

from binance_ws import listen_binance_liquidations
from bybit_ws import listen_bybit_liquidations
from okx_ws import listen_okx_liquidations
from htx_ws import listen_htx_liquidations
from deribit_ws import listen_deribit_liquidations

from config import MONGO_URI

client = MongoClient(MONGO_URI)
db = client["liquidation_data"]

if __name__ == "__main__":
    print("📡 시작: 실시간 청산 수집 중...")

    listen_binance_liquidations(db)
    listen_bybit_liquidations(db)
    listen_okx_liquidations(db)
    listen_htx_liquidations(db)
    listen_deribit_liquidations(db)

    while True:
        time.sleep(1)
