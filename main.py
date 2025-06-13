import time
from pymongo import MongoClient
from binance_ws import listen_binance_liquidations

from config import MONGO_URI

client = MongoClient(MONGO_URI)
db = client["liquidation_data"]

if __name__ == "__main__":
    print("📡 시작: 실시간 청산 수집 중...")
    listen_binance_liquidations(db)