from time import sleep
import time
from binance.client import Client
from binance.enums import *

api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
b_client = Client(api_key, api_secret)


times = 0
n = 30
for a in range(1, n+1):
    start = time.time()
    order = b_client.create_order(
        symbol='BTCUSDT',
        side=SIDE_BUY, type=ORDER_TYPE_LIMIT, timeInForce=TIME_IN_FORCE_GTC,
        quantity=0.001,
        price="30000")
    b_client.cancel_order(
                symbol=order["symbol"],
                orderId=order["orderId"]
            )
    d = time.time() - start
    print(a, d)
    times += d
    # sleep(0.5)
print(times / n)