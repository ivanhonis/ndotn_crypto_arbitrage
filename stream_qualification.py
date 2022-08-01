import datetime
import sys
import time

from binance import ThreadedWebsocketManager, Client

api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
action_count = 0

# b_client = Client(api_key, api_secret)




def main():
    print('itt')


    def print_msg(msg):
        if msg['stream'] == 'btcusdt@depth@100ms':
            print(action_count, 'D', msg['data']['b'][0][0], msg['data']['a'][0][0], " - ",
                  msg['data']['b'][1][0], msg['data']['a'][1][0], " - ",
                  "Es", round((float(msg['data']['b'][1][0]) + float(msg['data']['a'][1][0])) / 2, 8))
        elif msg['stream'] == 'btcusdt@bookTicker':
            print(action_count, '   b', msg['data']['b'], msg['data']['a'],
                  "Es", round((float(msg['data']['b']) + float(msg['data']['a'])) / 2, 8))
        elif msg['stream'] == 'btcusdt@trade':
            print(action_count, '      t', msg['data']['p'], 'id', msg['data']['t'])

    def handle_socket_message(msg):
        global action_count
        print_msg(msg)

        action_count += 1
        if action_count == 50:
            print_msg(msg)
            start = datetime.datetime.now()
            order = b_client.order_market_buy(
                symbol='BTCUSDT',
                quantity=str(0.001))
            print("speed 1", datetime.datetime.now() - start )
            print("buy1", order['fills'][0]['price'])
            print(order)
            order = b_client.order_market_buy(
                symbol='BTCUSDT',
                quantity=str(0.001))
            print("speed 2", datetime.datetime.now() - start )

            print("buy2", order['fills'][0]['price'])
            order = b_client.order_market_sell(
                symbol='BTCUSDT',
                quantity=str(0.002))
            print("speed 3", datetime.datetime.now() - start )

            print("sell", order['fills'][0]['price'])
        if action_count > 60:
            b_client.close_connection()

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()
    # streams = ['btcusdt@bookTicker', 'btcusdt@depth@100ms', 'btcusdt@trade']
    streams = ['btcusdt@bookTicker']
    twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)
    twm.join()

if __name__ == "__main__":
   main()


# from binance import ThreadedDepthCacheManager
#
# def main():
#
#     dcm = ThreadedDepthCacheManager()
#     # start is required to initialise its internal loop
#     dcm.start()
#
#     def handle_depth_cache(depth_cache):
#         print(f"symbol {depth_cache.symbol}")
#         print("top 5 bids")
#         print(depth_cache.get_bids()[:5])
#         print("top 5 asks")
#         print(depth_cache.get_asks()[:5])
#         print("last update time {}".format(depth_cache.update_time))
#
#     dcm_name = dcm.start_depth_cache(handle_depth_cache, symbol='ETHBTC')
#
#     # multiple depth caches can be started
#     # dcm_name = dcm.start_depth_cache(handle_depth_cache, symbol='ETHBTC')
#
#     dcm.join()
#
#
# if __name__ == "__main__":
#    main()