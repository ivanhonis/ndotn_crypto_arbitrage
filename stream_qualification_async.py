import asyncio
import sys

from binance import AsyncClient, BinanceSocketManager, Client
from datetime import datetime
api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
b_client = Client(api_key, api_secret)
exchange_info = b_client.get_exchange_info()
action_count = 0

symbols = ['USDT', 'ETH', 'BTC', 'ADA', 'LINK', 'DOT', 'TRX', 'FTM', 'SOL',
                 'MATIC', 'ETC', 'NEO', 'ENJ', 'WAVES', 'ATOM', 'ONE', 'ZEC',
                'ONT', 'HOT', 'CHZ', 'WIN', 'AXS', 'GALA', 'ANKR', 'RUNE', 'ICP', 'LRC',
                'ZIL', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS',
                'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'ADX', 'NULS',
                'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC',
                'IOST', 'NANO', 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN',
                'USDC', 'BCHSV', 'PHB', 'COCOS', 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI',
                'SRM', 'KSM', 'SUSHI', 'BEL', 'NEAR', 'SLP', 'REEF', 'C98', 'MINA', 'VOXEL']

selected_symbols = symbols[:25]


all_pairs = []
for sy in exchange_info['symbols']:
    all_pairs.append(sy['symbol'])


def get_symbol_info2(symbol):
    for item in exchange_info['symbols']:
        if item['symbol'] == symbol.upper():
            return item
    return None

selected_pairs = {}
for si1 in selected_symbols:
    for si2 in selected_symbols:
        if si1 + si2 in all_pairs and \
                get_symbol_info2(si1 + si2)['status'] == 'TRADING' and \
                "MARKET" in get_symbol_info2(si1 + si2)['orderTypes']:
            selected_pairs[si1 + si2] = [si1, si2]

socket_list = []
for sp in tuple(selected_pairs.keys()):
    socket_list.append(sp.lower() + '@bookTicker')


async def main():
    global action_count
    
    def print_msg(msg):
        global action_count

        if msg['stream'].split('@')[1] == 'depth':
            print(action_count, msg['stream'].split('@')[0], 'D', msg['data']['b'][0][0], msg['data']['a'][0][0], " - ",
                  msg['data']['b'][1][0], msg['data']['a'][1][0], " - ",
                  "Es", round((float(msg['data']['b'][1][0]) + float(msg['data']['a'][1][0])) / 2, 8))
        elif msg['stream'].split('@')[1] == 'bookTicker':
            print(action_count, msg['stream'].split('@')[0], '   b', msg['data']['b'], msg['data']['a'],
                  "Es", round((float(msg['data']['b']) + float(msg['data']['a'])) / 2, 8))
        elif msg['stream'].split('@')[1] == 'trade':
            print(action_count, msg['stream'].split('@')[0], '      t', msg['data']['p'], 'id', msg['data']['t'])
        else:
            # időhúzás
            c = 0
            a = 2
            a = a ** 3
            b = c - a * 2
    
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    # ts = bm.trade_socket('BNBBTC')
    # ts = bm.multiplex_socket(['btcusdt@bookTicker', 'btcusdt@depth@100ms', 'btcusdt@trade'])
    print(len(socket_list))
    ts = bm.multiplex_socket(socket_list)
    # then start receiving messages
    async with ts as tscm:
        while True:
            msg = await tscm.recv()
            print_msg(msg)
            action_count += 1
            if action_count == 3350:
                # start = datetime.now()
                order = b_client.order_market_buy(
                    symbol='ETHUSDT',
                    quantity=str(0.014))
                # print("speed 1", datetime.now() - start)
                print("buy1", order['fills'][0]['price'])
            if action_count == 3380:
                order = b_client.order_market_sell(
                    symbol='ETHUSDT',
                    quantity=str(0.014))
                # print("speed 3", datetime.now() - start)
                print("sell", order['fills'][0]['price'])
            if action_count > 3420:
                b_client.close_connection()
                sys.exit(0)

    await client.close_connection()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())