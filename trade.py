from binance.helpers import round_step_size
from binance.client import Client
import time
import sys

selected_symbols = ['BTC', 'BNB', 'ETH', 'ADA', 'LINK', 'DOT', 'TRX', 'FTM', 'BUSD', 'SOL',
                        'USDT', 'MATIC', 'ETC', 'NEO', 'ENJ', 'WAVES', 'ATOM', 'ONE', 'ZEC', 'MANA',
                        'ONT', 'HOT', 'CHZ', 'WIN', 'AXS', 'GALA', 'ANKR', 'RUNE', 'ICP', 'LRC',
                        'ZIL', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS',
                        'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'NULS', 'ADX',
                        'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC',
                        'IOST', 'NANO', 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN', 'DAI',
                        'USDC', 'BCHSV', 'PHB', 'COCOS', 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI',
                        'SRM', 'KSM', 'SUSHI', 'BEL', 'NEAR', 'SLP', 'REEF', 'TRY', 'MINA', 'LAZIO', 'VOXEL']

api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
b_client = Client(api_key, api_secret)
i_account = b_client.get_account()
exchange_info = b_client.get_exchange_info()
# print(exchange_info['symbols'])

all_pairs = []
for s in exchange_info['symbols']:
    all_pairs.append(s['symbol'])
# print(exchange_info['symbols'])

def get_symbol_info2(exchange_info, symbol):
    for item in exchange_info['symbols']:
        if item['symbol'] == symbol.upper():
            return item

pair_info = {}
for si1 in selected_symbols:
    for si2 in selected_symbols:
        if si1 + si2 in all_pairs:
            filters = get_symbol_info2(exchange_info, si1 + si2)['filters'][2]
            step_size = float(filters['stepSize'])
            min_qty = float(filters['minQty'])
            pair_info[si1 + si2] = [si1 + si2, "SELL", step_size, min_qty]
            pair_info[si2 + si1] = [si1 + si2, "BUY", step_size, min_qty]

# print(pair_info)
# ---------------------------------------------------------------------------------------

prices_result = b_client.get_all_tickers()
cross_price = {}
for pr in prices_result:
    cross_price[pr['symbol']] = float(pr['price'])


def get_amount_by_symbol(account, symbol):
    for i_i in account['balances']:
        if i_i['asset'] == symbol:
            return float(i_i['free'])
    return 0.0

estimated_amount = {}
for sesy in selected_symbols:
    estimated_amount[sesy] = get_amount_by_symbol(i_account, sesy)

def print_estimated_amount():
    print("Wallet -----------------------------")
    for ea in estimated_amount:
        if estimated_amount[ea] != 0:
            print(" ", ea, estimated_amount[ea])
print_estimated_amount()


def trade(from_symbol, pair_symbol, side, step_size, cross_price, min_qt, estimated_amount):
    print("Trade", "-" * 60)
    print(" from_symbol", from_symbol)
    print(" pair_symbol", pair_symbol)
    print(" side", side)
    print(" step_size", step_size)
    print(" cross_price", cross_price)
    print(" min_qt", min_qt)
    if side == "BUY":
        # meg kell becsülni hogy mennyit tudok venni
        trade_qty = estimated_amount[from_symbol] / cross_price
        print(" trade_qty", trade_qty)
        # kerekítem kereskedhető mennyiségre
        # egy kicsivel (min_qty) kevesebbet veszek, hogy biztosan teljesüljön a tranzakció
        # ha emelkedik az ár a kiszámolt mennyiséget már nem tudom megvenni !!!!
        rounded_trade_qty = round(round_step_size(trade_qty, step_size), 8)
        print(" 1 rounded_trade_qty", rounded_trade_qty)
        if rounded_trade_qty > trade_qty:
            rounded_trade_qty = round(rounded_trade_qty -  step_size, 8)
        print(" 2 rounded_trade_qty", rounded_trade_qty)

        # print("trade_qty", trade_qty)
        # print("rounded_trade_qty", rounded_trade_qty)

        if rounded_trade_qty > min_qt:
            order = b_client.order_market_buy(
                symbol=pair_symbol,
                quantity=str(rounded_trade_qty))
            # print(order)

        # order = b_client.create_test_order(
        #    symbol=pair_symbol,
        #    side=Client.SIDE_BUY,
        #    type=Client.ORDER_TYPE_MARKET,
        #    quantity=rounded_trade_qty
        # )

        return rounded_trade_qty  # ez a becsült darb amit kapnif ogok


    elif side == "SELL":
        rounded_trade_qty = round(round_step_size(estimated_amount[from_symbol], step_size), 8)
        if rounded_trade_qty > estimated_amount[from_symbol]:
            rounded_trade_qty -= step_size

        print(" rounded_trade_qty", rounded_trade_qty)

        if rounded_trade_qty > min_qt:
            order = b_client.order_market_sell(
                symbol=pair_symbol,
                quantity=str(rounded_trade_qty))
            # print(order)

        # order = b_client.create_test_order(
        #    symbol=pair_symbol,
        #    side=Client.SIDE_SELL,
        #    type=Client.ORDER_TYPE_MARKET,
        #    quantity=rounded_trade_qty
        # )

        # print(order)
        return round(rounded_trade_qty * cross_price, 8)  # ez a becsült darb amit kapnif ogok


arb = ["BNB", "USDT", "BTC", "BNB"]
start = time.time()
for i in range(len(arb) - 1):
    from_symbol = arb[i]
    to_symbol = arb[i + 1]
    from_to_symbol = from_symbol + to_symbol
    # print(pair_info[from_to_symbol])
    i_pair_symbol = pair_info[from_to_symbol][0]
    i_side = pair_info[from_to_symbol][1]
    i_step_size = pair_info[from_to_symbol][2]
    i_min_qty = pair_info[from_to_symbol][3]
    estimated_qt = trade(from_symbol=from_symbol,
                         pair_symbol=i_pair_symbol,
                         side=i_side,
                         step_size=i_step_size,
                         cross_price=cross_price[i_pair_symbol],
                         min_qt=i_min_qty,
                         estimated_amount=estimated_amount)
    estimated_amount[to_symbol] = estimated_qt
    estimated_amount[from_symbol] = 0

    print_estimated_amount()

print("SPEED: ", time.time() - start)
time.sleep(3)
sys.exit(0)




# print(cross_price)

# sys.exit()






qt_estimate_price = 1 / cross_price[i_pair_symbol]

print(time.time() - start)

print(to_symbol, round(rounded_trade_qty * qt_estimate_price, 8))