import sys

from binance.client import Client
import datetime
import pickle

from os import listdir
from os.path import isfile, join


def unix_to_datetime(ts):
    return datetime.datetime.fromtimestamp(int(ts) / 1000)


def print_trades(i_trades):
    min_id = int(i_trades[0]['id'])
    min_datetime = str(i_trades[0]['time'])
    max_id = int(i_trades[0]['id'])
    max_datetime = str(i_trades[0]['time'])

    for ix in i_trades:
        if min_id > int(ix['id']):
            min_id = int(ix['id'])
            min_datetime = str(ix['time'])
        if max_id < int(ix['id']):
            max_id = int(ix['id'])
            max_datetime = str(ix['time'])


    ids = []
    for ix in i_trades:
        ids.append(int(ix['id']))

    # print(ids)
    act_id = min_id
    missing = []
    while act_id < max_id:
        if act_id not in ids:
            missing.append(act_id)
        act_id += 1

    # print("first: ", 0, i_trades[0]['id'], unix_to_datetime(i_trades[0]['time']))
    # print("last: ", len(i_trades), i_trades[-1]['id'], unix_to_datetime(i_trades[-1]['time']))
    print("min id", min_id, "max id", max_id)
    print("min dt", str(unix_to_datetime(min_datetime)), "max dt", str(unix_to_datetime(max_datetime)))
    print("len", len(i_trades), "MB:", len(str(i_trades)) / 1024 / 1024)
    if len(missing) > 0:
        print('missing id:', len(missing), missing[0], missing[-1])
    else:
        print('missing id: 0')


def get_last_ids(symbols):
    from_id = {}
    for sy in symbols:
        trades = client.get_historical_trades(symbol=sy)
        from_id[sy] = trades[-1]['id']
        # print(sy, from_id[sy])
    return from_id


def is_all_symbols_off(i_dict, i_len):
    i_ret = 0
    for i_id in i_dict:
        i_ret += i_dict[i_id]
    if i_ret == 0:
        return True
    else:
        return False


def save_collected_data(symbol, i_trades):
    print("save symbol:", symbol)
    min_id = int(i_trades[0]['id'])
    min_datetime = str(i_trades[0]['time'])
    max_id = int(i_trades[0]['id'])
    max_datetime = str(i_trades[0]['time'])

    for ix in i_trades:
        if min_id > int(ix['id']):
            min_id = int(ix['id'])
            min_datetime = str(ix['time'])
        if max_id < int(ix['id']):
            max_id = int(ix['id'])
            max_datetime = str(ix['time'])


    # ids = []
    # for ix in i_trades:
    #     ids.append(int(ix['id']))
    #
    # # print(ids)
    # act_id = min_id
    # missing = []
    # while act_id < max_id:
    #     if act_id not in ids:
    #         missing.append(act_id)
    #     act_id += 1

    # print("first: ", 0, i_trades[0]['id'], unix_to_datetime(i_trades[0]['time']))
    # print("last: ", len(i_trades), i_trades[-1]['id'], unix_to_datetime(i_trades[-1]['time']))
    print("  min id:", min_id, "max id:", max_id)
    print("  min dt:", str(unix_to_datetime(min_datetime)), "max dt:", str(unix_to_datetime(max_datetime)))
    print("  len:", len(i_trades))
    # if len(missing) > 0:
    #     print('  missing id:', len(missing), missing[0], missing[-1])
    # else:
    #     print('  missing id: 0')

    f_name = symbol + "-" + str(min_id) + "-" + str(max_id)
    print("  file name:", f_name)
    pickle.dump(i_trades, open(path + f_name + ".pickle", "wb"))

path = "D:/Apa/Coder/Binance_tick_data/"

symbols = ["ATOMUSDT", "BTCUSDT", "ETHUSDT", "NMRUSDT", "SANDUSDT", "SOLUSDT", "FTMUSDT", "XRPUSDT",
           "LUNAUSDT", "MANAUSDT", "NEARUSDT", "AVAXUSDT", "TRXUSDT", "ROSEUSDT", "ONEUSDT", "ALGOUSDT",
           "DOTUSDT", "VETUSDT", "ATOMUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
           "THETAUSDT", "OMGUSDT"]

collected_data = {}
symbol_run = {}

for sy in symbols:
    collected_data[sy] = []
    symbol_run[sy] = 1  ## 1 fut 0 leáll a lekérdezés


# get last id
last_saved_id = {}
files = [f for f in listdir(path) if isfile(join(path, f))]
for fn in files:
    symbol = fn.split('-')[0]
    from_id = id(fn.split('-')[1])
    to_id = fn.split('-')[-1]
    to_id = int(to_id[0:len(to_id) - 7])
    # print(symbol, from_id, to_id)
    if symbol in last_saved_id:
        if last_saved_id[symbol] < to_id:
            last_saved_id[symbol] = to_id
    else:
        last_saved_id[symbol] = to_id
print(last_saved_id)



# a = datetime.datetime.now()
# c = a - datetime.timedelta(minutes=60 * 10)
# back_to_dt = datetime.datetime(c.year, c.month, c.day, c.hour, c.minute)
# back_to_dt = datetime.datetime(2022, 5, 2, 0, 0)


api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
client = Client(api_key, api_secret)

last_read_id = get_last_ids(symbols)
print(last_read_id)

# sys.exit(0)

while True:
    for sy in symbols:
        if symbol_run[sy] == 1:
            trades = client.get_historical_trades(symbol=sy, limit=1000, fromId=last_saved_id[sy] + 1)
            # print(trades)
            client.close_connection()
            collected_data[sy] += trades
            last_saved_id[sy] += 1 + 1000
            # print(sy, "-" * 80)
            # print_trades(collected_data[sy])

        # stop - download
        # print(unix_to_datetime(collected_data[sy][-1]['time']), back_to_dt)
        if symbol_run[sy] == 1 and last_saved_id[sy] > last_read_id[sy]:
            print('set 0', sy)
            symbol_run[sy] = 0

    # Save data
    for sy in symbols:
        if len(collected_data[sy]) > 300000:  ## kb 25 MB
            save_collected_data(sy, collected_data[sy])
            collected_data[sy] = []

    if is_all_symbols_off(symbol_run, 26):
        break

for sy in symbols:
    if len(collected_data[sy]) > 0:
        save_collected_data(sy, collected_data[sy])
