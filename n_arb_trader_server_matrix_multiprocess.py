# note for install external server
# sudo apt update
# sudo apt install python3 python3-pip
# pip install pandas
# pip install numpy
# pip install python-binance
# pip install networkx

import sys
from decimal import *
# import math
import numpy as np
# import random
# import textwrap

import time
from datetime import datetime

import asyncio
import multiprocessing as mp
from multiprocessing import shared_memory, Process, Lock
lock = Lock()

# Binanace
from binance import AsyncClient, BinanceSocketManager, Client
from binance.enums import *
import requests

# Global variables for multi commication

class narbitrage_mp(object):
    def __init__(self, prc_inf):
        self.cores = prc_inf[0]  # összesen hány process van
        self.process = prc_inf[1]  # én hanyadik process vagyok
        self.shared_memory_name = prc_inf[2]  # én hanyadik process vagyok
        self.mpi = str(self.process) + "/" + str(self.cores) + " core ->"
        print(self.mpi, "starts.")

        self.start_symbol = 'USDT'
        self.symbols_no = 1000  # over 1000 it is max
        self.lot_size = 50  # USDor start symbol
        self.spread = 0.08 / 100  # % ezzel kalkulálom a profitot. minimum 3 * ennyinek kell lenni
        self.tick_modifier1 = 0
        self.tick_modifier2 = 0
        self.tick_modifier3 = 0

        self.trade_tick_modifier1 = 0  # ha - akkor az ordebooknál jobban akar venni, limit árat az orderbookhoz képestennyivel adja meg
        self.trade_tick_modifier2 = 0
        self.trade_tick_modifier3 = 0
        
        self.ab1 = np.array([])
        self.ab2 = np.array([])
        self.ab3 = np.array([])
        self.calculate_count = 1
        
        self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
        self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
        self.bx_client = Client(self.api_key, self.api_secret)
        self.account = self.get_account()
        self.exchange_info = self.get_exchange_info()

        self.symbols = ['USDT', 'BTC', 'ETH', '1INCH', 'AAVE', 'ACH', 'ADA', 'AKRO', 'ALGO', 'ALICE', 'ALPHA', 'ANC',
                        'ANKR', 'ANT', 'APE', 'AR', 'ASTR', 'ATA', 'ATOM', 'AUD', 'AUDIO', 'AVA',
                        'AVAX', 'AXS', 'BAKE', 'BAT', 'BCH', 'BEL', 'BETA', 'BICO', 'BIDR', 'BLZ',
                        'BOND', 'BRL', 'BTCDOWN', 'BTCST', 'BTTC', 'BURGER', 'BUSD',
                        'C98', 'CAKE', 'CELO', 'CELR', 'CHR', 'CHZ', 'COCOS', 'COMP', 'CRV', 'CTK',
                        'DAI', 'DAR', 'DASH', 'DENT', 'DGB', 'DOGE', 'DOT', 'DOTDOWN', 'DUSK', 'DYDX',
                        'EGLD', 'ELF', 'ENJ', 'ENS', 'EOS', 'EPX', 'ETC', 'ETHDOWN', 'EUR', 'FET',
                        'FIDA', 'FIL', 'FLM', 'FLOW', 'FLUX', 'FORTH', 'FRONT', 'FTM', 'FTT', 'FXS', 'GAL',
                        'GALA', 'GBP', 'GLMR', 'GMT', 'GRT', 'GTC', 'HBAR', 'HIGH', 'HIVE', 'HNT', 'HOT',
                        'ICP', 'IDEX', 'IMX', 'IOST', 'IOTA', 'IOTX', 'JASMY', 'JST', 'KAVA', 'KDA', 'KLAY',
                        'KSM', 'LDO', 'LEVER', 'LINA', 'LINK', 'LIT', 'LOKA', 'LRC', 'LTC', 'LUNA', 'LUNC',
                        'MANA', 'MASK', 'MATIC', 'MBOX', 'MINA', 'MIR', 'MOVR', 'MTL', 'NEAR', 'NEO', 'NMR',
                        'OGN', 'ONE', 'ONG', 'OOKI', 'OP', 'PEOPLE', 'PLA', 'POND', 'PORTO', 'POWR', 'PUNDIX',
                        'PYR', 'QNT', 'QTUM', 'RAD', 'REQ', 'RNDR', 'ROSE', 'RSR', 'RUNE', 'RVN', 'SAND',
                        'SHIB', 'SKL', 'SLP', 'SNX', 'SOL', 'SRM', 'STMX', 'STORJ', 'STPT', 'STX', 'SUSHI',
                        'SXP', 'T', 'THETA', 'TKO', 'TLM', 'TRB', 'TRX', 'TRY', 'TUSD', 'UNFI', 'UNI', 'USDC',
                        'USTC', 'VET', 'VGX', 'VIDT', 'VOXEL', 'WAVES', 'WBTC', 'WIN', 'WING', 'WNXM',
                        'WOO', 'WTC', 'XLM', 'XMR', 'XRP', 'XTZ', 'YFI', 'YFII', 'YGG', 'ZEC', 'ZIL', 'ZRX']

        ## off symbols
        self.off_symbols = ['BIDR', 'BUSD']
        for osy in self.off_symbols:
            self.symbols.remove(osy)

        self.selected_symbols = self.symbols[:self.symbols_no]  ## kiválasztam amivel dolgozok szűkíthetem a kört

        self.all_pairs = self.defa_all_pairs()
        self.selected_pairs = self.defa_selected_pairs()  ##a kiválasztott szimbólumokhoz kapcsolódó párokat kiválasztom
        self.price = self.defa_price_dict()
        self.socket_list = self.defa_socket_list()
        self.pai = self.defa_pair_info()  # pair info

        # az a számlám miből mennyi van, azért hívom becsült mnnyiségnek mert
        # kötés közben nincs idő lekérdezni a számlát ezért csak megsaccolom azt
        self.wallet = {}

        self.refresh_map = None
        self.triangles = None
        self.print_info()
        self.arb_matrix()
        self.run_analys = False
        self.last_arb = ""
        self.max_profit = 0
        self.start_asyc_websocket()

    def print_info(self):
        if self.process == 1:
            print("Number of pairs:", len(self.selected_pairs))
            print("Spread:", self.spread * 100, "%")
            print("Price modifier 1 (orderbook, trade):", self.tick_modifier1, self.trade_tick_modifier1, " tick")
            print("Price modifier 2 (orderbook, trade):", self.tick_modifier2, self.trade_tick_modifier2, " tick")
            print("Price modifier 3 (orderbook, trade):", self.tick_modifier3, self.trade_tick_modifier3, " tick")
            print("Start symbol:", self.start_symbol)
            print("Lot size:", self.lot_size)

    def get_slice_index(self, xlen, parts, slice_no):
        slices = np.array_split(list(np.arange(0, xlen)), parts)
        start = slices[slice_no - 1][0]
        end = slices[slice_no - 1][-1:][0]
        end += 1
        end = min(end, xlen)
        return start, end

    def arb_matrix(self):
        print(self.mpi, "Create arb martix")

        maxi_tri = np.chararray((0, 4), itemsize=10)
        # print(maxi_tri)

        nrow = np.chararray((1, 4), itemsize=10)
        for s1 in self.selected_symbols:
            nrow[0][0] = self.start_symbol
            nrow[0][3] = self.start_symbol
            for s2 in self.selected_symbols:
                nrow[0][1] = s1
                nrow[0][2] = s2
                maxi_tri = np.vstack([maxi_tri, nrow])

        # kiszedem az egymás mellet ugyan olyanokat
        del_row_id = []
        for row_x in range(maxi_tri.shape[0]):
            if maxi_tri[row_x][0] == maxi_tri[row_x][1] \
                    or maxi_tri[row_x][1] == maxi_tri[row_x][2] \
                    or maxi_tri[row_x][2] == maxi_tri[row_x][3]:
                del_row_id.append(row_x)
        maxi_tri = np.delete(maxi_tri, del_row_id, 0)

        # felépítem a párokat
        self.triangles = np.chararray((0, 3), itemsize=20)
        prow = np.chararray((1, 3), itemsize=20)
        for i in range(maxi_tri.shape[0]):
            prow[0][0] = maxi_tri[i][0] + maxi_tri[i][1]
            prow[0][1] = maxi_tri[i][1] + maxi_tri[i][2]
            prow[0][2] = maxi_tri[i][2] + maxi_tri[i][3]
            self.triangles = np.vstack([self.triangles, prow])

        all_pairs_way = []
        for sp in self.selected_pairs:
            all_pairs_way.append("".join([self.selected_pairs[sp][0], self.selected_pairs[sp][1]]))
            all_pairs_way.append("".join([self.selected_pairs[sp][1], self.selected_pairs[sp][0]]))

        ## kitörlöm azokat a kombinációkat amelyek nem is léteznek
        del_index = []
        for row_x in range(self.triangles.shape[0]):
            for col_x in range(3):
                if self.triangles[row_x][col_x].decode('UTF-8') not in all_pairs_way:
                    del_index.append(row_x)
        self.triangles = np.delete(self.triangles, del_index, 0)
        
        #processre szétdarabolom
        start_x, end_x = self.get_slice_index(self.triangles.shape[0], self.cores, self.process)
        self.triangles = self.triangles[start_x:end_x, :]

        # Felépítem a frissítési mapot
        self.refresh_map = {}
        for apw in all_pairs_way:
            self.refresh_map[apw] = [[], [], []]
        for apw in all_pairs_way:
            for apw_col in range(3):
                col_map = []
                for apw_row in range(self.triangles.shape[0]):
                    if self.triangles[apw_row][apw_col].decode('UTF-8') == apw:
                        col_map.append(apw_row)
                self.refresh_map[apw][apw_col] = col_map.copy()

        ## kitörlöm azokat a párokat amik sehol nem lettek felhasználba
        ## vektorok szorzásánál ezeket felesleges szorozgatni
        
        # Ezt azért nem teszem meg mert akkor a mappolás újra kellene csinálni
        
        # del_dic = []
        # for pm in self.refresh_map:
        #     if 0 == len(self.refresh_map[pm][0]) + len(self.refresh_map[pm][1]) + len(self.refresh_map[pm][2]):
        #         del_dic.append(pm)
        #
        # if del_dic:
        #     print("ezeket sehová nem tudom bekombinálni")
        #     print(del_dic)
        
        # nem akarok np arrayt használni, mert azt bonyolúlt lehet a processzek köt mozgatni
        # ezért kézzelkészítek üres arrayt

        self.ab1 = np.full(self.triangles.shape[0], 0.00000000, dtype=float)
        self.ab2 = np.full(self.triangles.shape[0], 0.00000000, dtype=float)
        self.ab3 = np.full(self.triangles.shape[0], 0.00000000, dtype=float)

        print(self.mpi, "Created arb martix shape:", self.triangles.shape)

    def get_account(self):
        return self.bx_client.get_account()

    def get_exchange_info(self):
        return self.bx_client.get_exchange_info()

    def defa_all_pairs(self):
        i_all_pairs = []
        for sy in self.exchange_info['symbols']:
            i_all_pairs.append(sy['symbol'])
        return i_all_pairs

    def defa_selected_pairs(self):
        i_selected_pairs = {}
        for si1 in self.selected_symbols:
            for si2 in self.selected_symbols:
                if si1 + si2 in self.all_pairs and \
                        self.get_symbol_info(si1 + si2)['status'] == 'TRADING' and \
                        "MARKET" in self.get_symbol_info(si1 + si2)['orderTypes']:
                    i_selected_pairs[si1 + si2] = [si1, si2]
        return i_selected_pairs

    def get_symbol_info(self, symbol):
        for item in self.exchange_info['symbols']:
            if item['symbol'] == symbol.upper():
                return item
        return None

    def defa_price_dict(self):
        i_selected_pairs = {}
        for si1 in self.selected_symbols:
            for si2 in self.selected_symbols:
                i_selected_pairs["".join([si1, si2])] = 1.0
                i_selected_pairs["".join([si2, si1])] = 1.0
        return i_selected_pairs

    def defa_pair_info(self):
        # ez egy hogyan, miről, mire megyek katalógus
        pair_info = {}
        for si1 in self.selected_symbols:
            for si2 in self.selected_symbols:
                if si1 + si2 in self.all_pairs:
                    filters = self.get_symbol_info(si1 + si2)['filters'][2]
                    base = self.get_symbol_info(si1 + si2)['baseAsset']
                    quot = self.get_symbol_info(si1 + si2)['quoteAsset']
                    step_size = float(filters['stepSize'])
                    ticksize = float(self.get_symbol_info(si1 + si2)['filters'][0]['tickSize'])
                    min_qty = float(filters['minQty'])
                    data_sell = {'orig_symbol': si1 + si2,
                                 'side': 'SELL',
                                 'step_size': step_size,
                                 'min_quote': min_qty,
                                 'base': base,
                                 'quote': quot,
                                 'tick_size': ticksize
                                 }

                    data_buy = {'orig_symbol': si1 + si2,
                                'side': 'BUY',
                                'step_size': step_size,
                                'min_quote': min_qty,
                                'base': base,
                                'quote': quot,
                                'tick_size': ticksize
                                }

                    pair_info[si1 + si2] = data_sell
                    pair_info[si2 + si1] = data_buy

        return pair_info

    def defa_socket_list(self):
        i_socket_list = []
        for sp in tuple(self.selected_pairs.keys()):
            i_socket_list.append(sp.lower() + '@bookTicker')
        return i_socket_list

    def isin_triangles(self, what):
        what = str(what)
        for i1 in range(self.triangles.shape[0]):
            for i2 in range(self.triangles.shape[1]):
                if self.triangles[i1][i2].decode('UTF-8') == what:
                    return True
        return False

    def get_BNBUSDT(self):
        res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BNBUSDT")
        return float(res.json()["price"])

    def get_BNBBTC(self):
        res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BNBBTC")
        return float(res.json()["price"])

    def get_amount_by_symbol(self, symbol):
        for i_i in self.account['balances']:
            if i_i['asset'] == symbol:
                return float(i_i['free'])
        return 0.0

    def print_wallet(self):
        self.account = self.bx_client.get_account()

        estimated_amount = {}
        for sesy in self.selected_symbols:
            estimated_amount[sesy] = self.get_amount_by_symbol(sesy)

        # BNB külön kezelem
        estimated_amount["BNB"] = self.get_amount_by_symbol("BNB")  # mivel erre nem lehet kereskedni ezt külön beteszem
        bnbusdt = self.get_BNBUSDT()
        bnbbtc = self.get_BNBBTC()
        self.price["BNBUSDT"] = bnbusdt
        self.price["USDTBNB"] = 1 / bnbusdt

        self.price["BNBBTC"] = bnbbtc
        self.price["BTCBNB"] = 1 / bnbbtc

        print("Spot wallet:")
        print(" symbol    amount       USD        BTC")
        total_in_usdt = 0
        total_in_btc = 0

        for ea in estimated_amount:
            if estimated_amount[ea] != 0:
                if ea == 'USDT':
                    price_usdt = 1
                else:
                    prc1 = 0 if self.price[ea + "USDT"] == 1 else self.price[ea + "USDT"]
                    prc2 = 0 if self.price["USDT" + ea] == 1 else 1 / self.price["USDT" + ea]
                    price_usdt = prc1 if ea + "USDT" in self.selected_pairs else prc2

                if ea == 'BTC':
                    price_btc = 1
                else:
                    prc1 = 0 if self.price[ea + "BTC"] == 1 else self.price[ea + "BTC"]
                    prc2 = 0 if self.price["BTC" + ea] == 1 else 1 / self.price["BTC" + ea]
                    price_btc = prc1 if ea + "BTC" in self.selected_pairs else prc2

                symbol_value_in_usdt = round(estimated_amount[ea] * price_usdt, 3)
                symbol_value_in_btc = round(estimated_amount[ea] * price_btc, 8)
                total_in_usdt += symbol_value_in_usdt
                total_in_btc += symbol_value_in_btc
                eap = ea + "     "
                amount = '{0:.8f}'.format(estimated_amount[ea]) + "                    "
                vusdt = '{0:.2f}'.format(symbol_value_in_usdt) + "                   "
                vbtc = '{0:.8f}'.format(symbol_value_in_btc) + "                   "
                print(" ", eap[0:5], amount[0:15], vusdt[0:10], vbtc[0:10], )

        total_usdtstr = '{0:.2f}'.format(total_in_usdt) + "                                  "
        total_btcstr = '{0:.8f}'.format(total_in_btc) + "                                  "

        print("________________________________________________")
        print("Total:                 ", total_usdtstr[:10], total_btcstr[:10])

    def round_qty_with_step_size(self, quantity, step_size, reduce=0):
        reduce = Decimal(reduce * step_size)  # ennyi darabbal visszaveszi
        quantity = Decimal(str(quantity))
        return round(float(quantity - quantity % Decimal(str(step_size)) - reduce), 8)

    def start_asyc_websocket(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(self.asyc_websocket())
        loop.close()

    async def asyc_websocket(self):
        ### ezek nem közösek !!!!!
        client = await AsyncClient.create()
        bm = BinanceSocketManager(client)

        i_socket_list = []
        for sp in tuple(self.selected_pairs.keys()):
            base_asset = self.pai[sp]['base']
            quote_asset = self.pai[sp]['quote']
            
            if self.isin_triangles(base_asset + quote_asset) or  self.isin_triangles(quote_asset + base_asset):
                i_socket_list.append(sp.lower() + '@bookTicker')

        print(self.mpi, 'Number of sockets:', len(i_socket_list))
        ts = bm.multiplex_socket(i_socket_list)
        # then start receiving messages
        async with ts as tscm:
            while True:
                res = await tscm.recv()
                # print(self.process, res)

                # socker data

                s1 = self.selected_pairs[res['data']['s']][0]
                s2 = self.selected_pairs[res['data']['s']][1]
                # print(res['data']['s'], s1, s2)
                s1s2 = "".join([s1, s2])
                s2s1 = "".join([s2, s1])

                tick_size = self.pai[s1s2]['tick_size']

                bid = round(float(res['data']['b']), 8)
                # bid_mod = bid - (self.tick_modifier1 * tick_size)
                # set_bid = bid if s1 == self.start_symbol else bid_mod
                ask = round(float(res['data']['a']), 8)
                # ask_mod = ask + (self.tick_modifier1 * tick_size)
                # set_ask = round(1 / ask, 8) if s2 == self.start_symbol else round(1 / ask_mod, 8)

                self.price[s1 + s2] = bid
                self.price[s2 + s1] = ask

                # print(s1s2, tick_size, bid, ask, bid - (self.tick_modifier1 * tick_size),
                #       round(1 / (ask + (self.tick_modifier1 * tick_size)), 8))

                np.put(self.ab1, self.refresh_map[s1s2][0], bid - (self.tick_modifier1 * tick_size))
                np.put(self.ab2, self.refresh_map[s1s2][1], bid - (self.tick_modifier2 * tick_size))
                np.put(self.ab3, self.refresh_map[s1s2][2], bid - (self.tick_modifier3 * tick_size))

                np.put(self.ab1, self.refresh_map[s2s1][0], round(1 / (ask + (self.tick_modifier1 * tick_size)), 8))
                np.put(self.ab2, self.refresh_map[s2s1][1], round(1 / (ask + (self.tick_modifier2 * tick_size)), 8))
                np.put(self.ab3, self.refresh_map[s2s1][2], round(1 / (ask + (self.tick_modifier3 * tick_size)), 8))

                profit_array = np.multiply(np.multiply(self.ab1, self.ab2), self.ab3)
                max_row = np.argmax(profit_array)
                min_row = np.argmin(profit_array)
                profit = self.ab1[max_row] * self.ab2[max_row] * self.ab3[max_row] - (self.spread * 3)
                # self.max_profit = max(self.max_profit, profit)

                # self.calculate_count += 1
                # if self.calculate_count % 100000 == 0:
                #     print(self.mpi, self.calculate_count)
                #     self.max_profit = 0

                sy1 = self.triangles[max_row][0].decode('UTF-8')
                sy2 = self.triangles[max_row][1].decode('UTF-8')
                sy3 = self.triangles[max_row][2].decode('UTF-8')

                arb_str = str([sy1, sy2, sy3])

                existing_shm = shared_memory.SharedMemory(name=self.shared_memory_name)
                np_array = np.ndarray((1,), dtype=np.int64, buffer=existing_shm.buf)

                if profit > 1 and np_array[0] == 1 and self.last_arb != arb_str:
                    
                    lock.acquire()
                    np_array[0] = 0
                    lock.release()
                    existing_shm.close()
                    
                    cp1 = cpx1 = self.ab1[max_row]
                    cp2 = cpx2 = self.ab2[max_row]
                    cp3 = cpx3 = self.ab3[max_row]

                    t_side1 = self.pai[sy1]['side']
                    t_side2 = self.pai[sy2]['side']
                    t_side3 = self.pai[sy3]['side']

                    # if t_side1 == "BUY":
                    #     cpx1 = 1 / cpx1
                    # if t_side2 == "BUY":
                    #     cpx2 = 1 / cpx2
                    # if t_side3 == "BUY":
                    #     cpx3 = 1 / cpx3
                    #
                    # est_profit = cp1 * cp2 * cp3

                    self.last_arb = arb_str

                    # print("")
                    # print("")
                    # print("Start trade:                    ", sy1, '      ',
                    #       sy2, '    ',
                    #       sy3, '              ')
                    # print("Estimated:             ",
                    #       '               {0:.8f}'.format(cpx1)[-18:],
                    #       '               {0:.8f}'.format(cpx2)[-18:],
                    #       '               {0:.8f}'.format(cpx3)[-18:], ' ' * 11,
                    #       '               {0:.8f}'.format(est_profit))

                    # sp1 egyenes
                    # spmx ha kell reciprok
                    # sp1 = self.price[sy1]
                    # sp2 = self.price[sy2]
                    # sp3 = self.price[sy3]

                    # print("Orderbook prices:      ",
                    #       '               {0:.8f}'.format(sp1)[-18:],
                    #       '               {0:.8f}'.format(sp2)[-18:],
                    #       '               {0:.8f}'.format(sp3)[-18:])
                    #
                    # print("Orderbook prices (1/): ",
                    #       '               {0:.8f}'.format(1 / sp1)[-18:],
                    #       '               {0:.8f}'.format(1 / sp2)[-18:],
                    #       '               {0:.8f}'.format(1 / sp3)[-18:])

                    
                    if 1 == 1:
                        
                        t_symbol1 = self.pai[sy1]['orig_symbol']
                        t_step_size1 = self.pai[sy1]['step_size']
                        t_min_qt1 = self.pai[sy1]['min_quote']
                        t_ticksize = self.pai[sy1]['tick_size']
                        t_amount_mod_buy = self.round_qty_with_step_size(
                            self.lot_size / ((self.price[sy1] - (t_ticksize * self.trade_tick_modifier1))),
                            t_step_size1, 1)
                        t_amount1 = t_amount_mod_buy if t_side1 == "BUY" else self.lot_size
    
                        # roundv = len(str(self.price[sy1]).split('.')[1])
    
                        t_price1 = self.price[sy1] - (t_ticksize * self.trade_tick_modifier1) \
                            if t_side1 == "BUY" else \
                            self.price[t_symbol1] + (t_ticksize * self.trade_tick_modifier1)
    
                        # trade
                        for trade_try in range(2):
                            # print(trade_try, 'try, 1 symbol', t_symbol1, 'price', self.price[sy1], self.price[t_symbol1],
                            #       't_price {0:.8f}'.format(t_price1), 'side', t_side1, 'quantity', t_amount1, 'minqt',
                            #       t_min_qt1)
                            order1 = self.bx_client.order_limit(symbol=t_symbol1,
                                                                price='{0:.8f}'.format(t_price1),
                                                                side=t_side1,
                                                                quantity=t_amount1,
                                                                timeInForce=TIME_IN_FORCE_IOC)
                            if order1['status'] != 'EXPIRED':
                                break
                        # print(order1)
                        if order1['status'] != 'EXPIRED':
                            executedQty_1 = round(float(order1['executedQty']), 8)
                            cummulativeQuoteQty_1 = round(float(order1['cummulativeQuoteQty']), 8)
                            t_amount2 = executedQty_1 if t_side1 == "BUY" else cummulativeQuoteQty_1
                            t_symbol2 = self.pai[sy2]['orig_symbol']
                            t_step_size2 = self.pai[sy2]['step_size']
                            t_min_qt2 = self.pai[sy2]['min_quote']
                            t_ticksize2 = self.pai[sy2]['tick_size']
    
                            t_amount2 = self.round_qty_with_step_size(t_amount2,
                                                                      t_step_size2) if t_side2 == "SELL" else t_amount2
                            # print('2 symbol', t_symbol2, 'side', t_side2, 'quantity', t_amount2, 'minqt', t_min_qt2)
                            order2 = self.bx_client.order_market(symbol=t_symbol2,
                                                                 side=SIDE_BUY if t_side2 == "BUY" else SIDE_SELL,
                                                                 quantity=None if t_side2 == "BUY" else t_amount2,
                                                                 quoteOrderQty=t_amount2 if t_side2 == "BUY" else None)
                            executedQty_2 = round(float(order2['executedQty']), 8)
                            cummulativeQuoteQty_2 = round(float(order2['cummulativeQuoteQty']), 8)
                            t_amount3 = executedQty_2 if t_side2 == "BUY" else cummulativeQuoteQty_2
                            t_symbol3 = self.pai[sy3]['orig_symbol']
                            t_step_size3 = self.pai[sy3]['step_size']
                            t_min_qt3 = self.pai[sy3]['min_quote']
                            t_ticksize3 = self.pai[sy3]['tick_size']
                            t_amount3 = self.round_qty_with_step_size(t_amount3,
                                                                      t_step_size3) if t_side3 == "SELL" else t_amount3
                            # print('3 symbol', t_symbol3, 'side', t_side3, 'quantity', t_amount3, 'minqt', t_min_qt3)
                            order3 = self.bx_client.order_market(symbol=t_symbol3,
                                                                 side=SIDE_BUY if t_side3 == "BUY" else SIDE_SELL,
                                                                 quantity=None if t_side3 == "BUY" else t_amount3,
                                                                 quoteOrderQty=t_amount3 if t_side3 == "BUY" else None)
                            # print(order3)
    
                            px1 = p1 = round(float(order1['fills'][0]['price']), 8)
                            px2 = p2 = round(float(order2['fills'][0]['price']), 8)
                            px3 = p3 = round(float(order3['fills'][0]['price']), 8)
    
                            if t_side1 == "BUY":
                                px1 = 1 / px1
                            if t_side2 == "BUY":
                                px2 = 1 / px2
                            if t_side3 == "BUY":
                                px3 = 1 / px3
    
                            realised_profit = round(px1 * px2 * px3, 8)
    
                            # print("Realised profit:       ",
                            #       '               {0:.8f}'.format(px1)[-18:],
                            #       '               {0:.8f}'.format(px2)[-18:],
                            #       '               {0:.8f}'.format(px3)[-18:], ' ' * 12,
                            #       '               {0:.8f}'.format(round(px1 * px2 * px3, 8)))
    
                            print("Realised:              ",
                                  '               {0:.8f}'.format(p1)[-18:],
                                  '               {0:.8f}'.format(p2)[-18:],
                                  '               {0:.8f}'.format(p3)[-18:], ' ' * 12,
                                  '               {0:.8f}'.format(realised_profit))
    
                            dif1 = cpx1 - p1 if t_side1 == "BUY" else p1 - cpx1
                            dif2 = cpx2 - p2 if t_side2 == "BUY" else p2 - cpx2
                            dif3 = cpx3 - p3 if t_side3 == "BUY" else p3 - cpx3
    
                            print("Dif result - est      :",
                                  '               {0:.8f}'.format(dif1)[-18:],
                                  '               {0:.8f}'.format(dif2)[-18:],
                                  '               {0:.8f}'.format(dif3)[-18:])
    
                            print("Tick size:             ",
                                  '               {0:.8f}'.format(self.pai[sy1]['tick_size'])[-18:],
                                  '               {0:.8f}'.format(self.pai[sy2]['tick_size'])[-18:],
                                  '               {0:.8f}'.format(self.pai[sy3]['tick_size'])[-18:])
    
                            print("Side:               ",
                                  "              " + t_side1,
                                  "              " + t_side2,
                                  "              " + t_side3)
    
                            self.print_wallet()
                            time.sleep(20)
                        else:
                            print("Start order failed.")

                    existing_shm = shared_memory.SharedMemory(name=self.shared_memory_name)
                    np_array = np.ndarray((1,), dtype=np.int64, buffer=existing_shm.buf)
                    lock.acquire()
                    np_array[0] = 1
                    lock.release()
                    existing_shm.close()
                else:
                    existing_shm.close()

        # ez sosem fog lefutni mert a szervernek nincs leállítási funkciója csak kilövöm éskész
        await client.close_connection()

if __name__ == '__main__':
    
    a = np.array([1])
    shm = mp.shared_memory.SharedMemory(create=True, size=a.nbytes)
    # # Now create a NumPy array backed by shared memory
    np_array = np.ndarray(a.shape, dtype=np.int64, buffer=shm.buf)
    np_array[:] = a[:]  # Copy the original data into shared memory
    
    used_cores = mp.cpu_count()
    params = []
    for x in range(used_cores):
        params.append([used_cores, x + 1, shm.name])
    n_arb = narbitrage_mp
    xpool = mp.Pool(used_cores)
    res = xpool.map(n_arb, params)

    

