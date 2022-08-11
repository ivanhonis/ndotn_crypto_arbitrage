# note for install external server
# sudo apt update
# sudo apt install python3 python3-pip
# pip install numpy
# pip install python-binance

import sys
from decimal import *
import numpy as np
import time
from datetime import datetime
import asyncio

from multiprocessing import shared_memory, Lock, Pool, cpu_count
lock = Lock()

# Binanace
from binance import AsyncClient, BinanceSocketManager, Client
from binance.enums import *
from binance.exceptions import BinanceAPIException
import requests

from threading import Thread
import pickle
# Global variables for multi commication
bid_ask_flow = {}
order_qty = {}

class narbitrage_mp(object):
    def __init__(self, prc_inf):
        global bid_ask_flow, order_qty
        self.cores = prc_inf[0]  # összesen hány process van
        self.process = prc_inf[1]  # én hanyadik process vagyok
        self.shared_memory_name = prc_inf[2]  # megosztott memória trade lockhoz
        # print(self.shared_memory_name)
        self.deal_counter_shared_memory_name = prc_inf[3]  # megosztott memória dealhoz
        self.mpi = str(self.process) + "/" + str(self.cores) + " core ->"
        if self.process == self.cores:
            print(self.mpi, "starts. Last speak.")

        self.load_triangles = "triangles_all.npy"  # ha üres akkor nem tölti be hanem megcsinálja
        # self.load_triangles = "triangles_top250.npy"  # ha üres akkor nem tölti be hanem megcsinálja
        # self.load_triangles = ""
        # self.save_triangles = "triangles_all.npy"  # ha üres akkor nem tölti be hanem megcsinálja
        self.save_triangles = ""
        self.max_triangles = 3500
        self.start_symbol = 'USDT'
        self.symbols_no = 1000  # over 1000 it is max
        self.lot_size = 17  # USDor start symbol
        self.spread = 0.05 / 100  # % ezzel kalkulálom a profitot. minimum 3 * ennyinek kell lenni
        self.spred_x_3 = self.spread * 3
        self.spred_xxx = (1 - self.spread) ** 3
        self.tick_modifier1 = 0
        self.tick_modifier2 = 0
        self.tick_modifier3 = 0

        existing_shm = shared_memory.SharedMemory(name=self.shared_memory_name)
        self.trade_lock = np.ndarray((1,), dtype=np.int64, buffer=existing_shm.buf)
        # existing_shm.close()



        self.trade_tick_modifier1 = 0  # -1 próbáld megvenni olcsóbban (pushing) +1 drágábban is jó
        self.trade_tick_modifier2 = 0
        self.trade_tick_modifier3 = 0
        
        self.ab1 = np.array([])
        self.ab2 = np.array([])
        self.ab3 = np.array([])
        self.spread_array = np.array([])
        # self.ab1_qty = np.array([])
        # self.ab2_qty = np.array([])
        # self.ab3_qty = np.array([])
        self.calculate_count = 1
        
        self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
        self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
        self.bx_client = Client(self.api_key, self.api_secret)
        self.account = self.get_account()
        self.exchange_info = self.get_exchange_info()
        self.save_btcusdt = 0
        self.c = {"qty_ready": 0}

        # self.symbols = ['USDT', 'BTC', 'ETH', '1INCH', 'AAVE', 'ACH', 'ADA', 'AKRO', 'ALGO', 'ALICE', 'ALPHA', 'ANC',
        #                 'ANKR', 'ANT', 'APE', 'AR', 'ASTR', 'ATA', 'ATOM', 'AUD', 'AUDIO', 'AVA',
        #                 'AVAX', 'AXS', 'BAKE', 'BAT', 'BCH', 'BEL', 'BETA', 'BICO', 'BIDR', 'BLZ',
        #                 'BOND', 'BRL', 'BTCDOWN', 'BTCST', 'BTTC', 'BURGER', 'BUSD',
        #                 'C98', 'CAKE', 'CELO', 'CELR', 'CHR', 'CHZ', 'COCOS', 'COMP', 'CRV', 'CTK',
        #                 'DAI', 'DAR', 'DASH', 'DENT', 'DGB', 'DOGE', 'DOT', 'DOTDOWN', 'DUSK', 'DYDX',
        #                 'EGLD', 'ELF', 'ENJ', 'ENS', 'EOS', 'EPX', 'ETC', 'ETHDOWN', 'EUR', 'FET',
        #                 'FIDA', 'FIL', 'FLM', 'FLOW', 'FLUX', 'FORTH', 'FRONT', 'FTM', 'FTT', 'FXS', 'GAL',
        #                 'GALA', 'GBP', 'GLMR', 'GMT', 'GRT', 'GTC', 'HBAR', 'HIGH', 'HIVE', 'HNT', 'HOT',
        #                 'ICP', 'IDEX', 'IMX', 'IOST', 'IOTA', 'IOTX', 'JASMY', 'JST', 'KAVA', 'KDA', 'KLAY',
        #                 'KSM', 'LDO', 'LEVER', 'LINA', 'LINK', 'LIT', 'LOKA', 'LRC', 'LTC', 'LUNA', 'LUNC',
        #                 'MANA', 'MASK', 'MATIC', 'MBOX', 'MINA', 'MIR', 'MOVR', 'MTL', 'NEAR', 'NEO', 'NMR',
        #                 'OGN', 'ONE', 'ONG', 'OOKI', 'OP', 'PEOPLE', 'PLA', 'POND', 'PORTO', 'POWR', 'PUNDIX',
        #                 'PYR', 'QNT', 'QTUM', 'RAD', 'REQ', 'RNDR', 'ROSE', 'RSR', 'RUNE', 'RVN', 'SAND',
        #                 'SHIB', 'SKL', 'SLP', 'SNX', 'SOL', 'SRM', 'STMX', 'STORJ', 'STPT', 'STX', 'SUSHI',
        #                 'SXP', 'T', 'THETA', 'TKO', 'TLM', 'TRB', 'TRX', 'TRY', 'TUSD', 'UNFI', 'UNI', 'USDC',
        #                 'USTC', 'VET', 'VGX', 'VIDT', 'VOXEL', 'WAVES', 'WBTC', 'WIN', 'WING', 'WNXM',
        #                 'WOO', 'WTC', 'XLM', 'XMR', 'XRP', 'XTZ', 'YFI', 'YFII', 'YGG', 'ZEC', 'ZIL', 'ZRX']
        #
        self.symbols = ['1INCH', 'AAVE', 'ACA', 'ACH', 'ACM', 'ADA', 'ADX', 'AE', 'AERGO', 'AGI', 'AGIX', 'AGLD',
                        'AION', 'AKRO', 'ALCX', 'ALGO', 'ALICE', 'ALPACA', 'ALPHA', 'ALPINE', 'AMB', 'AMP', 'ANC',
                        'ANKR', 'ANT', 'ANY', 'APE', 'API3', 'APPC', 'AR', 'ARDR', 'ARK', 'ARN', 'ARPA', 'ASR', 'AST',
                        'ASTR', 'ATA', 'ATM', 'ATOM', 'AUCTION', 'AUD', 'AUDIO', 'AUTO', 'AVA', 'AVAX', 'AXS', 'BADGER',
                        'BAKE', 'BAL', 'BAND', 'BAR', 'BAT', 'BCC', 'BCD', 'BCH', 'BCHA', 'BCHABC', 'BCHSV', 'BCN',
                        'BCPT', 'BDOT', 'BEAM', 'BEAR', 'BEL', 'BETA', 'BETH', 'BGBP', 'BICO', 'BIDR', 'BIFI', 'BKRW',
                        'BLZ', 'BNB', 'BNBBEAR', 'BNBBULL', 'BNT', 'BNX', 'BOND', 'BOT', 'BQX', 'BRD', 'BRL', 'BSW',
                        'BTC', 'BTCB', 'BTCST', 'BTG', 'BTS', 'BTT', 'BTTC', 'BULL', 'BURGER', 'BUSD', 'BVND', 'BZRX',
                        'C98', 'CAKE', 'CDT', 'CELO', 'CELR', 'CFX', 'CHAT', 'CHESS', 'CHR', 'CHZ', 'CITY', 'CKB',
                        'CLOAK', 'CLV', 'CMT', 'CND', 'COCOS', 'COMP', 'COS', 'COTI', 'COVER', 'CREAM', 'CRV', 'CTK',
                        'CTSI', 'CTXC', 'CVC', 'CVP', 'CVX', 'DAI', 'DAR', 'DASH', 'DATA', 'DCR', 'DEGO', 'DENT',
                        'DEXE', 'DF', 'DGB', 'DGD', 'DIA', 'DLT', 'DNT', 'DOCK', 'DODO', 'DOGE', 'DOT', 'DREP', 'DUSK',
                        'DYDX', 'EASY', 'EDO', 'EGLD', 'ELF', 'ENG', 'ENJ', 'ENS', 'EOS', 'EOSBEAR', 'EOSBULL', 'EPS',
                        'EPX', 'ERD', 'ERN', 'ETC', 'ETH', 'ETHBEAR', 'ETHBULL', 'EUR', 'EVX', 'EZ', 'FARM', 'FET',
                        'FIDA', 'FIL', 'FIO', 'FIRO', 'FIS', 'FLM', 'FLOW', 'FLUX', 'FOR', 'FORTH', 'FRONT', 'FTM',
                        'FTT', 'FUEL', 'FUN', 'FXS', 'GAL', 'GALA', 'GAS', 'GBP', 'GHST', 'GLM', 'GLMR', 'GMT', 'GNO',
                        'GNT', 'GO', 'GRS', 'GRT', 'GTC', 'GTO', 'GVT', 'GXS', 'HARD', 'HBAR', 'HC', 'HEGIC', 'HIGH',
                        'HIVE', 'HNT', 'HOT', 'HSR', 'ICN', 'ICP', 'ICX', 'IDEX', 'IDRT', 'ILV', 'IMX', 'INJ', 'INS',
                        'IOST', 'IOTA', 'IOTX', 'IQ', 'IRIS', 'JASMY', 'JOE', 'JST', 'JUV', 'KAVA', 'KDA', 'KEEP',
                        'KEY', 'KLAY', 'KMD', 'KNC', 'KP3R', 'KSM', 'LAZIO', 'LDO', 'LEND', 'LEVER', 'LINA', 'LINK',
                        'LIT', 'LOKA', 'LOOM', 'LPT', 'LRC', 'LSK', 'LTC', 'LTO', 'LUN', 'LUNA', 'LUNC', 'MANA', 'MASK',
                        'MATIC', 'MBL', 'MBOX', 'MC', 'MCO', 'MDA', 'MDT', 'MDX', 'MFT', 'MINA', 'MIR', 'MITH', 'MKR',
                        'MLN', 'MOB', 'MOD', 'MOVR', 'MTH', 'MTL', 'MULTI', 'NANO', 'NAS', 'NAV', 'NBS', 'NCASH',
                        'NEAR', 'NEBL', 'NEO', 'NEXO', 'NGN', 'NKN', 'NMR', 'NPXS', 'NU', 'NULS', 'NXS', 'OAX', 'OCEAN',
                        'OG', 'OGN', 'OM', 'OMG', 'ONE', 'ONG', 'ONT', 'OOKI', 'OP', 'ORN', 'OST', 'OXT', 'PAX', 'PAXG',
                        'PEOPLE', 'PERL', 'PERP', 'PHA', 'PHB', 'PHX', 'PIVX', 'PLA', 'PNT', 'POA', 'POE', 'POLS',
                        'POLY', 'POND', 'PORTO', 'POWR', 'PPT', 'PROM', 'PROS', 'PSG', 'PUNDIX', 'PYR', 'QI', 'QKC',
                        'QLC', 'QNT', 'QSP', 'QTUM', 'QUICK', 'RAD', 'RAMP', 'RARE', 'RAY', 'RCN', 'RDN', 'REEF', 'REI',
                        'REN', 'RENBTC', 'REP', 'REQ', 'RGT', 'RIF', 'RLC', 'RNDR', 'ROSE', 'RPX', 'RSR', 'RUB', 'RUNE',
                        'RVN', 'SALT', 'SAND', 'SANTOS', 'SC', 'SCRT', 'SFP', 'SHIB', 'SKL', 'SKY', 'SLP', 'SNGLS',
                        'SNM', 'SNT', 'SNX', 'SOL', 'SPARTA', 'SPELL', 'SRM', 'SSV', 'STEEM', 'STMX', 'STORJ', 'STORM',
                        'STPT', 'STRAT', 'STRAX', 'STX', 'SUB', 'SUN', 'SUPER', 'SUSD', 'SUSHI', 'SWRV', 'SXP', 'SYS',
                        'T', 'TCT', 'TFUEL', 'THETA', 'TKO', 'TLM', 'TNB', 'TNT', 'TOMO', 'TORN', 'TRB', 'TRIBE',
                        'TRIG', 'TROY', 'TRU', 'TRX', 'TRY', 'TUSD', 'TUSDB', 'TVK', 'TWT', 'UAH', 'UFT', 'UMA', 'UNFI',
                        'UNI', 'USDC', 'USDP', 'USDS', 'USDSB', 'USDT', 'UST', 'USTC', 'UTK', 'VAI', 'VEN', 'VET',
                        'VGX', 'VIA', 'VIB', 'VIBE', 'VIDT', 'VITE', 'VOXEL', 'VTHO', 'WABI', 'WAN', 'WAVES', 'WAXP',
                        'WBTC', 'WIN', 'WING', 'WINGS', 'WNXM', 'WOO', 'WPR', 'WRX', 'WTC', 'XEC', 'XEM', 'XLM', 'XMR',
                        'XNO', 'XRP', 'XRPBEAR', 'XRPBULL', 'XTZ', 'XVG', 'XVS', 'XZC', 'YFI', 'YFII', 'YGG', 'YOYO',
                        'ZAR', 'ZEC', 'ZEN', 'ZIL', 'ZRX']
        
        # ezeket előre teszem hogy minden szűkítésnél bent legyenű
        axd = ['USDT', 'BTC', 'ETH']
        for xd in axd:
            self.symbols.remove(xd)
        self.symbols = ['USDT', 'BTC', 'ETH'] + self.symbols


        ## off symbols
        self.off_symbols = ['BNB', 'USDC', 'TUSD', 'TRY']
        for osy in self.off_symbols:
            self.symbols.remove(osy)

        self.selected_symbols = self.symbols[:self.symbols_no]  ## kiválasztam amivel dolgozok szűkíthetem a kört
        # print(self.mpi, "Symbols:", len(self.selected_symbols))
        self.all_pairs = self.defa_all_pairs()
        self.selected_pairs = self.defa_selected_pairs2()  ##a kiválasztott szimbólumokhoz kapcsolódó párokat kiválasztom
        self.price = self.defa_price_dict()


        # self.price_flow = self.defa_price_dict()

        self.pai = self.defa_pair_info2()  # pair info
        self.monitor_recieved_data = 1
        self.monitor_data_manager_count = 1
        self.monitor_deal_hunter_count = 1
        self.sleep_data_manager = 0.005
        self.sleep_deal_hunter = 0.01
        self.deal_count = 0

        self.run_id = self.get_run_id()

        # self.save_deal_data("symbol1", "side1", 1234567.12345678,
        #                        "symbol2", "side2", 1234567.12345678,
        #                        "symbol3", "side3", 1234567.12345678,
        #                        1.1234567, 0.0095)

        self.wallet = {}
        self.roll_back = False
        self.refresh_map = None
        self.triangles = None
        self.print_info()
        self.refresh_wallet()
        self.arb_matrix()
        self.last_arb = ""
        self.wallet_last_total = 0
        if self.process == 1:
            self.goto_start_asset()
        if self.wallet[self.start_symbol] < self.lot_size and self.process == 1:
                print("Not enough money.")
                sys.exit()
        self.strat_threads()
        self.start_asyc_websocket()

    def set_trade_lock(self, pos):
        lock.acquire()
        self.trade_lock[0] = pos  #  ha 0 nem lehet kereskedni
        lock.release()

    def get_run_id(self):
        inow = datetime.now()
        istr = str(inow.day) + "_" +\
               str(inow.hour) + "_" + \
               str(inow.minute)
        return "nDot_" + istr

    def monitoring(self):
        while True:
            sleep_monitoring = 5

            over_weight = 1.15  ## ha lehet akkor a data mangement mindíg kicsit gyorsabb legyen mist a érkezés
            time.sleep(sleep_monitoring)
            # print(self.mpi, "monitoring last", sleep_monitoring, "sec.")
            # print(self.mpi, "monitor_recieved_data", self.monitor_recieved_data)
            # print(self.mpi, "monitor_datamanager_count", self.monitor_data_manager_count)
            # print(self.mpi, "monitor_deal_hunter_count", self.monitor_deal_hunter_count)
            # print(self.mpi, "sleep_data_manager now", self.sleep_data_manager)
            self.sleep_data_manager = self.sleep_data_manager / \
                                      ((self.monitor_recieved_data * over_weight) / self.monitor_data_manager_count)
            print(self.mpi, "sleep_data_manager new", self.sleep_data_manager)
            self.monitor_recieved_data = 1
            self.monitor_data_manager_count = 1
            self.monitor_deal_hunter_count = 1


    def strat_threads(self):
        pass
        # task1 = Thread(target=self.data_manager_loop, args=[])
        # task2 = Thread(target=self.data_manager_loop2, args=[])
        task3 = Thread(target=self.deal_hunter_loop, args=[])
        # task1.start()
        # task2.start()
        task3.start()

        # task3 = Thread(target=self.monitoring, args=[])
        # task3.start()

        # task3.start()

    def goto_start_asset(self):
        print("Refactor your portfolio to:", self.start_symbol)
        # self.print_wallet()
        for sy in self.wallet:
            if self.wallet[sy] > 0 and sy != 'BNB':
                pair_sell = "".join([sy, self.start_symbol])  # xxxusdt
                pair_buy = "".join([self.start_symbol, sy])  # usdtxxx
                if pair_sell in self.all_pairs:
                    min_quite_sell = self.pai[pair_sell]['minqty']
                    step_size_sell = self.pai[pair_sell]['stepsize']
                    base_sell = self.pai[pair_sell]['base']
                    quote_sell = self.pai[pair_sell]['quote']
                    trade_symbol_sell = "".join([base_sell, quote_sell])
                    # print(self.wallet[sy], step_size_sell)
                    qty_sell = self.round_with_step_size(self.wallet[sy], step_size_sell)
                    if qty_sell > min_quite_sell:
                        try:
                            orderx = self.bx_client.order_market(symbol=trade_symbol_sell,
                                                                 side=SIDE_SELL,
                                                                 quantity='{0:.8f}'.format(qty_sell))
                            # print(orderx)
                        except BinanceAPIException as e:
                            pass
                            # print(e.status_code)
                            # print(e.message)
                        else:
                            print('  Symbol:', trade_symbol_sell, 'side', SIDE_SELL, 'quantity', qty_sell)
                elif pair_buy in self.all_pairs:
                    min_quite_buy = self.pai[pair_buy]['minqty']
                    step_size_buy = self.pai[pair_buy]['stepsize']
                    minnotional_buy = self.pai[pair_buy]['minnotional']
                    # avgpricemins = self.pai[pair_buy]['avgpricemins']
                    base_buy = self.pai[pair_buy]['base']
                    quote_buy = self.pai[pair_buy]['quote']
                    trade_symbol_buy = "".join([base_buy, quote_buy])
                    qty_buy = self.wallet[sy]
                    if qty_buy > minnotional_buy:
                        try:
                            orderx = self.bx_client.order_market(symbol=trade_symbol_buy,
                                                                 side=SIDE_BUY,
                                                                 quoteOrderQty='{0:.8f}'.format(qty_buy))
                            # print(orderx)
                        except BinanceAPIException as e:
                            pass
                            # print(e.status_code)
                            # print(e.message)
                        else:
                            print('  Symbol', trade_symbol_buy, 'side', SIDE_BUY, 'quoteOrderQty', qty_buy)

        self.refresh_wallet()
        self.print_wallet()

    def print_info(self):
        if self.process == 1:
            print("")
            print("nDot.io Arbitrage")
            print("  Start symbol:", self.start_symbol)
            print("  Lot size:", self.lot_size, self.start_symbol)
            print("  Spread:", self.spread * 100, "%")
            print("  Off symbols:", self.off_symbols)
            # print("Number of pairs:", len(self.selected_pairs))
            print("  Price modifier 1 (orderbook, trade):", self.tick_modifier1, self.trade_tick_modifier1, " tick")
            print("  Price modifier 2 (orderbook, trade):", self.tick_modifier2, self.trade_tick_modifier2, " tick")
            print("  Price modifier 3 (orderbook, trade):", self.tick_modifier3, self.trade_tick_modifier3, " tick")


    def get_slice_index(self, xlen, parts, slice_no):
        slices = np.array_split(list(np.arange(0, xlen)), parts)
        start = slices[slice_no - 1][0]
        end = slices[slice_no - 1][-1:][0]
        end += 1
        end = min(end, xlen)
        return start, end

    def arb_matrix(self):
        all_pairs_way = []
        for sp in self.selected_pairs:
            all_pairs_way.append("".join([self.selected_pairs[sp][0], self.selected_pairs[sp][1]]))
            all_pairs_way.append("".join([self.selected_pairs[sp][1], self.selected_pairs[sp][0]]))
        
        if not self.load_triangles:
            # print(self.mpi, "Create arb martix")
    
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
    
            ## kitörlöm azokat a kombinációkat amelyek nem is léteznek
            del_index = []
            for row_x in range(self.triangles.shape[0]):
                for col_x in range(3):
                    if self.triangles[row_x][col_x].decode('UTF-8') not in all_pairs_way:
                        del_index.append(row_x)
            self.triangles = np.delete(self.triangles, del_index, 0)

            if self.save_triangles:
                with open(self.save_triangles, 'wb') as file:
                    np.save(file, self.triangles)
                print("Triangle save:", self.save_triangles)
        else:
            with open(self.load_triangles, 'rb') as file:
                self.triangles = np.load(file)
        

        self.triangles = self.triangles[:self.max_triangles, :]  # fejlesztéshez, még vissza tudom venni a számát
        # print(self.mpi, 'Reduced triangles size:', self.triangles.shape)
        # processre szétdarabolom
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
        self.spread_array = np.full(self.triangles.shape[0], self.spred_xxx, dtype=float)

        # self.ab1_qty = np.full(self.triangles.shape[0], 0.00000000, dtype=float)
        # self.ab2_qty = np.full(self.triangles.shape[0], 0.00000000, dtype=float)
        # self.ab3_qty = np.full(self.triangles.shape[0], 0.00000000, dtype=float)

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
                symbo_info = self.get_symbol_info(''.join([si1, si2]))
                if ''.join([si1, si2]) in self.all_pairs and \
                        symbo_info['status'] == 'TRADING' and \
                        "MARKET" in symbo_info['orderTypes']:
                    i_selected_pairs[''.join([si1, si2])] = [si1, si2]
        return i_selected_pairs

    def defa_selected_pairs2(self):
        sydi = {}
        for sx in self.exchange_info['symbols']:
            if sx['status'] == 'TRADING' and "MARKET" in sx['orderTypes']:
                sydi[sx['symbol']] = [sx['baseAsset'], sx['quoteAsset']]
        return sydi

    def get_symbol_info(self, symbol):
        for item in self.exchange_info['symbols']:
            if item['symbol'] == symbol.upper():
                return item
        return None

    def defa_price_dict(self):
        i_selected_pairs = {}
        for si1 in self.selected_symbols:
            for si2 in self.selected_symbols:
                i_selected_pairs["".join([si1, si2])] = 0.0
                i_selected_pairs["".join([si2, si1])] = 0.0
        return i_selected_pairs

    # def defa_price_dict(self):
    #     i_selected_pairs = {}
    #     for si1 in self.selected_symbols:
    #         for si2 in self.selected_symbols:
    #             i_selected_pairs["".join([si1, si2])] = 1.0
    #             i_selected_pairs["".join([si2, si1])] = 1.0
    #     return i_selected_pairs

    def defa_pair_info2(self):
        pair_info = {}
        for sx in self.exchange_info['symbols']:
            if sx['status'] == 'TRADING' and "MARKET" in sx['orderTypes']:
                    base = sx['baseAsset']
                    quot = sx['quoteAsset']
                    # if base == "USDT" and quot == "TRY":
                    filters = {}
                    for fx in sx['filters']:
                        filters[fx['filterType']] = fx

                    ticksize = float(filters['PRICE_FILTER']['tickSize'])
                    step_size = float(filters['LOT_SIZE']['stepSize'])
                    min_qty = float(filters['LOT_SIZE']['minQty'])
                    market_minqt = float(filters['MARKET_LOT_SIZE']['minQty'])
                    market_stepsize = float(filters['MARKET_LOT_SIZE']['stepSize'])
                    minnotional = float(filters['MIN_NOTIONAL']['minNotional'])
                    avgpricemins = float(filters['MIN_NOTIONAL']['avgPriceMins'])

                    
                    
                    
                    # if sx['filters'][0]['filterType'] == 'PRICE_FILTER':
                    #     ticksize = float(sx['filters'][0]['tickSize'])
                    # else:
                    #     print("Filter hiba. PRICE_FILTER")
                    #     sys.exit()
                    #
                    # if sx['filters'][2]['filterType'] == 'LOT_SIZE':
                    #     step_size = float(sx['filters'][2]['stepSize'])
                    #     min_qty = float(sx['filters'][2]['minQty'])
                    # else:
                    #     print("Filter hiba. LOT_SIZE")
                    #     sys.exit()
                    #
                    # if sx['filters'][6]['filterType'] == 'MARKET_LOT_SIZE':
                    #     market_minqt = float(sx['filters'][6]['minQty'])
                    #     market_stepsize = float(sx['filters'][6]['stepSize'])
                    # else:
                    #     print("Filter hiba. MARKET_LOT_SIZE")
                    #     sys.exit()
                    #
                    # if sx['filters'][3]['filterType'] == 'MIN_NOTIONAL':
                    #     minnotional = float(sx['filters'][3]['minNotional'])
                    #     avgpricemins = float(sx['filters'][3]['avgPriceMins'])
                    # else:
                    #     print("Filter hiba. MIN_NOTIONAL")
                    #     sys.exit()

                    data_both = {'orig_symbol': sx['symbol'],
                                 'stepsize': step_size,
                                 'minqty': min_qty,
                                 'base': base,
                                 'quote': quot,
                                 'tick_size': ticksize,
                                 'market_minqty': market_minqt,
                                 'market_stepsize': market_stepsize,
                                 'minnotional': minnotional,
                                 'avgpricemins': avgpricemins}

                    data_sell = data_both.copy()
                    data_buy = data_both.copy()
                    data_sell['side'] = 'SELL'
                    data_sell['symbol_from'] = sx['baseAsset']
                    data_sell['symbol_to'] = sx['quoteAsset']
                    data_buy['side'] = 'BUY'
                    data_buy['symbol_from'] = sx['quoteAsset']
                    data_buy['symbol_to'] = sx['baseAsset']

                    pair_info[sx['baseAsset'] + sx['quoteAsset']] = data_sell
                    pair_info[sx['quoteAsset'] + sx['baseAsset']] = data_buy

        return pair_info

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

    def refresh_wallet(self):
        self.account = self.bx_client.get_account()
        self.wallet = {}
        for sesy in self.selected_symbols:
            self.wallet[sesy] = self.get_amount_by_symbol(sesy)

    def print_wallet(self):
        # BNB külön kezelem mert nincs benne a selected symbolsban
        self.wallet["BNB"] = self.get_amount_by_symbol("BNB")  # mivel erre nem lehet kereskedni ezt külön beteszem
        bnbusdt = self.get_BNBUSDT()
        bnbbtc = self.get_BNBBTC()
        self.price["BNBUSDT"] = bnbusdt
        self.price["USDTBNB"] = 1 / bnbusdt

        self.price["BNBBTC"] = bnbbtc
        self.price["BTCBNB"] = 1 / bnbbtc

        print("")
        print("Spot wallet:")
        print(" symbol    amount       USD        BTC")
        total_in_usdt = 0
        total_in_btc = 0

        # print(self.wallet)
        for ea in self.wallet:
            # print(ea)
            if self.wallet[ea] != 0:
                if ea == 'USDT':
                    price_usdt = 1
                else:
                    # prc1 = 0 if self.price[ea + "USDT"] == 1 else self.price[ea + "USDT"]
                    # prc2 = 0 if self.price["USDT" + ea] == 1 else 1 / self.price["USDT" + ea]
                    prc1 = self.price[ea + "USDT"]
                    prc2 = 1 / self.price["USDT" + ea] if self.price["USDT" + ea] != 0 else 0
                    price_usdt = prc1 if ea + "USDT" in self.selected_pairs else prc2

                if "BTC" not in self.off_symbols:
                    if ea == 'BTC':
                        price_btc = 1
                    else:
                        # prc1 = 0 if self.price[ea + "BTC"] == 1 else self.price[ea + "BTC"]
                        # prc2 = 0 if self.price["BTC" + ea] == 1 else 1 / self.price["BTC" + ea]
                        prc1 = self.price[ea + "BTC"]
                        prc2 = 1 / self.price["BTC" + ea] if self.price["BTC" + ea] != 0 else 0
                        price_btc = prc1 if ea + "BTC" in self.selected_pairs else prc2
                else:
                    price_btc = 0

                symbol_value_in_usdt = round(self.wallet[ea] * price_usdt, 3)
                total_in_usdt += symbol_value_in_usdt

                symbol_value_in_btc = round(self.wallet[ea] * price_btc, 8)
                total_in_btc += symbol_value_in_btc
                eap = ea + "     "
                amount = '{0:.8f}'.format(self.wallet[ea]) + "                    "
                vusdt = '{0:.2f}'.format(symbol_value_in_usdt) + "                   "
                vbtc = '{0:.8f}'.format(symbol_value_in_btc) + "                   "
                print(" ", eap[0:5], amount[0:15], vusdt[0:10], vbtc[0:10], )

        total_usdtstr = '{0:.2f}'.format(total_in_usdt) + "                                  "
        total_btcstr = '{0:.8f}'.format(total_in_btc) + "                                  "
        last_total = '{0:.8f}'.format(self.wallet_last_total) + "                                  "

        print("________________________________________________")
        print("Total:                 ", total_usdtstr[:10], total_btcstr[:10])
        print("Last total:            ", str(self.get_status("total1")) + "." + str(self.get_status("total2")))
        total_in_usdt = round(total_in_usdt, 3)
        self.set_status("total1", int(str(total_in_usdt).split(".")[0]))
        self.set_status("total2", int(str(total_in_usdt).split(".")[1]))
        self.print_deal_counter()
        self.wallet_last_total = total_in_usdt
        print("")

    def round_with_step_size(self, quantity, step_size, reduce=0):
        reduce = Decimal(reduce * step_size)  # ennyi darabbal visszaveszi
        quantity = Decimal(str(quantity))
        if step_size > 0:
            i_return = round(float(quantity - quantity % Decimal(str(step_size)) - reduce), 8)
        else:
            i_return = round(float(quantity - reduce), 8)
        return i_return

    def start_asyc_websocket(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket())
        loop.close()


    async def asyc_websocket(self):
        global bid_ask_flow, order_qty
        max_sockets = 1500

        i_socket_list = []
        for sp in tuple(self.selected_pairs.keys()):
            base_asset = self.pai[sp]['base']
            quote_asset = self.pai[sp]['quote']
            if base_asset in self.off_symbols or quote_asset in self.off_symbols:
                pass
                # print("off", base_asset + quote_asset)
            else:
                if self.isin_triangles(base_asset + quote_asset) or self.isin_triangles(quote_asset + base_asset):
                    i_socket_list.append(sp.lower() + '@bookTicker')
                    i_socket_list.append(sp.lower() + '@depth5@100ms')
                    s_symbol = base_asset + quote_asset
                    i_symbol = quote_asset + base_asset
                    # létehozom az üres dictionerykat is
                    bid_ask_flow[s_symbol.upper()] = 0
                    bid_ask_flow[i_symbol.upper()] = 0
                    order_qty[s_symbol.upper()] = 0
                    order_qty[i_symbol.upper()] = 0


        # max_sockets = 150
        # socket_count = 1
        # i_socket_list = []
        # i_socket_list.append('btcusdt@bookTicker')
        # all_bid_ask_flow["BTCUSDT"] = 1
        # all_bid_ask_flow["USDTBTC"] = 1
        # for sp in tuple(self.selected_pairs.keys()):
        #     base_asset = self.pai[sp]['base']
        #     quote_asset = self.pai[sp]['quote']
        #     if base_asset in self.off_symbols or quote_asset in self.off_symbols:
        #         pass
        #         # print("off", base_asset + quote_asset)
        #     else:
        #         if sp.lower() != "btcusdt" and socket_count != max_sockets:
        #             if self.isin_triangles(base_asset + quote_asset) or self.isin_triangles(quote_asset + base_asset):
        #                 i_socket_list.append(sp.lower() + '@bookTicker')
        #                 s_symbol = base_asset + quote_asset
        #                 i_symbol = quote_asset + base_asset
        #                 all_bid_ask_flow[s_symbol.upper()] = 1
        #                 all_bid_ask_flow[i_symbol.upper()] = 1
        #                 socket_count += 1
        #     if socket_count == max_sockets:
        #         break

        time.sleep(.3 * self.process)
        i_socket_list = i_socket_list[:max_sockets]

        if self.process == self.cores:
            print(self.mpi, 'Number of sockets:', len(i_socket_list),
                  "Created arb martix shape:", self.triangles.shape, "Last speak.")

        client = await AsyncClient.create()
        bm = BinanceSocketManager(client)
        ts = bm.multiplex_socket(i_socket_list)
        tcount = 1

        async with ts as tscm:
            while True:
                res = await tscm.recv()
                self.monitor_recieved_data += 1
                # print(self.process, res)

                # socker data

                if res['stream'][-4:] == "cker":
                    s1 = self.selected_pairs[res['data']['s']][0]
                    s2 = self.selected_pairs[res['data']['s']][1]
                    # print(res['data']['s'], s1, s2)
                    s1s2 = "".join([s1, s2])
                    s2s1 = "".join([s2, s1])
    
                    bid = round(float(res['data']['b']), 8)
                    bid_qty = round(float(res['data']['B']), 8)
                    ask = round(float(res['data']['a']), 8)
                    ask_qty = round(float(res['data']['A']), 8)
    
                    rec_ask = 1 / ask
    
                    bid_ask_flow[s1s2] = bid
                    bid_ask_flow[s2s1] = rec_ask
    
                    order_qty[s1s2] = bid_qty
                    order_qty[s2s1] = ask_qty
    
                    self.price[s1s2] = bid
                    self.price[s2s1] = ask
    
                    # np.put(self.ab1, self.refresh_map[s1s2][0], bid)
                    # np.put(self.ab2, self.refresh_map[s1s2][1], bid)
                    # np.put(self.ab3, self.refresh_map[s1s2][2], bid)
    
                    # np.put(self.ab1, self.refresh_map[s2s1][0], rec_ask)
                    # np.put(self.ab2, self.refresh_map[s2s1][1], rec_ask)
                    # np.put(self.ab3, self.refresh_map[s2s1][2], rec_ask)
                else:
                    s1 = self.selected_pairs[res['stream'].split("@")[0].upper()][0]
                    s2 = self.selected_pairs[res['stream'].split("@")[0].upper()][1]
                    # print(res['data']['s'], s1, s2)
                    s1s2 = "".join([s1, s2])
                    s2s1 = "".join([s2, s1])
                    # print(res['stream'].split("@")[0])
                    # print(res['data']['bids'][2][0])
                    # bid = round(float(res['data']['bids'][2][0]), 8)
                    # bid_qty = round(float(res['data']['bids'][2][1]), 8)
                    # ask = round(float(res['data']['asks'][2][0]), 8)
                    # ask_qty = round(float(res['data']['bids'][2][1]), 8)
    
                    # rec_ask = 1 / ask
    
                    # bid_ask_flow[s1s2] = bid
                    # bid_ask_flow[s2s1] = rec_ask
    
                    # order_qty[s1s2] = bid_qty
                    # order_qty[s2s1] = ask_qty
                    #
                    # self.price[s1s2] = round(float(res['data']['bids'][0][0]), 8)
                    # self.price[s2s1] = round(float(res['data']['asks'][0][0]), 8)
    
                    np.put(self.ab1, self.refresh_map[s1s2][0], round(float(res['data']['bids'][0][0]), 8))
                    np.put(self.ab2, self.refresh_map[s1s2][1], round(float(res['data']['bids'][1][0]), 8))
                    np.put(self.ab3, self.refresh_map[s1s2][2], round(float(res['data']['bids'][0][0]), 8))
    
                    np.put(self.ab1, self.refresh_map[s2s1][0], 1 / round(float(res['data']['asks'][0][0]), 8))
                    np.put(self.ab2, self.refresh_map[s2s1][1], 1 / round(float(res['data']['asks'][1][0]), 8))
                    np.put(self.ab3, self.refresh_map[s2s1][2], 1 / round(float(res['data']['asks'][0][0]), 8))
                
                # profit_array = self.ab1 * self.ab2 * self.ab3 * self.spread_array
                # profit = np.max(profit_array)
                #
                # if profit > 1:
                #     max_row = np.argmax(profit_array)
                #     sy1 = self.triangles[max_row][0].decode('UTF-8')
                #     sy2 = self.triangles[max_row][1].decode('UTF-8')
                #     sy3 = self.triangles[max_row][2].decode('UTF-8')
                #
                #     await self.deal_hunter(sy1, sy2, sy3, max_row, profit)
        #

        # ez sosem fog lefutni mert a szervernek nincs leállítási funkciója csak kilövöm éskész
        await client.close_connection()

    # def data_manager_loop(self):
    #     time.sleep(5)
    #     ilen = int(len(list(bid_ask_flow)) / 2)
    #     ilist = list(bid_ask_flow.keys())[0:ilen]
    #     print(self.mpi, "Start data manager.")
        # while True:
        #     self.monitor_data_manager_count += 1
        #     self.data_manager(ilist)
        #     time.sleep(self.sleep_data_manager)


    # def data_manager_loop2(self):
    #     time.sleep(5)
    #     ilen = int(len(list(bid_ask_flow)) / 2)
    #     ilist = list(bid_ask_flow.keys())[ilen:1500]
    #     print(self.mpi, "Start data manager.")
        # while True:
        #     self.monitor_data_manager_count += 1
        #     self.data_manager(ilist)
        #     time.sleep(self.sleep_data_manager)
    #
    # def data_manager_slice(self, ifrom, ito):
    #     global bid_ask_flow, order_qty
    #     for sy in ilist:
    #
    #         np.put(self.ab1, self.refresh_map[sy][0], bid_ask_flow[sy])
    #         np.put(self.ab2, self.refresh_map[sy][1], bid_ask_flow[sy])
    #         np.put(self.ab3, self.refresh_map[sy][2], bid_ask_flow[sy])
    #
    #         np.put(self.ab1_qty, self.refresh_map[sy][0], order_qty[sy])
    #         np.put(self.ab2_qty, self.refresh_map[sy][1], order_qty[sy])
    #         np.put(self.ab3_qty, self.refresh_map[sy][2], order_qty[sy])

    # def data_manager(self, ilist):
    #     global bid_ask_flow, order_qty
    #     for sy in ilist:
    #         print(sy)
    #         time.sleep(1)
    #         tick_size = self.pai[sy]['tick_size']
    #         self.price_flow[sy] = all_bid_ask_flow[sy]

            # np.put(self.ab1, self.refresh_map[sy][0], bid_ask_flow[sy])
            # np.put(self.ab2, self.refresh_map[sy][1], bid_ask_flow[sy])
            # np.put(self.ab3, self.refresh_map[sy][2], bid_ask_flow[sy])
            #
            # np.put(self.ab1_qty, self.refresh_map[sy][0], order_qty[sy])
            # np.put(self.ab2_qty, self.refresh_map[sy][1], order_qty[sy])
            # np.put(self.ab3_qty, self.refresh_map[sy][2], order_qty[sy])

            # np.put(self.ab1, self.refresh_map[s2s1][0], 1 / (ask + (self.tick_modifier1 * tick_size)))
            # np.put(self.ab2, self.refresh_map[s2s1][1], 1 / (ask + (self.tick_modifier2 * tick_size)))
            # np.put(self.ab3, self.refresh_map[s2s1][2], 1 / (ask + (self.tick_modifier3 * tick_size)))

    def deal_hunter_loop(self):
        time.sleep(10)
        if self.process == self.cores:
            print(self.mpi, "Start deal hunter. Last speak.")
        while True:
            self.monitor_deal_hunter_count += 1
            profit_array = self.ab1 * self.ab2 * self.ab3 * self.spread_array
            profit = np.max(profit_array)

            if profit > 1:
                max_row = np.argmax(profit_array)
                sy1 = self.triangles[max_row][0].decode('UTF-8')
                sy2 = self.triangles[max_row][1].decode('UTF-8')
                sy3 = self.triangles[max_row][2].decode('UTF-8')
                self.deal_hunter(sy1, sy2, sy3, max_row, profit)
            time.sleep(self.sleep_deal_hunter)

    def deal_hunter(self, sy1, sy2, sy3, max_row, profit):
        global bid_ask_flow, order_qty
        # if self.trade_lock[0] == 1:
        #     self.set_trade_lock(0)
        #     for x in range(1000):
        #         print(sy1, '{0:.8f}'.format(self.price[sy1]),
        #               sy2, '{0:.8f}'.format(self.price[sy2]),
        #               sy3,  '{0:.8f}'.format(self.price[sy3]))
        #         time.sleep(.01)
        #     self.set_trade_lock(1)
        # return
        if self.last_arb == ''.join([sy1, sy2, sy3]) \
                or self.trade_lock[0] == 0\
                or min(self.price[sy1], self.price[sy2], self.price[sy3]) == 0:
            return

        self.set_trade_lock(0)
        # print(self.mpi, "Trade *********************************************************************")
        # profit_array = self.ab1 * self.ab2 * self.ab3
        # print(profit_array)
        # -1 legnagyobb -2 a második legnagyobb stb stb.
        # max_row = profit_array.argsort()[-1]
        # max_row = np.argmax(profit_array)
        # sy1 = self.triangles[max_row][0].decode('UTF-8')
        # sy2 = self.triangles[max_row][1].decode('UTF-8')
        # sy3 = self.triangles[max_row][2].decode('UTF-8')

        # arb_str = "".join([sy1, " - ", sy2, " - ", sy3])

        # self.data_manager([sy1, sy2, sy3])

        # existing_shm = shared_memory.SharedMemory(name=self.shared_memory_name)
        # np_array = np.ndarray((1,), dtype=np.int64, buffer=existing_shm.buf)

        # amount0 = self.lot_size
        # amount1 = amount0 * self.ab1[max_row]
        # amount2 = amount1 * self.ab2[max_row]
        # amount3 = amount2 * self.ab2[max_row]

        # print(round(amount3 - ((self.lot_size*self.spread) * 3), 3))
        # time.sleep(3)

        # qty_ready = min(order_qty[sy1] - (amount1 * 1.2),
        #                 order_qty[sy2] - (amount2 * 1.2),
        #                 order_qty[sy3] - (amount3 * 1.2))
        # qty_ready = 1

        # ezt azért csinálom így hogy a profit számolás csak akkor fusson ha kell
        # ha profit előtte lenne mindíg kellene számolni
        # if self.last_arb != arb_str \
        #         and np_array[0] == 1 \
        #         and 1 < self.ab1[max_row] * self.ab2[max_row] * self.ab3[max_row] - self.spred_x_3\
        #         and qty_ready > 0:

        # existing_shm = shared_memory.SharedMemory(name=self.shared_memory_name)
        # np_array = np.ndarray((1,), dtype=np.int64, buffer=existing_shm.buf)
        # lock.acquire()
        # np_array[0] = 0
        # lock.release()
        # existing_shm.close()

        # profit = self.ab1[max_row] * self.ab2[max_row] * self.ab3[max_row] - self.spred_x_3

        saved_ob_price1 = saved_ob_price_rec_1 = self.ab1[max_row]
        saved_ob_price2 = saved_ob_price_rec_2 = self.ab2[max_row]
        saved_ob_price3 = saved_ob_price_rec_3 = self.ab3[max_row]

        saved_orig_price1 = self.price[sy1]
        saved_orig_price2 = self.price[sy2]
        saved_orig_price3 = self.price[sy3]

## JUMP

        # jump_sy = "".join([self.start_symbol, self.pai[sy2]["symbol_to"]])
        # jump_symbol_side = self.pai[jump_sy]["side"]
        # jump_symbol = self.pai[jump_sy]["orig_symbol"]
        # jump_sy_back = "".join([self.pai[sy2]["symbol_from"], self.start_symbol])
        # jump_symbol_back_side = self.pai[jump_sy_back]["side"]
        # jump_symbol_back = self.pai[jump_sy_back]["orig_symbol"]

# market test
        # if self.pai[sy2]["symbol_to"] == "ETH":
        #     jump_sy = "ETHUSDT"
        #
        #     trx_qty = 0
        #     print("orig prices:", '{0:.8f}'.format(saved_orig_price1),
        #           '{0:.8f}'.format(saved_orig_price2),
        #           '{0:.8f}'.format(saved_orig_price3),
        #           round(saved_ob_price1 * saved_ob_price2 * saved_ob_price3 - self.spred_x_3, 8))
        #
        #     trip = 3
        #     trx_qty = 0
        #     for vx in range(trip):
        #         trx_qty = self.market_test(jump_sy, 1, trx_qty)
        #         print("new prices:", '{0:.8f}'.format(self.price[sy1]),
        #               '{0:.8f}'.format(self.price[sy2]),
        #               '{0:.8f}'.format(self.price[sy3]),
        #               round(self.ab1[max_row] * self.ab2[max_row] * self.ab3[max_row] - self.spred_x_3, 8))
        #         time.sleep(.5)
        #     self.market_test(jump_sy, 2, trx_qty * trip)

# ## JUMP
#             start_price = self.price[jump_sy]
#             print(jump_sy, jump_symbol,jump_symbol_side, start_price)
#             for l in range(20):
#                 time.sleep(.25)
#                 print(jump_sy_back, jump_symbol_back, jump_symbol_back_side, self.price[jump_sy_back])
#             self.lot_size = self.lot_size / start_price * self.price[jump_sy_back]- ((self.lot_size * self.spread) * 3)
#             print("profit", self.lot_size)

        # t_side1 = self.pai[sy1]['side']
        # t_side2 = self.pai[sy2]['side']
        # t_side3 = self.pai[sy3]['side']
        #
        # t_symbol1 = self.pai[sy1]['orig_symbol']
        # t_symbol2 = self.pai[sy2]['orig_symbol']
        # t_symbol3 = self.pai[sy3]['orig_symbol']

        # amount0 = self.lot_size
        # amount1 = amount0 * saved_ob_price1
        # amount2 = amount1 * saved_ob_price2
        # amount3 = amount2 * saved_ob_price3
        #
        # qty_ready = min(self.ab1_qty[max_row] - amount1,
        #                 self.ab2_qty[max_row] - amount2,
        #                 self.ab3_qty[max_row] - amount3)
        #
        # if qty_ready > 0:
        #     self.c["qty_ready"] += 1
        #
        # print(self.c["qty_ready"], "min qty", qty_ready)
        # print("side", t_side1, t_side2, t_side3)
        # print("symbol", t_symbol1, t_symbol2, t_symbol3)
        # print("calculated qty:", amount0, amount1, amount2, amount3)
        # print("orderbook qty: ", amount0, self.ab1_qty[max_row], self.ab2_qty[max_row], self.ab3_qty[max_row])
        # print("flow price: ", amount0, self.ab1[max_row], self.ab2[max_row], self.ab3[max_row])
        # print("orig price: ", amount0, self.price[sy1], self.price[sy2], self.price[sy3])

        print("")
        print(self.mpi, "Trade:")

        print("                         ", sy1, '      ',
              sy2, '    ',
              sy3, profit)


        self.roll_back = False
        t_side1 = self.pai[sy1]['side']
        t_side2 = self.pai[sy2]['side']
        t_side3 = self.pai[sy3]['side']

        # data collector --------------------------------
        # t_symbol1 = self.pai[sy1]['orig_symbol']
        # t_symbol2 = self.pai[sy2]['orig_symbol']
        # t_symbol3 = self.pai[sy3]['orig_symbol']
        #
        # self.save_deal_data(t_symbol1, t_side1, saved_ob_price1,
        #                     t_symbol2, t_side2, saved_ob_price2,
        #                     t_symbol3, t_side3, saved_ob_price3,
        #                     profit, self.spread)
        # time.sleep(3)
        # data collector --------------------------------

        # Order1 ------------------------------------------------------------
        # print(self.mpi, "Trade", arb_str)
        t_symbol1 = self.pai[sy1]['orig_symbol']
        t_base1 = self.pai[sy1]['base']
        t_quote1 = self.pai[sy1]['quote']
        t_step_size1 = self.pai[sy1]['stepsize']
        # t_min_qt1 = self.pai[sy1]['minqty']
        t_ticksize1 = self.pai[sy1]['tick_size']

        t_price1 = saved_orig_price1 + (t_ticksize1 * self.trade_tick_modifier1) \
            if t_side1 == "BUY" else saved_orig_price1 - (t_ticksize1 * self.trade_tick_modifier1)
        t_amount_mod_buy = self.round_with_step_size(
            self.lot_size / t_price1, t_step_size1, 1)
        t_amount1 = t_amount_mod_buy if t_side1 == "BUY" else self.lot_size


        # trade
        # print(self.wallet)
        for trade_try in range(2):
            print('  ', trade_try, 'try, 1 Symbol:', t_symbol1,
                  'Price: {0:.8f}'.format(t_price1),
                  'Side:', t_side1,
                  'Qty:', t_amount1)
            # print("x")
            try:
                order1 = self.bx_client.order_limit(symbol=t_symbol1,
                                                    price='{0:.8f}'.format(t_price1),
                                                    side=t_side1,
                                                    quantity=t_amount1,
                                                    timeInForce=TIME_IN_FORCE_FOK)
            except BinanceAPIException as e:
                print(e.status_code)
                print(e.message)
            # print(order1)

            if order1['status'] in [ORDER_STATUS_PARTIALLY_FILLED,
                                    ORDER_STATUS_FILLED]:
                break
        if order1['status'] in [ORDER_STATUS_PARTIALLY_FILLED,
                                ORDER_STATUS_FILLED]:
            # Order2 ------------------------------------------------------------
            executedQty_1 = round(float(order1['executedQty']), 8)
            cummulativeQuoteQty_1 = round(float(order1['cummulativeQuoteQty']), 8)
            # print(1)
            if "BUY" == t_side1:
                # print("3")
                self.wallet[t_base1] += executedQty_1
                # print("4")
                self.wallet[t_quote1] -= cummulativeQuoteQty_1
            else:
                # print("5")
                self.wallet[t_quote1] += cummulativeQuoteQty_1
                # print("6")
                self.wallet[t_base1] -= executedQty_1

            t_symbol2 = self.pai[sy2]['orig_symbol']
            t_base2 = self.pai[sy2]['base']
            t_quote2 = self.pai[sy2]['quote']
            t_step_size2 = self.pai[sy2]['stepsize']
            t_min_qt2 = self.pai[sy2]['minqty']
            t_ticksize2 = self.pai[sy2]['tick_size']
            # print("wallet", self.wallet[t_quote2], t_quote2)

            # print("Rollback calc")
            # self.data_manager([sy1, sy2, sy3])
            # print("  new", saved_ob_price1, bid_ask_flow[sy2], bid_ask_flow[sy3],
            #       saved_ob_price1 * bid_ask_flow[sy2] * bid_ask_flow[sy3] - self.spred_x_3)
            # print("  orig", saved_ob_price1, saved_ob_price2, saved_ob_price3, profit)
            if True or self.get_fills_avg_price_flow(order1) * bid_ask_flow[sy2] * bid_ask_flow[sy3] * self.spred_xxx > 1:
            # if saved_ob_price1 * saved_ob_price2 * saved_ob_price3 - self.spred_x_3 > 1:
                print("  go 2")
                mod_price = self.price[sy2]
                print('   Price modification:', saved_orig_price2, "->", mod_price)
            # else:
            #     mod_price = saved_orig_price2

                # t_price2 = mod_price + (t_ticksize2 * self.trade_tick_modifier2) \
                #     if t_side2 == "BUY" else \
                #     mod_price - (t_ticksize2 * self.trade_tick_modifier2)
                #
                # t_amount_mod2_buy = self.round_with_step_size(
                #     self.wallet[t_quote2] / ((t_price2 - (t_ticksize2 * self.trade_tick_modifier2))),
                #     t_step_size2, 1)
                # print("t_amount_mod2_buy", t_amount_mod2_buy)
                # t_amount2 = t_amount_mod2_buy if t_side2 == "BUY" else self.wallet[t_base2]
                # print("t_amount2", t_amount2)
                # t_amount2 = self.round_with_step_size(t_amount2, t_step_size2)
                # print("t_amount2", t_amount2)


                # print("7")
                # t_amount2 = self.wallet[t_base2] if t_side2 == "SELL" else self.wallet[t_quote2]
                # t_amount2 = self.round_qty_with_step_size(t_amount2,
                #                                           t_step_size2) if t_side2 == "SELL" else t_amount2
                ticker_steps = 0
                for trade_try2 in range(0, 2):

                    t_price2 = mod_price + (ticker_steps * t_ticksize2 * trade_try2) \
                        if t_side2 == "BUY" else \
                        mod_price - (ticker_steps * t_ticksize2 * trade_try2)

                    t_amount_mod2_buy = self.round_with_step_size(
                        self.wallet[t_quote2] / t_price2, t_step_size2, 1)
                    # print("t_amount_mod2_buy", t_amount_mod2_buy)
                    t_amount2 = t_amount_mod2_buy if t_side2 == "BUY" else self.wallet[t_base2]
                    # print("t_amount2", t_amount2)
                    t_amount2 = self.round_with_step_size(t_amount2, t_step_size2)

                    # print(self.wallet[t_quote1], self.wallet[t_base1])
                    # print(self.wallet[t_quote2], self.wallet[t_base2])
                    print("  ", trade_try2, 'try, 2 Symbol:', t_symbol2,
                          'Price', '{0:.8f}'.format(t_price2),
                          'Side:', t_side2,
                          'Qty:', t_amount2)

                    try:
                        # flow_profit = saved_ob_price1 * self.price_flow[sy2] * self.price_flow[
                        #     sy3] - self.spred_x_3
                        # print(saved_ob_price1, saved_ob_price2, saved_ob_price3)
                        # print(saved_ob_price1, self.price_flow[sy2], self.price_flow[sy3])
                        # print(saved_ob_price1, 1 / self.price_flow[sy2], self.price_flow[sy3])
                        # print(saved_ob_price1, self.price[t_base2 + t_quote2], self.price_flow[sy3])
                        # print(saved_ob_price1, self.price[t_quote2 + t_base2], self.price_flow[sy3])

                        # if flow_profit > 1:
                        # print("GO GO GO", flow_profit)

                        order2 = self.bx_client.order_limit(symbol=t_symbol2,
                                                            price='{0:.8f}'.format(t_price2),
                                                            side=t_side2,
                                                            quantity=t_amount2,
                                                            timeInForce=TIME_IN_FORCE_FOK)
                        print(order2)
                        # print(saved_ob_price1, self.price_flow[sy2], self.price_flow[sy3])
                        # print(saved_ob_price1, 1 / self.price_flow[sy2], self.price_flow[sy3])

                    except BinanceAPIException as e:
                        print(e.status_code)
                        print(e.message)

                    if order2['status'] in [ORDER_STATUS_PARTIALLY_FILLED,
                                            ORDER_STATUS_FILLED]:
                        break
                if order2['status'] in [ORDER_STATUS_PARTIALLY_FILLED,
                                        ORDER_STATUS_FILLED]:

                    # Order3 ------------------------------------------------------------
                    # print(order2)
                    executedQty_2 = round(float(order2['executedQty']), 8)
                    cummulativeQuoteQty_2 = round(float(order2['cummulativeQuoteQty']), 8)
                    if t_side2 == "BUY":
                        # print("3")
                        self.wallet[t_base2] += executedQty_2
                        # print("4")
                        self.wallet[t_quote2] -= cummulativeQuoteQty_2
                    else:
                        # print("5")
                        self.wallet[t_quote2] += cummulativeQuoteQty_2
                        # print("6")
                        self.wallet[t_base2] -= executedQty_2

                    t_symbol3 = self.pai[sy3]['orig_symbol']
                    t_base3 = self.pai[sy3]['base']
                    t_quote3 = self.pai[sy3]['quote']
                    t_step_size3 = self.pai[sy3]['stepsize']
                    t_min_qt3 = self.pai[sy3]['minqty']
                    t_ticksize3 = self.pai[sy3]['tick_size']

                    if t_side3 == "SELL":
                        t_amount3 = self.round_with_step_size(self.wallet[t_base3], t_step_size3)
                        t_quantity3 = t_amount3
                        t_quoteOrderQty3 = None
                    else:
                        t_amount3 = self.wallet[t_quote3]
                        t_quantity3 = None
                        t_quoteOrderQty3 = t_amount3

                    # t_amount3 = self.wallet[t_base3] if t_side3 == "SELL" else self.wallet[t_quote3]
                    # t_amount3 = self.round_qty_with_step_size(t_amount3,
                    #
                    # print(self.wallet[t_quote2], self.wallet[t_base2])
                    print('       3 Symbol:', t_symbol3,
                          'Side', t_side3,
                          'Qty:', t_amount3)

                    try:
                        order3 = self.bx_client.order_market(symbol=t_symbol3,
                                                             side=t_side3,
                                                             quantity=t_quantity3,
                                                             quoteOrderQty=t_quoteOrderQty3)
                        print(order3)
                    except BinanceAPIException as e:
                        print(e.status_code)
                        print(e.message)
                else:
                    # Roll Back  ------------------------------------------------------------
                    self.trade_roll_back(sy1, t_symbol1, t_quote1, t_base1, t_side1, t_step_size1, order1)
                    # if sy1 == t_symbol1:
                    #     t_asset_rb = t_quote1
                    # else:
                    #     t_asset_rb = t_base1
                    #
                    # t_quoteOrderQtyrb = self.wallet[t_asset_rb]
                    # inv_side = "SELL" if t_side1 == "BUY" else "BUY"
                    #
                    # if inv_side == "BUY":
                    #     quantityrb = None
                    #     quoteOrderQtyrb = self.wallet[t_asset_rb]
                    # else:
                    #     quantityrb = self.round_with_step_size(self.wallet[t_asset_rb], t_step_size1)
                    #     quoteOrderQtyrb = None
                    #
                    # # print("ROLL BACK NEED....", t_asset_rb + self.start_symbol,
                    # #       t_symbol1, inv_side, quantityrb, quoteOrderQtyrb)
                    # print('    Roll back: Symbol:', t_symbol1,
                    #       'Side:', inv_side,
                    #       'Qty:', quantityrb, quoteOrderQtyrb)
                    # try:
                    #     orderrb = self.bx_client.order_market(symbol=t_symbol1,
                    #                                           side=inv_side,
                    #                                           quantity=quantityrb,
                    #                                           quoteOrderQty=quoteOrderQtyrb)
                    #     # print(orderrb)
                    # except BinanceAPIException as e:
                    #     print(e.status_code)
                    #     print(e.message)
                    # self.roll_back = True
            else:
                self.trade_roll_back(sy1, t_symbol1, t_quote1, t_base1, t_side1, t_step_size1, order1)
                # self.save_deal_data("/".join([self.pai[sy1]["base"], self.pai[sy1]["base"]]), t_side1, saved_ob_price1,
                #                     "/".join([self.pai[sy2]["base"], self.pai[sy2]["base"]]), t_side2, saved_ob_price2,
                #                     "/".join([self.pai[sy3]["base"], self.pai[sy3]["base"]]), t_side3, saved_ob_price3,
                #                     profit, self.spread)

## END PRINT ----------------------------------------------------

            if t_side1 == "BUY" and saved_ob_price1 > 0:
                saved_ob_price_rec_1 = 1 / saved_ob_price1
            if t_side2 == "BUY" and saved_ob_price1 > 0:
                saved_ob_price_rec_2 = 1 / saved_ob_price2
            if t_side3 == "BUY" and saved_ob_price1 > 0:
                saved_ob_price_rec_3 = 1 / saved_ob_price3

            est_profit = saved_ob_price1 * saved_ob_price2 * saved_ob_price3
            print("")
            print("")

            print("  Calculated prices:             ",
                  '               {0:.8f}'.format(saved_ob_price_rec_1)[-18:],
                  '               {0:.8f}'.format(saved_ob_price_rec_2)[-18:],
                  '               {0:.8f}'.format(saved_ob_price_rec_3)[-18:])


            print('  Calculated Profit:       0.0%', '{0:.8f}   '.format(est_profit),
                  '0.05%', '{0:.8f}   '.format(est_profit - (3 * 0.0005)),
                  '0.075% ', '{0:.8f}   '.format(est_profit - (3 * 0.00075)))

            if not self.roll_back:

                self.set_status("deal")

                real_price_rec1 = real_price1 = self.get_fills_avg_price(order1)
                real_price_rec2 = real_price2 = self.get_fills_avg_price(order2)
                real_price_rec3 = real_price3 = self.get_fills_avg_price(order3)

                if t_side1 == "BUY":
                    real_price_rec1 = 1 / real_price1
                if t_side2 == "BUY":
                    real_price_rec2 = 1 / real_price2
                if t_side3 == "BUY":
                    real_price_rec3 = 1 / real_price3

                realised_profit = round(real_price_rec1 * real_price_rec2 * real_price_rec3, 8)

                print("")
                print("  Realised:              ",
                      '               {0:.8f}'.format(real_price1)[-18:],
                      '               {0:.8f}'.format(real_price2)[-18:],
                      '               {0:.8f}'.format(real_price3)[-18:])

                print('  Profit:                     0.0%', '{0:.8f}   '.format(realised_profit),
                      '0.05%', '{0:.8f}   '.format(realised_profit - (3 * 0.0005)),
                      '0.075% ', '{0:.8f}   '.format(realised_profit - (3 * 0.00075)))

                dif1 = saved_ob_price_rec_1 - real_price1 if t_side1 == "BUY" else real_price1 - saved_ob_price_rec_1
                dif2 = saved_ob_price_rec_2 - real_price2 if t_side2 == "BUY" else real_price2 - saved_ob_price_rec_2
                dif3 = saved_ob_price_rec_3 - real_price3 if t_side3 == "BUY" else real_price3 - saved_ob_price_rec_3

                print("")
                print("  Dif result - est      :",
                      '               {0:.8f}'.format(dif1)[-18:],
                      '               {0:.8f}'.format(dif2)[-18:],
                      '               {0:.8f}'.format(dif3)[-18:])

                print("  Tick size:             ",
                      '               {0:.8f}'.format(self.pai[sy1]['tick_size'])[-18:],
                      '               {0:.8f}'.format(self.pai[sy2]['tick_size'])[-18:],
                      '               {0:.8f}'.format(self.pai[sy3]['tick_size'])[-18:])

                print("  Side:               ",
                      "              " + t_side1,
                      "              " + t_side2,
                      "              " + t_side3)

                # self.save_deal_data("/".join([self.pai[sy1]["base"], self.pai[sy1]["base"]]), t_side1,
                #                     saved_ob_price1,
                #                     "/".join([self.pai[sy2]["base"], self.pai[sy2]["base"]]), t_side2,
                #                     saved_ob_price2,
                #                     "/".join([self.pai[sy3]["base"], self.pai[sy3]["base"]]), t_side3,
                #                     saved_ob_price3,
                #                     profit, self.spread)

            self.refresh_wallet()
            self.print_wallet()
        else:
            print("   Start order failed.")
            self.set_status("faile")
            self.print_deal_counter()

        self.set_trade_lock(1)
        # existing_shm = shared_memory.SharedMemory(name=self.shared_memory_name)
        # np_array = np.ndarray((1,), dtype=np.int64, buffer=existing_shm.buf)
        # lock.acquire()
        # np_array[0] = 1
        # lock.release()
        # existing_shm.close()
        self.last_arb = "".join([sy1, sy2, sy3])
        print("")

        # else:
        #     existing_shm.close()

    def market_test(self, isy, step, trade_qt_in=0):

        t_symbolx = self.pai[isy]['orig_symbol']
        t_sidex = self.pai[isy]['side']
        t_basex = self.pai[isy]['base']
        t_quotex = self.pai[isy]['quote']
        t_step_sizex = self.pai[isy]['stepsize']
        t_min_qtx = self.pai[isy]['minqty']
        t_ticksizex = self.pai[isy]['tick_size']
        t_minnotionalx = self.pai[isy]['minnotional']
        t_market_minqtyx = self.pai[isy]['market_minqty']

        t_inv_sidex = "SELL" if t_sidex == "BUY" else "BUY"


        if trade_qt_in == 0:
            trade_qt = self.round_with_step_size((t_minnotionalx * 1.1) * bid_ask_flow[isy], t_step_sizex)
        else:
            trade_qt = trade_qt_in



        print('t_symbolx, t_sidex, trade_qt, t_minnotional,t_market_minqty')
        print(t_symbolx,t_sidex,trade_qt,t_minnotionalx,t_market_minqtyx)

        start_time = datetime.now()
        if step == 1 or step == 3:
            try:
                orderx1 = self.bx_client.order_market(symbol=t_symbolx,
                                                      side=t_sidex,
                                                      quantity='{0:.8f}'.format(trade_qt))
                # print(orderx1)
                new_qty = round(float(orderx1['executedQty']), 8)
                print("realised price", self.get_fills_avg_price(orderx1))
            except BinanceAPIException as e:
                print(e.status_code)
                print(e.message)

        if step == 2 or step == 3:
            try:
                orderx2 = self.bx_client.order_market(symbol=t_symbolx,
                                                      side=t_inv_sidex,
                                                      quantity='{0:.8f}'.format(trade_qt_in))
                # print(orderx2)
                print("realised price", self.get_fills_avg_price(orderx2))
            except BinanceAPIException as e:
                print(e.status_code)
                print(e.message)
        print("speed:", datetime.now() - start_time)
        return trade_qt


    def save_deal_data(self,
                       sy1,side1,price1,
                       sy2,side2,price2,
                       sy3,side3,price3,
                       profit, spread):

        ideals = [sy1, side1, '{0:.8f}'.format(price1),
             sy2, side2, '{0:.8f}'.format(price2),
             sy3, side3, '{0:.8f}'.format(price3),
             '{0:.8f}'.format(profit), '{0:.8f}'.format(spread)]

        with open("deals_" + self.run_id+'.txt', 'a') as f:
            f.write(",".join(ideals)+"\n")


    def trade_roll_back(self, sy1, t_symbol1, t_quote1, t_base1, t_side1, t_step_size1, order1):
        # if sy1 == t_symbol1:
        #     t_asset_rb = t_quote1
        # else:
        #     t_asset_rb = t_base1

        # print(sy1, t_symbol1)

        inv_side = "SELL" if t_side1 == "BUY" else "BUY"

        if inv_side == "BUY":
            quantityrb = None
            quoteOrderQtyrb = self.wallet[t_quote1]
        else:
            quantityrb = self.round_with_step_size(self.wallet[t_base1], t_step_size1)
            quoteOrderQtyrb = None

        try:
            # print("symbol:", t_symbol1,
            #       "Side:", inv_side,
            #       "quantity:", quantityrb,
            #       "quoteOrderQty:", quoteOrderQtyrb)
            orderrb = self.bx_client.order_market(symbol=t_symbol1,
                                                  side=inv_side,
                                                  quantity=quantityrb,
                                                  quoteOrderQty=quoteOrderQtyrb)
            # print(orderrb)
        except BinanceAPIException as e:
            print(e.status_code)
            print(e.message)


        fillsx1 = self.get_fills_avg_price(order1)
        fillsxrb = self.get_fills_avg_price(orderrb)
        rb_profit = round((1 - (fillsx1 / fillsxrb)) * 100, 3) if inv_side else round((1 - (fillsxrb / fillsx1)) * 100, 3)
        print("   Rollback:", t_base1 + t_quote1, t_side1, fillsx1, " -> ", inv_side, fillsxrb, rb_profit, "%")
        self.roll_back = True

        self.set_status("roll")

    def set_status(self, what, value=0):
        # [deals, roll banck, last total, last total, failed start]
        deal_counter_existing_shm = shared_memory.SharedMemory(name=self.deal_counter_shared_memory_name)
        deal_counter_np_array = np.ndarray((5,), dtype=np.int64, buffer=deal_counter_existing_shm.buf)
        lock.acquire()
        if what == "deal":
            deal_counter_np_array[0] += 1
        elif what == "roll":
            deal_counter_np_array[1] += 1
        elif what == "total1":
            deal_counter_np_array[2] = value
        elif what == "total2":
            deal_counter_np_array[3] = value
        elif what == "faile":
            deal_counter_np_array[4] += 1
        lock.release()
        deal_counter_existing_shm.close()

    def get_status(self, what):
        # [deals, roll banck, last total, last total, failed start]
        deal_counter_existing_shm = shared_memory.SharedMemory(name=self.deal_counter_shared_memory_name)
        deal_counter_np_array = np.ndarray((5,), dtype=np.int64, buffer=deal_counter_existing_shm.buf)
        lock.acquire()
        if what == "deal":
            i_return = deal_counter_np_array[0]
        elif what == "roll":
            i_return = deal_counter_np_array[1]
        elif what == "total1":
            i_return = deal_counter_np_array[2]
        elif what == "total2":
            i_return = deal_counter_np_array[3]
        elif what == "faile":
            i_return = deal_counter_np_array[4]
        else:
            i_return = 0
        lock.release()
        deal_counter_existing_shm.close()
        return i_return

    def print_deal_counter(self):
        # [deals, roll banck, last total, last total, failed start]
        print("Closed trades:", self.get_status("deal"),
              "   Roll backs:", self.get_status("roll"),
              "   Failed:", self.get_status("faile"))

    def get_fills_avg_price(self, order_result):
        # print(order_result)
        fills = order_result['fills']
        total_qty = 0.0
        total_amount = 0.0
        for fs in fills:
            # self.commission[fs['commissionAsset']] += float(fs['commission'])
            total_qty += float(fs['qty'])
            total_amount += (float(fs['qty']) * float(fs['price']))
        return round(total_amount / total_qty, 8)

    def get_fills_avg_price_flow(self, order_result):
        # print(order_result)
        fills = order_result['fills']
        total_qty = 0.0
        total_amount = 0.0
        for fs in fills:
            # self.commission[fs['commissionAsset']] += float(fs['commission'])
            total_qty += float(fs['qty'])
            total_amount += (float(fs['qty']) * float(fs['price']))
        avg_price = round(total_amount / total_qty, 8)
        print(order_result)
        i_ret = 1/avg_price if order_result["side"] == 'BUY' else avg_price
        return i_ret


if __name__ == '__main__':
    
    a = np.array([1], dtype=np.int64)  # 1 = mehet a trade
    shm = shared_memory.SharedMemory(create=True, size=a.nbytes)
    np_array = np.ndarray(a.shape, dtype=np.int64, buffer=shm.buf)
    np_array[:] = a[:]  # Copy the original data into shared memory

    deal_counter = np.array([0, 0, 0, 0, 0], dtype=np.int64)
    # [deals, roll banck, last total, last total, failed start]
    deal_counter_shm = shared_memory.SharedMemory(create=True, size=deal_counter.nbytes)
    deal_counter_np_array = np.ndarray(deal_counter.shape, dtype=np.int64, buffer=deal_counter_shm.buf)
    deal_counter_np_array[:] = deal_counter[:]  # Copy the original data into shared memory

    used_cores = cpu_count()
    # used_cores = 1
    
    params = []
    for x in range(used_cores):
        params.append([used_cores, x + 1, shm.name, deal_counter_shm.name])
    n_arb = narbitrage_mp
    xpool = Pool(used_cores)
    res = xpool.map(n_arb, params)

    

