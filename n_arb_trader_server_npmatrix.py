# note for install external server
# sudo apt update
# sudo apt install python3 python3-pip
# pip install pandas
# pip install numpy
# pip install python-binance
# pip install networkx

import sys
import os
from decimal import *
import math
import numpy as np
# import pandas as pd
import random
import textwrap


# from functools import reduce
import time
from datetime import datetime

import asyncio
from threading import Thread

# Binanace
from binance import AsyncClient, BinanceSocketManager, Client
from binance.enums import *
import requests

ab1 = np.array([])
ab2 = np.array([])
ab3 = np.array([])
last_arb = ""
trade_in_progress = False
calculate_count = 0

class n_arbitrage:

    def __init__(self):

        self.lot_size = 90
        self.spread = 0.085  # % ezzel kalkulálom a megfelelő triangles-t d
        self.symbols_no = 1150  # over 1000 it is max
        self.orderbook_modifier = 0.032 / 100

        self.spread_mod_triangle = (1 - (self.spread / 100)) ** 3
        self.socket_thread = None

        self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
        self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
        self.b_client = None

        self.bx_client = Client(self.api_key, self.api_secret)

        self.account = self.get_account()
        self.exchange_info = self.get_exchange_info()


        # self.symbols = ['AGLD', 'STPT', 'MXN', 'UGX', 'RENBTC', 'GLM', 'RAY', 'NEAR', 'AUDIO', 'HNT', 'ADADOWN', 'CDT', 'SPARTA', 'SUSD', 'FARM', 'XNO', 'AION', 'NPXS', 'DGB', 'ZRX', 'BCD', 'EASY', 'SANTOS', 'WING', 'WNXM', 'BCH', 'JST', 'ADAUP', 'HOT', 'AR', 'IRIS', 'RAMP', 'BCX', 'SEK', 'TRIG', 'RCN', 'COVER', 'FLM', 'GNO', 'VITE', 'GNT', 'BKRW', 'CFX', 'XPR', 'SFP', 'DIA', 'RDN', 'ACA', 'ARDR', 'LOOMOLD', 'NEBL', 'ACH', 'SLPOLD', 'BEL', 'JUV', 'ACM', 'MINA', 'GRTDOWN', 'VTHO', 'PYROLD', 'SGB', 'SALT', 'STORM', 'REN', 'REP', 'ADA', 'ELF', 'REQ', 'STORJ', 'CHF', 'ADD', 'BZRX', 'SGT', 'DF', 'RARE', 'EOSDOWN', 'PAXG', 'YOYO', 'PAX', 'CHR', 'VND', 'BCHDOWN', 'WAVES', 'CHZ', 'ADX', 'XRP', 'WPR', 'JASMY', 'AED', 'FIDA', 'SAND', 'DKK', 'OCEAN', 'FOR', 'UMA', 'DREPOLD', 'SCRT', 'TUSD', 'EZ', 'TKO', 'WABI', 'RGT', 'IDRT', 'ENG', 'ENJ', 'UNIDOWN', 'YFII', 'KZT', 'OAX', 'GRT', 'GRS', 'UND', 'HARD', 'TFUEL', 'ENS', 'LEND', 'DLT', 'TROY', 'XLMUP', 'UNI', 'BTCDOWN', 'TLM', 'HUF', 'SBTC', 'CKB', 'WRX', 'XTZ', 'LUNA', 'ETHDOWN', 'AGI', 'BCHA', 'EON', 'EOP', 'EOS', 'GO', 'NCASH', 'RIF', 'NSBT', 'SKL', 'XDATA', 'GTC', 'PEN', 'BLINK', 'SOLO', 'SXPDOWN', 'HC', 'SKY', 'BURGER', 'NAS', 'NAV', 'GTO', 'WTC', 'XVG', 'EPS', 'DNT', 'CLV', 'FLOW', 'XTZDOWN', 'XVS', 'STEEM', 'BVND', 'SLP', 'VRT', 'NBS', 'DON', 'LAZIO', 'DOT', 'IQ', 'GRTUP', '1INCH', 'KNCL', 'CHESS', 'MITH', 'ERD', 'DEGO', 'CND', 'GYEN', 'UNFI', 'FTM', 'POWR', 'ERN', 'GVT', 'WINGS', 'FTT', 'VOXEL', 'PHA', 'RLC', 'PHB', 'TRXDOWN', 'ATOM', 'XRPUP', 'QUICK', 'BLZ', 'SNM', 'BOBA', 'MBL', 'MTLX', 'SNT', 'PHP', 'SNX', 'LTCDOWN', 'FUN', 'SNMOLD', 'COP', 'COS', 'API3', 'USD', 'QKC', 'SUSHIUP', 'ROSE', 'GLMR', 'XYM', 'PURSE', 'SOL', 'TRXUP', 'CITY', 'ETC', 'BNC', 'CELR', 'UST', 'OGN', 'ETH', 'NEO', 'TOMO', 'CELO', 'KLAY', 'AUCTION', 'BADGER', 'HIGH', 'GXS', 'TRB', 'BNT', 'QLC', 'LBA', 'MDA', 'BNX', 'UTK', 'WSOL', 'HEGIC', 'MA', 'AMB', 'MC', 'TRU', 'FUEL', 'DREP', 'TRY', 'TRX', 'MDT', 'NFT', 'MDX', 'XRPDOWN', 'AERGO', 'EUR', 'AMP', 'BOT', 'NULS', 'AUTO', 'NGN', 'ANC', 'BDOT', 'EGLD', 'ANTOLD', 'SPELL', 'PUNDIX', 'FXS', 'PLA', 'HNST', 'EVX', 'CRV', 'BAKE', 'ANT', 'NU', 'FLUX', 'ANY', 'LINKUP', 'SRM', 'QISWAP', 'TORN', 'PLN', 'QNT', 'ALICE', 'OG', 'MFT', 'OM', 'BTTOLD', 'BETH', 'BQX', 'WETH', 'PHBV1', 'BETA', 'BRD', 'SSV', 'BUSD', 'CTK', 'ARPA', 'DOTDOWN', 'BRL', 'ALCX', 'CTR', 'MATIC', 'IOTX', 'SHIB', 'TVK', 'FRONT', 'ZAR', 'DOCK', 'STX', 'PNT', 'QI', 'DENT', 'MBOX', 'SUB', 'POA', 'IOST', 'CAKE', 'ETHUP', 'POE', 'OMG', 'BAND', 'SUN', 'ASTR', 'SUNOLD', 'BTC', 'TWT', 'NKN', 'RSR', 'IOTA', 'CVC', 'REEF', 'BTG', 'MIR', 'KES', 'ARK', 'LOKA', 'CVP', 'ARN', 'KEY', 'BTS', 'SPARTAOLD', 'ARS', 'CVX', 'ONE', 'LINKDOWN', 'ONG', 'ANKR', 'SUSHI', 'ALGO', 'SC', 'WBTC', 'ONT', 'PPT', 'ONX', 'BTTC', 'RUB', 'PIVX', 'ASR', 'FIRO', 'AXSOLD', 'AST', 'MANA', 'DOTUP', 'ATA', 'MEETONE', 'QSP', 'ATD', 'NMR', 'MKR', 'DODO', 'LIT', 'ICP', 'ZEC', 'ATM', 'APPC', 'JEX', 'ICX', 'LOOM', 'ZEN', 'KP3R', 'DOGE', 'DUSK', 'ALPHA', 'BOLT', 'SXP', 'HBAR', 'RVN', 'MLN', 'AUD', 'LTOOLD', 'IDR', 'CTSI', 'KAVA', 'C98', 'PSG', 'HCC', 'VIDT', 'NOK', 'AVA', 'SYS', 'COCOS', 'STRAX', 'EOSUP', 'CZK', 'GAS', 'COVEROLD', 'AAVEDOWN', 'THETA', 'BCHUP', 'WAN', 'ORN', 'PERL', 'XLMDOWN', 'MASK', 'AAVE', 'GBP', 'PERP', '1INCHUP', 'SXPUP', 'YFIDOWN', 'BOND', 'YFI', 'PERLOLD', 'MOD', 'BICO', 'OST', 'XEC', 'YGG', 'PEOPLE', 'AXS', 'ZIL', 'VAI', 'XEM', 'CTXC', 'KEYFI', 'XTZUP', 'BIDR', 'BCHSV', 'AAVEUP', 'SUSHIDOWN', 'COMP', 'ETHBNT', 'OMOLD', 'OOKI', 'RUNE', 'FORTH', 'KMD', 'GHST', 'IDEX', 'DEXE', 'AVAX', 'UAH', 'KNC', 'PROS', 'PROM', 'BTCUP', 'CHAT', 'BGBP', 'LPT', 'HIVE', 'BIFI', 'PORTO', 'SNGLS', 'PYR', 'WAXP', 'DAI', 'YFIUP', 'DAR', 'FET', 'LRC', 'REPV1', 'ADXOLD', 'MTH', 'MTL', 'VET', 'ALPACA', 'USDT', 'USDS', 'OXT', 'USDP', 'DASH', 'NVT', 'SWRV', 'EDO', 'ILV', 'GHS', 'BTCST', 'HKD', 'JOE', 'LSK', 'KEEP', 'CAD', 'BEAM', 'CAN', 'DCR', 'CREAM', 'DATA', 'IMX', 'ENTRP', 'FILUP', 'UNIUP', 'LTC', 'USDC', 'WIN', 'LTCUP', 'INJ', 'TCT', 'PARA', 'LTO', 'VGX', 'TRIBE', 'NXS', 'EFI', 'DYDX', 'AGIX', 'INR', 'CBK', 'CBM', 'INS', 'POND', 'JPY', 'LINA', 'XLM', 'LINK', 'QTUM', 'FILDOWN', 'SUPER', 'UFT', 'POLS', 'KSM', 'LUN', 'FIL', 'POLY', 'STMX', 'RNDR', 'BAL', 'FIO', 'GALA', 'VIB', 'VIA', 'FIS', 'BAR', 'RAD', 'BAT', 'VRAB', 'AKRO', 'NZD', 'MOVR', 'XMR', '1INCHDOWN', 'COTI']

        # self.symbols = ['USDT', 'BTC', 'ETH', 'ADA', 'LINK', 'DOT', 'TRX', 'FTM', 'SOL',
        #                  'MATIC', 'ETC', 'NEO', 'ENJ', 'WAVES', 'ATOM', 'ONE', 'ZEC',
        #                 'ONT', 'HOT', 'CHZ', 'WIN', 'AXS', 'GALA', 'ANKR', 'RUNE', 'ICP', 'LRC',
        #                 'ZIL', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS',
        #                 'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'ADX', 'NULS',
        #                 'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC',
        #                 'IOST', 'NANO', 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN',
        #                 'USDC', 'BCHSV', 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI',
        #                 'SRM', 'KSM', 'SUSHI', 'BEL', 'NEAR', 'SLP', 'C98', 'MINA', 'VOXEL']

        # self.symbols = ['USDT', 'BTC', 'ETH', 'ADA', 'LINK', 'DOT', 'TRX', 'FTM', 'SOL',
        #                  'MATIC', 'ETC', 'NEO', 'ENJ', 'WAVES', 'ATOM', 'ONE', 'ZEC',
        #                 'ONT', 'HOT', 'CHZ', 'WIN', 'AXS', 'GALA', 'ANKR', 'RUNE', 'ICP', 'LRC',
        #                 'ZIL', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS',
        #                 'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'ADX', 'NULS',
        #                 'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC',
        #                 'IOST', 'NANO', 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN',
        #                 'USDC', 'BCHSV', 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI',
        #                 'SRM', 'KSM', 'SUSHI', 'NEAR', 'SLP', 'C98', 'MINA', 'VOXEL']
        
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
        
        self.off_symbols = ['BIDR', 'BUSD', 'TRY']
        for osy in self.off_symbols:
            self.symbols.remove(osy)

        self.all_pairs = self.defa_all_pairs()

        self.selected_symbols = self.symbols[:self.symbols_no]  ## kiválasztam amivel dolgozok

        self.start_symbols = ['USDT']
        # self.commission = self.defa_commission()
        self.selected_pairs = self.defa_selected_pairs()  ##a kiválasztott szimbólumokhoz kapcsolódó párokat kiválasztom


        # convert_multipyert azért használom, hogy egy adott arbirage profitját gyorsan tudjam kiszámolni
        # ne a pandasban kelljen keresgetni mert az nagyon lassú
        self.price = self.defa_price_dict()

        self.socket_list = self.defa_socket_list()
        self.pair_info = self.defa_pair_info()

        # az a számlám miből mennyi van, azért hívom becsült mnnyiségnek mert
        # kötés közben nincs idő lekérdezni a számlát ezért csak megsaccolom azt
        self.estimated_amount = {}

        self.refresh_map = None
        self.maxi_pairs = None
        self.arb_matrix()
        self.run_analys = False
        self.last_arb = ""

    def arb_matrix(self):
        global ab1, ab2, ab3
        print("Create arb martix")

        maxi_tri = np.chararray((0, 4), itemsize=10)
        # print(maxi_tri)

        start_symbol = self.start_symbols[0]
        nrow = np.chararray((1, 4), itemsize=10)
        for s1 in self.selected_symbols:
            nrow[0][0] = start_symbol
            nrow[0][3] = start_symbol
            for s2 in self.selected_symbols:
                nrow[0][1] = s1
                nrow[0][2] = s2
                maxi_tri = np.vstack([maxi_tri, nrow])

        # kiszedem az egymás mellet ugyan olyanokat
        while True:
            del_row_id = -1
            for row_x in range(maxi_tri.shape[0]):
                if maxi_tri[row_x][0] == maxi_tri[row_x][1] \
                        or maxi_tri[row_x][1] == maxi_tri[row_x][2] \
                        or maxi_tri[row_x][2] == maxi_tri[row_x][3]:
                    del_row_id = row_x
                    break
            if del_row_id == -1:
                break
            else:
                maxi_tri = np.delete(maxi_tri, del_row_id, 0)

        # felépítem a párokat
        self.maxi_pairs = np.chararray((0, 3), itemsize=20)
        prow = np.chararray((1, 3), itemsize=20)
        for i in range(maxi_tri.shape[0]):
            prow[0][0] = maxi_tri[i][0] + maxi_tri[i][1]
            prow[0][1] = maxi_tri[i][1] + maxi_tri[i][2]
            prow[0][2] = maxi_tri[i][2] + maxi_tri[i][3]
            self.maxi_pairs = np.vstack([self.maxi_pairs, prow])

        all_pairs_way = []
        for sp in self.selected_pairs:
            all_pairs_way.append("".join([self.selected_pairs[sp][0], self.selected_pairs[sp][1]]))
            all_pairs_way.append("".join([self.selected_pairs[sp][1], self.selected_pairs[sp][0]]))

        ## kitörlöm azokat a kombinációkat amelyek nem is léteznek
        del_index = []
        for row_x in range(self.maxi_pairs.shape[0]):
            for col_x in range(3):
                if self.maxi_pairs[row_x][col_x].decode('UTF-8') not in all_pairs_way:
                    del_index.append(row_x)
        self.maxi_pairs = np.delete(self.maxi_pairs, del_index, 0)

        trade_pairs = self.maxi_pairs
        trade_side = self.maxi_pairs

        # Felépítem a frissítési mapot
        self.refresh_map = {}
        for apw in all_pairs_way:
            self.refresh_map[apw] = [[], [], [], []]
        for apw in all_pairs_way:
            for apw_col in range(3):
                col_map = []
                for apw_row in range(self.maxi_pairs.shape[0]):
                    if self.maxi_pairs[apw_row][apw_col].decode('UTF-8') == apw:
                        col_map.append(apw_row)
                self.refresh_map[apw][apw_col] = col_map.copy()

        ## kitörlöm azokat a párokat amik sehol nem lettek felhasználba
        ## vektorok szorzásánál ezeket felesleges szorozgatni
        del_dic = []
        for pm in self.refresh_map:
            if 0 == len(self.refresh_map[pm][0]) + len(self.refresh_map[pm][1]) + len(self.refresh_map[pm][2]):
                del_dic.append(pm)

        if del_dic:
            print("ezeket sehová nem tudom bekombinálni")
            print(del_dic)

        ab1 = np.full(self.maxi_pairs.shape[0], 0.00000000, dtype=float)
        ab2 = np.full(self.maxi_pairs.shape[0], 0.00000000, dtype=float)
        ab3 = np.full(self.maxi_pairs.shape[0], 0.00000000, dtype=float)


        print("Created arb martix shape:", self.maxi_pairs.shape)

    # def get_historical_klines_1W(self, symbol):
    #     loop = asyncio.get_event_loop()
    #     return loop.run_until_complete(self._get_historical_klines_1W(symbol))
    #
    # # def get_triangle_profit_mod_spread(self, arb):
    # #     return self.spread_mod_triangle * self.price[arb[0] + arb[1]] * self.price[arb[1] + arb[2]] * self.price[arb[2] + arb[3]]
    #
    # # def get_triangle_profit_mod_fee(self, arb):
    # #     return self.official_fee_mod_triangle * self.price[arb[0] + arb[1]] * self.price[arb[1] + arb[2]] * self.price[arb[2] + arb[3]]

    def find_pair(self, what):
        what = str(what)
        for i1 in range(self.maxi_pairs.shape[0]):
            for i2 in range(self.maxi_pairs.shape[1]):
                if self.maxi_pairs[i1][i2].decode('UTF-8') == what:
                    return True
        return False

    async def open_binance_client(self):
        self.b_client = await AsyncClient.create(self.api_key, self.api_secret)

    async def close_binance_client(self):
        await self.b_client.close_connection()

    async def _get_account(self):
        await self.open_binance_client()
        res = await self.b_client.get_account()
        await self.close_binance_client()
        return res

    def get_account(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._get_account())

    def get_BNB(self):
        res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BNBUSDT")
        return float(res.json()["price"])

    async def _get_exchange_info(self):
        await self.open_binance_client()
        res = await self.b_client.get_exchange_info()
        await self.close_binance_client()
        return res

    def get_exchange_info(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._get_exchange_info())

    def get_symbol_info2(self, symbol):
        for item in self.exchange_info['symbols']:
            if item['symbol'] == symbol.upper():
                return item
        return None

    def get_amount_by_symbol(self, symbol):
        for i_i in self.account['balances']:
            if i_i['asset'] == symbol:
                return float(i_i['free'])
        return 0.0

    def print_estimated_amount(self):
        self.account = self.bx_client.get_account()
        self.estimated_amount = self.get_estimated_amount()
        print("Spot wallet:")
        total_in_USDT = 0
        self.price["BNBUSDT"] = self.get_BNB()
        for ea in self.estimated_amount:
            if self.estimated_amount[ea] != 0:
                symbol_value_in_USDT = round(self.estimated_amount[ea] * self.price[ea + "USDT"], 8)
                total_in_USDT += symbol_value_in_USDT
                eap = ea + "     "
                print(" ", eap[0:5],
                      '{0:.8f}'.format(self.estimated_amount[ea]),
                      '{0:.1f}'.format(symbol_value_in_USDT))
            
        print("Total(USDT):", '{0:.2f}'.format(total_in_USDT))

    def get_estimated_amount(self):
        estimated_amount = {}
        for sesy in self.selected_symbols:
            estimated_amount[sesy] = self.get_amount_by_symbol(sesy)
        estimated_amount["BNB"] = self.get_amount_by_symbol("BNB")
        return estimated_amount

    def defa_pair_info(self):
        pair_info = {}
        for si1 in self.selected_symbols:
            for si2 in self.selected_symbols:
                if si1 + si2 in self.all_pairs:
                    filters = self.get_symbol_info2(si1 + si2)['filters'][2]
                    base = self.get_symbol_info2(si1 + si2)['baseAsset']
                    quot = self.get_symbol_info2(si1 + si2)['quoteAsset']
                    step_size = float(filters['stepSize'])
                    # print(self.get_symbol_info2(si1 + si2)['symbol'], step_size)
                    min_qty = float(filters['minQty'])
                    pair_info[si1 + si2] = [si1 + si2, "SELL", step_size, min_qty, base, quot]
                    pair_info[si2 + si1] = [si1 + si2, "BUY", step_size, min_qty, base, quot]
                    # ez egy miről mire megyek katalógus
        return pair_info

    def defa_all_pairs(self):
        i_all_pairs = []
        for sy in self.exchange_info['symbols']:
            i_all_pairs.append(sy['symbol'])
        return i_all_pairs

    def defa_socket_list(self):
        i_socket_list = []
        for sp in tuple(self.selected_pairs.keys()):
            i_socket_list.append(sp.lower() + '@bookTicker')
        return i_socket_list

    def defa_price_dict(self):
        i_selected_pairs = {}
        for si1 in self.selected_symbols:
            for si2 in self.selected_symbols:
                i_selected_pairs["".join([si1, si2])] = 1.0
                i_selected_pairs["".join([si2, si1])] = 1.0
        return i_selected_pairs

    def defa_selected_pairs(self):
        i_selected_pairs = {}
        for si1 in self.selected_symbols:
            for si2 in self.selected_symbols:
                if si1 + si2 in self.all_pairs and \
                        self.get_symbol_info2(si1 + si2)['status'] == 'TRADING' and \
                        "MARKET" in self.get_symbol_info2(si1 + si2)['orderTypes']:

                    # print(si1 + si2)
                    i_selected_pairs[si1 + si2] = [si1, si2]
        return i_selected_pairs

    def round_qty_with_step_size(slef, quantity, step_size, reduce=0):
        reduce = Decimal(reduce * step_size)  # ennyi darabbal visszaveszi
        quantity = Decimal(str(quantity))
        return round(float(quantity - quantity % Decimal(str(step_size)) - reduce), 8)

    def start(self):
        self.socket_thread = Thread(target=self.start_asyc_websocket, daemon=True)
        self.socket_thread.start()

    async def asyc_websocket(self):
        global ab1, ab2, ab3, last_arb, trade_in_progress, calculate_count
        client = await AsyncClient.create()
        bm = BinanceSocketManager(client)

        # start any sockets here, i.e a trade socket
        # ts = bm.trade_socket('BNBBTC')
        
        i_socket_list = []
        for sp in tuple(self.selected_pairs.keys()):
            if self.find_pair(sp):
                i_socket_list.append(sp.lower() + '@bookTicker')

        print('Number of sockets:', len(i_socket_list))
        ts = bm.multiplex_socket(i_socket_list)
        # then start receiving messages
        async with ts as tscm:
            while True:
                res = await tscm.recv()
                # print(res)

                # szét kell választani szimbolumokra, mivel nem egyen hosszúságú ezért használok
                # dictionariket
                # print(res)
                s1 = self.selected_pairs[res['data']['s']][0]
                s2 = self.selected_pairs[res['data']['s']][1]
                # print(res['data']['s'], s1, s2)
                s1s2 = "".join([s1, s2])
                s2s1 = "".join([s2, s1])

                bid = round(float(res['data']['b']), 8)
                bid_mod = round(bid * (1 - self.orderbook_modifier), 8)
                ask = round(float(res['data']['a']), 8)
                ask_mod = round(ask * (1 + self.orderbook_modifier), 8)
                ask_rec = round(1 / ask_mod, 8)

                self.price[s1 + s2] = bid
                self.price[s2 + s1] = ask

                np.put(ab1, self.refresh_map[s1s2][0], bid_mod)
                np.put(ab2, self.refresh_map[s1s2][1], bid_mod)
                np.put(ab3, self.refresh_map[s1s2][2], bid_mod)

                np.put(ab1, self.refresh_map[s2s1][0], ask_rec)
                np.put(ab2, self.refresh_map[s2s1][1], ask_rec)
                np.put(ab3, self.refresh_map[s2s1][2], ask_rec)

                # if self.run_analys:
                #
                #     profit_array = np.multiply(np.multiply(ab1, ab2), ab3)
                #     calculate_count += 1
                #
                #     max_row = np.argmax(profit_array)
                #     profit = ab1[max_row] * ab2[max_row] * ab3[max_row] * self.spread_mod_triangle
                #     if calculate_count % 10000 == 0:
                #         print("Calculated arb: ", calculate_count, profit)
                #
                #     arb_str = str([self.maxi_pairs[max_row][0].decode('UTF-8'),
                #                     self.maxi_pairs[max_row][1].decode('UTF-8'),
                #                     self.maxi_pairs[max_row][2].decode('UTF-8')])
                #
                #     if profit > 1 and not trade_in_progress and last_arb != arb_str:
                #         trade_in_progress = True
                #         last_arb = arb_str
                #
                #         # sp1 egyenes
                #         # spmx ha kell reciprok
                #         sp1 = self.price[self.maxi_pairs[max_row][0].decode('UTF-8')]
                #         spmx1 = ab1[max_row]
                #         sp2 = self.price[self.maxi_pairs[max_row][1].decode('UTF-8')]
                #         spmx2 = ab2[max_row]
                #         sp2 = self.price[self.maxi_pairs[max_row][2].decode('UTF-8')]
                #         spmx3 = ab3[max_row]
                #
                #         print(self.maxi_pairs[max_row][0].decode('UTF-8'),
                #               self.maxi_pairs[max_row][1].decode('UTF-8'),
                #               self.maxi_pairs[max_row][2].decode('UTF-8'), profit)
                #
                #         print("Orderbook prices:     ",
                #               '{0:.8f}'.format(ab1[max_row]),
                #               '{0:.8f}'.format(ab2[max_row]),
                #               '{0:.8f}'.format(ab3[max_row]))
                #
                #         print("Orderbook prices (1/):",
                #               '{0:.8f}'.format(1 / ab1[max_row]),
                #               '{0:.8f}'.format(1 / ab2[max_row]),
                #               '{0:.8f}'.format(1 / ab3[max_row]))
                #
                #         t_amount1 = self.lot_size
                #         sy = self.maxi_pairs[max_row][0].decode('UTF-8')
                #         t_side1 = self.pair_info[sy][1]
                #         t_symbol1 = self.pair_info[sy][0]
                #         t_step_size1 = self.pair_info[sy][2]
                #         t_amount_mod_1 = self.round_qty_with_step_size(t_amount1 * spmx1, t_step_size1, 1)
                #         t_price1 = sp1
                #         print(t_price1)
                #         order1 = self.bx_client.order_limit(symbol=t_symbol1,
                #                                             price=t_price1,
                #                                             side=SIDE_BUY,
                #                                             quantity=t_amount_mod_1,
                #                                             timeInForce=TIME_IN_FORCE_FOK)
                #         print(order1)
                #         if order1['status'] == ORDER_STATUS_FILLED:
                #             executedQty_1 = round(float(order1['executedQty']), 8)
                #             # cummulativeQuoteQty_1 = round(float(order1['cummulativeQuoteQty']), 8)
                #             t_amount2 = executedQty_1 ## ez mindig BUY         if t_side1 == "BUY" else cummulativeQuoteQty_1
                #             # print(t_amount2)
                #             sy = self.maxi_pairs[max_row][1].decode('UTF-8')
                #             t_side2 = self.pair_info[sy][1]
                #             t_symbol2 = self.pair_info[sy][0]
                #             t_step_size2 = self.pair_info[sy][2]
                #             t_min_qt2 = self.pair_info[sy][3]
                #             # t_amount2 = self.round_qty_with_step_size(t_amount2, t_step_size2) if t_side2 == "SELL" else t_amount2
                #             t_price2 = sp2
                #             t_amount_mod_2 = self.round_qty_with_step_size(t_amount2 * spmx2, t_step_size2, 1) if t_side1 == "BUY" else t_amount2
                #             # print("Side,amount, minqt", t_side2, t_amount2, t_min_qt2, t_step_size2)
                #             order2 = self.bx_client.order_limit(symbol=t_symbol2,
                #                                                 side=SIDE_BUY if t_side2 == "BUY" else SIDE_SELL,
                #                                                 price=t_price2,
                #                                                 quantity=t_amount_mod_2,
                #                                                 timeInForce=TIME_IN_FORCE_IOC)
                #             # print(order2)
                #             if order2['status'] == ORDER_STATUS_FILLED:
                #                 executedQty_2 = round(float(order2['executedQty']), 8)
                #                 cummulativeQuoteQty_2 = round(float(order2['cummulativeQuoteQty']), 8)
                #                 t_amount3 = executedQty_2 if t_side2 == "BUY" else cummulativeQuoteQty_2
                #
                #                 # print(t_amount3)
                #                 sy = self.maxi_pairs[max_row][2].decode('UTF-8')
                #                 t_side3 = self.pair_info[sy][1]
                #                 t_symbol3 = self.pair_info[sy][0]
                #                 t_step_size3 = self.pair_info[sy][2]
                #                 t_min_qt3 = self.pair_info[sy][3]
                #                 t_amount3 = self.round_qty_with_step_size(t_amount3, t_step_size3) if t_side3 == "SELL" else t_amount3
                #                 # print("Side,amount, minqt", t_side3, t_amount3, t_min_qt3, t_step_size3)
                #
                #                 order3 = self.bx_client.order_market(symbol=t_symbol3,
                #                                                side=SIDE_BUY if t_side3 == "BUY" else SIDE_SELL,
                #                                            quantity=None if t_side3 == "BUY" else t_amount3,
                #                                            quoteOrderQty=t_amount3 if t_side3 == "BUY" else None)
                #                 # print(order3)
                #
                #                 p1 = round(float(order1['fills'][0]['price']),8)
                #                 p2 = round(float(order2['fills'][0]['price']),8)
                #                 p3 = round(float(order3['fills'][0]['price']),8)
                #
                #                 if t_side1 == "BUY":
                #                     p1 = 1 / p1
                #                 if t_side2 == "BUY":
                #                     p2 = 1 / p2
                #                 if t_side3 == "BUY":
                #                     p3 = 1 / p3
                #
                #                 print("Traded prices:   ", p1, p2, p3, round(p1 * p2 * p3, 8))
                #
                #                 self.print_estimated_amount()
                #                 time.sleep(8)
                #             else:
                #                 print("Start price failed at 2nd. order")
                #         else:
                #             print("Start price failed at 1st order")
                #         trade_in_progress = False

                if self.run_analys:

                    profit_array = np.multiply(np.multiply(ab1, ab2), ab3)
                    max_row = np.argmax(profit_array)
                    profit = ab1[max_row] * ab2[max_row] * ab3[max_row] * self.spread_mod_triangle
                    calculate_count += 1

                    if calculate_count % 25000 == 0:
                        print("Calculated arb: ", calculate_count, profit, 'trade_in_progress', trade_in_progress)

                    arb_str = str([self.maxi_pairs[max_row][0].decode('UTF-8'),
                                    self.maxi_pairs[max_row][1].decode('UTF-8'),
                                    self.maxi_pairs[max_row][2].decode('UTF-8')])

                    if profit > 1 and not trade_in_progress and last_arb != arb_str:
                    # if profit > 1 and not trade_in_progress:
                        trade_in_progress = True
                        last_arb = arb_str

                        print(self.maxi_pairs[max_row][0].decode('UTF-8'),
                              self.maxi_pairs[max_row][1].decode('UTF-8'),
                              self.maxi_pairs[max_row][2].decode('UTF-8'), profit)

                        # sp1 egyenes
                        # spmx ha kell reciprok
                        sp1 = self.price[self.maxi_pairs[max_row][0].decode('UTF-8')]
                        sp2 = self.price[self.maxi_pairs[max_row][1].decode('UTF-8')]
                        sp3 = self.price[self.maxi_pairs[max_row][2].decode('UTF-8')]
                        
                        print("Orderbook prices:     ",
                              '{0:.8f}'.format(sp1),
                              '{0:.8f}'.format(sp2),
                              '{0:.8f}'.format(sp3))

                        print("Orderbook prices (1/):",
                              '{0:.8f}'.format(1 / sp1),
                              '{0:.8f}'.format(1 / sp2),
                              '{0:.8f}'.format(1 / sp3))

                        t_amount1 = self.lot_size
                        sy = self.maxi_pairs[max_row][0].decode('UTF-8')
                        t_side1 = self.pair_info[sy][1]
                        t_symbol1 = self.pair_info[sy][0]
                        t_step_size1 = self.pair_info[sy][2]
                        t_min_qt1 = self.pair_info[sy][3]
                        t_amount_mod_1 = self.round_qty_with_step_size(t_amount1 * 1 / self.price[sy], t_step_size1, 1)
                        t_price1 = sp1
                        print('1 symbol', t_symbol1, 'price', t_price1, 'side', SIDE_BUY, 'quantity', t_amount_mod_1, 'minqt', t_min_qt1 )
                        order1 = self.bx_client.order_limit(symbol=t_symbol1,
                                                            price=t_price1,
                                                            side=SIDE_BUY,
                                                            quantity=t_amount_mod_1,
                                                            timeInForce=TIME_IN_FORCE_IOC)
                        print(order1)
                        if order1['status'] != 'EXPIRED':
                            executedQty_1 = round(float(order1['executedQty']), 8)
                            # cummulativeQuoteQty_1 = round(float(order1['cummulativeQuoteQty']), 8)
                            t_amount2 = executedQty_1  # első csak buy lehet if t_side1 == "BUY" else cummulativeQuoteQty_1
                            sy = self.maxi_pairs[max_row][1].decode('UTF-8')
                            t_side2 = self.pair_info[sy][1]
                            t_symbol2 = self.pair_info[sy][0]
                            t_step_size2 = self.pair_info[sy][2]
                            t_min_qt2 = self.pair_info[sy][3]
                            t_amount2 = self.round_qty_with_step_size(t_amount2, t_step_size2) if t_side2 == "SELL" else t_amount2
                            print('2 symbol', t_symbol2, 'side', t_side2, 'quantity', t_amount2, 'minqt', t_min_qt2)
                            order2 = self.bx_client.order_market(symbol=t_symbol2,
                                                           side=SIDE_BUY if t_side2 == "BUY" else SIDE_SELL,
                                                           quantity=None if t_side2 == "BUY" else t_amount2,
                                                           quoteOrderQty=t_amount2 if t_side2 == "BUY" else None)
                            executedQty_2 = round(float(order2['executedQty']), 8)
                            cummulativeQuoteQty_2 = round(float(order2['cummulativeQuoteQty']), 8)
                            t_amount3 = executedQty_2 if t_side2 == "BUY" else cummulativeQuoteQty_2

                            sy = self.maxi_pairs[max_row][2].decode('UTF-8')
                            t_side3 = self.pair_info[sy][1]
                            t_symbol3 = self.pair_info[sy][0]
                            t_step_size3 = self.pair_info[sy][2]
                            t_min_qt3 = self.pair_info[sy][3]
                            t_amount3 = self.round_qty_with_step_size(t_amount3, t_step_size3) if t_side3 == "SELL" else t_amount3
                            print('3 symbol', t_symbol3, 'side', t_side3, 'quantity', t_amount3, 'minqt', t_min_qt3)
                            order3 = self.bx_client.order_market(symbol=t_symbol3,
                                                           side=SIDE_BUY if t_side3 == "BUY" else SIDE_SELL,
                                                       quantity=None if t_side3 == "BUY" else t_amount3,
                                                       quoteOrderQty=t_amount3 if t_side3 == "BUY" else None)
                            print(order3)

                            p1 = round(float(order1['fills'][0]['price']), 8)
                            p2 = round(float(order2['fills'][0]['price']), 8)
                            p3 = round(float(order3['fills'][0]['price']), 8)

                            if t_side1 == "BUY":
                                p1 = 1 / p1
                            if t_side2 == "BUY":
                                p2 = 1 / p2
                            if t_side3 == "BUY":
                                p3 = 1 / p3

                            print("Traded prices:   ", p1, p2, p3, round(p1 * p2 * p3, 8))

                            self.print_estimated_amount()
                            time.sleep(20)
                        else:
                            print("Start price failed:")
                            # trade_in_progress = False
                        trade_in_progress = False

        await client.close_connection()

    def start_asyc_websocket(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(self.asyc_websocket())
        loop.close()

if __name__ == '__main__':
    n_arb = n_arbitrage()
    n_arb.start()
    init_time = 8
    print("Number of pairs:", len(n_arb.selected_pairs))
    print("Spread:", n_arb.spread, "%")
    print("Start symbol:", n_arb.start_symbols)
    print("Lot size:", n_arb.lot_size)
    n_arb.print_estimated_amount()
    n_arb.run_analys = True
    while True:
        time.sleep(20)


