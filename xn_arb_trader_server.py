# note for install external server
# sudo apt update
# sudo apt install python3 python3-pip
# pip install pandas
# pip install numpy
# pip install python-binance
# pip install networkx

import sys
from decimal import *
import math
import numpy as np
import pandas as pd
import textwrap

from collections import defaultdict
import networkx as nx

# from functools import reduce
import time
from datetime import datetime

import asyncio
from threading import Thread

# Binanace
from binance import AsyncClient, BinanceSocketManager, Client
from binance.helpers import round_step_size
import requests


# nDot
# from n_riport import n_riport
class n_arbitrage:

    def __init__(self):

        self.real_trade = True
        self.official_fee = 0.075  # %   ezzel számolom ki a profitot
        self.spread = 0.06  # % ezzel kalkulálom a megfelelő triangles-t

        self.max_cicle_lengt = 3  # maximum ennyi kriptóbol állhat a triangle

        self.spread_mod_triangle = (1 - (self.spread / 100)) ** self.max_cicle_lengt
        self.official_fee_mod_triangle = (1 - (self.official_fee / 100)) ** self.max_cicle_lengt

        self.spread_mod = (1 - (self.spread / 100))
        self.official_fee_mod = (1 - (self.official_fee / 100))

        self.arb_check_delay = 0.25  # arbitrás kereéséek közötti várakozáa 0.5 = 2xmásodpercenként
        # self.riport = n_riport()
        # self.stop_tradeing_at_USDT = 60

        self.socket_thread1 = None
        self.socket_thread2 = None
        self.socket_thread3 = None
        self.socket_thread4 = None

        # self.stop_thread = None

        self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
        self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
        self.b_client = None

        self.bx_client = Client(self.api_key, self.api_secret)

        self.account = self.get_account()
        self.exchange_info = self.get_exchange_info()


        # self.symbols = ['AGLD', 'STPT', 'MXN', 'UGX', 'RENBTC', 'GLM', 'RAY', 'NEAR', 'AUDIO', 'HNT', 'ADADOWN', 'CDT', 'SPARTA', 'SUSD', 'FARM', 'XNO', 'AION', 'NPXS', 'DGB', 'ZRX', 'BCD', 'EASY', 'SANTOS', 'WING', 'WNXM', 'BCH', 'JST', 'ADAUP', 'HOT', 'AR', 'IRIS', 'RAMP', 'BCX', 'SEK', 'TRIG', 'RCN', 'COVER', 'FLM', 'GNO', 'VITE', 'GNT', 'BKRW', 'CFX', 'XPR', 'SFP', 'DIA', 'RDN', 'ACA', 'ARDR', 'LOOMOLD', 'NEBL', 'ACH', 'SLPOLD', 'BEL', 'JUV', 'ACM', 'MINA', 'GRTDOWN', 'VTHO', 'PYROLD', 'SGB', 'SALT', 'STORM', 'REN', 'REP', 'ADA', 'ELF', 'REQ', 'STORJ', 'CHF', 'ADD', 'BZRX', 'SGT', 'DF', 'RARE', 'EOSDOWN', 'PAXG', 'YOYO', 'PAX', 'CHR', 'VND', 'BCHDOWN', 'WAVES', 'CHZ', 'ADX', 'XRP', 'WPR', 'JASMY', 'AED', 'FIDA', 'SAND', 'DKK', 'OCEAN', 'FOR', 'UMA', 'DREPOLD', 'SCRT', 'TUSD', 'EZ', 'TKO', 'WABI', 'RGT', 'IDRT', 'ENG', 'ENJ', 'UNIDOWN', 'YFII', 'KZT', 'OAX', 'GRT', 'GRS', 'UND', 'HARD', 'TFUEL', 'ENS', 'LEND', 'DLT', 'TROY', 'XLMUP', 'UNI', 'BTCDOWN', 'TLM', 'HUF', 'SBTC', 'CKB', 'WRX', 'XTZ', 'LUNA', 'ETHDOWN', 'AGI', 'BCHA', 'EON', 'EOP', 'EOS', 'GO', 'NCASH', 'RIF', 'NSBT', 'SKL', 'XDATA', 'GTC', 'PEN', 'BLINK', 'SOLO', 'SXPDOWN', 'HC', 'SKY', 'BURGER', 'NAS', 'NAV', 'GTO', 'WTC', 'XVG', 'EPS', 'DNT', 'CLV', 'FLOW', 'XTZDOWN', 'XVS', 'STEEM', 'BVND', 'SLP', 'VRT', 'NBS', 'DON', 'LAZIO', 'DOT', 'IQ', 'GRTUP', '1INCH', 'KNCL', 'CHESS', 'MITH', 'ERD', 'DEGO', 'CND', 'GYEN', 'UNFI', 'FTM', 'POWR', 'ERN', 'GVT', 'WINGS', 'FTT', 'VOXEL', 'PHA', 'RLC', 'PHB', 'TRXDOWN', 'ATOM', 'XRPUP', 'QUICK', 'BLZ', 'SNM', 'BOBA', 'MBL', 'MTLX', 'SNT', 'PHP', 'SNX', 'LTCDOWN', 'FUN', 'SNMOLD', 'COP', 'COS', 'API3', 'USD', 'QKC', 'SUSHIUP', 'ROSE', 'GLMR', 'XYM', 'PURSE', 'SOL', 'TRXUP', 'CITY', 'ETC', 'BNC', 'CELR', 'UST', 'OGN', 'ETH', 'NEO', 'TOMO', 'CELO', 'KLAY', 'AUCTION', 'BADGER', 'HIGH', 'GXS', 'TRB', 'BNT', 'QLC', 'LBA', 'MDA', 'BNX', 'UTK', 'WSOL', 'HEGIC', 'MA', 'AMB', 'MC', 'TRU', 'FUEL', 'DREP', 'TRY', 'TRX', 'MDT', 'NFT', 'MDX', 'XRPDOWN', 'AERGO', 'EUR', 'AMP', 'BOT', 'NULS', 'AUTO', 'NGN', 'ANC', 'BDOT', 'EGLD', 'ANTOLD', 'SPELL', 'PUNDIX', 'FXS', 'PLA', 'HNST', 'EVX', 'CRV', 'BAKE', 'ANT', 'NU', 'FLUX', 'ANY', 'LINKUP', 'SRM', 'QISWAP', 'TORN', 'PLN', 'QNT', 'ALICE', 'OG', 'MFT', 'OM', 'BTTOLD', 'BETH', 'BQX', 'WETH', 'PHBV1', 'BETA', 'BRD', 'SSV', 'BUSD', 'CTK', 'ARPA', 'DOTDOWN', 'BRL', 'ALCX', 'CTR', 'MATIC', 'IOTX', 'SHIB', 'TVK', 'FRONT', 'ZAR', 'DOCK', 'STX', 'PNT', 'QI', 'DENT', 'MBOX', 'SUB', 'POA', 'IOST', 'CAKE', 'ETHUP', 'POE', 'OMG', 'BAND', 'SUN', 'ASTR', 'SUNOLD', 'BTC', 'TWT', 'NKN', 'RSR', 'IOTA', 'CVC', 'REEF', 'BTG', 'MIR', 'KES', 'ARK', 'LOKA', 'CVP', 'ARN', 'KEY', 'BTS', 'SPARTAOLD', 'ARS', 'CVX', 'ONE', 'LINKDOWN', 'ONG', 'ANKR', 'SUSHI', 'ALGO', 'SC', 'WBTC', 'ONT', 'PPT', 'ONX', 'BTTC', 'RUB', 'PIVX', 'ASR', 'FIRO', 'AXSOLD', 'AST', 'MANA', 'DOTUP', 'ATA', 'MEETONE', 'QSP', 'ATD', 'NMR', 'MKR', 'DODO', 'LIT', 'ICP', 'ZEC', 'ATM', 'APPC', 'JEX', 'ICX', 'LOOM', 'ZEN', 'KP3R', 'DOGE', 'DUSK', 'ALPHA', 'BOLT', 'SXP', 'HBAR', 'RVN', 'MLN', 'AUD', 'LTOOLD', 'IDR', 'CTSI', 'KAVA', 'C98', 'PSG', 'HCC', 'VIDT', 'NOK', 'AVA', 'SYS', 'COCOS', 'STRAX', 'EOSUP', 'CZK', 'GAS', 'COVEROLD', 'AAVEDOWN', 'THETA', 'BCHUP', 'WAN', 'ORN', 'PERL', 'XLMDOWN', 'MASK', 'AAVE', 'GBP', 'PERP', '1INCHUP', 'SXPUP', 'YFIDOWN', 'BOND', 'YFI', 'PERLOLD', 'MOD', 'BICO', 'OST', 'XEC', 'YGG', 'PEOPLE', 'AXS', 'ZIL', 'VAI', 'XEM', 'CTXC', 'KEYFI', 'XTZUP', 'BIDR', 'BCHSV', 'AAVEUP', 'SUSHIDOWN', 'COMP', 'ETHBNT', 'OMOLD', 'OOKI', 'RUNE', 'FORTH', 'KMD', 'GHST', 'IDEX', 'DEXE', 'AVAX', 'UAH', 'KNC', 'PROS', 'PROM', 'BTCUP', 'CHAT', 'BGBP', 'LPT', 'HIVE', 'BIFI', 'PORTO', 'SNGLS', 'PYR', 'WAXP', 'DAI', 'YFIUP', 'DAR', 'FET', 'LRC', 'REPV1', 'ADXOLD', 'MTH', 'MTL', 'VET', 'ALPACA', 'USDT', 'USDS', 'OXT', 'USDP', 'DASH', 'NVT', 'SWRV', 'EDO', 'ILV', 'GHS', 'BTCST', 'HKD', 'JOE', 'LSK', 'KEEP', 'CAD', 'BEAM', 'CAN', 'DCR', 'CREAM', 'DATA', 'IMX', 'ENTRP', 'FILUP', 'UNIUP', 'LTC', 'USDC', 'WIN', 'LTCUP', 'INJ', 'TCT', 'PARA', 'LTO', 'VGX', 'TRIBE', 'NXS', 'EFI', 'DYDX', 'AGIX', 'INR', 'CBK', 'CBM', 'INS', 'POND', 'JPY', 'LINA', 'XLM', 'LINK', 'QTUM', 'FILDOWN', 'SUPER', 'UFT', 'POLS', 'KSM', 'LUN', 'FIL', 'POLY', 'STMX', 'RNDR', 'BAL', 'FIO', 'GALA', 'VIB', 'VIA', 'FIS', 'BAR', 'RAD', 'BAT', 'VRAB', 'AKRO', 'NZD', 'MOVR', 'XMR', '1INCHDOWN', 'COTI']

        self.symbols = ['BTC', 'ETH', 'USDT', 'ADA', 'LINK', 'DOT', 'TRX', 'FTM', 'SOL',
                        'MATIC', 'ETC', 'NEO', 'ENJ', 'WAVES', 'ATOM', 'ONE', 'ZEC',
                        'ONT', 'HOT', 'CHZ', 'WIN', 'AXS', 'GALA', 'ANKR', 'RUNE', 'ICP', 'LRC',
                        'ZIL', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS',
                        'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'ADX', 'NULS',
                        'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC',
                        'IOST', 'NANO', 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN',
                        'USDC', 'BCHSV', 'PHB', 'COCOS', 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI',
                        'SRM', 'KSM', 'SUSHI', 'BEL', 'NEAR', 'SLP', 'REEF', 'C98', 'MINA', 'VOXEL']

        # OFF BNB

        self.all_pairs = self.defa_all_pairs()

        self.selected_symbols = self.symbols[20:45] + self.symbols[:3]  ## kiválasztam amivel dolgozok
        self.arb_symbols = ['USDT']
        # self.commission = self.defa_commission()
        self.selected_pairs = self.defa_selected_pairs()  ##a kiválasztott szimbólumokhoz kapcsolódó párokat kiválasztom
        self.df = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        self.df = self.df.astype(float)
        # print(self.df)
        # self.df = self.df.fillna(0)
        # print(self.df)

        # self.df_save = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        # self.df_save = self.df.astype(float)

        # self.df_fee = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        # self.df_fee = self.df.astype(float)

        # self.df_sim_price = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        # self.df_sim_price = self.df.astype(float)


        # convert_multipyert azért használom, hogy egy adott arbirage profitját gyorsan tudjam kiszámolni
        # ne a pandasban kelljen keresgetni mert az nagyon lassú
        self.price_mod_fee = self.defa_price_dict()
        self.price = self.defa_price_dict()

        self.socket_list = self.defa_socket_list()

        # self.riport.live_text = self.get_settings()
        # self.riport.stamp_live()

        self.pair_info = self.defa_pair_info()

        # az a számlám miből mennyi van, azért hívom becsült mnnyiségnek mert
        # kötés közben nincs idő lekérdezni a számlát ezért csak megsaccolom azt
        self.estimated_amount = self.get_estimated_amount()

        self.traded_prices = []
        self.triangle_profit_result = 0.0
        # self.summa_amount_USDT = 0.0

        # print(self.get_historical_klines_1W("BTCUSDT"))





    async def _get_historical_klines_1W(self, symbol):
        await self.open_binance_client()
        res = await self.b_client.get_historical_klines(symbol, self.b_client.KLINE_INTERVAL_1MONTH, "1 Jan, 2022")
        await self.close_binance_client()
        # return  (Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, Number of trades, Taker buy base asset volume, Taker buy quote asset volume, Ignore)
        return res

    def get_historical_klines_1W(self, symbol):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._get_historical_klines_1W(symbol))

    # def get_triangle_profit_mod_spread(self, arb):
    #     return self.spread_mod_triangle * self.price[arb[0] + arb[1]] * self.price[arb[1] + arb[2]] * self.price[arb[2] + arb[3]]

    # def get_triangle_profit_mod_fee(self, arb):
    #     return self.official_fee_mod_triangle * self.price[arb[0] + arb[1]] * self.price[arb[1] + arb[2]] * self.price[arb[2] + arb[3]]

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

    async def _order_market_sell(self, symbol, quantity):
        # print(' _order_market_sell', symbol, quantity)
        order = await self.b_client.order_market_sell(
                symbol=symbol,
                quantity=str(quantity))
        return order

    def order_market_sell(self, symbol, quantity):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self._order_market_sell(symbol, quantity))


    async def _order_market_buy(self, symbol, quantity):
        # print(' _order_market_buy', symbol, quantity)
        order = await self.b_client.order_market_buy(
                symbol=symbol,
                quantity=str(quantity))
        return order

    def order_market_buy(self, symbol, quantity):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._order_market_buy(symbol, quantity))


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

    # def get_settings(self):
    #     i_r = ""
        i_r += "Fee: " + str(self.fee) + "%" + "\n"
        # i_r += "Spread: " + str(self.spread) + "%" + "\n"
        # i_r += "Arbitrage check delay: " + str(self.arb_check_delay) + " sec" + "\n"
        # i_r += "Selected symbols: " + textwrap.fill(str(self.selected_symbols), width=80) + "\n"
        # return i_r

    def get_amount_by_symbol(self, symbol):
        for i_i in self.account['balances']:
            if i_i['asset'] == symbol:
                return float(i_i['free'])
        return 0.0

    def print_estimated_amount(self):
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
        # print("Profit:", round(summa_in_USDT - self.summa_amount_USDT, 8))
        # self.summa_amount_USDT = summa_in_USDT
        # if summa_in_USDT < self.stop_tradeing_at_USDT:
        #     sys.exit()
        
    def refresh_estimated_amount(self):
        self.account = self.get_account()
        self.estimated_amount = self.get_estimated_amount()

    def reduce_BNB(self, percent):
        percent = percent/100
        if "BNB" in self.estimated_amount.keys():
            self.estimated_amount["BNB"] = self.estimated_amount["BNB"] * (1 - percent)

    def defa_commission(self):
        comission = {}
        for sesy in self.selected_symbols:
            comission[sesy] = 0.0
        comission["BNB"] = 0.0
        return comission

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

    # def defa_symbols_frequency(self):
    #     i_selected_symbols_frequency = {}
    #     for fr in self.selected_symbols:
    #         i_selected_symbols_frequency[fr] = 0
    #     i_selected_symbols_frequency[-1] = 0
    #     return i_selected_symbols_frequency

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

    async def get_fills_qty(self, order_result):
        # print(order_result)
        fills = order_result['fills']
        total_qty = 0.0
        total_amount = 0.0
        for fs in fills:
            # self.commission[fs['commissionAsset']] += float(fs['commission'])
            total_qty += float(fs['qty'])
            total_amount += (float(fs['qty']) * float(fs['price']))
        avg_price = total_amount / total_qty
        return avg_price, total_qty

    async def _trade(self, from_symbol, to_symbol, pair_symbol, side,
                     step_size, min_qt,
                     base_asset, quote_asset,
                     estimated_amount):
        print(" Trade log:", from_symbol, "->", to_symbol, " - ", pair_symbol, side)

        # print('nprice', "ETHUSDT", self.price['ETHUSDT'], "USDTETH", self.price['USDTETH'])
        # print('1/nprice', "ETHUSDT", 1/self.price['ETHUSDT'], "USDTETH", 1/self.price['USDTETH'])

        # order = self.bx_client.order_market_buy(
        #     symbol='ETHUSDT',
        #     quantity=str(0.014))
        # # print("speed 1", datetime.now() - start)
        # print("buy1", order['fills'][0]['price'], round(1/ float(order['fills'][0]['price']), 8))
        #
        # order = self.bx_client.order_market_sell(
        #     symbol='ETHUSDT',
        #     quantity=str(0.014))
        # # print("speed 3", datetime.now() - start)
        # print("sell", order['fills'][0]['price'], round(1 / float(order['fills'][0]['price']), 8))
        #
        # sys.exit(0)

        if side == "BUY" and self.real_trade:
            # print(estimated_amount[quote_asset], self.price[from_symbol + to_symbol])
            trade_qty = estimated_amount[quote_asset] * self.price["".join([from_symbol, to_symbol])]
            rounded_trade_qty = self.round_qty_with_step_size(trade_qty, step_size, 5)
            # print('rounded_trade_qty', rounded_trade_qty)
            print('est quote:', estimated_amount[quote_asset], "rounded_trade_qty", rounded_trade_qty)
            order = self.bx_client.order_market_buy(
                symbol=pair_symbol,
                # quantity=str(rounded_trade_qty),
                quantity=rounded_trade_qty)
            # order = await self._order_market_buy(
            #     symbol=pair_symbol,
            #     quantity=rounded_trade_qty)

            traded_price, traded_qty = await self.get_fills_qty(order)
            # print('{0:.8f}'.format(traded_price), '{0:.8f}'.format(1 / traded_price))
            self.traded_prices.append(1 / traded_price)
            return traded_qty
        elif side == "SELL" and self.real_trade:
            rounded_trade_qty = self.round_qty_with_step_size(estimated_amount[base_asset], step_size)
            # if rounded_trade_qty > estimated_amount[from_symbol]:
            #     rounded_trade_qty = round(rounded_trade_qty - step_size, 8)


            ## biztonsági tartalék mozgó árakra
            # rounded_trade_qty = round(rounded_trade_qty - step_size, 8)

            # print("price", self.price[from_symbol + to_symbol])
            # print("rounded_trade_qty", side, rounded_trade_qty)
            # order = await self._order_market_sell(
            #     symbol=pair_symbol,
            #     quantity=rounded_trade_qty)

            order = self.bx_client.order_market_sell(
                symbol=pair_symbol,
                quantity=rounded_trade_qty)

            traded_price, traded_qty = await self.get_fills_qty(order)
            # print('{0:.8f}'.format(traded_price), '{0:.8f}'.format(1 / traded_price))
            self.traded_prices.append(traded_price)
            return traded_price * traded_qty

    def round_qty_with_step_size(slef, quantity, step_size, reduce=0):
        reduce = Decimal(reduce * step_size)  # ennyi darabbal visszaveszi
        quantity = Decimal(str(quantity))
        return round(float(quantity - quantity % Decimal(str(step_size)) - reduce), 8)

    def trade_triangle(self, arb):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._trade_triangle(arb))

    def get_arb_profit(self):
        i_p = 1
        for tp in self.traded_prices:
            i_p = i_p * tp
        return i_p * self.official_fee_mod_triangle

    async def _trade_triangle(self, arb):
        await self.open_binance_client()

        est_prices = self.est_trade_prices(arb)
        if np.prod(est_prices) * self.spread_mod_triangle > 1:
            self.traded_prices = []
            for i in range(len(arb) - 1):
                from_symbol = arb[i]
                to_symbol = arb[i + 1]
                from_to_symbol = "".join([from_symbol, to_symbol])
                # print(self.pair_info[from_to_symbol])
                i_pair_symbol = self.pair_info[from_to_symbol][0]
                i_side = self.pair_info[from_to_symbol][1]
                i_step_size = self.pair_info[from_to_symbol][2]
                i_min_qty = self.pair_info[from_to_symbol][3]
                base_asset = self.pair_info[from_to_symbol][4]
                qoute_asset = self.pair_info[from_to_symbol][5]
                # sys.exit(0)
                estimated_qty = await self._trade(from_symbol=from_symbol,
                                                 to_symbol=to_symbol,
                                                 pair_symbol=i_pair_symbol,
                                                 side=i_side,
                                                 step_size=i_step_size,
                                                 min_qt=i_min_qty,
                                                 quote_asset=qoute_asset,
                                                 base_asset=base_asset,
                                                 estimated_amount=self.estimated_amount.copy())
                self.estimated_amount[to_symbol] = estimated_qty + self.estimated_amount[to_symbol]
                self.estimated_amount[from_symbol] = 0
            # self.account = await self.b_client.get_account()
            self.account = await self.b_client.get_account()
            self.estimated_amount = self.get_estimated_amount()
            print("")
            print(datetime.now(), arb)
            print("Est prices:     ",
                  '{0:.8f}'.format(est_prices[0]),
                  '{0:.8f}'.format(est_prices[1]),
                  '{0:.8f}'.format(est_prices[2]), "Gross profit: ",
                  np.prod(est_prices))

            print("Realised prices:",
                  '{0:.8f}'.format(self.traded_prices[0]),
                  '{0:.8f}'.format(self.traded_prices[1]),
                  '{0:.8f}'.format(self.traded_prices[2]), "Gross profit: ",
                  np.prod(self.traded_prices)
                  )
            self.print_estimated_amount()
            # sys.exit(0)
        await self.close_binance_client()
            # sys.exit(0)
            # self.estimated_amount = self.defa_estimated_amount()
            # print(" Start trade triangle:                           ", arb)
            # print(" Speed:", time.time() - start, "                     ", "Profit (-fee):",
            #       reduce(lambda x, y: x * y, self.triangle_profit) * n_arb.official_fee_mod_triangle)
            # print(" Realised prices:", str(self.triangle_profit))
            # print()

    def arb_bellman_ford_negative_cycles(self, g, s):
        """
        Bellman Ford, modified so that it returns cycles.
        Runtime is O(VE).

        :param g: graph
        :type g: networkx weighted DiGraph
        :param s: source vertex
        :type s: str
        :return: all negative-weight cycles reachable from a source vertex
        :rtype: str list (empty if no neg-weight cyc)
        """
        n = len(g.nodes())
        d = defaultdict(lambda: math.inf)  # distances dict
        # print(d)
        p = defaultdict(lambda: -1)  # predecessor dict
        # print(p)
        d[s] = 0

        for _ in range(n - 1):
            for u, v in g.edges():
                # Bellman-Ford relaxation
                weight = g[u][v]["weight"]
                if d[u] + weight < d[v]:
                    d[v] = d[u] + weight
                    p[v] = u  # update pred

        # Find cycles if they exist
        all_cycles = []
        seen = defaultdict(lambda: False)

        for u, v in g.edges():
            weight = g[u][v]["weight"]
            # If we can relax further, there must be a neg-weight cycle
            if seen[v]:
                continue

            if d[u] + weight < d[v]:
                cycle = []
                x = v
                while True:
                    # Walk back along predecessors until a cycle is found
                    seen[x] = True
                    cycle.append(x)
                    x = p[x]
                    if x == v or x in cycle:
                        break
                # Slice to get the cyclic portion
                idx = cycle.index(x)
                cycle.append(x)
                scicle = cycle[idx:][::-1]
                if scicle[0] in self.arb_symbols and len(scicle) == self.max_cicle_lengt + 1:
                    all_cycles.append(cycle[idx:][::-1])
                    # print(cycle[idx:][::-1])
                    break
                # all_cycles.append(cycle[idx:][::-1])
        return all_cycles

    def arb_find(self, sources=None):
        """
        Looks for arbitrage opportunities within a snapshot, i.e negative-weight cycles
        that include the currencies given in the sources list
        :param filename: filename of snapshot, defaults to "snapshot.csv"
        :type filename: str, optional
        :param find_all: whether to find all paths, defaults to False.
                         If false, sources must be provided.
        :type find_all: bool, optional
        :param sources: list of starting nodes – should choose the 'most connected' pairs,
                        defaults to None.
        :type sources: str list, optional
        :return: list of negative-weight cycles, or None if none exist
        :rtype: str list
        """
        # Read df and convert to negative logs so we can use Bellman Ford
        # Negative weight cycles thus correspond to arbitrage opps
        # Transpose log_df so that graph has same API as the dataframe

        g = nx.DiGraph(-np.log(self.df).fillna(0).T)
        # print(self.df)
        #
        # g = nx.DiGraph(self.df.T)

    
        if nx.negative_edge_cycle(g):
            # nincs szükség mindre, elég az első
            for s in sources:
                res = self.arb_bellman_ford_negative_cycles(g, s)
                if res:
                    # print("arb find", res[0])
                    return [res[0]]
                else:
                    return []
        else:
            return []

    def start(self):
        self.socket_thread1 = Thread(target=self.start_asyc_websocket, args=(1,), daemon=True)
        self.socket_thread2 = Thread(target=self.start_asyc_websocket, args=(2,), daemon=True)
        self.socket_thread3 = Thread(target=self.start_asyc_websocket, args=(3,), daemon=True)
        self.socket_thread4 = Thread(target=self.start_asyc_websocket, args=(4,), daemon=True)

        print("Start thread 1")
        self.socket_thread1.start()
        time.sleep(3)
        print("Start thread 2")
        self.socket_thread2.start()
        time.sleep(3)
        print("Start thread 3")
        self.socket_thread3.start()
        time.sleep(3)
        print("Start thread 4")
        self.socket_thread4.start()


        # self.socket_thread1.join()
        # self.socket_thread2.join()

        # monitoring the error
        # self.stop_thread = Thread(target=self.restart_stream, daemon=True)
        # self.stop_thread.start()

    # def stop(self):
    #     self.run_restart_stream = False
    #     self.b_twm.stop_socket(self.multiplex_socket)
    #     self.b_twm.stop()
    #     sys.exit(0)

    # self.b_twm.stop_client()

    # def start_websocket(self):
    #     self.b_twm = ThreadedWebsocketManager(api_key=self.api_key, api_secret=self.api_secret)
    #     self.b_twm.start()
    #     self.multiplex_socket = self.b_twm.start_multiplex_socket(callback=self.socket_handler,
    #                                                               streams=self.socket_list)

    async def asyc_websocket(self, slice):
        client = await AsyncClient.create()
        bm = BinanceSocketManager(client)

        # start any sockets here, i.e a trade socket
        # ts = bm.trade_socket('BNBBTC')
        x_slist_len = int(len(self.socket_list) / 4)
        if slice == 1:
            ts = bm.multiplex_socket(self.socket_list[:x_slist_len])
        elif slice == 2:
            ts = bm.multiplex_socket(self.socket_list[x_slist_len:x_slist_len * 2])
        elif slice == 3:
            ts = bm.multiplex_socket(self.socket_list[x_slist_len * 2:x_slist_len * 3])
        elif slice == 4:
            ts = bm.multiplex_socket(self.socket_list[x_slist_len * 3:])


        # then start receiving messages
        async with ts as tscm:
            while True:
                res = await tscm.recv()
                # szét kell választani szimbolumokra, mivel nem egyen hosszúságú ezért használok
                # dictionariket
                # print(res)
                s1 = self.selected_pairs[res['data']['s']][0]
                s2 = self.selected_pairs[res['data']['s']][1]
                # print(res['data']['s'], s1, s2)

                bid = float(res['data']['b'])
                ask = float(res['data']['a'])

                self.df[s1][s2] = round(bid * self.spread_mod, 8)
                self.df[s2][s1] = round((1 / ask) * self.spread_mod, 8)

                # self.price_mod_fee[s1 + s2] = bid * self.official_fee_mod
                # self.price_mod_fee[s2 + s1] = (1 / ask) * self.official_fee_mod

                self.price["".join([s1, s2])] = bid
                self.price["".join([s2, s1])] = round(1 / ask, 8)

        await client.close_connection()

    def start_asyc_websocket(self, slice):
        # print('start_asyc_websocket', slice)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket(slice))
        loop.close()

    def est_trade_prices(self, arb):
        est_prices = []
        for i in range(len(arb) - 1):
            from_to_symbol = "".join([arb[i], arb[i + 1]])
            i_side = n_arb.pair_info[from_to_symbol][1]
            base_asset = n_arb.pair_info[from_to_symbol][4]
            qoute_asset = n_arb.pair_info[from_to_symbol][5]
            if i_side == "BUY":
                est_prices.append(round(n_arb.price["".join([qoute_asset, base_asset])], 8))
            else:
                est_prices.append(round(n_arb.price["".join([base_asset, qoute_asset])], 8))
        return est_prices



if __name__ == '__main__':
    n_arb = n_arbitrage()
    n_arb.start()

    init_time = 8
    print("Init price dataframe:")
    for i in range(init_time + 1):
        print("\r ", init_time, " /", i, end="")
        time.sleep(1)
    print("")
    print("Number of pairs:", len(n_arb.selected_pairs))
    print("Fee:", n_arb.official_fee, "%")
    print("Spread:", n_arb.spread, "%")
    print("Start symbol:", n_arb.arb_symbols)
    print("arb_chk_delay:", n_arb.arb_check_delay, "sec")
    n_arb.print_estimated_amount()

    # n_arb.riport.create("A1_symbol_frequency",
    #                     "Arbitrazsban erintett symbol-ok gyakorisaga, leggyakoribb tirangles, leggyakoribb kezdo symbol-ok")

    # traded_triangle_count = 0
    # founded_trianles_count = 0
    # ha egy triangel meg van akkor az többször is észreveszi,
    # ezért nem folgalkozok azokkal amelyek egymás után ugyan azok
    last_triangle = []
    while True:
        for arb in n_arb.arb_find(n_arb.arb_symbols):  # ez egyben egy if is :)
            if "ETH" in arb and "BTC" in arb:
                pass
            else:
                print("Try:", arb)
                n_arb.trade_triangle(arb)
