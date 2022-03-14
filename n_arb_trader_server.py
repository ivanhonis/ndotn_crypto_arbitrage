# import sys
import asyncio
import sys
# import winsound
import requests
# import json

import numpy as np
import pandas as pd
import math
from functools import reduce
import time
from threading import Thread
import textwrap

# Bellman Ford - Graph
import networkx as nx
from collections import defaultdict

# Binanace
# from binance.client import Client
# from binance import ThreadedWebsocketManager
from binance import AsyncClient, BinanceSocketManager
from binance.helpers import round_step_size

# nDot
from n_riport import n_riport


class n_arbitrage:

    def __init__(self):
        self.arb_symbols = ['USDT']
        self.official_fee = 0.1  # %   for profit triangle profit calc
        self.spread = 0.9  # %
        # self.gap = 0.07  # %
        self.arb_check_delay = 0.00  # arbitrás kereéséek közötti várakozáa 0.5 = 2xmásodpercenként
        # self.riport = n_riport()
        self.stop_tradeing_at_USDT = 60

        self.socket_thread = None
        # self.stop_thread = None

        self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
        self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
        self.b_client = None
        self.account = self.get_account()
        self.exchange_info = self.get_exchange_info()

        # self.gap_mod = 1 - (self.gap / 100)
        self.spread_mod = (1 - (self.spread / 100)) ** 3
        self.official_fee_mod = (1 - (self.official_fee / 100)) ** 3

        # self.symbols = ['AGLD', 'STPT', 'MXN', 'UGX', 'RENBTC', 'GLM', 'RAY', 'NEAR', 'AUDIO', 'HNT', 'ADADOWN', 'CDT', 'SPARTA', 'SUSD', 'FARM', 'XNO', 'AION', 'NPXS', 'DGB', 'ZRX', 'BCD', 'EASY', 'SANTOS', 'WING', 'WNXM', 'BCH', 'JST', 'ADAUP', 'HOT', 'AR', 'IRIS', 'RAMP', 'BCX', 'SEK', 'TRIG', 'RCN', 'COVER', 'FLM', 'GNO', 'VITE', 'GNT', 'BKRW', 'CFX', 'XPR', 'SFP', 'DIA', 'RDN', 'ACA', 'ARDR', 'LOOMOLD', 'NEBL', 'ACH', 'SLPOLD', 'BEL', 'JUV', 'ACM', 'MINA', 'GRTDOWN', 'VTHO', 'PYROLD', 'SGB', 'SALT', 'STORM', 'REN', 'REP', 'ADA', 'ELF', 'REQ', 'STORJ', 'CHF', 'ADD', 'BZRX', 'SGT', 'DF', 'RARE', 'EOSDOWN', 'PAXG', 'YOYO', 'PAX', 'CHR', 'VND', 'BCHDOWN', 'WAVES', 'CHZ', 'ADX', 'XRP', 'WPR', 'JASMY', 'AED', 'FIDA', 'SAND', 'DKK', 'OCEAN', 'FOR', 'UMA', 'DREPOLD', 'SCRT', 'TUSD', 'EZ', 'TKO', 'WABI', 'RGT', 'IDRT', 'ENG', 'ENJ', 'UNIDOWN', 'YFII', 'KZT', 'OAX', 'GRT', 'GRS', 'UND', 'HARD', 'TFUEL', 'ENS', 'LEND', 'DLT', 'TROY', 'XLMUP', 'UNI', 'BTCDOWN', 'TLM', 'HUF', 'SBTC', 'CKB', 'WRX', 'XTZ', 'LUNA', 'ETHDOWN', 'AGI', 'BCHA', 'EON', 'EOP', 'EOS', 'GO', 'NCASH', 'RIF', 'NSBT', 'SKL', 'XDATA', 'GTC', 'PEN', 'BLINK', 'SOLO', 'SXPDOWN', 'HC', 'SKY', 'BURGER', 'NAS', 'NAV', 'GTO', 'WTC', 'XVG', 'EPS', 'DNT', 'CLV', 'FLOW', 'XTZDOWN', 'XVS', 'STEEM', 'BVND', 'SLP', 'VRT', 'NBS', 'DON', 'LAZIO', 'DOT', 'IQ', 'GRTUP', '1INCH', 'KNCL', 'CHESS', 'MITH', 'ERD', 'DEGO', 'CND', 'GYEN', 'UNFI', 'FTM', 'POWR', 'ERN', 'GVT', 'WINGS', 'FTT', 'VOXEL', 'PHA', 'RLC', 'PHB', 'TRXDOWN', 'ATOM', 'XRPUP', 'QUICK', 'BLZ', 'SNM', 'BOBA', 'MBL', 'MTLX', 'SNT', 'PHP', 'SNX', 'LTCDOWN', 'FUN', 'SNMOLD', 'COP', 'COS', 'API3', 'USD', 'QKC', 'SUSHIUP', 'ROSE', 'GLMR', 'XYM', 'PURSE', 'SOL', 'TRXUP', 'CITY', 'ETC', 'BNC', 'CELR', 'UST', 'OGN', 'ETH', 'NEO', 'TOMO', 'CELO', 'KLAY', 'AUCTION', 'BADGER', 'HIGH', 'GXS', 'TRB', 'BNT', 'QLC', 'LBA', 'MDA', 'BNX', 'UTK', 'WSOL', 'HEGIC', 'MA', 'AMB', 'MC', 'TRU', 'FUEL', 'DREP', 'TRY', 'TRX', 'MDT', 'NFT', 'MDX', 'XRPDOWN', 'AERGO', 'EUR', 'AMP', 'BOT', 'NULS', 'AUTO', 'NGN', 'ANC', 'BDOT', 'EGLD', 'ANTOLD', 'SPELL', 'PUNDIX', 'FXS', 'PLA', 'HNST', 'EVX', 'CRV', 'BAKE', 'ANT', 'NU', 'FLUX', 'ANY', 'LINKUP', 'SRM', 'QISWAP', 'TORN', 'PLN', 'QNT', 'ALICE', 'OG', 'MFT', 'OM', 'BTTOLD', 'BETH', 'BQX', 'WETH', 'PHBV1', 'BETA', 'BRD', 'SSV', 'BUSD', 'CTK', 'ARPA', 'DOTDOWN', 'BRL', 'ALCX', 'CTR', 'MATIC', 'IOTX', 'SHIB', 'TVK', 'FRONT', 'ZAR', 'DOCK', 'STX', 'PNT', 'QI', 'DENT', 'MBOX', 'SUB', 'POA', 'IOST', 'CAKE', 'ETHUP', 'POE', 'OMG', 'BAND', 'SUN', 'ASTR', 'SUNOLD', 'BTC', 'TWT', 'NKN', 'RSR', 'IOTA', 'CVC', 'REEF', 'BTG', 'MIR', 'KES', 'ARK', 'LOKA', 'CVP', 'ARN', 'KEY', 'BTS', 'SPARTAOLD', 'ARS', 'CVX', 'ONE', 'LINKDOWN', 'ONG', 'ANKR', 'SUSHI', 'ALGO', 'SC', 'WBTC', 'ONT', 'PPT', 'ONX', 'BTTC', 'RUB', 'PIVX', 'ASR', 'FIRO', 'AXSOLD', 'AST', 'MANA', 'DOTUP', 'ATA', 'MEETONE', 'QSP', 'ATD', 'NMR', 'MKR', 'DODO', 'LIT', 'ICP', 'ZEC', 'ATM', 'APPC', 'JEX', 'ICX', 'LOOM', 'ZEN', 'KP3R', 'DOGE', 'DUSK', 'ALPHA', 'BOLT', 'SXP', 'HBAR', 'RVN', 'MLN', 'AUD', 'LTOOLD', 'IDR', 'CTSI', 'KAVA', 'C98', 'PSG', 'HCC', 'VIDT', 'NOK', 'AVA', 'SYS', 'COCOS', 'STRAX', 'EOSUP', 'CZK', 'GAS', 'COVEROLD', 'AAVEDOWN', 'THETA', 'BCHUP', 'WAN', 'ORN', 'PERL', 'XLMDOWN', 'MASK', 'AAVE', 'GBP', 'PERP', '1INCHUP', 'SXPUP', 'YFIDOWN', 'BOND', 'YFI', 'PERLOLD', 'MOD', 'BICO', 'OST', 'XEC', 'YGG', 'PEOPLE', 'AXS', 'ZIL', 'VAI', 'XEM', 'CTXC', 'KEYFI', 'XTZUP', 'BIDR', 'BCHSV', 'AAVEUP', 'SUSHIDOWN', 'COMP', 'ETHBNT', 'OMOLD', 'OOKI', 'RUNE', 'FORTH', 'KMD', 'GHST', 'IDEX', 'DEXE', 'AVAX', 'UAH', 'KNC', 'PROS', 'PROM', 'BTCUP', 'CHAT', 'BGBP', 'LPT', 'HIVE', 'BIFI', 'PORTO', 'SNGLS', 'PYR', 'WAXP', 'DAI', 'YFIUP', 'DAR', 'FET', 'LRC', 'REPV1', 'ADXOLD', 'MTH', 'MTL', 'VET', 'ALPACA', 'USDT', 'USDS', 'OXT', 'USDP', 'DASH', 'NVT', 'SWRV', 'EDO', 'ILV', 'GHS', 'BTCST', 'HKD', 'JOE', 'LSK', 'KEEP', 'CAD', 'BEAM', 'CAN', 'DCR', 'CREAM', 'DATA', 'IMX', 'ENTRP', 'FILUP', 'UNIUP', 'LTC', 'USDC', 'WIN', 'LTCUP', 'INJ', 'TCT', 'PARA', 'LTO', 'VGX', 'TRIBE', 'NXS', 'EFI', 'DYDX', 'AGIX', 'INR', 'CBK', 'CBM', 'INS', 'POND', 'JPY', 'LINA', 'XLM', 'LINK', 'QTUM', 'FILDOWN', 'SUPER', 'UFT', 'POLS', 'KSM', 'LUN', 'FIL', 'POLY', 'STMX', 'RNDR', 'BAL', 'FIO', 'GALA', 'VIB', 'VIA', 'FIS', 'BAR', 'RAD', 'BAT', 'VRAB', 'AKRO', 'NZD', 'MOVR', 'XMR', '1INCHDOWN', 'COTI']

        self.symbols = ['BTC', 'ETH', 'ADA', 'LINK', 'DOT', 'TRX', 'FTM', 'SOL',
                        'USDT', 'MATIC', 'ETC', 'NEO', 'ENJ', 'WAVES', 'ATOM', 'ONE', 'ZEC',
                        'ONT', 'HOT', 'CHZ', 'WIN', 'AXS', 'GALA', 'ANKR', 'RUNE', 'ICP', 'LRC',
                        'ZIL', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS',
                        'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'ADX', 'NULS',
                        'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC',
                        'IOST', 'NANO', 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN',
                        'USDC', 'BCHSV', 'PHB', 'COCOS', 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI',
                        'SRM', 'KSM', 'SUSHI', 'BEL', 'NEAR', 'SLP', 'REEF', 'C98', 'MINA', 'VOXEL']

        # OFF BNB

        self.all_pairs = self.defa_all_pairs()

        self.selected_symbols = self.symbols[:50]  ## kiválasztam amivel dolgozok
        self.commission = self.defa_commission()
        self.selected_pairs = self.defa_selected_pairs()  ##a kiválasztott szimbólumokhoz kapcsolódó párokat kiválasztom
        self.df = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        self.df = self.df.astype(float)

        self.df_save = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        self.df_save = self.df.astype(float)

        # self.df_fee = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        # self.df_fee = self.df.astype(float)

        # self.df_sim_price = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        # self.df_sim_price = self.df.astype(float)

        self.convert_multiplier = self.defa_convert_multiplier()

        self.socket_list = self.defa_socket_list()

        # self.riport.live_text = self.get_settings()
        # self.riport.stamp_live()

        self.pair_info = self.defa_pair_info()
        self.estimated_amount = self.defa_estimated_amount()
        self.triangle_profit = []
        self.triangle_profit_result = 0.0
        self.summa_amount_USDT = 0.0
        
    def get_triangle_profit_by_spread(self, arb):
        self.triangle_profit_result = self.spread_mod * self.convert_multiplier[arb[0] + arb[1]] * self.convert_multiplier[arb[1] + arb[2]] * self.convert_multiplier[arb[2] + arb[3]]
        return self.triangle_profit_result

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
        return 1 / float(res.json()["price"])

    async def _order_market_sell(self, symbol, quantity):
        order = await self.b_client.order_market_sell(
                symbol=symbol,
                quantity=str(quantity))
        return order

    def order_market_sell(self, symbol, quantity):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._order_market_sell(symbol, quantity))

    async def _order_market_buy(self, symbol, quantity):
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

    def get_settings(self):
        i_r = ""
        # i_r += "Fee: " + str(self.fee) + "%" + "\n"
        i_r += "Spread: " + str(self.spread) + "%" + "\n"
        i_r += "Arbitrage check delay: " + str(self.arb_check_delay) + " sec" + "\n"
        i_r += "Selected symbols: " + textwrap.fill(str(self.selected_symbols), width=80) + "\n"
        return i_r

    def get_amount_by_symbol(self, symbol):
        for i_i in self.account['balances']:
            if i_i['asset'] == symbol:
                return float(i_i['free'])
        return 0.0

    def print_estimated_amount(self):
        print(" Spot wallet:")
        summa_in_USDT = 0
        self.convert_multiplier["BNBUSDT"] = self.get_BNB()
        for ea in self.estimated_amount:
            if self.estimated_amount[ea] != 0:
                summa_in_USDT += (self.estimated_amount[ea] * self.convert_multiplier[ea + "USDT"])
                eap = ea + "     "
                print("  ", eap[0:5], '{0:.8f}'.format(self.estimated_amount[ea]))
            
        print("Summa in USDT:", summa_in_USDT, "Profit:", round(summa_in_USDT - self.summa_amount_USDT, 8))
        self.summa_amount_USDT = summa_in_USDT
        if summa_in_USDT < self.stop_tradeing_at_USDT:
            sys.exit()
        
    def refresh_estimated_amount(self):
        self.account = self.get_account()
        self.estimated_amount = self.defa_estimated_amount()

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

    def defa_estimated_amount(self):
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
                    # print(self.get_symbol_info2(si1 + si2)['filters'])
                    filters = self.get_symbol_info2(si1 + si2)['filters'][2]
                    step_size = float(filters['stepSize'])
                    min_qty = float(filters['minQty'])
                    pair_info[si1 + si2] = [si1 + si2, "SELL", step_size, min_qty]
                    pair_info[si2 + si1] = [si1 + si2, "BUY", step_size, min_qty]
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

    def defa_convert_multiplier(self):
        i_selected_pairs = {}
        for si1 in self.selected_symbols:
            for si2 in self.selected_symbols:
                i_selected_pairs[si1 + si2] = 1.0
                i_selected_pairs[si2 + si1] = 1.0
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
        fills = order_result['fills']
        total_qty = 0.0
        total_amount = 0.0
        for fs in fills:
            self.commission[fs['commissionAsset']] += float(fs['commission'])
            total_qty += float(fs['qty'])
            total_amount += (float(fs['qty']) * float(fs['price']))
        avg_price = total_amount / total_qty
        return avg_price, total_qty

    async def _trade(self, from_symbol, to_symbol, pair_symbol, side, step_size, min_qt, estimated_amount):
        print(" Trade log:", from_symbol, "->", to_symbol, "   ", pair_symbol, side)
        # print(" ", from_symbol, "->", to_symbol)
        print(" ", pair_symbol, side)
        # print(" side", side)
        print(" step_size", step_size)
        # print(" min_qt", min_qt)
        print(" etimated amount from_symbol", estimated_amount[from_symbol])
        if side == "BUY":
            trade_qty = (estimated_amount[from_symbol] * self.convert_multiplier[from_symbol + to_symbol])
            rounded_trade_qty = round_step_size(trade_qty - step_size - step_size, step_size)
            # if rounded_trade_qty > min_qt:
            print("trade_qty", trade_qty)
            print("price", self.convert_multiplier[from_symbol + to_symbol])
            print("rounded_trade_qty", side, rounded_trade_qty)
            order = await self._order_market_buy(
                symbol=pair_symbol,
                quantity=rounded_trade_qty)

            traded_price, traded_qty = await self.get_fills_qty(order)
            self.triangle_profit.append(1 / traded_price)
            # print(order)
            print("traded_price", traded_price)
            print("traded_qty", traded_qty)
            return traded_qty
        elif side == "SELL":
            rounded_trade_qty = round_step_size(estimated_amount[from_symbol], step_size)
            if rounded_trade_qty > estimated_amount[from_symbol]:
                rounded_trade_qty = round(rounded_trade_qty - step_size, 8)


            ## biztonsági tartalék mozgó árakra
            # rounded_trade_qty = round(rounded_trade_qty - step_size, 8)

            print("price", self.convert_multiplier[from_symbol + to_symbol])
            print("rounded_trade_qty", side, rounded_trade_qty)
            order = await self._order_market_sell(
                symbol=pair_symbol,
                quantity=rounded_trade_qty)
            # print(order)
            traded_price, traded_qty = await self.get_fills_qty(order)
            self.triangle_profit.append(traded_price)
            print("traded_price", traded_price)
            print("traded_qty", traded_qty)
            return traded_price * traded_qty

            # if rounded_trade_qty > min_qt:
            #     print("rounded_trade_qty", rounded_trade_qty)
            #     order = await self._order_market_sell(
            #         symbol=pair_symbol,
            #         quantity=rounded_trade_qty)
            #     print(order)
            #     # meg kell becsülni, hogy mennyit kaptam a to_symbol ból
            #     # cross_price = self.df_sim_price.at[to_symbol, from_symbol]
            #     # return rounded_trade_qty * cross_price
            #     traded_price, traded_qty = await self.get_fills_qty(order)
            #     self.triangle_profit.append(1 / traded_price)
            #     return traded_price * (traded_qty - step_size)
            # else:
            #     print("SELL minimum problem")
            #     return 0

            # order = b_client.create_test_order(
            #    symbol=pair_symbol,
            #    side=Client.SIDE_SELL,
            #    type=Client.ORDER_TYPE_MARKET,
            #    quantity=rounded_trade_qty
            # )

    def trade_triangle(self, arb):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._trade_triangle(arb))

    async def _trade_triangle(self, arb):
        await self.open_binance_client()
        start = time.time()
        self.triangle_profit = [1.0]
        for i in range(len(arb) - 1):
            from_symbol = arb[i]
            to_symbol = arb[i + 1]
            from_to_symbol = from_symbol + to_symbol
            # print(pair_info[from_to_symbol])
            i_pair_symbol = self.pair_info[from_to_symbol][0]
            i_side = self.pair_info[from_to_symbol][1]
            i_step_size = self.pair_info[from_to_symbol][2]
            i_min_qty = self.pair_info[from_to_symbol][3]
            estimated_qty = await self._trade(from_symbol=from_symbol,
                                             to_symbol=to_symbol,
                                             pair_symbol=i_pair_symbol,
                                             side=i_side,
                                             step_size=i_step_size,
                                             min_qt=i_min_qty,
                                             estimated_amount=self.estimated_amount)
            self.estimated_amount[to_symbol] = estimated_qty + self.estimated_amount[to_symbol]
            self.estimated_amount[from_symbol] = 0
            # self.print_estimated_amount()
        self.account = await self.b_client.get_account()
        await self.close_binance_client()
        self.estimated_amount = self.defa_estimated_amount()
        print(" Start trade triangle:                           ", arb)
        print(" Speed:", time.time() - start, "                     ", "Profit (-fee):",
              reduce(lambda x, y: x * y, self.triangle_profit) * n_arb.official_fee_mod)
        print(" Realised prices:", str(self.triangle_profit))
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
        p = defaultdict(lambda: -1)  # predecessor dict
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
                all_cycles.append(cycle[idx:][::-1])
        return all_cycles

    def arb_all_negative_cycles(self, g):
        """
        Get all negative-weight cycles by calling Bellman-Ford on
        each vertex. O(V^2 E)

        :param g: graph
        :type g: networkx weighted DiGraph
        :return: list of negative-weight cycles
        :rtype: list of str list
        """
        all_paths = []
        for v in g.nodes():
            all_paths.append(self.arb_bellman_ford_negative_cycles(g, v))
        flatten = lambda l: [item for sublist in l for item in sublist]
        return [list(i) for i in set(tuple(j) for j in flatten(all_paths))]

    # def calculate_arb(self, cycle, g, verbose=True):
    #     """
    #     For a given negative-weight cycle on the log graph, calculate and
    #     print the arbitrage
    #
    #     :param cycle: the negative-weight cycle
    #     :type cycle: list
    #     :param g: graph
    #     :type g: networkx weighted DiGraph
    #     :param verbose: whether to print path and arb
    #     :type verbose: bool
    #     :return: fractional value of the arbitrage
    #     :rtype: float
    #     """
    #     total = 0
    #     for (p1, p2) in zip(cycle, cycle[1:]):
    #         total += g[p1][p2]["weight"]
    #     # print(total)
    #     arb = np.exp(-total) - 1
    #     # if verbose:
    #     # print("Path:", cycle)
    #     # print(f"{arb*100:.2g}%\n")
    #     return arb

    # def calculate_arb(self, cycle, g, verbose=True):
    #     total = 0
    #     for (p1, p2) in zip(cycle, cycle[1:]):
    #         print("weight", g[p1][p2]["weight"])
    #         total += g[p1][p2]["weight"]
    #     arb = np.exp(-total) - 1
    #     if verbose:
    #         print("Path:", cycle)
    #         print(f"{arb * 100:.2g}%\n")
    #     return arb
    #
    # def find_arbitrage(filename="snapshot.csv"):
    #     df = pd.read_csv(filename, header=0, index_col=0)
    #     g = nx.DiGraph(-np.log(df).fillna(0).T)
    #
    #     if nx.negative_edge_cycle(g):
    #         print("ARBITRAGE FOUND\n" + "=" * 15 + "\n")
    #         for p in all_negative_cycles(g):
    #             calculate_arb(p, g)
    #     else:
    #         print("No arbitrage opportunities")

    # def arb_find(self):
    
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
        # df_save = self.df.copy()
        # df_save.to_clipboard(excel=True)
        g = nx.DiGraph(-np.log(self.df).fillna(0).T)
    
        if nx.negative_edge_cycle(g):
            all_paths = []
            for s in sources:
                all_paths.append(self.arb_bellman_ford_negative_cycles(g, s))
            flatten = lambda l: [item for sublist in l for item in sublist]
            # unique_cycles = [list(i) for i in set(tuple(j) for j in flatten(all_paths))]
            return [list(i) for i in set(tuple(j) for j in flatten(all_paths))]
        
            # for p in unique_cycles:
            #     calculate_arb(p, g)
            # return unique_cycles
        else:
            return []
        
        
        
        
        

    #     """
    #     Looks for arbitrage opportunities within a snapshot, i.e negative-weight cycles
    #     that include the currencies given in the sources list
    #
    #     :param find_all: whether to find all paths, defaults to False.
    #                      If false, sources must be provided.
    #     :type find_all: bool, optional
    #     :param sources: list of starting nodes – should choose the 'most connected' pairs,
    #                     defaults to None.
    #     :type sources: str list, optional
    #     :return: list of negative-weight cycles, or None if none exist
    #     :rtype: str list
    #     """
    #     # Read df and convert to negative logs so we can use Bellman Ford
    #     # Negative weight cycles thus correspond to arbitrage opps
    #     # Transpose log_df so that graph has same API as the dataframe
    #     # print(self.df)
    #     g = nx.DiGraph(-np.log(self.df).fillna(0).T)
    #
    #
    #     if nx.negative_edge_cycle(g):
    #         return self.arb_all_negative_cycles(g)
    #     else:
    #         return []
    #
    # # def stop_all_sockets(self):
    # #     # print("stop 1")
    #     # time.sleep(1)
    #     for i_stream in self.socket_list:
    #         self.b_twm.stop_socket(i_stream)
    #     self.b_twm.stop()

    # def start_multiplex_socket(self):
    #     self.b_twm.start_multiplex_socket(callback=self.socket_handler, streams=self.socket_list)
    #     # self.b_twm.join()

    def start(self):

        self.socket_thread = Thread(target=self.start_asyc_websocket, daemon=True)
        self.socket_thread.start()

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

    async def asyc_websocket(self):
        client = await AsyncClient.create()
        bm = BinanceSocketManager(client)

        # start any sockets here, i.e a trade socket
        # ts = bm.trade_socket('BNBBTC')

        ts = bm.multiplex_socket(self.socket_list)
        # then start receiving messages
        async with ts as tscm:
            while True:
                res = await tscm.recv()

                s1 = self.selected_pairs[res['data']['s']][0]
                s2 = self.selected_pairs[res['data']['s']][1]

                self.convert_multiplier[s1 + s2] = self.df[s1][s2] = float(res['data']['b']) * self.spread_mod
                self.convert_multiplier[s2 + s1] = self.df[s2][s1] = (1 / float(res['data']['a'])) * self.spread_mod

                # self.df_save[s1][s2] = float(res['data']['b'])
                # self.df_save[s2][s1] = float(res['data']['a'])

                # self.df[s1][s2] = (float(res['data']['b']) + float(res['data']['a'])) / 2
                # self.df[s2][s1] = (float(res['data']['b']) + float(res['data']['a'])) / 2

                # self.df_sim_price[s1][s2] = float(res['data']['b']) * self.gap_mod
                # self.df_sim_price[s2][s1] = (1 / float(res['data']['a'])) * self.gap_mod

                # self.pairs_tune[s2 + s1] = float(res['data']['b'])
                # self.pairs_tune[s1 + s2] = (1 / float(res['data']['a']))


                # self.df_sim_price[s1][s2] = float(res['data']['b'])
                # self.df_sim_price[s2][s1] = round(1 / float(res['data']['a']), 8)

                # self.df_fee[s1][s2] = round(float(res['data']['b']) * self.fee_mod, 8)
                # self.df_fee[s2][s1] = round((1 / float(res['data']['a'])) * self.fee_mod, 8)

                # self.df_sim_price[s1][s2] = round((float(res['data']['b']) + float(res['data']['a'])) / 2, 8)
                # self.df_sim_price[s2][s1] = round(1 / ((float(res['data']['b']) + float(res['data']['a'])) / 2), 8)

        await client.close_connection()

    def start_asyc_websocket(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(self.asyc_websocket())
        loop.close()


if __name__ == '__main__':
    n_arb = n_arbitrage()
    n_arb.start()
    # print("Start Trade -------------------")
    # n_arb.print_estimated_amount()
    for i in range(20):
        print("\r", i, end="")
        time.sleep(1)

    # cross_price = 0
    # start2 = time.time()
    # for ix in range(1000000):
    #     cross_price = n_arb.df.at["USDT", "BTC"] + n_arb.df.at["USDT", "BTC"] + n_arb.df.at["USDT", "BTC"]
    # print("Time1:", time.time() - start2)
    #
    # cross_price = 0
    # start2 = time.time()
    # for ix in range(1000000):
    #     cross_price = n_arb.convert_multiplier["USDTBTC"] + n_arb.convert_multiplier["USDTBTC"] + n_arb.convert_multiplier["USDTBTC"]
    # print("Time2:", time.time() - start2)
    #
    # print("BTC -> USDT", n_arb.df.at["USDT", "BTC"])  # fordítva kell címezni ez a BTC USDT transfer muiltiplíer adja meg
    # print("BTC -> USDT", n_arb.convert_multiplier["BTCUSDT"])
    #
    # print("USDT -> BTC", n_arb.df.at["BTC", "USDT"])
    # print("USDT -> BTC", n_arb.convert_multiplier["USDTBTC"])

    # sys.exit(0)

    # # copy to clipboard
    # # n_arb.df.to_clipboard(excel=True)
    # arb = ["USDT", "BTC", "ETH", "USDT"]
    # n_arb.trade_triangle(arb)
    # print("Stop Trade -------------------")
    for i in range(5):
        print(" ")
    print("Number of pairs:", len(n_arb.selected_pairs))
    print("Profit fee:", n_arb.official_fee)
    print("Start symbol:", n_arb.arb_symbols)
    print("arb_chk_delay:", n_arb.arb_check_delay)
    print("Start n_arb_trader_server ....................................")
    n_arb.print_estimated_amount()
    # print(n_arb.commission)
    # sys.exit(0)
    start2 = time.time()
    # for es in n_arb.exchange_info["symbols"]:
    #     if es["symbol"] == "NULSBNB" or es["symbol"] == "BNBUSDT":
    #         print(es)

    # n_arb.riport.create("A1_symbol_frequency",
    #                     "Arbitrazsban erintett symbol-ok gyakorisaga, leggyakoribb tirangles, leggyakoribb kezdo symbol-ok")


    # sys.exit(0)
    # all_arb_count = 0
    # profit_arr = []
    # profit_arr_save = []
    # transactions_count = 0
    # circle_count = 0
    # live_update_val = int((1/n_arb.arb_check_delay) * 60 * live_update)
    traded_triangle_count = 0
    while True:
        # time.sleep(n_arb.arb_check_delay)
        # n_arb.df_save.to_clipboard(excel=True)
        for arb in n_arb.arb_find(n_arb.arb_symbols):  # ez egyben egy if is :)
            # triangel_profit = n_arb.official_fee_mod * n_arb.pairs_tune[arb[0] + arb[1]] * n_arb.pairs_tune[arb[1] + arb[2]] * n_arb.pairs_tune[arb[2] + arb[3]]
            # print(triangel_profit)
            # est_profit =
            if arb[0] in n_arb.arb_symbols and len(arb) == 4 and n_arb.get_triangle_profit_by_spread(arb) > 1:
                print("Est profit", n_arb.triangle_profit_result, "   Prices:   ", n_arb.convert_multiplier[arb[0] + arb[1]], n_arb.convert_multiplier[arb[1] + arb[2]], n_arb.convert_multiplier[arb[2] + arb[3]])
                # print("Est profit", n_arb.triangle_profit_result, "   Prices:   ", 1 / n_arb.convert_multiplier[arb[0] + arb[1]], 1 / n_arb.convert_multiplier[arb[1] + arb[2]], 1 / n_arb.convert_multiplier[arb[2] + arb[3]])

                # print(traded_triangle_count, arb)
                # print(n_arb.pairs_tune[arb[0] + arb[1]], n_arb.pairs_tune[arb[1] + arb[2]], n_arb.pairs_tune[arb[2] + arb[3]])
                # sys.exit(0)
                # break
                # profit_tunel_avg = (n_arb.pairs_tune[arb[0] + arb[1]] + n_arb.pairs_tune[arb[1] + arb[2]] + n_arb.pairs_tune[arb[2] + arb[3]]) / 3
                # profit_tunel_max = max(n_arb.pairs_tune[arb[0] + arb[1]], n_arb.pairs_tune[arb[1] + arb[2]], n_arb.pairs_tune[arb[2] + arb[3]])
                # profit_tunel_min = min(n_arb.pairs_tune[arb[0] + arb[1]], n_arb.pairs_tune[arb[1] + arb[2]], n_arb.pairs_tune[arb[2] + arb[3]])

                # print(traded_triangle_count, arb, n_arb.pairs_tune[arb[0] + arb[1]], n_arb.pairs_tune[arb[1] + arb[2]], n_arb.pairs_tune[arb[2] + arb[3]])
                # traded_triangle_count += 1
                # print(traded_triangle_count, "Start trade triangle:                           ", arb)
                n_arb.trade_triangle(arb)
                traded_triangle_count += 1
                n_arb.print_estimated_amount()
                # winsound.Beep(800, 1000)
                print("Time:", time.time() - start2)
                break
                # n_arb.refresh_estimated_amount()
                # n_arb.reduce_BNB(1)  # %
    
    
                # print("Triangle finishd.")
                # print(" ")
                    # sys.exit(0)
                        # arb_profit = 1
                        # arb_profit_save = 1
                        # for i in range(len(arb) - 1):
                        #     transactions_count += 1
                        #     arb_profit *= n_arb.df_fee.at[arb[i], arb[i + 1]]
                        #     arb_profit_save *= df_fee_save.at[arb[i], arb[i + 1]]
                        # print(n_arb.df_fee.at["USDT", "BTC"])
                        # print(n_arb.df_fee.at["BTC", "USDT"])
                        # profit_arr.append(arb_profit)
                        # profit_arr_save.append(arb_profit_save)
                        # print(arb_profit, arb_profit_save, arb_profit - arb_profit_save)
    
                        # i_triangle = ""
                        # for s in range(len(arb) - 1):
                        #     i_triangle += arb[s] + "_"
                        #     n_arb.freq_selected_symbols[arb[s]] += 1
    
                        # i_triangle = i_triangle[:-1]
                        # if i_triangle in n_arb.freq_triangles.keys():
                        #     n_arb.freq_triangles[i_triangle] += 1
                        # else:
                        #     n_arb.freq_triangles[i_triangle] = 1
    
                        # if i_start_symbol in n_arb.freq_start_symbol.keys():
                        #     n_arb.freq_start_symbol[i_start_symbol] += 1
                        # else:
                        #     n_arb.freq_start_symbol[i_start_symbol] = 1
    
                        # print(arb)
                        # print("transaction count:", transactions_count,
                        #       "arb count:", all_arb_count,
                        #       "avg_profit:", avg_profit)
    
                # if int(all_arb_count / 10) == all_arb_count / 10:
                #     n_arb.riport.clear()
                #     i_ssf = dict(sorted(n_arb.freq_selected_symbols.items(), key=lambda item: item[1]))
                #     n_arb.riport.add("Selected symbols:", i_ssf)
                #     n_arb.riport.add_section("Most frequent triangles:")
                #     n_arb.riport.add("Triangles:", n_arb.freq_triangles)
                #     n_arb.riport.add_section("Most frequent start symbols:")
                #     n_arb.riport.add("Symbols:", n_arb.freq_start_symbol)
                #     n_arb.riport.add_section("Profit and transactions:")
                #     n_arb.riport.add("Transaction count:", transactions_count)
                #     n_arb.riport.add("Profit array:", profit_arr)
                #     n_arb.riport.add("Average profit", sum(profit_arr)/len(profit_arr))
                #     n_arb.riport.add("Profit array save:", profit_arr_save)
                #     n_arb.riport.add("Average profit save", sum(profit_arr_save) / len(profit_arr_save))
                #     n_arb.riport.write()
    
            # circle_count += 1
            # if circle_count % 800 == 0:
            #     print("connect :)")
            #     n_arb.refresh_estimated_amount()
            #     n_arb.reduce_BNB(1)
            #     # n_arb.b_client.stream_keepalive()
    
    
            # if circle_count > live_update_val:
            #     circle_count = 0
                # n_arb.riport.stamp_live()
