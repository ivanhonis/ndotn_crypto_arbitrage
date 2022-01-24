import asyncio
from binance.client import Client
# from binance import ThreadedWebsocketManager
from binance import AsyncClient, BinanceSocketManager

import networkx as nx
from collections import defaultdict

import pandas as pd
# from tqdm import tqdm
import numpy as np
import math
import time
from threading import Thread
import textwrap
# import sys
# import datetime
# import csv

from n_riport import n_riport


class n_arbitrage:

    def __init__(self):

        self.spread = 0.12  # %
        self.fee = 0.07  # %
        self.arb_check_delay = 0.2  # arbitrás kereéséek közötti várakozáa 0.5 = 2xmásodpercenként
        self.riport = n_riport()

        self.socket_thread = None
        # self.stop_thread = None

        self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
        self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
        self.b_client = Client(self.api_key, self.api_secret)

        self.fee_mod = 1 - (self.fee / 100)
        self.spread_mod = 1 - (self.spread / 100)

        # self.symbols = ['BTC', 'BNB', 'ETH', 'XRP', 'ADA', 'LINK', 'LTC', 'DOT', 'TRX', 'FTM', 'DOGE', 'BUSD',
        #                 'SOL', 'USDT', 'MATIC', 'EOS', 'ETC', 'BTT', 'LUNA', 'NEO', 'XLM', 'AVAX', 'ENJ', 'WAVES',
        #                 'ATOM', 'ONE', 'ALGO', 'SXP', 'SAND', 'SHIB', 'ZEC', 'BAT', 'MANA', 'ONT', 'HOT', 'VET',
        #                 'CHZ', 'WIN', 'BCH', 'AXS', 'GALA', 'GTO', 'ANKR', 'RUNE', 'CAKE', 'AAVE', 'ICP', 'LRC',
        #                 'BCPT', 'ZIL', 'THETA', 'PAX', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI',
        #                 'GRT', 'FIS', 'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'NULS', 'ADX',
        #                 'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC', 'IOST', 'NANO',
        #                 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN', 'USDC', 'BCHSV', 'PHB', 'COCOS',
        #                 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI', 'SRM', 'KSM', 'SUSHI', 'BEL', 'NEAR', 'SLP',
        #                 'REEF', 'C98', 'MINA', 'LAZIO', 'VOXEL']

        self.symbols = ['BTC', 'BNB', 'ETH', 'ADA', 'LINK', 'DOT', 'TRX', 'FTM', 'BUSD', 'SOL',
                        'USDT', 'MATIC', 'ETC', 'NEO', 'ENJ', 'WAVES', 'ATOM', 'ONE', 'ZEC', 'MANA',
                        'ONT', 'HOT', 'CHZ', 'WIN', 'AXS', 'GALA', 'ANKR', 'RUNE', 'ICP', 'LRC',
                        'ZIL', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS',
                        'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'NULS', 'ADX',
                        'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC',
                        'IOST', 'NANO', 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN',
                        'USDC', 'BCHSV', 'PHB', 'COCOS', 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI',
                        'SRM', 'KSM', 'SUSHI', 'BEL', 'NEAR', 'SLP', 'REEF', 'C98', 'MINA', 'LAZIO', 'VOXEL']

        self.all_pairs = self.defa_all_pairs()

        self.selected_symbols = self.symbols[:50]  ## kiválasztam amivel dolgozok
        self.selected_pairs = self.defa_selected_pairs()  ##a kiválasztott szimbólumokhoz kapcsolódó párokat kiválasztom

        self.freq_selected_symbols = self.defa_symbols_frequency()  ## a symbólum gyakoriság méréséhez előkészítem a dictionarit
        self.freq_triangles = {}
        self.freq_start_symbol = {}

        self.df = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        self.df = self.df.astype(float)

        self.df_fee = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        self.df_fee = self.df.astype(float)

        # self.df_sim_price = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        # self.df_sim_price = self.df.astype(float)

        self.socket_list = self.defa_socket_list()

        self.riport.live_text = self.get_settings()
        self.riport.stamp_live()

    def get_settings(self):
        i_r = ""
        # i_r += "Fee: " + str(self.fee) + "%" + "\n"
        i_r += "Spread: " + str(self.spread) + "%" + "\n"
        i_r += "Arbitrage check delay: " + str(self.arb_check_delay) + " sec" + "\n"
        i_r += "Selected symbols: " + textwrap.fill(str(self.selected_symbols), width=80) + "\n"
        return i_r

    def defa_all_pairs(self):
        i_all_pairs = []
        for sy in self.b_client.get_exchange_info()['symbols']:
            i_all_pairs.append(sy['symbol'])
        return i_all_pairs

    def defa_socket_list(self):
        i_socket_list = []
        for sp in tuple(self.selected_pairs.keys()):
            i_socket_list.append(sp.lower() + '@bookTicker')
        return i_socket_list

    def defa_symbols_frequency(self):
        i_selected_symbols_frequency = {}
        for fr in self.selected_symbols:
            i_selected_symbols_frequency[fr] = 0
        i_selected_symbols_frequency[-1] = 0
        return i_selected_symbols_frequency

    def defa_selected_pairs(self):
        i_selected_pairs = {}
        for si1 in self.selected_symbols:
            for si2 in self.selected_symbols:
                if si1 + si2 in self.all_pairs:
                    i_selected_pairs[si1 + si2] = [si1, si2]
        return i_selected_pairs

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


    def arb_find(self):

        """
        Looks for arbitrage opportunities within a snapshot, i.e negative-weight cycles
        that include the currencies given in the sources list

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

        if nx.negative_edge_cycle(g):
            return self.arb_all_negative_cycles(g)
        else:
            return []

    # def stop_all_sockets(self):
    #     # print("stop 1")
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

                self.df[s1][s2] = round(float(res['data']['b']) * self.spread_mod, 8)
                self.df[s2][s1] = round((1 / float(res['data']['a'])) * self.spread_mod, 8)

                self.df_fee[s1][s2] = round(float(res['data']['b']) * self.fee_mod, 8)
                self.df_fee[s2][s1] = round((1 / float(res['data']['a'])) * self.fee_mod, 8)

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
    live_update = 10  #minuta
    n_arb.start()
    n_arb.riport.create("A1_symbol_frequency",
                        "Arbitrazsban erintett symbol-ok gyakorisaga, leggyakoribb tirangles, leggyakoribb kezdo symbol-ok")

    all_arb_count = 0
    profit_arr = []
    profit_arr_save = []
    transactions_count = 0
    circle_count = 0
    live_update_val = int((1/n_arb.arb_check_delay) * 60 * live_update)
    while True:

        time.sleep(n_arb.arb_check_delay)
        # print(n_arb.df)
        df_fee_save = n_arb.df_fee.copy()
        arb_result = n_arb.arb_find()
        if len(arb_result) > 0:

            i_start_symbol = ""

            for arb in arb_result:
                arb = arb[::-1]  ## pozitív ciklusra kell fordítani !!!! FONTOS
                i_start_symbol = str(arb[0])
                if arb[0] != -1:
                    arb_profit = 1
                    arb_profit_save = 1
                    for i in range(len(arb) - 1):
                        transactions_count += 1
                        arb_profit *= n_arb.df_fee.at[arb[i], arb[i + 1]]
                        arb_profit_save *= df_fee_save.at[arb[i], arb[i + 1]]
                    # print(n_arb.df_fee.at["USDT", "BTC"])
                    # print(n_arb.df_fee.at["BTC", "USDT"])
                    profit_arr.append(arb_profit)
                    profit_arr_save.append(arb_profit_save)
                    # print(arb_profit, arb_profit_save, arb_profit - arb_profit_save)

                    i_triangle = ""
                    for s in range(len(arb) - 1):
                        i_triangle += arb[s] + "_"
                        n_arb.freq_selected_symbols[arb[s]] += 1

                    i_triangle = i_triangle[:-1]
                    if i_triangle in n_arb.freq_triangles.keys():
                        n_arb.freq_triangles[i_triangle] += 1
                    else:
                        n_arb.freq_triangles[i_triangle] = 1

                    if i_start_symbol in n_arb.freq_start_symbol.keys():
                        n_arb.freq_start_symbol[i_start_symbol] += 1
                    else:
                        n_arb.freq_start_symbol[i_start_symbol] = 1

                    # print(arb)
                    # print("transaction count:", transactions_count,
                    #       "arb count:", all_arb_count,
                    #       "avg_profit:", avg_profit)

            if int(all_arb_count / 10) == all_arb_count / 10:
                n_arb.riport.clear()
                i_ssf = dict(sorted(n_arb.freq_selected_symbols.items(), key=lambda item: item[1]))
                n_arb.riport.add("Selected symbols:", i_ssf)
                n_arb.riport.add_section("Most frequent triangles:")
                n_arb.riport.add("Triangles:", n_arb.freq_triangles)
                n_arb.riport.add_section("Most frequent start symbols:")
                n_arb.riport.add("Symbols:", n_arb.freq_start_symbol)
                n_arb.riport.add_section("Profit and transactions:")
                n_arb.riport.add("Transaction count:", transactions_count)
                n_arb.riport.add("Profit array:", profit_arr)
                n_arb.riport.add("Average profit", sum(profit_arr)/len(profit_arr))
                n_arb.riport.add("Profit array save:", profit_arr_save)
                n_arb.riport.add("Average profit save", sum(profit_arr_save) / len(profit_arr_save))
                n_arb.riport.write()

        circle_count += 1
        if circle_count > live_update_val:
            circle_count = 0
            n_arb.riport.stamp_live()
