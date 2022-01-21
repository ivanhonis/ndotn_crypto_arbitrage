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
# import sys
# import datetime
# import csv

from n_riport import n_riport


class n_arbitrage:

    def __init__(self):

        self.fee = 0.1  # %
        self.arb_check_delay = 0.2  # arbitrás kereéséek közötti várakozáa 0.5 = 2xmásodpercenként

        self.riport = n_riport()

        # self.speed_defa = {}
        # for i in range(70):
        #     self.speed_defa[i] = 0
        # self.speed_defa[-1] = 0
        # self.speed_defa[-2] = 0
        # self.speed_data = self.speed_defa.copy()
        # self.speed_find_arb = self.speed_defa.copy()

        self.run_restart_stream = True

        # self.stream_error = False
        # self.multiplex_socket = None
        self.socket_thread = None
        # self.stop_thread = None

        self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
        self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
        self.b_client = Client(self.api_key, self.api_secret)

        self.fee_bid = 1 - (self.fee / 100)
        self.fee_ask = 1 + (self.fee / 100)

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

        self.all_pairs = ['ETHBTC', 'LTCBTC', 'BNBBTC', 'NEOBTC', 'QTUMETH', 'EOSETH', 'SNTETH', 'BNTETH', 'BCCBTC',
                          'GASBTC', 'BNBETH', 'BTCUSDT', 'ETHUSDT', 'HSRBTC', 'OAXETH', 'DNTETH', 'MCOETH', 'ICNETH',
                          'MCOBTC', 'WTCBTC', 'WTCETH', 'LRCBTC', 'LRCETH', 'QTUMBTC', 'YOYOBTC', 'OMGBTC', 'OMGETH',
                          'ZRXBTC', 'ZRXETH', 'STRATBTC', 'STRATETH', 'SNGLSBTC', 'SNGLSETH', 'BQXBTC', 'BQXETH',
                          'KNCBTC', 'KNCETH', 'FUNBTC', 'FUNETH', 'SNMBTC', 'SNMETH', 'NEOETH', 'IOTABTC', 'IOTAETH',
                          'LINKBTC', 'LINKETH', 'XVGBTC', 'XVGETH', 'SALTBTC', 'SALTETH', 'MDABTC', 'MDAETH', 'MTLBTC',
                          'MTLETH', 'SUBBTC', 'SUBETH', 'EOSBTC', 'SNTBTC', 'ETCETH', 'ETCBTC', 'MTHBTC', 'MTHETH',
                          'ENGBTC', 'ENGETH', 'DNTBTC', 'ZECBTC', 'ZECETH', 'BNTBTC', 'ASTBTC', 'ASTETH', 'DASHBTC',
                          'DASHETH', 'OAXBTC', 'ICNBTC', 'BTGBTC', 'BTGETH', 'EVXBTC', 'EVXETH', 'REQBTC', 'REQETH',
                          'VIBBTC', 'VIBETH', 'HSRETH', 'TRXBTC', 'TRXETH', 'POWRBTC', 'POWRETH', 'ARKBTC', 'ARKETH',
                          'YOYOETH', 'XRPBTC', 'XRPETH', 'MODBTC', 'MODETH', 'ENJBTC', 'ENJETH', 'STORJBTC', 'STORJETH',
                          'BNBUSDT', 'VENBNB', 'YOYOBNB', 'POWRBNB', 'VENBTC', 'VENETH', 'KMDBTC', 'KMDETH', 'NULSBNB',
                          'RCNBTC', 'RCNETH', 'RCNBNB', 'NULSBTC', 'NULSETH', 'RDNBTC', 'RDNETH', 'RDNBNB', 'XMRBTC',
                          'XMRETH', 'DLTBNB', 'WTCBNB', 'DLTBTC', 'DLTETH', 'AMBBTC', 'AMBETH', 'AMBBNB', 'BCCETH',
                          'BCCUSDT', 'BCCBNB', 'BATBTC', 'BATETH', 'BATBNB', 'BCPTBTC', 'BCPTETH', 'BCPTBNB', 'ARNBTC',
                          'ARNETH', 'GVTBTC', 'GVTETH', 'CDTBTC', 'CDTETH', 'GXSBTC', 'GXSETH', 'NEOUSDT', 'NEOBNB',
                          'POEBTC', 'POEETH', 'QSPBTC', 'QSPETH', 'QSPBNB', 'BTSBTC', 'BTSETH', 'BTSBNB', 'XZCBTC',
                          'XZCETH', 'XZCBNB', 'LSKBTC', 'LSKETH', 'LSKBNB', 'TNTBTC', 'TNTETH', 'FUELBTC', 'FUELETH',
                          'MANABTC', 'MANAETH', 'BCDBTC', 'BCDETH', 'DGDBTC', 'DGDETH', 'IOTABNB', 'ADXBTC', 'ADXETH',
                          'ADXBNB', 'ADABTC', 'ADAETH', 'PPTBTC', 'PPTETH', 'CMTBTC', 'CMTETH', 'CMTBNB', 'XLMBTC',
                          'XLMETH', 'XLMBNB', 'CNDBTC', 'CNDETH', 'CNDBNB', 'LENDBTC', 'LENDETH', 'WABIBTC', 'WABIETH',
                          'WABIBNB', 'LTCETH', 'LTCUSDT', 'LTCBNB', 'TNBBTC', 'TNBETH', 'WAVESBTC', 'WAVESETH',
                          'WAVESBNB', 'GTOBTC', 'GTOETH', 'GTOBNB', 'ICXBTC', 'ICXETH', 'ICXBNB', 'OSTBTC', 'OSTETH',
                          'OSTBNB', 'ELFBTC', 'ELFETH', 'AIONBTC', 'AIONETH', 'AIONBNB', 'NEBLBTC', 'NEBLETH',
                          'NEBLBNB', 'BRDBTC', 'BRDETH', 'BRDBNB', 'MCOBNB', 'EDOBTC', 'EDOETH', 'WINGSBTC', 'WINGSETH',
                          'NAVBTC', 'NAVETH', 'NAVBNB', 'LUNBTC', 'LUNETH', 'TRIGBTC', 'TRIGETH', 'TRIGBNB', 'APPCBTC',
                          'APPCETH', 'APPCBNB', 'VIBEBTC', 'VIBEETH', 'RLCBTC', 'RLCETH', 'RLCBNB', 'INSBTC', 'INSETH',
                          'PIVXBTC', 'PIVXETH', 'PIVXBNB', 'IOSTBTC', 'IOSTETH', 'CHATBTC', 'CHATETH', 'STEEMBTC',
                          'STEEMETH', 'STEEMBNB', 'NANOBTC', 'NANOETH', 'NANOBNB', 'VIABTC', 'VIAETH', 'VIABNB',
                          'BLZBTC', 'BLZETH', 'BLZBNB', 'AEBTC', 'AEETH', 'AEBNB', 'RPXBTC', 'RPXETH', 'RPXBNB',
                          'NCASHBTC', 'NCASHETH', 'NCASHBNB', 'POABTC', 'POAETH', 'POABNB', 'ZILBTC', 'ZILETH',
                          'ZILBNB', 'ONTBTC', 'ONTETH', 'ONTBNB', 'STORMBTC', 'STORMETH', 'STORMBNB', 'QTUMBNB',
                          'QTUMUSDT', 'XEMBTC', 'XEMETH', 'XEMBNB', 'WANBTC', 'WANETH', 'WANBNB', 'WPRBTC', 'WPRETH',
                          'QLCBTC', 'QLCETH', 'SYSBTC', 'SYSETH', 'SYSBNB', 'QLCBNB', 'GRSBTC', 'GRSETH', 'ADAUSDT',
                          'ADABNB', 'CLOAKBTC', 'CLOAKETH', 'GNTBTC', 'GNTETH', 'GNTBNB', 'LOOMBTC', 'LOOMETH',
                          'LOOMBNB', 'XRPUSDT', 'BCNBTC', 'BCNETH', 'BCNBNB', 'REPBTC', 'REPETH', 'REPBNB', 'BTCTUSD',
                          'TUSDBTC', 'ETHTUSD', 'TUSDETH', 'TUSDBNB', 'ZENBTC', 'ZENETH', 'ZENBNB', 'SKYBTC', 'SKYETH',
                          'SKYBNB', 'EOSUSDT', 'EOSBNB', 'CVCBTC', 'CVCETH', 'CVCBNB', 'THETABTC', 'THETAETH',
                          'THETABNB', 'XRPBNB', 'TUSDUSDT', 'IOTAUSDT', 'XLMUSDT', 'IOTXBTC', 'IOTXETH', 'QKCBTC',
                          'QKCETH', 'AGIBTC', 'AGIETH', 'AGIBNB', 'NXSBTC', 'NXSETH', 'NXSBNB', 'ENJBNB', 'DATABTC',
                          'DATAETH', 'ONTUSDT', 'TRXBNB', 'TRXUSDT', 'ETCUSDT', 'ETCBNB', 'ICXUSDT', 'SCBTC', 'SCETH',
                          'SCBNB', 'NPXSBTC', 'NPXSETH', 'VENUSDT', 'KEYBTC', 'KEYETH', 'NASBTC', 'NASETH', 'NASBNB',
                          'MFTBTC', 'MFTETH', 'MFTBNB', 'DENTBTC', 'DENTETH', 'ARDRBTC', 'ARDRETH', 'ARDRBNB',
                          'NULSUSDT', 'HOTBTC', 'HOTETH', 'VETBTC', 'VETETH', 'VETUSDT', 'VETBNB', 'DOCKBTC', 'DOCKETH',
                          'POLYBTC', 'POLYBNB', 'PHXBTC', 'PHXETH', 'PHXBNB', 'HCBTC', 'HCETH', 'GOBTC', 'GOBNB',
                          'PAXBTC', 'PAXBNB', 'PAXUSDT', 'PAXETH', 'RVNBTC', 'RVNBNB', 'DCRBTC', 'DCRBNB', 'USDCBNB',
                          'MITHBTC', 'MITHBNB', 'BCHABCBTC', 'BCHSVBTC', 'BCHABCUSDT', 'BCHSVUSDT', 'BNBPAX', 'BTCPAX',
                          'ETHPAX', 'XRPPAX', 'EOSPAX', 'XLMPAX', 'RENBTC', 'RENBNB', 'BNBTUSD', 'XRPTUSD', 'EOSTUSD',
                          'XLMTUSD', 'BNBUSDC', 'BTCUSDC', 'ETHUSDC', 'XRPUSDC', 'EOSUSDC', 'XLMUSDC', 'USDCUSDT',
                          'ADATUSD', 'TRXTUSD', 'NEOTUSD', 'TRXXRP', 'XZCXRP', 'PAXTUSD', 'USDCTUSD', 'USDCPAX',
                          'LINKUSDT', 'LINKTUSD', 'LINKPAX', 'LINKUSDC', 'WAVESUSDT', 'WAVESTUSD', 'WAVESPAX',
                          'WAVESUSDC', 'BCHABCTUSD', 'BCHABCPAX', 'BCHABCUSDC', 'BCHSVTUSD', 'BCHSVPAX', 'BCHSVUSDC',
                          'LTCTUSD', 'LTCPAX', 'LTCUSDC', 'TRXPAX', 'TRXUSDC', 'BTTBTC', 'BTTBNB', 'BTTUSDT', 'BNBUSDS',
                          'BTCUSDS', 'USDSUSDT', 'USDSPAX', 'USDSTUSD', 'USDSUSDC', 'BTTPAX', 'BTTTUSD', 'BTTUSDC',
                          'ONGBNB', 'ONGBTC', 'ONGUSDT', 'HOTBNB', 'HOTUSDT', 'ZILUSDT', 'ZRXBNB', 'ZRXUSDT', 'FETBNB',
                          'FETBTC', 'FETUSDT', 'BATUSDT', 'XMRBNB', 'XMRUSDT', 'ZECBNB', 'ZECUSDT', 'ZECPAX', 'ZECTUSD',
                          'ZECUSDC', 'IOSTBNB', 'IOSTUSDT', 'CELRBNB', 'CELRBTC', 'CELRUSDT', 'ADAPAX', 'ADAUSDC',
                          'NEOPAX', 'NEOUSDC', 'DASHBNB', 'DASHUSDT', 'NANOUSDT', 'OMGBNB', 'OMGUSDT', 'THETAUSDT',
                          'ENJUSDT', 'MITHUSDT', 'MATICBNB', 'MATICBTC', 'MATICUSDT', 'ATOMBNB', 'ATOMBTC', 'ATOMUSDT',
                          'ATOMUSDC', 'ATOMPAX', 'ATOMTUSD', 'ETCUSDC', 'ETCPAX', 'ETCTUSD', 'BATUSDC', 'BATPAX',
                          'BATTUSD', 'PHBBNB', 'PHBBTC', 'PHBUSDC', 'PHBTUSD', 'PHBPAX', 'TFUELBNB', 'TFUELBTC',
                          'TFUELUSDT', 'TFUELUSDC', 'TFUELTUSD', 'TFUELPAX', 'ONEBNB', 'ONEBTC', 'ONEUSDT', 'ONETUSD',
                          'ONEPAX', 'ONEUSDC', 'FTMBNB', 'FTMBTC', 'FTMUSDT', 'FTMTUSD', 'FTMPAX', 'FTMUSDC', 'BTCBBTC',
                          'BCPTTUSD', 'BCPTPAX', 'BCPTUSDC', 'ALGOBNB', 'ALGOBTC', 'ALGOUSDT', 'ALGOTUSD', 'ALGOPAX',
                          'ALGOUSDC', 'USDSBUSDT', 'USDSBUSDS', 'GTOUSDT', 'GTOPAX', 'GTOTUSD', 'GTOUSDC', 'ERDBNB',
                          'ERDBTC', 'ERDUSDT', 'ERDPAX', 'ERDUSDC', 'DOGEBNB', 'DOGEBTC', 'DOGEUSDT', 'DOGEPAX',
                          'DOGEUSDC', 'DUSKBNB', 'DUSKBTC', 'DUSKUSDT', 'DUSKUSDC', 'DUSKPAX', 'BGBPUSDC', 'ANKRBNB',
                          'ANKRBTC', 'ANKRUSDT', 'ANKRTUSD', 'ANKRPAX', 'ANKRUSDC', 'ONTPAX', 'ONTUSDC', 'WINBNB',
                          'WINBTC', 'WINUSDT', 'WINUSDC', 'COSBNB', 'COSBTC', 'COSUSDT', 'TUSDBTUSD', 'NPXSUSDT',
                          'NPXSUSDC', 'COCOSBNB', 'COCOSBTC', 'COCOSUSDT', 'MTLUSDT', 'TOMOBNB', 'TOMOBTC', 'TOMOUSDT',
                          'TOMOUSDC', 'PERLBNB', 'PERLBTC', 'PERLUSDC', 'PERLUSDT', 'DENTUSDT', 'MFTUSDT', 'KEYUSDT',
                          'STORMUSDT', 'DOCKUSDT', 'WANUSDT', 'FUNUSDT', 'CVCUSDT', 'BTTTRX', 'WINTRX', 'CHZBNB',
                          'CHZBTC', 'CHZUSDT', 'BANDBNB', 'BANDBTC', 'BANDUSDT', 'BNBBUSD', 'BTCBUSD', 'BUSDUSDT',
                          'BEAMBNB', 'BEAMBTC', 'BEAMUSDT', 'XTZBNB', 'XTZBTC', 'XTZUSDT', 'RENUSDT', 'RVNUSDT',
                          'HCUSDT', 'HBARBNB', 'HBARBTC', 'HBARUSDT', 'NKNBNB', 'NKNBTC', 'NKNUSDT', 'XRPBUSD',
                          'ETHBUSD', 'BCHABCBUSD', 'LTCBUSD', 'LINKBUSD', 'ETCBUSD', 'STXBNB', 'STXBTC', 'STXUSDT',
                          'KAVABNB', 'KAVABTC', 'KAVAUSDT', 'BUSDNGN', 'BNBNGN', 'BTCNGN', 'ARPABNB', 'ARPABTC',
                          'ARPAUSDT', 'TRXBUSD', 'EOSBUSD', 'IOTXUSDT', 'RLCUSDT', 'MCOUSDT', 'XLMBUSD', 'ADABUSD',
                          'CTXCBNB', 'CTXCBTC', 'CTXCUSDT', 'BCHBNB', 'BCHBTC', 'BCHUSDT', 'BCHUSDC', 'BCHTUSD',
                          'BCHPAX', 'BCHBUSD', 'BTCRUB', 'ETHRUB', 'XRPRUB', 'BNBRUB', 'TROYBNB', 'TROYBTC', 'TROYUSDT',
                          'BUSDRUB', 'QTUMBUSD', 'VETBUSD', 'VITEBNB', 'VITEBTC', 'VITEUSDT', 'FTTBNB', 'FTTBTC',
                          'FTTUSDT', 'BTCTRY', 'BNBTRY', 'BUSDTRY', 'ETHTRY', 'XRPTRY', 'USDTTRY', 'USDTRUB', 'BTCEUR',
                          'ETHEUR', 'BNBEUR', 'XRPEUR', 'EURBUSD', 'EURUSDT', 'OGNBNB', 'OGNBTC', 'OGNUSDT', 'DREPBNB',
                          'DREPBTC', 'DREPUSDT', 'BULLUSDT', 'BULLBUSD', 'BEARUSDT', 'BEARBUSD', 'ETHBULLUSDT',
                          'ETHBULLBUSD', 'ETHBEARUSDT', 'ETHBEARBUSD', 'TCTBNB', 'TCTBTC', 'TCTUSDT', 'WRXBNB',
                          'WRXBTC', 'WRXUSDT', 'ICXBUSD', 'BTSUSDT', 'BTSBUSD', 'LSKUSDT', 'BNTUSDT', 'BNTBUSD',
                          'LTOBNB', 'LTOBTC', 'LTOUSDT', 'ATOMBUSD', 'DASHBUSD', 'NEOBUSD', 'WAVESBUSD', 'XTZBUSD',
                          'EOSBULLUSDT', 'EOSBULLBUSD', 'EOSBEARUSDT', 'EOSBEARBUSD', 'XRPBULLUSDT', 'XRPBULLBUSD',
                          'XRPBEARUSDT', 'XRPBEARBUSD', 'BATBUSD', 'ENJBUSD', 'NANOBUSD', 'ONTBUSD', 'RVNBUSD',
                          'STRATBUSD', 'STRATBNB', 'STRATUSDT', 'AIONBUSD', 'AIONUSDT', 'MBLBNB', 'MBLBTC', 'MBLUSDT',
                          'COTIBNB', 'COTIBTC', 'COTIUSDT', 'ALGOBUSD', 'BTTBUSD', 'TOMOBUSD', 'XMRBUSD', 'ZECBUSD',
                          'BNBBULLUSDT', 'BNBBULLBUSD', 'BNBBEARUSDT', 'BNBBEARBUSD', 'STPTBNB', 'STPTBTC', 'STPTUSDT',
                          'BTCZAR', 'ETHZAR', 'BNBZAR', 'USDTZAR', 'BUSDZAR', 'BTCBKRW', 'ETHBKRW', 'BNBBKRW',
                          'WTCUSDT', 'DATABUSD', 'DATAUSDT', 'XZCUSDT', 'SOLBNB', 'SOLBTC', 'SOLUSDT', 'SOLBUSD',
                          'BTCIDRT', 'BNBIDRT', 'USDTIDRT', 'BUSDIDRT', 'CTSIBTC', 'CTSIUSDT', 'CTSIBNB', 'CTSIBUSD',
                          'HIVEBNB', 'HIVEBTC', 'HIVEUSDT', 'CHRBNB', 'CHRBTC', 'CHRUSDT', 'BTCUPUSDT', 'BTCDOWNUSDT',
                          'GXSUSDT', 'ARDRUSDT', 'ERDBUSD', 'LENDUSDT', 'HBARBUSD', 'MATICBUSD', 'WRXBUSD', 'ZILBUSD',
                          'MDTBNB', 'MDTBTC', 'MDTUSDT', 'STMXBNB', 'STMXBTC', 'STMXETH', 'STMXUSDT', 'KNCBUSD',
                          'KNCUSDT', 'REPBUSD', 'REPUSDT', 'LRCBUSD', 'LRCUSDT', 'IQBNB', 'IQBUSD', 'PNTBTC', 'PNTUSDT',
                          'BTCGBP', 'ETHGBP', 'XRPGBP', 'BNBGBP', 'GBPBUSD', 'DGBBNB', 'DGBBTC', 'DGBBUSD', 'BTCUAH',
                          'USDTUAH', 'COMPBTC', 'COMPBNB', 'COMPBUSD', 'COMPUSDT', 'BTCBIDR', 'ETHBIDR', 'BNBBIDR',
                          'BUSDBIDR', 'USDTBIDR', 'BKRWUSDT', 'BKRWBUSD', 'SCUSDT', 'ZENUSDT', 'SXPBTC', 'SXPBNB',
                          'SXPBUSD', 'SNXBTC', 'SNXBNB', 'SNXBUSD', 'SNXUSDT', 'ETHUPUSDT', 'ETHDOWNUSDT', 'ADAUPUSDT',
                          'ADADOWNUSDT', 'LINKUPUSDT', 'LINKDOWNUSDT', 'VTHOBNB', 'VTHOBUSD', 'VTHOUSDT', 'DCRBUSD',
                          'DGBUSDT', 'GBPUSDT', 'STORJBUSD', 'SXPUSDT', 'IRISBNB', 'IRISBTC', 'IRISBUSD', 'MKRBNB',
                          'MKRBTC', 'MKRUSDT', 'MKRBUSD', 'DAIBNB', 'DAIBTC', 'DAIUSDT', 'DAIBUSD', 'RUNEBNB',
                          'RUNEBTC', 'RUNEBUSD', 'MANABUSD', 'DOGEBUSD', 'LENDBUSD', 'ZRXBUSD', 'DCRUSDT', 'STORJUSDT',
                          'XRPBKRW', 'ADABKRW', 'BTCAUD', 'ETHAUD', 'AUDBUSD', 'FIOBNB', 'FIOBTC', 'FIOBUSD',
                          'BNBUPUSDT', 'BNBDOWNUSDT', 'XTZUPUSDT', 'XTZDOWNUSDT', 'AVABNB', 'AVABTC', 'AVABUSD',
                          'USDTBKRW', 'BUSDBKRW', 'IOTABUSD', 'MANAUSDT', 'XRPAUD', 'BNBAUD', 'AUDUSDT', 'BALBNB',
                          'BALBTC', 'BALBUSD', 'YFIBNB', 'YFIBTC', 'YFIBUSD', 'YFIUSDT', 'BLZBUSD', 'KMDBUSD',
                          'BALUSDT', 'BLZUSDT', 'IRISUSDT', 'KMDUSDT', 'BTCDAI', 'ETHDAI', 'BNBDAI', 'USDTDAI',
                          'BUSDDAI', 'JSTBNB', 'JSTBTC', 'JSTBUSD', 'JSTUSDT', 'SRMBNB', 'SRMBTC', 'SRMBUSD', 'SRMUSDT',
                          'ANTBNB', 'ANTBTC', 'ANTBUSD', 'ANTUSDT', 'CRVBNB', 'CRVBTC', 'CRVBUSD', 'CRVUSDT', 'SANDBNB',
                          'SANDBTC', 'SANDUSDT', 'SANDBUSD', 'OCEANBNB', 'OCEANBTC', 'OCEANBUSD', 'OCEANUSDT', 'NMRBNB',
                          'NMRBTC', 'NMRBUSD', 'NMRUSDT', 'DOTBNB', 'DOTBTC', 'DOTBUSD', 'DOTUSDT', 'LUNABNB',
                          'LUNABTC', 'LUNABUSD', 'LUNAUSDT', 'IDEXBTC', 'IDEXBUSD', 'RSRBNB', 'RSRBTC', 'RSRBUSD',
                          'RSRUSDT', 'PAXGBNB', 'PAXGBTC', 'PAXGBUSD', 'PAXGUSDT', 'WNXMBNB', 'WNXMBTC', 'WNXMBUSD',
                          'WNXMUSDT', 'TRBBNB', 'TRBBTC', 'TRBBUSD', 'TRBUSDT', 'ETHNGN', 'DOTBIDR', 'LINKAUD',
                          'SXPAUD', 'BZRXBNB', 'BZRXBTC', 'BZRXBUSD', 'BZRXUSDT', 'WBTCBTC', 'WBTCETH', 'SUSHIBNB',
                          'SUSHIBTC', 'SUSHIBUSD', 'SUSHIUSDT', 'YFIIBNB', 'YFIIBTC', 'YFIIBUSD', 'YFIIUSDT', 'KSMBNB',
                          'KSMBTC', 'KSMBUSD', 'KSMUSDT', 'EGLDBNB', 'EGLDBTC', 'EGLDBUSD', 'EGLDUSDT', 'DIABNB',
                          'DIABTC', 'DIABUSD', 'DIAUSDT', 'RUNEUSDT', 'FIOUSDT', 'UMABTC', 'UMAUSDT', 'EOSUPUSDT',
                          'EOSDOWNUSDT', 'TRXUPUSDT', 'TRXDOWNUSDT', 'XRPUPUSDT', 'XRPDOWNUSDT', 'DOTUPUSDT',
                          'DOTDOWNUSDT', 'SRMBIDR', 'ONEBIDR', 'LINKTRY', 'USDTNGN', 'BELBNB', 'BELBTC', 'BELBUSD',
                          'BELUSDT', 'WINGBNB', 'WINGBTC', 'SWRVBNB', 'SWRVBUSD', 'WINGBUSD', 'WINGUSDT', 'LTCUPUSDT',
                          'LTCDOWNUSDT', 'LENDBKRW', 'SXPEUR', 'CREAMBNB', 'CREAMBUSD', 'UNIBNB', 'UNIBTC', 'UNIBUSD',
                          'UNIUSDT', 'NBSBTC', 'NBSUSDT', 'OXTBTC', 'OXTUSDT', 'SUNBTC', 'SUNUSDT', 'AVAXBNB',
                          'AVAXBTC', 'AVAXBUSD', 'AVAXUSDT', 'HNTBTC', 'HNTUSDT', 'BAKEBNB', 'BURGERBNB', 'SXPBIDR',
                          'LINKBKRW', 'FLMBNB', 'FLMBTC', 'FLMBUSD', 'FLMUSDT', 'SCRTBTC', 'SCRTETH', 'CAKEBNB',
                          'CAKEBUSD', 'SPARTABNB', 'UNIUPUSDT', 'UNIDOWNUSDT', 'ORNBTC', 'ORNUSDT', 'TRXNGN', 'SXPTRY',
                          'UTKBTC', 'UTKUSDT', 'XVSBNB', 'XVSBTC', 'XVSBUSD', 'XVSUSDT', 'ALPHABNB', 'ALPHABTC',
                          'ALPHABUSD', 'ALPHAUSDT', 'VIDTBTC', 'VIDTBUSD', 'AAVEBNB', 'BTCBRL', 'USDTBRL', 'AAVEBTC',
                          'AAVEETH', 'AAVEBUSD', 'AAVEUSDT', 'AAVEBKRW', 'NEARBNB', 'NEARBTC', 'NEARBUSD', 'NEARUSDT',
                          'SXPUPUSDT', 'SXPDOWNUSDT', 'DOTBKRW', 'SXPGBP', 'FILBNB', 'FILBTC', 'FILBUSD', 'FILUSDT',
                          'FILUPUSDT', 'FILDOWNUSDT', 'YFIUPUSDT', 'YFIDOWNUSDT', 'INJBNB', 'INJBTC', 'INJBUSD',
                          'INJUSDT', 'AERGOBTC', 'AERGOBUSD', 'LINKEUR', 'ONEBUSD', 'EASYETH', 'AUDIOBTC', 'AUDIOBUSD',
                          'AUDIOUSDT', 'CTKBNB', 'CTKBTC', 'CTKBUSD', 'CTKUSDT', 'BCHUPUSDT', 'BCHDOWNUSDT', 'BOTBTC',
                          'BOTBUSD', 'ETHBRL', 'DOTEUR', 'AKROBTC', 'AKROUSDT', 'KP3RBNB', 'KP3RBUSD', 'AXSBNB',
                          'AXSBTC', 'AXSBUSD', 'AXSUSDT', 'HARDBNB', 'HARDBTC', 'HARDBUSD', 'HARDUSDT', 'BNBBRL',
                          'LTCEUR', 'RENBTCBTC', 'RENBTCETH', 'DNTBUSD', 'DNTUSDT', 'SLPETH', 'ADAEUR', 'LTCNGN',
                          'CVPETH', 'CVPBUSD', 'STRAXBTC', 'STRAXETH', 'STRAXBUSD', 'STRAXUSDT', 'FORBTC', 'FORBUSD',
                          'UNFIBNB', 'UNFIBTC', 'UNFIBUSD', 'UNFIUSDT', 'FRONTETH', 'FRONTBUSD', 'BCHABUSD', 'ROSEBTC',
                          'ROSEBUSD', 'ROSEUSDT', 'AVAXTRY', 'BUSDBRL', 'AVAUSDT', 'SYSBUSD', 'XEMUSDT', 'HEGICETH',
                          'HEGICBUSD', 'AAVEUPUSDT', 'AAVEDOWNUSDT', 'PROMBNB', 'PROMBUSD', 'XRPBRL', 'XRPNGN',
                          'SKLBTC', 'SKLBUSD', 'SKLUSDT', 'BCHEUR', 'YFIEUR', 'ZILBIDR', 'SUSDBTC', 'SUSDETH',
                          'SUSDUSDT', 'COVERETH', 'COVERBUSD', 'GLMBTC', 'GLMETH', 'GHSTETH', 'GHSTBUSD', 'SUSHIUPUSDT',
                          'SUSHIDOWNUSDT', 'XLMUPUSDT', 'XLMDOWNUSDT', 'LINKBRL', 'LINKNGN', 'LTCRUB', 'TRXTRY',
                          'XLMEUR', 'DFETH', 'DFBUSD', 'GRTBTC', 'GRTETH', 'GRTUSDT', 'JUVBTC', 'JUVBUSD', 'JUVUSDT',
                          'PSGBTC', 'PSGBUSD', 'PSGUSDT', 'BUSDBVND', 'USDTBVND', '1INCHBTC', '1INCHUSDT', 'REEFBTC',
                          'REEFUSDT', 'OGBTC', 'OGUSDT', 'ATMBTC', 'ATMUSDT', 'ASRBTC', 'ASRUSDT', 'CELOBTC',
                          'CELOUSDT', 'RIFBTC', 'RIFUSDT', 'CHZTRY', 'XLMTRY', 'LINKGBP', 'GRTEUR', 'BTCSTBTC',
                          'BTCSTBUSD', 'BTCSTUSDT', 'TRUBTC', 'TRUBUSD', 'TRUUSDT', 'DEXEETH', 'DEXEBUSD', 'EOSEUR',
                          'LTCBRL', 'USDCBUSD', 'TUSDBUSD', 'PAXBUSD', 'CKBBTC', 'CKBBUSD', 'CKBUSDT', 'TWTBTC',
                          'TWTBUSD', 'TWTUSDT', 'FIROBTC', 'FIROETH', 'FIROUSDT', 'BETHETH', 'DOGEEUR', 'DOGETRY',
                          'DOGEAUD', 'DOGEBRL', 'DOTNGN', 'PROSETH', 'LITBTC', 'LITBUSD', 'LITUSDT', 'BTCVAI',
                          'BUSDVAI', 'SFPBTC', 'SFPBUSD', 'SFPUSDT', 'DOGEGBP', 'DOTTRY', 'FXSBTC', 'FXSBUSD',
                          'DODOBTC', 'DODOBUSD', 'DODOUSDT', 'FRONTBTC', 'EASYBTC', 'CAKEBTC', 'CAKEUSDT', 'BAKEBUSD',
                          'UFTETH', 'UFTBUSD', '1INCHBUSD', 'BANDBUSD', 'GRTBUSD', 'IOSTBUSD', 'OMGBUSD', 'REEFBUSD',
                          'ACMBTC', 'ACMBUSD', 'ACMUSDT', 'AUCTIONBTC', 'AUCTIONBUSD', 'PHABTC', 'PHABUSD', 'DOTGBP',
                          'ADATRY', 'ADABRL', 'ADAGBP', 'TVKBTC', 'TVKBUSD', 'BADGERBTC', 'BADGERBUSD', 'BADGERUSDT',
                          'FISBTC', 'FISBUSD', 'FISUSDT', 'DOTBRL', 'ADAAUD', 'HOTTRY', 'EGLDEUR', 'OMBTC', 'OMBUSD',
                          'OMUSDT', 'PONDBTC', 'PONDBUSD', 'PONDUSDT', 'DEGOBTC', 'DEGOBUSD', 'DEGOUSDT', 'AVAXEUR',
                          'BTTTRY', 'CHZBRL', 'UNIEUR', 'ALICEBTC', 'ALICEBUSD', 'ALICEUSDT', 'CHZBUSD', 'CHZEUR',
                          'CHZGBP', 'BIFIBNB', 'BIFIBUSD', 'LINABTC', 'LINABUSD', 'LINAUSDT', 'ADARUB', 'ENJBRL',
                          'ENJEUR', 'MATICEUR', 'NEOTRY', 'PERPBTC', 'PERPBUSD', 'PERPUSDT', 'RAMPBTC', 'RAMPBUSD',
                          'RAMPUSDT', 'SUPERBTC', 'SUPERBUSD', 'SUPERUSDT', 'CFXBTC', 'CFXBUSD', 'CFXUSDT', 'ENJGBP',
                          'EOSTRY', 'LTCGBP', 'LUNAEUR', 'RVNTRY', 'THETAEUR', 'XVGBUSD', 'EPSBTC', 'EPSBUSD',
                          'EPSUSDT', 'AUTOBTC', 'AUTOBUSD', 'AUTOUSDT', 'TKOBTC', 'TKOBIDR', 'TKOBUSD', 'TKOUSDT',
                          'PUNDIXETH', 'PUNDIXUSDT', 'BTTBRL', 'BTTEUR', 'HOTEUR', 'WINEUR', 'TLMBTC', 'TLMBUSD',
                          'TLMUSDT', '1INCHUPUSDT', '1INCHDOWNUSDT', 'BTGBUSD', 'BTGUSDT', 'HOTBUSD', 'BNBUAH',
                          'ONTTRY', 'VETEUR', 'VETGBP', 'WINBRL', 'MIRBTC', 'MIRBUSD', 'MIRUSDT', 'BARBTC', 'BARBUSD',
                          'BARUSDT', 'FORTHBTC', 'FORTHBUSD', 'FORTHUSDT', 'CAKEGBP', 'DOGERUB', 'HOTBRL', 'WRXEUR',
                          'EZBTC', 'EZETH', 'BAKEUSDT', 'BURGERBUSD', 'BURGERUSDT', 'SLPBUSD', 'SLPUSDT', 'TRXAUD',
                          'TRXEUR', 'VETTRY', 'SHIBUSDT', 'SHIBBUSD', 'ICPBTC', 'ICPBNB', 'ICPBUSD', 'ICPUSDT',
                          'BTCGYEN', 'USDTGYEN', 'SHIBEUR', 'SHIBRUB', 'ETCEUR', 'ETCBRL', 'DOGEBIDR', 'ARBTC', 'ARBNB',
                          'ARBUSD', 'ARUSDT', 'POLSBTC', 'POLSBNB', 'POLSBUSD', 'POLSUSDT', 'MDXBTC', 'MDXBNB',
                          'MDXBUSD', 'MDXUSDT', 'MASKBNB', 'MASKBUSD', 'MASKUSDT', 'LPTBTC', 'LPTBNB', 'LPTBUSD',
                          'LPTUSDT', 'ETHUAH', 'MATICBRL', 'SOLEUR', 'SHIBBRL', 'AGIXBTC', 'ICPEUR', 'MATICGBP',
                          'SHIBTRY', 'MATICBIDR', 'MATICRUB', 'NUBTC', 'NUBNB', 'NUBUSD', 'NUUSDT', 'XVGUSDT',
                          'RLCBUSD', 'CELRBUSD', 'ATMBUSD', 'ZENBUSD', 'FTMBUSD', 'THETABUSD', 'WINBUSD', 'KAVABUSD',
                          'XEMBUSD', 'ATABTC', 'ATABNB', 'ATABUSD', 'ATAUSDT', 'GTCBTC', 'GTCBNB', 'GTCBUSD', 'GTCUSDT',
                          'TORNBTC', 'TORNBNB', 'TORNBUSD', 'TORNUSDT', 'MATICTRY', 'ETCGBP', 'SOLGBP', 'BAKEBTC',
                          'COTIBUSD', 'KEEPBTC', 'KEEPBNB', 'KEEPBUSD', 'KEEPUSDT', 'SOLTRY', 'RUNEGBP', 'SOLBRL',
                          'SCBUSD', 'CHRBUSD', 'STMXBUSD', 'HNTBUSD', 'FTTBUSD', 'DOCKBUSD', 'ADABIDR', 'ERNBNB',
                          'ERNBUSD', 'ERNUSDT', 'KLAYBTC', 'KLAYBNB', 'KLAYBUSD', 'KLAYUSDT', 'RUNEEUR', 'MATICAUD',
                          'DOTRUB', 'UTKBUSD', 'IOTXBUSD', 'PHAUSDT', 'SOLRUB', 'RUNEAUD', 'BUSDUAH', 'BONDBTC',
                          'BONDBNB', 'BONDBUSD', 'BONDUSDT', 'MLNBTC', 'MLNBNB', 'MLNBUSD', 'MLNUSDT', 'GRTTRY',
                          'CAKEBRL', 'ICPRUB', 'DOTAUD', 'AAVEBRL', 'EOSAUD', 'DEXEUSDT', 'LTOBUSD', 'ADXBUSD',
                          'QUICKBTC', 'QUICKBNB', 'QUICKBUSD', 'C98USDT', 'C98BUSD', 'C98BNB', 'C98BTC', 'CLVBTC',
                          'CLVBNB', 'CLVBUSD', 'CLVUSDT', 'QNTBTC', 'QNTBNB', 'QNTBUSD', 'QNTUSDT', 'FLOWBTC',
                          'FLOWBNB', 'FLOWBUSD', 'FLOWUSDT', 'XECBUSD', 'AXSBRL', 'AXSAUD', 'TVKUSDT', 'MINABTC',
                          'MINABNB', 'MINABUSD', 'MINAUSDT', 'RAYBNB', 'RAYBUSD', 'RAYUSDT', 'FARMBTC', 'FARMBNB',
                          'FARMBUSD', 'FARMUSDT', 'ALPACABTC', 'ALPACABNB', 'ALPACABUSD', 'ALPACAUSDT', 'TLMTRY',
                          'QUICKUSDT', 'ORNBUSD', 'MBOXBTC', 'MBOXBNB', 'MBOXBUSD', 'MBOXUSDT', 'VGXBTC', 'VGXETH',
                          'FORUSDT', 'REQUSDT', 'GHSTUSDT', 'TRURUB', 'FISBRL', 'WAXPUSDT', 'WAXPBUSD', 'WAXPBNB',
                          'WAXPBTC', 'TRIBEBTC', 'TRIBEBNB', 'TRIBEBUSD', 'TRIBEUSDT', 'GNOUSDT', 'GNOBUSD', 'GNOBNB',
                          'GNOBTC', 'ARPATRY', 'PROMBTC', 'MTLBUSD', 'OGNBUSD', 'XECUSDT', 'C98BRL', 'SOLAUD',
                          'SUSHIBIDR', 'XRPBIDR', 'POLYBUSD', 'ELFUSDT', 'DYDXUSDT', 'DYDXBUSD', 'DYDXBNB', 'DYDXBTC',
                          'ELFBUSD', 'POLYUSDT', 'IDEXUSDT', 'VIDTUSDT', 'SOLBIDR', 'AXSBIDR', 'BTCUSDP', 'ETHUSDP',
                          'BNBUSDP', 'USDPBUSD', 'USDPUSDT', 'GALAUSDT', 'GALABUSD', 'GALABNB', 'GALABTC', 'FTMBIDR',
                          'ALGOBIDR', 'CAKEAUD', 'KSMAUD', 'WAVESRUB', 'SUNBUSD', 'ILVUSDT', 'ILVBUSD', 'ILVBNB',
                          'ILVBTC', 'RENBUSD', 'YGGUSDT', 'YGGBUSD', 'YGGBNB', 'YGGBTC', 'STXBUSD', 'SYSUSDT', 'DFUSDT',
                          'SOLUSDC', 'ARPARUB', 'LTCUAH', 'FETBUSD', 'ARPABUSD', 'LSKBUSD', 'AVAXBIDR', 'ALICEBIDR',
                          'FIDAUSDT', 'FIDABUSD', 'FIDABNB', 'FIDABTC', 'DENTBUSD', 'FRONTUSDT', 'CVPUSDT', 'AGLDBTC',
                          'AGLDBNB', 'AGLDBUSD', 'AGLDUSDT', 'RADBTC', 'RADBNB', 'RADBUSD', 'RADUSDT', 'UNIAUD',
                          'HIVEBUSD', 'STPTBUSD', 'BETABTC', 'BETABNB', 'BETABUSD', 'BETAUSDT', 'SHIBAUD', 'RAREBTC',
                          'RAREBNB', 'RAREBUSD', 'RAREUSDT', 'AVAXBRL', 'AVAXAUD', 'LUNAAUD', 'TROYBUSD', 'AXSETH',
                          'FTMETH', 'SOLETH', 'SSVBTC', 'SSVETH', 'LAZIOTRY', 'LAZIOEUR', 'LAZIOBTC', 'LAZIOUSDT',
                          'CHESSBTC', 'CHESSBNB', 'CHESSBUSD', 'CHESSUSDT', 'FTMAUD', 'FTMBRL', 'SCRTBUSD', 'ADXUSDT',
                          'AUCTIONUSDT', 'CELOBUSD', 'FTMRUB', 'NUAUD', 'NURUB', 'REEFTRY', 'REEFBIDR', 'SHIBDOGE',
                          'DARUSDT', 'DARBUSD', 'DARBNB', 'DARBTC', 'BNXBTC', 'BNXBNB', 'BNXBUSD', 'BNXUSDT', 'RGTUSDT',
                          'RGTBTC', 'RGTBUSD', 'RGTBNB', 'LAZIOBUSD', 'OXTBUSD', 'MANATRY', 'ALGORUB', 'SHIBUAH',
                          'LUNABIDR', 'AUDUSDC', 'MOVRBTC', 'MOVRBNB', 'MOVRBUSD', 'MOVRUSDT', 'CITYBTC', 'CITYBNB',
                          'CITYBUSD', 'CITYUSDT', 'ENSBTC', 'ENSBNB', 'ENSBUSD', 'ENSUSDT', 'SANDETH', 'DOTETH',
                          'MATICETH', 'ANKRBUSD', 'SANDTRY', 'MANABRL', 'KP3RUSDT', 'QIUSDT', 'QIBUSD', 'QIBNB',
                          'QIBTC', 'PORTOBTC', 'PORTOUSDT', 'PORTOTRY', 'PORTOEUR', 'POWRUSDT', 'POWRBUSD', 'AVAXETH',
                          'SLPTRY', 'FISTRY', 'LRCTRY', 'CHRETH', 'FISBIDR', 'VGXUSDT', 'GALAETH', 'JASMYUSDT',
                          'JASMYBUSD', 'JASMYBNB', 'JASMYBTC', 'AMPBTC', 'AMPBNB', 'AMPBUSD', 'AMPUSDT', 'PLABTC',
                          'PLABNB', 'PLABUSD', 'PLAUSDT', 'PYRBTC', 'PYRBUSD', 'PYRUSDT', 'RNDRBTC', 'RNDRUSDT',
                          'RNDRBUSD', 'ALCXBTC', 'ALCXBUSD', 'ALCXUSDT', 'SANTOSBTC', 'SANTOSUSDT', 'SANTOSBRL',
                          'SANTOSTRY', 'MCBTC', 'MCBUSD', 'MCUSDT', 'BELTRY', 'COCOSBUSD', 'DENTTRY', 'ENJTRY',
                          'NEORUB', 'SANDAUD', 'SLPBIDR', 'ANYBTC', 'ANYBUSD', 'ANYUSDT', 'BICOBTC', 'BICOBUSD',
                          'BICOUSDT', 'FLUXBTC', 'FLUXBUSD', 'FLUXUSDT', 'ALICETRY', 'FXSUSDT', 'GALABRL', 'GALATRY',
                          'LUNATRY', 'REQBUSD', 'SANDBRL', 'MANABIDR', 'SANDBIDR', 'VOXELBTC', 'VOXELBNB', 'VOXELBUSD',
                          'VOXELUSDT', 'COSBUSD', 'CTXCBUSD', 'FTMTRY', 'MANABNB', 'MINATRY', 'XTZTRY', 'HIGHBTC',
                          'HIGHBUSD', 'HIGHUSDT', 'CVXBTC', 'CVXBUSD', 'CVXUSDT', 'PEOPLEBTC', 'PEOPLEBUSD',
                          'PEOPLEUSDT', 'OOKIBUSD', 'OOKIUSDT', 'COCOSTRY', 'GXSBNB', 'LINKBNB', 'LUNAETH', 'MDTBUSD',
                          'NULSBUSD', 'SPELLBTC', 'SPELLUSDT', 'SPELLBUSD', 'USTBTC', 'USTBUSD', 'USTUSDT', 'JOEBTC',
                          'JOEBUSD', 'JOEUSDT', 'ATOMETH', 'DUSKBUSD', 'EGLDETH', 'ICPETH', 'LUNABRL', 'LUNAUST',
                          'NEARETH', 'ROSEBNB', 'VOXELETH', 'ALICEBNB', 'ATOMTRY', 'ETHUST', 'GALAAUD', 'LRCBNB',
                          'ONEETH', 'OOKIBNB']

        self.selected_symbols = self.symbols[:50]  ## kiválasztam amivel dolgozok
        self.selected_pairs = self.defa_selected_pairs()  ##a kiválasztott szimbólumokhoz kapcsolódó párokat kiválasztom

        self.freq_selected_symbols = self.defa_symbols_frequency()  ## a symbólum gyakoriság méréséhez előkészítem a dictionarit
        self.freq_triangles = {}
        self.freq_start_symbol = {}

        self.df = pd.DataFrame(columns=self.selected_symbols, index=self.selected_symbols)
        self.df = self.df.astype(float)

        self.socket_list = self.defa_socket_list()

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
                # print(res)
                # i_sec = datetime.datetime.now().second
                # self.speed_data[i_sec] += 1
                # if i_sec == 59:
                #     self.speed_data = self.speed_defa.copy()

                s1 = self.selected_pairs[res['data']['s']][0]
                s2 = self.selected_pairs[res['data']['s']][1]

                self.df[s1][s2] = round(float(res['data']['b']) * self.fee_bid, 8)
                self.df[s2][s1] = round(1 / (float(res['data']['a']) * self.fee_ask), 8)

        await client.close_connection()

    def start_asyc_websocket(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(self.asyc_websocket())
        loop.close()


if __name__ == '__main__':
    n_arb = n_arbitrage()
    n_arb.start()
    n_arb.riport.create("A1_symbol_frequency",
                        "Arbitrázsban érintett symbol-ok gyakorisága, leggyakoribb tirangles, leggyakoribb kezdő symbol-ok")

    all_arb = 0
    while True:
        # i_sec = datetime.datetime.now().second
        # n_arb.speed_find_arb[i_sec] += 1
        # if i_sec == 59:
        #     n_arb.speed_find_arb = n_arb.speed_defa.copy()

        time.sleep(n_arb.arb_check_delay)
        # print(n_arb.df)
        arb_result = n_arb.arb_find()
        if len(arb_result) > 0:
            all_arb += 1
            i_start_symbol = ""

            for arr in arb_result:
                i_start_symbol = str(arr[0])
                if arr[0] != - 1:
                    i_triangle = ""
                    for s in range(len(arr) - 1):
                        i_triangle += arr[s] + "_"
                        n_arb.freq_selected_symbols[arr[s]] += 1

                    i_triangle = i_triangle[:-1]
                    if i_triangle in n_arb.freq_triangles.keys():
                        n_arb.freq_triangles[i_triangle] += 1
                    else:
                        n_arb.freq_triangles[i_triangle] = 1

                    if i_start_symbol in n_arb.freq_start_symbol.keys():
                        n_arb.freq_start_symbol[i_start_symbol] += 1
                    else:
                        n_arb.freq_start_symbol[i_start_symbol] = 1
            # arb_profit = 1
            # print(str(arb_result))
            # print(" \r" + str(arb_result[0]), end="")
            # print(n_arb.df)
            # n_arb.df.to_clipboard(excel=True, sep=None)
            # for i in range(len(arb_result[0]) - 1):
            #     arb_profit = arb_profit / n_arb.df.at[arb_result[0][i], arb_result[0][i + 1]]
            #     print(arb_result[0][i] + " - " + arb_result[0][i+1], n_arb.df.at[arb_result[0][i], arb_result[0][i + 1]])
            # print(arb_profit)

            # act_sec = datetime.datetime.now().second
            # s1 = n_arb.speed_data[act_sec - 2]
            # s2 = n_arb.speed_data[act_sec - 1]
            # s3 = n_arb.speed_data[act_sec]
            #
            # s4 = "0000" + str(n_arb.speed_find_arb[datetime.datetime.now().second - 1])
            #
            # max_speed = max(s1, s2, s3, max_speed)
            # max_speed_str = "0000" + str(max_speed)
            #
            # avg_speed = "0000" + str(int((s1 + s2 + s3) / 3 / 100) * 100)

            # print("\r - Speed:",
            #       s4[-4:],
            #       " / ",
            #       max_speed_str[-4:],
            #       " / ",
            #       avg_speed[-4:],
            #       "Most effective symbols:",
            #       dict((k, v) for k, v in n_arb.selected_symbols_frequency.items() if v > 0),
            #       "number of arb situation:",
            #       all_arb,
            #       end="")

            if int(all_arb / 10) == all_arb / 10:
                n_arb.riport.clear()
                i_ssf = dict(sorted(n_arb.freq_selected_symbols.items(), key=lambda item: item[1]))
                n_arb.riport.add("Selected symbols:", i_ssf)
                n_arb.riport.add_section("Most frequent triangles:")
                n_arb.riport.add("Triangles:", n_arb.freq_triangles)
                n_arb.riport.add_section("Most frequent start symbols:")
                n_arb.riport.add("Symbols:", n_arb.freq_start_symbol)
                n_arb.riport.write()
