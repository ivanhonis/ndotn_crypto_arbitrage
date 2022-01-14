from binance.client import Client
from binance import ThreadedWebsocketManager

import networkx as nx
from collections import defaultdict

import pandas as pd
from tqdm import tqdm
import numpy as np
import math
import time


class n_arbitrage:

    def __init__(self):

        self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
        self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
        self.b_client = Client(self.api_key, self.api_secret)
        self.b_twm = ThreadedWebsocketManager(api_key=self.api_key, api_secret=self.api_secret)
        self.b_twm.start()

        self.symbols = ['BTC', 'BNB', 'ETH', 'XRP', 'ADA', 'LINK', 'LTC', 'DOT', 'TRX', 'FTM', 'BUSD',
                        'SOL', 'USDT', 'MATIC', 'EOS', 'ETC', 'LUNA', 'NEO', 'XLM', 'AVAX', 'ENJ', 'WAVES',
                        'ATOM', 'ONE', 'ALGO', 'SXP', 'SAND', 'SHIB', 'ZEC', 'MANA', 'VET',
                        'CHZ', 'BCH', 'AXS', 'GALA', 'ANKR', 'RUNE', 'CAKE', 'AAVE', 'ICP', 'LRC',
                        'ZIL', 'THETA', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS', 'ALICE']

        self.all_pairs = ['ETHBTC', 'LTCBTC', 'BNBBTC', 'NEOBTC', 'QTUMETH', 'EOSETH', 'SNTETH', 'BNTETH', 'BCCBTC', 'GASBTC', 'BNBETH', 'BTCUSDT', 'ETHUSDT', 'HSRBTC', 'OAXETH', 'DNTETH', 'MCOETH', 'ICNETH', 'MCOBTC', 'WTCBTC', 'WTCETH', 'LRCBTC', 'LRCETH', 'QTUMBTC', 'YOYOBTC', 'OMGBTC', 'OMGETH', 'ZRXBTC', 'ZRXETH', 'STRATBTC', 'STRATETH', 'SNGLSBTC', 'SNGLSETH', 'BQXBTC', 'BQXETH', 'KNCBTC', 'KNCETH', 'FUNBTC', 'FUNETH', 'SNMBTC', 'SNMETH', 'NEOETH', 'IOTABTC', 'IOTAETH', 'LINKBTC', 'LINKETH', 'XVGBTC', 'XVGETH', 'SALTBTC', 'SALTETH', 'MDABTC', 'MDAETH', 'MTLBTC', 'MTLETH', 'SUBBTC', 'SUBETH', 'EOSBTC', 'SNTBTC', 'ETCETH', 'ETCBTC', 'MTHBTC', 'MTHETH', 'ENGBTC', 'ENGETH', 'DNTBTC', 'ZECBTC', 'ZECETH', 'BNTBTC', 'ASTBTC', 'ASTETH', 'DASHBTC', 'DASHETH', 'OAXBTC', 'ICNBTC', 'BTGBTC', 'BTGETH', 'EVXBTC', 'EVXETH', 'REQBTC', 'REQETH', 'VIBBTC', 'VIBETH', 'HSRETH', 'TRXBTC', 'TRXETH', 'POWRBTC', 'POWRETH', 'ARKBTC', 'ARKETH', 'YOYOETH', 'XRPBTC', 'XRPETH', 'MODBTC', 'MODETH', 'ENJBTC', 'ENJETH', 'STORJBTC', 'STORJETH', 'BNBUSDT', 'VENBNB', 'YOYOBNB', 'POWRBNB', 'VENBTC', 'VENETH', 'KMDBTC', 'KMDETH', 'NULSBNB', 'RCNBTC', 'RCNETH', 'RCNBNB', 'NULSBTC', 'NULSETH', 'RDNBTC', 'RDNETH', 'RDNBNB', 'XMRBTC', 'XMRETH', 'DLTBNB', 'WTCBNB', 'DLTBTC', 'DLTETH', 'AMBBTC', 'AMBETH', 'AMBBNB', 'BCCETH', 'BCCUSDT', 'BCCBNB', 'BATBTC', 'BATETH', 'BATBNB', 'BCPTBTC', 'BCPTETH', 'BCPTBNB', 'ARNBTC', 'ARNETH', 'GVTBTC', 'GVTETH', 'CDTBTC', 'CDTETH', 'GXSBTC', 'GXSETH', 'NEOUSDT', 'NEOBNB', 'POEBTC', 'POEETH', 'QSPBTC', 'QSPETH', 'QSPBNB', 'BTSBTC', 'BTSETH', 'BTSBNB', 'XZCBTC', 'XZCETH', 'XZCBNB', 'LSKBTC', 'LSKETH', 'LSKBNB', 'TNTBTC', 'TNTETH', 'FUELBTC', 'FUELETH', 'MANABTC', 'MANAETH', 'BCDBTC', 'BCDETH', 'DGDBTC', 'DGDETH', 'IOTABNB', 'ADXBTC', 'ADXETH', 'ADXBNB', 'ADABTC', 'ADAETH', 'PPTBTC', 'PPTETH', 'CMTBTC', 'CMTETH', 'CMTBNB', 'XLMBTC', 'XLMETH', 'XLMBNB', 'CNDBTC', 'CNDETH', 'CNDBNB', 'LENDBTC', 'LENDETH', 'WABIBTC', 'WABIETH', 'WABIBNB', 'LTCETH', 'LTCUSDT', 'LTCBNB', 'TNBBTC', 'TNBETH', 'WAVESBTC', 'WAVESETH', 'WAVESBNB', 'GTOBTC', 'GTOETH', 'GTOBNB', 'ICXBTC', 'ICXETH', 'ICXBNB', 'OSTBTC', 'OSTETH', 'OSTBNB', 'ELFBTC', 'ELFETH', 'AIONBTC', 'AIONETH', 'AIONBNB', 'NEBLBTC', 'NEBLETH', 'NEBLBNB', 'BRDBTC', 'BRDETH', 'BRDBNB', 'MCOBNB', 'EDOBTC', 'EDOETH', 'WINGSBTC', 'WINGSETH', 'NAVBTC', 'NAVETH', 'NAVBNB', 'LUNBTC', 'LUNETH', 'TRIGBTC', 'TRIGETH', 'TRIGBNB', 'APPCBTC', 'APPCETH', 'APPCBNB', 'VIBEBTC', 'VIBEETH', 'RLCBTC', 'RLCETH', 'RLCBNB', 'INSBTC', 'INSETH', 'PIVXBTC', 'PIVXETH', 'PIVXBNB', 'IOSTBTC', 'IOSTETH', 'CHATBTC', 'CHATETH', 'STEEMBTC', 'STEEMETH', 'STEEMBNB', 'NANOBTC', 'NANOETH', 'NANOBNB', 'VIABTC', 'VIAETH', 'VIABNB', 'BLZBTC', 'BLZETH', 'BLZBNB', 'AEBTC', 'AEETH', 'AEBNB', 'RPXBTC', 'RPXETH', 'RPXBNB', 'NCASHBTC', 'NCASHETH', 'NCASHBNB', 'POABTC', 'POAETH', 'POABNB', 'ZILBTC', 'ZILETH', 'ZILBNB', 'ONTBTC', 'ONTETH', 'ONTBNB', 'STORMBTC', 'STORMETH', 'STORMBNB', 'QTUMBNB', 'QTUMUSDT', 'XEMBTC', 'XEMETH', 'XEMBNB', 'WANBTC', 'WANETH', 'WANBNB', 'WPRBTC', 'WPRETH', 'QLCBTC', 'QLCETH', 'SYSBTC', 'SYSETH', 'SYSBNB', 'QLCBNB', 'GRSBTC', 'GRSETH', 'ADAUSDT', 'ADABNB', 'CLOAKBTC', 'CLOAKETH', 'GNTBTC', 'GNTETH', 'GNTBNB', 'LOOMBTC', 'LOOMETH', 'LOOMBNB', 'XRPUSDT', 'BCNBTC', 'BCNETH', 'BCNBNB', 'REPBTC', 'REPETH', 'REPBNB', 'BTCTUSD', 'TUSDBTC', 'ETHTUSD', 'TUSDETH', 'TUSDBNB', 'ZENBTC', 'ZENETH', 'ZENBNB', 'SKYBTC', 'SKYETH', 'SKYBNB', 'EOSUSDT', 'EOSBNB', 'CVCBTC', 'CVCETH', 'CVCBNB', 'THETABTC', 'THETAETH', 'THETABNB', 'XRPBNB', 'TUSDUSDT', 'IOTAUSDT', 'XLMUSDT', 'IOTXBTC', 'IOTXETH', 'QKCBTC', 'QKCETH', 'AGIBTC', 'AGIETH', 'AGIBNB', 'NXSBTC', 'NXSETH', 'NXSBNB', 'ENJBNB', 'DATABTC', 'DATAETH', 'ONTUSDT', 'TRXBNB', 'TRXUSDT', 'ETCUSDT', 'ETCBNB', 'ICXUSDT', 'SCBTC', 'SCETH', 'SCBNB', 'NPXSBTC', 'NPXSETH', 'VENUSDT', 'KEYBTC', 'KEYETH', 'NASBTC', 'NASETH', 'NASBNB', 'MFTBTC', 'MFTETH', 'MFTBNB', 'DENTBTC', 'DENTETH', 'ARDRBTC', 'ARDRETH', 'ARDRBNB', 'NULSUSDT', 'HOTBTC', 'HOTETH', 'VETBTC', 'VETETH', 'VETUSDT', 'VETBNB', 'DOCKBTC', 'DOCKETH', 'POLYBTC', 'POLYBNB', 'PHXBTC', 'PHXETH', 'PHXBNB', 'HCBTC', 'HCETH', 'GOBTC', 'GOBNB', 'PAXBTC', 'PAXBNB', 'PAXUSDT', 'PAXETH', 'RVNBTC', 'RVNBNB', 'DCRBTC', 'DCRBNB', 'USDCBNB', 'MITHBTC', 'MITHBNB', 'BCHABCBTC', 'BCHSVBTC', 'BCHABCUSDT', 'BCHSVUSDT', 'BNBPAX', 'BTCPAX', 'ETHPAX', 'XRPPAX', 'EOSPAX', 'XLMPAX', 'RENBTC', 'RENBNB', 'BNBTUSD', 'XRPTUSD', 'EOSTUSD', 'XLMTUSD', 'BNBUSDC', 'BTCUSDC', 'ETHUSDC', 'XRPUSDC', 'EOSUSDC', 'XLMUSDC', 'USDCUSDT', 'ADATUSD', 'TRXTUSD', 'NEOTUSD', 'TRXXRP', 'XZCXRP', 'PAXTUSD', 'USDCTUSD', 'USDCPAX', 'LINKUSDT', 'LINKTUSD', 'LINKPAX', 'LINKUSDC', 'WAVESUSDT', 'WAVESTUSD', 'WAVESPAX', 'WAVESUSDC', 'BCHABCTUSD', 'BCHABCPAX', 'BCHABCUSDC', 'BCHSVTUSD', 'BCHSVPAX', 'BCHSVUSDC', 'LTCTUSD', 'LTCPAX', 'LTCUSDC', 'TRXPAX', 'TRXUSDC', 'BTTBTC', 'BTTBNB', 'BTTUSDT', 'BNBUSDS', 'BTCUSDS', 'USDSUSDT', 'USDSPAX', 'USDSTUSD', 'USDSUSDC', 'BTTPAX', 'BTTTUSD', 'BTTUSDC', 'ONGBNB', 'ONGBTC', 'ONGUSDT', 'HOTBNB', 'HOTUSDT', 'ZILUSDT', 'ZRXBNB', 'ZRXUSDT', 'FETBNB', 'FETBTC', 'FETUSDT', 'BATUSDT', 'XMRBNB', 'XMRUSDT', 'ZECBNB', 'ZECUSDT', 'ZECPAX', 'ZECTUSD', 'ZECUSDC', 'IOSTBNB', 'IOSTUSDT', 'CELRBNB', 'CELRBTC', 'CELRUSDT', 'ADAPAX', 'ADAUSDC', 'NEOPAX', 'NEOUSDC', 'DASHBNB', 'DASHUSDT', 'NANOUSDT', 'OMGBNB', 'OMGUSDT', 'THETAUSDT', 'ENJUSDT', 'MITHUSDT', 'MATICBNB', 'MATICBTC', 'MATICUSDT', 'ATOMBNB', 'ATOMBTC', 'ATOMUSDT', 'ATOMUSDC', 'ATOMPAX', 'ATOMTUSD', 'ETCUSDC', 'ETCPAX', 'ETCTUSD', 'BATUSDC', 'BATPAX', 'BATTUSD', 'PHBBNB', 'PHBBTC', 'PHBUSDC', 'PHBTUSD', 'PHBPAX', 'TFUELBNB', 'TFUELBTC', 'TFUELUSDT', 'TFUELUSDC', 'TFUELTUSD', 'TFUELPAX', 'ONEBNB', 'ONEBTC', 'ONEUSDT', 'ONETUSD', 'ONEPAX', 'ONEUSDC', 'FTMBNB', 'FTMBTC', 'FTMUSDT', 'FTMTUSD', 'FTMPAX', 'FTMUSDC', 'BTCBBTC', 'BCPTTUSD', 'BCPTPAX', 'BCPTUSDC', 'ALGOBNB', 'ALGOBTC', 'ALGOUSDT', 'ALGOTUSD', 'ALGOPAX', 'ALGOUSDC', 'USDSBUSDT', 'USDSBUSDS', 'GTOUSDT', 'GTOPAX', 'GTOTUSD', 'GTOUSDC', 'ERDBNB', 'ERDBTC', 'ERDUSDT', 'ERDPAX', 'ERDUSDC', 'DOGEBNB', 'DOGEBTC', 'DOGEUSDT', 'DOGEPAX', 'DOGEUSDC', 'DUSKBNB', 'DUSKBTC', 'DUSKUSDT', 'DUSKUSDC', 'DUSKPAX', 'BGBPUSDC', 'ANKRBNB', 'ANKRBTC', 'ANKRUSDT', 'ANKRTUSD', 'ANKRPAX', 'ANKRUSDC', 'ONTPAX', 'ONTUSDC', 'WINBNB', 'WINBTC', 'WINUSDT', 'WINUSDC', 'COSBNB', 'COSBTC', 'COSUSDT', 'TUSDBTUSD', 'NPXSUSDT', 'NPXSUSDC', 'COCOSBNB', 'COCOSBTC', 'COCOSUSDT', 'MTLUSDT', 'TOMOBNB', 'TOMOBTC', 'TOMOUSDT', 'TOMOUSDC', 'PERLBNB', 'PERLBTC', 'PERLUSDC', 'PERLUSDT', 'DENTUSDT', 'MFTUSDT', 'KEYUSDT', 'STORMUSDT', 'DOCKUSDT', 'WANUSDT', 'FUNUSDT', 'CVCUSDT', 'BTTTRX', 'WINTRX', 'CHZBNB', 'CHZBTC', 'CHZUSDT', 'BANDBNB', 'BANDBTC', 'BANDUSDT', 'BNBBUSD', 'BTCBUSD', 'BUSDUSDT', 'BEAMBNB', 'BEAMBTC', 'BEAMUSDT', 'XTZBNB', 'XTZBTC', 'XTZUSDT', 'RENUSDT', 'RVNUSDT', 'HCUSDT', 'HBARBNB', 'HBARBTC', 'HBARUSDT', 'NKNBNB', 'NKNBTC', 'NKNUSDT', 'XRPBUSD', 'ETHBUSD', 'BCHABCBUSD', 'LTCBUSD', 'LINKBUSD', 'ETCBUSD', 'STXBNB', 'STXBTC', 'STXUSDT', 'KAVABNB', 'KAVABTC', 'KAVAUSDT', 'BUSDNGN', 'BNBNGN', 'BTCNGN', 'ARPABNB', 'ARPABTC', 'ARPAUSDT', 'TRXBUSD', 'EOSBUSD', 'IOTXUSDT', 'RLCUSDT', 'MCOUSDT', 'XLMBUSD', 'ADABUSD', 'CTXCBNB', 'CTXCBTC', 'CTXCUSDT', 'BCHBNB', 'BCHBTC', 'BCHUSDT', 'BCHUSDC', 'BCHTUSD', 'BCHPAX', 'BCHBUSD', 'BTCRUB', 'ETHRUB', 'XRPRUB', 'BNBRUB', 'TROYBNB', 'TROYBTC', 'TROYUSDT', 'BUSDRUB', 'QTUMBUSD', 'VETBUSD', 'VITEBNB', 'VITEBTC', 'VITEUSDT', 'FTTBNB', 'FTTBTC', 'FTTUSDT', 'BTCTRY', 'BNBTRY', 'BUSDTRY', 'ETHTRY', 'XRPTRY', 'USDTTRY', 'USDTRUB', 'BTCEUR', 'ETHEUR', 'BNBEUR', 'XRPEUR', 'EURBUSD', 'EURUSDT', 'OGNBNB', 'OGNBTC', 'OGNUSDT', 'DREPBNB', 'DREPBTC', 'DREPUSDT', 'BULLUSDT', 'BULLBUSD', 'BEARUSDT', 'BEARBUSD', 'ETHBULLUSDT', 'ETHBULLBUSD', 'ETHBEARUSDT', 'ETHBEARBUSD', 'TCTBNB', 'TCTBTC', 'TCTUSDT', 'WRXBNB', 'WRXBTC', 'WRXUSDT', 'ICXBUSD', 'BTSUSDT', 'BTSBUSD', 'LSKUSDT', 'BNTUSDT', 'BNTBUSD', 'LTOBNB', 'LTOBTC', 'LTOUSDT', 'ATOMBUSD', 'DASHBUSD', 'NEOBUSD', 'WAVESBUSD', 'XTZBUSD', 'EOSBULLUSDT', 'EOSBULLBUSD', 'EOSBEARUSDT', 'EOSBEARBUSD', 'XRPBULLUSDT', 'XRPBULLBUSD', 'XRPBEARUSDT', 'XRPBEARBUSD', 'BATBUSD', 'ENJBUSD', 'NANOBUSD', 'ONTBUSD', 'RVNBUSD', 'STRATBUSD', 'STRATBNB', 'STRATUSDT', 'AIONBUSD', 'AIONUSDT', 'MBLBNB', 'MBLBTC', 'MBLUSDT', 'COTIBNB', 'COTIBTC', 'COTIUSDT', 'ALGOBUSD', 'BTTBUSD', 'TOMOBUSD', 'XMRBUSD', 'ZECBUSD', 'BNBBULLUSDT', 'BNBBULLBUSD', 'BNBBEARUSDT', 'BNBBEARBUSD', 'STPTBNB', 'STPTBTC', 'STPTUSDT', 'BTCZAR', 'ETHZAR', 'BNBZAR', 'USDTZAR', 'BUSDZAR', 'BTCBKRW', 'ETHBKRW', 'BNBBKRW', 'WTCUSDT', 'DATABUSD', 'DATAUSDT', 'XZCUSDT', 'SOLBNB', 'SOLBTC', 'SOLUSDT', 'SOLBUSD', 'BTCIDRT', 'BNBIDRT', 'USDTIDRT', 'BUSDIDRT', 'CTSIBTC', 'CTSIUSDT', 'CTSIBNB', 'CTSIBUSD', 'HIVEBNB', 'HIVEBTC', 'HIVEUSDT', 'CHRBNB', 'CHRBTC', 'CHRUSDT', 'BTCUPUSDT', 'BTCDOWNUSDT', 'GXSUSDT', 'ARDRUSDT', 'ERDBUSD', 'LENDUSDT', 'HBARBUSD', 'MATICBUSD', 'WRXBUSD', 'ZILBUSD', 'MDTBNB', 'MDTBTC', 'MDTUSDT', 'STMXBNB', 'STMXBTC', 'STMXETH', 'STMXUSDT', 'KNCBUSD', 'KNCUSDT', 'REPBUSD', 'REPUSDT', 'LRCBUSD', 'LRCUSDT', 'IQBNB', 'IQBUSD', 'PNTBTC', 'PNTUSDT', 'BTCGBP', 'ETHGBP', 'XRPGBP', 'BNBGBP', 'GBPBUSD', 'DGBBNB', 'DGBBTC', 'DGBBUSD', 'BTCUAH', 'USDTUAH', 'COMPBTC', 'COMPBNB', 'COMPBUSD', 'COMPUSDT', 'BTCBIDR', 'ETHBIDR', 'BNBBIDR', 'BUSDBIDR', 'USDTBIDR', 'BKRWUSDT', 'BKRWBUSD', 'SCUSDT', 'ZENUSDT', 'SXPBTC', 'SXPBNB', 'SXPBUSD', 'SNXBTC', 'SNXBNB', 'SNXBUSD', 'SNXUSDT', 'ETHUPUSDT', 'ETHDOWNUSDT', 'ADAUPUSDT', 'ADADOWNUSDT', 'LINKUPUSDT', 'LINKDOWNUSDT', 'VTHOBNB', 'VTHOBUSD', 'VTHOUSDT', 'DCRBUSD', 'DGBUSDT', 'GBPUSDT', 'STORJBUSD', 'SXPUSDT', 'IRISBNB', 'IRISBTC', 'IRISBUSD', 'MKRBNB', 'MKRBTC', 'MKRUSDT', 'MKRBUSD', 'DAIBNB', 'DAIBTC', 'DAIUSDT', 'DAIBUSD', 'RUNEBNB', 'RUNEBTC', 'RUNEBUSD', 'MANABUSD', 'DOGEBUSD', 'LENDBUSD', 'ZRXBUSD', 'DCRUSDT', 'STORJUSDT', 'XRPBKRW', 'ADABKRW', 'BTCAUD', 'ETHAUD', 'AUDBUSD', 'FIOBNB', 'FIOBTC', 'FIOBUSD', 'BNBUPUSDT', 'BNBDOWNUSDT', 'XTZUPUSDT', 'XTZDOWNUSDT', 'AVABNB', 'AVABTC', 'AVABUSD', 'USDTBKRW', 'BUSDBKRW', 'IOTABUSD', 'MANAUSDT', 'XRPAUD', 'BNBAUD', 'AUDUSDT', 'BALBNB', 'BALBTC', 'BALBUSD', 'YFIBNB', 'YFIBTC', 'YFIBUSD', 'YFIUSDT', 'BLZBUSD', 'KMDBUSD', 'BALUSDT', 'BLZUSDT', 'IRISUSDT', 'KMDUSDT', 'BTCDAI', 'ETHDAI', 'BNBDAI', 'USDTDAI', 'BUSDDAI', 'JSTBNB', 'JSTBTC', 'JSTBUSD', 'JSTUSDT', 'SRMBNB', 'SRMBTC', 'SRMBUSD', 'SRMUSDT', 'ANTBNB', 'ANTBTC', 'ANTBUSD', 'ANTUSDT', 'CRVBNB', 'CRVBTC', 'CRVBUSD', 'CRVUSDT', 'SANDBNB', 'SANDBTC', 'SANDUSDT', 'SANDBUSD', 'OCEANBNB', 'OCEANBTC', 'OCEANBUSD', 'OCEANUSDT', 'NMRBNB', 'NMRBTC', 'NMRBUSD', 'NMRUSDT', 'DOTBNB', 'DOTBTC', 'DOTBUSD', 'DOTUSDT', 'LUNABNB', 'LUNABTC', 'LUNABUSD', 'LUNAUSDT', 'IDEXBTC', 'IDEXBUSD', 'RSRBNB', 'RSRBTC', 'RSRBUSD', 'RSRUSDT', 'PAXGBNB', 'PAXGBTC', 'PAXGBUSD', 'PAXGUSDT', 'WNXMBNB', 'WNXMBTC', 'WNXMBUSD', 'WNXMUSDT', 'TRBBNB', 'TRBBTC', 'TRBBUSD', 'TRBUSDT', 'ETHNGN', 'DOTBIDR', 'LINKAUD', 'SXPAUD', 'BZRXBNB', 'BZRXBTC', 'BZRXBUSD', 'BZRXUSDT', 'WBTCBTC', 'WBTCETH', 'SUSHIBNB', 'SUSHIBTC', 'SUSHIBUSD', 'SUSHIUSDT', 'YFIIBNB', 'YFIIBTC', 'YFIIBUSD', 'YFIIUSDT', 'KSMBNB', 'KSMBTC', 'KSMBUSD', 'KSMUSDT', 'EGLDBNB', 'EGLDBTC', 'EGLDBUSD', 'EGLDUSDT', 'DIABNB', 'DIABTC', 'DIABUSD', 'DIAUSDT', 'RUNEUSDT', 'FIOUSDT', 'UMABTC', 'UMAUSDT', 'EOSUPUSDT', 'EOSDOWNUSDT', 'TRXUPUSDT', 'TRXDOWNUSDT', 'XRPUPUSDT', 'XRPDOWNUSDT', 'DOTUPUSDT', 'DOTDOWNUSDT', 'SRMBIDR', 'ONEBIDR', 'LINKTRY', 'USDTNGN', 'BELBNB', 'BELBTC', 'BELBUSD', 'BELUSDT', 'WINGBNB', 'WINGBTC', 'SWRVBNB', 'SWRVBUSD', 'WINGBUSD', 'WINGUSDT', 'LTCUPUSDT', 'LTCDOWNUSDT', 'LENDBKRW', 'SXPEUR', 'CREAMBNB', 'CREAMBUSD', 'UNIBNB', 'UNIBTC', 'UNIBUSD', 'UNIUSDT', 'NBSBTC', 'NBSUSDT', 'OXTBTC', 'OXTUSDT', 'SUNBTC', 'SUNUSDT', 'AVAXBNB', 'AVAXBTC', 'AVAXBUSD', 'AVAXUSDT', 'HNTBTC', 'HNTUSDT', 'BAKEBNB', 'BURGERBNB', 'SXPBIDR', 'LINKBKRW', 'FLMBNB', 'FLMBTC', 'FLMBUSD', 'FLMUSDT', 'SCRTBTC', 'SCRTETH', 'CAKEBNB', 'CAKEBUSD', 'SPARTABNB', 'UNIUPUSDT', 'UNIDOWNUSDT', 'ORNBTC', 'ORNUSDT', 'TRXNGN', 'SXPTRY', 'UTKBTC', 'UTKUSDT', 'XVSBNB', 'XVSBTC', 'XVSBUSD', 'XVSUSDT', 'ALPHABNB', 'ALPHABTC', 'ALPHABUSD', 'ALPHAUSDT', 'VIDTBTC', 'VIDTBUSD', 'AAVEBNB', 'BTCBRL', 'USDTBRL', 'AAVEBTC', 'AAVEETH', 'AAVEBUSD', 'AAVEUSDT', 'AAVEBKRW', 'NEARBNB', 'NEARBTC', 'NEARBUSD', 'NEARUSDT', 'SXPUPUSDT', 'SXPDOWNUSDT', 'DOTBKRW', 'SXPGBP', 'FILBNB', 'FILBTC', 'FILBUSD', 'FILUSDT', 'FILUPUSDT', 'FILDOWNUSDT', 'YFIUPUSDT', 'YFIDOWNUSDT', 'INJBNB', 'INJBTC', 'INJBUSD', 'INJUSDT', 'AERGOBTC', 'AERGOBUSD', 'LINKEUR', 'ONEBUSD', 'EASYETH', 'AUDIOBTC', 'AUDIOBUSD', 'AUDIOUSDT', 'CTKBNB', 'CTKBTC', 'CTKBUSD', 'CTKUSDT', 'BCHUPUSDT', 'BCHDOWNUSDT', 'BOTBTC', 'BOTBUSD', 'ETHBRL', 'DOTEUR', 'AKROBTC', 'AKROUSDT', 'KP3RBNB', 'KP3RBUSD', 'AXSBNB', 'AXSBTC', 'AXSBUSD', 'AXSUSDT', 'HARDBNB', 'HARDBTC', 'HARDBUSD', 'HARDUSDT', 'BNBBRL', 'LTCEUR', 'RENBTCBTC', 'RENBTCETH', 'DNTBUSD', 'DNTUSDT', 'SLPETH', 'ADAEUR', 'LTCNGN', 'CVPETH', 'CVPBUSD', 'STRAXBTC', 'STRAXETH', 'STRAXBUSD', 'STRAXUSDT', 'FORBTC', 'FORBUSD', 'UNFIBNB', 'UNFIBTC', 'UNFIBUSD', 'UNFIUSDT', 'FRONTETH', 'FRONTBUSD', 'BCHABUSD', 'ROSEBTC', 'ROSEBUSD', 'ROSEUSDT', 'AVAXTRY', 'BUSDBRL', 'AVAUSDT', 'SYSBUSD', 'XEMUSDT', 'HEGICETH', 'HEGICBUSD', 'AAVEUPUSDT', 'AAVEDOWNUSDT', 'PROMBNB', 'PROMBUSD', 'XRPBRL', 'XRPNGN', 'SKLBTC', 'SKLBUSD', 'SKLUSDT', 'BCHEUR', 'YFIEUR', 'ZILBIDR', 'SUSDBTC', 'SUSDETH', 'SUSDUSDT', 'COVERETH', 'COVERBUSD', 'GLMBTC', 'GLMETH', 'GHSTETH', 'GHSTBUSD', 'SUSHIUPUSDT', 'SUSHIDOWNUSDT', 'XLMUPUSDT', 'XLMDOWNUSDT', 'LINKBRL', 'LINKNGN', 'LTCRUB', 'TRXTRY', 'XLMEUR', 'DFETH', 'DFBUSD', 'GRTBTC', 'GRTETH', 'GRTUSDT', 'JUVBTC', 'JUVBUSD', 'JUVUSDT', 'PSGBTC', 'PSGBUSD', 'PSGUSDT', 'BUSDBVND', 'USDTBVND', '1INCHBTC', '1INCHUSDT', 'REEFBTC', 'REEFUSDT', 'OGBTC', 'OGUSDT', 'ATMBTC', 'ATMUSDT', 'ASRBTC', 'ASRUSDT', 'CELOBTC', 'CELOUSDT', 'RIFBTC', 'RIFUSDT', 'CHZTRY', 'XLMTRY', 'LINKGBP', 'GRTEUR', 'BTCSTBTC', 'BTCSTBUSD', 'BTCSTUSDT', 'TRUBTC', 'TRUBUSD', 'TRUUSDT', 'DEXEETH', 'DEXEBUSD', 'EOSEUR', 'LTCBRL', 'USDCBUSD', 'TUSDBUSD', 'PAXBUSD', 'CKBBTC', 'CKBBUSD', 'CKBUSDT', 'TWTBTC', 'TWTBUSD', 'TWTUSDT', 'FIROBTC', 'FIROETH', 'FIROUSDT', 'BETHETH', 'DOGEEUR', 'DOGETRY', 'DOGEAUD', 'DOGEBRL', 'DOTNGN', 'PROSETH', 'LITBTC', 'LITBUSD', 'LITUSDT', 'BTCVAI', 'BUSDVAI', 'SFPBTC', 'SFPBUSD', 'SFPUSDT', 'DOGEGBP', 'DOTTRY', 'FXSBTC', 'FXSBUSD', 'DODOBTC', 'DODOBUSD', 'DODOUSDT', 'FRONTBTC', 'EASYBTC', 'CAKEBTC', 'CAKEUSDT', 'BAKEBUSD', 'UFTETH', 'UFTBUSD', '1INCHBUSD', 'BANDBUSD', 'GRTBUSD', 'IOSTBUSD', 'OMGBUSD', 'REEFBUSD', 'ACMBTC', 'ACMBUSD', 'ACMUSDT', 'AUCTIONBTC', 'AUCTIONBUSD', 'PHABTC', 'PHABUSD', 'DOTGBP', 'ADATRY', 'ADABRL', 'ADAGBP', 'TVKBTC', 'TVKBUSD', 'BADGERBTC', 'BADGERBUSD', 'BADGERUSDT', 'FISBTC', 'FISBUSD', 'FISUSDT', 'DOTBRL', 'ADAAUD', 'HOTTRY', 'EGLDEUR', 'OMBTC', 'OMBUSD', 'OMUSDT', 'PONDBTC', 'PONDBUSD', 'PONDUSDT', 'DEGOBTC', 'DEGOBUSD', 'DEGOUSDT', 'AVAXEUR', 'BTTTRY', 'CHZBRL', 'UNIEUR', 'ALICEBTC', 'ALICEBUSD', 'ALICEUSDT', 'CHZBUSD', 'CHZEUR', 'CHZGBP', 'BIFIBNB', 'BIFIBUSD', 'LINABTC', 'LINABUSD', 'LINAUSDT', 'ADARUB', 'ENJBRL', 'ENJEUR', 'MATICEUR', 'NEOTRY', 'PERPBTC', 'PERPBUSD', 'PERPUSDT', 'RAMPBTC', 'RAMPBUSD', 'RAMPUSDT', 'SUPERBTC', 'SUPERBUSD', 'SUPERUSDT', 'CFXBTC', 'CFXBUSD', 'CFXUSDT', 'ENJGBP', 'EOSTRY', 'LTCGBP', 'LUNAEUR', 'RVNTRY', 'THETAEUR', 'XVGBUSD', 'EPSBTC', 'EPSBUSD', 'EPSUSDT', 'AUTOBTC', 'AUTOBUSD', 'AUTOUSDT', 'TKOBTC', 'TKOBIDR', 'TKOBUSD', 'TKOUSDT', 'PUNDIXETH', 'PUNDIXUSDT', 'BTTBRL', 'BTTEUR', 'HOTEUR', 'WINEUR', 'TLMBTC', 'TLMBUSD', 'TLMUSDT', '1INCHUPUSDT', '1INCHDOWNUSDT', 'BTGBUSD', 'BTGUSDT', 'HOTBUSD', 'BNBUAH', 'ONTTRY', 'VETEUR', 'VETGBP', 'WINBRL', 'MIRBTC', 'MIRBUSD', 'MIRUSDT', 'BARBTC', 'BARBUSD', 'BARUSDT', 'FORTHBTC', 'FORTHBUSD', 'FORTHUSDT', 'CAKEGBP', 'DOGERUB', 'HOTBRL', 'WRXEUR', 'EZBTC', 'EZETH', 'BAKEUSDT', 'BURGERBUSD', 'BURGERUSDT', 'SLPBUSD', 'SLPUSDT', 'TRXAUD', 'TRXEUR', 'VETTRY', 'SHIBUSDT', 'SHIBBUSD', 'ICPBTC', 'ICPBNB', 'ICPBUSD', 'ICPUSDT', 'BTCGYEN', 'USDTGYEN', 'SHIBEUR', 'SHIBRUB', 'ETCEUR', 'ETCBRL', 'DOGEBIDR', 'ARBTC', 'ARBNB', 'ARBUSD', 'ARUSDT', 'POLSBTC', 'POLSBNB', 'POLSBUSD', 'POLSUSDT', 'MDXBTC', 'MDXBNB', 'MDXBUSD', 'MDXUSDT', 'MASKBNB', 'MASKBUSD', 'MASKUSDT', 'LPTBTC', 'LPTBNB', 'LPTBUSD', 'LPTUSDT', 'ETHUAH', 'MATICBRL', 'SOLEUR', 'SHIBBRL', 'AGIXBTC', 'ICPEUR', 'MATICGBP', 'SHIBTRY', 'MATICBIDR', 'MATICRUB', 'NUBTC', 'NUBNB', 'NUBUSD', 'NUUSDT', 'XVGUSDT', 'RLCBUSD', 'CELRBUSD', 'ATMBUSD', 'ZENBUSD', 'FTMBUSD', 'THETABUSD', 'WINBUSD', 'KAVABUSD', 'XEMBUSD', 'ATABTC', 'ATABNB', 'ATABUSD', 'ATAUSDT', 'GTCBTC', 'GTCBNB', 'GTCBUSD', 'GTCUSDT', 'TORNBTC', 'TORNBNB', 'TORNBUSD', 'TORNUSDT', 'MATICTRY', 'ETCGBP', 'SOLGBP', 'BAKEBTC', 'COTIBUSD', 'KEEPBTC', 'KEEPBNB', 'KEEPBUSD', 'KEEPUSDT', 'SOLTRY', 'RUNEGBP', 'SOLBRL', 'SCBUSD', 'CHRBUSD', 'STMXBUSD', 'HNTBUSD', 'FTTBUSD', 'DOCKBUSD', 'ADABIDR', 'ERNBNB', 'ERNBUSD', 'ERNUSDT', 'KLAYBTC', 'KLAYBNB', 'KLAYBUSD', 'KLAYUSDT', 'RUNEEUR', 'MATICAUD', 'DOTRUB', 'UTKBUSD', 'IOTXBUSD', 'PHAUSDT', 'SOLRUB', 'RUNEAUD', 'BUSDUAH', 'BONDBTC', 'BONDBNB', 'BONDBUSD', 'BONDUSDT', 'MLNBTC', 'MLNBNB', 'MLNBUSD', 'MLNUSDT', 'GRTTRY', 'CAKEBRL', 'ICPRUB', 'DOTAUD', 'AAVEBRL', 'EOSAUD', 'DEXEUSDT', 'LTOBUSD', 'ADXBUSD', 'QUICKBTC', 'QUICKBNB', 'QUICKBUSD', 'C98USDT', 'C98BUSD', 'C98BNB', 'C98BTC', 'CLVBTC', 'CLVBNB', 'CLVBUSD', 'CLVUSDT', 'QNTBTC', 'QNTBNB', 'QNTBUSD', 'QNTUSDT', 'FLOWBTC', 'FLOWBNB', 'FLOWBUSD', 'FLOWUSDT', 'XECBUSD', 'AXSBRL', 'AXSAUD', 'TVKUSDT', 'MINABTC', 'MINABNB', 'MINABUSD', 'MINAUSDT', 'RAYBNB', 'RAYBUSD', 'RAYUSDT', 'FARMBTC', 'FARMBNB', 'FARMBUSD', 'FARMUSDT', 'ALPACABTC', 'ALPACABNB', 'ALPACABUSD', 'ALPACAUSDT', 'TLMTRY', 'QUICKUSDT', 'ORNBUSD', 'MBOXBTC', 'MBOXBNB', 'MBOXBUSD', 'MBOXUSDT', 'VGXBTC', 'VGXETH', 'FORUSDT', 'REQUSDT', 'GHSTUSDT', 'TRURUB', 'FISBRL', 'WAXPUSDT', 'WAXPBUSD', 'WAXPBNB', 'WAXPBTC', 'TRIBEBTC', 'TRIBEBNB', 'TRIBEBUSD', 'TRIBEUSDT', 'GNOUSDT', 'GNOBUSD', 'GNOBNB', 'GNOBTC', 'ARPATRY', 'PROMBTC', 'MTLBUSD', 'OGNBUSD', 'XECUSDT', 'C98BRL', 'SOLAUD', 'SUSHIBIDR', 'XRPBIDR', 'POLYBUSD', 'ELFUSDT', 'DYDXUSDT', 'DYDXBUSD', 'DYDXBNB', 'DYDXBTC', 'ELFBUSD', 'POLYUSDT', 'IDEXUSDT', 'VIDTUSDT', 'SOLBIDR', 'AXSBIDR', 'BTCUSDP', 'ETHUSDP', 'BNBUSDP', 'USDPBUSD', 'USDPUSDT', 'GALAUSDT', 'GALABUSD', 'GALABNB', 'GALABTC', 'FTMBIDR', 'ALGOBIDR', 'CAKEAUD', 'KSMAUD', 'WAVESRUB', 'SUNBUSD', 'ILVUSDT', 'ILVBUSD', 'ILVBNB', 'ILVBTC', 'RENBUSD', 'YGGUSDT', 'YGGBUSD', 'YGGBNB', 'YGGBTC', 'STXBUSD', 'SYSUSDT', 'DFUSDT', 'SOLUSDC', 'ARPARUB', 'LTCUAH', 'FETBUSD', 'ARPABUSD', 'LSKBUSD', 'AVAXBIDR', 'ALICEBIDR', 'FIDAUSDT', 'FIDABUSD', 'FIDABNB', 'FIDABTC', 'DENTBUSD', 'FRONTUSDT', 'CVPUSDT', 'AGLDBTC', 'AGLDBNB', 'AGLDBUSD', 'AGLDUSDT', 'RADBTC', 'RADBNB', 'RADBUSD', 'RADUSDT', 'UNIAUD', 'HIVEBUSD', 'STPTBUSD', 'BETABTC', 'BETABNB', 'BETABUSD', 'BETAUSDT', 'SHIBAUD', 'RAREBTC', 'RAREBNB', 'RAREBUSD', 'RAREUSDT', 'AVAXBRL', 'AVAXAUD', 'LUNAAUD', 'TROYBUSD', 'AXSETH', 'FTMETH', 'SOLETH', 'SSVBTC', 'SSVETH', 'LAZIOTRY', 'LAZIOEUR', 'LAZIOBTC', 'LAZIOUSDT', 'CHESSBTC', 'CHESSBNB', 'CHESSBUSD', 'CHESSUSDT', 'FTMAUD', 'FTMBRL', 'SCRTBUSD', 'ADXUSDT', 'AUCTIONUSDT', 'CELOBUSD', 'FTMRUB', 'NUAUD', 'NURUB', 'REEFTRY', 'REEFBIDR', 'SHIBDOGE', 'DARUSDT', 'DARBUSD', 'DARBNB', 'DARBTC', 'BNXBTC', 'BNXBNB', 'BNXBUSD', 'BNXUSDT', 'RGTUSDT', 'RGTBTC', 'RGTBUSD', 'RGTBNB', 'LAZIOBUSD', 'OXTBUSD', 'MANATRY', 'ALGORUB', 'SHIBUAH', 'LUNABIDR', 'AUDUSDC', 'MOVRBTC', 'MOVRBNB', 'MOVRBUSD', 'MOVRUSDT', 'CITYBTC', 'CITYBNB', 'CITYBUSD', 'CITYUSDT', 'ENSBTC', 'ENSBNB', 'ENSBUSD', 'ENSUSDT', 'SANDETH', 'DOTETH', 'MATICETH', 'ANKRBUSD', 'SANDTRY', 'MANABRL', 'KP3RUSDT', 'QIUSDT', 'QIBUSD', 'QIBNB', 'QIBTC', 'PORTOBTC', 'PORTOUSDT', 'PORTOTRY', 'PORTOEUR', 'POWRUSDT', 'POWRBUSD', 'AVAXETH', 'SLPTRY', 'FISTRY', 'LRCTRY', 'CHRETH', 'FISBIDR', 'VGXUSDT', 'GALAETH', 'JASMYUSDT', 'JASMYBUSD', 'JASMYBNB', 'JASMYBTC', 'AMPBTC', 'AMPBNB', 'AMPBUSD', 'AMPUSDT', 'PLABTC', 'PLABNB', 'PLABUSD', 'PLAUSDT', 'PYRBTC', 'PYRBUSD', 'PYRUSDT', 'RNDRBTC', 'RNDRUSDT', 'RNDRBUSD', 'ALCXBTC', 'ALCXBUSD', 'ALCXUSDT', 'SANTOSBTC', 'SANTOSUSDT', 'SANTOSBRL', 'SANTOSTRY', 'MCBTC', 'MCBUSD', 'MCUSDT', 'BELTRY', 'COCOSBUSD', 'DENTTRY', 'ENJTRY', 'NEORUB', 'SANDAUD', 'SLPBIDR', 'ANYBTC', 'ANYBUSD', 'ANYUSDT', 'BICOBTC', 'BICOBUSD', 'BICOUSDT', 'FLUXBTC', 'FLUXBUSD', 'FLUXUSDT', 'ALICETRY', 'FXSUSDT', 'GALABRL', 'GALATRY', 'LUNATRY', 'REQBUSD', 'SANDBRL', 'MANABIDR', 'SANDBIDR', 'VOXELBTC', 'VOXELBNB', 'VOXELBUSD', 'VOXELUSDT', 'COSBUSD', 'CTXCBUSD', 'FTMTRY', 'MANABNB', 'MINATRY', 'XTZTRY', 'HIGHBTC', 'HIGHBUSD', 'HIGHUSDT', 'CVXBTC', 'CVXBUSD', 'CVXUSDT', 'PEOPLEBTC', 'PEOPLEBUSD', 'PEOPLEUSDT', 'OOKIBUSD', 'OOKIUSDT', 'COCOSTRY', 'GXSBNB', 'LINKBNB', 'LUNAETH', 'MDTBUSD', 'NULSBUSD', 'SPELLBTC', 'SPELLUSDT', 'SPELLBUSD', 'USTBTC', 'USTBUSD', 'USTUSDT', 'JOEBTC', 'JOEBUSD', 'JOEUSDT', 'ATOMETH', 'DUSKBUSD', 'EGLDETH', 'ICPETH', 'LUNABRL', 'LUNAUST', 'NEARETH', 'ROSEBNB', 'VOXELETH', 'ALICEBNB', 'ATOMTRY', 'ETHUST', 'GALAAUD', 'LRCBNB', 'ONEETH', 'OOKIBNB']

        self.arb_df = pd.DataFrame(columns=self.symbols, index=self.symbols)

        self.streams = []

    def load_prices(self):
        for s1 in tqdm(self.symbols):
            for s2 in self.symbols:
                pair = s1 + s2
                if pair in self.all_pairs:
                    i_res = self.b_client.get_order_book(symbol=pair)
                    if len(i_res['bids']) > 0:
                        self.arb_df[s1][s2] = float(i_res['bids'][0][0])
                        self.arb_df[s2][s1] = 1 / float(i_res['asks'][0][0])
                    else:
                        print(pair)

        self.arb_df = self.arb_df.astype(float)

    def bellman_ford_negative_cycles(self, g, s):
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

    def all_negative_cycles(self, g):
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
            all_paths.append(self.bellman_ford_negative_cycles(g, v))
        flatten = lambda l: [item for sublist in l for item in sublist]
        return [list(i) for i in set(tuple(j) for j in flatten(all_paths))]

    def calculate_arb(self, cycle, g, verbose=True):
        """
        For a given negative-weight cycle on the log graph, calculate and
        print the arbitrage

        :param cycle: the negative-weight cycle
        :type cycle: list
        :param g: graph
        :type g: networkx weighted DiGraph
        :param verbose: whether to print path and arb
        :type verbose: bool
        :return: fractional value of the arbitrage
        :rtype: float
        """
        total = 0
        for (p1, p2) in zip(cycle, cycle[1:]):
            total += g[p1][p2]["weight"]
        # print(total)
        arb = np.exp(-total) - 1
        # if verbose:
            # print("Path:", cycle)
            # print(f"{arb*100:.2g}%\n")
        return arb

    def find_arbitrage(self, df, find_all=False, sources=None):
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
        g = nx.DiGraph(-np.log(df).fillna(0).T)

        if nx.negative_edge_cycle(g):
            # print("ARBITRAGE FOUND\n" + "=" * 15 + "\n")

            if find_all:
                unique_cycles = self.all_negative_cycles(g)
            else:
                all_paths = []
                for s in sources:
                    all_paths.append(self.bellman_ford_negative_cycles(g, s))
                flatten = lambda l: [item for sublist in l for item in sublist]
                unique_cycles = [list(i) for i in set(tuple(j) for j in flatten(all_paths))]

            for p in unique_cycles:
                self.calculate_arb(p, g)
            return unique_cycles

        else:
            print("No arbitrage opportunities")
            return None

    def handle_socket_message(self, msg):
        # print(f"message type: {msg['e']}")
        print(msg)

    def add_depth_socket(self, symbol):
        print(self.streams)
        self.b_twm.start_depth_socket(callback=self.handle_socket_message, symbol=symbol)
        self.streams.append(symbol.lower() + '@bookTicker')
        print(self.streams)

    def start_multiplex_socket(self):
        self.b_twm.start_multiplex_socket(callback=self.handle_socket_message, streams=self.streams)
        self.b_twm.join()


if __name__ == '__main__':

    n_arb = n_arbitrage()
    print("itt")
    # n_arb.load_prices()
    # print(n_arb.arb_df)
    # print('------------------')
    # print(n_arb.find_arbitrage(df=n_arb.arb_df, find_all=True))

    n_arb.add_depth_socket("BNBBTC")
    n_arb.add_depth_socket("ETHBTC")
    n_arb.start_multiplex_socket()


    for i in range(10):
        print(i)
        time.sleep(1)

        # ide oda




#
#
#
#     twm.start_kline_socket(callback=handle_socket_message, symbol=symbol)
#
#     # multiple sockets can be started
#     twm.start_depth_socket(callback=handle_socket_message, symbol=symbol)
#
#     # or a multiplex socket can be started like this
#     # see Binance docs for stream names
#     streams = ['bnbbtc@miniTicker', 'bnbbtc@bookTicker']
#     twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)
#
#     twm.join()
#
#
# if __name__ == "__main__":
#    main()

