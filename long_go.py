import asyncio
import sys
# import os
import requests
import numpy as np
from functools import reduce
import time
from threading import Thread
# import textwrap


# Binanace
from binance import AsyncClient, BinanceSocketManager
from binance.helpers import round_step_size


# nDot
# from n_riport import n_riport


class n_arbitrage:
	
	def __init__(self):
		self.official_fee = 0.075  # %   for profit calc
		self.spread = 0.09  # %
		self.prices_fallen = 0.35  # %
		self.official_fee_mod = 1 - (self.official_fee / 100)
		self.spread_mod = 1 - (self.spread / 100)
		self.history_length = 80
		self.speed_delay = 0.085
		self.stop_loss = -1.9  # %

		# self.riport = n_riport()
		# self.stop_tradeing_at_USDT = 60
		
		self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
		self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
		self.b_client = None
		self.account = self.get_account()
		self.exchange_info = self.get_exchange_info()
		

		# self.symbols = ['AGLD', 'STPT', 'MXN', 'UGX', 'RENBTC', 'GLM', 'RAY', 'NEAR', 'AUDIO', 'HNT', 'ADADOWN', 'CDT', 'SPARTA', 'SUSD', 'FARM', 'XNO', 'AION', 'NPXS', 'DGB', 'ZRX', 'BCD', 'EASY', 'SANTOS', 'WING', 'WNXM', 'BCH', 'JST', 'ADAUP', 'HOT', 'AR', 'IRIS', 'RAMP', 'BCX', 'SEK', 'TRIG', 'RCN', 'COVER', 'FLM', 'GNO', 'VITE', 'GNT', 'BKRW', 'CFX', 'XPR', 'SFP', 'DIA', 'RDN', 'ACA', 'ARDR', 'LOOMOLD', 'NEBL', 'ACH', 'SLPOLD', 'BEL', 'JUV', 'ACM', 'MINA', 'GRTDOWN', 'VTHO', 'PYROLD', 'SGB', 'SALT', 'STORM', 'REN', 'REP', 'ADA', 'ELF', 'REQ', 'STORJ', 'CHF', 'ADD', 'BZRX', 'SGT', 'DF', 'RARE', 'EOSDOWN', 'PAXG', 'YOYO', 'PAX', 'CHR', 'VND', 'BCHDOWN', 'WAVES', 'CHZ', 'ADX', 'XRP', 'WPR', 'JASMY', 'AED', 'FIDA', 'SAND', 'DKK', 'OCEAN', 'FOR', 'UMA', 'DREPOLD', 'SCRT', 'TUSD', 'EZ', 'TKO', 'WABI', 'RGT', 'IDRT', 'ENG', 'ENJ', 'UNIDOWN', 'YFII', 'KZT', 'OAX', 'GRT', 'GRS', 'UND', 'HARD', 'TFUEL', 'ENS', 'LEND', 'DLT', 'TROY', 'XLMUP', 'UNI', 'BTCDOWN', 'TLM', 'HUF', 'SBTC', 'CKB', 'WRX', 'XTZ', 'LUNA', 'ETHDOWN', 'AGI', 'BCHA', 'EON', 'EOP', 'EOS', 'GO', 'NCASH', 'RIF', 'NSBT', 'SKL', 'XDATA', 'GTC', 'PEN', 'BLINK', 'SOLO', 'SXPDOWN', 'HC', 'SKY', 'BURGER', 'NAS', 'NAV', 'GTO', 'WTC', 'XVG', 'EPS', 'DNT', 'CLV', 'FLOW', 'XTZDOWN', 'XVS', 'STEEM', 'BVND', 'SLP', 'VRT', 'NBS', 'DON', 'LAZIO', 'DOT', 'IQ', 'GRTUP', '1INCH', 'KNCL', 'CHESS', 'MITH', 'ERD', 'DEGO', 'CND', 'GYEN', 'UNFI', 'FTM', 'POWR', 'ERN', 'GVT', 'WINGS', 'FTT', 'VOXEL', 'PHA', 'RLC', 'PHB', 'TRXDOWN', 'ATOM', 'XRPUP', 'QUICK', 'BLZ', 'SNM', 'BOBA', 'MBL', 'MTLX', 'SNT', 'PHP', 'SNX', 'LTCDOWN', 'FUN', 'SNMOLD', 'COP', 'COS', 'API3', 'USD', 'QKC', 'SUSHIUP', 'ROSE', 'GLMR', 'XYM', 'PURSE', 'SOL', 'TRXUP', 'CITY', 'ETC', 'BNC', 'CELR', 'UST', 'OGN', 'ETH', 'NEO', 'TOMO', 'CELO', 'KLAY', 'AUCTION', 'BADGER', 'HIGH', 'GXS', 'TRB', 'BNT', 'QLC', 'LBA', 'MDA', 'BNX', 'UTK', 'WSOL', 'HEGIC', 'MA', 'AMB', 'MC', 'TRU', 'FUEL', 'DREP', 'TRY', 'TRX', 'MDT', 'NFT', 'MDX', 'XRPDOWN', 'AERGO', 'EUR', 'AMP', 'BOT', 'NULS', 'AUTO', 'NGN', 'ANC', 'BDOT', 'EGLD', 'ANTOLD', 'SPELL', 'PUNDIX', 'FXS', 'PLA', 'HNST', 'EVX', 'CRV', 'BAKE', 'ANT', 'NU', 'FLUX', 'ANY', 'LINKUP', 'SRM', 'QISWAP', 'TORN', 'PLN', 'QNT', 'ALICE', 'OG', 'MFT', 'OM', 'BTTOLD', 'BETH', 'BQX', 'WETH', 'PHBV1', 'BETA', 'BRD', 'SSV', 'BUSD', 'CTK', 'ARPA', 'DOTDOWN', 'BRL', 'ALCX', 'CTR', 'MATIC', 'IOTX', 'SHIB', 'TVK', 'FRONT', 'ZAR', 'DOCK', 'STX', 'PNT', 'QI', 'DENT', 'MBOX', 'SUB', 'POA', 'IOST', 'CAKE', 'ETHUP', 'POE', 'OMG', 'BAND', 'SUN', 'ASTR', 'SUNOLD', 'BTC', 'TWT', 'NKN', 'RSR', 'IOTA', 'CVC', 'REEF', 'BTG', 'MIR', 'KES', 'ARK', 'LOKA', 'CVP', 'ARN', 'KEY', 'BTS', 'SPARTAOLD', 'ARS', 'CVX', 'ONE', 'LINKDOWN', 'ONG', 'ANKR', 'SUSHI', 'ALGO', 'SC', 'WBTC', 'ONT', 'PPT', 'ONX', 'BTTC', 'RUB', 'PIVX', 'ASR', 'FIRO', 'AXSOLD', 'AST', 'MANA', 'DOTUP', 'ATA', 'MEETONE', 'QSP', 'ATD', 'NMR', 'MKR', 'DODO', 'LIT', 'ICP', 'ZEC', 'ATM', 'APPC', 'JEX', 'ICX', 'LOOM', 'ZEN', 'KP3R', 'DOGE', 'DUSK', 'ALPHA', 'BOLT', 'SXP', 'HBAR', 'RVN', 'MLN', 'AUD', 'LTOOLD', 'IDR', 'CTSI', 'KAVA', 'C98', 'PSG', 'HCC', 'VIDT', 'NOK', 'AVA', 'SYS', 'COCOS', 'STRAX', 'EOSUP', 'CZK', 'GAS', 'COVEROLD', 'AAVEDOWN', 'THETA', 'BCHUP', 'WAN', 'ORN', 'PERL', 'XLMDOWN', 'MASK', 'AAVE', 'GBP', 'PERP', '1INCHUP', 'SXPUP', 'YFIDOWN', 'BOND', 'YFI', 'PERLOLD', 'MOD', 'BICO', 'OST', 'XEC', 'YGG', 'PEOPLE', 'AXS', 'ZIL', 'VAI', 'XEM', 'CTXC', 'KEYFI', 'XTZUP', 'BIDR', 'BCHSV', 'AAVEUP', 'SUSHIDOWN', 'COMP', 'ETHBNT', 'OMOLD', 'OOKI', 'RUNE', 'FORTH', 'KMD', 'GHST', 'IDEX', 'DEXE', 'AVAX', 'UAH', 'KNC', 'PROS', 'PROM', 'BTCUP', 'CHAT', 'BGBP', 'LPT', 'HIVE', 'BIFI', 'PORTO', 'SNGLS', 'PYR', 'WAXP', 'DAI', 'YFIUP', 'DAR', 'FET', 'LRC', 'REPV1', 'ADXOLD', 'MTH', 'MTL', 'VET', 'ALPACA', 'USDT', 'USDS', 'OXT', 'USDP', 'DASH', 'NVT', 'SWRV', 'EDO', 'ILV', 'GHS', 'BTCST', 'HKD', 'JOE', 'LSK', 'KEEP', 'CAD', 'BEAM', 'CAN', 'DCR', 'CREAM', 'DATA', 'IMX', 'ENTRP', 'FILUP', 'UNIUP', 'LTC', 'USDC', 'WIN', 'LTCUP', 'INJ', 'TCT', 'PARA', 'LTO', 'VGX', 'TRIBE', 'NXS', 'EFI', 'DYDX', 'AGIX', 'INR', 'CBK', 'CBM', 'INS', 'POND', 'JPY', 'LINA', 'XLM', 'LINK', 'QTUM', 'FILDOWN', 'SUPER', 'UFT', 'POLS', 'KSM', 'LUN', 'FIL', 'POLY', 'STMX', 'RNDR', 'BAL', 'FIO', 'GALA', 'VIB', 'VIA', 'FIS', 'BAR', 'RAD', 'BAT', 'VRAB', 'AKRO', 'NZD', 'MOVR', 'XMR', '1INCHDOWN', 'COTI']
		
		# self.symbols = ['BTC', 'ETH', 'ADA', 'LINK', 'DOT', 'TRX', 'FTM', 'SOL', 'BUSD',
		# 				'USDT', 'MATIC', 'ETC', 'NEO', 'ENJ', 'WAVES', 'ATOM', 'ONE', 'ZEC',
		# 				'ONT', 'HOT', 'CHZ', 'WIN', 'AXS', 'GALA', 'ANKR', 'RUNE', 'ICP', 'LRC',
		# 				'ZIL', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS',
		# 				'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'ADX', 'NULS',
		# 				'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC',
		# 				'IOST', 'NANO', 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN',
		# 				'USDC', 'BCHSV', 'PHB', 'COCOS', 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI',
		# 				'SRM', 'KSM', 'SUSHI', 'BEL', 'NEAR', 'SLP', 'REEF', 'C98', 'MINA', 'VOXEL']

	# # USDT Setup -----------------------------------------------------------------
	# 	self.symbols = ["ATOM", "BTC", "ETH", "NMR", "SAND", "SOL", "FTM", "XRP",
	# 					"LUNA", "MANA", "NEAR", "AVAX", "TRX", "ROSE", "ONE", "ALGO", "DOT", "VET",
	# 					"ATOM", "LRC", "ETC", "LINK", "SHIB", "BCH", "THETA", "OMG", "ICP", "EGLD",
	# 					"FIL", "CRV", "SUSHI", "EOS", "DYDX", "ANT", "CHR", "ZEC", "DUSK", "CHZ",
	# 					"ENJ"]
	# 	self.quote_symbols = ["USDT"]
	# 	self.max_open_position = 10
	# 	self.stock_size = 800  # usd

		# USDT Setup -----------------------------------------------------------------
		self.symbols = ['ADA', 'ALGO', 'ALICE', 'ANT', 'ATOM', 'AVAX', 'AXS', 'BCH', 'BNB', 'BTC', 'BUSD',
						'CAKE', 'CFX', 'CHR', 'CHZ', 'COCOS', 'CRV', 'DAR', 'DOGE', 'DOT', 'DYDX', 'EGLD',
						'ENJ', 'ENS', 'EOS', 'ETC', 'ETH', 'FIL', 'FTM', 'GALA', 'GLMR', 'HNT', 'ICP', 'IMX',
						'JST', 'KAVA', 'LINK', 'LRC', 'LTC', 'LUNA', 'MANA', 'MATIC', 'MBOX', 'NEAR',
						'ONE', 'PEOPLE', 'PNT', 'ROSE', 'RUNE', 'SAND', 'SHIB', 'SOL', 'SUN', 'SUSHI',
						'TFUEL', 'THETA', 'TLM', 'TRX', 'USDC', 'UST', 'VET', 'VOXEL', 'WIN', 'XRP', 'ZEC']
		
		## DAR OFF
		self.quote_symbols = ["USDT"]
		self.max_open_position = 18
		self.stock_size = 450  # usd and eur

		# BTC SETUP --------------------------------------------------------------------
	# 	self.symbols = ['ADA', 'ATOM', 'AVAX', 'AXS', 'BNB', 'DOT', 'ENJ', 'ETH', 'FTM',
	# 					'GALA', 'LINK', 'LRC', 'LTC', 'LUNA', 'MANA', 'MATIC', 'NEAR',
	# 					'SAND', 'SOL', 'TFUEL', 'XRP']
	# 	self.quote_symbols = ["BTC"]
	# 	self.max_open_position = 10
	# 	self.stock_size = .025  # usd


		# OFF BNB
		
		self.all_pairs = self.defa_all_pairs()
		
		self.selected_symbols = self.symbols ## kiválasztam amivel dolgozok
		# self.commission = self.defa_commission()
		self.selected_pairs = self.defa_selected_pairs()  ##a kiválasztott szimbólumokhoz kapcsolódó párokat kiválasztom
		self.convert_multiplier = self.defa_convert_multiplier()

		self.max_fallen_array = []
		self.max_fallen_symbols_list = []
		self.historical_price = {}
		self.moving_avg_price = {}
		self.moving_avg_window = 4
		# self.long_short_none = {}
		self.symbol_traded_price = {}
		self.defa_current_and_fallen()

		self.open_positions = []

		
		self.socket_list = self.defa_socket_list()
		
		# self.riport.live_text = self.get_settings()
		# self.riport.stamp_live()
		
		self.pair_info = self.defa_pair_info()
		self.estimated_amount = self.defa_estimated_amount()
		self.summa_amount_USDT = 0.0

	def moving_average(self, x, w=7):
		return np.convolve(x, np.ones(w), 'valid') / w

	def print_long(self):
		print_str = ""
		for symbol in self.open_positions:
			gap = round((1 - ((1/self.symbol_traded_price[symbol]) / self.convert_multiplier[symbol])) * 100, 3)
			print_str += symbol + " " + str(gap) + "% "
		print("  Liquidtion loss:", self.liquidation_value_oop(), "USDT Long positions:", print_str)

	def liquidation_value_oop(self):
		i_profit = 0
		for symbol in self.open_positions:
			i_profit += (self.stock_size * ((self.official_fee_mod ** 2) * self.symbol_traded_price[symbol] * self.convert_multiplier[symbol])) - self.stock_size
		return round(i_profit, 8)

	def defa_current_and_fallen(self):
		self.max_fallen_array = []
		self.max_fallen_symbols_list = []
		# i_current_price = {}
		for si1 in self.selected_symbols:
			for si2 in self.quote_symbols:
				self.historical_price[si1 + si2] = [0.0] * self.history_length
				self.historical_price[si2 + si1] = [0.0] * self.history_length
				self.moving_avg_price[si1 + si2] = [0.0] * self.history_length
				self.moving_avg_price[si2 + si1] = [0.0] * self.history_length
				# self.long_short_none[si1 + si2] = "NONE"
				self.symbol_traded_price[si1 + si2] = 0.0
				self.max_fallen_symbols_list.append(si1 + si2)  # orig_pairs
				self.max_fallen_array.append(0.0)  # orig_pairs
				self.max_fallen_symbols_list.append(si2 + si1)  # invert_pairs
				self.max_fallen_array.append(0.0)  # invert_pairs
		return
	
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
	
	# def get_settings(self):
	# 	i_r = ""
	# 	i_r += "Fee: " + str(self.fee) + "%" + "\n"
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
		percent = percent / 100
		if "BNB" in self.estimated_amount.keys():
			self.estimated_amount["BNB"] = self.estimated_amount["BNB"] * (1 - percent)
	
	# def defa_commission(self):
	# 	comission = {}
	# 	for sesy in self.selected_symbols:
	# 		comission[sesy] = 0.0
	# 	comission["BNB"] = 0.0
	# 	return comission
	
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
			i_socket_list.append(sp.lower() + '@trade')
		return i_socket_list
	
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
			for si2 in self.quote_symbols:
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

	def start(self):
		self.socket_thread = Thread(target=self.start_asyc_websocket, daemon=True)
		self.socket_thread.start()

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
				if res["stream"][-5:] == "trade":
					self.historical_price[res['data']['s']].append(float(res['data']['p']))
					self.historical_price[res['data']['s']] = self.historical_price[res['data']['s']][1:self.history_length + 1]
					self.moving_avg_price[res['data']['s']] = self.moving_average(self.historical_price[res['data']['s']], self.moving_avg_window)
					# print(self.current_price[res['data']['s']])
					position = self.max_fallen_symbols_list.index(res['data']['s'])
					self.max_fallen_array[position] = ((1 - (self.historical_price[res['data']['s']][-1] / max(self.historical_price[res['data']['s']]))) * 100)
				else:
					s1 = self.selected_pairs[res['data']['s']][0]
					s2 = self.selected_pairs[res['data']['s']][1]
				
					self.convert_multiplier[s1 + s2] = float(res['data']['b'])
					self.convert_multiplier[s2 + s1] = (1 / float(res['data']['a']))

		await client.close_connection()
	
	def start_asyc_websocket(self):
		loop = asyncio.new_event_loop()
		asyncio.set_event_loop(loop)
		
		loop.run_until_complete(self.asyc_websocket())
		loop.close()


if __name__ == '__main__':
	n_arb = n_arbitrage()
	n_arb.start()
	print("Start Long go -------------------")
	# orig_pair = n_arb.orig_pair
	# invert_pair = n_arb.invert_pair
	# n_arb.print_estimated_amount()
	for i in range(5):
		print("\r", i, end="")
		time.sleep(1)
	# while n_arb.current_price[orig_pair][-1] == 0:
	# 	pass
	
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

	sum_profit = 0
	stock_size = n_arb.stock_size

	turn_count = 0
	while True:
		turn_count += 1
		time.sleep(n_arb.speed_delay)
		max_value = max(n_arb.max_fallen_array)
		max_pos = n_arb.max_fallen_array.index(max_value)
		max_symbol = n_arb.max_fallen_symbols_list[max_pos]
		max_invert_symbol = n_arb.selected_pairs[max_symbol][1] + n_arb.selected_pairs[max_symbol][0]
		# print("\r",
		# 	  max_symbol,
		# 	  max_value,
		# 	  max_invert_symbol,
		# 	  end="")
		if max_symbol not in n_arb.open_positions \
				and max_value > n_arb.prices_fallen \
				and n_arb.moving_avg_price[max_symbol][-1] > n_arb.moving_avg_price[max_symbol][-2] < n_arb.moving_avg_price[max_symbol][-3]:
			if len(n_arb.open_positions) < n_arb.max_open_position:
				n_arb.symbol_traded_price[max_symbol] = n_arb.convert_multiplier[max_invert_symbol]
				print("LONG:", max_symbol, 1 / n_arb.symbol_traded_price[max_symbol])
				n_arb.max_fallen_array[max_pos] = 0
				n_arb.historical_price[max_symbol] = [0.0] * n_arb.history_length
				# n_arb.long_short_none[max_symbol] = "LONG"
				n_arb.open_positions.append(max_symbol)
				n_arb.print_long()
				turn_count = 0
			else:
				l_value = 0.0
				l_symbol = ""
				for symbol in n_arb.open_positions:
					gap = (1 - ((1 / n_arb.symbol_traded_price[symbol]) / n_arb.convert_multiplier[symbol])) * 100
					if l_value > gap:
						l_value = gap
						l_symbol = symbol
				if l_value < n_arb.stop_loss:
					i_profit = ((n_arb.official_fee_mod ** 2) * n_arb.symbol_traded_price[l_symbol] *
								n_arb.convert_multiplier[l_symbol])
					print("STOP LOSS for free slot:", l_symbol, n_arb.convert_multiplier[l_symbol], "   Profit: ", i_profit)
					max_pos = n_arb.max_fallen_symbols_list.index(l_symbol)
					n_arb.max_fallen_array[max_pos] = 0
					n_arb.historical_price[l_symbol] = [0.0] * n_arb.history_length
					n_arb.symbol_traded_price[l_symbol] = 0.0
					# n_arb.long_short_none[l_symbol] = "NONE"
					sum_profit += ((stock_size * i_profit) - stock_size)
					print("  Sum profit:", stock_size, sum_profit)
					n_arb.open_positions.remove(l_symbol)
					n_arb.print_long()
					turn_count = 0
				else:
					print("Not enough free slot for:", max_symbol)
					max_pos = n_arb.max_fallen_symbols_list.index(max_symbol)
					n_arb.max_fallen_array[max_pos] = 0
					n_arb.historical_price[max_symbol] = [0.0] * n_arb.history_length
					n_arb.symbol_traded_price[max_symbol] = 0.0
					# n_arb.long_short_none[l_symbol] = "NONE"
					n_arb.print_long()
					turn_count = 0

		for symbol in n_arb.open_positions:
			if ((n_arb.spread_mod ** 2) * n_arb.symbol_traded_price[symbol] * n_arb.convert_multiplier[symbol]) > 1\
				and n_arb.historical_price[symbol][-1] < n_arb.historical_price[symbol][-2] > n_arb.historical_price[symbol][-3]:
				i_profit = ((n_arb.official_fee_mod ** 2) * n_arb.symbol_traded_price[symbol] * n_arb.convert_multiplier[symbol])
				print("STOP:", symbol, n_arb.convert_multiplier[symbol], "   Profit: ", i_profit)
				max_pos = n_arb.max_fallen_symbols_list.index(symbol)
				n_arb.max_fallen_array[max_pos] = 0
				n_arb.historical_price[symbol] = [0.0] * n_arb.history_length
				n_arb.symbol_traded_price[symbol] = 0.0
				# n_arb.long_short_none[symbol] = "NONE"
				sum_profit += ((stock_size * i_profit) - stock_size)
				print("  Sum profit:", stock_size, sum_profit)
				n_arb.open_positions.remove(symbol)
				n_arb.print_long()
				turn_count = 0
				
			# STOP LOSS
			elif ((1 - ((1 / n_arb.symbol_traded_price[symbol]) / n_arb.convert_multiplier[symbol])) * 100) < n_arb.stop_loss:
				i_profit = ((n_arb.official_fee_mod ** 2) * n_arb.symbol_traded_price[symbol] * n_arb.convert_multiplier[symbol])
				print("STOP LOSS (over limit):", symbol, n_arb.convert_multiplier[symbol], "   Profit: ", i_profit)
				max_pos = n_arb.max_fallen_symbols_list.index(symbol)
				n_arb.max_fallen_array[max_pos] = 0
				n_arb.historical_price[symbol] = [0.0] * n_arb.history_length
				n_arb.symbol_traded_price[symbol] = 0.0
				# n_arb.long_short_none[symbol] = "NONE"
				sum_profit += ((stock_size * i_profit) - stock_size)
				print("  Sum profit:", stock_size, sum_profit)
				n_arb.open_positions.remove(symbol)
				n_arb.print_long()
				turn_count = 0
		
		if turn_count > (240 / n_arb.speed_delay):
			n_arb.print_long()
			turn_count = 0
