import threading
from time import sleep

import alpaca_trade_api as tradeapi
import pandas as pd

base_url = 'https://paper-api.alpaca.markets'
data_url = 'wss://data.alpaca.markets'
trade_taken = False

# instantiate REST API
api = tradeapi.REST(base_url=base_url, api_version='v2')

# init WebSocket
conn = tradeapi.stream2.StreamConn(
	base_url=base_url, data_url=data_url, data_stream='alpacadatav1'
)


def wait_for_market_open():
	clock = api.get_clock()
	if not clock.is_open:
		time_to_open = clock.next_open - clock.timestamp
		sleep_time = round(time_to_open.total_seconds())
		sleep(sleep_time)
	return clock


# define websocket callbacks
data_df = None


@conn.on(r'^T.ENZL$')
async def on_second_bars_EWN(conn, channel, bar):
	if data_df is not None:
		data_df.enzl[-1] = bar.price


@conn.on(r'^T.EWA$')
async def on_second_bars_ENZL(conn, channel, bar):
	if data_df is not None:
		data_df.ewa[-1] = bar.price


# start WebSocket in a thread
streams = ['T.ENZL', 'T.EWA']
ws_thread = threading.Thread(target=conn.run, daemon=True, args=(streams,))
ws_thread.start()


# main
while True:

	clock = wait_for_market_open()

	ewa = api.get_barset('EWA', 'day', limit=25)
	enzl = api.get_barset('ENZL', 'day', limit=25)

	data_df = pd.concat(
		[ewa.df.EWA.close, enzl.df.ENZL.close],
		axis=1,
		join='inner',
		keys=['ewa', 'enzl'],
	)
	data_df.enzl[-1] = 0
	data_df.ewa[-1] = 0

	spread_df = data_df.pct_change()
	spread_df = spread_df[:-1]
	spread_df['spread'] = spread_df.ewa - spread_df.enzl
	max_divergence = spread_df.spread.tail(20).abs().max()

	while data_df.enzl[-1] == 0 or data_df.ewa[-1] == 0:
		sleep(1)


	while not trade_taken:
		# check for trade opportunities
		spread_df = data_df.pct_change()[:2]
		spread_df['spread'] = spread_df.ewa - spread_df.enzl

		if abs(spread_df.spread[-1]) > max_divergence:
			# there is a trade - calculate position sizing
			acct = api.get_account()
			acct_size = float(acct.equity)
			ewa_size = round(acct_size / data_df.ewa[-1])
			enzl_size = round(acct_size / data_df.enzl[-1])

			if spread_df.spread[-1] < 0:
				# EWA - ENZL is negative -> Long EWA short ENZL
				long_ewa = True
				ewa_side = 'buy'
				enzl_side = 'sell'

			else:
				# EWA - ENZL is positive -> Short EWA long ENZL
				long_ewa = False
				ewa_side = 'sell'
				enzl_side = 'buy'

			# submit order
			api.submit_order(
				symbol='EWA',
				qty=ewa_size,
				side=ewa_side,
				time_in_force='gtc',
				type='market',
			)

			api.submit_order(
				symbol='ENZL',
				qty=enzl_size,
				side=enzl_side,
				time_in_force='gtc',
				type='market',
			)

			trade_taken = True
			stop_loss = acct_size * 0.02 * -1
			take_profit = acct_size * max_divergence
			break

		sleep(1)

		# check if the market is still open
		if pd.Timestamp.now(tz='America/New_York') > clock.next_close:
			trade_taken = False
			break


	while trade_taken:
		# In a trade - check for exit
		pnl = data_df.ewa[-1] * ewa_size - data_df.enzl[-1] * enzl_size

		if not long_ewa:
			pnl *= -1  # inverse the p&l calculation

		if pnl < stop_loss or pnl > take_profit:
			# Either stop or take profit hit - close trade
			api.close_position('EWA')
			api.close_position('ENZL')
			trade_taken = False
			break

		if pd.Timestamp.now(tz='America/New_York') > clock.next_close:
			break

