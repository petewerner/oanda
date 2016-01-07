#!/usr/bin/env python

###
# streaming events or prices from OANDA with ZeroMQ - Pete Werner, 2016
# more info: http://petewerner.blogspot.com/2016/01/streaming-oanda-with-python-and-zeromq.html
###

import zmq
import requests
from requests.exceptions import ConnectionError
import sys
import time

#oanda settings
acct_id = 'YOUR_ACCOUNT_ID'
api_key = 'YOUR_API_KEY'
stream_host = 'stream-fxpractice.oanda.com'

#our local zeromq endpoints
zmq_events_addr = 'tcp://127.0.0.1:8008'
zmq_events_filter = 'EVT'
zmq_prices_addr = 'tcp://127.0.0.1:8009'
zmq_prices_filter = 'TCK'

class OandaStream(object):

	def __init__(self, name, zmq_addr, zmq_filter, timeout):
		self.name = name
		self.zmq_addr = zmq_addr
		self.zmq_filter = zmq_filter
		self.timeout = timeout
		self.zmq_sock = None
		self.oda_conn = None

	def stream_open(self, params):
		"""open the OANDA stream"""
		url = 'https://%s/v1/%s' % (stream_host, self.name)
		headers = {'Authorization': 'Bearer %s' % api_key} 
		res = requests.get(url, headers=headers, params=params, stream=True, timeout=self.timeout)
		if res.status_code != 200:
			print "OANDA request error %s" % (res.status_code)
			raise ConnectionError(res.content)
		return res

	def zmq_socket(self):
		"""create ZeroMQ publisher socket"""
		sock = zmq.Context().socket(zmq.PUB)
		print "ZMQ: opening %s" % (self.zmq_addr)
		sock.bind(self.zmq_addr)
		return sock
	
	def main_loop(self):
		"""receive data from oanda, and pass it on via zmq"""
		print "%s stream starting" % (self.name)
		for line in self.oda_conn.iter_lines(1):
			if not line:
				continue
			#heartbeat, print to stdout but dont pass via zmq
			if line[0:12].find('heartbeat') != -1:
				print line
				continue
			#oanda disconnected us for some reason
			if line[0:20].find('disconnect') != -1:	
				raise ConnectionError(line)
			#join our filter string and data from oanda 
			msg = "%s:%s" % (self.zmq_filter, line)
			print "sending: %s" % (msg)
			self.zmq_sock.send_string(msg)

	def stream(self, params):
		"""start the stream, and handle re-connection"""

		self.zmq_sock = self.zmq_socket()
		
		while True:
			#initially not connected to oanda
			connected = False
			ntries = 0
			while not connected:
				try:
					#try to connect to the oanda stream
					self.oda_conn = self.stream_open(params)
					connected = True
				except ConnectionError as e:
					#couldnt connect, wait and try again
					ntries += 1
					print "connection to oanda failed, %s, try %d" % (e.message, ntries)
					wait_time = min(60, ntries * 2)
					print "waiting %ds before reconnection" % (wait_time)
					time.sleep(wait_time)
				
			try:
				#now connected to oanda, stream events
				self.main_loop()
			except ConnectionError as e:
				print "caught exception: %s" % (e.message)


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "usage: %s [events|prices|client]" % (sys.argv[0])
		sys.exit(1)

	mode = sys.argv[1]
	if mode == 'events':
		#stream account events
		oda = OandaStream(mode, zmq_events_addr, zmq_events_filter, 20)
		params = {'accountIds' : acct_id}
		oda.stream(params)
	elif mode == 'prices':
		#stream tick data for the below instruments
		instruments = ['EUR_USD', 'AUD_USD']
		oda = OandaStream(mode, zmq_prices_addr, zmq_prices_filter, 10)
		params = {'accountId' : acct_id,
				'instruments' : ','.join(instruments)
			}
		oda.stream(params)
	elif mode == 'client':
		#subscribe to the events and prices feed
		ctx = zmq.Context()
		sock = ctx.socket(zmq.SUB)
		sock.connect(zmq_events_addr)
		sock.setsockopt(zmq.SUBSCRIBE, zmq_events_filter)
		sock.connect(zmq_prices_addr)
		sock.setsockopt(zmq.SUBSCRIBE, zmq_prices_filter)
		while True:
			s = sock.recv_string()
			event = s[:4]
			msg = s[4:] 
			print "event: %s, msg %s" % (event, msg)
	else:
		print "usage: %s [events|prices|client]" % (sys.argv[0])
		sys.exit(1)

