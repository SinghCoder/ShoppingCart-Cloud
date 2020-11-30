from constants import Constants
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import *
from kazoo.recipe.watchers import ChildrenWatch
from crush import Crush
import sys
import os
import time
import random
import logging
import logging.handlers
# import gevent
# import zerorpc
import collections
import os
import socket
import json

debug = False

class Gateway():
	
	logger = logging.getLogger('gatewaylogger')
	logger.setLevel(logging.DEBUG)
	ch = logging.FileHandler('../logs/gateway.log', 'w')
	formatter = logging.Formatter('[%(asctime)s] %(message)s %(funcName)s:%(lineno)d')
	ch.setFormatter(formatter)
	logger.addHandler(ch)
	zk = None
	constants = Constants()
	crush_object = Crush()

	def __init__(self):
		super().__init__()		
		try:
			Gateway.zk = KazooClient(hosts='127.0.0.1:2181')
			Gateway.zk.start()
			# print(Gateway.constants.SERVER_PREFIX + Gateway.constants.MESSAGE_CONNECTED + "with 127.0.0.1:2181")	
			Gateway.zk.add_listener(self.connection_listener)
			ChildrenWatch(Gateway.zk, '/nodes', func=Gateway.handle_dbnodes_change)
		except Exception as e:
			Gateway.print_error(e)
		self.add_myself_to_zookeeper()
		self.dbnodes = []		
	
	@staticmethod
	def print_error(e):
		print(Gateway.constants.ERROR_PREFIX + e.__str__())

	def add_myself_to_zookeeper(self):
		hostname = socket.gethostname()
		ip = socket.gethostbyname(hostname)
		try:
			node_data = {'ip' : ip}
			Gateway.zk.ensure_path("/gateways")
			Gateway.zk.create("/gateways/gateway",str.encode(json.dumps(node_data)), ephemeral=True, sequence=True)
			print('Added a gateway node to zookeeper')
		except Exception as e:
			Gateway.print_error(e)

	def connection_listener(self, state):

		if state == KazooState.LOST:
			print('session lost')
		elif state == KazooState.SUSPENDED:
			print('session suspended')
		else:
			print('running in state {}'.format(state))

	@staticmethod
	def handle_dbnodes_change(children):
		# print('Nodes cluster changed, current cluster configuration:')
		# for node in children:
		# 	data,stat = Gateway.zk.get('/nodes/{}'.format(node))
		# 	print('Node: {}'.format(node))
		# 	print('Data: {}'.format(json.loads(data.decode())))
		# 	print('Data version: {}'.format(stat.version))
		# 	print('Data length: {}'.format(stat.data_length))
		crush_map_children = []
		for i in range(len(children)):
			crush_map_children.append(Gateway.constants.CRUSH_MAP_CHILDREN_NODE_FMT.format(i, -2-i, i, children[i]))
		crush_map = json.loads(Gateway.constants.CRUSH_MAP_FMT.format(','.join(crush_map_children)))
		# crush_map =	Gateway.constants.DEFAULT_CRUSH_MAP
		print(crush_map)
		if len(crush_map['trees'][0]['children']) == 0:
			return
		Gateway.crush_object.parse(crush_map)
		Gateway.zk.ensure_path('/crush_map')
		Gateway.zk.set('/crush_map', str.encode(json.dumps(crush_map)))
		# print('Mapping for 1234 => ', Gateway.crush_object.map(rule="data", value=1234, replication_count=2))

if __name__ == "__main__":
	gateway = Gateway()
	while True:
		time.sleep(5)
		print('Watching changes in /nodes')
	# time.sleep(30)