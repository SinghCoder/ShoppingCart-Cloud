from constants import Constants
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import *
from kazoo.recipe.watchers import ChildrenWatch
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

	def __init__(self):
		super().__init__()		
		try:
			Gateway.zk = KazooClient(hosts='127.0.0.1:2181')
			Gateway.zk.start()
			Gateway.logger.debug(Gateway.constants.SERVER_PREFIX + Gateway.constants.MESSAGE_CONNECTED + "with 127.0.0.1:2181")	
			Gateway.zk.add_listener(self.connection_listener)
			ChildrenWatch(Gateway.zk, '/nodes', func=Gateway.handle_dbnodes_change)
		except Exception as e:
			Gateway.print_error(e)
		self.add_myself_to_zookeeper()
		self.dbnodes = []
	
	@staticmethod
	def print_error(e):
		Gateway.logger.debug(Gateway.constants.ERROR_PREFIX + e.__str__())

	def add_myself_to_zookeeper(self):
		hostname = socket.gethostname()
		ip = socket.gethostbyname(hostname)
		try:
			node_data = {'ip' : ip}
			Gateway.zk.ensure_path("/gateways")
			Gateway.zk.create("/gateways/gateway",str.encode(json.dumps(node_data)), ephemeral=True, sequence=True)
			Gateway.logger.debug('Added a gateway node to zookeeper')
		except Exception as e:
			Gateway.print_error(e)

	def connection_listener(self, state):

		if state == KazooState.LOST:
			Gateway.logger.debug('session lost')
		elif state == KazooState.SUSPENDED:
			Gateway.logger.debug('session suspended')
		else:
			Gateway.logger.debug('running in state {}'.format(state))

	@staticmethod
	def handle_dbnodes_change(children):
		Gateway.logger.debug('Nodes cluster changed, current cluster configuration:')
		for node in children:
			data,stat = Gateway.zk.get('/nodes/{}'.format(node))
			Gateway.logger.debug('Node: {}'.format(node))
			Gateway.logger.debug('Data: {}'.format(json.loads(data.decode())))
			Gateway.logger.debug('Data version: {}'.format(stat.version))
			Gateway.logger.debug('Data length: {}'.format(stat.data_length))
		crush_map_children = []
		for i in range(len(children)):
			crush_map_children.append(Gateway.constants.CRUSH_MAP_CHILDREN_NODE_FMT.format(i, -2-i, i, children[i]))
		print('New Crush Map: ', Gateway.constants.CRUSH_MAP_FMT.format(','.join(crush_map_children)))

if __name__ == "__main__":
	gateway = Gateway()
	while True:
		time.sleep(5)
		Gateway.logger.debug('Watching changes in /nodes')
	# time.sleep(30)