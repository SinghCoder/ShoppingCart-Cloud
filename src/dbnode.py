from constants import Constants
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import *
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
from crush import Crush
from dbapi import DbApi

class DbNode():
	
	logger = logging.getLogger('dbnodelogger')
	logger.setLevel(logging.DEBUG)
	ch = logging.FileHandler('../logs/dbnode.log', 'w')
	formatter = logging.Formatter('[%(asctime)s] %(message)s %(funcName)s:%(lineno)d')
	ch.setFormatter(formatter)
	logger.addHandler(ch)
	
	constants = Constants()
	zk = KazooClient(hosts='127.0.0.1:2181')
	flask_port = None
	dbapi = None		

	def __init__(self):
		super().__init__()

		DbNode.flask_port = DbNode.dbapi.flask_port_no

		DbNode.zk.add_listener(DbNode.connection_listener)
		DbNode.zk.start()
		print(DbNode.constants.SERVER_PREFIX + DbNode.constants.MESSAGE_CONNECTED + "with 127.0.0.1:2181")
		self.add_myself_to_zookeeper()
	
	@staticmethod
	def print_error(e):
		print(DbNode.constants.ERROR_PREFIX + e.__str__())

	def add_myself_to_zookeeper(self):
		hostname = socket.gethostname()
		ip = socket.gethostbyname(hostname)
		try:
			print(ip)
			print(DbNode.flask_port)
			node_data = {'ip' : ip, 'flask_port' : DbNode.flask_port}
			DbNode.zk.ensure_path("/nodes")
			DbNode.zk.create("/nodes/node",str.encode(json.dumps(node_data)), ephemeral=True, sequence=True)
			print('Added myself to /nodes, children list:')
			child_node_list = DbNode.zk.get_children('/nodes')
			if child_node_list:
				print('subnode list:{}'.format(child_node_list))
		except Exception as e:
			DbNode.print_error(e)
		pass

	@staticmethod
	def connection_listener(state):

		if state == KazooState.LOST:
			print('session lost')
		elif state == KazooState.SUSPENDED:
			print('session suspended')
		else:
			print('running in state {}'.format(state))

if __name__ == "__main__":
	DbNode.dbapi = DbApi()
	DbNode.dbapi.set_flask_port()

	dbnode = DbNode()

	DbNode.dbapi.run_app()

	while True:
		time.sleep(5)