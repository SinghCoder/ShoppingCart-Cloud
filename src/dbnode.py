from constants import Constants
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import *
import sys
import time
import logging
import logging.handlers
import socket
import json
from dbapi import DbAPI

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

	def __init__(self):
		super().__init__()

		DbNode.zk.add_listener(DbNode.connection_listener)
		DbNode.zk.start()
		self.add_myself_to_zookeeper()
	
	@staticmethod
	def print_error(e):
		print(DbNode.constants.ERROR_PREFIX + e.__str__())

	def add_myself_to_zookeeper(self):
		hostname = socket.gethostname()
		ip = socket.gethostbyname(hostname)
		# ToDo: Remove this after testing
		ip = '127.0.0.1'
		try:
			# print(ip)
			# print(DbNode.flask_port)
			node_data = {'ip' : ip, 'flask_port' : DbNode.flask_port}
			DbNode.zk.ensure_path("/nodes")
			DbNode.zk.create("/nodes/node",str.encode(json.dumps(node_data)), ephemeral=True, sequence=True)
			# print('Added myself to /nodes, children list:')
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
	dbapi = DbAPI(sys.argv[1])
	DbNode.flask_port = dbapi.flask_port
	dbnode = DbNode()
	dbapi.start()

	while True:
		time.sleep(5)