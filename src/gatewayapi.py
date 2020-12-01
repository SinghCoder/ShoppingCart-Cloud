from tinydb import TinyDB, Query
from flask import Flask, request

from constants import Constants
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import *
import json, math
import requests
from crush import Crush
from gateway import Gateway

from hashlib import md5
from struct import unpack

app = Flask(__name__)

crush_object = Crush()

zk = KazooClient(hosts='172.25.0.101:2181')
zk.start()

@app.route("/productslist", methods=['GET'])
def products_list():
	children = zk.get_children("/nodes")    
	combined_product_list = []
	unique_products = []

	products = {}

	for node_name in children:
		node_data, _ = zk.get("/nodes/{}".format(node_name))
		node_data = json.loads(node_data.decode("utf-8"))
		ip = node_data['ip']
		port = int(node_data['flask_port'])

		call_api = 'http://' + ip + ':' + str(port) + '/productslist'
		list_of_products = json.loads(requests.get(call_api).text)
		for prod in list_of_products:
			if prod['name'] not in products:
				unique_products.append(prod['name'])
				combined_product_list.append(prod)
				products[prod['name']] = {}
				products[prod['name']]['name'] = prod['name']
				products[prod['name']]['quantity'] = prod['quantity']
				products[prod['name']]['version'] = prod['version']
			else:
				if prod['version'] > products[prod['name']]['version']:
					products[prod['name']]['version'] = prod['version']
					products[prod['name']]['quantity'] = prod['quantity']

	# print(combined_product_list)
	return json.dumps([item for item in products.values()])

@app.route("/product", methods=["GET"])
def get_product():
	req_product_name = request.args.get('name')
	response_data = {'error' : 'No error', 'result' : {}}
	return_status = 200
	if req_product_name is None:
		response_data['error'] = 'Name not present in query'
		return_status = 400
	else:
		req_product_name = req_product_name.lower()

		req_product_hash = unpack("<IIII",md5(req_product_name.encode('utf-8')).digest())[0] % 2147483647
		# print('Hash: {}'.format(req_product_hash))

		children = zk.get_children("/nodes")
		num_nodes = len(children)

		read_quorom, write_quorom = (num_nodes + 1) // 2, (num_nodes + 1) // 2

		crush_map, _ = zk.get("/crush_map")
		crush_map = json.loads(crush_map.decode("utf-8"))
		crush_object.parse(crush_map)
		
		node_names = crush_object.map(rule="data", value=req_product_hash, replication_count=read_quorom)
		print(node_names)
		nodes_info = {}
		versions = []
		for node_name in node_names:
			node_data, _ = zk.get("/nodes/{}".format(node_name))
			node_data = json.loads(node_data.decode("utf-8"))
			nodes_info[node_name] = {}
			nodes_info[node_name]['ip_port'] = node_data
			ip = node_data['ip']
			port = int(node_data['flask_port'])
			req_url = 'http://' + ip + ':' + str(port) + '/product'
			resp = requests.get(req_url, params={'name' : req_product_name})
			prod_data = resp.json()
			# print(prod_data)
			nodes_info[node_name]['prod_data'] = prod_data['result']
			if 'version' in prod_data['result']:
				versions.append(prod_data['result']['version'])
		
		latest_version = 0
		prod_post_url = 'http://{}:{}/product'
		min_qty = 0
		post_data = {}
		if len(versions) != 0:
			versions = sorted(versions)
			latest_version = versions[-1]
			min_qty = math.inf
			
			for node_name in node_names:
				node_info = nodes_info[node_name]
				if node_info['prod_data']['version'] == latest_version:
					min_qty = min(min_qty, int(node_info['prod_data']['quantity']))
			if latest_version != 0:
				post_data = {'name' : req_product_name, 'quantity' : min_qty, 'version' : latest_version + 1}
			# If multiple latest versions with diff data => concurrent writes, take the minimum value
			for node_name in node_names:
				node_info = nodes_info[node_name]
				resp = requests.post(prod_post_url.format(node_info['ip_port']['ip'], node_info['ip_port']['flask_port']), json = post_data)
				# print(node_info, min_qty)
				print(resp.text)

		response_data['result'] = post_data
	
	response = app.response_class(response=json.dumps(response_data),
								  status=return_status,
								  mimetype='application/json')
	return response
	

@app.route("/product", methods=["POST"])
def update_product():
	response_data = {'error' : 'No error', 'result' : ''}
	product_data = request.get_json()
	return_status = 200
	if product_data is not None:
		if product_data.get('name') is not None and product_data.get('quantity') is not None:
			name = product_data.get('name').lower()
			quantity = product_data.get('quantity')
			prod_name_hash = unpack("<IIII",md5(name.encode('utf-8')).digest())[0] % 2147483647
			children = zk.get_children("/nodes")
			num_nodes = len(children)

			read_quorom, write_quorom = (num_nodes + 1) // 2, (num_nodes + 1) // 2

			crush_map, _ = zk.get("/crush_map")
			crush_map = json.loads(crush_map.decode("utf-8"))
			crush_object.parse(crush_map)
			
			node_names = crush_object.map(rule="data", value=prod_name_hash, replication_count=read_quorom)
			nodes_info = {}
			versions = []
			for node_name in node_names:
				node_data, _ = zk.get("/nodes/{}".format(node_name))
				node_data = json.loads(node_data.decode("utf-8"))
				nodes_info[node_name] = {}
				nodes_info[node_name]['ip_port'] = node_data
				ip = node_data['ip']
				port = int(node_data['flask_port'])
				req_url = 'http://' + ip + ':' + str(port) + '/product'
				resp = requests.get(req_url, params={'name' : name})
				prod_data = resp.json()
				# print(prod_data)
				nodes_info[node_name]['prod_data'] = None
				if len(prod_data['result']) != 0:
					nodes_info[node_name]['prod_data'] = prod_data['result']
					versions.append(prod_data['result']['version'])
			
			min_qty = 0
			latest_version = 0
			prod_post_url = 'http://{}:{}/product'
			# Existing product update
			if len(versions) != 0:
				versions = sorted(versions)
				latest_version = versions[-1]
				min_qty = math.inf

				for node_name in node_names:
					node_info = nodes_info[node_name]
					if node_info['prod_data']['version'] == latest_version:
						min_qty = min(min_qty, int(node_info['prod_data']['quantity']))

			post_data = {'name' : name, 'quantity' : quantity + min_qty, 'version' : latest_version + 1}        
			# If multiple latest versions with diff data => concurrent writes, take the minimum value
			for node_name in node_names:
				node_info = nodes_info[node_name]
				try:
					resp = requests.post(prod_post_url.format(node_info['ip_port']['ip'], node_info['ip_port']['flask_port']), json = post_data)
					print(resp.text)
				except Exception as e:
					ip = node_info['ip_port']['ip']
					print(f'{e.__str__()}: error occured while updating products info in node {node_name} (IP: {ip})')
					return_status = 400
					response_data['error'] = 'Product updation unsuccessful'
		else:
			response_data['error'] = 'Some of the required parameters not found in the request.'
			return_status = 400
	else:
		response_data['error'] = 'Request format is not correct.. Please recheck'
		return_status = 400
	response = app.response_class(response=json.dumps(response_data),
								status=return_status,
								mimetype='application/json')
	return response

@app.route("/userslist", methods=['GET'])
def users_list():
	children = zk.get_children("/nodes")    
	users = {}

	for node_name in children:
		node_data, _ = zk.get("/nodes/{}".format(node_name))
		node_data = json.loads(node_data.decode("utf-8"))
		ip = node_data['ip']
		port = int(node_data['flask_port'])

		call_api = 'http://' + ip + ':' + str(port) + '/userslist'
		list_of_users = json.loads(requests.get(call_api).text)
		for user in list_of_users:
			if user['email'] not in users:
				users[user['email']] = {}
				users[user['email']]['email'] = user['email']
				users[user['email']]['cart'] = user['cart']
				users[user['email']]['version'] = user['version']
			else:
				if users[user['email']]['version'] < user['version']:
					users[user['email']]['version'] = user['version']
					users[user['email']]['cart'] = user['cart']

	# print(combined_user_list)
	return json.dumps([user for user in users.values()])

@app.route("/user", methods=["GET"])
def get_user():
	req_user_email = request.args.get('email')
	response_data = {'error' : 'No error', 'result' : {}}
	return_status = 200
	if req_user_email is None:
		response_data['error'] = 'Email not present in query'
		return_status = 400
	else:
		req_user_email = req_user_email.lower()

		req_user_hash = unpack("<IIII",md5(req_user_email.encode('utf-8')).digest())[0] % 2147483647
		# print('Hash: {}'.format(req_user_hash))

		children = zk.get_children("/nodes")
		num_nodes = len(children)

		read_quorom, write_quorom = (num_nodes + 1) // 2, (num_nodes + 1) // 2

		crush_map, _ = zk.get("/crush_map")
		crush_map = json.loads(crush_map.decode("utf-8"))
		crush_object.parse(crush_map)
		
		node_names = crush_object.map(rule="data", value=req_user_hash, replication_count=read_quorom)
		nodes_info = {}
		versions = []
		for node_name in node_names:
			node_data, _ = zk.get("/nodes/{}".format(node_name))
			node_data = json.loads(node_data.decode("utf-8"))
			nodes_info[node_name] = {}
			nodes_info[node_name]['ip_port'] = node_data
			ip = node_data['ip']
			port = int(node_data['flask_port'])
			req_url = 'http://' + ip + ':' + str(port) + '/user'
			resp = requests.get(req_url, params={'email' : req_user_email})
			user_data = resp.json()
			# print(user_data)
			nodes_info[node_name]['user_data'] = user_data['result']
			if 'version' in user_data['result']:
				versions.append(user_data['result']['version'])
		
		latest_version = 0
		user_post_url = 'http://{}:{}/user'
		new_cart = {}
		post_data = {}
		if len(versions) != 0:
			versions = sorted(versions)
			latest_version = versions[-1]
			
			for node_name in node_names:
				node_info = nodes_info[node_name]
				if node_info['user_data']['version'] == latest_version:
					for prod_name, prod_qty in node_info['user_data']['cart'].items():
						if prod_name in new_cart:
							new_cart[prod_name] = max(new_cart[prod_name], int(prod_qty))
						else:
							new_cart[prod_name] = int(prod_qty)
			if latest_version != 0:
				post_data = {'email' : req_user_email, 'products' : new_cart, 'version' : latest_version + 1}
			# If multiple latest versions with diff data => concurrent writes, take the minimum value
			for node_name in node_names:
				node_info = nodes_info[node_name]
				resp = requests.post(user_post_url.format(node_info['ip_port']['ip'], node_info['ip_port']['flask_port']), json = post_data)
				print(resp.text)

		response_data['result'] = post_data
	
	response = app.response_class(response=json.dumps(response_data),
								  status=return_status,
								  mimetype='application/json')
	return response

@app.route("/createuser", methods=["POST"])
def update_user():
	user_data = request.get_json()
	response_data = {'error' : 'No error', 'result' : ''}
	return_status = 200
	if user_data is not None:
		if user_data.get('email') is not None:
			email = user_data.get('email')
			user_mail_hash = unpack("<IIII",md5(email.encode('utf-8')).digest())[0] % 2147483647
			children = zk.get_children("/nodes")
			num_nodes = len(children)

			read_quorom, write_quorom = (num_nodes + 1) // 2, (num_nodes + 1) // 2

			crush_map, _ = zk.get("/crush_map")
			crush_map = json.loads(crush_map.decode("utf-8"))
			crush_object.parse(crush_map)
			
			node_names = crush_object.map(rule="data", value=user_mail_hash, replication_count=read_quorom)
			nodes_info = {}
			versions = []
			for node_name in node_names:
				node_data, _ = zk.get("/nodes/{}".format(node_name))
				node_data = json.loads(node_data.decode("utf-8"))
				nodes_info[node_name] = {}
				nodes_info[node_name]['ip_port'] = node_data
				ip = node_data['ip']
				port = int(node_data['flask_port'])
				req_url = 'http://' + ip + ':' + str(port) + '/user'
				resp = requests.get(req_url, params={'email' : email})
				user_data = resp.json()
				# print(user_data)
				nodes_info[node_name]['user_data'] = None
				if 'version' in user_data['result']:
					nodes_info[node_name]['user_data'] = user_data['result']
					versions.append(user_data['result']['version'])
			
			min_qty = 0
			latest_version = 0
			user_post_url = 'http://{}:{}/createuser'
			# Existing user update
			if len(versions) != 0:
				versions = sorted(versions)
				latest_version = versions[-1]
				min_qty = math.inf

				for node_name in node_names:
					node_info = nodes_info[node_name]
					if node_info['user_data']['version'] == latest_version:
						min_qty = min(min_qty, int(node_info['prod_data']['quantity']))

			post_data = {'email' : email, 'version' : latest_version + 1}        
			response_data['error'] = 'User updation successful'
			# If multiple latest versions with diff data => concurrent writes, take the minimum value
			for node_name in node_names:
				node_info = nodes_info[node_name]
				try:
					resp = requests.post(user_post_url.format(node_info['ip_port']['ip'], node_info['ip_port']['flask_port']), json = post_data)
					print(resp.text)
				except Exception as e:
					ip = node_info['ip_port']['ip']
					print(f'{e.__str__()}: error occured while updating products info in node {node_name} (IP: {ip})')
					return_status = 400
					response_data['error'] = 'User updation unsuccessful'
					break
		else:
			response_data['error'] = 'No email field present'
			return_status = 400
	else:
		response_data['error'] = 'No data found!! Please correct request format'
		return_status = 400
	response = app.response_class(response=json.dumps(response_data),
								status=return_status,
								mimetype='application/json')
	return response

@app.route("/addtocart", methods=["POST"])
def add_to_cart():
	user_data = request.get_json()
	response_data = {'error' : 'No error', 'result' : ''}
	return_status = 200
	if user_data is not None:
		if user_data.get('email') is not None:
			email = user_data.get('email')
			products = user_data.get('products')
			user_mail_hash = unpack("<IIII",md5(email.encode('utf-8')).digest())[0] % 2147483647
			children = zk.get_children("/nodes")
			num_nodes = len(children)

			read_quorom, write_quorom = (num_nodes + 1) // 2, (num_nodes + 1) // 2

			crush_map, _ = zk.get("/crush_map")
			crush_map = json.loads(crush_map.decode("utf-8"))
			crush_object.parse(crush_map)
			
			node_names = crush_object.map(rule="data", value=user_mail_hash, replication_count=read_quorom)
			nodes_info = {}
			versions = []
			for node_name in node_names:
				node_data, _ = zk.get("/nodes/{}".format(node_name))
				node_data = json.loads(node_data.decode("utf-8"))
				nodes_info[node_name] = {}
				nodes_info[node_name]['ip_port'] = node_data
				ip = node_data['ip']
				port = int(node_data['flask_port'])
				req_url = 'http://' + ip + ':' + str(port) + '/user'
				resp = requests.get(req_url, params={'email' : email})
				user_data = resp.json()
				# print(user_data)
				nodes_info[node_name]['user_data'] = None
				if 'version' in user_data['result']:
					nodes_info[node_name]['user_data'] = user_data['result']
					versions.append(user_data['result']['version'])
			
			latest_version = 0
			cart_post_url = 'http://{}:{}/addtocart'
			new_cart = {}
			post_data = {}
			if len(versions) != 0:
				versions = sorted(versions)
				latest_version = versions[-1]
				
				for node_name in node_names:
					node_info = nodes_info[node_name]
					if node_info['user_data']['version'] == latest_version:
						for prod_name, prod_qty in node_info['user_data']['cart']:
							if prod_name in new_cart:
								new_cart[prod_name] = max(new_cart[prod_name], int(prod_qty))
							else:
								new_cart[prod_name] = int(prod_qty)
			for product, quantity in products.items():
				resp = requests.post("http://127.0.0.1:5000/product", json = {"name" : product, "quantity": -1*int(quantity)})
				print(resp.text)
				if product in new_cart:
					new_cart[product] = new_cart[product] + int(quantity)
				else:
					new_cart[product] = int(quantity)
			post_data = {'email' : email, 'products' : new_cart, 'version' : latest_version + 1}
			# If multiple latest versions with diff data => concurrent writes, take the minimum value
			for node_name in node_names:
				node_info = nodes_info[node_name]
				resp = requests.post(cart_post_url.format(node_info['ip_port']['ip'], node_info['ip_port']['flask_port']), json = post_data)
				print(resp.text)
		else:
			response_data['error'] = 'No email field present'
			return_status = 400
	else:
		response_data['error'] = 'No data found!! Please correct request format'
		return_status = 400
	response = app.response_class(response=json.dumps(response_data),
								status=return_status,
								mimetype='application/json')
	return response

app.run(host="0.0.0.0", debug = True) 