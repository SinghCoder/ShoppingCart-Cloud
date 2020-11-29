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

product_db = TinyDB('product_db.json')
user_db = TinyDB('user_db.json')
crush_object = Crush()

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

@app.route("/productslist", methods=['GET'])
def products_list():
    children = zk.get_children("/nodes")    
    combined_product_list = []
    unique_products = []

    for node_name in children:
        node_data, _ = zk.get("/nodes/{}".format(node_name))
        node_data = json.loads(node_data.decode("utf-8"))
        ip = node_data['ip']
        port = int(node_data['flask_port'])

        call_api = 'http://' + ip + ':' + str(port) + '/productslist'
        list_of_products = json.loads(requests.get(call_api).text)
        for prod in list_of_products:
            if prod['name'] not in unique_products:
                unique_products.append(prod['name'])
                combined_product_list.append(prod)

    print(combined_product_list)
    return json.dumps(combined_product_list)

@app.route("/product", methods=["GET"])
def get_product():
    req_product_name = request.args.get('name')
    response_data = {'error' : 'No error', 'result' : ''}
    return_status = 200
    if req_product_name is None:
        response_data['error'] = 'Name not present in query'
        return_status = 400
    else:
        req_product_name = req_product_name.lower()

        req_product_hash = unpack("<IIII",md5(req_product_name.encode('utf-8')).digest())[0] % 2147483647
        print('Hash: {}'.format(req_product_hash))

        children = zk.get_children("/nodes")
        num_nodes = len(children)
        print('*********************************')
        print(num_nodes)

        read_quorom, write_quorom = (num_nodes + 1) // 2, (num_nodes + 1) // 2

        crush_map, _ = zk.get("/crush_map")
        crush_map = json.loads(crush_map.decode("utf-8"))
        crush_object.parse(crush_map)
        
        node_names = crush_object.map(rule="data", value=req_product_hash, replication_count=read_quorom)
        nodes_info = {}
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
            print(prod_data)
            nodes_info[node_name]['prod_data'] = prod_data['result'][0]
        
        versions = sorted([nodes_info[node_name]['prod_data']['version'] for node_name in node_names])
        latest_version = versions[-1]
        min_qty = math.inf
        num_latest_count = 0
        prod_post_url = 'http://{}:{}/product'
        
        for node_name in node_names:
            node_info = nodes_info[node_name]
            if node_info['prod_data']['version'] == latest_version:
                min_qty = min(min_qty, float(node_info['prod_data']['quantity']))
                num_latest_count = num_latest_count + 1

        post_data = {'name' : req_product_name, 'quantity' : min_qty, 'version' : latest_version}        
        # If multiple latest versions with diff data => concurrent writes, take the minimum value
        for node_name in node_names:
            node_info = nodes_info[node_name]
            if node_info['prod_data']['version'] != latest_version or node_info['prod_data']['quantity'] != min_qty:                    
                requests.post(prod_post_url.format(node_info['ip_port']['ip'], node_info['ip_port']['flask_port']), data = post_data)

        response_data['result'] = post_data
    
    response = app.response_class(response=json.dumps(response_data),
                                  status=return_status,
                                  mimetype='application/json')
    return response
    

@app.route("/product", methods=["POST"])
def update_product():
    pass

@app.route("/userslist", methods=['GET'])
def users_list():
    pass

@app.route("/user", methods=["GET"])
def get_user():
    pass

@app.route("/createuser", methods=["POST"])
def update_user():
    pass

@app.route("/addtocart", methods=["POST"])
def add_to_cart():
    pass

app.run(debug = True) 