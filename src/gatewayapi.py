from tinydb import TinyDB, Query
from flask import Flask, request

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
import socket
import json
import requests
from crush import Crush
from gateway import Gateway

FLASK_PORT_NO = 5000

app = Flask(__name__)

product_db = TinyDB('product_db.json')
user_db = TinyDB('user_db.json')

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
print(Gateway.constants.SERVER_PREFIX + Gateway.constants.MESSAGE_CONNECTED + "with 127.0.0.1:2181")	

@app.route("/productslist", methods=['GET'])
def products_list():
    crush_object = Crush()
    children = zk.get_children("/nodes")
    # print("There are %s children with names %s" % (len(children), children))
    num_nodes = len(children)
    # print(num_nodes)
    read_quorom, write_quorom = (num_nodes + 1) // 2, (num_nodes + 1) // 2

    crush_map, _ = zk.get("/crush_map")
    crush_map = json.loads(crush_map.decode("utf-8"))
    # print(crush_map)
    # print(type(crush_map))
    crush_object.parse(crush_map)

    # print('Mapping for 1234 => ', crush_object.map(rule="data", value=1234, replication_count=2))
    node_names = crush_object.map(rule="data", value=1234, replication_count=read_quorom)

    combined_product_list = []

    for node_name in children:
        ip_port, _ = zk.get("/nodes/{}".format(node_name))
        ip_port = json.loads(ip_port.decode("utf-8"))
        ip = ip_port['ip']
        port = int(ip_port['flask_port'])
        print(ip)
        print(port)


        # Idk why ip is geting printed as 127.0.1.1 instead of 127.0.0.1



        call_api = 'https://' + ip + ':' + str(port) + '/productslist'
        print(call_api)

        # list_of_products = requests.get(call_api)
        # print(list_of_products)
        
    # Replace these two lines and return JSONed combined_products_list instead
    products_list = product_db.all()
    return json.dumps(products_list)
    

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

        crush_object = Crush()
        children = zk.get_children("/nodes")
        num_nodes = len(children)
        read_quorom, write_quorom = (num_nodes + 1) // 2, (num_nodes + 1) // 2

        crush_map, _ = zk.get("/crush_map")
        crush_map = json.loads(crush_map.decode("utf-8"))
        crush_object.parse(crush_map)

        node_names = crush_object.map(rule="data", value=req_product_name, replication_count=read_quorom)

        for node_name in node_names:
            ip_port, _ = zk.get("/nodes/{}".format(node_name))
            ip_port = json.loads(ip_port.decode("utf-8"))
            ip = ip_port['ip']
            port = int(ip_port['flask_port'])
            print(ip)
            print(port)

            # Still work to do


        # Product = Query()
        # products_matching_query = product_db.search(Product.name == req_product_name)
        # response_data['result'] = products_matching_query

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