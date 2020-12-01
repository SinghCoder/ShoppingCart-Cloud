from tinydb import TinyDB, Query
from flask import Flask, request
import json, socket

app = Flask(__name__)
flask_port_no = None
product_db = None
user_db = None

@app.route("/productslist", methods=['GET'])
def products_list():
	products_list = product_db.all()
	return json.dumps(products_list)

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
		Product = Query()
		products_matching_query = product_db.search(Product.name == req_product_name)
		if len(products_matching_query) == 0:
			response_data['error'] = 'No such product found'
			return_status = 400
		else:
			response_data['result'] = products_matching_query[0]
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
			version = product_data.get('version')
			Product = Query()
			matching_products = product_db.search(Product.name == name)
			if matching_products is None or len(matching_products) == 0:
				product_db.insert({'name' : name, 'quantity' : quantity, 'version' : version})
				response_data['result'] = 'Product inserted successfully'
			else:
				response_data['result'] = 'Product updated successfully'
				product_db.update({'quantity' : int(quantity), 'version' : version }, Product.name == name)
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
	users_list = user_db.all()
	return json.dumps(users_list)

@app.route("/user", methods=["GET"])
def get_user():
	req_user_email = request.args.get('email')
	response_data = {'error' : 'No error', 'result' : {}}
	return_status = 200
	if req_user_email is None:
		response_data['error'] = 'Email not present'
		return_status = 400
	else:
		User = Query()
		users_matching_query = user_db.search(User.email == req_user_email)
		if len(users_matching_query) != 0:
			response_data['result'] = users_matching_query[0]
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
			version = user_data.get('version')
			User = Query()
			matching_users = user_db.search(User.email == email)
			if matching_users is None or len(matching_users) == 0:
				user_db.insert({'email' : email, 'cart' : {}, 'version' : version})
				response_data['result'] = 'User inserted successfully'
			else:
				response_data['error'] = 'User already exist'
				return_status = 403
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
	print(user_data)
	response_data = {'error' : 'No error', 'result' : ''}
	return_status = 200
	if user_data is not None:
		if user_data.get('email') is not None:
			email = user_data.get('email')
			version = user_data.get('version')
			User = Query()
			matching_users = user_db.search(User.email == email)
			if matching_users is None or len(matching_users) == 0:
				response_data['error'] = 'User does not exists'
				return_status = 403
			else:                
				if ('products' not in user_data):                    
					response_data['error'] = 'Parameters passed incorrectly, please recheck'
					return_status = 400
				else:
					products = user_data['products']
					user_to_be_updated = matching_users[0]
					cart = user_to_be_updated['cart']
					for product, quantity in products.items():
						product_name = product.lower()
						cart[product_name] = quantity
					user_db.update({'cart' : cart, 'version' : version}, User.email == email)
					response_data['result'] = 'Items added to cart successfully'
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

def set_flask_port():
	global flask_port_no
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.bind(('localhost', 0))
	flask_port_no = sock.getsockname()[1]
	sock.close()

def run_app():
	app.run(host="0.0.0.0", port=flask_port_no)

class DbAPI():
	def __init__(self):
		global product_db, user_db
		set_flask_port()
		self.flask_port = flask_port_no
		product_db = TinyDB('product_db.json')
		user_db = TinyDB('user_db.json')
	
	def start(self):		
		run_app()