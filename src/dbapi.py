from tinydb import TinyDB, Query
from flask import Flask, request
import json

app = Flask(__name__)

product_db = TinyDB('product_db.json')
user_db = TinyDB('user_db.json')

@app.route("/productslist", methods=['GET'])
def products_list():
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
        Product = Query()
        products_matching_query = product_db.search(Product.name == req_product_name)
        response_data['result'] = products_matching_query
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
            Product = Query()
            matching_products = product_db.search(Product.name == name)
            if matching_products is None or len(matching_products) == 0:
                product_db.insert({'name' : name, 'quantity' : quantity, 'version' : 1})
                response_data['result'] = 'Product inserted successfully'
            else:
                response_data['result'] = 'Product updated successfully'
                product_db.update({'quantity' : int(matching_products[0]['quantity']) + int(quantity), 'version' : int(matching_products[0]['version']) + 1 }, Product.name == name)
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
    response_data = {'error' : 'No error', 'result' : ''}
    return_status = 200
    if req_user_email is None:
        response_data['error'] = 'Email not present'
        return_status = 400
    else:
        User = Query()
        users_matching_query = user_db.search(User.email == req_user_email)
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
            User = Query()
            matching_users = user_db.search(User.email == email)
            if matching_users is None or len(matching_users) == 0:
                user_db.insert({'email' : email, 'cart' : {}, 'version' : 1})
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
    response_data = {'error' : 'No error', 'result' : ''}
    return_status = 200
    if user_data is not None:
        if user_data.get('email') is not None:
            email = user_data.get('email')
            User = Query()
            matching_users = user_db.search(User.email == email)
            if matching_users is None or len(matching_users) == 0:
                response_data['error'] = 'User does not exists'
                return_status = 403
            else:                
                if ('products' not in user_data) or (not isinstance(user_data['products'], list)):                    
                    response_data['error'] = 'Parameters passed incorrectly, please recheck'
                    return_status = 400
                else:
                    products = user_data['products']
                    user_to_be_updated = matching_users[0]
                    cart = user_to_be_updated['cart']
                    for product in products:
                        product_name = product['name'].lower()
                        if product_name in cart:
                            cart[product_name] = cart[product_name] + product['quantity']
                        else:
                            cart[product_name] = product['quantity']
                    user_db.update({'cart' : cart, 'version' : int(user_to_be_updated['version']) + 1}, User.email == email)
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

app.run(debug = True) 