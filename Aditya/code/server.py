import flask
import argparse,json,os,random
from flask import request,jsonify
from crush import Crush
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
logging.basicConfig()

crushmap = '''
{
  "trees": [
    {
      "type": "root", "name": "dc1", "id": -1,
      "children": [
        {
         "type": "host", "name": "domain0", "id": -2,
         "children": [
          { "id": 0, "name": "n1", "weight": 65536 }
         ]
        },
        {
         "type": "host", "name": "domain1", "id": -3,
         "children": [
          { "id": 1, "name": "n2", "weight": 65536 }
         ]
        },
        {
         "type": "host", "name": "domain2", "id": -4,
         "children": [
          { "id": 2, "name": "n3", "weight": 65536 }
         ]
        }
      ]
    }
  ],
  "rules": {
    "data": [
      [ "take", "dc1" ],
      [ "chooseleaf", "firstn", 0, "type", "host" ],
      [ "emit" ]
    ]
  }
}
'''

app = flask.Flask(__name__)
app.config["DEBUG"] = True

def read(user_id,read_quorom=1):
    '''
    userID is the key here
    '''
    c = Crush()
    c.parse(json.loads(zk.get('/crushmap')[0]))
    nodes = c.map(rule="data", value=int(user_id), replication_count=read_quorom)
    for node in nodes:
        file_name = os.getcwd()+"/nodes/"+node+".json"
        with open(file_name,'r') as f:
            data = json.loads(f.read())
            if user_id not in data:
                return "data for this user not present in node: "+node
            else:
                return jsonify(data[user_id])

def write(user_id,item_id,count,operation,write_quorom=3):
    '''
    maintains atomicity of write operation, otherwise inconsistency can come
    '''
    c = Crush()
    path = "/children/"+user_id
    zk.ensure_path(path)
    c.parse(json.loads(zk.get('/crushmap')[0]))
    nodes = c.map(rule="data", value=int(user_id), replication_count=write_quorom)
    random.shuffle(nodes)
    for node in nodes:
        file_name = os.getcwd()+"/nodes/"+node+".json"
        app.logger.info('writing to file '+file_name)
        content = ''
        data = ''
        with open(file_name) as f:
            content = f.read()
            if content != '':
                data = json.loads(content)
            else:
                data = {}
        with open(file_name,'w') as f:
            if operation == 'add':
                if user_id not in data:
                    data[user_id] = {}
                data[user_id][item_id] = count
            elif operation == 'delete':
                if user_id not in data:
                    json.dump(data,f)
                    return "user info not present"
                ret = data[user_id].pop(item_id,None)
                if ret is None:
                    json.dump(data,f)
                    return "item: "+str(item_id)+"not present for the user: "+str(user_id)
            elif operation == 'update':
                if user_id not in data:
                    json.dump(data,f)
                    return "user info not present"
                if item_id not in data[user_id]:
                    json.dump(data,f)
                    return str(user_id)+" can't update item that is not added"
                data[user_id][item_id] = count
            # data["ADMIN"] = list(set(data["ADMIN"].append(userID)))        
            json.dump(data,f)
    if operation == "add":
        return 'added item: '+ str(item_id) + ' with quantity: ' + str(count)
    elif operation == 'delete':
        return "deleted item: "+str(item_id)
    elif operation == 'update':
        return 'updated item: '+ str(item_id) + ' to COUNT: ' + str(count)
@app.route('/list', methods=['GET'])
def list_items():
    if 'user' in request.args:
        user_id = str(request.args['user'])
        return read(user_id)
    else:
        app.logger.error("No user_id provided.")
        return "Please specify a user id."

@app.route('/', methods=['GET'])
def home():
    return "<h1>E-Cart Application</h1>"

@app.route('/add', methods=['GET'])
def add_items():
    if 'user' in request.args and 'item_id' in request.args and 'item_count' in request.args:
        user_id = str(request.args['user'])
        item_id = str(request.args['item_id'])
        item_count = str(request.args['item_count'])
        return write(user_id,item_id,item_count,"add")
    else:
        app.logger.error("Insufficient arguments")
        return "Please specify userid,item_id and item_count in request."

@app.route('/delete', methods=['GET'])
def delete_items():
    if 'user' in request.args:
        user_id = str(request.args['user'])
        item_id = str(request.args['item_id'])
        # item_count = str(request.args['item_count'])
        return write(user_id,item_id,0,"delete")
    else:
        app.logger.error("Insufficient arguments")
        return "Please specify userid,item_id and item_count in request."

@app.route('/update', methods=['GET'])
def update_items():
    if 'user' in request.args:
        user_id = str(request.args['user'])
        item_id = str(request.args['item_id'])
        item_count = str(request.args['item_count'])
        return write(user_id,item_id,item_count,"update")
    else:
        app.logger.error("Insufficient arguments")
        return "Please specify userid,item_id and item_count in request."

@app.route('/admin', methods=['GET'])
def admin_view():
    user_list = zk.get_children("/children")
    response = {}
    for user in user_list:
        response[user] = read(user)
    return response

# @app.route('/admin/get', methods=['GET'])
# def admin_query():
#     if 'item_id' in request.args:

#     else:
#         app.logger.error('Insufficient arguments')
#         return "Please specify item_id in request"
uri = '127.0.0.1:2181'
zk = KazooClient(hosts=uri)
zk.start()
if zk.exists("/crushmap"):
    zk.delete('/crushmap')
    # print(zk.get('/crushmap'))
zk.create("/crushmap",bytes(crushmap, 'utf-8'))
# print(zk.get('/crushmap'))
def main():
    parser = argparse.ArgumentParser(description='Gateway implementation')
    parser.add_argument('--port',type=int,default=3000)
    # parser.add_argument('--zk',type=str,default='2181')
    args = parser.parse_args()
    app.run(host='0.0.0.0',port=args.port)

if __name__ == '__main__':
    main()