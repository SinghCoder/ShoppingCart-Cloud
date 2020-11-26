class Constants():
	
	#Prefix
	CLI_PREFIX = "[Client] "
	ERROR_PREFIX = "[ERROR] "
	SERVER_PREFIX = "[Server] "

	#Message
		#Connection
	MESSAGE_CONNECTED = "Connection successfully ESTABLISHED "
	MESSAGE_DISCONNECTED = "Connection successfully STOPPED "
		#Commands
	MESSAGE_COMMAND_EMPTY = "Empty command"
	MESSAGE_COMMAND_MANY_ARGUMMENTS = "Command have many argumments"
	MESSAGE_COMMAND_FEW_ARGUMMENTS = "Command have few argumments"
			#Create    
	MESSAGE_CREATE_SUCESS = "Node created successfully"
	MESSAGE_CREATE_FAILED = "Node created failed"
			#Read
	MESSAGE_EXISTS_TRUE = "Node exists"
	MESSAGE_EXISTS_FALSE = "Node NOT exists"
			#List
	MESSAGE_LIST = " childrens path: "
			#Update
	MESSAGE_UPDATE_SUCESS = "Node updated successfully"
	MESSAGE_UPDATE_FAILED = "Node NOT updated"
			#Delete
	MESSAGE_DELETE_SUCESS = "Node deleted successfully"
	MESSAGE_DELETE_FAILED = "Node NOT deleted"
		#Usage
			#Incorrect
	MESSAGE_INCORRECT_USAGE_LINE_COMMAND = "Incorrect calling line command"
	MESSAGE_INCORRECT_USAGE_COMMAND = "Incorrect calling command"
			#Best Usage
	MESSAGE_USAGE_LINE_COMMAND = "Usage:\'python3 cli.py <Server IP> <Server Port>\'"
	MESSAGE_USAGE_CREATE_COMMAND = "Usage:\'CREATE/UPDATE <path> <value>\'"
	MESSAGE_USAGE_READ_COMMAND = "Usage:\'READ/EXISTS/LIST <path> <value>\'"
		#Erros
	MESSAGE_BAD_VERSION = "Version doesn't match"	
	CRUSH_MAP_FMT = """{{
		\"trees\": [
			{{
				\"type\": \"root\", \"name\": \"master\", \"id\": -1,
				\"children\": [
					{}
				]
			}}
		],
		\"rules\": {{
			\"data\": [
				[ \"take\", \"master\" ],
				[ \"chooseleaf\", \"firstn\", 0, \"type\", \"host\" ],
				[ \"emit\" ]
			]
		}}
	}}"""	
	CRUSH_MAP_CHILDREN_NODE_FMT = """{{
						\"type\": \"host\", \"name\": \"domain{}\", \"id\": {},
						\"children\": [
							{{ \"id\": {}, \"name\": \"{}\", \"weight\": 65536 }}
						]
					}}"""
	DEFAULT_CRUSH_MAP = """
			{
			"trees": [
				{
				"type": "root", "name": "dc1", "id": -1,
				"children": [
					{
					"type": "host", "name": "host0", "id": -2,
					"children": [
					{ "id": 0, "name": "device0", "weight": 65536 },
					{ "id": 1, "name": "device1", "weight": 131072 }
					]
					},
					{
					"type": "host", "name": "host1", "id": -3,
					"children": [
					{ "id": 2, "name": "device2", "weight": 65536 },
					{ "id": 3, "name": "device3", "weight": 131072 }
					]
					},
					{
					"type": "host", "name": "host2", "id": -4,
					"children": [
					{ "id": 4, "name": "device4", "weight": 65536 },
					{ "id": 5, "name": "device5", "weight": 131072 }
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

			"""