from flask import Flask, request
from flask_cors import CORS  # Import CORS
import sys
from Address import Address
from app import KVStore
from typing import List
from utils.RPCHandler import RPCHandler
from messages.Execute import ExecuteRequest, ExecuteResponse

_kvStore : KVStore

class Client:
    rpc_handler: RPCHandler
    client_addr: Address

    def __init__(self, client_ip: str, client_port: int):
        Client.rpc_handler = RPCHandler(f"Client.py")
        Client.client_addr = Address(client_ip, client_port)
        print(f"Client started at {client_ip}:{client_port}\n")

    @staticmethod
    def _execute(server_address: Address, command: str) -> str:
        # Executing the request
        req = ExecuteRequest({
            "command": command,  # Remove unnecessary spaces
            "value": ""
        })
        print(server_address)
        resp = Client.rpc_handler.request(server_address, "execute", req)
        return resp

# Flask Server

app = Flask(__name__)
CORS(app)  # Apply CORS to the Flask app

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/execute_command', methods=['POST'])
def execute_command():
    try:
        print(request)
        data = request.get_json()
        command: str = data['command']
        print(command)

        # Is INVALID COMMAND??
        if(_kvStore._execute_single_command(command) == "Invalid command" and command != "request_log"):
            # throw an exception
            raise Exception("Invalid command")
        
        _address = Address(data['address']['ip'], int(data['address']['port']))
        response = Client._execute(_address, command)
        return response
    except Exception as e:
        # make response 400
        return {"error": str(e)}, 400

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Client.py [client_ip] [client_port]")
        sys.exit(1)

    Client(sys.argv[1], int(sys.argv[2]))
    _kvStore = KVStore()
    app.run(host=sys.argv[1], port=int(sys.argv[2]))