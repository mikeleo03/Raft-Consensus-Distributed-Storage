import sys
from Address import Address
from xmlrpc.client import ServerProxy
from typing import List
from utils.RPCHandler import RPCHandler
from messages.Execute import ExecuteRequest, ExecuteResponse
from flask import Flask, request


class Client:
    rpc_handler: RPCHandler
    client_addr: Address

    def __init__(self, client_ip: str, client_port: int):
        Client.rpc_handler = RPCHandler(f"Client.py")
        Client.client_addr = Address(client_ip, client_port)
        print(f"Client started at {client_ip}:{client_port}\n")

    def _test_execute(server_address: Address, command: str) -> str:
        # Executing the request
        req = ExecuteRequest({
            "command": " " + (command),
            "value": " "
        })
        print(server_address)
        resp = Client.rpc_handler.request(server_address, "execute", req)
        return resp

# Flask Server

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

# get json data from request
@app.route('/test_execute', methods=['POST'])
def test_execute():
    data = request.get_json()
    command = data['command']
    _address = Address(data['address']['ip'], int(data['address']['port']))
    response = Client._test_execute(_address, command)
    return response



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Client.py [client_ip] [client_port]")

    Client(sys.argv[1], int(sys.argv[2]))

    app.run(host=sys.argv[1], port=int(sys.argv[2]))