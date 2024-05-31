import sys
from Address import Address
from xmlrpc.client import ServerProxy
from typing import List
from utils.RPCHandler import RPCHandler
from messages.Execute import ExecuteRequest, ExecuteResponse


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Client.py [server_ip] [server_port]")

    rpc_handler = RPCHandler(f"Client.py")
    server_addr = Address(sys.argv[1], int(sys.argv[2]))
    while True:
        inp = input("Enter command: ").split()
        if inp[0] == "exit":
            break
        else:
            if (inp[0] == "request_log"):
                # TODO: Handle the request_log
                resp = ""
                print(resp)
            else:
                # Which means, the command is "execute", Executing the request
                req = ExecuteRequest({
                    "command": " ".join(inp[0:]),
                    "value": " "
                })
                resp = rpc_handler.request(server_addr, "execute", req)
                print(resp)