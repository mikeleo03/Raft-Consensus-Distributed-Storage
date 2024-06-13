from Address       import Address
from Raft          import RaftNode
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from app           import KVStore
import sys
import threading


def start_serving(addr: Address, contact_node_addr: Address):
    print(f"Starting Raft Server at {addr.ip}:{addr.port}")
    _raftNode = RaftNode(KVStore(), addr, contact_node_addr)
    
    class PrintRequestHandler(SimpleXMLRPCRequestHandler):
        def do_POST(self):
            print(f"Received POST request from {self.client_address}")
            # print received data
            return SimpleXMLRPCRequestHandler.do_POST(self)

    try:
        with SimpleXMLRPCServer((addr.ip, addr.port), requestHandler=PrintRequestHandler) as server:
            #print server address
            _ip = server.server_address[0]
            _port = server.server_address[1]
            print(f"\nServer started at {_ip}:{_port}\n")
            server.register_introspection_functions()
            server.register_instance(_raftNode)
            server.serve_forever()
    except KeyboardInterrupt:
        _raftNode.election_term = 0xDEAD
   

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: Server.py ip port [contact_ip] [contact_port]")
        exit()

    contact_addr = None
    if len(sys.argv) == 5:
        contact_addr = Address(sys.argv[3], int(sys.argv[4]))
    server_addr = Address(sys.argv[1], int(sys.argv[2]))

    start_serving(server_addr, contact_addr)
