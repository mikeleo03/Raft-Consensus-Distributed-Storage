import asyncio
from threading     import Thread
from xmlrpc.client import ServerProxy
from typing        import Any, List
from enum          import Enum
from Address       import Address
import time
import socket
import json
from structs.NodeType import NodeType
from app import KVStore
from structs.Log import Log


class RaftNode:
    HEARTBEAT_INTERVAL   = 1
    ELECTION_TIMEOUT_MIN = 2
    ELECTION_TIMEOUT_MAX = 3
    RPC_TIMEOUT          = 0.5

    def __init__(self, application : KVStore, addr: Address, contact_addr: Address = None):
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        self.address:             Address           = addr
        self.type:                NodeType          = NodeType.FOLLOWER
        self.log:                 List[Log]    = []
        self.app:                 KVStore               = application
        self.election_term:       int               = 0
        self.cluster_addr_list:   List[Address]     = []
        self.cluster_leader_addr: Address           = None
        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)



    # Internal Raft Node methods
    def __print_log(self, text: str):
        print(f"[{self.address}] [{time.strftime('%H:%M:%S')}] {text}")

    def __initialize_as_leader(self):
        self.__print_log("Initialize as leader node...")
        self.cluster_leader_addr = self.address
        self.type                = NodeType.LEADER
        request = {
            "cluster_leader_addr": self.address
        }
        # TODO : Inform to all node this is new leader
        self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__leader_heartbeat()])
        self.heartbeat_thread.start()
        

    async def __leader_heartbeat(self):
        # TODO : Send periodic heartbeat
        while self.type == NodeType.Leader:
            self.__print_log("[Leader] Sending heartbeat...")
            for addr in self.cluster_addr_list:
                if(self.address != addr):
                    self.send_heartbeat_msg(addr)
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)

    def send_heartbeat_msg(self, addr):
        return

    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        response = {
            "status": "redirected",
            "address": {
                "ip":   contact_addr.ip,
                "port": contact_addr.port,
            }
        }
        while response["status"] != "success":
            redirected_addr = Address(response["address"]["ip"], response["address"]["port"])
            response        = self.__send_request(self.address, "apply_membership", redirected_addr)
        self.log                 = response["log"]
        self.cluster_addr_list   = response["cluster_addr_list"]
        self.cluster_leader_addr = redirected_addr

    def __send_request(self, request: Any, rpc_name: str, addr: Address) -> "json":
        # Warning : This method is blocking
        node         = ServerProxy(f"http://{addr.ip}:{addr.port}")
        json_request = json.dumps(request)
        rpc_function = getattr(node, rpc_name)
        response     = json.loads(rpc_function(json_request))
        self.__print_log(response)
        return response

    # Inter-node RPCs
    def heartbeat(self, json_request: str) -> "json":
        # TODO : Implement heartbeat
        response = {
            "heartbeat_response": "ack",
            "address":            self.address,
        }
        return json.dumps(response)
    
    #Append log to the log list
    def append_log(self,  json_request: str):
        request = json.loads(json_request)
        term = request['term']
        command = request['command']
        value = request['value']
        log_entry = Log(term, command, value)
        self.log.append(log_entry)
        response = {
            "address": self.address,
            "success": True
        }
        return json.dumps(response)
        
    #Replicate log to the followers
    async def __commit_log(self, entries: List[Log]):
        ack_count = 0 
        for addr in self.cluster_addr_list:
            # Not sending to itself
            if addr != self.address:
                request = {
                    "term": self.election_term,
                    "leader_id": self.address,
                    "prev_log_index": self.log[-1].index,
                    "prev_log_term": self.log[-1].term,
                    "entries": entries,
                    "leader_commit": len(self.log) - 1
                }
                
            try:
                response = self.__send_request(request, 'append_entries', addr)
                if response['success']:
                    ack_count += 1 
                    self.__print_log(f"Log entries replicated to {addr}")
                else:
                    self.__print_log(f"Failed to replicate log entries to {addr}")
            except Exception as e:
                self.__print_log(f"Error replicating log to {addr}: {e}")
                
        if ack_count >= (len(self.cluster_addr_list) // 2) + 1:
            # Majority of acks received, consider the log entry committed
            self.__print_log("Majority of acks received. Log entry committed.")
            # Commit the log entry to the state machine by applying the command
            for entry in entries:
                self.app.executing_log(entry)
        else:
            # Majority not yet reached
            self.__print_log("Majority of acks not yet received.")  
             
    # Append Entries Request 
    '''
    When the leader receives the response from the majority
    of followers for an append entries request, the log is set to the committed
    '''
    def __append_entries(self, json_request: str) -> "json":
        request = json.loads(json_request)
        term = request['term']
        leader_id = request['leader_id']
        # Index of the last log entry that the follower has replicated
        prev_log_index = request['prev_log_index']
         # Term of the last log entry that the follower has replicated
        prev_log_term = request['prev_log_term']
        # List of log entries after the prev_log_index
        entries = request['entries']
        #  The index of the highest log entry known to be committed by the leader.
        leader_commit = request['leader_commit']


        if len(entries) > 0 and len(self.log) > prev_log_index:
            if self.log[prev_log_index].term != prev_log_term:
                self.log = self.log[:prev_log_index]
        
        if prev_log_index + len(entries) > len(self.log):
            self.log = self.log[:prev_log_index + 1] + entries
        
        entries = self.log
        
        if leader_commit > len(self.log) - 1:
            for i in range(len(self.log), leader_commit + 1):
                self.log.append(entries[i])
            leader_commit = len(self.log) - 1
            
        response = {
            "term": self.election_term,
            "success": True
        }
        return json.dumps(response)
    
    
    # Client RPCs
    def execute(self, json_request: str) -> "json":
        request = json.loads(json_request)
        return json.dumps(request)
        

