import asyncio
from threading     import Thread
from xmlrpc.client import ServerProxy
from typing        import Any, List, TypedDict
from enum          import Enum
from Address       import Address
import time
import socket
import json
from structs.NodeType import NodeType
from app import KVStore
from structs.Log import Log
from structs.ColorLog import ColorLog

from messages.Base import BaseMessage, BaseResponse, ResponseStatus
from messages.Execute import ExecuteRequest, ExecuteResponse
from utils.MessageParser import MessageParser
from utils.RPCHandler import RPCHandler
from structs.Log import Log
from StableStorage import StableStorage
        

class RaftNode:
    HEARTBEAT_INTERVAL   = 1
    ELECTION_TIMEOUT_MIN = 2
    ELECTION_TIMEOUT_MAX = 3
    RPC_TIMEOUT          = 0.5
    
    class NodeType(Enum):
        FOLLOWER = 1
        CANDIDATE = 2
        LEADER = 3
    
    class StableVars(TypedDict):
        election_term: int
        voted_for: Address     
        log: List[Log] 
        commit_length: int

    def __init__(self, application : KVStore, addr: Address, contact_addr: Address = None):
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        self.address:             Address           = addr
        self.type:                NodeType          = NodeType.FOLLOWER
        self.log:                 List[Log]         = []
        self.app:                 KVStore           = application
        self.election_term:       int               = 0
        self.cluster_addr_list:   List[Address]     = []
        self.cluster_leader_addr: Address           = None
        self.current_time:        float             = time.time()
        
        # Get state from stable storage
        self.__fetch_stable_storage()
        
        # Additional vars
        self.cluster_addr_list:   List[Address]     = [] 
        self.message_parser:      MessageParser     = MessageParser()
        self.rpc_handler:         RPCHandler        = RPCHandler()
        
        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)

    def __fetch_stable_storage(self):
        self.stable_storage = StableStorage[RaftNode.StableVars](self.address)
        loaded = self.stable_storage.try_load()
        if loaded is not None:
            self.__print_log(f"Loaded stable storage: {loaded}")
            return

        self.__init_stable()

    def __init_stable(self):
        data = RaftNode.StableVars({
            'election_term': 0,
            'voted_for': None,
            'log': [],
            'commit_length': 0,
        })
        self.stable_storage.storeAll(data)

    # Internal Raft Node methods
    def __print_log(self, text: str):
        print(ColorLog.OKBLUE.value + f"[{self.address}]" + ColorLog.ENDC.value + f"[{time.strftime('%H:%M:%S')}] {text}")

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
    
        while True:
            if(time.time() - self.current_time > RaftNode.HEARTBEAT_INTERVAL):
                self.__print_log("[Leader] Sending heartbeat...")
                self.current_time = time.time()
            if(self.election_term == 0xDEAD): 
                self.__print_log("Stopping Leader Server...")
                break

            

        # while True:
        #     #listen to keyboard interrupt
        #     self.__print_log("[Leader] Sending heartbeat...")
        #     pass
        #     await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)

    def apply_membership(self, req) :
        try :
            if (self.type == NodeType.LEADER) :
                req = self.message_parser.deserialize(req)
                self.cluster_addr_list.append(req["address"])
                response = {
                    "status": ResponseStatus.SUCCESS.value,
                    "address": self.address,
                    "cluster_addr_list" : self.cluster_addr_list,
                    "reason" : "",
                    "log" : self.log
                }
                self.__print_log(f"Accepted a new follower : {req['address']['ip']}:{req['address']['port']}")
                return self.message_parser.serialize(response)
            else :
                response = {
                    "status": ResponseStatus.REDIRECTED.value,
                    "address": self.cluster_leader_addr,
                    "reason" : "NOT LEADER"
                }
                return self.message_parser.serialize(response)
        except Exception as e:
            self.__print_log(str(e))
            return self.message_parser.serialize(BaseResponse({
                "status": ResponseStatus.FAILED.value,
                "address": self.address,
                "reason": str(e), 
            }))


    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        retry_count = 0
        response = {
            "status": "redirected",
            "address": {
                "ip":   contact_addr.ip,
                "port": contact_addr.port,
            }
        }
        while response["status"] != "success":
            try :
                redirected_addr = Address(response["address"]["ip"], response["address"]["port"])
                response        = self.__send_request({"address" : self.address},"apply_membership", redirected_addr)
            except :
                if (retry_count < 5) :
                    print("Didn't get response from leader, retrying...")
                    time.sleep(RaftNode.HEARTBEAT_INTERVAL)
                    retry_count += 1
                else :
                    print("Leader failed to response 5 times, aborting membership application")
                    break
        if (response["status"] == "success") :
            self.log                 = response["log"]
            self.cluster_addr_list   = response["cluster_addr_list"]
            self.cluster_leader_addr = redirected_addr
            print(self.cluster_addr_list, self.cluster_leader_addr)

    def __send_request(self, request: BaseMessage, rpc_name: str, addr: Address) -> "json":
        # Warning : This method is blocking
        response     = self.rpc_handler.request(addr, rpc_name, request)
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
            print("Entries", entries)
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
    def execute(self, json_request: str) -> str:
        try:
            # Interface to client only sent to leader
            """ if self.type != RaftNode.NodeType.LEADER:
                return self.message_parser.serialize(ExecuteResponse({
                    "status": ResponseStatus.REDIRECTED.value,
                    "address": self.cluster_leader_addr,
                })) """

            # Deserialize the request
            request: ExecuteRequest = self.message_parser.deserialize(json_request)
            
            # Execution response
            self.app.executing_log(request)
            
            # Add to stable storage
            with self.stable_storage as stable_vars:
                log = Log({
                    "term": stable_vars["election_term"],
                    "command": request["command"],
                    "value": request["value"],
                })

                stable_vars["log"].append(log)
                self.stable_storage.storeAll(stable_vars)

            # Execution response
            response = ExecuteResponse({
                "status": ResponseStatus.SUCCESS.value,
                "address": self.address,
                "data": request["value"]
            })
            
            # Send the serialized response
            return self.message_parser.serialize(response)
        
        except Exception as e:
            self.__print_log(str(e))
            return self.message_parser.serialize(ExecuteResponse({
                "status": ResponseStatus.FAILED.value,
                "address": self.address,
                "reason": str(e), 
            }))
        

