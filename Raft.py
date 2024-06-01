import asyncio
from threading     import Thread
from xmlrpc.client import ServerProxy
from typing        import Any, List, Set, TypedDict
from enum          import Enum
from Address       import Address
import time
import socket
import json
import random
from structs import AppendEntry
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
    _LOG_ROLE        = {
        NodeType.FOLLOWER: ColorLog._CYAN.value + "[Follower]" + ColorLog._ENDC.value,
        NodeType.CANDIDATE: ColorLog._MAGENTA.value + "[Candidate]" + ColorLog._ENDC.value,
        NodeType.LEADER: ColorLog._BLUE.value + "[Leader]" + ColorLog._ENDC.value,
    }
    
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
        self.heartbeat_time:      float             = time.time()
        self.timeout_time:        float             = time.time() + RaftNode.ELECTION_TIMEOUT_MIN + (RaftNode.ELECTION_TIMEOUT_MAX - RaftNode.ELECTION_TIMEOUT_MIN) * random.random()
        self.is_timeout:          bool              = False
        self.current_time:        float             = time.time()
        self.votes_received:    Set[Address] 
        self.ack_length :         int               = 0
        
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
            self.__initialize_as_follower()

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
        print(ColorLog._BLUE.value + f"[{self.address}]" + ColorLog._ENDC.value + f"[{time.strftime('%H:%M:%S')}]" + RaftNode._LOG_ROLE[self.type] + f" {text}")

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

    def __initialize_as_follower(self):
        self.__print_log("Initialize as follower node...")
        self.type = NodeType.FOLLOWER
        self.election_term = 0
        self.voted_for = None
        self.follower_timeout_thread = Thread(target=asyncio.run,args=[self.__follower_timeout()])
        self.randomize_timeout()
        self.follower_timeout_thread.start()


        

    async def __leader_heartbeat(self):
        while self.type == NodeType.LEADER:
            self.ack_length = 0
            # print("mulai dari awal: " + str(self.ack_length))
            if(time.time() - self.heartbeat_time > RaftNode.HEARTBEAT_INTERVAL):
                self.__print_log(" Sending heartbeat...")
                for addr in self.cluster_addr_list:
                    if(self.address != addr):
                        print(addr)
                        self.send_heartbeat_msg(addr)
                        print("ini sekarang " + str(self.ack_length))
                print("ini total " + str(self.ack_length))
                # cek di sini nanti masalah ack length uda berapa
                self.heartbeat_time = time.time()

            if(self.election_term == 0xDEAD): 
                self.__print_log("Stopping Leader Server...")
                break

    async def __follower_timeout(self):
        while self.type == NodeType.FOLLOWER:
            if time.time() > self.timeout_time:
                self.__print_log("Timeout has occured, changing to candidate...")
                self.type = NodeType.CANDIDATE
                break
        self.__print_log("Starting election...")



    def send_heartbeat_msg(self, addr):
        with self.stable_storage as stable_vars:
            request = {
                "leader_addr" : self.address,
                "election_term": stable_vars["election_term"],
                "prev_last_term": 0,
                "prev_last_index": len(stable_vars["log"])  #length log terakhir
            }
            if(len(stable_vars["log"]) > 0):
                request["prev_last_term"] = stable_vars["log"][-1]["term"],
            response = self.__send_request(request, "heartbeat", addr)
            if response["status"] != ResponseStatus.SUCCESS.value:
                return
            else:
                self.ack_length += 1
            if(response["election_term"] == stable_vars["election_term"] and self.type == RaftNode.NodeType.LEADER) and response["sync"]:
                self.__commit_log(stable_vars["log"])
            elif(response["election_term"] > stable_vars["election_term"]):
                stable_vars.update({
                        "election_term": response["election_term"],
                        "voted_for": None,
                })
                self.stable_storage.storeAll(stable_vars)
                self.type = RaftNode.NodeType.FOLLOWER
                self.votes_received = set[Address]()



    def apply_membership(self, req) :
        try :
            if (self.type == NodeType.LEADER) :
                req = self.message_parser.deserialize(req)

                self.cluster_addr_list.append(Address(**req["address"]))
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
            "status": ResponseStatus.REDIRECTED.value,
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
                    self.__print_log(" Didn't get response from leader, retrying...")
                    time.sleep(RaftNode.HEARTBEAT_INTERVAL)
                    retry_count += 1
                else :
                    self.__print_log("Leader failed to response 5 times, aborting membership application")
                    break
        if (response["status"] == "success") :
            self.log                 = response["log"]
            self.cluster_addr_list   = response["cluster_addr_list"]
            self.cluster_leader_addr = redirected_addr
            self.__print_log(f"Membership applied, current cluster: {self.cluster_addr_list}")
            self.__print_log(f"Current leader: {self.cluster_leader_addr}")

    def __send_request(self, request: BaseMessage, rpc_name: str, addr: Address) -> "json":
        # Warning : This method is blocking
        response     = self.rpc_handler.request(addr, rpc_name, request)
        # self.__print_log(response)
        return response

    # Inter-node RPCs
    def heartbeat(self, json_request: str) -> "json":
        # TODO : Implement heartbeat for follower
        self.randomize_timeout()
        request = self.message_parser.deserialize(json_request)
        with self.stable_storage as stable_vars:
            if request["election_term"] == stable_vars["election_term"]:    #masih di election term yang sama, klo candidate di restart jd follower
                self.type = RaftNode.NodeType.FOLLOWER
                self.cluster_leader_addr = request["leader_addr"]
                self.votes_received = set[Address]()
            elif request["election_term"] > stable_vars["election_term"]:
                stable_vars.update({
                    "election_term": stable_vars["election_term"],
                    "voted_for" : None,
                })
                self.stable_storage.storeAll(stable_vars)
            all_sync = (request["election_term"]== stable_vars["election_term"] and len(stable_vars["log"]) >= request["prev_last_index"]) and (request["prev_last_index"] == 0 or stable_vars["log"][-1]["term"] == request["prev_last_term"])
            response = {
                "heartbeat_response": "ack",
                "address":            self.address,
                "status": ResponseStatus.SUCCESS.value,
                "election_term": stable_vars["election_term"],
                "reason": ""
            }
            if(all_sync):
                response["sync"] = True
                # append entries
            else:
                response["sync"] = False
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
        

    def randomize_timeout(self):
        self.timeout_time = time.time() + RaftNode.ELECTION_TIMEOUT_MIN + (RaftNode.ELECTION_TIMEOUT_MAX - RaftNode.ELECTION_TIMEOUT_MIN) * random.random()
        

