import asyncio
from threading import Thread
from xmlrpc.client import ServerProxy
from typing import Any, List, Set, TypedDict, Dict
from enum import Enum
from Address import Address
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
from StableStorage import StableStorage
import math

class RaftNode:
    HEARTBEAT_INTERVAL = 1
    ELECTION_TIMEOUT_MIN = 2
    ELECTION_TIMEOUT_MAX = 3
    RPC_TIMEOUT = 0.5
    _LOG_ROLE = {
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

    def __init__(self, application: KVStore, addr: Address, contact_addr: Address = None):
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        self.address: Address = addr
        self.type: NodeType = NodeType.FOLLOWER
        self.log: List[Log] = []
        self.app: KVStore = application
        self.election_term: int = 0
        self.cluster_addr_list: List[Address] = []
        self.cluster_leader_addr: Address = None
        self.heartbeat_time: float = time.time()
        self.timeout_time: float = time.time() + RaftNode.ELECTION_TIMEOUT_MIN + (RaftNode.ELECTION_TIMEOUT_MAX - RaftNode.ELECTION_TIMEOUT_MIN) * random.random()
        self.is_timeout: bool = False
        self.current_time: float = time.time()
        self.votes_received: Set[Address] = set()
        self.ack_length: Dict[Address, int] = {}
        self.sent_length: Dict[Address, int] = {}
        
        # Get state from stable storage
        self.__fetch_stable_storage()
        
        # Additional vars
        self.message_parser: MessageParser = MessageParser()
        self.rpc_handler: RPCHandler = RPCHandler()
        
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

    def __print_log(self, text: str):
        print(ColorLog._BLUE.value + f"[{self.address}]" + ColorLog._ENDC.value + f"[{time.strftime('%H:%M:%S')}]" + RaftNode._LOG_ROLE[self.type] + f" {text}")

    def __initialize_as_leader(self):
        self.__print_log("Initialize as leader node...")
        self.cluster_leader_addr = self.address
        self.type = NodeType.LEADER
        request = {
            "cluster_leader_addr": self.address
        }
        self.heartbeat_thread = Thread(target=asyncio.run, args=[self.__leader_heartbeat()])
        self.heartbeat_thread.start()

    def __initialize_as_follower(self):
        self.__print_log("Initialize as follower node...")
        self.type = NodeType.FOLLOWER
        self.election_term = 0
        self.voted_for = None
        self.follower_timeout_thread = Thread(target=asyncio.run, args=[self.__follower_timeout()])
        self.randomize_timeout()
        self.follower_timeout_thread.start()

    async def __leader_heartbeat(self):
        while self.type == NodeType.LEADER:
            if time.time() - self.heartbeat_time > RaftNode.HEARTBEAT_INTERVAL:
                self.__print_log("Sending heartbeat...")
                for addr in self.cluster_addr_list:
                    if self.address != addr:
                        self.send_heartbeat_msg(addr)
                self.heartbeat_time = time.time()

            if self.election_term == 0xDEAD: 
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
            prev_last_index = self.sent_length.get(addr, 0)
            next_index = stable_vars["log"][prev_last_index:]
            prev_last_term = stable_vars["log"][prev_last_index - 1]["term"] if prev_last_index > 0 else 0
            request = {
                "leader_addr": self.address,
                "election_term": stable_vars["election_term"],
                "prev_last_term": prev_last_term,
                "prev_last_index": prev_last_index,
                "entries": next_index,
                "leader_commit": stable_vars["commit_length"],
            }
        
        print("request", request)
        response = self.__send_request(request, "heartbeat", addr)
        if response["status"] != ResponseStatus.SUCCESS.value:
            return
        
        ack = response["ack"]
        with self.stable_storage as stable_vars:
            if (response["election_term"] == stable_vars["election_term"] and self.type == NodeType.LEADER) and response["sync"]:
                if ack >= self.ack_length.get(addr, 0):
                    self.ack_length[addr] = ack
                    self.sent_length[addr] = ack
                    self.__commit_log(stable_vars)
                    
            elif response["election_term"] > stable_vars["election_term"]:
                stable_vars.update({
                        "election_term": response["election_term"] + 1,
                        "voted_for": None,
                })
                self.stable_storage.storeAll(stable_vars)
                self.type = NodeType.FOLLOWER
                self.votes_received = set()

    def apply_membership(self, req) :
        try:
            if self.type == NodeType.LEADER:
                req = self.message_parser.deserialize(req)

                self.cluster_addr_list.append(Address(**req["address"]))
                response = {
                    "status": ResponseStatus.SUCCESS.value,
                    "address": self.address,
                    "cluster_addr_list": self.cluster_addr_list,
                    "reason": "",
                    "log": self.log
                }
                self.__print_log(f"Accepted a new follower : {req['address']['ip']}:{req['address']['port']}")
                return self.message_parser.serialize(response)
            else:
                response = {
                    "status": ResponseStatus.REDIRECTED.value,
                    "address": self.cluster_leader_addr,
                    "reason": "NOT LEADER"
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
                "ip": contact_addr.ip,
                "port": contact_addr.port,
            }
        }
        while response["status"] != "success":
            try:
                redirected_addr = Address(response["address"]["ip"], response["address"]["port"])
                response = self.__send_request({"address": self.address}, "apply_membership", redirected_addr)
            except:
                if retry_count < 5:
                    self.__print_log("Didn't get response from leader, retrying...")
                    time.sleep(RaftNode.HEARTBEAT_INTERVAL)
                    retry_count += 1
                else:
                    self.__print_log("Leader failed to respond 5 times, aborting membership application")
                    break
        if response["status"] == "success":
            self.log = response["log"]
            self.cluster_addr_list = response["cluster_addr_list"]
            self.cluster_leader_addr = redirected_addr
            self.__print_log(f"Membership applied, current cluster: {self.cluster_addr_list}")
            self.__print_log(f"Current leader: {self.cluster_leader_addr}")

    def __send_request(self, request: BaseMessage, rpc_name: str, addr: Address) -> "json":
        response = self.rpc_handler.request(addr, rpc_name, request)
        if response is None:
            raise Exception(f"Failed to get a response from {addr} for {rpc_name} request")
        self.__print_log(f"Sent request to {addr} : {request}")
        self.__print_log(f"Received response from {addr} : {response}")
        return response

    # Inter-node RPCs
    def heartbeat(self, json_request: str) -> "json":
        self.randomize_timeout()
        request = self.message_parser.deserialize(json_request)
        with self.stable_storage as stable_vars:
            if request["election_term"] == stable_vars["election_term"]:
                self.type = NodeType.FOLLOWER
                self.cluster_leader_addr = request["leader_addr"]
                self.votes_received = set()
            elif request["election_term"] > stable_vars["election_term"]:
                stable_vars.update({
                    "election_term": request["election_term"],
                    "voted_for": None,
                })
                self.stable_storage.storeAll(stable_vars)
            all_sync = (
                                (request["prev_last_index"] == 0 or stable_vars["log"][request["prev_last_index"]-1]["term"] == stable_vars["election_term"] ) and 
                len(stable_vars["log"]) >= request["prev_last_index"]
            )  and (
                request["prev_last_index"] == 0 or stable_vars["log"][request["prev_last_index"]]["term"] == request["prev_last_term"]
            )
            response = {
                "heartbeat_response": "ack",
                "address": self.address,
                "status": ResponseStatus.SUCCESS.value,
                "election_term": stable_vars["election_term"],
                "reason": ""
            }
            print("ack_length", self.ack_length)
            if all_sync:
                self.__append_entries(request["entries"], request["prev_last_index"], request["leader_commit"], stable_vars)
                ack = request["prev_last_index"] + len(request["entries"])
                response["ack"] = ack
                response["sync"] = True
            else:
                response["ack"] = 0
                response["sync"] = False
        return self.message_parser.serialize(response)

    def __commit_log(self, stable_var: StableVars):
        min_ack = math.floor(len(self.cluster_addr_list) / 2) + 1
        log = stable_var["log"]
        
        latest_ack = 0
        for i in range(len(log)):
            ack_count = 0
            for addr in self.cluster_addr_list:
                if self.ack_length.get(addr, 0) >= i:
                    ack_count += 1
            if ack_count >= min_ack:
                latest_ack = i
            
        if latest_ack > stable_var["commit_length"] and log[latest_ack].term == stable_var["election_term"]:
            for i in range(stable_var["commit_length"], latest_ack + 1):
                self.app.executing_log(log[i])
            stable_var["commit_length"] = latest_ack
            self.stable_storage.storeAll(stable_var)
            self.__print_log(f"Committed up to index {latest_ack}")

    def __append_entries(self, entries, prev_last_index, leader_commit, stable_var):
        if prev_last_index >= len(stable_var["log"]) or stable_var["log"][prev_last_index]["term"] != entries[0]["term"]:
            stable_var["log"] = stable_var["log"][:prev_last_index]
            stable_var["log"] += entries
            self.stable_storage.storeAll(stable_var)
            self.__commit_log(stable_var)
        return

    # Client RPCs
    def execute(self, json_request: str) -> str:
        try:
            request: ExecuteRequest = self.message_parser.deserialize(json_request)
            with self.stable_storage as stable_vars:
                log = Log({
                    "term": stable_vars["election_term"],
                    "command": request["command"],
                    "value": request["value"],
                })
                stable_vars["log"].append(log)
                self.stable_storage.storeAll(stable_vars)
                self.ack_length[self.address] = len(stable_vars["log"])
                self.sent_length[self.address] = len(stable_vars["log"])

            response = ExecuteResponse({
                "status": ResponseStatus.SUCCESS.value,
                "address": self.address,
                "data": request["value"]
            })
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
