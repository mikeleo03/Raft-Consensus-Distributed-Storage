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
    RETRY_COUNT = 5
    ELECTION_TIMEOUT_MIN = 8
    ELECTION_TIMEOUT_MAX = 18
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
        self.address:             Address           = addr
        self.type:                NodeType          = NodeType.FOLLOWER
        self.log:                 List[Log]         = []
        self.app:                 KVStore           = application
        self.election_term:       int               = 0
        self.cluster_addr_list:   List[Address]     = []
        self.cluster_leader_addr: Address           = None
        self.heartbeat_time:      float             = time.time()
        self.timeout_time:        float             = time.time() + RaftNode.ELECTION_TIMEOUT_MIN + (RaftNode.ELECTION_TIMEOUT_MAX - RaftNode.ELECTION_TIMEOUT_MIN) * random.random()
        """ DELETE FOR LATER, DEBUGGING TIME"""
        self.debug_time:         float             = time.time()
        self.current_time:        float             = time.time()
        self.votes_received:    Set[Address] 
        self.ack_length:        Dict[Address, int]  = {}
        self.sent_length:       Dict[Address, int]  = {}

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
        print(ColorLog.colorize(f"[{self.address}]", ColorLog._BLUE) + f"[{time.strftime('%H:%M:%S')}]" + RaftNode._LOG_ROLE[self.type] + " " + text)

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

            self.debug()

    async def __follower_timeout(self):
        while self.type == NodeType.FOLLOWER:
            if time.time() > self.timeout_time:
                self.__print_log(ColorLog.colorize("[TIMEOUT] ", ColorLog._RED) + "Timeout has occured, changing to candidate...")
                self.type = NodeType.CANDIDATE
                break

            if self.election_term == 0xDEAD: 
                self.__print_log("Stopping Follower Server...")
                return
            self.debug()
        self.__print_log("Starting election...")
        self.election_term += 1
        self.__start_election()

    def __start_election(self):
        #randomize timeout
        self.randomize_timeout()

        # initialize voting
        self.voted_for = self.address
        while(time.time() < self.timeout_time):
            self.votes_received = set()
            self.__print_log(f"Starting election for term {self.election_term}")
            self.__print_log(f"Voted for {self.address}")
            self.votes_received.add(self.address)
            self.__print_log(f"Sending vote requests to other nodes...")
            for addr in self.cluster_addr_list:
                if self.address != addr:
                    self.send_vote_request(addr)
                if(self.type != NodeType.CANDIDATE):
                    return
            self.__print_log(f"Vote results: {self.votes_received}")
            self.__print_log("retrying election...")

            if self.election_term == 0xDEAD: 
                self.__print_log("Stopping Follower Server...")
                return
        


        if(self.type == NodeType.CANDIDATE):
            self.__print_log(ColorLog.colorize("[TIMEOUT]", ColorLog._RED) + " Timeout Occured, retrying election for next term...")
            self.election_term += 1
            return self.__start_election()
        else:
            return #finish election

    def send_vote_request(self, addr: Address):
        with self.stable_storage as stable_vars:
            # request = {
            #     "candidate_addr": self.address,
            #     "election_term": stable_vars["election_term"],
            #     "last_log_term": stable_vars["log"][-1]["term"] if len(stable_vars["log"]) > 0 else 0,
            #     "last_log_index": len(stable_vars["log"]) - 1,
            # }
            request : BaseMessage = {
                "candidate_addr": self.address,
                "election_term": self.election_term,
                # "last_log_term": stable_vars["log"][-1]["term"] if len(stable_vars["log"]) > 0 else 0,
                # "last_log_index": len(stable_vars["log"]) - 1,
            }
            try:
                if(self.type == NodeType.FOLLOWER):
                    return self.__initialize_as_follower()
                response = self.__send_request(request, "vote", addr)
            except Exception as e:
                self.__print_log(f"Failed to get response from {addr} for vote request. Exception: {e}")
                return
            
            # unsuccessful vote request
            if response["status"] != ResponseStatus.SUCCESS.value:
                self.__print_log(f"Failed to get voting from {addr} for vote request")
                self.__print_log(f"Reason: {response['reason']}")
                return

            # if response["election_term"] > stable_vars["election_term"]: # get heartbeats from other node leader
            if response["election_term"] > self.election_term:
                stable_vars.update({
                    "election_term": response["election_term"],
                    "voted_for": None,
                })
                self.election_term = response["election_term"]
                self.stable_storage.storeAll(stable_vars)
                self.type = NodeType.FOLLOWER
                self.votes_received = set()
                return self.__initialize_as_follower()

            if response["status"] == ResponseStatus.SUCCESS.value:
                self.votes_received.add(addr)
                if len(self.votes_received) >= math.floor(len(self.cluster_addr_list) / 2) + 1:
                    self.type = NodeType.LEADER
                    self.__print_log("Election won, changing to leader...")
                    self.__print_log(f"Voting result: {self.votes_received}")
                    return self.__initialize_as_leader()
    

    def send_heartbeat_msg(self, addr: Address):
        with self.stable_storage as stable_vars:
            prev_last_index = self.sent_length.get(addr, 0)
            next_index = stable_vars["log"][prev_last_index:]
            prev_last_term = stable_vars["log"][prev_last_index - 1]["term"] if prev_last_index > 0 else 0

            try:
                response = self.__send_request({
                    "leader_addr": self.address,
                    "election_term": stable_vars["election_term"],
                    "prev_last_term": prev_last_term,
                    "prev_last_index": prev_last_index,
                    "entries": next_index,
                    "leader_commit": stable_vars["commit_length"],
                }, "heartbeat", addr)
                if response is None:
                    self.__print_log(f"No response from {addr} for heartbeat.")
                    return
                if response["status"] != ResponseStatus.SUCCESS.value:
                    return
            except Exception as e:
                self.__print_log(f"Got an exception when sending heartbeat to {addr}. Something went wrong")
                self.__print_log(f"Exception: {e}")
                return
            
            ack = response["ack"]
            if response["election_term"] == stable_vars["election_term"] and self.type == NodeType.LEADER and response.get("sync"):
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


    """
    RPC Method to apply new membership to the cluster

    """
    def apply_membership(self, req) :
        try:
            if self.type == NodeType.LEADER:
                req = self.message_parser.deserialize(req)
                
                # make sure that the new follower is not already in the cluster
                # self.cluster_addr_list.append(Address(**req["address"]))
                if Address(**req["address"]) in self.cluster_addr_list:
                    response = {
                        "status": ResponseStatus.SUCCESS.value,
                        "address": self.address,
                        "cluster_addr_list": self.cluster_addr_list,
                        "reason": "Already in the cluster",
                        "log": self.log
                    }
                    return self.message_parser.serialize(response)

                self.cluster_addr_list.append(Address(**req["address"]))
                response = {
                    "status": ResponseStatus.SUCCESS.value,
                    "address": self.address,
                    "cluster_addr_list": self.cluster_addr_list,
                    "reason": "Success applying membership",
                    "log": self.log
                }
                self.__print_log(f"Accepted a new follower : {req['address']['ip']}:{req['address']['port']}")

                # iterate for every node in the cluster to update the membership of the cluster
                for addr in self.cluster_addr_list:
                    if addr != self.address and addr != Address(**req["address"]):
                        self.__print_log(ColorLog._MAGENTA.value + f" Updating membership for {addr} " + ColorLog._ENDC.value)
                        try:
                            res = self.__send_request({"address": Address(**req["address"])}, "update_membership", addr)
                        except:
                            pass
       
                        
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
        

    """
    RPC Method to update new membership in the cluster (for Follower/Candidate)
    """
    def update_membership(self, req) :
        # self.__print_log("Updating membership")
        # self.__print_log(ColorLog._MAGENTA.value + req + ColorLog._ENDC.value)
        if self.type == NodeType.FOLLOWER:
            req = self.message_parser.deserialize(req)
            # self.__print_log(ColorLog._MAGENTA.value + f" Received new membership: {req['address']['ip']}:{req['address']['port']} " + ColorLog._ENDC.value)
            _new_addr = Address(**req["address"])
            self.cluster_addr_list.append(_new_addr)

    
    """
    Method to try to apply membership to the cluster when initializing a new node, 
    # Params:
    - contact_addr: Address , Leader of the cluster
    """
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
                if retry_count < RaftNode.RETRY_COUNT:
                    self.__print_log("Didn't get response from leader, retrying...")
                    time.sleep(RaftNode.HEARTBEAT_INTERVAL)
                    retry_count += 1
                else:
                    self.__print_log(ColorLog.colorize(f"Leader failed to respond {RaftNode.RETRY_COUNT} times, aborting membership application", ColorLog._RED))
                    exit()
        if response["status"] == "success":
            self.log = response["log"]
            # self.cluster_addr_list = response["cluster_addr_list"]
            # make response["cluster_addr_list"] as list of Address
            for addr in response["cluster_addr_list"]:
                self.cluster_addr_list.append(Address(**addr))
            self.cluster_leader_addr = redirected_addr
            self.__print_log(f"Membership applied, current cluster: {self.cluster_addr_list}")
            self.__print_log(f"Current leader: {self.cluster_leader_addr}")

    def __send_request(self, request: BaseMessage, rpc_name: str, addr: Address) -> "json":
        self.__print_log(f"Sent request to {addr} : {request}")
        self.__print_log(f"RPC Name: {rpc_name}")
        response = self.rpc_handler.request(addr, rpc_name, request)
        if response is None:
            raise Exception(" " + ColorLog._WARNING.value + f"Failed to get a response from {addr} for {rpc_name} request" + ColorLog._ENDC.value + " ")
        self.__print_log(f"Received response from {addr} : {response}")
        return response

    """
    Internode RPC Method to send heartbeat to other nodes
    """
    def heartbeat(self, json_request: str) -> "json":
        self.type = NodeType.FOLLOWER # make sure when receiving heartbeat, the node is a follower
        self.randomize_timeout()
        request = self.message_parser.deserialize(json_request)
        self.__print_log(f"Received heartbeat from {request['leader_addr']}")
        with self.stable_storage as stable_vars:
            if request["election_term"] == stable_vars["election_term"]:
                self.type = NodeType.FOLLOWER
                self.cluster_leader_addr = Address(**request["leader_addr"])
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
                "reason": "",
                "ack": 5,
            }
            if all_sync:
                self.__append_entries(request["entries"], request["prev_last_index"], request["leader_commit"], stable_vars)
                ack = int(request["prev_last_index"]) + len(request["entries"])
                response["ack"] = ack
                response["sync"] = True
            else:
                response["ack"] = 0
                response["sync"] = False
        return self.message_parser.serialize(response)
    
    """
    RPC Method to vote for a candidate
    """
    def vote(self, json_request: str) -> str:
        request = self.message_parser.deserialize(json_request)
        self.__print_log(f"Received vote request from {request['candidate_addr']} with election term {request['election_term']}")
        
        ## TO DO: FIXXXX THE RESPONSE! TEMPOARY RESPONSE to allow the voting

        # randomize response status generator
        _response_status : ResponseStatus = ResponseStatus.SUCCESS if (self.election_term < int(request["election_term"]))\
                                                                   else ResponseStatus.FAILED
        response = {
            "status": _response_status.value,
            "election_term": max(int(request["election_term"]),self.election_term),
            "address": self.address,
            "reason": "",
        }

        # with self.stable_storage as stable_vars:
        #     self.__print_log(ColorLog._MAGENTA.value + f"CP PPPPPP" + ColorLog._ENDC.value)
        #     if request["election_term"] < stable_vars["election_term"]:
        #         self.__print_log(ColorLog._MAGENTA.value + f"CP 1" + ColorLog._ENDC.value)
        #         response = {
        #             "status": ResponseStatus.SUCCESS.value,
        #             "election_term": stable_vars["election_term"],
        #             "address": self.address,
        #             "reason": "Already voted for a candidate with higher term",
        #         }
        #     elif request["last_log_term"] < stable_vars["log"][-1]["term"]:
        #         self.__print_log(ColorLog._MAGENTA.value + f"CP 2" + ColorLog._ENDC.value)
        #         response = {
        #             "status": ResponseStatus.SUCCESS.value,
        #             "election_term": stable_vars["election_term"],
        #             "address": self.address,
        #             "reason": "Candidate's log is not up-to-date",
        #         }
        #     else:
        #         self.__print_log(ColorLog._MAGENTA.value + f"CP else" + ColorLog._ENDC.value)
        #         stable_vars.update({
        #             "election_term": request["election_term"],
        #             "voted_for": request["candidate_addr"],
        #         })
        #         self.__print_log(ColorLog._MAGENTA.value + f"CP" + ColorLog._ENDC.value)
        #         self.stable_storage.storeAll(stable_vars)
        #         self.__print_log(ColorLog._MAGENTA.value + f"CP" + ColorLog._ENDC.value)
        #         response = {
        #             "status": ResponseStatus.SUCCESS.value,
        #             "election_term": request["election_term"],
        #             "address": self.address,
        #             "reason": "",
        #         }
        # self.__print_log(f"Sending vote response to {request['candidate_addr']} : {response}")
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
                print("Dari commit", log[i]["value"])
                                              
            stable_var["commit_length"] = latest_ack
            self.stable_storage.storeAll(stable_var)
            self.__print_log(f"Committed up to index {latest_ack}")

    def __append_entries(self, entries, prev_last_index, leader_commit, stable_var):
        log = stable_var["log"]

        if len(entries) > 0 and len(log) > prev_last_index:
            idx = min(len(log), prev_last_index + len(entries)) - 1
            if log[idx]["term"] != entries[idx - prev_last_index]["term"]:
                log = log[:prev_last_index]
        
        if prev_last_index + len(entries) > len(log):
            for i in range(len(log) - prev_last_index, len(entries)):
                log.append(entries[i])
        
        stable_var["log"] = log

        commit_length = stable_var["commit_length"]
        if leader_commit > commit_length:
            for i in range(commit_length, leader_commit):
                self.app.executing_log(log[i])
                print("Dari append entries", log[i]["value"])
            stable_var["commit_length"] = leader_commit

        self.stable_storage.storeAll(stable_var)

    # Client RPCs
    def execute(self, json_request: str) -> str:
        try:
            request: ExecuteRequest = self.message_parser.deserialize(json_request)
            with self.stable_storage as stable_vars:
                log = Log({
                    "term": stable_vars["election_term"],
                    "command": request["command"],
                    "value": "",
                })
                self.app.executing_log(log)
                request["value"] = log["value"]
                stable_vars["log"].append(log)
                self.log.append(log)
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

    # delete for later, USING FOR DEBUGGING IN 1hz PERIOD
    def debug(self):
        if(time.time() - self.debug_time > 1):
            # self.__print_log("Debugging")
            # self.__print_log(f"Current Cluster: {self.cluster_addr_list}")
            # self.__print_log(f"Current Log: {self.log}")
            # # iterarte to print thype of slef.cluster_addr_list
            # for addr in self.cluster_addr_list:
            #     self.__print_log(f"Address: {addr}")
            self.debug_time = time.time()
            
