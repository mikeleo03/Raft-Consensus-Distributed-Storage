from utils.MessageParser import MessageParser
from messages.Base import BaseMessage, BaseResponse, ResponseStatus
import json
from Address import Address
from xmlrpc.client import ServerProxy


class RPCHandler:
    def __init__(self, id: str | None = None):
        self.message_parser = MessageParser()
        self.id = id
        
    def __logging(self, message: str):
        print(f"[RPCHandler-{self.id}] {message}")

    def __call(self, addr: Address, rpc_name: str, message: BaseMessage):
        node = ServerProxy(f"http://{addr.ip}:{addr.port}")
        json_request = self.message_parser.serialize(message)
        self.__logging(f"Sending request to {addr.ip}:{addr.port}...")
        rpc_function = getattr(node, rpc_name)

        try:
            response = rpc_function(json_request)
            self.__logging(f"Response from {addr.ip}:{addr.port}: {response}")
            return response
        except Exception as e:
            self.__logging(f"Error while sending request to {addr.ip}:{addr.port}: {e}")
            # TODO : Handle error

    def request(self, addr: Address, rpc_name: str, message: BaseMessage):
        redirect_addr = addr
        response = BaseResponse({
            'status': ResponseStatus.REDIRECTED.value,
            'address': redirect_addr,
        })

        while response["status"] == ResponseStatus.REDIRECTED.value:
            redirect_addr = Address(
                response["address"]["ip"],
                response["address"]["port"],
            )
            response = self.message_parser.deserialize(self.__call(redirect_addr, rpc_name, message))

        # TODO: handle fail response
        if response["status"] == ResponseStatus.FAILED.value:
            self.__logging("Failed to send request")

        response["address"] = redirect_addr
        return response