from utils.MessageParser import MessageParser
from messages.Base import BaseMessage, BaseResponse, ResponseStatus
import json
from Address import Address
from xmlrpc.client import ServerProxy


class RPCHandler:
    def __init__(self):
        self.message_parser = MessageParser()

    def __call(self, addr: Address, rpc_name: str, message: BaseMessage):
        node = ServerProxy(f"http://{addr.ip}:{addr.port}")
        json_request = self.message_parser.serialize(message)
        print(f"Sending request to {addr.ip}:{addr.port}...")
        rpc_function = getattr(node, rpc_name)

        try:
            response = rpc_function(json_request)
            print(f"Response from {addr.ip}:{addr.port}: {response}")
            return response
        except Exception as e:
            print(f"Error while sending request to {addr.ip}:{addr.port}: {e}")
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
            print("Failed to send request")

        response["address"] = redirect_addr
        return response