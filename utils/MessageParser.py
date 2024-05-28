import json
from messages.Base import BaseMessage

class MessageParser:
    def serialize(self, message: BaseMessage) -> str:
        return json.dumps(message)

    def deserialize(self, json_message: str) -> BaseMessage:
        return json.loads(json_message)