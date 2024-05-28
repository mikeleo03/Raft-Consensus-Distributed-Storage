from typing import TypedDict
from Address import Address
from enum import Enum

class BaseMessage(TypedDict):
    ...

class BaseRequest(BaseMessage):
    ...

class ResponseStatus(Enum):
    SUCCESS = "success"
    REDIRECTED = "redirected"
    FAILED = "failed"
    ONPROCESS = "onprocess"

class BaseResponse(BaseMessage):
    status: ResponseStatus
    address: Address
    reason: str