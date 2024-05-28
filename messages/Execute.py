from messages.Base import BaseRequest, BaseResponse

class ExecuteRequest(BaseRequest):
    command: str
    value: str

class ExecuteResponse(BaseResponse):
    data: dict