from typing import List
from Address import Address
from structs.Log import Log


class AppendEntry:
    pass

class Request:
    def __init__(self, leader_addr, prev_last_index, prev_last_term, election_term, prev_log_index, entries, leader_commit):
        self.leader_addr: Address = leader_addr
        self.prev_last_index: int = prev_last_index
        self.prev_last_term: int = prev_log_index
        self.election_term: int = election_term,
        self.entries: List[Log] = entries
        self.leader_commit: int = leader_commit

    def toDict(self):
        return {
            "leader_addr" : {
                "ip" : self.leader_addr.ip,
                "port": self.leader_addr.port
            },
            "prev_last_index" : self.prev_last_index,
            "prev_last_term" : self.prev_last_term,
            "election_term" : self.election_term,
            "entries" : self.entries,
            "leader_commit" : self.leader_commit,
        }


class Response:
    def __init__(self, success) -> None:
        self.success: bool = success
    def toDict(self):
        return {
            "success" : self.success
        }
    

