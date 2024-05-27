from enum import Enum

class NodeType(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3
    