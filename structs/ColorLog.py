from enum import Enum

class ColorLog(Enum):
    _HEADER = '\033[95m'
    _BLUE = '\033[94m'
    _CYAN = '\033[96m'
    _MAGENTA = '\033[95m'
    _WARNING = '\033[93m'
    _FAIL = '\033[91m'
    _ENDC = '\033[0m'
    _BOLD = '\033[1m'
    _UNDERLINE = '\033[4m'