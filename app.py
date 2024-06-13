from structs.Log import Log
import unittest
import subprocess
import asyncio

class KVStore:
    ALLOWED_COMMANDS = ["ping", "get", "set", "strln", "del", "append"]

    def __init__(self):
        self.store = {}

    def __ping(self):
        return "PONG"

    def __get(self, key):
        return self.store.get(key, "")

    def __set(self, key, value):
        self.store[key] = value
        return "OK"

    def __strln(self, key):
        return len(self.store.get(key, ""))

    def __delete(self, key):
        return self.store.pop(key, "")

    def __append(self, key, value):
        self.store[key] = self.store.get(key, "") + value
        return "OK"

    def _execute_single_command(self, command : str | None):
        command_parts = command.split()
        if len(command_parts) < 1:
            return "Invalid command"

        command_name = command_parts[0]
        if command_name not in self.ALLOWED_COMMANDS:
            return "Invalid command"

        if command_name == "ping":
            return self.__ping()
            
        elif command_name == "get":
            if len(command_parts) < 2:
                return "Invalid command"
            key = command_parts[1]
            return self.__get(key)
            
        elif command_name == "set":
            if len(command_parts) < 3:
                return "Invalid command"
            key = command_parts[1]
            value = " ".join(command_parts[2:])
            return self.__set(key, value)
        
        elif command_name == "strln":
            if len(command_parts) < 2:
                return "Invalid command"
            key = command_parts[1]
            return self.__strln(key)
        
        elif command_name == "del":
            if len(command_parts) < 2:
                return "Invalid command"
            key = command_parts[1]
            return self.__delete(key)
        
        elif command_name == "append":
            if len(command_parts) < 3:
                return "Invalid command"
            key = command_parts[1]
            value = " ".join(command_parts[2:])
            return self.__append(key, value)
        
    def executing_log(self, log: Log):
        commands = log['command'].split('; ')
        result = ""
        for command in commands:
            result = self._execute_single_command(command.strip())
            if result == "Invalid command":
                break
        log['value'] = result

    def data(self):
        return self.store


if __name__ == '__main__':
    print("Running unit tests...")
    unittest.main(verbosity=0)