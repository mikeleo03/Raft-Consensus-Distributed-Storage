import time
import subprocess
import sys
import unittest
import os
import signal
import warnings

import requests
from app import KVStore
from structs.ColorLog import ColorLog

# Suppress ResourceWarning
warnings.simplefilter("ignore", ResourceWarning)

class TestKVStore(unittest.TestCase):
    def test_ping(self):
        kv_store = KVStore()
        log = {'term': 1, 'command': 'ping', 'value': ''}
        kv_store.executing_log(log)
        self.assertEqual(log['value'], "PONG")
        print("✅ Unit test ping passed")

    def test_set_and_get(self):
        kv_store = KVStore()
        log_set = {'term': 1, 'command': 'set kunci value', 'value': ''}
        kv_store.executing_log(log_set)
        self.assertEqual(log_set['value'], "OK")

        log_get = {'term': 2, 'command': 'get kunci', 'value': ''}
        kv_store.executing_log(log_get)
        self.assertEqual(log_get['value'], "value")
        print("✅ Unit test set and get passed")

    def test_append(self):
        kv_store = KVStore()
        log_set = {'term': 1, 'command': 'set kunci value', 'value': ''}
        kv_store.executing_log(log_set)
        self.assertEqual(log_set['value'], "OK")
        
        log_append = {'term': 2, 'command': 'append kunci value', 'value': ''}
        kv_store.executing_log(log_append)
        self.assertEqual(log_append['value'], "OK")

        log_get = {'term': 3, 'command': 'get kunci', 'value': ''}
        kv_store.executing_log(log_get)
        self.assertEqual(log_get['value'], "valuevalue")
        print("✅ Unit test append passed")

    def test_delete(self):
        kv_store = KVStore()
        log_set = {'term': 1, 'command': 'set kunci value', 'value': ''}
        kv_store.executing_log(log_set)
        self.assertEqual(log_set['value'], "OK")

        log_get = {'term': 2, 'command': 'del kunci', 'value': ''}
        kv_store.executing_log(log_get)
        self.assertEqual(log_get['value'], "value")
        
        log_get = {'term': 3, 'command': 'get kunci', 'value': ''}
        kv_store.executing_log(log_get)
        self.assertEqual(log_get['value'], "")
        print("✅ Unit test del passed")
        
    def test_strlen(self):
        kv_store = KVStore()
        log_set = {'term': 1, 'command': 'set kunci value', 'value': ''}
        kv_store.executing_log(log_set)
        self.assertEqual(log_set['value'], "OK")

        log_get = {'term': 2, 'command': 'strln kunci', 'value': ''}
        kv_store.executing_log(log_get)
        self.assertEqual(log_get['value'], 5)
        print("✅ Unit test strln passed")

    def test_transaction(self):
        kv_store = KVStore()
        log_transaction = {'term': 1, 'command': 'set kunci value; append kunci 123; get kunci', 'value': ''}
        kv_store.executing_log(log_transaction)
        self.assertEqual(log_transaction['value'], "value123")
        print("✅ Unit test transaction passed")

class TestMembership(unittest.TestCase):
    # def test_fail_to_apply_membership(self):
    #     print(ColorLog.colorize("Running test fail to apply membership for 45 seconds", ColorLog._HEADER))
    #     with subprocess.Popen(["python", "Server.py", "localhost", "5001", "localhost", "5000"], stdout=subprocess.PIPE) as follower:
    #         time.sleep(45)
    #         self.assertTrue(follower.poll() is not None)
    #         follower.kill()
    #     print("✅ Unit test fail to apply membership passed")
    
    # def test_success_to_apply_membership(self):
    #     print(ColorLog.colorize("Running test success to apply membership for 15 seconds", ColorLog._HEADER))
    #     leader = None
    #     follower = None
    #     try:
    #         leader = subprocess.Popen(["python", "Server.py", "localhost", "5000"], stdout=subprocess.PIPE)
    #         follower = subprocess.Popen(["python", "Server.py", "localhost", "5001", "localhost", "5000"], stdout=subprocess.PIPE)
    #         time.sleep(15)
    #         self.assertTrue(follower.poll() is None)
    #     finally:
    #         if follower:
    #             follower.terminate()
    #             follower.kill()
    #             follower.stdout.close()  # Ensure resources are released
    #         if leader:
    #             leader.terminate()
    #             leader.kill()
    #             leader.stdout.close()  # Ensure resources are released
    #     print("✅ Unit test success to apply membership passed")
        
    def test_success_commit_log(self):
        print(ColorLog.colorize("Running test success to commit log for 15 seconds", ColorLog._HEADER))
        leader = None
        follower = None
        client = None
        url = "http://localhost:5000/execute_command"
        try:
            leader = subprocess.Popen(["python", "Server.py", "localhost", "5001"], stdout=subprocess.PIPE)
            follower = subprocess.Popen(["python", "Server.py", "localhost", "5002", "localhost", "5001"], stdout=subprocess.PIPE)
            client = subprocess.Popen(["python", "Client.py", "localhost", "5000"], stdout=subprocess.PIPE)
            time.sleep(15)
            response = requests.post(url, json={
                "address": {
                    "ip": "localhost",
                    "port": 5001
                },
                "command": "ping",
            })
            
            response_log_1 =  requests.post(url, json={
                "address": {
                    "ip": "localhost",
                    "port": 5001
                },
                "command": "request_log",
            })
            response_log_2 =  requests.post(url, json={
                            "address": {
                                "ip": "localhost",
                                "port": 5002
                            },
                            "command": "request_log",
             })
            print("1",response_log_1.json())
            print("2",response_log_2.json())
            self.assertEqual(response_log_1.json()["data"], response_log_2.json()["data"])
        finally:
            if follower:
                follower.terminate()
                follower.kill()
                follower.stdout.close()  # Ensure resources are released
            if leader:
                leader.terminate()
                leader.kill()
                leader.stdout.close()  # Ensure resources are released
            if client:
                client.terminate()
                client.kill()
                client.stdout.close()  # Ensure resources are released

if __name__ == '__main__':
    unittest.main(verbosity=0)
