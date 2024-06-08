import time
import subprocess
import sys

def test_success() :
    leader = subprocess.Popen(["cmd.exe", "/c", "python Server.py localhost 5000"], stdout=subprocess.PIPE)
    follower = subprocess.Popen(["cmd.exe", "/c", "python Server.py localhost 5001 localhost 5000"], stdout=subprocess.PIPE)
    time.sleep(15)
    if (follower.poll() is None) :
        print("Membership change successful")
    else :
        print("Membership change failed")

def test_fail() :
    follower = subprocess.Popen(["cmd.exe", "/c", "python Server.py localhost 5001 localhost 5000"], stdout=subprocess.PIPE)
    time.sleep(15)
    if (follower.poll() is None) :
        print("Membership change successful")
    else :
        print("Membership change failed")

if __name__ == '__main__':
    # Memberhsip change test
    if (len(sys.argv) != 2) :
        print("Usage : python UnitTests.py [success/fail]")
        exit()

    is_success = sys.argv[1] == "success"

    if (is_success) :
        test_success()
    else :
        test_fail()
    