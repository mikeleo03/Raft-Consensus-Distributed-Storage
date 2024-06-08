import Server
import Address

if __name__ == '__main__':
    # Memberhsip change test
    leader = Server.start_serving(Address("localhost", 8000))