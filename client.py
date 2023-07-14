import json
import sys
import threading
import time
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer


class Client:
    def __init__(self):
        self.id = None
        self.host = None
        self.port = None
        self.config = None
        self.leader = None
        self.map = {}
        self.files = {}

    def createRPCServer(self):
        print("Creating the RPC server for the Node {0}".format(self.id))
        print("Node {0} IP:{1} port: {2}".format(self.id, self.host, self.port))
        thread = threading.Thread(target=self._executeRPCServer)
        thread.daemon = True
        thread.start()
        return thread

    def _executeRPCServer(self):
        server = SimpleXMLRPCServer((self.host, self.port), logRequests=True, allow_none=True)
        server.register_instance(self)
        try:
            print("Accepting connections..")
            server.serve_forever()
        except KeyboardInterrupt:
            print("Exiting")

    def createProxyMap(self):
        for k, v in self.config.items():
            uri = r"http://" + v[0] + ":" + str(v[1])
            self.map[k] = ServerProxy(uri, allow_none=True)
        print("Map: " +str(self.map))

    def heartbeatThread(self):
        thread = threading.Thread(target=self.heartbeat)
        thread.daemon = True
        thread.start()
        return thread
    
    def heartbeat(self):
        while True:
            if self.leader == self.id:
                for k, v, in self.map.items():
                    if k != str(self.id):
                        try:
                            v.receiveHeartbeat(1)
                            time.sleep(5)
                        except ConnectionRefusedError:
                            pass
    
    def receiveHeartbeat(self, value):
        if value == 1:
            # print("heartbeat received")
            pass

    def createFile(self, filename):
        print(filename)
        self.files[filename] = {1:[1,2], 2:[2,3], 3:[3,4], 4:[1,4]}

    def receiveChunk(self, chunk):
        pass

    def sendChunk(self, chunk):
        pass

    def deleteFile(self):
        pass

    def deleteChunk(self, chunk):
        pass

    def menu(self):
        while True:
            print("Display Files\t\t[f]")
            print("Create File\t\t[c]")
            print("Delete File\t\t[d]")
            print("Exit\t\t\t[e]")
            resp = input("Choice: ").lower().split()
            if not resp:
                continue
            elif resp[0] == 'f':
                print("===========================")
                print("Files:")
                for k, v in self.files.items():
                    print(f"{k}: {v}")                
                print("===========================")
            elif resp[0] == 'c':
                print("===========================")
                self.createFile(resp[1])
                print("===========================")
            elif resp[0] == 'd':
                print("===========================")
                self.deleteFile()
                print("===========================")
            elif resp[0] == 'e':
                exit(0)

    def main(self):        
        if len(sys.argv) > 1:
            self.id = sys.argv[1]

        print("Node number : " +self.id)
        with open("local_config.json", "r") as jsonfile:
            self.config = json.load(jsonfile)
            self.host = self.config[self.id][0]
            self.port = self.config[self.id][1]

        self.createRPCServer()
        self.createProxyMap()
        time.sleep(0.2)
        input("Press <enter> to start")
        print("===========================")
        self.leader = max(self.map)
        print("Node {0} is the leader".format(self.leader))
        
        # self.heartbeatThread()
        self.menu()

if __name__ == '__main__':
    client = Client()
    client.main()