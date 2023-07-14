import json
import math
import sys
import threading
import time
import os
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer


class Client:
    def __init__(self):
        self.FILE_SIZE = 10240000
        self.CHUNK_SIZE = 2560000
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

    def displayFile(self, file):
        pass

    def createFile(self, file):
        with open('data/' + file, 'wb') as fout:
            fout.write(bytes(file, 'utf-8')*self.FILE_SIZE)
            fout.close()

        parts_count = self.split(file)  
        chunk_locations = {}
        parts = [i for i in range (1, parts_count+1)]
        window = math.ceil(parts_count/4)
        overlap = math.ceil(parts_count/4)
        splits = [parts[i:i+window+overlap] for i in range(0, len(parts), window)]
        j = 0
        for i in splits[0]:
            if i not in splits[3] and j < overlap:
                splits[3].append(i)
                j += 1

        for i in range(len(splits)):
            for j in splits[i]:
                if j in chunk_locations:
                    chunk_locations[j].append(i+1)
                else:
                    chunk_locations[j] = [i+1]

        # print(parts_count)  
        # print(splits)
        # print(len(splits))
        # print(chunk_locations)

        self.files[file] = (parts_count, chunk_locations)

        os.remove('data/' + file)
        for i in range(1, parts):
            os.remove('data/' + file + '_' + str(i))

    def split(self, file):
        parts = 1
        input = open('data/' + file, 'rb')                   
        while 1:                                       
            chunk = input.read(self.CHUNK_SIZE)              
            if not chunk: break
            filename = os.path.join("data", (file + '_' + str(parts)))
            fileobj  = open(filename, 'wb')
            fileobj.write(chunk)
            parts += 1
            fileobj.close()                            
        input.close()
        return parts-1
    
    def merge(self, file):
        output = open('storage/' + file, 'wb')
        parts  = self.files[file]
        for part in range(1, parts):
            filepath = os.path.join("data", file + '_' + str(part))
            fileobj  = open(filepath, 'rb')
            while 1:
                filebytes = fileobj.read(self.CHUNK_SIZE)
                if not filebytes: break
                output.write(filebytes)
            fileobj.close()
        output.close()

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
            print("List Files\t\t[l]")
            print("Create File\t\t[c <filename>]")
            print("Delete File\t\t[d <filename>]")
            print("Exit\t\t\t[e]")
            resp = input("Choice: ").lower().split()
            if not resp:
                continue
            elif resp[0] == 'f':
                print("===========================")
                print("Files:")
                for k, v in self.files.items():
                    print(f"{k}") 
                resp2 = input("Choice: ").lower().split()
                self.displayFile(resp2[0])            
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