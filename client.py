import json
import math
import sys
import threading
import time
import os
import shutil
from xmlrpc.client import ServerProxy, Binary
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
        self.file_ledger = {}

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
            if self.leader != self.id:
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

    def getLedger(self):        
        if self.leader != self.id:
            try:
                self.file_ledger = self.map[self.leader].sendLedger()
            except ConnectionRefusedError:
                self.election()

    def election():
        pass

    def viewFile(self, file):        
        self.getLedger()
        print("Retrieving file..")        
        if file in self.file_ledger:
            chunk_locations = self.file_ledger[file]
            for k, v in chunk_locations.items():
                for i in v:
                    filename = file + '_' + str(k)
                    if self.id == str(i):
                        shutil.copy('storage' + self.id + '/' + filename, 'temp/' + filename)
                    else:
                        self.map[str(i)].sendChunk(self.id, filename)    
            self.merge(file, len(chunk_locations))
            f = open('temp/' + file,"r")
            print("File successfully retrieved!")
            print(f.read(20))
        else:
            print("File not present")

    def sendChunk(self, id, filename):
        with open('storage' + self.id + '/' + filename, "rb") as handle:
            binary_data = Binary(handle.read())
        self.map[str(id)].receiveChunk(filename, binary_data)

    def receiveChunk(self, filename, binary_data):
        with open('temp/' + filename, "wb") as handle:
            handle.write(binary_data.data)

    def createFile(self, file):
        with open('temp/' + file, 'wb') as fout:
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
                if str(j) in chunk_locations:
                    chunk_locations[str(j)].append(i+1)
                else:
                    chunk_locations[str(j)] = [i+1]

        # print(parts_count)  
        # print(splits)
        # print(len(splits))
        # print(chunk_locations)

        print("Storing file..")
        self.sendChunks(file, chunk_locations)
        self.file_ledger[file] = chunk_locations

        if self.leader != self.id:
            try:
                self.map[self.leader].updateLedger(file, chunk_locations)
            except ConnectionRefusedError:
                self.election()

        print(f"File {file} created with {parts_count} parts and located at nodes: {chunk_locations}")
        os.remove('temp/' + file)
        for i in range(1, parts_count + 1):
            os.remove('temp/' + file + '_' + str(i))

    def split(self, file):
        parts = 1
        input = open('temp/' + file, 'rb')                   
        while 1:                                       
            chunk = input.read(self.CHUNK_SIZE)              
            if not chunk: break
            filename = os.path.join("temp", (file + '_' + str(parts)))
            fileobj  = open(filename, 'wb')
            fileobj.write(chunk)
            parts += 1
            fileobj.close()                            
        input.close()
        return parts-1
    
    def merge(self, file, parts):
        output = open('temp/' + file, 'wb')
        for part in range(1, parts+1):
            filepath = os.path.join("temp", file + '_' + str(part))
            fileobj  = open(filepath, 'rb')
            while 1:
                filebytes = fileobj.read(self.CHUNK_SIZE)
                if not filebytes: break
                output.write(filebytes)
            fileobj.close()
        output.close()

    def sendChunks(self, file, chunk_locations):
        for k, v in chunk_locations.items():
            for i in v:
                filename = file + '_' + str(k)
                with open("temp/" + filename, "rb") as handle:
                    binary_data = Binary(handle.read())
                self.map[str(i)].saveChunk(filename, binary_data)

    def saveChunk(self, filename, binary_data):
        with open('storage' + self.id + '/' + filename, "wb") as handle:
            handle.write(binary_data.data)

    def deleteChunk(self, filename):
        os.remove('storage' + self.id + '/' + filename)

    def deleteFile(self, file):
        if file in self.file_ledger:
            chunk_locations = self.file_ledger[file]
            for k, v in chunk_locations.items():
                for i in v:
                    filename = file + '_' + str(k)
                    if self.id == str(i):
                        self.deleteChunk(filename)
                    else:
                        self.map[str(i)].deleteChunk(filename)
            if len(self.file_ledger) == 1:
                self.file_ledger.pop(file)
                self.file_ledger = {}
            else:
                self.file_ledger.pop(file)
            return True
        else:
            return False

    def sendLedger(self):
        return self.file_ledger

    def updateLedger(self, file, chunk_locations):
        self.file_ledger[file] = chunk_locations

    def menu(self):
        while True:
            print("List Files\t\t[l]")
            print("View File\t\t[v <filename>]")
            print("Create File\t\t[c <filename>]")
            print("Delete File\t\t[d <filename>]")
            print("Exit\t\t\t[e]")
            resp = input("Choice: ").lower().split()
            if not resp:
                continue
            elif resp[0] == 'l':
                self.getLedger()
                print("===========================")
                print("Files:")
                for k, v in self.file_ledger.items():
                    print(f"{k} | Nodes: {v}")           
                print("===========================")
            elif resp[0] == 'c':
                print("===========================")
                self.createFile(resp[1])
                print("===========================")
            elif resp[0] == 'v':
                print("===========================")
                self.viewFile(resp[1])
                print("===========================")
            elif resp[0] == 'd':
                if self.leader != self.id:
                    try:
                        flag = self.file_ledger = self.map[self.leader].deleteFile(resp[1])
                    except ConnectionRefusedError:
                        self.election()
                else:
                    flag = self.deleteFile(resp[1])
                print("===========================")
                if flag:
                    print("File successfully deleted")
                else:
                    print("File not present")
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