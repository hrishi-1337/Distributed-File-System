import os

fileSizeInBytes = 10240000
for i in range(10):
    with open('data/'+str(i)+'.data', 'wb') as fout:
        fout.write(os.urandom(fileSizeInBytes)) 