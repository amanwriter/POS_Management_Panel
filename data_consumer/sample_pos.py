from random import choice
import requests
# Sample script to read CSV files and make calls to consumer with the data

data_path = "/Users/amanmathur/Desktop/MUSE/Data/"
csv_files = ["BUSY7246fb6.csv","EASYSOL326dc49.csv","MARGfc72cd7.csv",
                "RETAIL_EXPERe490dde.csv","SOFT_GENd6661f5.csv"]

source_files = []
for file in csv_files:
    source_files.append(open(data_path+file, 'r'))
    # Reading header line
    source_files[-1].readline()


data_point = choice(source_files).readline()
prog = 0
while len(source_files):
    prog+=1
    if prog%10000==0:
        print(prog)
    try:
        requests.post('http://127.0.0.1:8000/consume', data=str(data_point.strip()))
    except:
        pass
    fd = choice(source_files)
    data_point = fd.readline()
    while not data_point:
        source_files.remove(fd)
        fd = choice(source_files)
        data_point = fd.readline()
