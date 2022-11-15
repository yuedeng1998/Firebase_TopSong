from ast import Raise
import sys
from urllib import response
import requests
import csv
import json

def csv2json(csv_path, pk):
    if not pk:
        raise ValueError('Dont know partition on which key')
    out = {}
    with open(csv_path, encoding='utf-8') as data:
        data_rows = csv.DictReader(data)
        for row in data_rows:
            id = row[pk]
            out[id] = row
    return out

def search_dest(root, path):
    if not root:
        root = {}
    node = root
    paths = path.split('/')
    for i in range(len(paths)):
        if not paths[i]:
            continue
        if paths[i] not in node:
            node['is_dict'] = True
            node[str(paths[i])] = {}
        node = node[str(paths[i])]
    node['is_dict'] = True
    return node

def partition_data(file_src, k, pk):
    _, ext = file_src.split('.')
    partitions = []
    if ext == 'csv':
        contents = csv2json(file_src, pk)
        length = len(contents.keys())
        interval = length//k
        last = 0
        partitions = [[] for i in range(k)]
        for i in range(k):
            for key, row in contents.items():
                key = int(key)
                if i < k-1 and key>=i*interval and key < i*interval+interval:
                    partitions[i].append(row.copy())
                if i == k-1 and key >= (k-1)*interval:
                    partitions[i].append(row.copy())
        return partitions


    with open(file_src) as f:
        contents = f.read()
    if len(contents) < k:
        raise ValueError('Not enougth data to partition')
    partlen = int(len(contents)/k)
    last = 0
    for i in range(k-1):
        partitions.append(contents[i*partlen:i*partlen+partlen])
        last = i*partlen+partlen
    partitions.append(contents[last:])
    return partitions

def add_new_data(data, k, datalist, data_addresses):
    for i in range(k):
        data[data_addresses[i][1:-5]] = datalist[i]
        # data[i+10] = datalist[i]
    return data

import random
def generate_address():
    n = 0 # inital value, obviously has null bytes
    while n & 0xff == 0 or n & 0xff00 == 0:
        n = random.randint(0x7fffffff0000, 0x7fffffffffff)
    return str(n)

#Return True if new address is Invalid
def check_address(new_address):
    durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
    response = requests.get(durl+ '.json').json()
    for key in response.keys():
        if new_address == key:
            return True
    return False

def upload(rurl, durl, file_src, data_path, k, pk):
    url = rurl +'.json'
    root = requests.get(url).json()
    dest_dict = search_dest(root, data_path)
    filename, extend = file_src.split('.')
    # not a dict
    dest_dict[filename] = {extend: {}}
    datanode = dest_dict[filename][extend]
# 2: Partition functions to assign k places return data_addresses
    data_addresses = []
    for i in range(k):
        new_address = generate_address()
        while check_address(new_address):
            new_address = generate_address()
        data_addresses.append('/' + new_address + '.json')
# 3: Append meta full file name path and storage dict 'p1: '/1.json'
    for i in range(k):
        datanode['p'+str(i+1)] = data_addresses[i]
    root = json.dumps(root)
# 4: Write data to places
    response = requests.put(url, root)
    datalist = partition_data(file_src, k, pk)
# read src file and partition into files
    urld = durl +'.json'
    data = requests.get(urld).json()
    data = add_new_data(data, k, datalist, data_addresses)
    out = json.dumps(data)
    response = requests.put(urld, out)
rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
# file_src = '/Users/stella/Firebase_TopSong/Seperate func/car.csv'


# data_path = '/spotify'
# file_src = 'spotify_top_charts_22_edit.csv'
data_path = '/tiktok'
file_src = 'TikTok_songs_2022_edit.csv'
k = 5
upload(rurl, durl, file_src, data_path, k, 'index')

# TODO FOR Task 2: 
# 1. need flag to choose unused location for new data 
# 2. hash function might need more cases: 
#       csv: cur just depend on the range of pk which is a int (force enter a pk(a column name, if txt enter ""))
#       txt: just divide equally by the length of the content 
# 3. current data_address is data:{"intkey": value, ....} 
#       might use more nested way for future larger dataset or more complex hash function

