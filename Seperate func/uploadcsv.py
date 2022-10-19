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
        maxkey = max(contents.keys())
        interval = int(maxkey)//k
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
        data_addresses.append('/d'+str(i+50)+'.json')
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
file_src = 'car.csv'
data_path = '/user/upcsv'
k = 6
upload(rurl, durl, file_src, data_path, k, 'car_ID')