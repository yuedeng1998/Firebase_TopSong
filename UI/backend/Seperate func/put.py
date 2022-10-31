import sys
from urllib import response
import requests
import csv
import json

file_src = 'car.txt'
data_path = '/user/johns/'


def partition_data(file_src, k):
    with open(file_src) as f:
        contents = f.read()
    partitions = []
    if len(contents) < k:
        raise ValueError('Not enougth data to partition')
    partlen = int(len(contents)/k)
    last = 0
    for i in range(k-1):
        partitions.append(contents[i*partlen:i*partlen+partlen])
        last = i*partlen+partlen
    partitions.append(contents[last:])
    return partitions


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


def add_new_data(data, k, datalist, data_addresses):
    for i in range(k):
        data[data_addresses[i][1:-5]] = datalist[i]
        # data[i+10] = datalist[i]
    return data


def upload(file_src, data_path, k):
    url = 'https://project-dc1b5-default-rtdb.firebaseio.com/root.json'
    root = requests.get(url).json()
    dest_dict = search_dest(root, data_path)
    filename, extend = file_src.split('.')
    # not a dict
    dest_dict[filename] = {extend: {}}
    datanode = dest_dict[filename][extend]
# 2: Partition functions to assign k places return data_addresses
    data_addresses = []
    for i in range(k):
        data_addresses.append('/d'+str(i+90)+'.json')
# 3: Append meta full file name path and storage dict 'p1: '/1.json'
    for i in range(k):
        datanode['p'+str(i+1)] = data_addresses[i]
    root = json.dumps(root)
# 4: Write data to places
    response = requests.put(url, root)
    datalist = partition_data(file_src, k)
# read src file and partition into files
    urld = 'https://project-dc1b5-default-rtdb.firebaseio.com/data/.json'
    data = requests.get(urld).json()
    data = add_new_data(data, k, datalist, data_addresses)
    out = json.dumps(data)
    response = requests.put(urld, out)


upload(file_src, data_path, 9)
# # Data Part
# datalist = partition_data(file_src, k)
# # read src file and partition into files
# urld = 'https://project-dc1b5-default-rtdb.firebaseio.com/data/.json'
# data = requests.get(urld).json()
# data = add_new_data(data, k, datalist, data_addresses)
# out = json.dumps(data)
# response = requests.put(urld, out)

# # Meta Data Part
# # 1: Search the meta data, find the right dict
# url = 'https://project-dc1b5-default-rtdb.firebaseio.com/root.json'
# root = requests.get(url).json()
# dest_dict = search_dict(root, data_path)
# filename, extend = file_src.split('.')
# # not a dict
# dest_dict[filename] = {extend: {}}
# datanode = dest_dict[filename][extend]
# # 2: Partition functions to assign k places return data_addresses
# k = 3
# data_addresses = []
# for i in range(k):
#     data_addresses.append('/d'+str(i+90)+'.json')
# # 3: Append meta full file name path and storage dict 'p1: '/1.json'
# for i in range(k):
#     datanode['p'+str(i+1)] = data_addresses[i]
# root = json.dumps(root)
# # 4: Write data to places
# response = requests.put(url, root)


# # Data Part
# datalist = partition_data(file_src, k)
# # read src file and partition into files
# urld = 'https://project-dc1b5-default-rtdb.firebaseio.com/data/.json'
# data = requests.get(urld).json()
# data = add_new_data(data, k, datalist, data_addresses)
# out = json.dumps(data)
# response = requests.put(urld, out)
