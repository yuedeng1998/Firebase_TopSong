

import sys
from urllib import response
import requests
import csv
import json
import string
import re


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
        data_addresses.append('/d'+str(i+70)+'.json')
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


def search_dict(root, path):
    if not root:
        root = {'root': {}}
    node = root['root']
    paths = path.split('/')
    for i in range(len(paths)):
        if not paths[i]:
            continue
        if paths[i] not in node:
            node['is_dict'] = True
            node[str(paths[i])] = {}
        node = node[str(paths[i])]
    node['is_dict'] = True
    return root


def make_dir(url, data_path):
    response = requests.get(url).json()
    out = search_dict(response, data_path)
    out = json.dumps(out)
    response = requests.put(url, out)


def ls_all_files(cur_path):
    response = requests.get(
        'https://project-dc1b5-default-rtdb.firebaseio.com/root'+cur_path + '.json').json()
    if not response:
        print('')
        return
    if 'is_dict' not in response.keys() or not response['is_dict']:
        raise ValueError('Not a Dictionary!')
    for key in response.keys():
        if key == 'is_dict':
            continue
        print(key)


def getPartitionLocations(file_path):
    file_path = file_path.replace('.', '/')
    response = requests.get(
        'https://project-dc1b5-default-rtdb.firebaseio.com/root'+file_path + '.json').json()
    if not response:
        raise ValueError('No Such File!')
    data_addresses = []
    for i in range(len(response.items())):
        data_addresses.append(response['p'+str(i+1)])
    return data_addresses


def readPartition(data_addresses):
    data_url = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
    file_content = []
    for address in data_addresses:
        file_url = data_url + address
        file_content.append(requests.get(file_url).json())
    return file_content


def rmPartition(data_addresses):
    data_url = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
    for address in data_addresses:
        file_url = data_url + address
        requests.delete(file_url)


def rm_file(file_path):
    if not file_path.__contains__('.'):
        raise ValueError('Only File can be delete')
    data_addresses = getPartitionLocations(file_path)
    rmPartition(data_addresses)
    file_path = file_path.replace('.', '/')
    requests.delete(
        'https://project-dc1b5-default-rtdb.firebaseio.com/root' + file_path + '.json')


def cat_file(file_path):
    data_addresses = getPartitionLocations(file_path)
    return readPartition(data_addresses)


if len(sys.argv) < 3:
    raise ValueError('Need valid data path!')
instruction = sys.argv[1]
data_path = sys.argv[2]
url = 'https://project-dc1b5-default-rtdb.firebaseio.com/.json'
if instruction == 'put':
    file_src = sys.argv[3]
    k = int(sys.argv[4])

if instruction == 'mkdir':
    make_dir(url, data_path)
elif instruction == 'ls':
    ls_all_files(data_path)
elif instruction == 'cat':
    print(cat_file(data_path))
elif instruction == 'rm':
    rm_file(data_path)
    # data folder need at least 1 attribute or it will be delete,
    #  if data we want to delete already deleted, no error
elif instruction == 'put':
    upload(file_src, data_path, k)

# TODO 1: Init create root and data

# TODO 2: avoid define url in func

# TODO 3: add/test if upload csv file
