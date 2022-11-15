

import sys
from urllib import response
import requests
import csv
import json




def init():
    response = requests.get(url).json()
    if not response:
        out = {'root':{'is_dict':True}, 'data':{'is_dict':True}}
        out = json.dumps(out)
        response = requests.put(url, out)

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


def upload(rurl, durl, file_src, data_path, k):
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
        data_addresses.append('/d'+str(i+30)+'.json')
# 3: Append meta full file name path and storage dict 'p1: '/1.json'
    for i in range(k):
        datanode['p'+str(i+1)] = data_addresses[i]
    root = json.dumps(root)
# 4: Write data to places
    response = requests.put(url, root)
    datalist = partition_data(file_src, k)
# read src file and partition into files
    urld = durl +'.json'
    data = requests.get(urld).json()
    data = add_new_data(data, k, datalist, data_addresses)
    out = json.dumps(data)
    response = requests.put(urld, out)


def search_dict(root, path):
    node = root
    if not node:
        node = {}
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


def make_dir(rurl, data_path):
    rurl +='.json'
    response = requests.get(rurl).json()
    out = search_dict(response, data_path)
    out = json.dumps(out)
    response = requests.put(rurl, out)


def ls_all_files(rurl, cur_path):
    response = requests.get(
       rurl+cur_path + '.json').json()
    if not response:
        print('')
        return
    if 'is_dict' not in response.keys() or not response['is_dict']:
        raise ValueError('Not a Dictionary!')
    for key in response.keys():
        if key == 'is_dict':
            continue
        node = response[key]
        if 'is_dict' not in node.keys():
            for ext in node.keys():
                filename = str(key) +'.' + str(ext)
                print(filename)
        else:
            print(key)


# def getPartitionLocations(rurl, file_path):
#     file_path = file_path.replace('.', '/')
#     response = requests.get(
#         rurl+file_path + '.json').json()
#     if not response:
#         raise ValueError('No Such File!')
#     data_addresses = []
#     for i in range(len(response.items())):
#         data_addresses.append(response['p'+str(i+1)])
#     return data_addresses

def getPartitionLocations(rurl, file_path):
    file_path = file_path.replace('.', '/')
    response = requests.get(
        rurl+file_path + '.json').json()
    if not response:
        raise ValueError('No Such File!')
    data_addresses = {}
    for i in range(len(response)):
        data_addresses['p'+str(i+1)] = response['p'+str(i+1)]
    return data_addresses

# def readPartition(data_url, data_addresses):
#     file_content = []
#     for address in data_addresses:
#         file_url = data_url + address
#         file_content.append(requests.get(file_url).json())
#     return file_content

def readPartition(data_url, rurl, file_path, parition_number):
    data_addresses = getPartitionLocations(rurl, file_path)
    address = data_addresses.get(parition_number)
    file_url = data_url + address
    return requests.get(file_url).json()


def rmPartition(data_url, data_addresses):
    for address in data_addresses:
        file_url = data_url + address
        requests.delete(file_url)

def rm_file(rurl, durl, file_path):
    if not file_path.__contains__('.'):
        raise ValueError('Only File can be delete')
    data_addresses = getPartitionLocations(rurl, file_path)
    locations = []
    for value in data_addresses.values():
        locations.append(value)
    rmPartition(durl, locations)
    file_path = file_path.replace('.', '/')
    requests.delete(
        rurl + file_path + '.json')


def cat_file(rurl, durl, file_path):
    data_addresses = getPartitionLocations(rurl, file_path)
    file_content = []
    for value in data_addresses.values():
        file_url = durl + value
        file_content.extend(requests.get(file_url).json())
    return file_content


if len(sys.argv) < 3:
    raise ValueError('Need valid data path!')
instruction = sys.argv[1]
data_path = sys.argv[2]
if instruction == 'put':
    file_src = sys.argv[3]
    k = int(sys.argv[4])
url = 'https://project-dc1b5-default-rtdb.firebaseio.com/.json'
init()
rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
if instruction == 'mkdir':
    make_dir(rurl, data_path)
elif instruction == 'ls':
    ls_all_files(rurl, data_path)
elif instruction == 'cat':
    print(cat_file(rurl, durl, data_path))
elif instruction == 'rm':
    rm_file(rurl, durl, data_path)
    # data folder need at least 1 attribute or it will be delete,
    #  if data we want to delete already deleted, no error
elif instruction == 'put':
    upload(rurl, durl, file_src, data_path, k)

