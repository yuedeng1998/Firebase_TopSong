from ast import Delete
import sys
from urllib import response
import requests
import csv
import json


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

# def rm_file(file_path):


file_path = '/user/heleon/hellow.txt'
data_addresses = getPartitionLocations(file_path)
rmPartition(data_addresses)
file_path = file_path.replace('.', '/')
response = requests.delete(
    'https://project-dc1b5-default-rtdb.firebaseio.com/root' + file_path + '.json')
