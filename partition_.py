from urllib import response
import requests
import csv
import json

#Return True if new address is Invalid
def check_address(new_address):
    durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
    response = requests.get(durl+ '.json').json()
    for key in response.keys():
        if new_address == key:
            return True
    return False

check_address('d500')

import random
def generate_address():
    n = 0 # inital value, obviously has null bytes
    while n & 0xff == 0 or n & 0xff00 == 0:
        n = random.randint(0x7fffffff0000, 0x7fffffffffff)
    return n

generate_address()

#改了一下 返回dictionary {p1:'/d10.json'}
def getPartitionLocations(file_path):
    file_path = file_path.replace('.', '/')
    response = requests.get(
        'https://project-dc1b5-default-rtdb.firebaseio.com/root'+file_path + '.json').json()
    if not response:
        raise ValueError('No Such File!')
    data_addresses = {}
    for i in range(len(response)):
        data_addresses['p'+str(i+1)] = response['p'+str(i+1)]
    return data_addresses

#改了一下 只read某一个partition
#原来的readPartition function移到cat_file
#cat_file返回一整个文件as a dictionary
def readPartition(file_path, parition_number):
    data_addresses = getPartitionLocations(file_path)
    address = data_addresses.get(parition_number)
    file_url = 'https://project-dc1b5-default-rtdb.firebaseio.com/data' + address
    return requests.get(file_url).json()

file_path = '/tiktok/TikTok_songs_2022_edit.csv'
readPartition(file_path, 'p1')
l = getPartitionLocations(file_path)
print(l)

