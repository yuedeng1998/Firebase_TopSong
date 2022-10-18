import sys
from urllib import response
import requests
import csv
import json

# url = 'https://project-dc1b5-default-rtdb.firebaseio.com/root/user/heleon.json'
# out = {'hellow': {'txt': {'p1': '/3.json', 'p2': '/0.json'}}, 'is_dict': True}
url = 'https://project-dc1b5-default-rtdb.firebaseio.com/data/.json'
out = {'d0': ' shiudnwbe', 'd1': 'fewibfwie',
       'd2': 'dwbefhweifwe', 'd9': 'webfiweb'}
response = requests.get(url).json()
out = json.dumps(out)
print(out)
response = requests.put(url, out)
print(response.text)


# file_path = '/user/heleon/hellow.txt'
# file_path = file_path.replace('.', '/')
# print(file_path)
# response = requests.get(
#     'https://project-dc1b5-default-rtdb.firebaseio.com/root'+file_path + '.json').json()
# if not response:
#     raise ValueError('No Such File!')
# data_addresses = []
# for i in range(len(response.items())):
#     data_addresses.append(response['p'+str(i+1)])
# print(data_addresses)

# data_url = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
# file_content = ""
# for address in data_addresses:
#     file_url = data_url + address
#     file_content += requests.get(file_url).json()
# print(file_content)


def get_data_address(file_path):
    file_path = file_path.replace('.', '/')
    response = requests.get(
        'https://project-dc1b5-default-rtdb.firebaseio.com/root'+file_path + '.json').json()
    if not response:
        raise ValueError('No Such File!')
    data_addresses = []
    for i in range(len(response.items())):
        data_addresses.append(response['p'+str(i+1)])
    return data_addresses


def read_file(data_addresses):
    data_url = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
    file_content = ""
    for address in data_addresses:
        file_url = data_url + address
        file_content += requests.get(file_url).json()
    return file_content


def cat_file(file_path):
    data_addresses = get_data_address(file_path)
    return read_file(data_addresses)
