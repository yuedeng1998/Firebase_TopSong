from flask import Flask
from command import *

app = Flask(__name__)


@app.route('/put/<data_path>/<k>', methods=['GET'])
def put(data_path, k):
    try:
        init()
        rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
        durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
        upload(rurl, durl, file_src, data_path, k)
        return "command success"
    except:
        return "command fail"

@app.route('/mkdir/<data_path>', methods=['GET'])
def mkr(data_path):
    try:
        init()
        rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
        make_dir(rurl, data_path)
        return "command success"
    except :
        return "command fail"


@app.route('/ls/<data_path>', methods=['GET'])
def ls(data_path):
    init()
    rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
    ls_all_files(rurl, data_path)
    return 1

@app.route('/cat/<data_path>', methods=['GET'])
def cat(data_path):
    try:
        init()
        rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
        durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
        res = cat_file(rurl, durl, data_path)
        return res
    except:
        return "command fail"

@app.route('/rm/<data_path>', methods=['GET'])
def rm(data_path):
    try:
        init()
        rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
        durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
        rm_file(rurl, durl, data_path)
        return "command success"
    except:
        return "command fail"
