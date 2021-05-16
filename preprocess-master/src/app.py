import os
import time
import queue
import logging
from flask_cors import CORS
from flask import Flask, request, jsonify
import threading

from sshtunnel import SSHTunnelForwarder

from translate_work import TranslateWork
from taggingclient_work import TaggingClientWork

import pymysql

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')

# example work function for dealing with data
def _work(start=0, end=1000, done_recall=None, mysql_info=None):
    logging.info('work start')
    for i in range(start, end):
        time.sleep(1)
        if done_recall:
            done_recall(i + 1, end - i - 1)
    done_recall(i+1, end)


app = Flask(__name__)

MYSQLINFO = {'host': '',
             'user': '',
             'password': '',
             'database': '',
             'port': ''}

SSHINFO = None

PROXYINFO = {}

MYSQLINFOLOCK = threading.Lock()

TASKQUEUE = queue.Queue()

TASKLIST = []

STATE = 'idle'

PROCESSING = []

PROCESSINGLOCK = threading.Lock()
TASKLISTLOCK = threading.Lock()


def done_recall(done, remain):
    global PROCESSING

    p = PROCESSING[-1]

    update_time = time.time()
    counts = done - p['done']

    with PROCESSINGLOCK:
        p['done'] = done
        p['remain'] = remain
        if counts != 0:
            p['remain_time'] = (update_time - float(p['update_time'])) / counts * remain
        p['update_time'] = str(update_time)


def work_thread():
    global STATE
    global PROCESSING

    def get_work(task_name='', **task):
        _task = task.copy()
        if task_name == 'translate':
            work = TranslateWork(**_task, mysql_info=MYSQLINFO,
                                 done_recall=done_recall, ssh_info=SSHINFO)
        elif task_name == 'tagging':
            print('tagging')
            work = TaggingClientWork(**_task, mysql_info=MYSQLINFO,
                                     done_recall=done_recall, ssh_info=SSHINFO)
        return work

    while True:
        STATE = 'idle'
        task = TASKQUEUE.get()
        logging.info(f'star {task}')

        STATE = 'working'
        with PROCESSINGLOCK:
            PROCESSING.append({'start': str(time.time()),
                               'update_time': str(time.time()),
                               'done': 0,
                               'remain': -1,
                               'remain_time': -1})
        with TASKLISTLOCK:
            taskinfo = TASKLIST[task['id']]
            taskinfo['status'] = 'processing'
            taskinfo['processing'] = PROCESSING[-1]

        with MYSQLINFOLOCK:
            logging.info(f'working start with {task}')
            logging.info(f'working start with {MYSQLINFO}')
            try:
                work = get_work(**task)
                work.run()
            except Exception as e:
                logging.error(e)
                logging.error(f'argument {task} mysql_info={MYSQLINFO}')
        logging.info(f'{task} finished')
        PROCESSING = PROCESSING[:-1]

        with TASKLISTLOCK:
            taskinfo = TASKLIST[task['id']]
            taskinfo['status'] = 'done'
            taskinfo['processing'] = {}

"""
@api {get} progressing Current task progressing
@apiName Current task progressing

@apiSuccess {String}   start 任务开始时间
@apiSuccess {String}   update_time 任务更新时间
@apiSuccess {Number}   done 完成的数据量
@apiSuccess {Number}   remain 剩余的数据量
@apiSuccess {Number}   remain_time 剩余时间

@apiSuccessExample {json} Success-Response:
HTTP/1.1 200 OK
{
"start": "1615259662.600217",
"update_time": "1615259667.650217",
"done": 100,
"remain": 76447,
"remain_time": 103.4332422
}
@apiErrorExample {json} Error-Response:
HTTP/1.1 200 OK
{
"start": "1615259662.600217",
"update_time
": "1615259662.600217",
"done": 0,
"remain": -1,
"remain_time": -1
}
"""
@app.route('/progressing', methods=['GET'])
def get_progress():
    with PROCESSINGLOCK:
        if len(PROCESSING) > 0:
            logging.info(PROCESSING[-1])
            return jsonify(PROCESSING[-1])
        else:
            logging.info('not have processing')
            return jsonify({'error': 'no progress'})


"""
@api {get} tasklist return task list
@apiName return task list
@apiSuccessExample {json} Success-Response:
HTTP/1.1 200 OK
[
    {
        "id":0,
        "table":"ms_paper_ghl_20201026",
        "task_name":"tagging",
        "status":"done",
        "processing":{

        }
    },
    {
        "id":1,
        "table":"ms_paper_ghl_20200917",
        "task_name":"tagging",
        "status":"processing",
        "processing":{
            "start":"1615468357.00511",
            "update_time":"1615468378.933632",
            "done":1664,
            "remain":104986,
            "remain_time":1083.309479702264
        }
    },
    {
        "id":2,
        "table":"ms_paper_ghl_20201019",
        "task_name":"tagging",
        "status":"waiting",
        "processing":{

        }
    }
]
"""
@app.route('/tasklist', methods=['GET'])
def get_tasklist():
    logging.info(TASKLIST)
    with TASKLISTLOCK:
        return jsonify(TASKLIST)


"""
@api {post} pushtask push tack
@apiName Push task
@apiParam {String} table mysql table name
@apiParam {String} task_name task name 'tagging' or 'translate'
@apiParam {Boolean} test DEBUG时候使用 optional
@apiParam {String} t_id id column for 'translate'
@apiParam {String} t_c 翻译时的中文列 for 'translate'
@apiParam {String} t_e 翻译时对应的英文列 for 'translate'
@apiParam {Boolean} translated  打标签的表是否源语言为英语 for 'tagging’


@apiParamExample {json} translate:
{
"table": "ms_paper_ghl_20201026",
"t_id": "id",
"t_c": "title",
"t_e": "title_E",
"test": False,
"task": "translate"
}

@apiParamExample {json} tagging:
{
"table": "ms_paper_ghl_20201026",
"task": "tagging",
"test": False,
"translated": False
}

@apiSuccessExample Success-Response:
HTTP/1.1 200 OK
[
    {
        "id":0,
        "table":"ms_paper_ghl_20201026",
        "task_name":"tagging",
        "status":"done",
        "processing":{

        }
    },
    {
        "id":1,
        "table":"ms_paper_ghl_20200917",
        "task_name":"tagging",
        "status":"processing",
        "processing":{
            "start":"1615468357.00511",
            "update_time":"1615468378.933632",
            "done":1664,
            "remain":104986,
            "remain_time":1083.309479702264
        }
    },
    {
        "id":2,
        "table":"ms_paper_ghl_20201019",
        "task_name":"tagging",
        "status":"waiting",
        "processing":{

        }
    }
]
"""
@app.route('/pushtask', methods=['POST'])
def create_store():
    task = request.json
    logging.info(f'pushtask {task}')
    task['id'] = len(TASKLIST)

    TASKQUEUE.put(task)
    with TASKLISTLOCK:
        TASKLIST.append({
            'id': task['id'],
            'table': task['table'],
            'task_name': task['task_name'],
            'subtime': str(time.time()),
            'status': 'waiting',
            'processing': {},
        })
        return jsonify(TASKLIST)


"""
@api {post} configmyssh Config ssh config
@apiName Config ssh config
@apiParam {String} ssh_address_or_host host ip:port '47.92.240.36:22'
@apiParam {String} ssh_username username 'jt'
@apiParam {String} ssh_password password
@apiParam {String} remote_bind_address mysql address ip:port

@apiParamExample {json} Request-Example:
{
"ssh_address_or_host":  "47.92.240.36:22",
"ssh_username": "SSH_USERNAME",
"ssh_password": "xxxxxxx",
"remote_bind_address":  "192.168.0.84:3306"
}

@apiSuccessExample Success-Response:
HTTP/1.1 200 OK
{
"connect": true,
"time": "1615259662.600217"
}
"""
@app.route('/configssh', methods=['POST'])
def config_ssh():
    global MYSQLINFO
    global SSHINFO
    ssh_info = request.json
    #logging.info(f'configmysql {ssh_info}')

    connect = True
    try:
        ssh_info['ssh_address_or_host'] = ssh_info['ssh_address_or_host'].split(':')
        ssh_info['ssh_address_or_host'][1] = int(ssh_info['ssh_address_or_host'][1])
        ssh_info['ssh_address_or_host'] = tuple(ssh_info['ssh_address_or_host'])

        ssh_info['remote_bind_address'] = ssh_info['remote_bind_address'].split(':')
        ssh_info['remote_bind_address'][1] = int(ssh_info['remote_bind_address'][1])
        ssh_info['remote_bind_address'] = tuple(ssh_info['remote_bind_address'])

        server = SSHTunnelForwarder(**ssh_info)
        server.start()

        mysql_info = MYSQLINFO.copy()
        print(mysql_info)
        mysql_info['host'] = '127.0.0.1'
        mysql_info['port'] = server.local_bind_port
        conn = pymysql.connect(**mysql_info, connect_timeout=3)
        with MYSQLINFOLOCK:
            SSHINFO = ssh_info
            MYSQLINFO = mysql_info
        # stamp
        conn.close()
        server.close()
        logging.info(f'configssh success')
    except pymysql.OperationalError as e:
        logging.info(f'configssh error {e}')
        SSHINFO = None
        connect = False
    return jsonify({'connect': connect,
                    'time': str(time.time())})

"""
@api {post} configmysql Config sql config
@apiName Config sql config
@apiParam {String} host
@apiParam {String} user
@apiParam {String} password
@apiParam {String} database
@apiParam {Number} port

@apiParamExample {json} Request-Example:
{
"host": "192.168.0.84",
"user": "root",
"password": "root",
"database": "dump",
"port": "3306"
}

@apiSuccessExample Success-Response:
HTTP/1.1 200 OK
{
"connect": true,
"time": "1615259662.600217"
}
"""
@app.route('/configmysql', methods=['POST'])
def config_mysql():
    global MYSQLINFO
    mysql_info = request.json
    logging.info(f'configmysql {mysql_info}')

    connect = True
    try:
        _ = pymysql.connect(**mysql_info, connect_timeout=3)
    except Exception as e:
        logging.error(e)
        connect = False

    # setting even false
    with MYSQLINFOLOCK:
        MYSQLINFO = mysql_info
    return jsonify({'connect': connect,
                    'time': str(time.time())})

@app.route('/config', methods=['GET'])
def getconfig():
    global MYSQLINFO
    global SSHINFO

    mysql_info = MYSQLINFO.copy()
    ssh_info = SSHINFO.copy()

    connect = True
    try:
        server = SSHTunnelForwarder(**ssh_info)
        server.start()

        mysql_info['port'] = server.local_bind_port
        conn = pymysql.connect(**mysql_info, connect_timeout=3)
        # stamp
        conn.close()
        server.close()
        logging.info(f'configssh success')
    except pymysql.OperationalError as e:
        logging.info(f'configssh error {e}')
        connect = False

    del mysql_info['password']
    del ssh_info['ssh_password']

    for k in mysql_info:
        mysql_info[k] = str(mysql_info[k])

    for k in ssh_info:
        if type(ssh_info[k]) is tuple:
            ssh_info[k] = ':'.join([str(i) for i in ssh_info[k]])

    return jsonify({'mysql': mysql_info,
                    'ssh': ssh_info,
                    'connect': connect})


if __name__ == '__main__':
    work_threading = threading.Thread(target=work_thread)
    work_threading.start()

    try:
        app.run(host='0.0.0.0',
                port=int(os.environ['PORT']),
                debug=True)
    except Exception as e:
        logging.error(e)
