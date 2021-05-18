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

STATE = 'idle'

PROCESSING = []

PROCESSINGLOCK = threading.Lock()


def done_recall(done, remain):
    global PROCESSING

    p = PROCESSING[-1]

    update_time = time.time()
    counts = done - p['done']

    with PROCESSINGLOCK:
        p['done'] = done
        p['remain'] = remain
        if counts != 0:
            p['remain_time'] = (update_time - p['update_time']) / counts * remain
        p['update_time'] = update_time


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
        PROCESSING.append({'start': time.time(),
                           'update_time': time.time(),
                           'done': 0,
                           'remain': -1,
                           'remain_time': -1})
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

@app.route('/progressing', methods=['GET'])
def get_progress():
    with PROCESSINGLOCK:
        if len(PROCESSING) > 0:
            logging.info(PROCESSING[-1])
            return jsonify(PROCESSING[-1])
        else:
            logging.info('not have processing')
            return jsonify({'error': 'no progress'})


@app.route('/pushtask', methods=['POST'])
def create_store():
    task = request.json
    logging.info(f'pushtask {task}')
    TASKQUEUE.put(task)

    return jsonify({'time': str(time.time())})


@app.route('/configssh', methods=['POST'])
def config_ssh():
    global MYSQLINFO
    global SSHINFO
    ssh_info = request.json
    #logging.info(f'configmysql {ssh_info}')

    connect = True
    try:
        import pymysql
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

@app.route('/configmysql', methods=['POST'])
def config_mysql():
    global MYSQLINFO
    mysql_info = request.json
    logging.info(f'configmysql {mysql_info}')

    import pymysql
    connect = True
    try:
        _ = pymysql.connect(**mysql_info, connect_timeout=3)
    except pymysql.OperationalError:
        connect = False

    # setting even false
    with MYSQLINFOLOCK:
        MYSQLINFO = mysql_info
    return jsonify({'connect': connect,
                    'time': str(time.time())})

if __name__ == '__main__':
    work_threading = threading.Thread(target=work_thread)
    work_threading.start()

    try:
        app.run(host='0.0.0.0',
                port=int(os.environ['PORT']),
                debug=True)
    except Exception as e:
        logging.error(e)
