import os
import time
import logging
import threading
from src import app

log = logging.getLogger(__name__)
os.environ['SSH_USERNAME']='jt'
os.environ['SSH_PASSWORD']='vdaubCp7yaSreqlT'
def test_tagging(client):
    res = client.post('/configmysql',json=dict(
        host='192.168.0.84',
        user='root',
        password='root',
        database='dump',
        port=3306
    ), follow_redirects=True)
    log.info('mysql '+str(res.get_json()))

    res = client.post('/configssh',json=dict(
        ssh_address_or_host= '47.92.240.36:22',
        ssh_username=os.environ['SSH_USERNAME'],
        ssh_password=os.environ['SSH_PASSWORD'],
        remote_bind_address= '192.168.0.84:3306'
    ), follow_redirects=True)
    log.info('ssh '+str(res.get_json()))

    res = client.post('/pushtask',json=dict(
        table="ms_paper_ghl_20201026",
        translated=False,
        task_name="tagging",
        test=True
    ), follow_redirects=True)
    log.info('pushtask '+str(res.get_json()))

    while True:
        time.sleep(3)
        res = client.get('/progressing').get_json()
        if 'done' not in res:
            break

def test_translate(client):
    res = client.post('/configmysql',json=dict(
        host='192.168.0.84',
        user='root',
        password='root',
        database='dump',
        port=3306
    ), follow_redirects=True)
    log.info('mysql '+str(res.get_json()))

    res = client.post('/configssh',json=dict(
        ssh_address_or_host= '47.92.240.36:22',
        ssh_username=os.environ['SSH_USERNAME'],
        ssh_password=os.environ['SSH_PASSWORD'],
        remote_bind_address= '192.168.0.84:3306'
    ), follow_redirects=True)
    log.info('ssh '+str(res.get_json()))

    res = client.post('/pushtask',json=dict(
        table="journal_article_20201028",
        t_id="id",
        t_c="title",
        t_e="title_E",
        task_name="translate"
    ), follow_redirects=True)
    log.info('pushtask '+str(res.get_json()))

    while True:
        time.sleep(3)
        res = client.get('/progressing').get_json()
        if 'done' not in res:
            break
