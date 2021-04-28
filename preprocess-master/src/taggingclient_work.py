import pickle
import queue
import threading
import hashlib
import requests
import time
import json

from work import Work


class TaggingClientWork(Work):
    def __init__(self, translated=False, server_ip='140.143.5.134:27777', **task):
        Work.__init__(self, **task)

        self.server_ip = server_ip

        with open('./data/field.pkl', 'rb') as f:
            self.field_dict = pickle.load(f)

        self.sql_get_total = f'select count(*) from {self.table} where field_l1 is NULL'
        if translated:
            self.sql_get_data = f'select id, title_E, keyword_E, venue_E from {self.table} '\
                f'where field_l1 is NULL limit 10000 offset {{}}'
        else:
            self.sql_get_data = f'select id, title, keyword, venue from {self.table} '\
                f'where field_l1 is NULL limit 10000 offset {{}}'
        self.sql_update_data = f"update {self.table} set field_l1=%s, field_l2=%s, field_l1_id=%s, field_l2_id=%s, finished=1 where id='{{}}'"

        self.threads = []
        for i in range(1):
            self.threads.append(threading.Thread(target=self.tagging))
        
        self.threads.append(threading.Thread(target=self.updatedata))

        for i in range(1):
            self.threads.append(threading.Thread(target=self.getdata))

    def format_in_data(self, r):
        return {'id': r[0], 'title': r[1],
                'keywords': r[2], 'venue': r[3],
                'abstract': ''}

    def format_update_data(self, k):
        ffid = self.field_dict[k['first'].lower()]
        ff = self.field_dict[ffid]
        sf = k['second']
        sfid = [self.field_dict[f.lower()] for f in sf]
        sf = [self.field_dict[d] for d in sfid]
        return (ff, json.dumps(sf), json.dumps(ffid), json.dumps(sfid))

    def tagging(self):
        base_key = 'buaa_label_2019'
    
        def get_api_key(base_key):
            base_key += ("-" + time.strftime("%F-%H"))
            md5 = hashlib.md5()
            base_key = base_key.encode('utf-8')
            md5.update(base_key)
            return md5.hexdigest()
    
        while not self.stop:
            try:
                k = self.intq.get(timeout=2)
            except queue.Empty:
                continue
            response = requests.post(f'http://{self.server_ip}/sci_label',
                                     data={'api_key': get_api_key(base_key),
                                           'paper_list': json.dumps(k)})
    
            out = json.loads(response.text)
    
            if out['msg'] == 'success':
                self.outq.put(out['data'])
            else:
                self.intq.put(k)

if __name__ == '__main__':
    import getpass
    SSHINFO = {
        'ssh_address_or_host': ('47.92.240.36', 22),  # 指定ssh登录的跳转机的address
        'ssh_username': 'jt',  # 跳转机的用户
        'ssh_password': getpass.getpass('passwd'),
        'remote_bind_address': ('192.168.0.84', 3306)
    }

    MYSQLINFO = {'host': '127.0.0.1',# '192.168.0.84',
                 'user': 'root',
                 'password': 'root',
                 'database': 'dump',
                 'port': 3306}
    task = {"table": "ms_paper_ghl_20201026", "translated": False}

    def done_recall(x, y):
        pass

    work = TaggingClientWork(**task, mysql_info=MYSQLINFO,
                             done_recall=done_recall, ssh_info=SSHINFO)
    work.run()
