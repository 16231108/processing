import json
import time
import threading
import queue
import pymysql
import logging
from sshtunnel import SSHTunnelForwarder


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')

_SETLOCK = threading.Lock()
_CURRENTID = set()
_TOTAL = 0
_REMAIN = 0


class Work:
    def __init__(self, table='', done_recall=None, mysql_info=None, ssh_info=None, test=False, **args):
        self.stop = False
        self.test = test
        self.table = table
        self.done_recall = done_recall
        self.mysql_info = mysql_info
        self.ssh_info = ssh_info
        if self.ssh_info is not None:
            self.server = SSHTunnelForwarder(**self.ssh_info)
            self.server.start()
            self.mysql_info['port'] = self.server.local_bind_port

        self.intq = queue.Queue(1000)
        self.outq = queue.Queue(1000)

    def run(self):
        self.set_total()
        if self.test:
            logging.info(f'get total {_TOTAL} {_REMAIN}')

        for t in self.threads:
            t.start()

        for t in self.threads:
            t.join()

    def set_total(self):
        global _TOTAL, _REMAIN

        conn = pymysql.connect(**self.mysql_info)
        cursor = conn.cursor()

        cursor.execute(self.sql_get_total)
        total = cursor.fetchone()[0]
        conn.close()

        _REMAIN = _TOTAL = total
        if _TOTAL == 0:
            self.stop = True
        return total

    def getdata(self):
        offset = 0

        conn = pymysql.connect(**self.mysql_info)
        k = []

        while not self.stop:

            cursor = conn.cursor()
            sqlcmd = self.sql_get_data.format(offset)
            try:
                cursor.execute(sqlcmd)
            except pymysql.err.OperationalError as e:
                print(e)
                conn.close()
                conn = pymysql.connect(**self.mysql_info)
                continue

            need_increase_offset = False
            for r in cursor:
                if r[0] in _CURRENTID:
                    need_increase_offset = True
                    continue

                with _SETLOCK:
                    _CURRENTID.add(r[0])

                k.append(self.format_in_data(r))

                if len(k) == 128:
                    self.intq.put(k)
                    k = []

            if need_increase_offset:
                offset += 10000
            else:
                offset -= 5000
                if offset < 0:
                    offset = 0

            while self.intq.qsize() > 100 and not self.stop:
                time.sleep(1)

            if len(k):
                self.intq.put(k)
                k = []
        logging.info('getdata finished')

    def updatedata(self):
        global _REMAIN
        conn = pymysql.connect(**self.mysql_info)
        cursor = conn.cursor()

        self.done_recall(0, _TOTAL)
        while not self.stop:
            k = self.outq.get()
            for i in list(k.keys()):
                sql = self.sql_update_data.format(i)

                with _SETLOCK:
                    _CURRENTID.remove(i)

                cursor.execute(sql, self.format_update_data(k[i]))

            conn.commit()
            _REMAIN -= min(len(k), _REMAIN)
            self.done_recall(_TOTAL - _REMAIN, _REMAIN)
            if _REMAIN <= 0 or\
               (self.test and _TOTAL - _REMAIN > 300):
                logging.info('updatedata want stop')
                self.stop = True
                break
        logging.info('updatedata finished')
