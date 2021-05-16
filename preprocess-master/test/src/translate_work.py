import re
import json
import threading
import requests
import random

import random
from work import Work

def getheaders():
    user_agent_list = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]
    UserAgent = random.choice(user_agent_list)

    return UserAgent

class Token:

    SALT_1 = "+-a^+6"
    SALT_2 = "+-3^+b+-f"

    def __init__(self):
        self.token_key = '406644.3293161072'

    def getTk(self, text, seed=None):
        if seed is None:
            seed = self._get_token_key()

        [first_seed, second_seed] = seed.split(".")

        try:
            d = bytearray(text.encode('UTF-8'))
        except UnicodeDecodeError:
            # This will probably only occur when d is actually a str containing UTF-8 chars, which means we don't need
            # to encode.
            d = bytearray(text)

        a = int(first_seed)
        for value in d:
            a += value
            a = self._work_token(a, self.SALT_1)
        a = self._work_token(a, self.SALT_2)
        a ^= int(second_seed)
        if 0 > a:
            a = (a & 2147483647) + 2147483648
        a %= 1E6
        a = int(a)
        return str(a) + "." + str(a ^ int(first_seed))

    def _get_token_key(self):
        return self.token_key

    """ Functions used by the token calculation algorithm """
    def _rshift(self, val, n):
        return val >> n if val >= 0 else (val + 0x100000000) >> n

    def _work_token(self, a, seed):
        for i in range(0, len(seed) - 2, 3):
            char = seed[i + 2]
            d = ord(char[0]) - 87 if char >= "a" else int(char)
            d = self._rshift(a, d) if seed[i + 1] == "+" else a << d
            a = a + d & 4294967295 if seed[i] == "+" else a ^ d
        return a
        
def translate(tk, content):
    if len(content) > 1200:
        return False

    headers = {
        'user-agent': getheaders(),
        'accept': "*/*",
        'cache-control': "no-cache",
        'accept-encoding': "gzip, deflate, br",
        'authority': "translate.google.cn",
        'method': "GET",
        'accept-language': "zh-CN,zh;q=0.9,en;q=0.8",
        'pragma': "no-cache",
        'referer': "https://translate.google.cn/",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "same-origin",
        'x-client-data': "CK21yQEIl7bJAQiitskBCMG2yQEIqZ3KAQjyn8oBCLeqygEIy67KAQi8sMoBCPe0ygEImbXKAQjstcoBGKukygEY9rHKAQ==",
        'Connection': "keep-alive",
        'Host': "translate.google.cn",
    }

    proxyHost = "http-dyn.abuyun.com"
    proxyPort = "9020"

    # 代理隧道验证信息
    proxyUser = "HX59410BP8D5878D"
    proxyPass = "6A2B9E88C34FD95A"

    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": proxyHost,
        "port": proxyPort,
        "user": proxyUser,
        "pass": proxyPass,
    }

    proxies = {
        'http': proxyMeta,
        'https': proxyMeta,
    }


    querystring = {"client": "webapp", "sl": "zh-CN", "tl": "en", "hl": "zh-CN",
                   "dt": ["at", "bd", "ex", "ld", "md", "qca", "rw", "rm", "ss", "t"], "clearbtn": "1", "otf": "1",
                   "pc": "1", "ssel": "3", "tsel": "3", "kc": "2", "tk": tk,
                   "q": content}

    url = "https://translate.google.cn/translate_a/single"

    response = requests.request("GET", url=url, params=querystring,
                                headers=headers, # proxies=proxies,
                                timeout=6)

    try:
        result = response.json()
    except json.decoder.JSONDecodeError:
        return False

    if result[0] is None:
        return False

    content_E = ""
    for i, value in enumerate(result[0]):
        if i < (len(result[0])-1):
            block = "\n".join(re.split('\n', value[0]))
            content_E = content_E + block
    return content_E

def run_tra(content):
    # content = content.replace(';', ' ; ')
    js = Token()
    if len(content) > 1100:
        contents = ['']
        count = 0
        for i in content.split('\n'):
            if len(i) > 1100:
                return False
            contents[count] += i + '\n'
            if len(contents[count]) > 1100:
                contents.append('')
                count += 1
        del content

        content_E = []
        for c in contents:
            tk = js.getTk(c)
            ce = translate(tk, c)
            if not ce:
                return False
            content_E.append(ce)

        content_E = '\n'.join(content_E)
        content_E = content_E.replace('\n\n', '\n')
        if content_E[-1] == '\n':
            content_E = content_E[:-1]
    else:
        tk = js.getTk(content)
        content_E = translate(tk, content)

    # content_E = content_E.replace('；', ';')
    return content_E

class TranslateWork(Work):
    def __init__(self, t_id='id', t_c='title', t_e='title_E', **task):
        Work.__init__(self, **task)

        self.t_id = t_id
        self.t_c = t_c
        self.t_e = t_e

        self.sql_get_total = f'select count(*) from {self.table}'\
            f' where {self.t_e} = "" OR {self.t_e} IS NULL'
        print(self.sql_get_total)

        self.sql_get_data = f'select {self.t_id}, {self.t_c} from {self.table}'\
            f' where {self.t_e} = "" OR {self.t_e} IS NULL LIMIT 10000 offset {{}}'

        self.sql_update_data = f"update {self.table} set {self.t_e}=%s"\
            f" where {self.t_id}='{{}}'"

        self.threads = []
        for _ in range(1):
            self.threads.append(threading.Thread(target=self.translate))
        
        self.threads.append(threading.Thread(target=self.updatedata))

        for _ in range(1):
            self.threads.append(threading.Thread(target=self.getdata))

    def format_in_data(self, r):
        return {self.t_id: r[0], self.t_c: r[1]}

    def format_update_data(self, k):
        return k

    def translate(self):
        while not self.stop:
            try:
                in_list = self.intq.get(timeout=2)
            except queue.Empty:
                continue
            error_count = 0
            failure_count = 0

            in_dict = {}
            for i in in_list:
                in_dict[i[self.t_id]] = i[self.t_c]

            while len(in_dict) != 0 and failure_count < 5 and error_count < 5:
                a_keys = []
                for i in in_dict:
                    a_keys.append(i)
                    #print(len('\n'.join([in_dict[i] for i in a_keys])))
                    if len('\n'.join([in_dict[i] for i in a_keys])) > 1100:
                        a_keys.pop()
                        break
            
                content = '\n'.join([in_dict[i] for i in a_keys])
                #print(a_keys)
                #print(content)
                #print('translate')

                try:
                    content_E = run_tra(content)
                except:
                    error_count += 1
                    continue  # retry

                if not content_E or len(content_E.split('\n')) != len(a_keys):
                    # fail
                    failure_count += 1
                    continue

                out = {}
                for k, c in zip(a_keys, content_E.split('\n')):
                    out[k] = c

                self.outq.put(out)

                for k in a_keys:
                    del in_dict[k]


if __name__ == '__main__':
    import getpass
    SSHINFO = {
        'ssh_address_or_host': ('47.92.240.36', 22),  # 指定ssh登录的跳转机的address
        'ssh_username': 'jt',  # 跳转机的用户
        'ssh_password': 'vdaubCp7yaSreqlT',#getpass.getpass('passwd'),
        'remote_bind_address': ('192.168.0.84', 3306)
    }

    MYSQLINFO = {'host': '127.0.0.1',
                 'user': 'root',
                 'password': 'root',
                 'database': 'dump',
                 'port': 3306}
    task = {"table": "ms_paper_ghl_20201026", "t_id": "id", "t_c": "title", "t_e": "title_E"}

    def done_recall(x, y):
        pass

    work = TranslateWork(**task, mysql_info=MYSQLINFO,
                         done_recall=done_recall, ssh_info=SSHINFO)
    work.run()
