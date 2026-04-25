import psycopg2
import cx_Oracle
import pymysql
import pymssql
import pyodbc
import os
import re
import time
import sqlparse
import copy
import configparser
import opg
import chardet
import ibm_db
import sys
from ftplib import FTP
import json
import ibm_db_dbi
from pymysql import OperationalError as PyMySQLOperationalError
from opg import MysqlConn

config = configparser.RawConfigParser()
rule_cfg = configparser.RawConfigParser()
case_cfg = configparser.RawConfigParser()
config.read(r'resource/config.ini', encoding='utf-8')
rule_cfg.read(r'resource/rule_config.ini', encoding='utf-8')
case_cfg.read(r'resource/case_dir.ini', encoding='utf-8')
pg_case = r'D:\Info\case_resource\postgre\pg_basedml'
# mss_case=r'resource/resource101'
mysql_case = r'D:\Info\case_resource\mysql\test'
mss_case_test=r'D:\Info\case_resource\sqlserver\test'
mss_case_all = r'D:\Info\case_resource\sqlserver\all'
mss_old_case =r'D:\Info\case_resource\sqlserver_old_case\basic'
mss_case_ddl = r'D:\Info\case_resource\sqlserver\basic'
mss_case_obj = r'D:\英方\Sqlserver\用例整理DDL_250530\数据对象'
mss_case_debug = r'D:\Info\case_resource\sqlserver\basic1'
oracle_case = r'D:\Info\case_resource\oracle\test'
root_directory = r'D:\Info\case_resource'  # 用例包#############
db2_case = r'D:\Info\case_resource\db2\db2-sql'  # 表修复数据类型路径

dds_mapping = json.loads(config.get('400_type', 'type'))
ignore_casefile=['DDL']#忽略带关键字的case文件，常见关键字 DML/DDL/PK/NOPK/OBJ/

class db2_conn():
    def __init__(self, conn):
        self.conn = conn
        self.stmt = None

    def execute(self, tmp_sql):
        if not self.conn:
            print('no conn was given')
        else:
            # print(self.conn)
            sql = tmp_sql
            self.stmt = ibm_db.prepare(self.conn, sql)
            result = ibm_db.execute(self.stmt)
            return result

    def fetchall(self):
        a = ibm_db
        result_tmp = []
        row = a.fetch_tuple(self.stmt)
        while row:
            result_tmp.append(row)
            row = ibm_db.fetch_tuple(self.stmt)
        return result_tmp

    def commit(self):  # db2的conn没有commit，捏个假的
        pass

    def close(self):
        pass
class read_case():
    def __init__(self, incr, src, type, *args):
        self.mysql = ('mysql', 'tdsql')  # mysql源
        self.pg = ('postgre', 'lightdb', 'dws', 'tdsql_pg', 'opengauss', 'opengauss_gbk')
        self.oracle = ('oracle', 'ob_oracle', 'oboracle')
        self.sqlserver = ('sqlserver', 'ssms')
        self.db2 = ('db2')
        self.dm = ('dm')
        self.obj = ('delimiter $$', 'create procedure', ' create trigger', 'create function')
        self.addr = ''
        self.current_cursor = None
        self.mode = ''
        self.case_name = ''
        self.numer = 0
        self.templine = ''
        self.sqllist = []  # 最终大列表
        self.sqldict = {}
        self.sqldict['dml'] = ''  # 结构体
        self.sqldict['before'] = []
        self.sqldict['tgt'] = []
        self.sqldict['crt'] = []
        self.tempstring = []  # 存储before等临时ddl、dml等
        self.result = []
        self.tempdict = {}
        if type!=0:
            self.case_dir = args
        else:self.case_dir=''
        self.pre_crt=0
        self.case_file_list = []
        self.before = []
        self.mss_err = ('type')
        self.src = src
        self.type = type
        self.incr_tbname = incr
        # self.dml=('insert','update','delete')
    def ergodic(self, type):
        # self.case_dir=config.get('case','case_dir')+type#注释该行启用手动设置case路径
        if self.case_dir:
            self.case_dir = self.case_dir[0]
        else:
            front = type + '_case'
            self.case_dir = case_cfg.get('case_file', front)
        print(f'用例路径{self.case_dir}')
        for files in os.listdir(self.case_dir):
            if files.endswith('.sql') and  not(any(x in files for x in ignore_casefile)):
                self.case_file_list.append('%s/%s' % (self.case_dir, files))
        return self.case_file_list
    def analysis(self, addr, pre_crt):  # 返回最终sql用例列表 每个用例为一个字典
        self.addr = r'%s' % addr
        self.pre_crt = pre_crt
        print('进程号', os.getpid())
        with open(self.addr, 'rb') as f:
            raw_data = f.read()
            encoding = chardet.detect(raw_data)['encoding']
            print(encoding)
        with open(self.addr, mode='r', encoding=encoding) as f:
            while True:
                self.templine = f.readline()
                if (self.templine == ''):
                    break
                else:
                    self.templine = self.templine.replace('\n', '').replace('\t', ' ')
                # 个别链路不支持重命名，启用该条减少用例执行时间
                # if self.templine.startswith('case') and  ('重命名' or '修改列属性') not in self.templine.replace(' ',''):
                if re.sub(r'\s|\t', '', self.templine).startswith('case') and \
                        not any(word in self.templine.replace(' ', '') for word in
                                ('外键', '重命名','分区', '触发器', '删除计算列')):  # 过滤，不执行带关键词的case,'触发器','存储过程'
                    # if self.templine.startswith('case'):
                    self.sqldict['casename'] = self.templine
                    print(self.templine)
                    self.numer += 1
                    # print(self.sqldict)
                    while True:
                        if re.sub(r'\s|\t', '', self.templine.lower()).startswith('src_sql'):
                            pass
                        else:
                            self.templine = f.readline()

                        # print(self.sqldict)
                        if self.mode == 'tgt':
                            if self.incr_tbname == 1:
                                self.incr()
                            else:
                                pass
                            self.before.append(self.sqldict['before'])
                            pos = f.tell()
                            while True:
                                tmp = f.readline()
                                if re.sub(r'\s|\t', '', tmp).startswith('case') or tmp == '':
                                    f.seek(pos);break
                                else:
                                    self.sqldict['tgt'].append(tmp)
                            self.sqllist.append(self.sqldict)
                            self.sqldict = {}
                            self.sqldict['dml'] = ''
                            self.sqldict['crt'] = []
                            self.sqldict['before'] = []
                            self.sqldict['tgt'] = []
                            self.mode = ''
                            tmp_str = ''
                            break
                        elif re.sub(r'\s|\t', '', self.templine.lower()).startswith('before'):  # before
                            while True:
                                self.templine = f.readline()
                                if re.sub(r'\s|\t', '', self.templine.lower()).startswith('go') or re.sub(r'\s|\t', '',self.templine.lower()).startswith('src_sql'):
                                    break
                                else:
                                    self.sqldict['before'].append(self.templine.replace('\n', ' ').replace('\t', ' '))
                            # print(self.sqldict)
                            continue
                        elif re.sub(r'\s|\t', '', self.templine.lower()).startswith('src'):
                            self.templine = '';tmp_str=''
                            self.mode = 'src'
                            tmp = None
                            while True:
                                self.templine = f.readline()
                                if overflow != 1:
                                    pass
                                else:
                                    self.templine = re.sub('8000', '200', self.templine)
                                if re.sub(r'\s|\t', '', self.templine).startswith('tgt'):
                                    self.mode = 'tgt'
                                    break
                                elif self.templine.startswith('sleep') or re.sub(r'\s|\t', '',self.templine.lower()).startswith('go') or re.findall('sleep\(.*?\)', self.templine.lower()):
                                    continue
                                elif self.templine.startswith('drop table'):
                                    self.sqldict['before'].append(self.templine.replace('\n', '').replace('\t', ''))
                                elif (self.templine.lower().startswith('create') or self.templine.lower().startswith('delimiter')) and any(keyword.lower() in self.templine.lower() for keyword in self.obj):
                                    self.sqldict['dml'] += self.templine;f = self.object(self.templine, f)
                                elif (self.pre_crt == 1 and 'create table' in self.templine.lower()):  # pre_Crt 先统一建表再执行dml
                                    while True:
                                        if (';' or '^p') in self.templine:
                                            tmp_str += self.templine
                                            if overflow != 1:pass
                                            else:self.templine = re.sub('8000', '200', self.templine)
                                            self.sqldict['crt'].append(tmp_str)
                                            tmp_str = ''
                                            break
                                        else:
                                            tmp_str += self.templine
                                            self.templine = f.readline()
                                            if overflow != 1:pass
                                            else:self.templine = re.sub('8000', '200', self.templine)
                                elif self.templine.split('--', 1).__len__() > 1:
                                    if self.templine.split('--', 1)[1].replace('\n', '').endswith(';') and re.sub(
                                            r'\s|\t', '', self.templine.split('--', 1)[0]):
                                        self.sqldict['dml'] += self.templine
                                    else:
                                        self.sqldict['dml'] += self.templine.split('--', 1)[0]
                                elif r'/*' in self.templine:
                                    while (r'*/' not in self.templine):
                                        print(1)
                                        self.templine = f.readline()
                                else:
                                    if type in self.oracle:
                                        self.sqldict['dml'] += r'%s' % self.templine.replace('\t', ' ')
                                    else:
                                        self.sqldict['dml'] += r'%s' % self.templine.replace('\n', ' ').replace('\t',
                                                                                                                ' ')
                        elif re.sub(r'\s|\t', '', self.templine.lower()).startswith('sleep'):
                            continue
        # print(self.sqllist)
        return self.sqllist, self.numer

    def execute(self, conn,event):  # mssql
        '''执行sql,支持执行多条sql语句。'''
        if not event:pass

        new_list = copy.copy(self.sqllist)
        dds = 0  # 针对AS400 是否将create语句解析为dds并模拟在绿屏执行,pre_crt先统一建表再统一dml~ 0：关闭，1：开启
        T1 = T2 = T3 = T4 = k = j = 0
        if self.type != 'db2':
            self.current_cursor = conn.cursor()
        else:
            self.current_cursor = db2_conn(conn)
            conn = self.current_cursor
        print('正在执行用例文件：\t%s\t\t\tcase数：%d' % (self.addr, self.numer))
        if self._check_control(event, f'用例文件 {self.addr}'):
            return
        T1 = time.perf_counter()
        if self.pre_crt == 1:
            crtlist = []
            before_list = []
            for i in range(self.numer):
                if '^p' not in self.sqllist[i]['dml'] and self.sqllist[i]['crt']:
                    #crtlist+=re.sub('\n','',self.sqllist[i]['crt'][0]).split(';')
                    crtlist +=self.sqllist[i]['crt']

                elif '^p' in self.sqllist[i]['dml'] and self.sqllist[i]['crt']:
                    crtlist+=self.sqllist[i]['crt']
                else:
                    pass
                before_list+=self.sqllist[i]['before']
            for item in before_list+crtlist:
                try:
                    if bool(re.sub('\n','',item)):
                        self.current_cursor.execute(item)
                        conn.commit();print(item);time.sleep(crt_delay)
                        if self._check_control(event, '建表预处理'):
                            return
                    else:
                        continue
                except None or (pyodbc.ProgrammingError,pymssql._pymssql.OperationalError, psycopg2.Error, pymssql.Error, pymysql.Error,cx_Oracle.Error) as e:
                    print(e)
            print(f'crt全部执行完毕，等待{3+2*len(crtlist)}s开始dml');self.interruptible_sleep(event,3+2*len(crtlist))
        else:
            pass
        for i in range(self.numer):
            if self._check_control(event, f'case {self.sqllist[i]["casename"]}'):
                return
            print('%s' % self.sqllist[i]['casename'], '\t\t\t进度：%d/%d' % (i + 1, self.numer))
            T2 = time.perf_counter();new = old = ''
            if '^p' not in self.sqllist[i]['dml']:
                #sqllist1 = self.sqllist[i]['dml'].split(';')
                sqllist1 = sqlparse.split(self.sqllist[i]['dml'])
            else:
                sqllist1 = self.sqllist[i]['dml'].split('^p')
            sqllist1 = [s for s in sqllist1 if s.strip()]

            for b in self.sqllist[i]['before']:
                if b == '' or self.pre_crt==1:
                    break
                else:
                    try:
                        self.current_cursor.execute(b.replace(';', ''))
                        # time.sleep(1)
                        conn.commit()
                        time.sleep(1)
                        print( re.sub('\n', '', b))
                        if self._check_control(event, f'case {self.sqllist[i]["casename"]} 前置SQL'):
                            return
                    except None or (pymssql._pymssql.OperationalError, psycopg2.Error, pymssql.Error, pymysql.Error,
                                    cx_Oracle.Error) as e:
                        if self.src in ('mssql', 'sqlserver'):
                            for i in self.mss_err:
                                if i in b:
                                    print('执行%s失败' % b, e)
                                    conn.rollback()
                                    # self.current_cursor.execute("ROLLBACK TRANSACTION")
                                    break
                        elif self.src not in ('db2'):
                            conn.commit()
                            print('执行%s失败' % b, e)
                    except:
                        print('执行%s失败' % b, ibm_db.conn_errormsg())
            for caseline in sqllist1:
                try:
                    if new != '':
                        caseline = re.sub(old.lower(), new.lower(), caseline)

                    if dds != 0 and 'create table' in caseline.lower():
                        new, old, crt = self.as400_ana(caseline.lower())
                        new_list[i]['dml'] = crt
                        self.as400_pf(new)
                        # print(f'特殊语句，跳过等候3s ：：{caseline}');continue
                    if 'create full' in caseline.lower():
                        conn.autocommit(True)
                        self.current_cursor.execute(caseline)
                        conn.autocommit(False)
                    else:
                        self.current_cursor.execute(caseline)
                        conn.commit()
                    k += 1
                    if self._check_control(event, f'case {self.sqllist[i]["casename"]} SQL执行'):
                        return
                    if self.type == 'db2_as400' and 'create table' in caseline.lower():
                        print('as400建表多等5s')
                    elif 'create table' in caseline.lower():
                        print(re.sub('\n', '', caseline),"执行完毕\nsmss建表后暂停8s")
                        time.sleep(6)  # 建表等6秒再插入
                    elif 'rename' in caseline or re.search('select.*?into',caseline):
                        print('rename或select into，特殊语句或触发表修复，等候20s');time.sleep(20)
                    print(re.sub('\n', '', caseline))
                    time.sleep(dml_delay)

                except (
                pymssql._pymssql.OperationalError, pymssql._pymssql.ProgrammingError, pymssql._pymssql.IntegrityError,
                psycopg2.Error, pymysql.Error, cx_Oracle.Error) as err:
                    print('执行语句%s失败，跳过该语句' % caseline, err)
                    conn.rollback()
                    j += 1
                    pass

                    # except event as e:
                    #     raise Exception

                except:
                    print(f'执行{caseline}\n失败', ibm_db.conn_errormsg())
            T3 = time.perf_counter()
            print('执行完毕，该条用例耗时：\t\t%f毫秒\n' % ((T3 - T2) * 1000))

            time.sleep(0.6)
            # print('暂停15s')
        T4 = time.perf_counter()
        print('该用例文件%s耗时：\t%f毫秒,\t\t成功%d/失败%d/共计%d' % (self.addr, ((T4 - T1) * 1000), k, j, k + j));time.sleep(1)
        self.numer = 0
        self.sqllist = []
    def colseconn(self):
        if self.current_cursor:
            self.current_cursor.close()
            print('cursor close')
        else:
            print('cursor has been closed')
    def object(self, str, f):
        obj = [str]
        t = [0, 0]
        while True:
            self.templine = f.readline()
            print(self.templine)
            # if  re.search(r'\bend\b|delimiter', self.templine.lower(), re.IGNORECASE) and t[0]==t[1]:
            #     self.sqldict['dml']+= self.templine;break
            if re.search(r'\bbegin\b', self.templine.lower(), re.IGNORECASE):
                t[0] += 1
            elif re.search(r'\bend\b', self.templine.lower(), re.IGNORECASE):
                t[1] += 1
                if t[0] == t[1]:
                    self.sqldict['dml'] += self.templine
                    break
            else:
                pass
            self.sqldict['dml'] += self.templine
        return f
    def incr(self):  # 表名拼接case号
        j = 0
        for i in self.sqldict['before']:
            if 'drop table' in i:  # tbname=re.search(r'(?<=drop table\s)(.*?)$',self.sqldict['before'][j]).group(0)
                if 'purge' in i:
                    tbname = re.search(r'(?<=drop table\s)(.*?)(?=\spurge)', self.sqldict['before'][j]).group(0)
                else:
                    tbname = re.search(r'(?<=drop table\s)(.*?)$', self.sqldict['before'][j]).group(0)
                break
            j += 1
        str = ''
        tbname = re.sub(";|\s", '', tbname)
        for i in self.sqldict['casename'][5:]:
            if i.isdigit():
                str += i
            else:
                break
        new = tbname + str;j = 0
        self.sqldict['dml'] = re.sub('/\n', '^p\n', re.sub(tbname, new, self.sqldict['dml']))
        for i in self.sqldict['before']:
            self.sqldict['before'][j] = re.sub(tbname, new, i)
            j += 1
    def define(self, type, db):
        if type in self.oracle:
            if type == 'oracle':
                conn = opg.MysqlConn(type)
                cur = conn.open_oracle()
            elif 'ob' in type:
                conn = opg.MysqlConn('ob_oracle')
                cur = conn.open_obora()
        elif type in self.mysql:
            type = 'mysql'
            conn = opg.MysqlConn(type)
            cur = conn.open()
        elif type in self.pg:
            type = 'postgre'
            conn = opg.MysqlConn(type)
            cur = conn.open_pg()
        elif type in self.sqlserver:  ##
            # type = 'sqlserver'

            conn = opg.MysqlConn(db)
            cur = conn.open_mssql()
        elif type in self.db2:  # cur为连接id
            type = 'db2'
            conn = opg.MysqlConn(type)
            cur = conn.open_db2_odbc()
            # db2_conn(cur)
        elif '400' in type:
            type = 'db2_as400'
            conn = opg.MysqlConn(type)
            cur = conn.open_as400_odbc()
        else:  # 默认mssql
            type = 'mssql'
            conn = opg.MysqlConn(type)
            cur = conn.open_mssql()
        self.type = type

        return cur, type
    def clean(self, conn):
        # conn=self.current_cursor
        i = j = 0
        if self.type != 'db2':
            self.current_cursor = conn.cursor()
        else:
            self.current_cursor = db2_conn(conn)
            conn = self.current_cursor
        # self.before=set(tuple(self.before))
        print(self.before)
        while True:
            a = input('press a to clean database witch connecting right now \t or \t press x exit')
            if a == 'a':
                for item in self.before:
                    for element in item:
                        j += 1
                        if element == '': break
                        try:
                            self.current_cursor.execute(element.upper())
                            conn.commit()
                            # print("语句%s执行完毕" % element)
                            i += 1
                        except (pymssql._pymssql.OperationalError, pymysql.Error, cx_Oracle.Error) as e:
                            print('执行%s失败' % element, e)
                        except:
                            print(f'执行{element}失败', ibm_db.conn_errormsg())
                print('clean 完毕,成功%d条/共计%d' % (i, j))
                break
            if a == 'x':
                print('Exit')
                break
    def as400_pf(self, tbname):
        conn = self.current_cursor
        user = config.get('db2_as400', 'username')
        mbr = config.get('db2_as400', 'mbr')
        ftp = FTP(config.get('db2_as400', 'host'))
        ftp.login(user, config.get('db2_as400', 'password'))
        # ftp.set_debuglevel(2)# 打印调试信息
        ftp.sendcmd('TYPE I')  # 设置为 Binary 模式
        # ftp.cwd(r"  /QSYS.LIB/JXGENIUS1.LIB/SRCT.FILE")
        ftp.cwd(rf"/home/{user}")
        with open('resource/test_report/pydds.txt', 'rb') as f:
            ftp.storbinary('STOR pydds.txt', f)
        ftp.quit()
        print('txt文件上传完毕')
        command = f"CPYFRMSTMF FROMSTMF('/home/{user}/pydds.txt') " \
                  f"TOMBR('/qsys.lib/{config.get('db2_as400', 'schema')}.lib/{config.get('db2_as400', 'srcpf')}.file/{mbr}.mbr') " \
                  "MBROPT(*REPLACE)"
        conn.execute("CALL QSYS2.QCMDEXC(?, ?)", (command, len(command)))
        print(command, len(command))
        crtpf = f"CRTPF FILE(JXGENIUS1/{tbname}) SRCFILE(JXGENIUS1/SRCT) SRCMBR(PYDDS)"
        print(crtpf)
        conn.execute("CALL QSYS2.QCMDEXC(?, ?)", (crtpf, len(crtpf)))
        print('建表完毕,暂停5秒')
        time.sleep(5)
    def as400_ana(self, caseline):
        kv = {'T'};i = 0;crt = [];pri_list = []
        tb = re.findall(r'table\s(.*?)\(', caseline)[0].upper()
        if len(tb) > 10:
            tbname = ''.join([i[0] for i in tb.split('_')])
            # tbname = re.sub(r'_','',tbname)[:7]+re.sub(r'_','',tbname)[-3:]
        else:
            tbname = re.sub(r'_', '', tb)
        if 'primary key' in caseline:
            crt.append(['A' + ' ' * 43 + 'UNIQUE'])
        crt.append(['A' + ' ' * 15 + f'R {tbname}'])
        col_list = re.findall(r'\((.*?)$', caseline)[0].split(',')
        for line in col_list:
            if ')' in line and '(' not in line:
                col_list[i - 1] = col_list[i - 1] + ',' + col_list[i]
                del col_list[i]
            i += 1
        i = 0
        for line in col_list:
            col_list[i] = re.sub('primary key', 'pri', col_list[i])
            col_list[i] = re.sub('not null|default.*$|check.*$|unique.*$', '', col_list[i])
            if 'constraint' in col_list[i] and 'pri' in col_list[i]:
                col_list[i] = re.sub('constraint.*pri', 'pri 1 ', col_list[i])
            elif 'constraint' in line and 'pri' not in line:
                col_list[i] = ''
            test = [j for j in col_list[i].split(' ') if j != '']
            if 'pri' not in test[0]:
                col_p = []
                if len(test[0]) > 10:
                    col_name = str(test[0])[:7] + str(test[0])[-3:]
                else:
                    col_name = str(test[0])[:10]
                col_name = re.sub('_', '', col_name).upper()
                if re.findall('\((.*?)\)', test[1]):
                    col_p = re.findall('\((.*?)\)', test[1])[0].split(',')
                col_t = re.sub('\(.*?\)', '', test[1])
                if col_t == 'decfloat': col_p = []
                col_type = dds_mapping[f"{col_t}"]
                if len(col_p) > 1:
                    p = (6 - len(col_p[0])) * ' ' + col_p[0] + col_type + ' ' + col_p[1]
                elif len(col_p) == 1:
                    p = (6 - len(col_p[0])) * ' ' + col_p[0] + col_type
                else:
                    p = (7 - len(col_type)) * ' ' + col_type
                if len(test) > 2 and 'changes' not in str(test):
                    pri_list.append(col_name)
                crt.append(['A' + ' ' * 17 + col_name + (10 - len(col_name)) * ' ' + p])
            else:
                pri_list.append(re.findall('\((.*?)\)', test[2])[0].split(','))
            i += 1
        for temp in pri_list:
            crt.append(['A' + ' ' * 15 + 'K' + ' ' + temp])
        x = 0
        # 500字段
        # while(x<500):
        #     crt.append(['A' + ' ' * 17 + col_name+str(x) + (10 - len(col_name+str(x))) * ' ' + p])
        #     x+=1
        with open(r'resource/test_report/pydds.txt', 'w') as f:
            for item in crt:
                f.write(f'{item[0]}\n')
                print(item[0])
        # self.as400_pf(tbname)
        return tbname, tb, crt
    def interruptible_sleep(self,stop_event, total_seconds):
        interval = 0.5  # 每半秒检查一次
        for _ in range(int(total_seconds / interval)):
            if self._check_control(stop_event, '等待阶段'):
                return
            time.sleep(interval)

    def _get_stop_event(self, control):
        if control and hasattr(control, 'is_set'):
            return control
        if isinstance(control, dict):
            return control.get('stop_event')
        return None

    def _get_resume_event(self, control):
        if isinstance(control, dict):
            return control.get('resume_event')
        return None

    def _check_control(self, control, label='任务'):
        stop_event = self._get_stop_event(control)
        resume_event = self._get_resume_event(control)
        if stop_event and stop_event.is_set():
            print(f'{label} 收到停止指令')
            return True
        if not resume_event:
            return False
        notified = False
        while not resume_event.is_set():
            if stop_event and stop_event.is_set():
                print(f'{label} 收到停止指令')
                return True
            if not notified:
                print(f'{label} 已暂停，等待继续')
                notified = True
            time.sleep(0.5)
        if notified:
            print(f'{label} 已继续')
        return False
# 建表延迟
crt_delay = 1
# dml/ddl延迟
dml_delay = 1.5
# 是否overflow，0、不处理行内容，1、缩短行款
overflow = 0
# 手动调试
if __name__ == '__main__':  # 手动调试
    # 表名唯一、源库类型、源库类型（手动调用不需要参数）、用例链路
    #db='mssqlssms2016 develop'#填写config.ini中的section，数据库节点名
    db = 'mssql##'#172.20.72.117 doris
    #db = 'mssql_chenqq'
    type = config.get(db, 'type')
    incr = 0   # 表名唯一,以斜杠/区分用例，oracle用例文件有大量重复表名，1开启此项，默认0-false
    pre_crt = 1#     每个用例文件统一建表后再进行dml、ddl等操作      ，1开启此项，默认0-false
    cdir = mss_case_all
    p = read_case(incr, type, 1, cdir)
    mssql, type = p.define(type, db)
    T1 = time.perf_counter()
    for casefile in p.ergodic(type):
        p.analysis(casefile, pre_crt)
        #p.execute(mssql,None)
    T2 = time.perf_counter()
    print('总共耗时：\t\t%f毫秒' % ((T2 - T1) * 1000))
    p.clean(mssql)
# 打包
if __name__ == '__main1__':
    if len(sys.argv) < 3:
        print("用法: switch.exe <DBname> <表名是否唯一 1 on 0 off> [用例文件路径名(选填)]\n"
              "<表名是否唯一 1 on 0 off> 用例文件有大量重复表名开启此项\n[用例文件路径名(选填)] 默认根据数据库类型使用resource/case_dir.ini内容")
        sys.exit(1)
    type = sys.argv[1]
    incr = sys.argv[2]
    if len(sys.argv) == 4:
        cdir = case_cfg.read('case_file', sys.argv[3])
        p = read_case(incr, type, 0, cdir)
    else:
        p = read_case(incr, type, 0)

    mssql, type = p.define(type)
    T1 = time.perf_counter()
    for casefile in p.ergodic(type):
        p.analysis(casefile)
        p.execute(mssql)
    T2 = time.perf_counter()
    print('总共耗时：\t\t%f毫秒' % ((T2 - T1) * 1000))
    p.clean(mssql)
    # conn=opg.MysqlConn('mssql')
    # p = read_case()
    # mssql=conn.open_mssql()#open_mssql()传回mssql游标，赋给mssql变量
    # p.clean(mssql)
# 400 dds调试
if __name__ == '__test400__':
    type = 'db2_as400'
    incr = 0  # 表名唯一,以斜杠/区分用例，oracle用例文件有大量重复表名，1开启此项，默认0-false
    cdir = db2_case
    p = read_case(incr, type, 0, cdir)
    mssql, type = p.define(type)
    T1 = time.perf_counter()
    p.as400_pf(mssql)
    T2 = time.perf_counter()
    print('总共耗时：\t\t%f毫秒' % ((T2 - T1) * 1000))
    p.clean(mssql)

    # p.analysis()
    # p.execute(conn.open_mssql())
