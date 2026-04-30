# coding=utf-8
import json,re,pandas,configparser,sys,decimal,copy,requests,cx_Oracle,ergodic,numpy,datetime,chardet,os
import time as stime
import threading
from contextlib import ExitStack
from hdfs import InsecureClient
import pyarrow.parquet as pq
import pyarrow as pa
from dateutil import parser
from datetime import timezone,timedelta,date,time
from dateutil.tz import tzoffset

from decimal import Decimal
from shapely import wkb
from shapely.wkt import dumps
import struct
import standard
from queue import Queue
from collections import defaultdict
import multiprocessing
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from connector.registry import load_config
config = configparser.RawConfigParser()
config2 = configparser.RawConfigParser()
config.read(r'resource/compare.ini', encoding='utf-8')
config2.read(r'resource/DB.ini', encoding='utf-8')
compare_file=r'resource/mapping.xlsx'
#pandas.set_option('display.encoding', 'gbk')


class ergodic_database():
    def __init__(self):
        self._thread_state = threading.local()
        self._conn_src_lock = threading.Lock()
        self._conn_tgt_lock = threading.Lock()
        self._type_cache_lock = threading.Lock()
        self.src_db=''
        self.tgt_db=''
        self.conf_src=''
        self.conf_tgt=''
        self.src_tab_list = []
        self.tgt_tab_list = []
        self.src = {'tab_num': 0,'tab_name': []}
        self.tgt = {'tab_num': 0,'tab_name': []}
        self.src_col = {'tab_num': 0,'tab_name': '','tab_col':[]}
        self.tgt_col = {'tab_num': 0,'tab_name': '','tab_col':[]}
        self.bfe=[]
        #列属性
        self.col_tup=('col_num','col_name','type','len','num_pre','num_scale','time_sh','null','tab_name')
        self.src_col_list=[]
        self.tgt_col_list =[]
        self.src_final=[]
        self.tgt_final=[]
        self.col_result=[]
        #表内容行
        self.srctable_list=[]
        self.tgttable_list = []
        self.column=[]
        self.templine=''
        self.templist=[]
        self.tempdict={}
        self.cons_err=[]
        self.type_cache={}

        #mapping
        self.mysql=('mysql','tdsql','tidb')#mysql源
        self.pg=('postgre','lightdb','dws','tdsql_pg','opengauss','opengauss_gbk')
        self.oracle=('oracle')
        self.sqlserver=('sqlserver')
        self.db2=('db2')
        self.ob = ('obmysql', 'oboracle','ob_mysql', 'ob_oracle')
        self.clickhouse=('clickhouse')
        self.dm = ('dm')
        self.es = ('es','elasticsearch','elastic')
        self.hive = ('Argo','argo','hive')
        self.hdfs = ('hdfs','hadoop')
        #self.gauss=None
        self.data=None
        self.conn_src=None
        self.conn_tgt=None
        self.conn_tgt_cur=None
        self.conn_src_cur=None
        self.esl=[]
        self.bfe_last=[]
        self.report_addr=r'resource/test_report/report.xlsx'
        self.tgt_pri= {}
        self.src_pri= []
        self.ob_mode=None
        self.control={}
        #multi
        #self.pool=multiprocessing.Pool()

    @property
    def table_src_connector(self):
        return getattr(self._thread_state, 'table_src_connector', None)

    @table_src_connector.setter
    def table_src_connector(self, value):
        self._thread_state.table_src_connector = value

    @property
    def table_tgt_connector(self):
        return getattr(self._thread_state, 'table_tgt_connector', None)

    @table_tgt_connector.setter
    def table_tgt_connector(self, value):
        self._thread_state.table_tgt_connector = value

    def _uses_connector_backend(self):
        return bool(getattr(self, 'src_factory', None) and getattr(self, 'tgt_factory', None))

    def _db_type(self, db_key):
        return load_config(db_key).type

    def _schema_name(self, role):
        explicit = getattr(self, f'{role}_schema', None)
        if explicit:
            return explicit
        db_key = self.src_db if role == 'src' else self.tgt_db
        return load_config(db_key).schema

    def _table_name_for_role(self, table_name, role):
        if role == 'tgt' and getattr(self, 'tgt_uppercase', False):
            return str(table_name).upper()
        return table_name

    def _acquire_compare_connector(self, role):
        pool = getattr(self, f'{role}_pool', None)
        if pool is not None:
            return pool.acquire()
        factory = getattr(self, f'{role}_factory', None)
        if factory is None:
            raise RuntimeError(f'missing {role} connector factory')
        return factory()

    def _release_compare_connector(self, role, connector, broken=False):
        if connector is None:
            return
        pool = getattr(self, f'{role}_pool', None)
        if pool is not None:
            pool.release(connector, broken=broken)
            return
        try:
            connector.close()
        except Exception:
            pass
#单独拿拿去表连接，方便并发结构
    def _acquire_table_compare_connectors(self):
        acquired = {'src': False, 'tgt': False}
        if self.table_src_connector is None:
            self.table_src_connector = self._acquire_compare_connector('src')
            acquired['src'] = True
        if self.table_tgt_connector is None:
            self.table_tgt_connector = self._acquire_compare_connector('tgt')
            acquired['tgt'] = True
        return acquired
#放回
    def _release_table_compare_connectors(self, acquired, broken_roles=None):
        broken_roles = broken_roles or set()
        for role in ('src', 'tgt'):
            if not acquired.get(role):
                continue
            if role == 'src':
                connector = self.table_src_connector
                self.table_src_connector = None
            else:
                connector = self.table_tgt_connector
                self.table_tgt_connector = None
            self._release_compare_connector(role, connector, broken=role in broken_roles)

    def _normalize_table_snapshot(self, index, table_name, columns):
        if isinstance(columns, dict):
            columns['tab_num'] = index#架构更改后tab好靠外部计数，后续考虑删掉
            columns['tab_name'] = table_name
            return columns
        tab_col = []
        for column in columns:
            tab_col.append({
                'col_num': column.ordinal,
                'col_name': column.name,
                'type': column.type_name,
                'len': column.length,
                'num_pre': column.precision,
                'num_scale': column.scale,
                'time_sh': column.scale,
                'null': 'YES' if column.nullable else 'NO' if column.nullable is not None else None,
                'tab_name': table_name,
            })
        return {'tab_num': index, 'tab_name': table_name, 'tab_col': tab_col}

    def _load_table_columns(self, table_name, role):
        connector = self.table_src_connector if role == 'src' else self.table_tgt_connector
        owns_connector = connector is None
        if owns_connector:
            connector = self._acquire_compare_connector(role)
        broken = False
        try:
            schema = self._schema_name(role)
            table_name = self._table_name_for_role(table_name, role)
            columns = connector.get_columns(table_name, schema)
            return self._normalize_table_snapshot(0, table_name, columns)['tab_col']
        except Exception:
            broken = True
            raise
        finally:
            if owns_connector:
                self._release_compare_connector(role, connector, broken=broken)
#并发获取源备字段属性
    def _load_table_columns_pair(self, table_name, src_columns, tgt_columns):
        if self.tgt_db_t == 'hbase':
            if not src_columns:
                src_columns = self._load_table_columns(table_name, 'src')
            return src_columns, copy.deepcopy(src_columns)
        if src_columns and tgt_columns:
            return src_columns, tgt_columns

        results = {'src': src_columns, 'tgt': tgt_columns}
        tasks = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            if not src_columns:
                tasks[executor.submit(self._load_table_columns, table_name, 'src')] = 'src'
            if not tgt_columns:
                tasks[executor.submit(self._load_table_columns, table_name, 'tgt')] = 'tgt'
            for future, role in tasks.items():
                results[role] = future.result()
        return results['src'], results['tgt']

    def _collect_schema_from_connector(self, role):
        connector = self._acquire_compare_connector(role)
        broken = False
        try:
            schema = self._schema_name(role)
            table_names = connector.list_tables(schema)
            if role == 'src':
                self.src_pri = connector.list_primary_tables(schema)
            else:
                self.tgt_pri = connector.list_primary_map(schema)

            # schema 阶段只拿表名，列信息延迟到单表 compare 时再查。
            snapshot = [
                {'tab_num': index, 'tab_name': table_name, 'tab_col': []}
                for index, table_name in enumerate(table_names, start=1)
            ]
            return len(snapshot), snapshot
        except Exception:
            broken = True
            raise
        finally:
            self._release_compare_connector(role, connector, broken=broken)

    def define_type(self):
        if not self._uses_connector_backend():
            raise RuntimeError('legacy connection logic has been removed; runner must provide connector factories')

        with ThreadPoolExecutor(max_workers=2) as executor:
            # schema、对象、数据三段都走同一套连接池；这里只是先借后还，不在表级别重复建连。
            src_future = executor.submit(self._collect_schema_from_connector, 'src')
            tgt_future = executor.submit(self._collect_schema_from_connector, 'tgt')
            src = src_future.result()
            tgt = tgt_future.result()

        self.conf_src = self.src_db_t
        self.conf_tgt = self.tgt_db_t
        return src, tgt

    def _resolve_order_columns(self, order_col):
        if not order_col or not order_col[1]:
            return None
        columns = []
        for value in order_col[1].values():
            if not value:
                continue
            for col in str(value).split(','):
                col_name = col.strip().strip('"').strip('`').strip('[]')
                if col_name and col_name not in columns:
                    columns.append(col_name)
        return columns or None

    def set_control(self, control=None):
        self.control = control or {}

    def _stop_event(self):
        if isinstance(self.control, dict):
            return self.control.get('stop_event')
        return None

    def _resume_event(self):
        if isinstance(self.control, dict):
            return self.control.get('resume_event')
        return None

    def _is_stop_requested(self):
        stop_event = self._stop_event()
        return bool(stop_event and stop_event.is_set())

    def _wait_if_paused(self, label='compare任务'):
        resume_event = self._resume_event()
        stop_event = self._stop_event()
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
            stime.sleep(0.5)
        if notified:
            print(f'{label} 已继续')
        return False

    def _check_control(self, label='compare任务'):
        if self._is_stop_requested():
            print(f'{label} 收到停止指令')
            return True
        return self._wait_if_paused(label)

    @property
    def bfe(self):
        return getattr(self._thread_state, 'bfe', [])

    @bfe.setter
    def bfe(self, value):
        self._thread_state.bfe = value

    @property
    def ora_search_cloumn(self):
        return getattr(self._thread_state, 'ora_search_cloumn', None)

    @ora_search_cloumn.setter
    def ora_search_cloumn(self, value):
        self._thread_state.ora_search_cloumn = value

    @property
    def esl(self):
        return getattr(self._thread_state, 'esl', [])

    @esl.setter
    def esl(self, value):
        self._thread_state.esl = value

    def _reset_compare_context(self, table_name=None):
        self.bfe = [table_name] if table_name else []
        self.ora_search_cloumn = None
        self.esl = []

    @staticmethod
    def _is_thread_safe_session(session):
        return bool(getattr(session, 'is_thread_local_proxy', False))

    def dm_tab(self,source):
        i=j=k=0
        ora1 = opg.MysqlConn(f'{source}')
        ora_conn=ora1.open_dm()
        ora=ora_conn.cursor()
        if self.src_db_t in self.dm:
            self.conn_src = ora_conn;db=self.src_db
        else:self.conn_tgt = ora_conn;db=self.tgt_db
        schema=config2.get(f'{db}','databasename')
        #self.conn_tgt = ora_conn
        ora.execute(config.get('dm','table_name').format(schema=schema))
        for self.templine in ora.fetchall():
            if self.templine[0].startswith('i2_logkeeper'):
                continue
            else:
                self.tgt['tab_name'].append(self.templine[0])
        self.tgt['tab_num']=self.tgt['tab_name'].__len__()
        final = self.tgt_final
        for self.templine in self.tgt['tab_name']:
            ora.execute(config.get('dm','col').format(schema=schema,tbname=self.templine))
            p=ora.fetchall()
            for item in p:
                item=list(item)
                if item[-1]=='N':item[-1]='NO'
                elif item[-1]=='Y':item[-1]='YES'
                self.tgt_col_list.append({self.col_tup[x]:item[x] for x in range(len(item))})
            i+=1
            self.tgt_col['tab_num']=i
            self.tgt_col['tab_name']=self.templine
            self.tgt_col['tab_col']=self.tgt_col_list
            self.tgt_col_list=[]
            final.append(self.tgt_col)
            self.tgt_col = {'tab_num': 0, 'tab_name': '', 'tab_col': []}
        #print(self.tgt_final)
        self.tgt['tab_num']=self.tgt['tab_name'].__len__()
        return  self.tgt['tab_num'], final
    def es_tab(self):
        tmp=[]
        host=config2.get('elastic','host')
        port=config2.get('elastic','port')
        schema=config2.get('elastic','schema')#赋值schema
        aliases_res=requests.get(f'http://{host}:{port}/_cat/aliases?v&format=json')
        a = copy.deepcopy(self.tgt)
        for x in aliases_res.json():
            if x['alias'].startswith('.'):continue
            else:
                tmp.append(x['alias'])
                a['tab_name'].append(re.sub(f'i2import_{schema}_','',x['alias']))#databasename 后续会加
        a['tab_num'] = tmp.__len__()
        final =copy.deepcopy(self.tgt_final)
        for self.templine in tmp:
            field_i = []
            field=requests.get(f'http://{host}:{port}/{self.templine}/_mapping').json()
            for value in field.items():
                field=value[1]['mappings']['properties']
                for i,key,val in zip(range(len(field)),list(field.keys()),list(field.values())):
                    k = []
                    k.append(i);k.append(key);k.append(val['type'])
                    for j in range(4):
                        k.append(None)
                    k.append('YES');k.append(None)
                    field_i.append(tuple(k))
            #print(field_i)
            for item in field_i:
                self.tgt_col_list.append({self.col_tup[x]:item[x] for x in range(len(item))})
            self.tgt_col['tab_name']=re.sub(f'i2import_{schema}_','',self.templine)
            self.tgt_col['tab_col']=self.tgt_col_list
            self.tgt_col_list=[]
            final.append(self.tgt_col)
            self.tgt_col = {'tab_num': 0, 'tab_name': '', 'tab_col': []}
        return  a['tab_num'],final
    def hive_tab(self):
        i=j=k=0
        ora1 = opg.MysqlConn('hive')# 默认为pg数据库
        ora_conn=ora1.open_hive()
        ora=ora_conn.cursor()
        schema=config2.get('hive','databasename')#赋值schema
        if self.conf_src in self.hive:
            self.conn_src = ora_conn
        else:self.conn_tgt = ora_conn
        #self.conn_tgt = ora_conn
        ora.execute(config.get('hive','table_name').format(dbname=schema))
        a = copy.deepcopy(self.tgt)
        p=ora.fetchall()
        for self.templine in p:
            if self.templine[0].startswith('i2_logkeeper'):
                continue
            else:
                a['tab_name'].append(self.templine[0])
        a['tab_num']= a['tab_name'].__len__()
        final =copy.deepcopy(self.tgt_final)
        for self.templine in a['tab_name']:
            ora.execute(rf'''{config.get('hive','col').format(dbname=schema,tbname=self.templine)}''')
            #ora.execute(rf"""select ORDINAL_POSITION,COLUMN_NAME,data_type,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,IS_NULLABLE from INFORMATION_SCHEMA.COLUMNS where Table_schema='{schema}' and TABLE_NAME="{self.templine}" order by ORDINAL_POSITION""")
            p=ora.fetchall()
            for item in p:
                t=list(item)
                if t[-2]=='TRUE':t[-2]='YES'
                else :t[-2]='No'
                item=tuple(t)
                self.tgt_col_list.append({self.col_tup[x]:item[x] for x in range(len(item))})
            i+=1
            self.tgt_col['tab_num']=i
            self.tgt_col['tab_name']=self.templine
            self.tgt_col['tab_col']=self.tgt_col_list
            self.tgt_col_list=[]
            final.append(self.tgt_col)
            self.tgt_col = {'tab_num': 0, 'tab_name': '', 'tab_col': []}
        ora.execute(config.get('hive', 'pri').format(dbname=config2.get('hive', 'databasename')))
        pri = {}
        for k in ora.fetchall():
            if k[0] not in pri:
                pri[k[0]]=[]
            pri[k[0]].append(k[1])
        self.tgt_pri=pri
        #print(self.tgt_final)
        a['tab_num']= a['tab_name'].__len__()
        return  a['tab_num'],final
    def hdfs_tab(self):
        i=k=0;j=1
        kb=config2.get('hdfs','kerberos')
        folder_path=config2.get('hdfs','local_dir')
        might_err=[]
        search_name=[]
        list_name=[]
        empty=[None,None,None,None,'yes',None]
        host,port,schema,dir,user=config2.get('hdfs','host'),config2.get('hdfs','port'),config2.get('hdfs','schema'),config2.get('hdfs','dir'),config2.get('hdfs','user')
        hdfs_path = folder_path + f'{schema}'
        print(f"{config2.get('hdfs','host')}:{config2.get('hdfs','port')}")
        hdfs_url=f"http://{host}:{port}"
        client = InsecureClient(hdfs_url,user=schema)
        a = copy.deepcopy(self.tgt)
        a['tab_num'] = a['tab_name'].__len__()
        if kb=='yes':
            file_info=os.listdir(hdfs_path)
            for file1 in file_info:
                if os.path.isdir(hdfs_path+'/'+file1):
                    for i in os.listdir(hdfs_path+'/'+file1):
                        if os.path.isfile(hdfs_path+'/'+file1+'/'+i):
                            list_name.append(file1+'/'+i)
                            search_name.append(i)
                            a['tab_name'].append(re.sub('.parquet','',i))
                        else:might_err.append(file1+'/'+i)
        else:
            file_info=client.list(hdfs_path=dir,status=True)
            for file in file_info:
                if file[1]['type'] == 'FILE':
                    search_name.append(file[0])
                    a['tab_name'].append(re.sub('.parquet', '', file[0]))
                else:
                    might_err.append(file[0])
        if self.conf_src in self.hdfs:
            self.conn_src = client
        else:self.conn_tgt = client
        self.conn_tgt = client
        if os.path.exists(hdfs_path):
            if len(os.listdir(hdfs_path)) == len(file_info):
                print(f'文件齐全，含目录共{len(os.listdir(hdfs_path)) }')
            else:print(f'本地文件缺失，hdfs{len(file_info)},local{len(os.listdir(hdfs_path))}')
        else:print(f'路径不存在{hdfs_path}')
        a['tab_num']= a['tab_name'].__len__()
        final =copy.deepcopy(self.tgt_final)
        #self.tgt_col = {'tab_num': 0, 'tab_name': '', 'tab_col': []}
        for self.templine,search in zip(a['tab_name'],list_name):
            parquet_file = pq.ParquetFile(hdfs_path+f'/{search}')
            #print(parquet_file.schema_arrow)
            for field in parquet_file.schema_arrow:
                item = []
                item.append(j)
                item.append(field.name)
                item.append(str(field.type))
                item+=empty
                self.tgt_col_list.append({self.col_tup[x]:item[x] for x in range(len(item))})
                j+=1
            j=1
            self.tgt_col['tab_num']=i
            self.tgt_col['tab_name']=self.templine
            self.tgt_col['tab_col']=self.tgt_col_list
            self.tgt_col_list=[]
            final.append(self.tgt_col)
            self.tgt_col = {'tab_num': 0, 'tab_name': '', 'tab_col': []}
        # ora.execute(config.get('hive', 'pri').format(dbname=config2.get('hive', 'databasename')))
        pri = {}
        self.tgt_pri=pri
        print('merr',might_err)
        return  a['tab_num'],final
    def hbase_tab(self):
        import hbase_conn#懒加载，连接hbase需要单独启动jvm，该操作过重
        #不认为关系型数据库-hbase会导致备端列少于源端，暂不考虑备端缺列情况，此处不打算查出全部列信息，仅返回表数量，列信息后续直接拿源端信息
        hc=hbase_conn.connFactory(self.tgt_db)
        #a=copy.deepcopy(self.tgt)
        tbnum=hc.hb_tab()
        self.conn_tgt_cur=hc
        return tbnum
    def map_analysis(self,file):
        if self.src_db_t in self.sqlserver:
            sheet='mapping_mss';complex='mss_complex_mapping'
        elif self.src_db_t in self.pg:
            sheet='mapping_pg';complex='pg_complex_mapping'
        elif self.src_db_t in self.mysql:
            sheet='mapping_mysql';complex='pg_complex_mapping'
        elif self.src_db_t in self.oracle:
            sheet='mapping_oracle';complex='oracle_complex_mapping'
        elif self.src_db_t in self.db2:
            sheet='mapping_db2';complex='db2_complex_mapping'
        else :sheet='mapping_mss';complex='mss_complex_mapping'#缺省ssms
        self.data = pandas.read_excel(file, sheet_name=sheet, keep_default_na=False)  # dataframe类型
        self.complex_map = pandas.read_excel(file, sheet_name=complex, keep_default_na=False)
        k=0
        if self.tgt_db_t in self.pg:x='postgre'
        elif self.tgt_db_t in self.mysql:x='mysql'
        elif self.tgt_db_t in self.oracle:x='oracle'
        elif self.tgt_db_t in self.clickhouse:x='clickhouse'
        elif self.tgt_db_t in self.sqlserver:x='sqlserver'
        elif self.tgt_db_t in self.dm:x='dm'
        elif self.tgt_db_t in self.es:x = 'elasticsearch'
        elif self.tgt_db_t in self.hive:x='hive'
        elif self.tgt_db_t in self.hdfs:x = 'hdfs'
        elif self.tgt_db_t =='hbase':x='hbase'
        elif self.tgt_db_t in self.ob:
            if 'oracle' in self.tgt_db_t:x='ob_oracle'
            else:x='ob_mysql'
        else:
            x='kafka'
            print('未在mapping映射表中找到备库类型，使用kafka映射表补位')
        for i in self.data.columns.values:
            if i==x:
                self.tgt_index=k
            if i==self.src_db_t:
                self.src_index=k
            k += 1
    def map_compare_simple(self,srcd,tgtd,tgt_col_temp):
        no_info='\033[0;31m NO! \033[0m \t %s列源备映射错误，源端为%s %s位\t期望更改为%s\t实际备端为%s %s位' % (srcd['col_name'], srcd['type'], srcd['len'],tgt_col_temp, tgtd['type'],tgtd['len'])
        yes_info='\033[0;32m YES! \033[0m \t %s列源备映射正确，源端为%s %s位\t期望更改为%s\t实际备端为%s %s位' % (srcd['col_name'], srcd['type'], srcd['len'],tgt_col_temp, tgtd['type'],tgtd['len'])
        no_cord = ' NO!  \t %s列源备映射错误，源端为%s %s位\t期望更改为%s\t实际备端为%s %s位' % (srcd['col_name'], srcd['type'], srcd['len'], tgt_col_temp, tgtd['type'], tgtd['len'])
        yes_cord = 'YES! \t %s列源备映射正确，源端为%s %s位\t期望更改为%s\t实际备端为%s %s位' % (srcd['col_name'], srcd['type'], srcd['len'], tgt_col_temp, tgtd['type'], tgtd['len'])

        if re.sub(r"\(.*?\)", '', tgtd['type']).replace(' ','').upper() == re.sub(r"\(.*?\)", '', tgt_col_temp).replace(' ','').upper():
            #print('%s列源备映射正确，源：%s 备%s' % (srcd['col_name'], srcd['type'], tgtd['type']))
            if re.search(r"\((.*?)\)", tgt_col_temp):
                    a = re.search(r"\((.*?)\)", tgt_col_temp).group(1)
                    b = a.split(',')
                    if len(b) == 1:
                        if a.isalpha():  # 备端映射表是n，比对源备字段
                            if tgtd['num_pre'] :
                                if tgtd['num_pre'] == srcd['num_pre']:
                                    print(yes_info);self.bfe.append(yes_cord)
                                else:
                                    print(no_info);self.bfe.append(no_cord)
                            elif tgtd['time_sh'] :
                                if srcd['time_sh'] == tgtd['time_sh']:
                                    print(yes_info);self.bfe.append(yes_cord)
                                else:
                                    print(no_info);self.bfe.append(no_cord)
                            else:
                                if srcd['len'] and tgtd['len'] and any(srcd['len'] * multiplier == tgtd['len'] for multiplier in [0.5,1,2,4]):
                                    print(yes_info); self.bfe.append(yes_cord)
                                elif srcd['len']==-1 and any(mul ==tgtd['len'] for mul in [2000,400,8000]):
                                    print(yes_info); self.bfe.append(yes_cord)
                                elif srcd['type']=='bit' and tgtd['type']=='tinyint' and tgtd['num_pre']==3:
                                    print(yes_info,'源端为bit，特殊情况具体分析，转换正确'); self.bfe.append(yes_info+'源端为bit，特殊情况具体分析，转换正确')
                                elif not srcd['len'] and not tgtd['len']:
                                    print('no,len不存在')
                                    self.bfe.append('no,len不存在')# 可能是char列，源备存储格式长度不同，待改进
                                else:print(no_info);self.bfe.append(no_cord)
                        elif a.isdigit():
                            if tgtd['num_pre']:
                                if int(a) * 1 or 3== tgtd['num_pre']:
                                    print(yes_info) ; self.bfe.append(yes_cord)
                                else:
                                    print(no_info) ; self.bfe.append(no_cord)
                            elif tgtd['time_sh']:
                                if int(a) == tgtd['time_sh']:
                                    print(yes_info) ; self.bfe.append(yes_cord)
                                else:
                                    print(no_info) ; self.bfe.append(no_cord)
                            else:
                                if int(a) == 1 or 2 or 4 * tgtd['len']:
                                    print(yes_info,'\t位数精度不存在，通过长度比较') ; self.bfe.append(yes_info+'\t位数精度不存在，通过长度比较')
                                else:
                                    print(no_info,'\t位数精度不存在，未通过长度比较') ;self.bfe.append(no_info+'\t位数精度不存在，未通过长度比较')
                    elif len(b) > 1:  # 少数情况，多次比较精度、长度
                        item, item_scale = b
                        if item.isalpha():
                            if tgtd['num_pre']:
                                if tgtd['num_pre'] == srcd['num_pre']:
                                    if tgtd['num_scale'] is not None:
                                        if srcd['num_scale'] == tgtd['num_scale']:
                                            print(yes_info) ; self.bfe.append(yes_cord)
                                    else:print("645行判断if tgtd['num_scale']为假")
                            else:
                                print('源备长度不一致,错误') and self.bfe.append('源备长度不一致,错误')
                        elif item.isdigit():
                            if tgtd['num_pre']:
                                if int(item) == tgtd['num_pre']:
                                    if tgtd['num_scale']:
                                        if int(item_scale) == tgtd['num_scale']: print(yes_info) ; self.bfe.append(yes_cord)
                            else:
                                print('列属性数值不符合映射表') and self.bfe.append('列属性数值不符合映射表')
            else:print(yes_info) ; self.bfe.append(yes_cord)

        elif  re.search(r"\(.*?\)", tgt_col_temp) is None:
            if tgt_col_temp.upper() == tgtd['type']:
                #print('yes，源端为%s，\t期望更改为%s\t实际备端为%s' % (srcd['type'], tgt_col_temp, tgtd['type']))
                print(yes_info) ; self.bfe.append(yes_cord)
            else:
                print(no_info) ; self.bfe.append(no_cord)
                #print('no，源端为%s，\t期望更改为%s\t实际备端为%s' % (srcd['type'], tgt_col_temp, tgtd['type']))
        else:
            print(no_info) ; self.bfe.append(no_cord)
            #print('%s列源备映射错误，源端为%s，\t期望更改为%s\t实际备端为%s' % (srcd['col_name'], srcd['type'], tgt_col_temp, tgtd['type']))
    #sqlserver
    def src_cons_ana(self,table_name,col_num,n):#sqlserver
        table_name=re.sub('\'',"''",table_name)
        columns_info = []
        if n == 'src':
            cursor = self.conn_src.cursor()
            #db_type = self.conf_src
        elif n == 'tgt':
            cursor = self.conn_tgt.cursor()
        cursor.execute(f"EXEC sp_help '{table_name}'")
        col_list=[]
        while True:
                if not cursor.description:
                    break
                f = cursor.fetchall()
                if not f:
                    break
                elif f[0] is None or len(f[0])== 1 or len(f[0])==4:
                    if not cursor.nextset():
                        break
                else:
                    col_list+=f
                    if not cursor.nextset():
                        break


        for item in col_list[0:col_num]:
                columns_info.append({
                    'col_name': item[0], 'pk': None,
                    'unique_name': None, 'unique_complex': None,
                    'check_name': '', 'check_define': '',
                    'col_default': None,
                    'index_name': None, 'index_type': None, 'index_complex': None
                })
        for item in col_list[col_num-1:]:
                if len(item)==3:#index
                    inno=item[2]
                    in_name=item[0]
                    in_type=re.findall(r'(.*?)\slocated\son',item[1])[0]
                    if len(inno.split(','))==1:
                        in_comp=None
                    else:in_comp=inno;inno=inno.split(',')[0]
                    for d in columns_info:
                        if d['col_name'] == inno :
                            d['index_name'] = in_name;d['index_type'] = in_type
                            d['index_complex'] = in_comp
                            break
                        # elif d['col_name'] == inno and self.conf_src== 'oracle' and 'nonclustered' in in_type:
                        #     d['unique_name'] = in_name;d['unique_complex']= in_comp;break
                        # elif d['col_name'] == inno and self.conf_src== 'oracle' and 'nonclustered' not in in_type:
                        #     d['index_name'] = in_name;d['index_type'] = in_type;d['index_complex'] = in_comp
                        #     break
                elif len(item)>4:
                    if 'CHECK' in item[0]:
                        if 'Table Level' not in item[0]:
                            ch_name=re.search(r'(?<=on\scolumn\s)(.*?)$', item[0]).group(0);ckname=item[1]
                        else:
                            ch_name=columns_info[0]['col_name'];ckname='LvTB'+item[1]#与oracle联动，表级check
                        for d in columns_info:
                            if d['col_name']==ch_name:
                                d['check_name']+=ckname;d['check_define']+=item[-1];break
                    if 'DEFAULT' in item[0]:
                        df_name = re.search(r'(?<=on\scolumn\s)(.*?)$', item[0]).group(0)
                        for d in columns_info:
                            if d['col_name']==df_name:
                                d['col_default']=re.findall(r'\((.*?)\)',item[-1])[0].replace('(','').replace(')','')
                                break

                    if 'UNIQUE' in item[0]:
                        if len(item[-1].split(','))==1:
                            un_name=item[-1];un_comp=None
                        else:un_comp=item[-1];un_name=item[-1].split(',')[0]

                        un=item[1]
                        for d in columns_info:
                            if d['col_name'] == un_name:
                                d['unique_name']=un;d['unique_complex']=un_comp;break
                    if 'PRIMARY' in item[0]:
                        if len(item[-1].split(',')) == 1:
                            pk='YES';pk_name=item[-1]
                        else:
                            pk = item[-1];pk_name = item[-1].split(',')[0]
                        for d in columns_info:
                            if d['col_name'] == pk_name:
                                d['pk']=pk;break
        #print(columns_info)
        return columns_info
    #oracle/postgre/mysql
    def cons_analysis(self, table_name,col_num,n):#加类型参数
        
        print('当前线程：',threading.current_thread().name,'正在获取：',table_name,'端内容')
        start = stime.monotonic()
        columns_info = [];dbname=''
        connector_types = {'sqlserver', 'mysql', 'tdsql', 'tidb', 'hbase'}
        if self._uses_connector_backend():
            db_type = self.conf_src if n == 'src' else self.conf_tgt
            if db_type in connector_types:
                role = 'src' if n == 'src' else 'tgt'
                connector = self.table_src_connector if role == 'src' else self.table_tgt_connector
                owns_connector = connector is None
                if owns_connector:
                    connector = self._acquire_compare_connector(role)
                schema = self._schema_name(role)
                try:
                    return connector.get_obj(self._table_name_for_role(table_name, role), schema, col_num=col_num)
                finally:
                    if owns_connector:
                        self._release_compare_connector(role, connector)
        if n == 'src' and self.conf_src not in self.db2:
            cursor = self.conn_src.cursor()
            db_type = self.conf_src;dbname=self.src_db
        elif n=='src' and self.conf_src in self.db2:
            cursor = self.conn_src
            db_type=self.conf_src;dbname=self.src_db
        elif n == 'tgt' and self.conf_tgt not in ('elastic','hdfs'):
            cursor = self.conn_tgt.cursor()
            db_type = self.conf_tgt;dbname=self.tgt_db
        elif n == 'tgt' and self.conf_tgt in ('elastic','hdfs'):
            db_type=self.conf_tgt
        else:
            cursor = self.conn_src.cursor()
            db_type = self.conf_src
            print('744 analysis传丢参数 n')
        if db_type in ('mysql','ob_mysql'):
            # 查询MySQL表的创建语句
            cursor.execute(f"SHOW CREATE TABLE `{table_name.lower()}`")
            create_table_result = cursor.fetchone()
            create_table_statement = create_table_result[1]
            # 解析创建语句，获取列信息
            lines = create_table_statement.split('\n')
            for line in lines[1:col_num+1]:  # 跳过表名和结束括号
                line = line.strip()
                if line.startswith('`'):  # 列名行
                    parts = line.split(' ')
                    column_name = parts[0].strip('`')
                    col_default = None
                    col_index_name = None
                    check_define = None
                    pk = None
                    unique_name = None
                    unique_complex = None
                    check_name = None
                    index_type = None
                    index_complex = None
                    # 查找默认值
                    if 'DEFAULT' in line:
                        start_index = line.find('DEFAULT') + len('DEFAULT')
                        end_index = line.find(' 'or',', start_index+1)
                        col_default = line[start_index:end_index].strip()
                    columns_info.append({
                            'col_name': column_name, 'pk': pk,
                            'unique_name': unique_name, 'unique_complex': unique_complex,
                            'check_name': check_name, 'check_define': check_define,
                            'col_default': col_default,
                            'index_name': col_index_name, 'index_type': index_type, 'index_complex': index_complex
                        })
                    # 查找索引名
            for line in lines[col_num:]:
                if 'PRIMARY' in line:
                    pk='PRIMARY'
                    pkn=re.findall(r"\((.*?)\)", line)[0].replace('`','')
                    if len(pkn.split(','))==1:
                        a='YES'
                    else:a=pkn;pkn=pkn.split(',')[0]
                    for d in columns_info:
                        fuk=d['col_name']
                        if d['col_name']==pkn:d['pk']=a;d['index_name']=pk;d['index_type']='UNIQUE';break
                elif 'KEY' in line or 'INDEX' in line :
                    cosn=re.findall(r"\(`(.*?)`\)", line)[0]
                    if len(cosn.split(','))==1:
                        na=re.sub('`','',cosn);complex=None
                    else:
                        na=re.sub('`','',cosn.split(',')[0]);complex=re.sub('`','',cosn)
                    cons_name=re.search(r"`(.*?)`", line).group(0)
                    for d in columns_info:
                        if d['col_name']==na:
                            d['index_name'] = cons_name
                            if 'UNIQUE' in line:
                                d['unique_name'] = cons_name;d['unique_complex'] = complex;d['index_type']='UNIQUE';break
                            else:pass
                elif 'CONSTRAINT' in line:
                    #cos = re.findall(r'\"\s(.*?)\s\(', line)
                    index_type='CHECK'
                    index_name=re.findall(r'(?<=CONSTRAINT\s`).*?(?=`\sCHECK)',line)[0].replace(' ','')
                    index_define_font=re.findall('\(\(\((.*?)\)\)\)',line)[0].replace(')'or'(','')
                    index_define=re.sub('`|\(|\s','',index_define_font)
                    name=re.sub('`','',re.search(r'`(.*?)`',index_define_font).group(0))
                    for d in columns_info:
                        if d['col_name']==name:
                            d['check_name']=index_name;d['check_define']=index_define;break
                else:pass
        elif db_type == 'postgre':
            # 查询PostgreSQL表的创建语句
            schema=config2.get(dbname,'schema')
            con_s=f'''SELECT pg_get_constraintdef(c.oid),c.conname FROM pg_constraint AS c JOIN pg_class AS t ON c.conrelid = t.oid WHERE t.relname = '{table_name.lower()}'  AND (c.contype <> 'p' or c.contype = 'p' or c.contype='d')
                     union 
                     SELECT pg_get_indexdef(indexrelid),null as conname FROM pg_index WHERE indrelid = '{schema}.{table_name.lower()}'::regclass'''
            def_s=f'''select column_name,column_default from INFORMATION_SCHEMA.columns where TABLE_NAME = '{table_name.lower()}' and table_schema = '{schema}' order by ordinal_position'''
            cursor.execute(def_s)
            #comment_s=f'''SELECT a.attname,col_description(a.attrelid,a.attnum) FROM pg_class as c,pg_attribute as a where a.attrelid = c.oid and a.attnum>0 and c.relname= '{table_name.lower()}' '''
            def_result = cursor.fetchall()
            cursor.execute(con_s)
            cos_result=cursor.fetchall()
            cursor.close()
            for item in def_result:
                column_name=item[0]
                if not item[1]:
                    col_default=item[1]
                elif item[1] and len(item[1].split(':'))==1:
                    col_default = item[1]
                else:col_default=item[1].split(':')[0]

                columns_info.append({
                'col_name': column_name, 'pk': None,
                'unique_name': None, 'unique_complex': None,
                'check_name': None, 'check_define': None,
                'col_default': col_default,
                'index_name': None, 'index_type': None, 'index_complex': None,'comment':None
            })
            for line in cos_result:
                a=line[0];b=line[1]
                if b is None :
                    index_type = re.findall(r'(?<=CREATE\s)(.*?)(?=\sINDEX\s)', a)
                    if index_type:
                        index_type=index_type[0]
                    else:index_type=None
                    index_name=re.findall(r'(?<=INDEX\s)(.*?)(?=ON\s)',a)[0]
                    inno=re.sub('"','',re.findall(r'(?<=btree\s\()(.*?)(?=\))',a)[0])
                    if len(inno.split(','))==1:
                        inno=inno;index_complex=None
                    else :index_complex=inno;inno=inno.split(',')[0]
                    for d in columns_info:
                        if d['col_name']==inno:
                            d['index_name']=index_name;d['index_type']=index_type;d['index_complex']=index_complex
                            break
                elif b is not None and 'UNIQUE' in a:
                    unno=re.findall(r'\((.*?)\)',a)[0]
                    if unno.split(',').__len__()==1:
                        un_complex=None
                    else:un_complex=unno;unno=unno.split(',')[0]
                    for d in columns_info:
                        if d['col_name']==unno:
                            d['unique_name']=b;d['unique_complex']=un_complex;break
                elif b is not None and 'CHECK' in a:
                    #a = re.sub('::text', '', re.findall(r'\s\(\((.*?\))\)',a)[0])
                    ch_define=re.sub('::text', '', re.findall(r'\s\((.*?\))\)',a)[0])
                    unno_befor=re.findall(r'(?<=\()(.*?)(?=\s>|\s<|\s=)',ch_define)[0]
                    unno=re.sub('\(|\)','',unno_befor)

                    for d in columns_info:
                        if d['col_name']==unno:
                            d['check_name']=b;d['check_define']=ch_define;break
                elif b is not None and 'PRIMARY' in a:
                    pkno=re.findall(r'\((.*?)\)',a)[0]
                    if pkno.split(',').__len__()==1:
                        pk='YES'
                    else:pk=pkno;pkno=pkno.split(',')[0]
                    for d in columns_info:
                        if d['col_name']==pkno:
                            d['pk']=pk;break
        elif db_type == 'oracle' or db_type=='ob_oracle':
            ind_statement=[];unique_col=[]
            table_name=re.sub("'","''",table_name.upper())
            tmp=[]
            # 查询Oracle表的创建语句 ob权限关系导致用户查不出user_cons/indexes。改为all_...
            if 'ob' not in db_type:
                schema=config2.get(dbname,'schema').upper()
                ora_index = f"SELECT to_char(DBMS_METADATA.GET_DDL('INDEX',u.index_name)) FROM user_INDEXES u where TABLE_OWNER='{schema}' and TABLE_NAME='{table_name.upper()}'"
            else:
                schema=config2.get(dbname,'databasename').upper()
                ora_index = config.get('oracle', 'ob_cons').format(schema=schema,tbname=table_name)
            #ora_index=f"SELECT to_char(DBMS_METADATA.GET_DDL('INDEX',u.index_name,{schema})) FROM all_INDEXES u where TABLE_OWNER='DBO' and TABLE_NAME='{table_name.upper()}'"
            try:
                cursor.execute(ora_index)
                c=cursor.fetchall()
            except:
                c=None
                pass
            indexlins =c#clob列表
            #cursor.execute(f"SELECT to_char(DBMS_METADATA.GET_DDL('TABLE', '{table_name.upper()}','{schema}')) FROM DUAL")
            #读取Blob，放到python里转换防止表字段过多超出cxoracle 4kB限制,fetch获取固定为链表+元组包裹，更改读取方式注意下面的语句拆分不要依旧使用[0][0]
            cursor.execute(#
                f"""SELECT DBMS_METADATA.GET_DDL('TABLE', '{table_name}','{schema}') FROM DUAL""")
            create_table_statement = cursor.fetchall()[0][0].read()
            cursor.execute(config.get('oracle','cons').format(tbname=table_name))
            ck_result=cursor.fetchall()#print(create_table_result,type(create_table_result))
            #create_table_statement = create_table_result[0][0].read().replace('\t','')
            lines=[]
            if db_type !='ob_oracle':
                for item in indexlins:
                    #ind_statement+=item[0].read().replace('\t','').split('\n')
                    ind_statement += item[0].replace('\t', '').split('\n')
                lines = create_table_statement.split('\n')+ind_statement
            elif indexlins:
                if indexlins!=create_table_statement:
                    lines += create_table_statement[0][0].split('\n')
                    for item in indexlins:
                        lines.append(re.sub('\n','',item[0]).split(';')[0])
                else:lines = create_table_statement[0][0].split('\n')
            else:lines = create_table_statement.split('\n')

            #print(lines)
            for line in lines[1:col_num+2]:
                line = line.strip()
                col_default = None;col_index_name = None;check_define = ''
                pk=None;unique_name=None;unique_complex=None;check_name='';index_type=None;index_complex=None
                if re.sub('\s|\t','',line).startswith('"') or re.sub('\s|\t','',line).startswith('("'):  # 列名行
                    parts = line.split(' ')
                    column_name = re.sub('\(|"|\t','',parts[0])
                    #col_default = None;col_index_name = None;check_define = None;constraint_name = None;cons_type = None
                    # 查找默认值
                    if 'DEFAULT' in line:
                        start_index = line.find('DEFAULT') + len('DEFAULT')
                        end_index = line.find((' 'or','), start_index+1)
                        col_default = line[start_index:end_index].strip()
                    columns_info.append({
                        'col_name': column_name,'pk':pk,
                        'unique_name':unique_name,'unique_complex':unique_complex,
                        'check_name':check_name,'check_define':check_define,
                        'col_default': col_default,
                        'index_name':col_index_name,'index_type': index_type, 'index_complex': index_complex
                    })
            if db_type=='ob_oracle':
                tmp=lines[col_num+1:]
            else:tmp=lines[col_num+2:]
            for line in tmp:# 查找索引名
                unique_name=None
                #col_default = None;col_index_name = None;index_contain = None;check_define = None;constraint_name = None;cons_type = None
                if 'CREATE' in line and 'INDEX' in line and table_name.upper() in line:
                    index_type=re.findall(r'(?<=CREATE).*?(?=INDEX)',line)[0].replace(' ','')
                    #index_name=re.findall(r'(?<=INDEX\s).*?(?=\sON|on\s)',line)[0].replace(' ','')
                    index_name=re.sub(f'\.|\s|"|{schema}','',re.findall(r'(?<=INDEX\s).*?(?=\sON|on\s)',line)[0])
                    x=re.findall(r"\((.*?)\)", line)
                    if not x:continue
                    else:x=x[0]
                    if len(x.split(','))==1:
                        col_name= re.sub('"|\s','',x);index_complex=None
                    elif len(x.split(','))>=1:
                        col_name=re.sub('"|\s','',x.split(',')[0]);index_complex=x
                    for d in columns_info:
                        if d['col_name']==col_name:
                            d['index_type']=index_type;d['index_name']=index_name;d['index_complex']=index_complex;break
                elif 'PRIMARY' in line:
                    x = re.findall(r"\((.*?)\)", line)[0].replace('"','')
                    if len(x.split(','))==1:
                        col_name= re.sub('"','',x);pk='YES'
                    elif len(x.split(','))>=1:
                        col_name=re.sub('"','',x.split(',')[0]);pk=x
                    for d in columns_info:
                        if d['col_name']==col_name:
                            d['pk']=pk;break
            for line in tmp:
                if 'CONSTRAINT' in line:
                    x=re.findall(r'\"\s(.*?)\s\(',line)
                    if x[0].startswith('UNIQUE'):
                        unique_name=re.findall(r'(?<=CONSTRAINT\s\").*?(?=\"\sUNIQUE)',line)[0]
                        temp_n=re.sub('\"','',re.findall(r'\s\(\"(.*?)\"\)',line)[0])
                        if len(temp_n.split(','))==1:
                            col_name=temp_n;unique_complex=None
                        else:col_name=temp_n.split(',')[0];unique_complex=temp_n
                    else:continue
                    # elif x[0].startswith('CHECK'):
                    #     check_name=re.findall(r'(?<=CONSTRAINT\s\").*?(?=\"\sCHECK)',line)[0]
                    #     check_define=re.findall(r'\s\((.*?)\)\s',line)[0]
                    #     if '"' in check_define:
                    #         col_name=re.search(r'\"(.*?)\"',check_define).group(0).replace('"','')
                    #     else:col_name=columns_info[0]['col_name']
                    for d in columns_info:
                        if d['col_name']==col_name:
                            if unique_name and d['index_name']:
                                if unique_name!=d['index_name'] and unique_name not in d['index_name']:
                                    d['unique_name']=unique_name;d['unique_complex']=unique_complex;break
                                else:d['index_type']=None;d['index_name']=None;d['index_complex']=None;d['unique_name']=unique_name;d['unique_complex']=unique_complex;break
                            elif unique_name:
                                d['unique_name']=unique_name;d['unique_complex']=unique_complex;break
                elif 'UNIQUE' in line and not any(x in line for x in ['CONSTRAINT','INDEX']):
                    temp_n = re.sub('\"', '', re.findall(r'\s\(\"(.*?)\"\)', line)[0])
                    if len(temp_n.split(',')) == 1:
                        col_name = temp_n;unique_complex = None
                    else:
                        col_name = temp_n.split(',')[0];unique_complex = temp_n
                    for d in columns_info:
                        if d['col_name']==col_name:
                            if d['index_name'] and not d['pk']:d['unique_name']=d['index_name'];d['unique_complex']=unique_complex;d['index_type']=None;d['index_name']=None;d['index_complex']=None;break
                            elif d['pk']:break
                            else:d['unique_name']='无名约束';break
            n=k=0
            for line in ck_result:
                if 'NULL' not in line[1]:
                    for j,x in enumerate(columns_info):
                        if x['col_name'] in line[1].upper():n+=1;k=j
                        else:continue
                    if n>1:
                        columns_info[0]['check_name']+='LvTB'+line[0];columns_info[0]['check_define']=line[1];n=k=0
                    else:columns_info[k]['check_name']=line[0];columns_info[k]['check_define']=line[1];n=k=0
                    # for d in columns_info:
                    #     if d
            pass
                    #max(elements, key=lambda x: count.get(x, 0))
                    # 将列信息添加到结果列表中
        elif db_type == 'sqlserver':
            columns_info=self.src_cons_ana(table_name,col_num,n)
        elif db_type == 'clickhouse':
            cursor.execute(f"SHOW CREATE TABLE {table_name}")#大小写敏感
            create_table_result = cursor.fetchall()[0][0]
            cursor.close()
            #create_table_statement = create_table_result[1]
            # 解析创建语句，获取列信息
            lines = create_table_result.split('\n')
            for line in lines[2:col_num + 2]:  # 跳过表名和结束括号
                line = line.strip()
                if line.startswith('`'):  # 列名行
                    parts = line.split(' ')
                    column_name = parts[0].strip('`')
                    col_default = None
                    col_index_name = None
                    check_define = None
                    pk = None
                    unique_name = None
                    unique_complex = None
                    check_name = None
                    index_type = None
                    index_complex = None

                    # 查找默认值
                    if 'DEFAULT' in line:
                        start_index = line.find('DEFAULT') + len('DEFAULT')
                        end_index = line.find(' ' or ',', start_index + 1)
                        col_default = line[start_index:end_index].strip()
                    columns_info.append({
                        'col_name': column_name, 'pk': pk,
                        'unique_name': unique_name, 'unique_complex': unique_complex,
                        'check_name': check_name, 'check_define': check_define,
                        'col_default': col_default,
                        'index_name': col_index_name, 'index_type': index_type, 'index_complex': index_complex
                    })
                    # 查找索引名
            for line in lines[col_num + 3:]:#暂时仅支持主键

                if 'PRIMARY' in line:
                    pk = 'PRIMARY'
                    pkn = re.findall(r"PRIMARY\sKEY\s(.*?)$", line)[0]
                    if '(' in pkn:
                        a = pkn;pkn = pkn.strip('()').split(',')[0]
                    else:
                        a = 'YES'
                    for d in columns_info:
                        fuk = d['col_name']
                        if d['col_name'] == pkn: d['pk'] = a.strip('()');d['index_name'] = pk;d['index_type'] = 'UNIQUE';break
        elif db_type == 'dm':#sysdba权限
            schema=config2.get(dbname,'databasename')
            con_s=config.get('dm','cons').format(tbname=table_name.upper(),schema=schema)
            def_s=config.get('dm','def').format(tbname=table_name.upper(),schema=schema)
            print(def_s)
            cursor.execute(def_s)
            #comment_s=f'''SELECT a.attname,col_description(a.attrelid,a.attnum) FROM pg_class as c,pg_attribute as a where a.attrelid = c.oid and a.attnum>0 and c.relname= '{table_name.lower()}' '''
            def_result = cursor.fetchall()
            print(con_s)
            cursor.execute(con_s)
            cos_result=cursor.fetchall()[0][0].split('\n')[2+col_num:]
            cursor.close()
            temp_col=[]
            tmp_name = []
            tmp_idx=[]
            for item in def_result:#不可能有同名索引
                temp=[]
                if item[0] not in tmp_name and item[2] and  'INDEX3355' not in item[2] and item[2] not in tmp_idx :
                    tmp_name.append(item[0])
                    tmp_idx.append(item[2])
                    for i in item[:3]:
                        temp.append(i)
                        #if not i:temp.append(None)
                    #column_name=item[0];col_default=item[1];index_name=item[2]
                    if item[-2] and  'NORMAL' not in item[-2]:temp.append(item[-2]+'ed，'+item[-1])
                        #index_type=item[-2]+'ed，'+item[-1]
                    elif item[-2]:temp.append('nonclustered，'+item[-1])
                    else:temp.append(None)
                elif  item[0] not in tmp_name :
                    #column_name=item[0];col_default=item[1];index_name=None;index_type=None
                    tmp_name.append(item[0])
                    for j in item[:2]:
                        temp.append(j)
                    temp.append(None);temp.append(None)
                else:
                    #tmp_name.append(item[0])
                    for k in temp_col:
                        if item[0] in k and item[2] and  'INDEX3355' not in item[2]:
                            if k[2]:k[2]+=item[2];k[3]+=item[4]
                            else:
                                k[2]=item[2];k[3]=item[4]

                        else:continue

                if temp :temp_col.append(temp)
                else:pass
            for h in temp_col:
                if h[0]:column_name=h[0]
                else:column_name=None
                if h[1]:col_default=h[1]
                else:col_default = None
                if h[2]:index_name=h[2]
                else:index_name=None
                if h[3]:index_type=h[3]
                else:index_type=None
                columns_info.append({
                'col_name': column_name, 'pk': None,
                'unique_name': None, 'unique_complex': None,
                'check_name': None, 'check_define': None,
                'col_default': col_default,
                'index_name': index_name, 'index_type': index_type, 'index_complex': None,'comment':None
            })
            for line in cos_result:
                if 'PRIMARY' in line:
                    pk = 'PRIMARY'
                    pkn = re.findall(r"\((.*?)\)", line)[0].replace('"', '')
                    if len(pkn.split(',')) == 1:
                        a = 'YES'
                    else:
                        a = pkn;pkn = pkn.split(',')[0]
                    for d in columns_info:
                        fuk = d['col_name']
                        if d['col_name'] == pkn: d['pk'] = a;d['index_name'] = pk;break
                elif 'UNIQUE' in line:
                    cosn = re.sub('\"','',re.findall(r"\(\"(.*?)\"\)", line)[0])
                    if 'CONSTRAINT' in line:
                        cons_name=re.findall('(?<=CONSTRAINT\s\").*?(?=\"\sUNIQUE)',line)[0]
                    else:cons_name='nonname'
                    if len(cosn.split(',')) == 1:
                        na = re.sub('"', '', cosn);complex = None
                    else:
                        nc=cosn.split(',');na = nc[0];complex = cosn
                    #cons_name = re.search(r"(.*?)", line).group(0)
                    for d in columns_info:
                        if d['col_name'] ==na:
                            #if 'UNIQUE' in line:
                            d['unique_name'] = cons_name;d['unique_complex'] = complex;break
                        # elif d['col_name'] in nc[1:]:
                        #     d['index_name']=None;d['index_type'] = None
                        else:
                            pass
                elif 'CHECK' in line:
                    # cos = re.findall(r'\"\s(.*?)\s\(', line)
                    if 'CONSTRAINT' in line:
                        check_name=re.findall('(?<=CONSTRAINT\s\").*?(?=\"\sCHECK)',line)[0]
                    else:check_name = 'nonname'
                    index_define_font = re.findall('\((.*?)\)', line)[0].replace(')' or '(', '')
                    index_define = re.sub('`|\(|\s', '', index_define_font)
                    name = re.sub('\(|\)|>|<|\s|\"','',re.findall('CHECK(.*?)=',line)[0])
                    for d in columns_info:
                        if d['col_name'] == name:
                            d['check_name'] = check_name;d['check_define'] = index_define;break
                else:
                    pass
        #elasticsearch没有任何约束、对象概念
        elif db_type == 'elastic':
            for item in list(self.esl.keys()):
                columns_info.append({
                'col_name': f"{item}", 'pk': None,
                'unique_name': None, 'unique_complex': None,
                'check_name': None, 'check_define': None,
                'col_default': None,
                'index_name': None, 'index_type': None, 'index_complex': None,'comment':None})
        #hbase需要按源库排序，直接复制
        elif db_type == 'hive':
            cursor.execute(config.get('hive','col_02').format(dbname=config2.get('hive','databasename'),tbname=table_name))
            col=cursor.fetchall()
            pri=self.tgt_pri[f'{table_name.lower()}']
            for item in col:
                if item[0] in pri and item[0]==pri[0]:
                    if len(pri)>1:pk=re.sub("\[|]|\'",'',str(pri))
                    else:pk='YES'
                else:pk=None
                columns_info.append({
                'col_name': f"{item[0]}", 'pk': pk,
                'unique_name': None, 'unique_complex': None,
                'check_name': None, 'check_define': None,
                'col_default': None,
                'index_name': None, 'index_type': None, 'index_complex': pk,'comment':None})
        elif db_type == 'db2':
            # 查询PostgreSQL表的创建语句
            schema = config2.get('db2', 'username').upper()
            pattern = r"^[^!=+\-*/%<>&|^]+"
            def_s=config.get('db2','def').format(schema=schema,tbname=table_name)
            con_s=config.get('db2','cons').format(schema=schema,tbname=table_name)
            uni_s=config.get('db2','uni').format(schema=schema,tbname=table_name)
            ind_s=config.get('db2','indx').format(schema=schema,tbname=table_name)
            cursor.execute(def_s)
            # comment_s=f'''SELECT a.attname,col_description(a.attrelid,a.attnum) FROM pg_class as c,pg_attribute as a where a.attrelid = c.oid and a.attnum>0 and c.relname= '{table_name.lower()}' '''
            def_result = cursor.fetchall()
            cursor.execute(con_s)
            cos_result = cursor.fetchall()
            cursor.execute(uni_s)
            uni_result= cursor.fetchall()
            cursor.execute(ind_s)
            ind_result = cursor.fetchall()
            cursor.close()
            for item in def_result:
                column_name = item[0]
                if not item[1]:
                    col_default = item[1]
                else:
                    col_default = item[1].split(':')[0]
                columns_info.append({
                    'col_name': column_name, 'pk': None,
                    'unique_name': None, 'unique_complex': None,
                    'check_name': None, 'check_define': None,
                    'col_default': col_default,
                    'index_name': None, 'index_type': None, 'index_complex': None, 'comment': None
                })
            for line in cos_result:
                a = line[0];b = line[1];c=line[2]
                if c not in ('C','P','U'):
                    index_type = line[2]
                    if index_type:
                        index_type = index_type[0]
                    else:
                        index_type = None
                    index_name = line[0]
                    inno = line[1]
                    if line[3] == 1:
                        inno = inno;index_complex = None
                    else:
                        index_complex = inno;inno = inno.split('+')[1]
                    for d in columns_info:
                        if d['col_name'] == inno:
                            d['index_name'] = index_name;d['index_type'] = index_type;d['index_complex'] = index_complex
                            break
                elif c =='U':
                    unno = line[1]
                    if re.split(r'\+|-',unno).__len__() == 2:
                        un_complex = None
                    else:
                        un_complex = unno;unno = unno.split('+')[1]
                    for d in columns_info:
                        if d['col_name'] == unno:
                            d['unique_name'] = a;d['unique_complex'] = un_complex;break
                elif c=='C':
                    # a = re.sub('::text', '', re.findall(r'\s\(\((.*?\))\)',a)[0])
                    ch_define = line[1]
                    #unno_befor = re.match(pattern,ch_define)
                    unno = re.match(pattern,ch_define).group(0)
                    for d in columns_info:
                        if d['col_name'] == unno:
                            d['check_name'] = a
                            d['check_define'] = ch_define
                            break
                elif c=='P':
                    pkno = line[1]
                    if re.split(r'\+|-',pkno).__len__() == 2:
                        pkno=re.sub('\+','',pkno)
                        pk = 'YES'
                    else:
                        pk = pkno;pkno = re.split(r'\+|-',pkno)[1]
                    for d in columns_info:
                        if d['col_name'] == pkno:
                            d['pk'] = pk;break
            for line in uni_result:#主键-唯一-？索引
                while(line[2]==1):
                    for d in columns_info:
                        if d['col_name'] == line[1]:
                            d['index_name'] = None;d['index_type'] = None;d['index_complex']=None;d['unique_name'] = line[0]
                            if d['pk']:
                                d['unique_name']=None;d['unique_complex']=None
                            break
                    break
            for line in ind_result:
                tmp=re.split(r'\+|-',line[2])

                if len(tmp)!=2:
                    complex=tmp
                else:complex=None
                name=tmp[1]
                for d in columns_info:
                    if d['col_name'] == name:

                        if d['unique_name'] and uni_result:
                            pass
                        elif d['pk']:
                            pass
                        else:d['unique_name']=None;d['unique_complex']=None;d['index_name']=line[0];d['index_type']=line[3];d['index_complex']=complex

            pass
        end = stime.monotonic()
        duration = end - start
        print(f"{n}:{table_name}列信息获取耗时: {duration * 1000:.2f}毫秒")
        return columns_info
    def cons_compare(self,tgt,src):#字段为单位追加至self.bfe
        col_result=[]
        tab_cons=[]
        #diff_cons= {'pri':'','multipri':'','unique':''}#记录唯一标识列
        final_diff_cons=[src.__len__()]
        diff_cons={}
        for s,t in zip(src,tgt):
            #print(len(src),len(tgt))
#pk 1列
            if s['pk'] and t['pk']:
                if s['pk'] == t['pk'] and s['pk']=='YES':
                    diff_cons['pri']=s['col_name']
                    col_result.append('YES')
                elif re.sub('\(\)|\s','',s['pk']).lower() == re.sub('\(\)|\s','',t['pk']).lower() and s['pk']!='YES':
                    diff_cons['multipri']=s['pk']
                    col_result.append('YES,复合主键%s'%s['pk'])
                else:col_result.append('NO! 主键不正常，源%s备%s'%(s['pk'],t['pk']))
            elif not s['pk'] and not t['pk']:
                col_result.append(None)
            elif s['pk'] and not t['pk'] or  not s['pk'] and t['pk']:
                col_result.append('NO! 主键不正常，源%s备%s'%(s['pk'],t['pk']))
#default 1列
            if s['col_default'] and t['col_default']:
                if s['col_default'] == re.sub('\(|\)','',t['col_default']):col_result.append('YES! %s'%t['col_default'])
                else:col_result.append('默认值不同源备%s%s'%(s['col_default'],t['col_default']))
            elif not s['col_default'] and not t['col_default']:col_result.append(None)
            elif not s['col_default'] and t['col_default']=='NULL':
                col_result.append('NULL')
            else:col_result.append('NO!源%s备%s'%(s['col_default'],t['col_default']))
#unique 2列
            if s['unique_name'] and t['unique_name'] :
                if s['unique_complex'] and t['unique_complex'] and re.sub('\(|\)|\"|`|\s|\+|,','',s['unique_complex']).lower() == re.sub('\(|\)|\"|`|\s|\+|,','',t['unique_complex']).lower() :
                    diff_cons['unique'] = s['unique_complex']
                    col_result.append('YES! %s'%t['unique_name']);col_result.append('YES! %s'%t['unique_complex'])
                elif not s['unique_complex'] and not t['unique_complex']:
                    col_result.append('YES! %s' % t['unique_name'])
                    col_result.append(None)
                elif  s['unique_name'] and self.conf_tgt=='dm' and t['unique_name']=='nonname':#dm适配无名约束unique
                    col_result.append('YES! %s' % t['unique_name']);col_result.append(None)
                else:
                    col_result.append('YES! %s'%t['unique_name']);col_result.append('NO! %s'%t['unique_complex'])
                    diff_cons['unique'] = s['col_name']
            elif not s['unique_name'] and not t['unique_name']:
                col_result.append(None);col_result.append(None)
            elif not s['unique_name'] and t['unique_name'] and t['index_type'] and s['index_type']:
                col_result.append(None)
                col_result.append(None)
            elif s['unique_name'] and not t['unique_name'] and s['unique_name']==t['index_name']:
                if self.conf_src=='oracle' and self.conf_tgt=='sqlserver':
                    col_result.append('YES! %s' % s['unique_name'])
                    col_result.append(None)
            else:
                col_result.append('NO!源%s备%s' % (s['unique_name'], t['unique_name']));col_result.append(None)
#check 3列 check匹配情况 源端checkdefine 备端check情况
            if s['check_name'] and t['check_name']:
                if s['check_name'][0:-10].lower() == t['check_name'][0:-10].lower() or (s['check_name'] and self.conf_tgt=='dm' and t['check_name']=='nonname'):
                    if re.sub('\(|\)|\[|\]|\s|\`|\"|_utf8mb4','',t['check_define']).lower() ==re.sub('\(|\)|\[|\]|\s|\`|\"|_utf8mb4','',s['check_define']).lower():
                        col_result.append('YES!');col_result.append('YES! %s'%(s['check_define']));col_result.append('YES! %s'%(s['check_define']))
                    else:col_result.append('YES!');col_result.append('YES! %s'%(s['check_define']));col_result.append('NO! check定义不同%s'%t['check_define']);
                #elif s['check_name'] and self.conf_tgt=='dm' and t['check_name']=='nonname':
                else:col_result.append('YES!');col_result.append('YES! %s'%(s['check_define']));col_result.append('NO! check名称不对')
            elif not s['check_name'] and not t['check_name']:col_result.append(None);col_result.append(None);col_result.append(None)
            else:col_result.append('NO!源%s备%s'%(s['check_name'],t['check_name']));col_result.append('YES! %s'%(s['check_define']));col_result.append(None)
#index 2列
            if not s['index_name'] and not t['index_name']:
                col_result.append(None);col_result.append(None)
            elif s['index_name'] and t['index_name']:
                col_result.append(f"YES!{s['index_name']}")
                if  not s['index_type'] or not t['index_type']:col_result.append('YES! 普通索引')
                elif s['pk'] and t['index_name']:col_result.append('YES! 主键相关索引')
                elif s['index_type'] and t['index_type'] and re.search(t['index_type'].lower(),s['index_type'],flags=re.IGNORECASE):
                    col_result.append('YES! 索引类型正确，源%s备%s'%(s['index_type'],t['index_type']))
                else:col_result.append('NO! 索引类型错误，源%s备%s' % (s['index_type'], t['index_type']))
            else:
                col_result.append('NO! 源备索引不一致源%s备%s'%(s['index_name'],t['index_name']))
                col_result.append(None)

            #print(col_result)
            tab_cons.append(col_result)
            col_result=[]

        final_diff_cons.append(diff_cons)
        return final_diff_cons, tab_cons
        #print(self.col_result)
    def row_contain(self,src,tgt,tbname,tbcol,tgcol,order_col,ora_search_column=None):#行数、内容比较
        connector_types = {'sqlserver', 'mysql', 'tdsql', 'tidb', 'hbase'}
        if self._uses_connector_backend() and self.conf_src in connector_types and self.conf_tgt in connector_types:
            src_connector = self.table_src_connector
            tgt_connector = self.table_tgt_connector
            own_src = src_connector is None
            own_tgt = tgt_connector is None
            if own_src:
                src_connector = self._acquire_compare_connector('src')
            if own_tgt:
                tgt_connector = self._acquire_compare_connector('tgt')
            try:
                order_columns = None if self.conf_tgt == 'hbase' else self._resolve_order_columns(order_col)
                src_rows = []
                for batch in src_connector.get_row(
                    tbname,
                    tbcol,
                    batch_size=getattr(self, 'batch_size', 1000),
                    schema=self._schema_name('src'),
                    order_by=order_columns,
                ):
                    src_rows.extend(tuple(row) for row in batch)

                tgt_rows = []
                for batch in tgt_connector.get_row(
                    self._table_name_for_role(tbname, 'tgt'),
                    tgcol,
                    batch_size=getattr(self, 'batch_size', 1000),
                    schema=self._schema_name('tgt'),
                    order_by=order_columns,
                ):
                    tgt_rows.extend(tuple(row) for row in batch)

                err = []
                src_row_count = len(src_rows)
                tgt_row_count = len(tgt_rows)
                src_col_count = len(src_rows[0]) if src_rows else len(tbcol)
                tgt_col_count = len(tgt_rows[0]) if tgt_rows else len(tgcol)
                if src_row_count != 0 and tgt_row_count != 0 and src_row_count == tgt_row_count and src_col_count == tgt_col_count:
                    message = f"Y row_count_equal src={src_row_count} tgt={tgt_row_count}"
                    print(message)
                    self.bfe.append(message)
                    err = standard.hash_compare_rows(
                        src_rows,
                        tgt_rows,
                        [colType['type'] for colType in tbcol],
                        tbname,
                        src_col_count=src_col_count,
                        tgt_col_count=tgt_col_count,
                        ignore_case=getattr(self, 'tgt_uppercase', False),
                    )['errors']
                elif src_row_count != 0 and tgt_row_count != 0 and src_row_count != tgt_row_count and src_col_count == tgt_col_count:
                    message = f"NO! row_count_diff src={src_row_count} tgt={tgt_row_count}"
                    print(message)
                    self.bfe.append(message)
                elif src_row_count != 0 and tgt_row_count != 0 and src_col_count != tgt_col_count:
                    message = f"NO! column_count_diff src={src_col_count} tgt={tgt_col_count}"
                    print(message)
                    self.bfe.append(message)
                elif tgt_row_count == 0 and src_row_count != 0:
                    message = f"NO! target_empty src_rows={src_row_count}"
                    print(message)
                    self.bfe.append(message)
                else:
                    print('~ empty_table')
                    self.bfe.append('~ empty_table')
                print(err)
                return err
            finally:
                if own_src:
                    self._release_compare_connector('src', src_connector)
                if own_tgt:
                    self._release_compare_connector('tgt', tgt_connector)
        src_conn = src or self.conn_src
        tgt_conn = tgt or self.conn_tgt
        src_cur=src_conn.cursor()
        #datetime_tuple=(datetime.time,datetime.datetime)
        if self.conf_tgt not in ('elastic','hdfs','hbase'):
            tgt_cur = tgt_conn.cursor()
        else:
            tgt_cur=None
        err=[]
        pattern = r'[\[\]（{ }）()【】{}，,￥$\'\"“”‘’]'
        if order_col[1]:
            # odsql=' order by '+','.join(str(v) for v in dict.fromkeys(order_col[1].values()))
            odsql='  order by  '+ ','.join(
                f'"{col.strip()}"'
                for v in order_col[1].values() or []
                for col in str(v).split(',')
                if col.strip())
        else:#diffcons为空，无主键无索引
            odsql=''
        if self.conf_src=='sqlserver':
            sql=self.mss_spc(tbcol,tbname)
        elif self.conf_src=='postgre':
            sql=f'select * from {tbname}'
        elif self.conf_src=='oracle':
            sql=config.get('oracle','row').format(columns=ora_search_column or self.ora_search_cloumn,tbname=tbname)
            odsql=odsql.upper()
        else:
            sql=f'select * from {tbname}'
        print(sql+odsql)
        src_cur.execute(sql+odsql)
        ccc=tuple(map(tuple, src_cur.fetchall()))
        s=pandas.DataFrame(ccc)
        row=[]
        #print(sql)

        tgt_tbname = self._table_name_for_role(tbname, 'tgt')
        if self.conf_tgt=='sqlserver':
            a=self.mss_spc(tgcol,tgt_tbname)
        elif self.conf_tgt=='mysql':
            a=f"""select * from `{tgt_tbname if getattr(self, 'tgt_uppercase', False) else tbname.lower()}`"""
        elif self.conf_tgt=='oracle':a=f'''select * from "{tgt_tbname.upper()}"''';odsql=odsql.upper()
        elif self.conf_tgt=='elastic':
            host = config2.get('elastic', 'host')
            port = config2.get('elastic', 'port')
            schema = config2.get('elastic', 'schema')  # 赋值schema
            tgt_cur=requests.get(f"http://{host}:{port}/i2import_{schema.lower()}_{tgt_tbname.lower()}/_search").json()
        elif self.tgt_db_t not in ('mysql','dm','hive','hdfs','hbase') and not self.tgt_db_t.endswith('gbk'):
            a='%s' % config.get('sqlserver', 'row') + " " + tgt_tbname
        elif self.tgt_db_t.endswith('gbk'):
            b = "set client_encoding to 'GBK';"
            tgt_cur.execute(b)
            a = '%s' % config.get('sqlserver', 'row') + " " + tgt_tbname
        elif self.conf_tgt=='dm':
            a=config.get('dm','select').format(schema=config2.get('dm','databasename'),tbname=tgt_tbname.upper())
        elif self.conf_tgt=='hive':
            ob=','.join(self.tgt_pri[f'{tgt_tbname.lower()}'])
            a=config.get('hive','row').format(dbname=config2.get('hive','databasename'),tbname=tgt_tbname,ob=ob)
        elif self.conf_tgt=='hdfs':
            hdfs_tbname=f'//{tgt_tbname}//{tgt_tbname}.parquet'
            Pyarrow_tname=config2.get('hdfs','local_dir')+config2.get('hdfs','schema')+hdfs_tbname
        elif self.conf_tgt=='hbase':
            hc=self.conn_tgt_cur
        #else : a='%s' % config.get('sqlserver', 'row') + " [" +f'{tbname.lower()}' + "] " + 'order by 1 asc'
        else: a=f"""select * from {tgt_tbname if getattr(self, 'tgt_uppercase', False) else tbname.lower()} """
        if self.conf_tgt not in ('elastic','hdfs','sqlserver','hbase'):
            print(a+odsql)
            tgt_cur.execute(a+odsql)
            xxx = tgt_cur.fetchall()
            t = pandas.DataFrame(xxx)
        elif self.conf_tgt =='elastic':
            t=self.pretreatment_es(tgt_cur['hits']['hits'])
        elif self.conf_tgt == 'sqlserver':
            print(a + odsql)
            tgt_cur.execute(a + odsql)
            xxx=tuple(map(tuple, tgt_cur.fetchall()))
            t = pandas.DataFrame(xxx)
        elif self.conf_tgt=='hbase':
            #hc传tbcol，按照源库顺序扫描，直接复制到dataframe，返回dataframe
            t=hc.scan_one_tb(tgt_tbname,tbcol)
        else:#如果是hdfs的parquet文件，先预处理成dataframe
            t=self.pretreatment_hdfs(Pyarrow_tname)
            pass

        if len(s)!=0 and len(t)!=0 and s.shape[0]==t.shape[0] and s.shape[1]==t.shape[1]:
            print('Y 行数相同,源%d行,备%d行'%(len(s[0]),len(t[0]))) and self.bfe.append('Y 行数相同,源%d行,备%d行'%(len(s[0]),len(t[0])))
            if not odsql or self.conf_tgt=='hbase':
                print('无主键表')
                tgt_sortable_cols = [col for col in t.columns if t[col].dtype != "object"]
                src_sortable_cols = [col for col in s.columns if s[col].dtype != "object"]
                if tgt_sortable_cols:
                    t = t.sort_values(by=tgt_sortable_cols, ascending=True)
                    s = s.sort_values(by=src_sortable_cols,ascending=True)
            elif  self.conf_tgt=='hbase':
                pass
            
            #hash比较
            err=standard.hash_compare(s,t,[colType['type'] for colType in tbcol],tbname,ignore_case=getattr(self, 'tgt_uppercase', False))['errors']
        elif len(s)!=0 and len(t)!=0 and s.shape[0]!=t.shape[0] and s.shape[1]==t.shape[1]:
            print('NO! 行数不同,源%d行,备%d行'%(s.shape[0],t.shape[0]));self.bfe.append('NO! 行数不同,源%d行,备%d行'%(len(s[0]),len(t[0])))
        elif len(s)!=0 and len(t)!=0 and len(s[0])==len(t[0]) and s.shape[1]!=t.shape[1]:
            print('NO! 列数不同,源%d列,备%d列' % (s.shape[1], t.shape[1]));self.bfe.append('NO! 列数不同,源%d列,备%d列' % (s.shape[1],s.shape[1]))
        elif len(t)==0 and len(s)!=0:
            print('\033[0;31m NO! \033[0m \t  行数不同,源%d行,备端空表'%(len(s[0])));self.bfe.append('\033[0;31m NO! \033[0m \t  行数不同,源%d行,备端空表'%(len(s[0])))
        else :print('~ 空表') ;self.bfe.append('~ 空表')
        #print(s,'\n',t,s.equals(t))
        print(err)
        return err
    def mss_spc(self,tgcol,tbname):
        f=[];i=0
        cur=self.conn_src.cursor()
        for col in tgcol:
            if col['type'] in self.type_cache:
                if 'binary' in col['col_name']:
                    tmpa = '''convert(varchar(max),{col_name},1)'''
                    f.append(f"{tmpa}".format(col_name=col['col_name']));i += 1
                else:
                    f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i+=1
            elif col['type'].startswith('varchar'):
                self.type_cache[col['type']]='''CONVERT(nvarchar(max),"{col_name}")'''
                f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i+=1
            elif col['type'].startswith('char') or col['type'].startswith('float') or col['type'].startswith('real'):#处理科学计数
                    self.type_cache[col['type']] ='''CONVERT(nvarchar(max),"{col_name}")'''
                    f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i+=1
            elif col['type'].startswith('text'):
                    self.type_cache[col['type']] ='''CONVERT(ntext,"{col_name}")'''
                    f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i+=1
            elif col['type'].startswith('geo'):
                    self.type_cache[col['type']]='"{col_name}".STAsText()'
                    f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i+=1
            elif col['type'].startswith('bigint'):
                    self.type_cache[col['type']]='convert(decimal(38,0),{col_name})'
                    f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i+=1
            elif col['type'].startswith('sql_variant') or col['type'].startswith('hierarchyid'):
                if 'binary' in col['col_name']:
                    tmpc = '''convert(varchar(max),{col_name},1)'''
                    f.append(f"{tmpc}".format(col_name=col['col_name']));i += 1
                else:
                    self.type_cache[col['type']] = '''cast("{col_name}" as nvarchar(max))'''
                    f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i+=1
            elif col['type'] in ('datetime2','datetimeoffset','time'):
                    self.type_cache[col['type']] = '''CONVERT(nvarchar(max),"{col_name}")'''
                    f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i+=1
            # elif col['type'].startswith('sql_variant'):
            #     self.type_cache[col['type']] = '''convert(nvarchar(max),"{col_name}",120)'''
            #     f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i += 1
            #     #cur.excute('')
            #     #pass
            elif 'offset' in col['type']:
                self.type_cache[col['type']] = '''CONVERT(varchar(800),"{col_name}")'''
                f.append(f"{self.type_cache[col['type']]}".format(col_name=col['col_name']));i += 1
            elif col['col_name'].startswith('**'):pass#timestamp类型不比较
            elif col['type'].startswith('timestamp'):
                self.type_cache[col['type']] = "'timestamp不参与比较'"
                f.append(f"{self.type_cache[col['type']]}");i += 1
            else:
                    f.append(rf'''"{col['col_name']}"''')
        if len(f)==1 or not f:
            sql=f'''select * from [{tbname}]'''
        elif i==0:
            sql = f'''select * from [{tbname}]'''
        else:
            col_str=','.join(f)
            sql=f'''select {col_str} from [{tbname}]'''
        return sql
    def process_datetime2(self,element):
        # 检查元素类型是否为 datetime64[ns]（Pandas datetime 类型）
        if element and isinstance(element, (datetime.datetime,datetime.date,datetime.time)):
            # 将 datetime 转换为字符串并返回
            return element.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:# 对于其他类型的元素，不做处理
            return element
    def map_compare(self,srcd,tgtd):
        error=[];c=0
        if srcd['null'].upper()[0]!=tgtd['null'].upper()[0]:
            a='NO! %s列源备库非空状态不一致，源库为%s,备库为%s'%(srcd['col_name'],srcd['null'],tgtd['null'])
            #self.bfe.append('NO! %s列源备库非空状态不一致，源库为%s,备库为%s'%(srcd['col_name'],srcd['null'],tgtd['null']))
            print(a)
            self.bfe[-1]=self.bfe[-1]+a
        for j in range(len(self.data.index.values)):
            b=re.sub(r"\(.*?\)",'',self.data.iloc[j,self.src_index]).lower()
            q=srcd['type']
            if b==re.sub('\(.*?\)','',srcd['type']).lower():
                src_col_temp=self.data.iloc[j,self.src_index]
                tgt_col_temp=self.data.iloc[j,self.tgt_index]#类型
                c=1
                if self.src_db_t in self.oracle and src_col_temp == 'number(p,s)':
                    #print(self.oracle_num(srcd, tgtd))
                    tgt_col_temp=self.oracle_num(srcd, tgtd)
                if tgt_col_temp.split("，").__len__()<=1 :
                    self.map_compare_simple(srcd,tgtd,tgt_col_temp)
                    break
                elif tgt_col_temp.split("，").__len__() >1:#特殊复杂情况情况
                    #print(tgt_col_temp.split("，"),type(tgt_col_temp.split("，")))
                    #tgt_col_temp=tgt_col_temp.split("，")
                    for i in range(len(self.complex_map.index.values)):
                        #print(self.complex_map)
                        if re.sub(r"\(.*?\)",'',self.complex_map.iloc[i,self.src_index])==b:
                            rule_param=self.complex_map.iloc[i,self.tgt_index]
                    if isinstance(rule_param,int) or isinstance(rule_param,numpy.int64):#只需判断一次
                        if (srcd['num_pre'] and srcd['num_pre']>=int(rule_param))or(srcd['len'] and srcd['len']>=int(rule_param)
                        or srcd['time_sh'] and srcd ['time_sh']>int(rule_param) or srcd['len']==-1):
                            s1 = tgt_col_temp.split("，")[-1]
                            self.map_compare_simple(srcd,tgtd,s1)
                        else:
                            s2 = tgt_col_temp.split("，")[0]
                            self.map_compare_simple(srcd,tgtd,s2)
                    elif len(rule_param.split("，"))>=1:#需判断3次
                        rule_param=rule_param.split("，")
                        if (srcd['num_pre'] and srcd['num_pre']>=int(rule_param[-1]))or(srcd['len'] and srcd['len']>=int(rule_param[-1])
                        or srcd['len']==-1):
                            x1=tgt_col_temp.split("，")[-1]
                            self.map_compare_simple(srcd,tgtd,x1)
                        elif (srcd['num_pre'] and srcd['num_pre']<int(rule_param[0]))or(srcd['len'] and srcd['len']<int(rule_param[0])):
                            x2 = tgt_col_temp.split("，")[0]
                            self.map_compare_simple(srcd,tgtd,x2)
                        else:self.map_compare_simple(srcd,tgtd,tgt_col_temp.split("，")[1])
        if c==0:
            print('未在映射表找到类型',srcd)
    def oracle_num(self,srcd,tgtd):#oracle源端numric特殊映射规则
        s=srcd['num_scale']
        p=srcd['num_pre']
        t='int'
        if not s and not p:
            if s == 0:t='int'
            else:t='varchar(100)'
        elif s==0:
            if p<5:t='smallint'
            elif 5<=p<10:t='int'
            elif 10<=p<19:t='bigint'
            elif 19<=p+abs(s)  and (p>65|s>30):t = f'varchar{p+abs(s)+2}'
        elif s>=0:
            if p<s:t=f'decimal({s},{s})'
            elif s<=p<65:t='decimal(p,s)'
        elif not s:
            t='varchar(100)'
        elif s<=0:
            if p+abs(s)<5:t='smallint'
            elif 5<=p+abs(s)<10:t='int'
            elif 10<=p+abs(s)<19:t='bigint'
            elif 19<=p+abs(s)  and (p>65|s>30):t = f'varchar{p+abs(s)+2}'#decimal({p},{s})
        else:t=f'decimal({p},{s})'
        return t
    def pretreatment_es(self,tgt_cur):#预处理es转dataframe
        tmp_list=[]
        #esl=self.esl.copy()
        sort=list(self.esl.keys())
        for i in tgt_cur:
            #sorted(i['_source'], key=lambda item: self.esl[item])
            for j in sort:
                if j in i['_source']:self.esl[f'{j}']=i['_source'][f'{j}']
                else:self.esl[f'{j}']=None
                a_dict=self.esl.copy()
            tmp_list.append(a_dict)
        #print(tmp_list)
        if len(tmp_list) !=0:
            t_sorted=pandas.DataFrame(tmp_list).sort_values(by=sort).reset_index(drop=True)
            t_sorted.columns=list(range(len(a_dict)))
        else:t_sorted=[]
        return t_sorted
    def pretreatment_hdfs(self,Pyarrow_name):#预处理parquet二进制
        table=pq.read_table(Pyarrow_name)
        decode_dict={"ISO-8859-1":'ISO-8859-1',"ascii":'utf-8',"gbk":'gbk',"None":'utf-8',"utf-8":'utf-8'}
        dec_mode=None
        #meta=pq.ParquetFile(Pyarrow_name).metadata
        #print(table.schema)
        columns={}
        for col in table.column_names:
            column_data = table[col]
            # 检查是否为二进制类型
            if pa.types.is_binary(column_data.type) or pa.types.is_large_binary(column_data.type):
                tmp_value=column_data.to_pandas().fillna(b'')
                for value in tmp_value:
                    if len(value) > 0:
                        dec_mode=decode_dict[f"{chardet.detect(value)['encoding']}"]
                        break
                if not dec_mode:dec_mode='utf-8'
                # 对二进制数组进行UTF-8解码
                columns[col] = column_data.to_pandas().apply(lambda x: x.decode(f'{dec_mode}') if x is not None else x)
            else:
                columns[col] = column_data.to_pandas()# 如果不是二进制列，直接转换为 Pandas 格式
        # 将处理后的列按照原顺序合并为 DataFrame
        df = pandas.DataFrame(columns)
        df[df.columns[0]] = df[df.columns[0]].astype(int)
        df.columns=range(len(df.columns))
        df.sort_values(by=0,inplace=True)
        print(df)
        return df
    def no_pri(self,k,xerr,src,tgt):
        np=[]
        src=list(src);tgt=list(tgt)
        err=[]#k=0,源库<备库，否则源库>备库
        if k==0:pass
        elif k==1:
            for i in xerr:
                if i not in self.src_pri:
                    np.append(i)
                else:err.append(i)
        print(f'{np}为无主键表，{err}可能为出错表,注意排查')
        if self.np_value==4:
            a = input('‘D’：删除无主键表，‘E’：删除出错表，‘X’：删除源库所有问题表，any key else quit/pass')
        else:a=6
        if str(a).lower() == 'd' or self.np_value==1:
            if k == 0:
                print('备库多表，暂不处理')
            elif k == 1:
                src[0] = src[0] - len(np)
                for j in np:
                    src[1]= list(filter(lambda table: table["tab_name"].lower() != j, src[1]))
        elif str(a).lower() == 'x' or self.np_value==3:
            if k == 0:
                print('备库多表，暂不处理')
            elif k == 1:
                src[0] = src[0] - len(xerr)
                for j in xerr:
                    src[1]= list(filter(lambda table: table["tab_name"].lower() != j, src[1]))
        elif str(a).lower() == 'e' or self.np_value==2:
            if k == 0:
                print('备库多表，暂不处理')
            elif k == 1:
                src[0] = src[0] - len(err)
                for j in err:
                    src[1]= list(filter(lambda table: table["tab_name"].lower() != j, src[1]))
        else:print(f'输入了{a}，不做任何处理')
        #print(src)
        return src,tgt
    def pre_dict(self,d,y):
        if not d:
            d['tab_col'] = y['tab_col'].copy()
        else:
            bucket = defaultdict(list)
            order = [item['col_name'] for item in y['tab_col']]  # 源表字段顺序
            for item in d['tab_col']:  # 分桶，以字段名为键，分组存储每一行
                bucket[item['col_name']].append(item)
            result = []
            for k in order:
                result.extend(bucket.get(k, []))
            d['tab_col'] = result.copy()
        return d['tab_col']
    def search_dicts(self,dlist,key, value,y):
        self.templist = None
        for d in dlist:
            if key in d and d[key].upper() == value: #and not value.upper().startswith('MSREPL'):
                if not d['tab_col'] or not y['tab_col']:
                    self.templist = d['tab_col']
                    break
                if len(d['tab_col'])==len(y['tab_col']):
                    self.pre_dict(d,y)
                if len(d['tab_col'])<len(y['tab_col']) and self.tgt_db_t!='hbase':
                    c = copy.deepcopy(y['tab_col'][0]);c['col_name'] = '**lost**'
                    for i in range(abs(len(d['tab_col'])-len(y['tab_col']))):
                        d['tab_col'].append(c)
                elif len(d['tab_col'])<=len(y['tab_col']) and self.tgt_db_t=='hbase':
                    if not d['tab_col']:d['tab_col']=y['tab_col'].copy()
                    else:
                        bucket=defaultdict(list)
                        order=[item['col_name'] for item in y['tab_col']]#源表字段顺序
                        for item in d['tab_col']:#分桶，以字段名为键，分组存储每一行
                            bucket[item['col_name']].append(item)
                        result=[]
                        for k in order:
                            result.extend(bucket.get(k, []))
                        d['tab_col']=result.copy()
                else:
                    c = copy.deepcopy(y['tab_col'][0]);c['col_name'] = '**lost**'
                    for i in range(abs(len(d['tab_col'])-len(y['tab_col']))):
                        y['tab_col'].append(c)
                self.templist=d['tab_col']
                break
            else:pass

        return self.templist,y
    def col_compair(self,src,tgt):
        self.ora_search_cloumn=None
        tmp=[]
        if self.conf_tgt=='elastic':
            sort_map={}
            for j in src:
                s=re.sub('\$|@|&|#|\*|\[|]|=|\+|','',j['col_name']).lower()
                if s !='lost':
                    sort_map[f"{s}"]=j['col_num']
                else:sort_map[f"{j['col_name']}"]=j['col_num']
            tgt=sorted(tgt,key=lambda item:sort_map[item['col_name']])#备端是es，预处理排序两侧字段
            self.esl=sort_map
        for i in range(len(src)): #src 所有字段的字典列表
            scol=(src[i])
            tcol=(tgt[i])
            if scol['col_name'].upper()==tcol['col_name'].upper():#列名相同
                self.map_compare(scol,tcol)
            elif scol['col_name'].upper().startswith('MSREPL'):
                continue
            else:
                print('NO! 备端缺失列%s'%scol['col_name'])
                self.bfe.append('NO! 备端缺失列%s'%scol['col_name'])
            if 'interval' not in scol['type'].lower() and 'rowid' not in scol['type'].lower():
                tmp.append(scol['col_name'])
            else:tmp.append(f"cast({scol['col_name']} as varchar(50))")
        self.ora_search_cloumn=(',').join(tmp)
        return self.ora_search_cloumn
    def dump_e(self,dmp):
        dump_excel=dmp
        #a = input('press a to dump infomation to EXCEL right now \t or \t press x exit go model ergodic operator later')
        while True:
            try:
                j=dump_excel.analysis(self.cons_err)
                if j==1:
                    print(f'源备没有同名表，不予打表，程序退出');break
                dump_excel.xlsx(self.src_db_t,self.tgt_db_t)
                break
            except PermissionError as e:
                #b = input('%s\npress c to continue \t or \t press x exit break' % e)
                print(f'{e},大概率后台打开了report.xlsx,10s后重试')
                stime.sleep(5)
                b='c'
                if b == 'c':
                    continue
                elif b == 'x':
                    break
                else:print('重新输入')
    def compare(self,src,tgt,err_handling,if_cpdata,control=None,tgt_uppercase=False,ignore_case=None):
        #源备库设置
        #src='sqlserver';tgt='postgre';err_handling=3;if_cpdata:0 是否内容比较(部分链路不支持)
        self.set_control(control)
        self.tgt_uppercase = tgt_uppercase if ignore_case is None else ignore_case
        self.src_db=src
        self.tgt_db=tgt
        self.src_db_t = config2.get(f'{self.src_db}', 'type')#根据输入的数据库具体名找config中对应的type值，可加try优化报错
        self.tgt_db_t = config2.get(f'{self.tgt_db}', 'type')
        self._reset_compare_context()
        self.bfe_last=[]
        self.col_result=[]
        self.cons_err=[]
        self.isom=[]
        self.np_value=err_handling#1：自动忽略所有无主键，2：忽略所有出错表，3：忽略所有问题表，4：手动选择
        self.map_analysis(compare_file)
        compare_total_start = stime.monotonic()
        # runner 传多少 worker，这里就吃多少；没传时再退回 4。
        concurrency = max(1, int(getattr(self, 'workers', getattr(self, 'max_workers', 4))))
        tmp_tb_list=[]
        schema_start = stime.monotonic()
        src,tgt=self.define_type()
        schema_cost_ms = (stime.monotonic() - schema_start) * 1000
        if src[0]!= tgt[0]:
            xsrc=[]
            xtgt=[]
            xerr=[]
            for nn in src[1]:
                xsrc.append(nn['tab_name'].lower())
            for mm in tgt[1]:
                xtgt.append(mm['tab_name'].lower())
            if src[0]<tgt[0]:
                k=0
                for x in xtgt:
                    if x in xsrc:
                        continue
                    else:xerr.append(x)
            else:
                k=1
                for x in xsrc:
                    if x in xtgt:
                        continue
                    else:xerr.append(x)
            print(f'源备表数量不一致，源端共计 {src[0]}个表，备端共计 {tgt[0]}个表，\n 缺失表单{xerr}')
            self.bfe_last.append(f'源备表数量不一致，源端共计 {src[0]}个表，备端共计 {tgt[0]}个表，\n 缺失表单{xerr}')
            src, tgt = self.no_pri(k, xerr, src, tgt)
        else:
            print('源端共计 %d个表，备端共计 %d个表'%(src[0],tgt[0]))
            self.bfe_last.append('源端共计 %d个表，备端共计 %d个表'%(src[0],tgt[0]))
        def pre_compare(src,tgt):
            for y in src[1]:
                a,y=self.search_dicts(tgt[1],'tab_name',y['tab_name'].upper(),y)#源备表名列表可能乱序，按源端表名匹配
                table_log=[y['tab_name']]
                if a is not None:
                    tmp_tb_list.append((y['tab_name'],a,y['tab_col']))
                else:
                    print('\033[0;34m ！ \033[0m备端查无该表 %s'%y['tab_name'])
                    table_log.append('！备端查无该表 %s'%y['tab_name'])
                    self.bfe_last.append(table_log)
            return tmp_tb_list
##分发前计时，
        tasklist_start = stime.monotonic()
        tasklist=pre_compare(src,tgt)
        tasklist_cost_ms = (stime.monotonic() - tasklist_start) * 1000
        compare_loop_start = stime.monotonic()
        with ThreadPoolExecutor(max_workers=concurrency) as cons_data_tp:
            futures = {}
            next_index = 0
            stop_requested = False
            while next_index < len(tasklist) or futures:#滚动提交，每完成一个任务就提交一个新的，方便控制状态
                if self._check_control('compare调度'):
                    stop_requested = True
                while not stop_requested and next_index < len(tasklist) and len(futures) < concurrency:
                    tab_name, a, tab_col = tasklist[next_index]
                    fut = cons_data_tp.submit(self.cons_data_compare, tab_name, a, tab_col, if_cpdata)
                    futures[fut] = tab_name
                    next_index += 1
                if not futures:
                    break
                done, _ = wait(list(futures.keys()), timeout=0.5, return_when=FIRST_COMPLETED)
                if not done:
                    continue
                for fut in done:
                    futures.pop(fut, None)
                    result = fut.result()
                    self.bfe_last.append(result['messages'])
                    if result['col_result'] is not None:
                        self.col_result.append(result['col_result'])
                    if result['cons_err'] is not None:
                        self.cons_err.append(result['cons_err'])
            if stop_requested:
                #self.bfe_last.append('compare任务已收到停止指令，停止继续提交新表')
                print('compare任务已收到停止指令，停止继续提交新表')
        compare_loop_cost_ms = (stime.monotonic() - compare_loop_start) * 1000
        compare_total_cost_ms = (stime.monotonic() - compare_total_start) * 1000
        print(f'[phase] schema={schema_cost_ms:.2f}ms tasklist={tasklist_cost_ms:.2f}ms compare_loop={compare_loop_cost_ms:.2f}ms total={compare_total_cost_ms:.2f}ms')
        #print(self.bfe_last)
        #打表
        report=json.dumps(self.bfe_last,ensure_ascii=False)
        cons=json.dumps(self.col_result,ensure_ascii=False)
        #err=json.dumps(self.cons_err,ensure_ascii=False)
        #err=json.dumps(self.cons_err)
        with open('resource/test_report/report.txt','w+') as f:
            f.write(report)
        with open('resource/test_report/cons.txt','w+') as f:
            f.write(cons)
        with open('resource/test_report/err.txt','w+',encoding='utf-8') as f:
            f.write(str(self.cons_err))
    def cons_data_compare(self,tab_name,a,tab_col,if_cpdata=0):
            start = stime.monotonic()
            self._reset_compare_context(tab_name)
            table_col_result = None
            table_cons_err = None
            acquired = {'src': False, 'tgt': False}
            broken_roles = set()
            columns_cost_ms = 0.0
            constraints_cost_ms = 0.0
            rows_cost_ms = 0.0
            connector_types = {'sqlserver', 'mysql', 'tdsql', 'tidb', 'hbase'}
            try:
                if self._check_control(f'表 {tab_name}'):
                    return {
                        'messages': list(self.bfe),
                        'col_result': table_col_result,
                        'cons_err': table_cons_err,
                    }
                print(f'表 {tab_name} 开始比对')
                if self._uses_connector_backend():
                    columns_start = stime.monotonic()
                    #并发获取tab_col
                    tab_col, a = self._load_table_columns_pair(tab_name, tab_col, a)
                    columns_cost_ms = (stime.monotonic() - columns_start) * 1000
                    acquired = self._acquire_table_compare_connectors()
                if len(a) == len(tab_col) or (self.tgt_db_t == 'hbase' and len(a) < len(tab_col)):
                    print('\033[0;34m %s \033[0m源备字段数相同，源端字段：%d \t备端字段%d' % (tab_name, len(tab_col), len(a)))
                    self.bfe.append('\033[0;34m %s \033[0m源备字段数相同，源端字段：%d \t备端字段%d' % (tab_name, len(tab_col), len(a)))
                    ora_search_column = self.col_compair(tab_col, a)
                    if self._check_control(f'表 {tab_name}'):
                        return {
                            'messages': list(self.bfe),
                            'col_result': table_col_result,
                            'cons_err': table_cons_err,
                        }
                    constraints_start = stime.monotonic()
                    if self.tgt_db_t != 'hbase':
                        if self._uses_connector_backend() and (self.conf_src in connector_types or self.conf_tgt in connector_types):
                            tgt_cons = self.cons_analysis(tab_name, len(a), 'tgt')
                            src_cons = self.cons_analysis(tab_name, len(a), 'src')
                        else:
                            if self.conf_tgt not in ('elastic', 'hdfs') and self.conn_tgt is not None and not self._is_thread_safe_session(self.conn_tgt):
                                with self._conn_tgt_lock:
                                    tgt_cons = self.cons_analysis(tab_name, len(a), 'tgt')
                            else:
                                tgt_cons = self.cons_analysis(tab_name, len(a), 'tgt')
                            if self.conn_src is not None and not self._is_thread_safe_session(self.conn_src):
                                with self._conn_src_lock:
                                    src_cons = self.cons_analysis(tab_name, len(a), 'src')
                            else:
                                src_cons = self.cons_analysis(tab_name, len(a), 'src')
                        order_col, table_col_result = self.cons_compare(tgt_cons, src_cons)
                    else:
                        if self._uses_connector_backend() and (self.conf_src in connector_types or self.conf_tgt in connector_types):
                            src_cons = self.cons_analysis(tab_name, len(a), 'src')
                        elif self.conn_src is not None and not self._is_thread_safe_session(self.conn_src):
                            with self._conn_src_lock:
                                src_cons = self.cons_analysis(tab_name, len(a), 'src')
                        else:
                            src_cons = self.cons_analysis(tab_name, len(a), 'src')
                        order_col, table_col_result = self.cons_compare(src_cons, src_cons)
                    constraints_cost_ms = (stime.monotonic() - constraints_start) * 1000
                    if if_cpdata == 1:
                        if self._check_control(f'表 {tab_name}'):
                            return {
                                'messages': list(self.bfe),
                                'col_result': table_col_result,
                                'cons_err': table_cons_err,
                            }
                        if self._uses_connector_backend() and not any(acquired.values()):
                            acquired = self._acquire_table_compare_connectors()
                        rows_start = stime.monotonic()
                        if self._uses_connector_backend() and self.conf_src in connector_types and self.conf_tgt in connector_types:
                            table_cons_err = self.row_contain(None, None, tab_name, tab_col, a, order_col, ora_search_column)
                        else:
                            with ExitStack() as stack:
                                if self.conn_src is not None and not self._is_thread_safe_session(self.conn_src):
                                    stack.enter_context(self._conn_src_lock)
                                if self.conn_tgt is not None and self.conf_tgt not in ('elastic', 'hdfs', 'hbase') and not self._is_thread_safe_session(self.conn_tgt):
                                    stack.enter_context(self._conn_tgt_lock)
                                table_cons_err = self.row_contain(self.conn_src, self.conn_tgt, tab_name, tab_col, a, order_col, ora_search_column)
                        rows_cost_ms = (stime.monotonic() - rows_start) * 1000
                elif self.src_db_t == 'hbase':
                    pass
                else:
                    print('\033[0;34m ！\033[0m%s\t源备字段数不同，源端字段：%d \t备端字段%d' % (tab_name, len(tab_col), len(a)))
                    self.bfe.append('！%s\t源备字段数不同，源端字段：%d \t备端字段%d' % (tab_name, len(tab_col), len(a)))
                print(f'[timing] {tab_name} columns={columns_cost_ms:.2f}ms constraints={constraints_cost_ms:.2f}ms rows={rows_cost_ms:.2f}ms')
                print('表 %s 比对完成，耗时%.2f毫秒' % (tab_name, (stime.monotonic() - start) * 1000))
                return {
                    'messages': list(self.bfe),
                    'col_result': table_col_result,
                    'cons_err': table_cons_err,
                }
            except Exception:
                broken_roles = {role for role, flag in acquired.items() if flag}
                raise
            finally:
                if self._uses_connector_backend():
                    self._release_table_compare_connectors(acquired, broken_roles)
if __name__ == '__main3__':#refact，新版从cli入口，compare调试基本弃用
    src = 'mssql##';tgt = 'hyperbase184';err_handling = 3;if_cpdata=1#9.0常用 mysql##/oracle/mssql##
    
    # import compare_refactor as cr
    # p=cr.ergodic_database()
    # #p=ergodic_database()
    # dump_excel=ergodic.get_casefile()
    # start=stime.monotonic()
    # p.compare(src,tgt,err_handling,if_cpdata)
    # print('总耗时%.2f毫秒'%((stime.monotonic()-start)*1000))
    # p.dump_e(dump_excel)
if __name__ == '__main__2':#手动调试
    src = 'mssql##';tgt = 'doris2';err_handling = 3;if_cpdata=0#9.0常用 mysql##/oracle/mssql##
    p=ergodic_database()
    dump_excel=ergodic.get_casefile()
    p.compare(src,tgt,err_handling,if_cpdata)
    p.dump_e(dump_excel)
if __name__ == '__main__2':#命令行调试cmd命令行调用
    print('Start:')
    if len(sys.argv) != 4:
        print("用法: compare.exe <src_db> <tgt_db> <是否内容比较 1 on 0 off>")
        sys.exit(1)
    src = sys.argv[1]
    tgt = sys.argv[2]
    if_cpdata=int(sys.argv[3])
    err_handling = 3
    # err_handling  1：自动忽略所有无主键，2：忽略所有出错表，3：忽略所有问题表，4：手动选择
    # if_cpdata 1:比较内容，0 不比较内容
    p = ergodic_database()
    dump_excel = ergodic.get_casefile()
    p.compare(src, tgt, err_handling, if_cpdata)
    p.dump_e(dump_excel)
