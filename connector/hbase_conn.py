import os,importlib,django,paramiko
import jpype,jpype.imports,sys
from jpype import JClass
import thrift,thrift_sasl,threading
from queue import Queue

from jpype.types import *
import time
import configparser
import pandas
jvm_path=r"C:\Program Files\Java\jre1.8.0_201\bin\server\jvm.dll"
config = configparser.ConfigParser(interpolation=None)
config.read(r'resource/config.ini', encoding='utf-8')
db_config = configparser.ConfigParser(interpolation=None)
db_config.read(r'resource/DB.ini', encoding='utf-8')

def ker_jvm(jars):
    jars.append('lib/hyperlib')
    jpype.startJVM(
        "-Djava.security.krb5.conf=lib/krb5.ini",
        "-Djava.security.auth.login.config=lib/hyperlib/jaas.conf",
        "-Dsun.security.krb5.debug=true",
        # "-Dnetworkaddress.cache.ttl=0",
        "-Dfile.encoding=UTF-8",
        "-Djavax.security.auth.useSubjectCredsOnly=false", classpath=jars, jvmpath=jvm_path)
    java_lang_System = jpype.JPackage("java").lang.System
    java_lang_System.setOut(jpype.java.io.PrintStream(jpype.java.io.FileOutputStream("C:\\temp\\jvm_krb5.log")))
    java_lang_System.setErr(jpype.java.io.PrintStream(jpype.java.io.FileOutputStream("C:\\temp\\jvm_krb5_err.log")))
def no_ker_jvm(jars):
    jpype.startJVM(
    "-Dfile.encoding=UTF-8",
    classpath=jars,
    jvmpath=jvm_path)
def use_db_ini():
    global config
    config = configparser.ConfigParser(interpolation=None)
    config.read(r'resource/DB.ini', encoding='utf-8')
    return config
def list_hbase_sources():
    result = []
    for section in db_config.sections():
        if db_config.get(section, 'type', fallback='').lower() == 'hbase':
            result.append({
                'name': section,
                'namespace': db_config.get(section, 'namespace', fallback='dbo'),
                'ker': db_config.get(section, 'ker', fallback='0'),
                'host': db_config.get(section, 'host', fallback=''),
            })
    return result
def open_hbase(dbname):
    T1 = T2 = 0#;exc_thd, rcv_thd = config.get(dbname, 'exec_threads'), config.get(dbname,'recv_threads')  # hbase_client DRIVER
    ifKerberos = config.get(dbname, 'ker');principal = config.get(dbname, 'principle');keytab = config.get(dbname, 'kpath')
    zk_host=config.get(dbname,'zk_host',fallback=config.get(dbname,'host'))
    zk_port=config.get(dbname,'zk_port',fallback='2181')
    parent=config.get(dbname,'znode_parent',fallback='/hbase')
    try:
        T1 = time.perf_counter()
        jars = [os.path.join("lib/hbase", i) for i in os.listdir("lib/hbase") if i.endswith(".jar")]
        # 前缀认证
        if ifKerberos == '1':
            if not jpype.isJVMStarted():
                print('no kerberos jvm exsist,starting...');ker_jvm(jars)
            else:print('kerberos jvm exsist,复用')
            print(principal, keytab)
            from org.apache.hadoop.security import UserGroupInformation
            from org.apache.hadoop.conf import Configuration
            conf = Configuration()
            conf.set("hadoop.security.authentication", "kerberos")
            UserGroupInformation.setConfiguration(conf)
            UserGroupInformation.loginUserFromKeytab(principal, keytab)
            print('kerberos票据获取已获取,ugi用户', UserGroupInformation.getCurrentUser())
            from org.apache.hadoop.hbase import HBaseConfiguration
            hbase_conf = HBaseConfiguration.create()
            ugi = UserGroupInformation.getLoginUser()
            from java.security import PrivilegedExceptionAction
            from org.apache.hadoop.hbase.client import ConnectionFactory
            def run():
                return ConnectionFactory.createConnection(hbase_conf)
            action = jpype.JProxy(PrivilegedExceptionAction, dict(run=run))
            conn = ugi.doAs(action)
        else:
            if not jpype.isJVMStarted():
                print('no jvm exsist,starting...');no_ker_jvm(jars)
            else:print('jvm exsist,复用')
            print('未启用kerberos认证')
            from org.apache.hadoop.hbase import HBaseConfiguration
            from org.apache.hadoop.hbase.client import ConnectionFactory
            from org.apache.hadoop.conf import Configuration
            hbase_conf = HBaseConfiguration.create()
            hbase_conf.set("hbase.zookeeper.quorum", f"{zk_host}")
            hbase_conf.set("hbase.zookeeper.property.clientPort", f"{zk_port}")
            # 如果有 znode parent 也要加
            hbase_conf.set("zookeeper.znode.parent", f"{parent}")
            print('start建立连接...')
            conn = ConnectionFactory.createConnection(hbase_conf)
            print("HBase 无认证连接成功:", conn)
        print('stablish conn done')
        T2 = time.perf_counter()
    except Exception as e:
        print(f"数据库连接失败！{e}")
        return
    print(f'{dbname}链接耗时:%f毫秒' % (((T2 - T1) * 1000)))
    return conn

class connFactory():
    def __init__(self,dbname):
        self.conn=open_hbase(dbname)
        self.src_col = {'tab_num': 0, 'tab_name': '', 'tab_col': []}
        self.col={'col_name': '', 'col_num': 1, 'len': None, 'null': 'NO', 'num_pre': 1, 'num_scale': 1, 'time_sh': None, 'type': 'text'}
        self.schema=config.get(dbname,'namespace')
        self.Configuration = JClass("org.apache.hadoop.conf.Configuration")
        self.HBaseConfiguration = JClass("org.apache.hadoop.hbase.HBaseConfiguration")
        self.ConnectionFactory = JClass("org.apache.hadoop.hbase.client.ConnectionFactory")
        self.TableName = JClass("org.apache.hadoop.hbase.TableName")
        self.Scan = JClass("org.apache.hadoop.hbase.client.Scan")
        self.ResultScanner = JClass("org.apache.hadoop.hbase.client.ResultScanner")
        self.Bytes = JClass("org.apache.hadoop.hbase.util.Bytes")
        self.Admin = JClass("org.apache.hadoop.hbase.client.Admin")
    def scan_one_tb(self,tbname,tbcol):
        tmp_dict = {}
        tmp_list = []
        for item in tbcol:
            col_name = item['col_name']
            tmp_list.append(col_name)
            tmp_dict[col_name] = ''
        conn = self.conn
        sctbname = f'{self.schema}:{tbname}'
        scan = self.Scan()
        table = conn.getTable(self.TableName.valueOf(sctbname))
        scanner = None
        rows = []
        try:
            scanner = table.getScanner(scan)
            for result in scanner:
                row_data = tmp_dict.copy()
                for cell in result.listCells():
                    qual = str(self.Bytes.toString(
                        cell.getQualifierArray(),
                        cell.getQualifierOffset(),
                        cell.getQualifierLength(),
                    ))
                    if qual in row_data:
                        row_data[qual] = str(self.Bytes.toString(
                            cell.getValueArray(),
                            cell.getValueOffset(),
                            cell.getValueLength(),
                        ))
                # compare.py 后续按默认整数列索引访问 DataFrame，这里按源字段顺序展开为列表
                rows.append([row_data[col_name] for col_name in tmp_list])
        finally:
            if scanner is not None:
                scanner.close()
            table.close()

        return pandas.DataFrame(rows)
    def pret_col(self,tmp,n):#按照tmp中字段最多的行来确定表的字段结构，n是该行在tmp中的索引，tmp是所有行的列表
        tab_col=[]
        col = {'col_name': '', 'col_num': 1, 'len': None, 'null': 'NO', 'num_pre': 1, 'num_scale': 1, 'time_sh': None,'type': 'text'}
        if not tmp:
            return [],[]

        for item in list(tmp[n-1].keys()):
            col['col_name']=item
            tab_col.append(col.copy())
        all_keys = set().union(*tmp)
        new_tmp = [{k: d.get(k,'') for k in all_keys} for d in tmp]
        #print(new_tmp)
                #standard=(standard+(tmp[n-1]-standard)).copy
        return  tab_col,new_tmp
    def hb_namespace(self):#浏览器用，返回所有 namespace
        conn=self.conn
        admin=conn.getAdmin()
        result=[]
        try:
            spaces=admin.listNamespaceDescriptors()
            for item in spaces:
                result.append(str(item.getName()))
            print(result)
            return result
        finally:
            admin.close()
    def hb_tab(self,namespace=None):#返回表名列表
        conn=self.conn
        admin=conn.getAdmin()
        result=[]
        try:
            use_namespace = namespace or self.schema
            tbs=admin.listTableNamesByNamespace(use_namespace)
            for t in tbs:
                result.append(str(t.getQualifierAsString()))
            print(result)
            return result
        finally:
            admin.close()
    def browse_one_tb(self,tbname,limit=200,namespace=None):#浏览器专用方法
        conn=self.conn
        tbno=0;final=[];rows=[];columns=[]
        table=None;scanner=None
        try:
            use_namespace = namespace or self.schema
            sctbname=f'{use_namespace}:'+tbname;tbno+=1
            scan = self.Scan()
            table = conn.getTable(self.TableName.valueOf(sctbname))
            scanner=table.getScanner(scan)
            result_dict={};new_value={};n=0;tmp=[]
            for result in scanner:
                rowkey = self.Bytes.toString(result.getRow())#1
                #colkey = n
                row_data = {'__rowkey__': str(rowkey)}
                for cell in result.listCells():
                    cf = self.Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                    qual = str(self.Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()))
                    val = str(self.Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()))
                    row_data[f"{qual}"] = val
                if len(row_data)>n:n+=1
                else:n=len(row_data)#记录最多字段行，n是tmp列表的索引
                tmp.append(row_data.copy())
                if limit and len(tmp) >= limit:
                    break
            result_dict['tab_col'],new_value['tab_col'] = self.pret_col(tmp,n);result_dict['tab_name']=tbname;result_dict['tab_num']=tbno
            final.append(result_dict)
            columns = ['__rowkey__']
            for item in result_dict['tab_col']:
                if item['col_name'] != '__rowkey__' and item['col_name'] not in columns:
                    columns.append(item['col_name'])
            for item in tmp:
                for key in item.keys():
                    if key not in columns:
                        columns.append(key)
            for item in tmp:
                rows.append([item.get(col, '') for col in columns])
            #print(final)
            return {'tab_num': tbno, 'tables': final, 'columns': columns, 'rows': rows}
        finally:
            if scanner is not None:
                scanner.close()
            if table is not None:
                table.close()
    def get_conn(self,dbname):
        return open_hbase(dbname)

if __name__=='__main__':

    cf=connFactory('hyperbase184')
    cf.hb_tab()
    #cf.scan_tb(cf.hb_tab())
    #scan_table(conn,"dbo:tab_nopk1")
