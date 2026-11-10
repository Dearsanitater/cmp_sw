打包：
pyinstaller compare.spec --distpath D:\\Info\\dist\\
daphne启动：
daphne -b 0.0.0.0 -p 8000 crm.asgi:application
参数：
compare.exe <src_db> <tgt_db> <是否内容比较 1 on 0 off> 例如：  compare.exe db2 oracle 0 //源db2 ，备oracle，不比较表内容
1、config.ini的数据名不能直接输入，仅能输入数据库类型，将自己的库改名为数据库类型，
db2,mysql,oracle,pg,mssql,ob_oracle,ob_mysql
2、是否内容比较 1 on 0 off

switch <db> <表名是否唯一 1 on 0 off> <用例文件路径名>
1、某些用例有大量重复表名，开启此项表名=表名+用例号(仅有数字)
2、用例路径 resource\case_dir.ini 中配置，程序只会读目录下的sql文件不会递归读文件夹
DB2INST1,DDS_TABLE_TIME_UPDATE,dbo_test,dds_table_time_update
（新）3、源库为as400时可以指定dds=1/0，将sql语句解析为dds语句建表；（dds相较传统sql建表差异较大）
问题：
1、连接db2报-1042
	（1）尝试db2cli能否正常连接
	（2）clidriver\bin\amd64.VC12.crt中找到msvcp120.dll、msvcr120.dll，放到C:\Windows\System32中
2、连接oracle报错cx-oracle缺库
需求8195_春秋航空8.2_mss-hb仅增量规则重新同步或第一次启动发表结构BAT报告_20261009005

打包：
#1、dpi复制到 _internal下 
2、复制clidriver到 _internal
3、复制ibm*3 到 internal下
4、复制resource文件夹到.exe同级目录
5、复制lib文件夹到.exe同级目录
\\172.20.64.7\i2soft\i2soft\i2soft\i2stream\iaproxy\feature\stage_change_alarm\8.1.2609171457
\\172.20.64.190\fileshare\rd_target\i2soft\8.2\release\8.2\8.2.2608051029\linux
------K8s-已配置chrony同步个机器节点实际
--k8s集群配置情况
ntp-117
master 10.1.131.123 root/root
node1 10.1.131.121 root/root
node2  10.1.131.126 root/root

hostnamectl set-hostname k8smaster
hostnamectl set-hostname k8snode2
k8s架构：
--ruleid不等同于jobid
创建规则-compare_rule-ruleid
excute比对任务-compare_job——jobid、srckey、tgtkey
cmp逻辑
cli.py-execution/runner.py-connector（获取各种库信息）-core/compare.py

db.ini/compare.ini/快照-docker
