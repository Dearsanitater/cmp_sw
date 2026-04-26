from django.apps import AppConfig
import os,json,pandas
import openpyxl
import multiprocessing
from multiprocessing import Queue,Process,Manager,Event
from multiprocessing.connection import Listener, Client
import queue
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import threading,sys,time,re
import ptest,process_test,configparser,sqlite3,uuid
from concurrent.futures import ProcessPoolExecutor
from django.http import JsonResponse,HttpResponse,FileResponse,StreamingHttpResponse
from django.shortcuts import render,redirect
from django.conf import settings
import hbase_conn
os.chdir(settings.BASE_DIR)
config_file=r'resource/rule_config.ini'
MEDIA_PATH = os.getenv("EXCEL_MEDIA_PATH", "resource/test_report/report.xlsx")
OUTPUT_PATH = os.getenv("EXCEL_OUTPUT_PATH", "resource/test_report/tmp.xlsx")
process_pool = ProcessPoolExecutor(max_workers=4)
#conn = sqlite3.connect('identifier.sqlite')
exec_task={}
DB_CONFIG_PATH = r'resource/DB.ini'
DJANGO_LOG_DIR = os.path.join(settings.BASE_DIR, 'crm', 'django_logs')
WORKER_AUTHKEY = b'hbase-worker'
WORKER_LOCK = threading.Lock()
WORKERS = {
    '0': {'name': 'no_ker', 'address': ('127.0.0.1', 61201), 'process': None},
    '1': {'name': 'ker', 'address': ('127.0.0.1', 61202), 'process': None},
}


def _read_rule_config(parser):
    # 历史 rule_config.ini 里有 GBK 内容，先试 UTF-8，失败再回退。
    last_error = None
    for encoding in ('utf-8', 'gbk', 'utf-8-sig'):
        try:
            parser.read(config_file, encoding=encoding)
            return encoding
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    return 'utf-8'


def _build_task_control():
    stop_event = Event()
    resume_event = Event()
    resume_event.set()
    return stop_event, resume_event


def _refresh_task(rule_id):
    task = exec_task.get(rule_id)
    if not task:
        return None
    process = task.get('process')
    if process and not process.is_alive() and task.get('status') not in ('finished', 'killed', 'failed'):
        task_state_machine(task, 'finish')
    return task


def _serialize_task(rule_id, task):
    process = task.get('process')
    return {
        'rule_id': rule_id,
        'task_type': task.get('task_type'),
        'pid': task.get('pid'),
        'status': task.get('status'),
        'is_alive': bool(process and process.is_alive()),
        'started_at': task.get('started_at'),
        'finished_at': task.get('finished_at'),
    }


def _get_running_task(rule_id):
    task = _refresh_task(rule_id)
    if not task:
        return None
    process = task.get('process')
    if process and process.is_alive():
        return task
    return None


def _register_task(rule_id, task_type, process, stop_event, resume_event):
    exec_task[rule_id] = {
        'process': process,
        'pid': process.pid,
        'task_type': task_type,
        'stop_event': stop_event,
        'resume_event': resume_event,
        'status': 'idle',
        'started_at': time.time(),
        'finished_at': None,
    }
    task_state_machine(exec_task[rule_id], 'start')
    return exec_task[rule_id]


def task_state_machine(task, event):
    transitions = {
        'idle': {'start': 'running'},
        'running': {
            'pause': 'paused',
            'stop': 'stopping',
            'kill': 'killed',
            'finish': 'finished',
            'fail': 'failed',
        },
        'paused': {
            'resume': 'running',
            'stop': 'stopping',
            'kill': 'killed',
            'fail': 'failed',
        },
        'stopping': {
            'finish': 'finished',
            'kill': 'killed',
            'fail': 'failed',
        },
        'finished': {},
        'killed': {},
        'failed': {},
    }
    current = task.get('status') or 'idle'
    next_status = transitions.get(current, {}).get(event)
    if not next_status:
        return False, f'{current} 状态不允许执行 {event}'
    task['status'] = next_status
    if next_status in ('finished', 'killed', 'failed'):
        task['finished_at'] = time.time()
    return True, next_status


def _task_not_found(rule_id):
    return JsonResponse({'status': 'error', 'message': f'规则 {rule_id} 没有运行中的任务'}, status=404)


def _wants_json(request):
    accept = request.headers.get('Accept', '')
    requested_with = request.headers.get('X-Requested-With', '')
    return 'application/json' in accept or requested_with == 'XMLHttpRequest'


def _read_db_ini():
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(DB_CONFIG_PATH, encoding='utf-8')
    return cfg


def _db_source_options():
    cfg = _read_db_ini()
    sources = []
    for section in cfg.sections():
        if section == '400_type':
            continue
        data = dict(cfg.items(section))
        data['name'] = section
        data['schema'] = data.get('schema') or data.get('namespace') or data.get('databasename') or ''
        data['host'] = data.get('host', '')
        data['port'] = data.get('port', '')
        data['type'] = data.get('type', '')
        data['databasename'] = data.get('databasename', '')
        sources.append(data)
    return sources


def _get_hbase_section(dbname):
    cfg = _read_db_ini()
    if not cfg.has_section(dbname):
        raise KeyError(f'未找到数据源 {dbname}')
    if cfg.get(dbname, 'type', fallback='').lower() != 'hbase':
        raise ValueError(f'{dbname} 不是 hbase 数据源')
    return {
        'name': dbname,
        'namespace': cfg.get(dbname, 'namespace', fallback='default'),
        'ker': cfg.get(dbname, 'ker', fallback='0'),
        'host': cfg.get(dbname, 'host', fallback=''),
    }


def _serialize_worker_error(message):
    return {'status': 'error', 'message': str(message)}


def _hbase_worker_main(worker_key, address, authkey):
    os.chdir(settings.BASE_DIR)
    hbase_conn.use_db_ini()
    cache = {}
    listener = Listener(address, authkey=authkey)
    print(f"hbase {WORKERS[worker_key]['name']} worker listening on {address}")
    while True:
        conn = listener.accept()
        try:
            payload = conn.recv()
            action = payload.get('action')
            if action == 'shutdown':
                conn.send({'status': 'success'})
                break
            dbname = payload.get('dbname')
            section = _get_hbase_section(dbname)
            if section['ker'] != worker_key:
                conn.send(_serialize_worker_error(f"{dbname} 应由 {'kerberos' if section['ker'] == '1' else 'non-kerberos'} worker 处理"))
                continue
            if dbname not in cache:
                cache[dbname] = hbase_conn.connFactory(dbname)
            hc = cache[dbname]
            if action == 'connect':
                namespaces = hc.hb_namespace()
                conn.send({
                    'status': 'success',
                    'dbname': dbname,
                    'namespaces': namespaces,
                    'namespace_count': len(namespaces),
                })
            elif action == 'tree':
                namespace = payload.get('namespace')
                conn.send({
                    'status': 'success',
                    'dbname': dbname,
                    'namespace': namespace,
                    'tables': hc.hb_tab(namespace=namespace),
                })
            elif action == 'scan':
                table = payload.get('table')
                namespace = payload.get('namespace')
                limit = int(payload.get('limit', 200))
                result = hc.browse_one_tb(table, limit=limit, namespace=namespace)
                conn.send({
                    'status': 'success',
                    'dbname': dbname,
                    'namespace': namespace,
                    'table': table,
                    'limit': limit,
                    'result': result,
                })
            else:
                conn.send(_serialize_worker_error(f'未知 action: {action}'))
        except Exception as e:
            try:
                conn.send(_serialize_worker_error(e))
            except Exception:
                pass
        finally:
            conn.close()
    listener.close()


def _ensure_hbase_worker(worker_key):
    with WORKER_LOCK:
        worker = WORKERS[worker_key]
        process = worker['process']
        if process is not None and process.is_alive():
            return
        process = Process(
            target=_hbase_worker_main,
            args=(worker_key, worker['address'], WORKER_AUTHKEY),
            daemon=True,
        )
        process.start()
        worker['process'] = process
    for _ in range(20):
        try:
            conn = Client(WORKERS[worker_key]['address'], authkey=WORKER_AUTHKEY)
            conn.close()
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"{WORKERS[worker_key]['name']} worker 启动失败")


def _call_hbase_worker(worker_key, payload):
    _ensure_hbase_worker(worker_key)
    conn = Client(WORKERS[worker_key]['address'], authkey=WORKER_AUTHKEY)
    try:
        conn.send(payload)
        return conn.recv()
    finally:
        conn.close()


def hbase_browser(request):
    return render(request, 'hbase_browser.html')


def hbase_sources(request):
    cfg = _read_db_ini()
    sources = []
    for section in cfg.sections():
        if cfg.get(section, 'type', fallback='').lower() == 'hbase':
            sources.append({
                'name': section,
                #'namespace': 'krb = '+cfg.get(section, 'ker', fallback='default'),
                'namespace': cfg.get(section,'host',fallback='default'),
                'ker': cfg.get(section, 'ker', fallback='0'),
                'host': cfg.get(section, 'host', fallback=''),
            })
    return JsonResponse({'status': 'success', 'sources': sources})


def hbase_connect(request):
    dbname = request.GET.get('dbname', '').strip()
    if not dbname:
        return JsonResponse({'status': 'error', 'message': '缺少 dbname'}, status=400)
    try:
        section = _get_hbase_section(dbname)
        result = _call_hbase_worker(section['ker'], {'action': 'connect', 'dbname': dbname})
        status = 200 if result.get('status') == 'success' else 500
        return JsonResponse(result, status=status)
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def hbase_tree(request):
    dbname = request.GET.get('dbname', '').strip()
    namespace = request.GET.get('namespace', '').strip()
    if not dbname or not namespace:
        return JsonResponse({'status': 'error', 'message': '缺少 dbname 或 namespace'}, status=400)
    try:
        section = _get_hbase_section(dbname)
        result = _call_hbase_worker(section['ker'], {'action': 'tree', 'dbname': dbname, 'namespace': namespace})
        status = 200 if result.get('status') == 'success' else 500
        return JsonResponse(result, status=status)
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def hbase_scan(request):
    dbname = request.GET.get('dbname', '').strip()
    namespace = request.GET.get('namespace', '').strip()
    table = request.GET.get('table', '').strip()
    limit = int(request.GET.get('limit', 200))
    if not dbname or not namespace or not table:
        return JsonResponse({'status': 'error', 'message': '缺少 dbname、namespace 或 table'}, status=400)
    try:
        section = _get_hbase_section(dbname)
        result = _call_hbase_worker(
            section['ker'],
            {'action': 'scan', 'dbname': dbname, 'namespace': namespace, 'table': table, 'limit': limit},
        )
    except Exception as e:
        result = {'status': 'error', 'message': str(e)}

    def generate():
        if result.get('status') != 'success':
            yield json.dumps({'type': 'error', 'message': result.get('message', 'scan failed')}, ensure_ascii=False) + '\n'
            return
        payload = result['result']
        columns = payload.get('columns', [])
        rows = payload.get('rows', [])
        yield json.dumps({
            'type': 'meta',
            'dbname': dbname,
            'namespace': namespace,
            'table': table,
            'limit': limit,
            'columns': columns,
        }, ensure_ascii=False) + '\n'
        for row in rows:
            row_dict = {}
            for index, column in enumerate(columns):
                row_dict[column] = row[index] if index < len(row) else ''
            yield json.dumps({'type': 'row', 'row': row_dict}, ensure_ascii=False) + '\n'
        yield json.dumps({'type': 'done', 'rows': len(rows)}, ensure_ascii=False) + '\n'

    return StreamingHttpResponse(generate(), content_type='application/x-ndjson; charset=utf-8')


def exec_switch(request,rule_id):
    running_task = _get_running_task(rule_id)
    if running_task:
        return JsonResponse({'status': 'error', 'message': f'规则 {rule_id} 已有运行中的任务'}, status=409)

    stop_event, resume_event = _build_task_control()
    rule_obj=ptest.rule(rule_id,'s')
    control = {'stop_event': stop_event, 'resume_event': resume_event}
    p = Process(target=ptest.incr_switch, args=(rule_obj,control))
    p.start()
    try:
        task = _register_task(rule_id, 'switch', p, stop_event, resume_event)
        print('已加入进程池',task['pid'])
    except Exception as e:
        return JsonResponse({"error": "未知错误", "message": str(e)})
    if _wants_json(request):
        return JsonResponse({'status': 'success', 'message': '脚本任务已启动', 'task': _serialize_task(rule_id, task)}, status=200)
    return redirect('/rule')
def exec_compare(request,rule_id):
    running_task = _get_running_task(rule_id)
    if running_task:
        return JsonResponse({'status': 'error', 'message': f'规则 {rule_id} 已有运行中的任务'}, status=409)

    stop_event, resume_event = _build_task_control()
    rule_obj = ptest.rule(rule_id, 'd')
    control = {'stop_event': stop_event, 'resume_event': resume_event}
    p = Process(target=ptest.start_comp, args=(rule_obj, control))
    p.start()
    try:
        task = _register_task(rule_id, 'compare', p, stop_event, resume_event)
        print('已加入进程池',task['pid'])
    except Exception as e:
        return JsonResponse({"error": "未知错误", "message": str(e)})
    if _wants_json(request):
        return JsonResponse({'status': 'success', 'message': '比对任务已启动', 'task': _serialize_task(rule_id, task)}, status=200)
    return redirect('/rule')
def create_rule(request):
    db_sources = _db_source_options()
    if request.method == 'POST':
        # 获取表单参数
        user_id = int(request.POST.get('user_id'))
        rule_name = request.POST.get('rule_name')
        rule_id = uuid.uuid1()#uuid
        src = request.POST.get('src')
        tgt = request.POST.get('tgt')
        unique_number = int(request.POST.get('unique_number'))
        error_handler = int(request.POST.get('error_handler'))
        print(type(rule_name))
        try:
              # 连接到数据库
            conn = sqlite3.connect('identifier.sqlite')
            cursor = conn.cursor()
            cursor.execute(f'''
                            INSERT INTO main.rule_cmp (user_id, rule_name, rule_id, src, tgt, unique_number, error_handler)
                            VALUES ({user_id}, '{rule_name}', '{rule_id}', '{src}', '{tgt}', {unique_number}, {error_handler})
                        ''')
            conn.commit()  # 提交事务
            conn.close()
            #return redirect('/rule')
        except sqlite3.Error as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
        #构建需要写入的内容
        config = configparser.ConfigParser()
        _read_rule_config(config)
        if not config.has_section(str(rule_id)):
            config.add_section(str(rule_id))
        config.set(str(rule_id), 'user_id', str(user_id))
        config.set(str(rule_id), 'rule_name', rule_name)
        config.set(str(rule_id), 'rule_id', str(rule_id))
        config.set(str(rule_id), 'src', src)
        config.set(str(rule_id), 'tgt', tgt)
        config.set(str(rule_id), 'unique_number', str(unique_number))
        config.set(str(rule_id), 'error_handler', str(error_handler))
        # 将更改写回配置文件
        with open(config_file, 'w', encoding='utf-8') as configfile:
            config.write(configfile)
        json_data={'status': 'success', 'message': 'Rule added successfully'}
        # GET 请求返回表单页面
        return redirect('/rule')
    return render(request, 'create_rules.html', {'db_sources': db_sources, 'db_sources_json': json.dumps(db_sources, ensure_ascii=False)})
def del_rule(request,rule_id):
    conn = sqlite3.connect('identifier.sqlite')  # 连接到数据库
    cursor = conn.cursor()
    sql=f'''delete from main.rule_cmp where rule_id="{rule_id}"'''
    print(sql)
    config = configparser.ConfigParser()
    _read_rule_config(config)
    cursor.execute(sql)
    if config.has_section(rule_id):
        # 删除指定的 section
        config.remove_section(rule_id)
        # 将修改后的配置写回文件
        with open(config_file, 'w', encoding='utf-8') as file:
            config.write(file)
    return JsonResponse({'status': 'success', 'message': '成功删除规则!'}, status=200)
def del_config(request,dbname):
    print(dbname)
    config = configparser.ConfigParser()
    config.read('resource/DB.ini', encoding='utf-8')
    if config.has_section(dbname):
        config.remove_section(dbname)
        with open('resource/DB.ini','w',encoding='utf-8') as file:
            config.write(file)
    return  JsonResponse({'status': 'success', 'message': '成功删除!'}, status=200)
def udp_config(request,dbname):
    if request.method == 'POST':
        data= json.loads(request.body)
        update_value=data['updatedValues']
        config = configparser.ConfigParser()
        config.read('resource/DB.ini',encoding='utf-8')
        for key,value in update_value.items():
            config.set(data['section'], key, value)
            print(key+'\t'+value)
        with open('resource/DB.ini','w',encoding='utf-8') as file:
            config.write(file)
        return JsonResponse({'status': 'success', 'message': 'Config updated successfully!'})
    return JsonResponse({'status': 'error', 'message': 'Invalid request method.'}, status=400)
def add_config(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        print(type(data),data)
        section=data['section']
        config = configparser.ConfigParser()
        config.read('resource/DB.ini', encoding='utf-8')
        if not config.has_section(data['section']):
            config.add_section(data['section'])
            del data['section']
            for item in data['extraKeys']:
                dic={item['key']:item['value']}
                data={**data,**dic}
            del data['extraKeys']
            for key,value in data.items():
                config.set(section,key,value)
            with open('resource/DB.ini','w',encoding='utf-8') as f:
                config.write(f)
            return JsonResponse({'status': 'success', 'message': 'Config updated successfully!'})
        else:
            JsonResponse({'status': 'error', 'message': 'section名称已存在，添加失败'}, status=400)
    else:
        return JsonResponse({'status': 'error', 'message': 'Invalid request method.'}, status=400)
#修改规则
def ch_rule():
    pass
def shut_rule(request,rule_id):
    if request.method == 'POST':
        task = _get_running_task(rule_id)
        if not task:
            return _task_not_found(rule_id)
        ok, message = task_state_machine(task, 'stop')
        if not ok:
            return JsonResponse({'status': 'error', 'message': message, 'task': _serialize_task(rule_id, task)}, status=409)
        task['stop_event'].set()
        task['resume_event'].set()
        return JsonResponse({'status': 'success', 'message': '已发送停止指令', 'task': _serialize_task(rule_id, task)}, status=200)
    else:
        return JsonResponse({"error": "11", "message": '错误访问方法'}, status=500)


def pause_rule(request, rule_id):
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': '错误访问方法'}, status=405)
    task = _get_running_task(rule_id)
    if not task:
        return _task_not_found(rule_id)
    if task.get('status') == 'paused':
        return JsonResponse({'status': 'success', 'message': '任务已经处于暂停状态', 'task': _serialize_task(rule_id, task)}, status=200)
    ok, message = task_state_machine(task, 'pause')
    if not ok:
        return JsonResponse({'status': 'error', 'message': message, 'task': _serialize_task(rule_id, task)}, status=409)
    task['resume_event'].clear()
    return JsonResponse({'status': 'success', 'message': '已发送暂停指令', 'task': _serialize_task(rule_id, task)}, status=200)


def resume_rule(request, rule_id):
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': '错误访问方法'}, status=405)
    task = _get_running_task(rule_id)
    if not task:
        return _task_not_found(rule_id)
    ok, message = task_state_machine(task, 'resume')
    if not ok:
        return JsonResponse({'status': 'error', 'message': message, 'task': _serialize_task(rule_id, task)}, status=409)
    task['resume_event'].set()
    return JsonResponse({'status': 'success', 'message': '已恢复任务', 'task': _serialize_task(rule_id, task)}, status=200)


def kill_rule(request, rule_id):
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': '错误访问方法'}, status=405)
    task = _refresh_task(rule_id)
    if not task:
        return _task_not_found(rule_id)
    process = task.get('process')
    if not process or not process.is_alive():
        return JsonResponse({'status': 'error', 'message': '任务未运行，无需强制终止', 'task': _serialize_task(rule_id, task)}, status=409)
    if process and process.is_alive():
        process.terminate()
        process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)
    ok, message = task_state_machine(task, 'kill')
    if not ok:
        return JsonResponse({'status': 'error', 'message': message, 'task': _serialize_task(rule_id, task)}, status=409)
    task['stop_event'].set()
    task['resume_event'].set()
    return JsonResponse({'status': 'success', 'message': '任务已强制终止', 'task': _serialize_task(rule_id, task)}, status=200)


def task_status(request, rule_id):
    if request.method != 'GET':
        return JsonResponse({'status': 'error', 'message': '错误访问方法'}, status=405)
    task = _refresh_task(rule_id)
    if not task:
        return _task_not_found(rule_id)
    return JsonResponse({'status': 'success', 'task': _serialize_task(rule_id, task)}, status=200)



def get_rule(request):

    if request.method=='GET':
        set=(ptest.get_rule())
        response = {'rules':set}
        #return JsonResponse(response)
        return render(request,'rule_home.html',response)
#纯json输出规则界面
def get_rule_test(request, rule_id=None):
    if request.method == 'GET':
        rules = ptest.get_rule()
        source_map = {item['name']: item for item in _db_source_options()}
        for rule in rules.values():
            src_info = source_map.get(rule.get('src', ''), {})
            tgt_info = source_map.get(rule.get('tgt', ''), {})
            rule['src_host'] = src_info.get('host', '')
            rule['src_schema'] = src_info.get('schema', '')
            rule['src_type'] = src_info.get('type', '')
            rule['tgt_host'] = tgt_info.get('host', '')
            rule['tgt_schema'] = tgt_info.get('schema', '')
            rule['tgt_type'] = tgt_info.get('type', '')
        selected_rule = rules.get(rule_id) if rule_id else None
        return render(request, 'rule_test.html', {'rules': rules, 'selected_rule': selected_rule, 'selected_rule_id': rule_id})


def jsonrule(request):
    if request.method=='GET':
        set=(ptest.get_rule())
        response = {'rules':set}
        return JsonResponse(response)
def detail(request,rule_id):
    conn1 = sqlite3.connect('identifier.sqlite')
    cursor=conn1.cursor()
    set=[]
    dict={}
    report=[]
    for item in  ptest.get_rule().items():
        set.append((item[0],item[1]['rule_name']))
        cursor.execute(f"select tbname from main.report_map where uid='{item[0]}' order by inc desc")
        result=cursor.fetchall()
        if result !=0:
            fet=[x[0] for x in result]
        else:fet=[]
        dict[f'{item[0]}']=fet
    print(json.dumps({'rules': set,'files': dict,'rule_id': rule_id}))
    return render(request, 'detail.html', {"rules": json.dumps(set),"files": json.dumps(dict),"rule_id": rule_id})
def serve_excel(request):
    file_path = os.path.join(MEDIA_PATH)  # 直接写死路径
    if os.path.exists(file_path):
        return FileResponse(open(file_path, "rb"), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        return HttpResponse("File not found", status=404)
def download_ffile(request):
    if request.method == 'POST':
        data= json.loads(request.body)
        print(type(data),data)
    try:
        sheets_to_keep = data  # 需要的工作表列表
        # 读取 Excel 并筛选工作表
        with pandas.ExcelFile(MEDIA_PATH) as xls:
            writer = pandas.ExcelWriter(OUTPUT_PATH, engine="xlsxwriter")
            for sheet in sheets_to_keep:
                if sheet in xls.sheet_names:
                    df = xls.parse(sheet)
                    df.to_excel(writer, sheet_name=sheet, index=False)
            writer.close()

        # 返回合并后的文件
        response = FileResponse(open(OUTPUT_PATH, "rb"))
        response["Content-Disposition"] = 'attachment; filename="filtered_output.xlsx"'
        return response

    except Exception as e:
        return HttpResponse(f"处理错误: {str(e)}", status=500)
def download_file(request):
    response = FileResponse(open(MEDIA_PATH, "rb"))
    response["Content-Disposition"] = 'attachment; filename="filtered_output.xlsx"'
    return response


def django_logs(request):
    os.makedirs(DJANGO_LOG_DIR, exist_ok=True)

    def safe_log_path(rel_path):
        normalized = os.path.normpath(rel_path or '')
        if normalized in ('', '.'):
            return None
        target = os.path.abspath(os.path.join(DJANGO_LOG_DIR, normalized))
        base = os.path.abspath(DJANGO_LOG_DIR)
        if os.path.commonpath([base, target]) != base:
            return None
        if not os.path.isfile(target):
            return None
        return target

    rel_file = request.GET.get('file', '')
    log_path = safe_log_path(rel_file)
    if log_path and request.GET.get('download') == '1':
        return FileResponse(open(log_path, 'rb'), as_attachment=True, filename=os.path.basename(log_path))

    entries = []
    for root, dirs, files in os.walk(DJANGO_LOG_DIR):
        dirs.sort()
        files.sort()
        rel_root = os.path.relpath(root, DJANGO_LOG_DIR)
        depth = 0 if rel_root == '.' else rel_root.count(os.sep) + 1
        if rel_root != '.':
            entries.append({
                'type': 'dir',
                'name': os.path.basename(root),
                'rel_path': rel_root.replace(os.sep, '/'),
                'depth': depth - 1,
                'size': '',
                'mtime': '',
            })
        for file_name in files:
            full_path = os.path.join(root, file_name)
            rel_path = os.path.relpath(full_path, DJANGO_LOG_DIR)
            stat = os.stat(full_path)
            entries.append({
                'type': 'file',
                'name': file_name,
                'rel_path': rel_path.replace(os.sep, '/'),
                'depth': depth,
                'size': stat.st_size,
                'mtime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stat.st_mtime)),
            })

    content = ''
    selected = ''
    if log_path:
        selected = os.path.relpath(log_path, DJANGO_LOG_DIR).replace(os.sep, '/')
        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

    return render(request, 'log_browser.html', {
        'entries': entries,
        'selected': selected,
        'content': content,
    })


class QueueMaintainer:
    def __init__(self,uuid,pid,usrid):
        self.que_dic={}
        self.pid=pid
        self.uuid=uuid
        self.user=usrid
        self.queue=queue.Queue(maxsize=2000)
        self.running=True
        self.dropped_count=0
        self.log_file = None
        self.log_path = self.build_log_path()
        self.open_log_file()
        self.que_dic[f'{pid}']={'uuid':f'{uuid}','queue':self.queue}
        #初始化时就调用异步线程读队列
        self.consumer_thread = threading.Thread(target=self.consume_queue, daemon=True)
        self.consumer_thread.start()
    def build_log_path(self):
        cfg = configparser.ConfigParser()
        _read_rule_config(cfg)
        rule_name = cfg.get(str(self.uuid), 'rule_name', fallback=str(self.uuid))
        safe_rule_name = re.sub(r'[\\/:*?"<>|\s]+', '_', rule_name).strip('._')
        if not safe_rule_name:
            safe_rule_name = str(self.uuid)
        timestamp = time.strftime('%y%m%d_%H%M%S')
        return os.path.join(DJANGO_LOG_DIR, safe_rule_name, f'{timestamp}.log')
    def open_log_file(self):
        try:
            os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
            self.log_file = open(self.log_path, 'a', encoding='utf-8', buffering=1)
        except Exception as e:
            self.log_file = None
            print(f'QueueMaintainer open log file error: {e}', file=sys.__stderr__)
    def write(self,message):
        if message != '':
            try:
                self.queue.put_nowait((self.pid, message))
            except queue.Full:
                self.dropped_count += 1
    def get_que(self,pid):
        return self.que_dic[f'{self.pid}']
    def consume_queue(self):
        while self.running or not self.queue.empty():
            try:
                process_id, message = self.queue.get(timeout=0.2)
                messages = [message]
                self.queue.task_done()  # 确保消费完成
                deadline = time.monotonic() + 0.15
                while len(messages) < 50:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        _, next_message = self.queue.get(timeout=remaining)
                        messages.append(next_message)
                        self.queue.task_done()
                    except queue.Empty:
                        break
                batch_message = ''.join(messages)
                self.write_log_file(batch_message)
                self.process_message(process_id, batch_message)
            except Exception as e:
                if isinstance(e, queue.Empty):
                    continue
                if sys.is_finalizing():
                    break
                print(f'QueueMaintainer consume_queue error: {e}', file=sys.__stderr__)
                continue

        print(f'进程{self.pid}停止读取队列，已清空队列')
    def stop(self):
        self.running=False
        if self.dropped_count:
            try:
                self.queue.put_nowait((self.pid, f'日志输出过快，已丢弃 {self.dropped_count} 条输出'))
            except queue.Full:
                pass
        self.consumer_thread.join(timeout=3)
        if self.consumer_thread.is_alive():
            self.flush_q()
        if self.log_file:
            try:
                self.log_file.close()
            except Exception:
                pass
    def flush_q(self):
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except queue.Empty:
                break
    def flush(self):
        pass
    def write_log_file(self, message):
        if not self.log_file:
            return
        try:
            self.log_file.write(message)
            if not message.endswith('\n'):
                self.log_file.write('\n')
            self.log_file.flush()
        except Exception as e:
            print(f'QueueMaintainer write log file error: {e}', file=sys.__stderr__)
            try:
                self.log_file.close()
            except Exception:
                pass
            self.log_file = None
    def process_message(self, process_id, message):
        """处理队列中的消息，可能是打印到控制台或Web页面"""
        # old_std=sys.stdout
        # sys.stdout = sys.__stdout__
        # print(f"[进程{process_id}] ruleid{self.uuid}{message}")
        # sys.stdout=old_std
        if sys.is_finalizing():
            self.running = False
            return
        channels_layer=get_channel_layer()
        if channels_layer is None:
            return
        group_name = 'messages_group'
        try:
            sender = async_to_sync(channels_layer.group_send)
            sender(
                group_name,  # 用户对应的 WebSocket 组
                {
                    'type': 'send_message',  # 消息类型
                    'rule_id': self.uuid,  # 进程 uuID
                    'message': message  # 消息内容
                }
            )
        except RuntimeError as e:
            if sys.is_finalizing() or 'interpreter shutdown' in str(e):
                self.running = False
                return
            raise


