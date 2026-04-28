from __future__ import annotations

import configparser
import json
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import pymysql
from django.http import FileResponse, JsonResponse
from django.shortcuts import redirect, render

import hbase_conn


BASE_DIR = Path(__file__).resolve().parents[3]
CONFIG_INI = BASE_DIR / "resource" / "config.ini"
DB_INI = BASE_DIR / "resource" / "DB.ini"
REPORT_PATH = BASE_DIR / "resource" / "test_report" / "report.xlsx"


def _mysql_settings():
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(CONFIG_INI, encoding="utf-8")
    if not cfg.has_section("mysql"):
        raise KeyError(f"missing [mysql] in {CONFIG_INI}")
    section = cfg["mysql"]
    return {
        "host": section.get("host"),
        "port": section.getint("port", fallback=3306),
        "user": section.get("username"),
        "password": section.get("password"),
        "database": section.get("databasename"),
        "charset": "utf8mb4",
        "autocommit": True,
        "cursorclass": pymysql.cursors.DictCursor,
    }


@contextmanager
def _mysql_conn():
    conn = pymysql.connect(**_mysql_settings())
    print('mysql已连接')
    try:
        yield conn
    finally:
        conn.close()


def _execute(sql, args=None):
    with _mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, args)
            return cursor


def _ensure_tables():
    statements = [
        """
        CREATE TABLE IF NOT EXISTS compare_rule (
            rule_id VARCHAR(64) PRIMARY KEY,
            user_id VARCHAR(64) NOT NULL DEFAULT '',
            rule_name VARCHAR(255) NOT NULL,
            src_key VARCHAR(128) NOT NULL,
            tgt_key VARCHAR(128) NOT NULL,
            unique_number TINYINT(1) NOT NULL DEFAULT 1,
            error_handler INT NOT NULL DEFAULT 3,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS compare_job (
            job_id VARCHAR(64) PRIMARY KEY,
            src_key VARCHAR(128) NOT NULL,
            tgt_key VARCHAR(128) NOT NULL,
            compare_schema TINYINT(1) NOT NULL DEFAULT 1,
            compare_data TINYINT(1) NOT NULL DEFAULT 1,
            status VARCHAR(32) NOT NULL DEFAULT 'planning',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at DATETIME NULL,
            finished_at DATETIME NULL,
            error_message TEXT NULL,
            max_pods INT NOT NULL DEFAULT 1,
            command VARCHAR(32) NULL,
            command_version BIGINT NOT NULL DEFAULT 0,
            total_tasks INT NOT NULL DEFAULT 0,
            success_tasks INT NOT NULL DEFAULT 0,
            failed_tasks INT NOT NULL DEFAULT 0,
            running_tasks INT NOT NULL DEFAULT 0,
            pending_tasks INT NOT NULL DEFAULT 0,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS compare_table_task (
            task_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            job_id VARCHAR(64) NOT NULL,
            table_name VARCHAR(512) NOT NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            worker_id VARCHAR(128) NULL,
            claim_token VARCHAR(64) NULL,
            retry_count INT NOT NULL DEFAULT 0,
            started_at DATETIME NULL,
            finished_at DATETIME NULL,
            heartbeat_at DATETIME NULL,
            error_message TEXT NULL,
            lease_until DATETIME NULL,
            claimed_at DATETIME NULL,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_job_table (job_id, table_name),
            KEY idx_task_claim (job_id, status, task_id),
            KEY idx_task_worker (worker_id, status),
            KEY idx_task_heartbeat (status, heartbeat_at),
            KEY idx_task_token (claim_token),
            KEY idx_job_status_lease (job_id, status, lease_until, task_id),
            CONSTRAINT fk_compare_table_task_job
                FOREIGN KEY (job_id)
                REFERENCES compare_job (job_id)
                ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]
    with _mysql_conn() as conn:
        with conn.cursor() as cursor:
            for sql in statements:
                cursor.execute(sql)


def _db_source_options():
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(DB_INI, encoding="utf-8")
    sources = []
    for section in cfg.sections():
        if section == "400_type":
            continue
        data = dict(cfg.items(section))
        data["name"] = section
        data["schema"] = data.get("schema") or data.get("namespace") or data.get("databasename") or ""
        data["host"] = data.get("host", "")
        data["port"] = data.get("port", "")
        data["type"] = data.get("type", "")
        data["databasename"] = data.get("databasename", "")
        sources.append(data)
    return sources


def _rules():
    _ensure_tables()
    with _mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT rule_id, user_id, rule_name, src_key, tgt_key, unique_number, error_handler
                FROM compare_rule
                ORDER BY updated_at DESC
                """
            )
            rows = cursor.fetchall()
    return {
        row["rule_id"]: {
            "rule_id": row["rule_id"],
            "user_id": row["user_id"],
            "rule_name": row["rule_name"],
            "src": row["src_key"],
            "tgt": row["tgt_key"],
            "unique_number": row["unique_number"],
            "error_handler": row["error_handler"],
        }
        for row in rows
    }


def _rule(rule_id):
    _ensure_tables()
    with _mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT rule_id, user_id, rule_name, src_key, tgt_key, unique_number, error_handler
                FROM compare_rule
                WHERE rule_id=%s
                """,
                (rule_id,),
            )
            row = cursor.fetchone()
    if row is None:
        return None
    return {
        "rule_id": row["rule_id"],
        "user_id": row["user_id"],
        "rule_name": row["rule_name"],
        "src": row["src_key"],
        "tgt": row["tgt_key"],
        "unique_number": row["unique_number"],
        "error_handler": row["error_handler"],
    }

#收到任务，写库
def _create_compare_job(rule_id, command="start"):
    rule = _rule(rule_id)
    if rule is None:
        return None
    job_id = uuid.uuid4().hex
    _ensure_tables()
    with _mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO compare_job (
                    job_id, src_key, tgt_key, compare_schema, compare_data,
                    status, command, command_version
                )
                VALUES (%s, %s, %s, 1, 1, 'planning', %s, 1)
                """,
                (job_id, rule["src"], rule["tgt"], command),
            )
    return {
        "job_id": job_id,
        "rule_id": rule_id,
        "src": rule["src"],
        "tgt": rule["tgt"],
        "status": "planning",
        "command": command,
    }


def _latest_job(rule_id):
    rule = _rule(rule_id)
    if rule is None:
        return None
    _ensure_tables()
    with _mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT job_id, status, command, command_version, total_tasks, pending_tasks,
                       running_tasks, success_tasks, failed_tasks, error_message, updated_at
                FROM compare_job
                WHERE src_key=%s AND tgt_key=%s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (rule["src"], rule["tgt"]),
            )
            return cursor.fetchone()


def _update_latest_job_command(rule_id, command):
    job = _latest_job(rule_id)
    if job is None:
        return None
    with _mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE compare_job
                SET command=%s,
                    command_version=command_version + 1,
                    status=CASE
                        WHEN %s IN ('cancel', 'stop', 'kill') THEN 'cancel_requested'
                        ELSE status
                    END
                WHERE job_id=%s
                """,
                (command, command, job["job_id"]),
            )
    return _latest_job(rule_id)


def _json_task(message, task=None, status="success", http_status=200):
    return JsonResponse(
        {
            "status": status,
            "message": message,
            "task": task or {},
        },
        status=http_status,
    )


def get_rule(request):
    return render(request, "rule_home.html", {"rules": _rules()})


def get_rule_test(request, rule_id=None):
    rules = _rules()
    source_map = {item["name"]: item for item in _db_source_options()}
    for rule in rules.values():
        src_info = source_map.get(rule.get("src", ""), {})
        tgt_info = source_map.get(rule.get("tgt", ""), {})
        rule["src_host"] = src_info.get("host", "")
        rule["src_schema"] = src_info.get("schema", "")
        rule["src_type"] = src_info.get("type", "")
        rule["tgt_host"] = tgt_info.get("host", "")
        rule["tgt_schema"] = tgt_info.get("schema", "")
        rule["tgt_type"] = tgt_info.get("type", "")
    selected_rule = rules.get(rule_id) if rule_id else None
    return render(
        request,
        "rule_test.html",
        {"rules": rules, "selected_rule": selected_rule, "selected_rule_id": rule_id},
    )


def jsonrule(request):
    return JsonResponse({"rules": _rules()})


def create_rule(request):
    db_sources = _db_source_options()
    if request.method == "POST":
        rule_id = uuid.uuid4().hex
        user_id = request.POST.get("user_id", "")
        rule_name = request.POST.get("rule_name", "")
        src = request.POST.get("src", "")
        tgt = request.POST.get("tgt", "")
        unique_number = int(request.POST.get("unique_number") or 1)
        error_handler = int(request.POST.get("error_handler") or 3)
        if not rule_name or not src or not tgt:
            return JsonResponse({"status": "error", "message": "rule_name/src/tgt required"}, status=400)
        _ensure_tables()
        with _mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO compare_rule (
                        rule_id, user_id, rule_name, src_key, tgt_key, unique_number, error_handler
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (rule_id, user_id, rule_name, src, tgt, unique_number, error_handler),
                )
        return redirect("/rule")
    return render(
        request,
        "create_rules.html",
        {"db_sources": db_sources, "db_sources_json": json.dumps(db_sources, ensure_ascii=False)},
    )


def del_rule(request, rule_id):
    _ensure_tables()
    with _mysql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM compare_rule WHERE rule_id=%s", (rule_id,))
    return JsonResponse({"status": "success", "message": "rule deleted"})


def ch_rule(request, rule_id=None):
    return JsonResponse({"status": "error", "message": "edit rule is not implemented yet"}, status=501)


def exec_compare(request, rule_id):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "POST required"}, status=405)
    task = _create_compare_job(rule_id, command="start")
    if task is None:
        return _json_task("rule not found", status="error", http_status=404)
    return _json_task("compare job submitted", task=task)


def exec_switch(request, rule_id):
    return _json_task(
        "script execution has been removed; create a planner/worker command instead",
        task={"rule_id": rule_id},
        status="error",
        http_status=410,
    )


def shut_rule(request, rule_id):
    task = _update_latest_job_command(rule_id, "stop")
    if task is None:
        return _json_task("job not found", status="error", http_status=404)
    return _json_task("stop requested", task=task)


def pause_rule(request, rule_id):
    task = _update_latest_job_command(rule_id, "pause")
    if task is None:
        return _json_task("job not found", status="error", http_status=404)
    return _json_task("pause requested", task=task)


def resume_rule(request, rule_id):
    task = _update_latest_job_command(rule_id, "resume")
    if task is None:
        return _json_task("job not found", status="error", http_status=404)
    return _json_task("resume requested", task=task)


def kill_rule(request, rule_id):
    task = _update_latest_job_command(rule_id, "kill")
    if task is None:
        return _json_task("job not found", status="error", http_status=404)
    return _json_task("kill requested", task=task)


def task_status(request, rule_id):
    task = _latest_job(rule_id)
    if task is None:
        return _json_task("job not found", status="error", http_status=404)
    return _json_task("job status", task=task)


def detail(request, rule_id):
    return render(request, "detail.html", {"rules": "{}", "files": "{}", "rule_id": rule_id})


def serve_excel(request):
    if not REPORT_PATH.exists():
        return JsonResponse({"status": "error", "message": "report not found"}, status=404)
    return FileResponse(open(REPORT_PATH, "rb"))


def download_ffile(request):
    return serve_excel(request)


def django_logs(request):
    return render(request, "log_browser.html", {"logs": []})


def hbase_browser(request):
    return render(request, "hbase_browser.html")


def hbase_sources(request):
    return JsonResponse({"status": "success", "sources": hbase_conn.list_hbase_sources()})


def hbase_connect(request):
    dbname = request.GET.get("dbname") or request.POST.get("dbname")
    if not dbname:
        return JsonResponse({"status": "error", "message": "dbname required"}, status=400)
    hc = hbase_conn.connFactory(dbname)
    return JsonResponse({"status": "success", "dbname": dbname, "namespaces": hc.hb_namespace()})


def hbase_tree(request):
    dbname = request.GET.get("dbname") or request.POST.get("dbname")
    namespace = request.GET.get("namespace") or request.POST.get("namespace")
    if not dbname:
        return JsonResponse({"status": "error", "message": "dbname required"}, status=400)
    hc = hbase_conn.connFactory(dbname)
    return JsonResponse({"status": "success", "dbname": dbname, "namespace": namespace, "tables": hc.hb_tab(namespace)})


def hbase_scan(request):
    dbname = request.GET.get("dbname") or request.POST.get("dbname")
    table = request.GET.get("table") or request.POST.get("table")
    namespace = request.GET.get("namespace") or request.POST.get("namespace")
    limit = int(request.GET.get("limit") or request.POST.get("limit") or 200)
    if not dbname or not table:
        return JsonResponse({"status": "error", "message": "dbname/table required"}, status=400)
    hc = hbase_conn.connFactory(dbname)
    return JsonResponse(
        {
            "status": "success",
            "dbname": dbname,
            "namespace": namespace,
            "table": table,
            "limit": limit,
            "result": hc.browse_one_tb(table, limit=limit, namespace=namespace),
        }
    )


def del_config(request, dbname):
    return JsonResponse({"status": "error", "message": "DB.ini editing is disabled in control-plane mode"}, status=410)


def udp_config(request, dbname):
    return JsonResponse({"status": "error", "message": "DB.ini editing is disabled in control-plane mode"}, status=410)


def add_config(request):
    return JsonResponse({"status": "error", "message": "DB.ini editing is disabled in control-plane mode"}, status=410)
