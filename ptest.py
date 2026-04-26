from __future__ import annotations

import configparser
import os
import sqlite3
import sys
import time
from pathlib import Path

import openpyxl

import compare
import ergodic
import opg


BASE_DIR = Path(__file__).resolve().parent
RULE_CONFIG = BASE_DIR / "resource" / "rule_config.ini"
DB_CONFIG = BASE_DIR / "resource" / "DB.ini"


def _read_rule_config(parser):
    last_error = None
    for encoding in ("utf-8", "gbk", "utf-8-sig"):
        try:
            parser.read(RULE_CONFIG, encoding=encoding)
            return encoding
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    return "utf-8"


def get_rule():
    parser = configparser.RawConfigParser()
    _read_rule_config(parser)
    return {section: dict(parser.items(section)) for section in parser.sections()}


def rule(section, mode="d"):
    rule_parser = configparser.RawConfigParser()
    _read_rule_config(rule_parser)
    db_parser = configparser.RawConfigParser()
    db_parser.read(DB_CONFIG, encoding="utf-8")

    values = dict(rule_parser[f"{section}"])
    src = values["src"]
    tgt = values["tgt"]
    src_type = db_parser.get(src, "type")
    item = {
        "rule": values.get("rule_name"),
        "src": src,
        "tgt": tgt,
        "type": src_type,
        "db": "db2" if src == "db2" else 0,
        "conn": None,
        "rule_id": values.get("rule_id", section),
        "if_unique": values.get("unique_number", "1"),
        "usrid": values.get("user_id", ""),
        "tgt_type": db_parser.get(tgt, "type"),
    }
    if mode == "s":
        conn = opg.MysqlConn(src)
        method_map = {
            "ob_oracle": conn.open_obora,
            "ob_mysql": conn.open,
            "mysql": conn.open,
            "sqlserver": conn.open_mssql,
            "oracle": conn.open_oracle,
            "postgre": conn.open_pg,
            "db2": conn.open_db2_odbc,
        }
        item["conn"] = method_map[src_type]
    return item


def _queue_stdout(rule_id, user_id):
    try:
        from crm.apps.api import apps

        return apps.QueueMaintainer(rule_id, os.getpid(), user_id)
    except Exception:
        return None


def _stop_requested(control):
    if isinstance(control, dict):
        control = control.get("stop_event")
    return bool(control and control.is_set())


def incr_switch(args, control=None):
    old_stdout = sys.stdout
    queue_stdout = _queue_stdout(args["rule_id"], args.get("usrid", ""))
    if queue_stdout is not None:
        sys.stdout = queue_stdout
    try:
        import switch

        conn = args["conn"]()
        p = switch.read_case(int(args["if_unique"]), args["type"], args["db"], switch.db2_case)
        start = time.perf_counter()
        for casefile in p.ergodic(args["type"]):
            if _stop_requested(control):
                print("received stop signal")
                break
            p.analysis(casefile, 1)
            p.execute(conn, control)
        print("switch finished, cost %.2fms" % ((time.perf_counter() - start) * 1000))
    except Exception as exc:
        print(f"switch subprocess error: {exc}")
    finally:
        sys.stdout = old_stdout
        if queue_stdout is not None:
            queue_stdout.stop()


def start_comp(args, control=None):
    old_stdout = sys.stdout
    queue_stdout = _queue_stdout(args["rule_id"], args.get("usrid", ""))
    if queue_stdout is not None:
        sys.stdout = queue_stdout
    try:
        worker = compare.ergodic_database()
        dump_excel = ergodic.get_casefile()
        start = time.monotonic()
        worker.compare(args["src"], args["tgt"], 3, 1, control=control)
        print("compare finished, cost %.2fms" % ((time.monotonic() - start) * 1000))
        worker.dump_e(dump_excel)
    finally:
        sys.stdout = old_stdout
        if queue_stdout is not None:
            queue_stdout.stop()

    report = BASE_DIR / "resource" / "test_report" / "report.xlsx"
    if report.exists():
        wb = openpyxl.load_workbook(report, data_only=True)
        last_sheet = wb.worksheets[-1].title
        conn = sqlite3.connect(BASE_DIR / "identifier.sqlite")
        try:
            cursor = conn.cursor()
            cursor.execute(
                "insert into main.report_map (uid,tbname) values (?, ?)",
                (args["rule_id"], last_sheet),
            )
            conn.commit()
        finally:
            conn.close()
