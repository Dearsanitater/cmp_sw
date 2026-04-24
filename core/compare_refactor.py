import copy
import decimal
import os
import re
import struct
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta

import cx_Oracle

import numpy
import pandas
from dateutil import parser
from dateutil.tz import tzoffset
from decimal import Decimal

from db_refactor import ConnectionFactory, DialectRegistry, get_db_type



import compare
class ergodic_database(compare.ergodic_database):
    PARALLEL_SCHEMA_TYPES = {
        "sqlserver",
        "oracle",
        "mysql",
        "postgre",
        "clickhouse",
        "db2",
        "dm",
        "hive",
        "ob_oracle",
        "ob_mysql",
    }

    def __init__(self, schema_collect_workers=None):
        super().__init__()
        self._factory = ConnectionFactory()
        self._dialects = DialectRegistry()
        self.schema_collect_workers = self._resolve_schema_collect_workers(schema_collect_workers)

    @staticmethod
    def _resolve_schema_collect_workers(schema_collect_workers):
        if schema_collect_workers is None:
            schema_collect_workers = os.getenv("DB_SCHEMA_WORKERS", "4")
        try:
            return max(1, int(schema_collect_workers))
        except (TypeError, ValueError):
            return 4

    def _apply_role_state(self, role, session, db_type, snapshot):
        target = self.src if role == "src" else self.tgt
        target["tab_num"] = snapshot.tab_num
        target["tab_name"] = copy.deepcopy(snapshot.table_names)
        if role == "src":
            self.conn_src = session
            self.conf_src = db_type
            if snapshot.primary_tables:
                self.src_pri = snapshot.primary_tables
        else:
            self.conn_tgt = session
            self.conf_tgt = db_type
        if snapshot.primary_map:
            self.tgt_pri = snapshot.primary_map

    def _collect_schema(self, source, role, explicit_type=None):
        db_type = explicit_type or get_db_type(source)
        session = self._factory.create_thread_local(source, explicit_type=db_type)
        collector = self._dialects.resolve(db_type)
        snapshot = collector.collect(
            session,
            source,
            worker_count=self.schema_collect_workers,
            session_factory=lambda: self._factory.create(source, explicit_type=db_type),
        )
        session.close()
        self._apply_role_state(role, session, db_type, snapshot)
        return snapshot.tab_num, snapshot.tables

    def src_tab(self, source, ty):
        return self._collect_schema(source, ty, "sqlserver")

    def ora_tab(self, source):
        return self._collect_schema(source, "src" if source == self.src_db else "tgt")

    def mysql_tab(self, source):
        return self._collect_schema(source, "src" if source == self.src_db else "tgt")

    def ck_tab(self, source):
        return self._collect_schema(source, "src" if source == self.src_db else "tgt", "clickhouse")

    def pg_tab(self, source):
        return self._collect_schema(source, "src" if source == self.src_db else "tgt", "postgre")

    def ob_tab(self, source):
        return self._collect_schema(source, "src" if source == self.src_db else "tgt")

    def db2_tab(self, source):
        return self._collect_schema(source, "src" if source == self.src_db else "tgt", "db2")

    def dm_tab(self, source):
        return self._collect_schema(source, "src" if source == self.src_db else "tgt", "dm")

    def hive_tab(self, source=None):
        db_key = source or self.src_db or self.tgt_db or "hive"
        role = "src" if db_key == self.src_db else "tgt"
        return self._collect_schema(db_key, role, "hive")

    def _load_endpoint(self, db_key, role):
        db_type = get_db_type(db_key)
        if db_type in {
            "sqlserver",
            "oracle",
            "mysql",
            "postgre",
            "clickhouse",
            "db2",
            "dm",
            "hive",
            "ob_oracle",
            "ob_mysql",
        }:
            return self._collect_schema(db_key, role, db_type)
        if role == "src":
            self.conf_src = db_type
        else:
            self.conf_tgt = db_type
        if db_type == "hdfs":
            return super().hdfs_tab()
        if db_type == "hbase":
            return super().hbase_tab()
        if db_type == "elastic":
            return super().es_tab()
        raise KeyError(f"Unsupported database type '{db_type}' for compare_refactor")

    def define_type(self):
        src_type = get_db_type(self.src_db)
        tgt_type = get_db_type(self.tgt_db)
        if src_type in self.PARALLEL_SCHEMA_TYPES and tgt_type in self.PARALLEL_SCHEMA_TYPES:
            with ThreadPoolExecutor(max_workers=2) as executor:
                src_future = executor.submit(self._load_endpoint, self.src_db, "src")
                tgt_future = executor.submit(self._load_endpoint, self.tgt_db, "tgt")
                src = src_future.result()
                tgt = tgt_future.result()
            return src, tgt

        src = self._load_endpoint(self.src_db, "src")
        if tgt_type !='hbase':
            tgt = self._load_endpoint(self.tgt_db, "tgt")
        else:
            list_hbasetbname=self._load_endpoint(self.tgt_db, "tgt");tgt_tmp_list=copy.deepcopy(src[1])#src[1]是表名列表，hbase方法仅返回表数量，表名直接拿源端，列信息后续直接拿源端信息
            tgt_tmp_list=[item for item in tgt_tmp_list if item['tab_name'] in list_hbasetbname]
            tgt=(len(list_hbasetbname),tgt_tmp_list)
        return src, tgt
