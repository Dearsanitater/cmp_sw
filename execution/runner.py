<<<<<<< ours
from __future__ import annotations

<<<<<<< ours
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from threading import Lock
import traceback

from connector.base import Column, DbConfig
from connector.registry import create_connector, load_config
from core.report_jsonl import JsonlReportWriter
from core.row_compare import compare_rows, normalize_col_type
from .pool import ConnectionPool
from .state import JobState, JobStatus, TableTaskState, TableTaskStatus, utc_now


LEGACY_COLUMN_KEYS = (
    "col_num",
    "col_name",
    "type",
    "len",
    "num_pre",
    "num_scale",
    "time_sh",
    "null",
    "tab_name",
)

SUPPORTED_DB_TYPES = {"sqlserver", "mysql"}


@dataclass(slots=True)
class DatabaseRuntime:
    db_key: str
    config: DbConfig
    schema_override: str | None = None

    @property
    def db_type(self) -> str:
        return self.config.type

    @property
    def schema(self) -> str | None:
        return self.schema_override or self.config.schema

    def open_connector(self):
        return create_connector(self.db_key)


class CompareJobRunner:
    """
    Compare worker 自己只维护执行期内的临时状态。
    真正可共享的状态通过 JSONL 事件落盘，后续可以平滑替换成 DB / MQ / writer-worker。
    """

    def __init__(self, job):
        self.job = job
        self.src_runtime = DatabaseRuntime(job.src_key, load_config(job.src_key), job.src_schema)
        self.tgt_runtime = DatabaseRuntime(job.tgt_key, load_config(job.tgt_key), job.tgt_schema)
        self._validate_job()

        self.state = JobState(
            job_id=job.job_id,
            src_key=job.src_key,
            tgt_key=job.tgt_key,
        )
        self.writer = JsonlReportWriter(job.report_dir, job.job_id)
        self._state_lock = Lock()
        self.src_pool = ConnectionPool(self.src_runtime.open_connector, size=job.workers)
        self.tgt_pool = ConnectionPool(self.tgt_runtime.open_connector, size=job.workers)

    def run(self) -> JobState:
        self._set_job_status(JobStatus.CREATED, "任务已创建")
        try:
            self._set_job_status(JobStatus.PLANNING, "开始采集源端和目标端表结构")
            src_snapshot = self._collect_snapshot(self.src_runtime)
            tgt_snapshot = self._collect_snapshot(self.tgt_runtime)

            with self._state_lock:
                self.state.src_snapshot = src_snapshot
                self.state.tgt_snapshot = tgt_snapshot

            table_tasks = self._build_table_tasks(src_snapshot, tgt_snapshot)
            with self._state_lock:
                self.state.table_tasks = table_tasks

            comparable_tasks = [task for task in table_tasks if task.src_table and task.tgt_table]
            self._set_job_status(JobStatus.READY, f"计划完成，可比对表 {len(comparable_tasks)} 张")

            if comparable_tasks:
                self._set_job_status(JobStatus.RUNNING, "开始执行表级比对")
                self._run_table_tasks(comparable_tasks)

            final_status = self._resolve_job_status()
            self._set_job_status(final_status, self._build_job_summary())
            self._emit_event("job_summary", {"summary": self._build_summary_payload()})
            return self.state
        except Exception as exc:
            self._set_job_status(JobStatus.FAILED, f"任务执行异常: {exc}")
            self._emit_event("job_exception", {"traceback": traceback.format_exc()})
            raise
        finally:
            self.src_pool.close_all()
            self.tgt_pool.close_all()
            self.writer.close()

    def _validate_job(self) -> None:
        if self.src_runtime.db_type not in SUPPORTED_DB_TYPES:
            raise ValueError(f"暂不支持源库类型: {self.src_runtime.db_type}")
        if self.tgt_runtime.db_type not in SUPPORTED_DB_TYPES:
            raise ValueError(f"暂不支持目标库类型: {self.tgt_runtime.db_type}")
        if self.job.workers < 1:
            raise ValueError("workers 至少为 1")
        if self.job.batch_size < 1:
            raise ValueError("batch_size 至少为 1")

    def _collect_snapshot(self, runtime: DatabaseRuntime) -> tuple[int, list[dict]]:
        connector = runtime.open_connector()
        try:
            tables = connector.list_tables(runtime.schema)
            snapshot = []
            for index, table_name in enumerate(tables, start=1):
                columns = connector.get_columns(table_name, runtime.schema)
                snapshot.append(self._to_legacy_table(index, table_name, columns))
            return len(snapshot), snapshot
        finally:
            connector.close()

    def _to_legacy_table(self, index: int, table_name: str, columns: list[Column]) -> dict:
        return {
            "tab_num": index,
            "tab_name": table_name,
            "tab_col": [self._to_legacy_column(table_name, column) for column in columns],
        }

    def _to_legacy_column(self, table_name: str, column: Column) -> dict:
        legacy_column = dict.fromkeys(LEGACY_COLUMN_KEYS)
        legacy_column["col_num"] = column.ordinal
        legacy_column["col_name"] = column.name
        legacy_column["type"] = column.type_name
        legacy_column["len"] = column.length
        legacy_column["num_pre"] = column.precision
        legacy_column["num_scale"] = column.scale
        legacy_column["time_sh"] = column.scale
        if column.nullable is None:
            legacy_column["null"] = None
        else:
            legacy_column["null"] = "YES" if column.nullable else "NO"
        legacy_column["tab_name"] = table_name
        return legacy_column

    def _build_table_tasks(
        self,
        src_snapshot: tuple[int, list[dict]],
        tgt_snapshot: tuple[int, list[dict]],
    ) -> list[TableTaskState]:
        tasks: list[TableTaskState] = []
        tgt_lookup = {table["tab_name"].lower(): table for table in tgt_snapshot[1]}
        matched_names: set[str] = set()

        for src_table in src_snapshot[1]:
            table_name = src_table["tab_name"]
            tgt_table = tgt_lookup.get(table_name.lower())
            if tgt_table is None:
                task = TableTaskState(
                    table_name=table_name,
                    src_table=src_table,
                    tgt_table=None,
                    status=TableTaskStatus.SKIPPED,
                    result={"messages": [f"目标端缺少表 {table_name}"]},
                    finished_at=utc_now(),
                )
                tasks.append(task)
                self.state.src_only_tables.append(table_name)
                self._emit_event("table_skipped", {"table_task": asdict(task)})
                continue

            matched_names.add(table_name.lower())
            tasks.append(TableTaskState(table_name=table_name, src_table=src_table, tgt_table=tgt_table))

        for tgt_table in tgt_snapshot[1]:
            table_name = tgt_table["tab_name"]
            if table_name.lower() in matched_names:
                continue
            task = TableTaskState(
                table_name=table_name,
                src_table=None,
                tgt_table=tgt_table,
                status=TableTaskStatus.SKIPPED,
                result={"messages": [f"源端缺少表 {table_name}"]},
                finished_at=utc_now(),
            )
            tasks.append(task)
            self.state.tgt_only_tables.append(table_name)
            self._emit_event("table_skipped", {"table_task": asdict(task)})

        return tasks

    def _run_table_tasks(self, table_tasks: list[TableTaskState]) -> None:
        with ThreadPoolExecutor(max_workers=self.job.workers) as executor:
            futures = {executor.submit(self._compare_one_table, task): task.table_name for task in table_tasks}
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    self._emit_event(
                        "table_exception",
                        {
                            "table_name": table_name,
                            "error": str(exc),
                            "traceback": traceback.format_exc(),
                        },
                    )

    def _compare_one_table(self, task: TableTaskState) -> None:
        self._set_table_status(task, TableTaskStatus.RUNNING, "开始表级比对")
        src_connector = None
        tgt_connector = None
        src_broken = False
        tgt_broken = False

        try:
            src_connector = self.src_pool.acquire()
            tgt_connector = self.tgt_pool.acquire()

            schema_result = self._compare_schema(task.src_table["tab_col"], task.tgt_table["tab_col"])
            result = {"schema_compare": schema_result, "messages": list(schema_result["messages"])}

            row_result = None
            if self.job.compare_rows and schema_result["src_common_columns"]:
                row_result = self._compare_table_rows(task, schema_result, src_connector, tgt_connector)
                result["row_compare"] = row_result
                result["messages"].extend(row_result["messages"])

            task_status = self._resolve_table_status(schema_result, row_result)
            self._finish_table_task(task, task_status, result)
        except Exception as exc:
            src_broken = True
            tgt_broken = True
            self._finish_table_task(
                task,
                TableTaskStatus.FAILED,
                {"messages": [f"表 {task.table_name} 执行异常: {exc}"]},
                error=traceback.format_exc(),
            )
        finally:
            self.src_pool.release(src_connector, broken=src_broken)
            self.tgt_pool.release(tgt_connector, broken=tgt_broken)

    def _compare_schema(self, src_columns: list[dict], tgt_columns: list[dict]) -> dict:
        tgt_lookup = {column["col_name"].lower(): column for column in tgt_columns}
        src_common_columns = []
        tgt_common_columns = []
        source_only_columns = []
        target_only_columns = []
        type_mismatches = []
        nullable_mismatches = []
        definition_mismatches = []

        for src_column in src_columns:
            tgt_column = tgt_lookup.pop(src_column["col_name"].lower(), None)
            if tgt_column is None:
                source_only_columns.append(src_column["col_name"])
                continue

            src_common_columns.append(src_column)
            tgt_common_columns.append(tgt_column)

            if not self._types_compatible(src_column, tgt_column):
                type_mismatches.append(
                    {
                        "col_name": src_column["col_name"],
                        "src_type": src_column["type"],
                        "tgt_type": tgt_column["type"],
                    }
                )

            if (src_column.get("null") or "").upper() != (tgt_column.get("null") or "").upper():
                nullable_mismatches.append(
                    {
                        "col_name": src_column["col_name"],
                        "src_null": src_column.get("null"),
                        "tgt_null": tgt_column.get("null"),
                    }
                )

            definition_diff = self._compare_column_definition(src_column, tgt_column)
            if definition_diff is not None:
                definition_mismatches.append(definition_diff)

        target_only_columns.extend(column["col_name"] for column in tgt_lookup.values())

        messages = [
            f"字段总数: 源 {len(src_columns)} / 目标 {len(tgt_columns)}",
            f"公共字段: {len(src_common_columns)}",
        ]
        if source_only_columns:
            messages.append(f"目标端缺少字段: {source_only_columns}")
        if target_only_columns:
            messages.append(f"目标端新增字段: {target_only_columns}")
        if type_mismatches:
            messages.append(f"字段类型不一致: {len(type_mismatches)} 处")
        if nullable_mismatches:
            messages.append(f"字段可空性不一致: {len(nullable_mismatches)} 处")
        if definition_mismatches:
            messages.append(f"字段精度/长度不一致: {len(definition_mismatches)} 处")

        matched = not any(
            [
                source_only_columns,
                target_only_columns,
                type_mismatches,
                nullable_mismatches,
                definition_mismatches,
            ]
        )
        if matched:
            messages.append("YES! schema 比对通过")
        else:
            messages.append("NO! schema 比对存在差异")

        return {
            "matched": matched,
            "messages": messages,
            "source_only_columns": source_only_columns,
            "target_only_columns": target_only_columns,
            "type_mismatches": type_mismatches,
            "nullable_mismatches": nullable_mismatches,
            "definition_mismatches": definition_mismatches,
            "src_common_columns": src_common_columns,
            "tgt_common_columns": tgt_common_columns,
        }

    def _types_compatible(self, src_column: dict, tgt_column: dict) -> bool:
        return normalize_col_type(src_column.get("type")) == normalize_col_type(tgt_column.get("type"))

    def _compare_column_definition(self, src_column: dict, tgt_column: dict) -> dict | None:
        family = normalize_col_type(src_column.get("type"))

        if family in {"string", "binary"}:
            src_length = src_column.get("len")
            tgt_length = tgt_column.get("len")
            if src_length is not None and tgt_length is not None and src_length != tgt_length:
                return {
                    "col_name": src_column["col_name"],
                    "field": "len",
                    "src_value": src_length,
                    "tgt_value": tgt_length,
                }

        if family == "number":
            src_pair = (src_column.get("num_pre"), src_column.get("num_scale"))
            tgt_pair = (tgt_column.get("num_pre"), tgt_column.get("num_scale"))
            if all(value is not None for value in src_pair + tgt_pair) and src_pair != tgt_pair:
                return {
                    "col_name": src_column["col_name"],
                    "field": "precision_scale",
                    "src_value": src_pair,
                    "tgt_value": tgt_pair,
                }

        if family == "datetime":
            src_scale = src_column.get("time_sh")
            tgt_scale = tgt_column.get("time_sh")
            if src_scale is not None and tgt_scale is not None and src_scale != tgt_scale:
                return {
                    "col_name": src_column["col_name"],
                    "field": "time_sh",
                    "src_value": src_scale,
                    "tgt_value": tgt_scale,
                }

        return None

    def _compare_table_rows(
        self,
        task: TableTaskState,
        schema_result: dict,
        src_connector,
        tgt_connector,
    ) -> dict:
        src_common_columns = schema_result["src_common_columns"]
        tgt_common_columns = schema_result["tgt_common_columns"]
        src_column_names = [column["col_name"] for column in src_common_columns]
        tgt_column_names = [column["col_name"] for column in tgt_common_columns]
        order_by = src_column_names[0]

        src_batches = src_connector.scan_rows(
            task.src_table["tab_name"],
            src_column_names,
            batch_size=self.job.batch_size,
            schema=self.src_runtime.schema,
            order_by=order_by,
        )
        tgt_batches = tgt_connector.scan_rows(
            task.tgt_table["tab_name"],
            tgt_column_names,
            batch_size=self.job.batch_size,
            schema=self.tgt_runtime.schema,
            order_by=tgt_column_names[0],
        )
        return compare_rows(src_batches, tgt_batches, src_common_columns)

    def _resolve_table_status(self, schema_result: dict, row_result: dict | None) -> str:
        if not schema_result["matched"]:
            return TableTaskStatus.FAILED
        if row_result is not None and not row_result["matched"]:
            return TableTaskStatus.FAILED
        return TableTaskStatus.SUCCESS

    def _resolve_job_status(self) -> str:
        task_statuses = [task.status for task in self.state.table_tasks]
        if not task_statuses:
            return JobStatus.SUCCESS
        if all(status == TableTaskStatus.SUCCESS for status in task_statuses):
            return JobStatus.SUCCESS
        if any(status in {TableTaskStatus.FAILED, TableTaskStatus.SKIPPED} for status in task_statuses):
            if any(status == TableTaskStatus.SUCCESS for status in task_statuses):
                return JobStatus.PARTIAL_FAILED
            return JobStatus.FAILED
        return JobStatus.SUCCESS

    def _build_job_summary(self) -> str:
        summary = self._build_summary_payload()
        return (
            f"任务结束: success={summary['success']}, "
            f"failed={summary['failed']}, skipped={summary['skipped']}"
        )

    def _build_summary_payload(self) -> dict:
        success = sum(task.status == TableTaskStatus.SUCCESS for task in self.state.table_tasks)
        failed = sum(task.status == TableTaskStatus.FAILED for task in self.state.table_tasks)
        skipped = sum(task.status == TableTaskStatus.SKIPPED for task in self.state.table_tasks)
        return {
            "job_id": self.job.job_id,
            "status": self.state.status,
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "src_only_tables": list(self.state.src_only_tables),
            "tgt_only_tables": list(self.state.tgt_only_tables),
        }

    def _set_job_status(self, status: str, message: str) -> None:
        with self._state_lock:
            self.state.status = status
            self.state.updated_at = utc_now()
            self.state.messages.append(message)
            if status in {JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.PARTIAL_FAILED}:
                self.state.finished_at = utc_now()

        self._emit_event(
            "job_status_changed",
            {
                "status": status,
                "message": message,
                "job_state": {
                    "job_id": self.state.job_id,
                    "status": self.state.status,
                    "updated_at": self.state.updated_at,
                    "finished_at": self.state.finished_at,
                },
            },
        )

    def _set_table_status(self, task: TableTaskState, status: str, message: str) -> None:
        with self._state_lock:
            task.status = status
            if status == TableTaskStatus.RUNNING and not task.started_at:
                task.started_at = utc_now()
            if status in {TableTaskStatus.SUCCESS, TableTaskStatus.FAILED, TableTaskStatus.SKIPPED}:
                task.finished_at = utc_now()

        payload = {"table_task": asdict(task), "message": message}
        self._emit_event("table_status_changed", payload)

    def _finish_table_task(
        self,
        task: TableTaskState,
        status: str,
        result: dict,
        error: str | None = None,
    ) -> None:
        with self._state_lock:
            task.result = result
            task.error = error

        self._set_table_status(task, status, result["messages"][-1] if result.get("messages") else status)
        self._emit_event("table_result", {"table_task": asdict(task)})

    def _emit_event(self, event_type: str, payload: dict) -> None:
        self.writer.write_event(
            event_type,
            {
                "job_id": self.job.job_id,
                "emitted_at": utc_now(),
                **payload,
            },
        )
=======
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from threading import Lock
import traceback
from dataclasses import asdict, dataclass
from threading import Lock
import traceback

from connector.base import Column, DbConfig
from connector.registry import create_connector, load_config
from core.report_jsonl import JsonlReportWriter
from core.row_compare import compare_rows, normalize_col_type
from connector.base import Column, DbConfig
from connector.registry import create_connector, load_config
from core.report_jsonl import JsonlReportWriter
from core.row_compare import compare_rows, normalize_col_type
from .pool import ConnectionPool
from .state import JobState, JobStatus, TableTaskState, TableTaskStatus, utc_now


LEGACY_COLUMN_KEYS = (
    "col_num",
    "col_name",
    "type",
    "len",
    "num_pre",
    "num_scale",
    "time_sh",
    "null",
    "tab_name",
)

SUPPORTED_DB_TYPES = {"sqlserver", "mysql"}


@dataclass(slots=True)
class DatabaseRuntime:
    db_key: str
    config: DbConfig
    schema_override: str | None = None

    @property
    def db_type(self) -> str:
        return self.config.type

    @property
    def schema(self) -> str | None:
        return self.schema_override or self.config.schema

    def open_connector(self):
        return create_connector(self.db_key)
from .state import JobState, JobStatus, TableTaskState, TableTaskStatus, utc_now


LEGACY_COLUMN_KEYS = (
    "col_num",
    "col_name",
    "type",
    "len",
    "num_pre",
    "num_scale",
    "time_sh",
    "null",
    "tab_name",
)

SUPPORTED_DB_TYPES = {"sqlserver", "mysql"}


@dataclass(slots=True)
class DatabaseRuntime:
    db_key: str
    config: DbConfig
    schema_override: str | None = None

    @property
    def db_type(self) -> str:
        return self.config.type

    @property
    def schema(self) -> str | None:
        return self.schema_override or self.config.schema

    def open_connector(self):
        return create_connector(self.db_key)


class CompareJobRunner:
    """
    Compare worker 自己只维护执行期内的临时状态。
    真正可共享的状态通过 JSONL 事件落盘，后续可以平滑替换成 DB / MQ / writer-worker。
    """
=======
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from threading import Lock
import traceback

from connector.base import Column, DbConfig
from connector.registry import create_connector, load_config
from core.report_jsonl import JsonlReportWriter
from core.row_compare import compare_rows, normalize_col_type
from .pool import ConnectionPool

>>>>>>> theirs

class CompareJobRunner:#用job初始化类，一个类只对应一个job
    def __init__(self, job):
        self.job = job
<<<<<<< ours
        self.src_runtime = DatabaseRuntime(job.src_key, load_config(job.src_key), job.src_schema)
        self.tgt_runtime = DatabaseRuntime(job.tgt_key, load_config(job.tgt_key), job.tgt_schema)
        self._validate_job()

        self.state = JobState(
            job_id=job.job_id,
            src_key=job.src_key,
            tgt_key=job.tgt_key,
        )
        self.writer = JsonlReportWriter(job.report_dir, job.job_id)
        self._state_lock = Lock()
        self.src_pool = ConnectionPool(self.src_runtime.open_connector, size=job.workers)
        self.tgt_pool = ConnectionPool(self.tgt_runtime.open_connector, size=job.workers)

    def run(self) -> JobState:
        self._set_job_status(JobStatus.CREATED, "任务已创建")
        try:
            self._set_job_status(JobStatus.PLANNING, "开始采集源端和目标端表结构")
            src_snapshot = self._collect_snapshot(self.src_runtime)
            tgt_snapshot = self._collect_snapshot(self.tgt_runtime)

            with self._state_lock:
                self.state.src_snapshot = src_snapshot
                self.state.tgt_snapshot = tgt_snapshot

            table_tasks = self._build_table_tasks(src_snapshot, tgt_snapshot)
            with self._state_lock:
                self.state.table_tasks = table_tasks

            comparable_tasks = [task for task in table_tasks if task.src_table and task.tgt_table]
            self._set_job_status(JobStatus.READY, f"计划完成，可比对表 {len(comparable_tasks)} 张")

            if comparable_tasks:
                self._set_job_status(JobStatus.RUNNING, "开始执行表级比对")
                self._run_table_tasks(comparable_tasks)

            final_status = self._resolve_job_status()
            self._set_job_status(final_status, self._build_job_summary())
            self._emit_event("job_summary", {"summary": self._build_summary_payload()})
            return self.state
        except Exception as exc:
            self._set_job_status(JobStatus.FAILED, f"任务执行异常: {exc}")
            self._emit_event("job_exception", {"traceback": traceback.format_exc()})
            raise
=======
>>>>>>> theirs
        self.src_pool = ConnectionPool(lambda: create_connector(job.src_key), size=job.workers)
        self.tgt_pool = ConnectionPool(lambda: create_connector(job.tgt_key), size=job.workers)

    def run(self):#cli单机并发
        try:
            src = self.src_pool.acquire()#连接池里是对应数据库的类，类中有方法，compare需要自行调用方法拿数据结构再做比对，考虑后续解耦
            tgt = self.tgt_pool.acquire()

            try:
                src_tables = set(src.list_tables())
                tgt_tables = set(tgt.list_tables())
            finally:
                self.src_pool.release(src)#查完了放回队列
                self.tgt_pool.release(tgt)

            common_tables = sorted(src_tables & tgt_tables)
            only_src = sorted(src_tables - tgt_tables)
            only_tgt = sorted(tgt_tables - src_tables)

            print(f"源端独有表: {len(only_src)}")
            print(f"目标端独有表: {len(only_tgt)}")
            print(f"共同表数量: {len(common_tables)}")

            with ThreadPoolExecutor(max_workers=self.job.workers) as executor:
                futures = {
                    executor.submit(self.compare_one_table, table): table
                    for table in common_tables
                }

                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        result = future.result()
                        print(f"[{table}] {result}")
                    except Exception as e:
                        print(f"[{table}] FAILED: {e}")

        finally:
            self.src_pool.close_all()
            self.tgt_pool.close_all()
<<<<<<< ours
            self.writer.close()

    def _validate_job(self) -> None:
        if self.src_runtime.db_type not in SUPPORTED_DB_TYPES:
            raise ValueError(f"暂不支持源库类型: {self.src_runtime.db_type}")
        if self.tgt_runtime.db_type not in SUPPORTED_DB_TYPES:
            raise ValueError(f"暂不支持目标库类型: {self.tgt_runtime.db_type}")
        if self.job.workers < 1:
            raise ValueError("workers 至少为 1")
        if self.job.batch_size < 1:
            raise ValueError("batch_size 至少为 1")

    def _collect_snapshot(self, runtime: DatabaseRuntime) -> tuple[int, list[dict]]:
        connector = runtime.open_connector()
        try:
            tables = connector.list_tables(runtime.schema)
            snapshot = []
            for index, table_name in enumerate(tables, start=1):
                columns = connector.get_columns(table_name, runtime.schema)
                snapshot.append(self._to_legacy_table(index, table_name, columns))
            return len(snapshot), snapshot
        finally:
            connector.close()

    def _to_legacy_table(self, index: int, table_name: str, columns: list[Column]) -> dict:
        return {
            "tab_num": index,
            "tab_name": table_name,
            "tab_col": [self._to_legacy_column(table_name, column) for column in columns],
        }

    def _to_legacy_column(self, table_name: str, column: Column) -> dict:
        legacy_column = dict.fromkeys(LEGACY_COLUMN_KEYS)
        legacy_column["col_num"] = column.ordinal
        legacy_column["col_name"] = column.name
        legacy_column["type"] = column.type_name
        legacy_column["len"] = column.length
        legacy_column["num_pre"] = column.precision
        legacy_column["num_scale"] = column.scale
        legacy_column["time_sh"] = column.scale
        if column.nullable is None:
            legacy_column["null"] = None
        else:
            legacy_column["null"] = "YES" if column.nullable else "NO"
        legacy_column["tab_name"] = table_name
        return legacy_column

    def _build_table_tasks(
        self,
        src_snapshot: tuple[int, list[dict]],
        tgt_snapshot: tuple[int, list[dict]],
    ) -> list[TableTaskState]:
        tasks: list[TableTaskState] = []
        tgt_lookup = {table["tab_name"].lower(): table for table in tgt_snapshot[1]}
        matched_names: set[str] = set()

        for src_table in src_snapshot[1]:
            table_name = src_table["tab_name"]
            tgt_table = tgt_lookup.get(table_name.lower())
            if tgt_table is None:
                task = TableTaskState(
                    table_name=table_name,
                    src_table=src_table,
                    tgt_table=None,
                    status=TableTaskStatus.SKIPPED,
                    result={"messages": [f"目标端缺少表 {table_name}"]},
                    finished_at=utc_now(),
                )
                tasks.append(task)
                self.state.src_only_tables.append(table_name)
                self._emit_event("table_skipped", {"table_task": asdict(task)})
                continue

            matched_names.add(table_name.lower())
            tasks.append(TableTaskState(table_name=table_name, src_table=src_table, tgt_table=tgt_table))

        for tgt_table in tgt_snapshot[1]:
            table_name = tgt_table["tab_name"]
            if table_name.lower() in matched_names:
                continue
            task = TableTaskState(
                table_name=table_name,
                src_table=None,
                tgt_table=tgt_table,
                status=TableTaskStatus.SKIPPED,
                result={"messages": [f"源端缺少表 {table_name}"]},
                finished_at=utc_now(),
            )
            tasks.append(task)
            self.state.tgt_only_tables.append(table_name)
            self._emit_event("table_skipped", {"table_task": asdict(task)})

        return tasks

    def _run_table_tasks(self, table_tasks: list[TableTaskState]) -> None:
        with ThreadPoolExecutor(max_workers=self.job.workers) as executor:
            futures = {executor.submit(self._compare_one_table, task): task.table_name for task in table_tasks}
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    self._emit_event(
                        "table_exception",
                        {
                            "table_name": table_name,
                            "error": str(exc),
                            "traceback": traceback.format_exc(),
                        },
                    )

    def _compare_one_table(self, task: TableTaskState) -> None:
        self._set_table_status(task, TableTaskStatus.RUNNING, "开始表级比对")
        src_connector = None
        tgt_connector = None
        src_broken = False
        tgt_broken = False

        try:
            src_connector = self.src_pool.acquire()
            tgt_connector = self.tgt_pool.acquire()

            schema_result = self._compare_schema(task.src_table["tab_col"], task.tgt_table["tab_col"])
            result = {"schema_compare": schema_result, "messages": list(schema_result["messages"])}

            row_result = None
            if self.job.compare_rows and schema_result["src_common_columns"]:
                row_result = self._compare_table_rows(task, schema_result, src_connector, tgt_connector)
                result["row_compare"] = row_result
                result["messages"].extend(row_result["messages"])

            task_status = self._resolve_table_status(schema_result, row_result)
            self._finish_table_task(task, task_status, result)
        except Exception as exc:
            src_broken = True
            tgt_broken = True
            self._finish_table_task(
                task,
                TableTaskStatus.FAILED,
                {"messages": [f"表 {task.table_name} 执行异常: {exc}"]},
                error=traceback.format_exc(),
            )
=======
>>>>>>> theirs

    def compare_one_table(self, table):#日后留给分布式，对比单表，并发该方法，原理一样：都从pool拿连接
        src = self.src_pool.acquire()
        tgt = self.tgt_pool.acquire()

        try:
            src_cols = src.get_columns(table)
            tgt_cols = tgt.get_columns(table)

            src_col_names = [c.name for c in src_cols]
            tgt_col_names = [c.name for c in tgt_cols]

            common_cols = [c for c in src_col_names if c in tgt_col_names]

            if not common_cols:
                return "NO COMMON COLUMNS"

            src_count = self.count_rows(src, table, common_cols)
            tgt_count = self.count_rows(tgt, table, common_cols)

            if src_count != tgt_count:
                return f"ROW COUNT DIFF src={src_count}, tgt={tgt_count}"

            src_hashes = self.hash_rows(src, table, common_cols)
            tgt_hashes = self.hash_rows(tgt, table, common_cols)

            if src_hashes == tgt_hashes:
                return "OK"

            return "DATA DIFF"

        finally:
<<<<<<< ours
            self.src_pool.release(src_connector, broken=src_broken)
            self.tgt_pool.release(tgt_connector, broken=tgt_broken)

    def _compare_schema(self, src_columns: list[dict], tgt_columns: list[dict]) -> dict:
        tgt_lookup = {column["col_name"].lower(): column for column in tgt_columns}
        src_common_columns = []
        tgt_common_columns = []
        source_only_columns = []
        target_only_columns = []
        type_mismatches = []
        nullable_mismatches = []
        definition_mismatches = []

        for src_column in src_columns:
            tgt_column = tgt_lookup.pop(src_column["col_name"].lower(), None)
            if tgt_column is None:
                source_only_columns.append(src_column["col_name"])
                continue

            src_common_columns.append(src_column)
            tgt_common_columns.append(tgt_column)

            if not self._types_compatible(src_column, tgt_column):
                type_mismatches.append(
                    {
                        "col_name": src_column["col_name"],
                        "src_type": src_column["type"],
                        "tgt_type": tgt_column["type"],
                    }
                )

            if (src_column.get("null") or "").upper() != (tgt_column.get("null") or "").upper():
                nullable_mismatches.append(
                    {
                        "col_name": src_column["col_name"],
                        "src_null": src_column.get("null"),
                        "tgt_null": tgt_column.get("null"),
                    }
                )

            definition_diff = self._compare_column_definition(src_column, tgt_column)
            if definition_diff is not None:
                definition_mismatches.append(definition_diff)

        target_only_columns.extend(column["col_name"] for column in tgt_lookup.values())

        messages = [
            f"字段总数: 源 {len(src_columns)} / 目标 {len(tgt_columns)}",
            f"公共字段: {len(src_common_columns)}",
        ]
        if source_only_columns:
            messages.append(f"目标端缺少字段: {source_only_columns}")
        if target_only_columns:
            messages.append(f"目标端新增字段: {target_only_columns}")
        if type_mismatches:
            messages.append(f"字段类型不一致: {len(type_mismatches)} 处")
        if nullable_mismatches:
            messages.append(f"字段可空性不一致: {len(nullable_mismatches)} 处")
        if definition_mismatches:
            messages.append(f"字段精度/长度不一致: {len(definition_mismatches)} 处")

        matched = not any(
            [
                source_only_columns,
                target_only_columns,
                type_mismatches,
                nullable_mismatches,
                definition_mismatches,
            ]
        )
        if matched:
            messages.append("YES! schema 比对通过")
        else:
            messages.append("NO! schema 比对存在差异")

        return {
            "matched": matched,
            "messages": messages,
            "source_only_columns": source_only_columns,
            "target_only_columns": target_only_columns,
            "type_mismatches": type_mismatches,
            "nullable_mismatches": nullable_mismatches,
            "definition_mismatches": definition_mismatches,
            "src_common_columns": src_common_columns,
            "tgt_common_columns": tgt_common_columns,
        }

    def _types_compatible(self, src_column: dict, tgt_column: dict) -> bool:
        return normalize_col_type(src_column.get("type")) == normalize_col_type(tgt_column.get("type"))

    def _compare_column_definition(self, src_column: dict, tgt_column: dict) -> dict | None:
        family = normalize_col_type(src_column.get("type"))

        if family in {"string", "binary"}:
            src_length = src_column.get("len")
            tgt_length = tgt_column.get("len")
            if src_length is not None and tgt_length is not None and src_length != tgt_length:
                return {
                    "col_name": src_column["col_name"],
                    "field": "len",
                    "src_value": src_length,
                    "tgt_value": tgt_length,
                }

        if family == "number":
            src_pair = (src_column.get("num_pre"), src_column.get("num_scale"))
            tgt_pair = (tgt_column.get("num_pre"), tgt_column.get("num_scale"))
            if all(value is not None for value in src_pair + tgt_pair) and src_pair != tgt_pair:
                return {
                    "col_name": src_column["col_name"],
                    "field": "precision_scale",
                    "src_value": src_pair,
                    "tgt_value": tgt_pair,
                }

        if family == "datetime":
            src_scale = src_column.get("time_sh")
            tgt_scale = tgt_column.get("time_sh")
            if src_scale is not None and tgt_scale is not None and src_scale != tgt_scale:
                return {
                    "col_name": src_column["col_name"],
                    "field": "time_sh",
                    "src_value": src_scale,
                    "tgt_value": tgt_scale,
                }

        return None

    def _compare_table_rows(
        self,
        task: TableTaskState,
        schema_result: dict,
        src_connector,
        tgt_connector,
    ) -> dict:
        src_common_columns = schema_result["src_common_columns"]
        tgt_common_columns = schema_result["tgt_common_columns"]
        src_column_names = [column["col_name"] for column in src_common_columns]
        tgt_column_names = [column["col_name"] for column in tgt_common_columns]
        order_by = src_column_names[0]

        src_batches = src_connector.scan_rows(
            task.src_table["tab_name"],
            src_column_names,
            batch_size=self.job.batch_size,
            schema=self.src_runtime.schema,
            order_by=order_by,
        )
        tgt_batches = tgt_connector.scan_rows(
            task.tgt_table["tab_name"],
            tgt_column_names,
            batch_size=self.job.batch_size,
            schema=self.tgt_runtime.schema,
            order_by=tgt_column_names[0],
        )
        return compare_rows(src_batches, tgt_batches, src_common_columns)

    def _resolve_table_status(self, schema_result: dict, row_result: dict | None) -> str:
        if not schema_result["matched"]:
            return TableTaskStatus.FAILED
        if row_result is not None and not row_result["matched"]:
            return TableTaskStatus.FAILED
        return TableTaskStatus.SUCCESS

    def _resolve_job_status(self) -> str:
        task_statuses = [task.status for task in self.state.table_tasks]
        if not task_statuses:
            return JobStatus.SUCCESS
        if all(status == TableTaskStatus.SUCCESS for status in task_statuses):
            return JobStatus.SUCCESS
        if any(status in {TableTaskStatus.FAILED, TableTaskStatus.SKIPPED} for status in task_statuses):
            if any(status == TableTaskStatus.SUCCESS for status in task_statuses):
                return JobStatus.PARTIAL_FAILED
            return JobStatus.FAILED
        return JobStatus.SUCCESS

    def _build_job_summary(self) -> str:
        summary = self._build_summary_payload()
        return (
            f"任务结束: success={summary['success']}, "
            f"failed={summary['failed']}, skipped={summary['skipped']}"
        )

    def _build_summary_payload(self) -> dict:
        success = sum(task.status == TableTaskStatus.SUCCESS for task in self.state.table_tasks)
        failed = sum(task.status == TableTaskStatus.FAILED for task in self.state.table_tasks)
        skipped = sum(task.status == TableTaskStatus.SKIPPED for task in self.state.table_tasks)
        return {
            "job_id": self.job.job_id,
            "status": self.state.status,
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "src_only_tables": list(self.state.src_only_tables),
            "tgt_only_tables": list(self.state.tgt_only_tables),
        }

    def _set_job_status(self, status: str, message: str) -> None:
        with self._state_lock:
            self.state.status = status
            self.state.updated_at = utc_now()
            self.state.messages.append(message)
            if status in {JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.PARTIAL_FAILED}:
                self.state.finished_at = utc_now()

        self._emit_event(
            "job_status_changed",
            {
                "status": status,
                "message": message,
                "job_state": {
                    "job_id": self.state.job_id,
                    "status": self.state.status,
                    "updated_at": self.state.updated_at,
                    "finished_at": self.state.finished_at,
                },
            },
        )

    def _set_table_status(self, task: TableTaskState, status: str, message: str) -> None:
        with self._state_lock:
            task.status = status
            if status == TableTaskStatus.RUNNING and not task.started_at:
                task.started_at = utc_now()
            if status in {TableTaskStatus.SUCCESS, TableTaskStatus.FAILED, TableTaskStatus.SKIPPED}:
                task.finished_at = utc_now()

        payload = {"table_task": asdict(task), "message": message}
        self._emit_event("table_status_changed", payload)

    def _finish_table_task(
        self,
        task: TableTaskState,
        status: str,
        result: dict,
        error: str | None = None,
    ) -> None:
        with self._state_lock:
            task.result = result
            task.error = error

        self._set_table_status(task, status, result["messages"][-1] if result.get("messages") else status)
        self._emit_event("table_result", {"table_task": asdict(task)})

    def _emit_event(self, event_type: str, payload: dict) -> None:
        self.writer.write_event(
            event_type,
            {
                "job_id": self.job.job_id,
                "emitted_at": utc_now(),
                **payload,
            },
        )
=======
            self.src_pool.release(src)
            self.tgt_pool.release(tgt)
>>>>>>> theirs

    def count_rows(self, connector, table, columns):
        count = 0
        for batch in connector.scan_rows(table, columns, self.job.batch_size):
            count += len(batch)
        return count

    def hash_rows(self, connector, table, columns):
        result = []

        for batch in connector.scan_rows(table, columns, self.job.batch_size):
            for row in batch:
                normalized = tuple("" if v is None else str(v).strip() for v in row)
                result.append(hash(normalized))

        return sorted(result)
<<<<<<< ours
>>>>>>> theirs
=======
>>>>>>> theirs
