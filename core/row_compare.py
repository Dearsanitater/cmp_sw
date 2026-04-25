from __future__ import annotations

import hashlib
from collections import Counter
from datetime import date, datetime, time
from decimal import Decimal
from typing import Iterable


TYPE_ALIASES = {
    "char": "string",
    "nchar": "string",
    "varchar": "string",
    "nvarchar": "string",
    "text": "string",
    "ntext": "string",
    "mediumtext": "string",
    "longtext": "string",
    "json": "string",
    "int": "number",
    "integer": "number",
    "tinyint": "number",
    "smallint": "number",
    "bigint": "number",
    "decimal": "number",
    "numeric": "number",
    "float": "number",
    "double": "number",
    "real": "number",
    "bit": "boolean",
    "bool": "boolean",
    "boolean": "boolean",
    "date": "datetime",
    "datetime": "datetime",
    "datetime2": "datetime",
    "smalldatetime": "datetime",
    "timestamp": "datetime",
    "time": "datetime",
    "binary": "binary",
    "varbinary": "binary",
    "blob": "binary",
    "tinyblob": "binary",
    "mediumblob": "binary",
    "longblob": "binary",
}


def normalize_col_type(type_name: str | None) -> str:
    if not type_name:
        return "other"
    base_name = str(type_name).strip().lower()
    if " " in base_name:
        base_name = base_name.split(" ", 1)[0]
    return TYPE_ALIASES.get(base_name, base_name)


def normalize_value(value, col_type: str | None = None) -> str:
    normalized_type = normalize_col_type(col_type)

    if value is None:
        return ""
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes):
        if normalized_type == "binary":
            return value.hex().upper()
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex().upper()
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, Decimal)):
        return str(value)
    if isinstance(value, float):
        return format(Decimal(str(value)).normalize(), "f").rstrip("0").rstrip(".") or "0"
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(normalize_value(item, col_type) for item in value) + "]"
    if isinstance(value, dict):
        ordered_items = sorted(value.items(), key=lambda item: str(item[0]))
        return "{" + ",".join(f"{normalize_value(k, col_type)}:{normalize_value(v, col_type)}" for k, v in ordered_items) + "}"

    text = str(value)
    if normalized_type == "number":
        try:
            return format(Decimal(text).normalize(), "f").rstrip("0").rstrip(".") or "0"
        except Exception:
            return text.strip()
    if normalized_type == "datetime":
        return text.strip()
    if normalized_type == "binary":
        return text.strip().upper()
    if normalized_type == "string":
        return text
    return text.strip().upper()


def normalize_row(row: tuple, column_defs: list[dict]) -> list[str]:
    normalized = []
    for value, column in zip(row, column_defs):
        normalized.append(normalize_value(value, column.get("type")))
    return normalized


def hash_row(row: list[str]) -> str:
    hasher = hashlib.blake2b(digest_size=16)
    for cell in row:
        data = cell.encode("utf-8", errors="replace")
        hasher.update(len(data).to_bytes(4, "big", signed=False))
        hasher.update(data)
    return hasher.hexdigest()


def compare_rows(
    src_batches: Iterable[list[tuple]],
    tgt_batches: Iterable[list[tuple]],
    column_defs: list[dict],
    sample_limit: int = 20,
) -> dict:
    src_hash_counter = Counter()
    tgt_hash_counter = Counter()
    src_samples: dict[str, list[str]] = {}
    tgt_samples: dict[str, list[str]] = {}
    src_count = 0
    tgt_count = 0

    for batch in src_batches:
        for row in batch:
            normalized = normalize_row(tuple(row), column_defs)
            row_hash = hash_row(normalized)
            src_hash_counter[row_hash] += 1
            src_samples.setdefault(row_hash, normalized)
            src_count += 1

    for batch in tgt_batches:
        for row in batch:
            normalized = normalize_row(tuple(row), column_defs)
            row_hash = hash_row(normalized)
            tgt_hash_counter[row_hash] += 1
            tgt_samples.setdefault(row_hash, normalized)
            tgt_count += 1

    errors = []
    for row_hash in sorted(set(src_hash_counter) | set(tgt_hash_counter)):
        src_occurs = src_hash_counter.get(row_hash, 0)
        tgt_occurs = tgt_hash_counter.get(row_hash, 0)
        if src_occurs == tgt_occurs:
            continue
        errors.append(
            {
                "hash": row_hash,
                "src_count": src_occurs,
                "tgt_count": tgt_occurs,
                "src_sample": src_samples.get(row_hash),
                "tgt_sample": tgt_samples.get(row_hash),
            }
        )
        if len(errors) >= sample_limit:
            break

    matched = src_hash_counter == tgt_hash_counter
    if matched:
        message = "YES! hash比对通过"
    else:
        message = f"NO! hash比对存在差异，差异哈希样本 {len(errors)} 条"

    return {
        "matched": matched,
        "messages": [message],
        "errors": errors,
        "src_count": src_count,
        "tgt_count": tgt_count,
    }
