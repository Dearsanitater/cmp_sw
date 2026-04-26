from __future__ import annotations

import importlib.util
from pathlib import Path


_LEGACY_OPG = Path(r"D:\PythonObj\pythonProject\opg.py")

if not _LEGACY_OPG.exists():
    raise ImportError(f"legacy opg.py not found: {_LEGACY_OPG}")

_spec = importlib.util.spec_from_file_location("_legacy_opg", _LEGACY_OPG)
_module = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_module)

MysqlConn = _module.MysqlConn
