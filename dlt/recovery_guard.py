"""Prevent an isolated recovery candidate from running restored work.

Presence of the marker, including an empty or malformed marker, fails closed.
This is an application safeguard, not protection against an owner modifying
SQLite directly. There is intentionally no activation path until full recovery
validation and explicit re-enablement are implemented.
"""
from contextlib import closing
from pathlib import Path
import sqlite3


class RecoveryQuarantined(ValueError):
    pass


def require_execution_allowed(db):
    marker = db.execute("SELECT 1 FROM sqlite_master WHERE name='recovery_quarantine' LIMIT 1").fetchone()
    if marker:
        raise RecoveryQuarantined('该数据库是隔离恢复候选，禁止启动服务或执行计划；完整恢复核验及启用流程尚未完成')


def require_path_execution_allowed(path):
    path = Path(path).resolve()
    if not path.exists():
        return
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        require_execution_allowed(db)
