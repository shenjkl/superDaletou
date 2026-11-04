"""Durable idempotency records for user cancellation requests."""
from contextlib import closing
import json
import sqlite3
from pathlib import Path


OPERATIONS = {'training_cancel', 'backtest_cancel'}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def initialize(db):
    db.execute('''CREATE TABLE IF NOT EXISTS task_cancellations (
        request_key TEXT PRIMARY KEY,
        operation TEXT NOT NULL CHECK(operation IN ('training_cancel','backtest_cancel')),
        request_json BLOB NOT NULL,
        result_json BLOB NOT NULL,
        created_at TEXT NOT NULL
    )''')
    db.execute("CREATE TRIGGER IF NOT EXISTS task_cancellations_no_update BEFORE UPDATE ON task_cancellations "
               "BEGIN SELECT RAISE(ABORT, 'immutable task cancellation'); END")
    db.execute("CREATE TRIGGER IF NOT EXISTS task_cancellations_no_delete BEFORE DELETE ON task_cancellations "
               "BEGIN SELECT RAISE(ABORT, 'immutable task cancellation'); END")


def lookup(db, request_key, operation, request):
    row = db.execute('SELECT operation,request_json,result_json FROM task_cancellations WHERE request_key=?',
                     (request_key,)).fetchone()
    if not row:
        return None
    if row[0] != operation or row[1] != canonical(request):
        raise ValueError('取消请求标识已用于不同目标')
    result = json.loads(row[2])
    if (not isinstance(result, dict) or result.get('request_key') != request_key
            or result.get('operation') != operation or result.get('request') != request
            or result.get('status') not in ('accepted', 'rejected')):
        raise ValueError('已保存的取消请求结果无法核验')
    return result


def save(db, request_key, operation, request, result, created_at):
    if operation not in OPERATIONS or result.get('request_key') != request_key:
        raise ValueError('取消请求内容无效')
    db.execute('INSERT INTO task_cancellations VALUES (?,?,?,?,?)',
               (request_key, operation, canonical(request), canonical(result), created_at))


def get(path, request_key):
    if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
        raise ValueError('取消请求标识无效')
    with closing(sqlite3.connect(Path(path))) as db:
        row = db.execute('SELECT operation,request_json,result_json FROM task_cancellations WHERE request_key=?',
                         (request_key,)).fetchone()
    if not row:
        raise KeyError(request_key)
    operation, request_raw, result_raw = row
    request, result = json.loads(request_raw), json.loads(result_raw)
    if (operation not in OPERATIONS or not isinstance(request, dict)
            or request_raw != canonical(request) or result_raw != canonical(result)
            or not isinstance(request.get('target_request_key'), str)
            or not isinstance(result, dict) or result.get('request_key') != request_key
            or result.get('operation') != operation or result.get('request') != request
            or result.get('status') not in ('accepted', 'rejected')):
        raise ValueError('取消请求记录无法核验')
    return result
