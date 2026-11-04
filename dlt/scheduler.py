"""Persistent single-owner polling schedule for explicitly enabled updates.

No schedule or service is installed on import. SQLite fencing prevents an
expired worker from importing data after another worker has acquired the plan.
"""
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import hashlib
from pathlib import Path
import sqlite3
import time
import uuid
from urllib.parse import urlencode

from dlt.official import BASE, HARD_MAX_BYTES, SourceError, update_snapshot, update_page
from dlt.snapshots import InvalidSnapshot, SnapshotStore, _canonical, parse_snapshot


class ScheduleError(ValueError):
    pass


class ScheduleCancelled(ScheduleError):
    pass


class ScheduleFenced(ScheduleError):
    def __init__(self, request_key, operation, request, retry_of=None):
        super().__init__('该更新操作已被封止，迟到请求不能执行')
        self.request_key = request_key
        self.operation = operation
        self.request = request
        self.retry_of = retry_of


class ScheduleUncertain(RuntimeError):
    """An external side effect could not be verified; preserve the pending request."""


UPDATE_WRITE_OPERATIONS = {'update_plan_create', 'update_plan_edit', 'update_plan_set_enabled',
                           'update_plan_run_now', 'update_run_cancel', 'autostart_set_enabled'}


def _write_key(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ScheduleError('更新计划请求标识无效')
    return value


def _validate_update_write(operation, request):
    if not isinstance(operation, str) or operation not in UPDATE_WRITE_OPERATIONS or not isinstance(request, dict):
        raise ScheduleError('更新计划请求无效')
    if operation == 'update_plan_create':
        if set(request) != {'config'}:
            raise ScheduleError('更新计划创建请求字段无效')
        _validate(request['config'])
    elif operation == 'update_plan_edit':
        if set(request) != {'plan_id', 'expected_sha256', 'config'}:
            raise ScheduleError('更新计划编辑请求字段无效')
        if (not isinstance(request['plan_id'], str) or not request['plan_id'].strip()
                or not isinstance(request['expected_sha256'], str)
                or len(request['expected_sha256']) != 64
                or any(c not in '0123456789abcdef' for c in request['expected_sha256'])):
            raise ScheduleError('更新计划编辑请求标识无效')
        fields = {'start_at', 'interval_seconds', 'timezone_offset_minutes', 'page_size', 'timeout_seconds',
                  'max_bytes', 'retry_count', 'retry_wait_seconds', 'max_pages'}
        if not isinstance(request['config'], dict) or set(request['config']) != fields:
            raise ScheduleError('更新计划编辑配置字段无效')
        _validate(request['config'])
    elif operation == 'update_plan_set_enabled':
        if (set(request) != {'plan_id', 'enabled'} or not isinstance(request['plan_id'], str)
                or not request['plan_id'].strip() or type(request['enabled']) is not bool):
            raise ScheduleError('更新计划启停请求字段无效')
    elif operation == 'update_plan_run_now':
        if set(request) != {'plan_id'} or not isinstance(request['plan_id'], str) or not request['plan_id'].strip():
            raise ScheduleError('立即补查请求字段无效')
    elif operation == 'autostart_set_enabled':
        if (set(request) != {'enabled', 'port'} or type(request['enabled']) is not bool
                or type(request['port']) is not int or not 1 <= request['port'] <= 65535):
            raise ScheduleError('登录启动请求字段无效')
    else:
        if (set(request) != {'plan_id', 'run_id'} or not isinstance(request['plan_id'], str)
                or not request['plan_id'].strip() or not isinstance(request['run_id'], str)
                or not request['run_id'].strip()):
            raise ScheduleError('更新运行取消请求字段无效')
    try:
        _canonical(request)
    except (TypeError, ValueError) as exc:
        raise ScheduleError('更新计划请求不是有效 JSON') from exc
    return request


def _update_write_object(operation, request):
    if operation == 'autostart_set_enabled':
        return ('windows_autostart', 'current_user')
    if operation == 'update_plan_create':
        return ('new_plan', hashlib.sha256(_canonical(request)).hexdigest())
    if operation == 'update_run_cancel':
        return ('update_run', request['plan_id'] + ':' + request['run_id'])
    return ('update_plan', request['plan_id'])


def _lookup_update_write(db, request_key, operation, request, retry_of=None):
    row = db.execute('SELECT operation,request_json,result_json FROM update_write_requests WHERE request_key=?',
                     (request_key,)).fetchone()
    if not row:
        return None
    if row[0] != operation or row[1] != _canonical(request):
        raise ScheduleError('更新计划请求标识已用于不同操作或内容')
    retry = db.execute('SELECT retry_of,operation,request_json FROM update_write_retries WHERE request_key=?',
                       (request_key,)).fetchone()
    if ((retry[0] if retry else None) != retry_of
            or (retry and (retry[1] != operation or retry[2] != _canonical(request)))):
        raise ScheduleError('更新计划请求的关联重试标识不匹配')
    result = json.loads(row[2])
    if not isinstance(result, dict):
        raise ScheduleError('已保存的更新计划结果无法核验')
    return result


def _reject_if_update_write_fenced(db, request_key, operation, request, retry_of=None):
    row = db.execute('SELECT operation,request_json FROM update_write_fences WHERE request_key=?',
                     (request_key,)).fetchone()
    if not row:
        return
    if row != (operation, _canonical(request)):
        raise ScheduleError('更新计划请求标识已用于不同的封止操作或内容')
    saved_retry = db.execute('SELECT retry_of,operation,request_json FROM update_write_retries WHERE request_key=?',
                             (request_key,)).fetchone()
    if ((saved_retry[0] if saved_retry else None) != retry_of
            or (saved_retry and (saved_retry[1] != operation or saved_retry[2] != _canonical(request)))):
        raise ScheduleError('更新计划请求的关联重试标识不匹配')
    raise ScheduleFenced(request_key, operation, request, retry_of)


def _claim_update_retry(db, request_key, operation, request, retry_of, created_at):
    existing = db.execute('SELECT retry_of,operation,request_json FROM update_write_retries WHERE request_key=?',
                          (request_key,)).fetchone()
    if retry_of is None:
        if existing:
            raise ScheduleError('该更新请求必须继续使用已保存的关联重试信息')
        return
    retry_of = _write_key(retry_of)
    if retry_of == request_key:
        raise ScheduleError('更新请求不能关联自身重试')
    original = db.execute('SELECT operation,request_json FROM update_write_fences WHERE request_key=?',
                          (retry_of,)).fetchone()
    if not original or original[0] != operation:
        raise ScheduleError('关联重试必须指向同类且已封止的更新请求')
    original_request = json.loads(original[1])
    if _update_write_object(operation, original_request) != _update_write_object(operation, request):
        raise ScheduleError('关联重试必须作用于原更新计划或运行')
    if existing:
        if existing != (retry_of, operation, _canonical(request)):
            raise ScheduleError('更新请求标识已用于不同的关联重试')
        return
    prior = db.execute('SELECT request_key,operation,request_json FROM update_write_retries WHERE retry_of=?',
                       (retry_of,)).fetchone()
    if prior:
        raise ScheduleError('原更新请求已有关联重试，请先核对该重试')
    db.execute('INSERT INTO update_write_retries VALUES (?,?,?,?,?)',
               (request_key, retry_of, operation, _canonical(request), created_at))


def _save_update_write(db, request_key, operation, request, result, created_at):
    if operation not in UPDATE_WRITE_OPERATIONS or not isinstance(result, dict):
        raise ScheduleError('更新计划请求结果无效')
    db.execute('INSERT INTO update_write_requests VALUES (?,?,?,?,?)',
               (request_key, operation, _canonical(request), _canonical(result), created_at))


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ScheduleError('timezone-aware timestamp required')
    return value.astimezone(timezone.utc)


def _parse(value):
    try:
        return _utc(datetime.fromisoformat(value))
    except (TypeError, ValueError) as exc:
        raise ScheduleError('valid timezone-aware start time required') from exc


def _validate(config):
    if not isinstance(config, dict):
        raise ScheduleError('plan configuration required')
    fields = ('start_at', 'interval_seconds', 'timezone_offset_minutes', 'page_size', 'timeout_seconds',
              'max_bytes', 'retry_count', 'retry_wait_seconds')
    missing = [name for name in fields if name not in config]
    if missing:
        raise ScheduleError('missing plan fields: ' + ', '.join(missing))
    _parse(config['start_at'])
    bounds = {'interval_seconds': (1, 31536000), 'timezone_offset_minutes': (-840, 840),
              'page_size': (1, 100), 'timeout_seconds': (1, 120), 'max_bytes': (1, HARD_MAX_BYTES),
              'retry_count': (0, 5), 'retry_wait_seconds': (1, 300)}
    for name, (low, high) in bounds.items():
        if type(config[name]) is not int or not low <= config[name] <= high:
            raise ScheduleError(f'invalid plan field: {name}')
    if 'run_once' in config and type(config['run_once']) is not bool:
        raise ScheduleError('run_once must be boolean')
    if 'max_pages' in config and (type(config['max_pages']) is not int or not 1 <= config['max_pages'] <= 1000):
        raise ScheduleError('invalid plan field: max_pages')
    return json.loads(_canonical(config))


def _check_change_history(db, plan_id, current_raw):
    """Verify an edited plan's retained configuration chain before reuse."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='update_plan_changes'").fetchone():
        return  # Legacy databases have no change history.
    previous = None
    try:
        for old_sha, before, new_sha, after, old_due, new_due, changed_at in db.execute(
                'SELECT old_sha256,old_config,new_sha256,new_config,old_due,new_due,changed_at '
                'FROM update_plan_changes WHERE plan_id=? ORDER BY seq', (plan_id,)):
            if (hashlib.sha256(before).hexdigest() != old_sha or hashlib.sha256(after).hexdigest() != new_sha
                    or (previous is not None and previous != old_sha)):
                raise ValueError('configuration chain mismatch')
            old = _validate(json.loads(before)); new = _validate(json.loads(after))
            if old.get('run_once') is True or new.get('run_once') is True:
                raise ValueError('one-off plan cannot have edits')
            _parse(old_due); _parse(changed_at)
            if _parse(new_due) != _parse(new['start_at']):
                raise ValueError('configuration due mismatch')
            previous = new_sha
        if previous is not None and previous != hashlib.sha256(current_raw).hexdigest():
            raise ValueError('current configuration mismatch')
    except (ValueError, TypeError, KeyError) as exc:
        raise ScheduleError('计划变更记录不一致，请先检查数据完整性') from exc


class Scheduler:
    def __init__(self, path, *, clock=None):
        self.path = Path(path)
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self):
        return _utc(self.clock())

    def initialize(self):
        SnapshotStore(self.path).initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS update_plans (
                    id TEXT PRIMARY KEY, config_json BLOB NOT NULL, enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
                    next_due TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS update_runs (
                    id TEXT PRIMARY KEY, plan_id TEXT NOT NULL REFERENCES update_plans(id), token TEXT NOT NULL,
                    due_at TEXT NOT NULL, started_at TEXT NOT NULL, lease_until TEXT NOT NULL,
                    status TEXT NOT NULL, ended_at TEXT, snapshot_id TEXT, error TEXT
                );
                CREATE TABLE IF NOT EXISTS update_attempts (
                    run_id TEXT NOT NULL REFERENCES update_runs(id), number INTEGER NOT NULL,
                    at TEXT NOT NULL, status TEXT NOT NULL, detail TEXT, PRIMARY KEY(run_id, number)
                );
                CREATE TABLE IF NOT EXISTS update_followups (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL REFERENCES update_runs(id),
                    status TEXT NOT NULL, details_json BLOB NOT NULL, recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS update_plan_changes (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, plan_id TEXT NOT NULL REFERENCES update_plans(id),
                    old_sha256 TEXT NOT NULL, old_config BLOB NOT NULL, new_sha256 TEXT NOT NULL, new_config BLOB NOT NULL,
                    old_due TEXT NOT NULL, new_due TEXT NOT NULL, changed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS update_run_configs (
                    run_id TEXT PRIMARY KEY REFERENCES update_runs(id), config_sha256 TEXT NOT NULL, config_json BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS update_write_requests (
                    request_key TEXT PRIMARY KEY,
                    operation TEXT NOT NULL CHECK(operation IN ('update_plan_create','update_plan_edit',
                        'update_plan_set_enabled','update_plan_run_now','update_run_cancel','autostart_set_enabled')),
                    request_json BLOB NOT NULL, result_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS update_write_requests_no_update BEFORE UPDATE ON update_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable update write request'); END;
                CREATE TRIGGER IF NOT EXISTS update_write_requests_no_delete BEFORE DELETE ON update_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable update write request'); END;
                CREATE TABLE IF NOT EXISTS update_write_fences (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL CHECK(operation IN (
                        'update_plan_create','update_plan_edit','update_plan_set_enabled','update_plan_run_now','update_run_cancel','autostart_set_enabled')),
                    request_json BLOB NOT NULL, request_sha256 TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS update_write_fences_no_update BEFORE UPDATE ON update_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable update write fence'); END;
                CREATE TRIGGER IF NOT EXISTS update_write_fences_no_delete BEFORE DELETE ON update_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable update write fence'); END;
                CREATE TABLE IF NOT EXISTS update_write_retries (
                    request_key TEXT PRIMARY KEY, retry_of TEXT NOT NULL UNIQUE, operation TEXT NOT NULL,
                    request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS update_write_retries_no_update BEFORE UPDATE ON update_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable update write retry'); END;
                CREATE TRIGGER IF NOT EXISTS update_write_retries_no_delete BEFORE DELETE ON update_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable update write retry'); END;
                CREATE TRIGGER IF NOT EXISTS update_attempts_no_update BEFORE UPDATE ON update_attempts
                    BEGIN SELECT RAISE(ABORT, 'immutable update attempt'); END;
                CREATE TRIGGER IF NOT EXISTS update_attempts_no_delete BEFORE DELETE ON update_attempts
                    BEGIN SELECT RAISE(ABORT, 'immutable update attempt'); END;
                CREATE TRIGGER IF NOT EXISTS update_followups_no_update BEFORE UPDATE ON update_followups
                    BEGIN SELECT RAISE(ABORT, 'immutable update followup'); END;
                CREATE TRIGGER IF NOT EXISTS update_followups_no_delete BEFORE DELETE ON update_followups
                    BEGIN SELECT RAISE(ABORT, 'immutable update followup'); END;
                CREATE TRIGGER IF NOT EXISTS update_plan_changes_no_update BEFORE UPDATE ON update_plan_changes
                    BEGIN SELECT RAISE(ABORT, 'immutable update plan change'); END;
                CREATE TRIGGER IF NOT EXISTS update_plan_changes_no_delete BEFORE DELETE ON update_plan_changes
                    BEGIN SELECT RAISE(ABORT, 'immutable update plan change'); END;
                CREATE TRIGGER IF NOT EXISTS update_run_configs_no_update BEFORE UPDATE ON update_run_configs
                    BEGIN SELECT RAISE(ABORT, 'immutable update run config'); END;
                CREATE TRIGGER IF NOT EXISTS update_run_configs_no_delete BEFORE DELETE ON update_run_configs
                    BEGIN SELECT RAISE(ABORT, 'immutable update run config'); END;
            ''')
            migrations = {
                'update_write_requests': (
                    "CREATE TABLE update_write_requests (request_key TEXT PRIMARY KEY, operation TEXT NOT NULL CHECK(operation IN ("
                    "'update_plan_create','update_plan_edit','update_plan_set_enabled','update_plan_run_now','update_run_cancel','autostart_set_enabled')), "
                    "request_json BLOB NOT NULL, result_json BLOB NOT NULL, created_at TEXT NOT NULL)",
                    'request_key,operation,request_json,result_json,created_at',
                    ('update_write_requests_no_update', 'update_write_requests_no_delete', 'immutable update write request')),
                'update_write_fences': (
                    "CREATE TABLE update_write_fences (request_key TEXT PRIMARY KEY, operation TEXT NOT NULL CHECK(operation IN ("
                    "'update_plan_create','update_plan_edit','update_plan_set_enabled','update_plan_run_now','update_run_cancel','autostart_set_enabled')), "
                    'request_json BLOB NOT NULL, request_sha256 TEXT NOT NULL, created_at TEXT NOT NULL)',
                    'request_key,operation,request_json,request_sha256,created_at',
                    ('update_write_fences_no_update', 'update_write_fences_no_delete', 'immutable update write fence')),
            }
            needed = []
            for table, migration in migrations.items():
                row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                if row and 'autostart_set_enabled' not in row[0]:
                    needed.append((table, migration))
            if needed:
                db.execute('BEGIN IMMEDIATE')
                for table, (create_sql, columns, triggers) in needed:
                    legacy = table + '_legacy_migration'
                    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (legacy,)).fetchone():
                        raise ScheduleError('更新写入日志结构迁移存在残留，请先检查数据库')
                    for trigger in triggers[:2]:
                        db.execute(f'DROP TRIGGER IF EXISTS {trigger}')
                    db.execute(f'ALTER TABLE {table} RENAME TO {legacy}')
                    db.execute(create_sql)
                    db.execute(f'INSERT INTO {table} ({columns}) SELECT {columns} FROM {legacy}')
                    db.execute(f'DROP TABLE {legacy}')
                    for operation, suffix in (('UPDATE', 'no_update'), ('DELETE', 'no_delete')):
                        name = triggers[0] if suffix == 'no_update' else triggers[1]
                        message = triggers[2]
                        db.execute(f"CREATE TRIGGER {name} BEFORE {operation} ON {table} "
                                   f"BEGIN SELECT RAISE(ABORT, '{message}'); END")

    def create_plan(self, config, *, request_key=None, retry_of=None):
        request = {'config': config}
        config = _validate(config)
        _validate_update_write('update_plan_create', request)
        self.initialize()
        if request_key is not None:
            request_key = _write_key(request_key)
        elif retry_of is not None:
            raise ScheduleError('关联重试必须提供新请求标识')
        now = self._now().isoformat()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            if request_key:
                _reject_if_update_write_fenced(db, request_key, 'update_plan_create', request, retry_of)
                old = _lookup_update_write(db, request_key, 'update_plan_create', request, retry_of)
                if old is not None:
                    return old['id']
                _claim_update_retry(db, request_key, 'update_plan_create', request, retry_of, now)
            plan_id = uuid.uuid4().hex
            db.execute('INSERT INTO update_plans VALUES (?, ?, 0, ?)',
                       (plan_id, _canonical(config), _parse(config['start_at']).isoformat()))
            result = {'id': plan_id, 'enabled': False}
            if request_key:
                _save_update_write(db, request_key, 'update_plan_create', request, result, now)
        return plan_id

    def set_enabled(self, plan_id, enabled, *, request_key=None, retry_of=None):
        if type(enabled) is not bool:
            raise ScheduleError('enabled must be boolean')
        if request_key is not None:
            request_key = _write_key(request_key)
        elif retry_of is not None:
            raise ScheduleError('关联重试必须提供新请求标识')
        request = {'plan_id': plan_id, 'enabled': enabled}
        _validate_update_write('update_plan_set_enabled', request)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            if request_key:
                _reject_if_update_write_fenced(db, request_key, 'update_plan_set_enabled', request, retry_of)
                old = _lookup_update_write(db, request_key, 'update_plan_set_enabled', request, retry_of)
                if old is not None:
                    return old
                _claim_update_retry(db, request_key, 'update_plan_set_enabled', request, retry_of,
                                    self._now().isoformat())
            if db.execute('UPDATE update_plans SET enabled=? WHERE id=?', (int(enabled), plan_id)).rowcount != 1:
                raise KeyError(plan_id)
            result = {'plan_id': plan_id, 'enabled': enabled}
            if request_key:
                _save_update_write(db, request_key, 'update_plan_set_enabled', request, result, self._now().isoformat())
        return result

    def create_one_off(self, plan_id, *, request_key=None, retry_of=None):
        """Durably queue one immediate update without altering its source plan."""
        now = self._now()
        if request_key is not None:
            request_key = _write_key(request_key)
        elif retry_of is not None:
            raise ScheduleError('关联重试必须提供新请求标识')
        request = {'plan_id': plan_id}
        _validate_update_write('update_plan_run_now', request)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            if request_key:
                _reject_if_update_write_fenced(db, request_key, 'update_plan_run_now', request, retry_of)
                old = _lookup_update_write(db, request_key, 'update_plan_run_now', request, retry_of)
                if old is not None:
                    return old['id']
                _claim_update_retry(db, request_key, 'update_plan_run_now', request, retry_of, now.isoformat())
            row = db.execute('SELECT config_json FROM update_plans WHERE id=?', (plan_id,)).fetchone()
            if not row:
                raise KeyError(plan_id)
            _check_change_history(db, plan_id, row[0])
            config = _validate({**json.loads(row[0]), 'start_at': now.isoformat(),
                                'run_once': True, 'parent_plan_id': plan_id})
            ident = uuid.uuid4().hex
            db.execute('INSERT INTO update_plans VALUES (?, ?, 1, ?)', (ident, _canonical(config), now.isoformat()))
            result = {'id': ident, 'status': 'queued', 'plan_id': plan_id}
            if request_key:
                _save_update_write(db, request_key, 'update_plan_run_now', request, result, now.isoformat())
        return ident

    def edit_plan(self, plan_id, config, *, expected_sha256, request_key=None, retry_of=None):
        """Change a paused recurring plan while preserving its previous config."""
        from dlt.recovery_guard import require_execution_allowed, require_path_execution_allowed
        require_path_execution_allowed(self.path)
        if (not isinstance(expected_sha256, str) or len(expected_sha256) != 64
                or any(c not in '0123456789abcdef' for c in expected_sha256)):
            raise ScheduleError('需要当前计划配置摘要，请重新加载')
        fields = {'start_at', 'interval_seconds', 'timezone_offset_minutes', 'page_size', 'timeout_seconds',
                  'max_bytes', 'retry_count', 'retry_wait_seconds', 'max_pages'}
        if not isinstance(config, dict) or set(config) != fields:
            raise ScheduleError('计划配置字段不完整或包含不允许修改的字段')
        request_config = config
        config = _validate(config)
        if request_key is not None:
            request_key = _write_key(request_key)
        elif retry_of is not None:
            raise ScheduleError('关联重试必须提供新请求标识')
        request = {'plan_id': plan_id, 'expected_sha256': expected_sha256, 'config': request_config}
        _validate_update_write('update_plan_edit', request)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            if request_key:
                _reject_if_update_write_fenced(db, request_key, 'update_plan_edit', request, retry_of)
                old_result = _lookup_update_write(db, request_key, 'update_plan_edit', request, retry_of)
                if old_result is not None:
                    return old_result
                _claim_update_retry(db, request_key, 'update_plan_edit', request, retry_of,
                                    self._now().isoformat())
            row = db.execute('SELECT config_json,enabled,next_due FROM update_plans WHERE id=?', (plan_id,)).fetchone()
            if not row:
                raise KeyError(plan_id)
            old, enabled, old_due = row
            if hashlib.sha256(old).hexdigest() != expected_sha256:
                raise ScheduleError('计划配置已变化，请重新加载后再编辑')
            old_config = _validate(json.loads(old))
            _parse(old_due)
            _check_change_history(db, plan_id, old)
            if old_config.get('run_once') is True:
                raise ScheduleError('一次性补查不能编辑')
            if enabled:
                raise ScheduleError('请先暂停计划，再编辑配置')
            if db.execute("SELECT 1 FROM update_runs WHERE plan_id=? AND status IN ('running','cancelling')", (plan_id,)).fetchone():
                raise ScheduleError('计划仍有运行中的任务，请等待结束或恢复中断任务')
            updated = _validate({**old_config, **config})
            new = _canonical(updated)
            if new == old:
                result = {'changed': False, 'config_sha256': expected_sha256}
            else:
                new_due = _parse(updated['start_at']).isoformat()
                digest = hashlib.sha256(new).hexdigest()
                db.execute('INSERT INTO update_plan_changes(plan_id,old_sha256,old_config,new_sha256,new_config,old_due,new_due,changed_at) '
                           'VALUES (?,?,?,?,?,?,?,?)',
                           (plan_id, expected_sha256, old, digest, new, old_due, new_due, self._now().isoformat()))
                db.execute('UPDATE update_plans SET config_json=?,next_due=? WHERE id=?', (new, new_due, plan_id))
                result = {'changed': True, 'config_sha256': digest}
            if request_key:
                _save_update_write(db, request_key, 'update_plan_edit', request, result, self._now().isoformat())
        return result

    def list_plans(self):
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT id, config_json, enabled, next_due FROM update_plans ORDER BY id').fetchall()
        return [{'id': ident, 'config': json.loads(config), 'config_sha256': hashlib.sha256(config).hexdigest(),
                 'enabled': bool(enabled), 'next_due': due}
                for ident, config, enabled, due in rows]

    def list_changes(self, plan_id):
        with closing(sqlite3.connect(self.path)) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='update_plan_changes'").fetchone():
                return []
            db.execute('BEGIN')
            current = db.execute('SELECT config_json FROM update_plans WHERE id=?', (plan_id,)).fetchone()
            if not current:
                raise KeyError(plan_id)
            rows = db.execute('SELECT seq,old_sha256,old_config,new_sha256,new_config,changed_at '
                              'FROM update_plan_changes WHERE plan_id=? ORDER BY seq', (plan_id,)).fetchall()
        chain_ok = (not rows or (isinstance(current[0], bytes) and all(rows[i][3] == rows[i+1][1] for i in range(len(rows)-1))
                                and rows[-1][3] == hashlib.sha256(current[0]).hexdigest()))
        result = []
        for seq, old_sha, old_raw, new_sha, new_raw, changed_at in rows:
            try:
                if (not chain_ok or hashlib.sha256(old_raw).hexdigest() != old_sha
                        or hashlib.sha256(new_raw).hexdigest() != new_sha):
                    raise ValueError('configuration digest mismatch')
                old, new = _validate(json.loads(old_raw)), _validate(json.loads(new_raw))
                _parse(changed_at)
                result.append({'seq': seq, 'status': 'recorded', 'changed_at': changed_at,
                               'old_config': old, 'new_config': new})
            except (ValueError, TypeError):
                result.append({'seq': seq, 'status': 'review_required', 'changed_at': None})
        return result

    def claim(self, plan_id):
        now = self._now()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            plan = db.execute('SELECT config_json, enabled, next_due FROM update_plans WHERE id=?', (plan_id,)).fetchone()
            if plan is None:
                raise KeyError(plan_id)
            try:
                config = _validate(json.loads(plan[0]))
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                raise ScheduleError(f'saved plan configuration is invalid: {exc}') from exc
            _check_change_history(db, plan_id, plan[0])
            enabled, due = bool(plan[1]), _parse(plan[2])
            active = db.execute("SELECT id, lease_until, status FROM update_runs WHERE plan_id=? AND status IN ('running','cancelling')", (plan_id,)).fetchone()
            recovered = False
            if active:
                if _parse(active[1]) > now:
                    return None
                if active[2] == 'cancelling':
                    db.execute("UPDATE update_runs SET status='cancelled',ended_at=?,error=? WHERE id=? AND status='cancelling'",
                               (now.isoformat(), '用户取消；原执行器已失效', active[0]))
                    if config.get('run_once') is True:
                        return None
                else:
                    db.execute("UPDATE update_runs SET status='interrupted', ended_at=?, error=? WHERE id=? AND status='running'",
                               (now.isoformat(), 'worker lease expired; recovery required', active[0]))
                    recovered = True
            one_off = config.get('run_once') is True
            if (not enabled and not (recovered and one_off)) or (not recovered and due > now):
                return None
            # Coalesce missed intervals into one latest-page update.
            if due <= now:
                steps = (now-due) // timedelta(seconds=config['interval_seconds']) + 1
                next_due = due + timedelta(seconds=config['interval_seconds']*steps)
                db.execute('UPDATE update_plans SET next_due=? WHERE id=?', (next_due.isoformat(), plan_id))
            if one_off:
                db.execute('UPDATE update_plans SET enabled=0 WHERE id=?', (plan_id,))
            run_id, token = uuid.uuid4().hex, uuid.uuid4().hex
            duration = (config['timeout_seconds']*(config['retry_count']+1)
                        + config['retry_wait_seconds']*config['retry_count'] + 30)
            lease = now + timedelta(seconds=duration)
            db.execute("INSERT INTO update_runs VALUES (?, ?, ?, ?, ?, ?, 'running', NULL, NULL, NULL)",
                       (run_id, plan_id, token, due.isoformat(), now.isoformat(), lease.isoformat()))
            config_sha256 = hashlib.sha256(plan[0]).hexdigest()
            db.execute('INSERT INTO update_run_configs VALUES (?,?,?)', (run_id, config_sha256, plan[0]))
        return {'run_id': run_id, 'plan_id': plan_id, 'token': token, 'config': config,
                'config_sha256': config_sha256, 'recovered': recovered}

    def _guard(self, claim):
        def guard(db):
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            row = db.execute('SELECT token, status, lease_until FROM update_runs WHERE id=?', (claim['run_id'],)).fetchone()
            if row and row[0] == claim['token'] and row[1] in ('cancelling', 'cancelled'):
                raise ScheduleCancelled('更新运行已取消')
            if not row or row[0] != claim['token'] or row[1] != 'running' or _parse(row[2]) <= self._now():
                raise ScheduleError('execution ownership expired or replaced')
            if 'config_sha256' in claim:
                saved = db.execute('SELECT config_sha256,config_json FROM update_run_configs WHERE run_id=?', (claim['run_id'],)).fetchone()
                if (not saved or not isinstance(saved[1], bytes) or saved[0] != claim['config_sha256'] or hashlib.sha256(saved[1]).hexdigest() != saved[0]
                        or _canonical(claim['config']) != saved[1]):
                    raise ScheduleError('运行配置记录不一致，无法继续发布')
        return guard

    def cancel_run(self, plan_id, run_id, *, request_key=None, retry_of=None):
        """Request cancellation without changing the recurring plan's state."""
        from dlt.recovery_guard import require_execution_allowed
        if request_key is not None:
            request_key = _write_key(request_key)
        request = {'plan_id': plan_id, 'run_id': run_id}
        _validate_update_write('update_run_cancel', request)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            if request_key:
                _reject_if_update_write_fenced(db, request_key, 'update_run_cancel', request, retry_of)
                old = _lookup_update_write(db, request_key, 'update_run_cancel', request, retry_of)
                if old is not None:
                    return old
                _claim_update_retry(db, request_key, 'update_run_cancel', request, retry_of,
                                    self._now().isoformat())
            elif retry_of is not None:
                raise ScheduleError('关联重试必须提供新请求标识')
            row = db.execute('SELECT status FROM update_runs WHERE id=? AND plan_id=?', (run_id, plan_id)).fetchone()
            if not row:
                raise KeyError(run_id)
            if row[0] == 'running':
                db.execute("UPDATE update_runs SET status='cancelling',error=? WHERE id=? AND status='running'",
                           ('用户请求取消', run_id))
                status = 'cancelling'
            else:
                status = row[0]
            result = {'plan_id': plan_id, 'run_id': run_id, 'status': status}
            if request_key:
                _save_update_write(db, request_key, 'update_run_cancel', request, result, self._now().isoformat())
            return result

    def set_autostart_enabled(self, enabled, port, *, request_key, retry_of=None):
        """Apply and verify a registry change while holding the request/fence write lock."""
        request_key = _write_key(request_key)
        request = _validate_update_write('autostart_set_enabled', {'enabled': enabled, 'port': port})
        if retry_of is not None:
            retry_of = _write_key(retry_of)
        self.initialize()
        now = self._now().isoformat()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            _reject_if_update_write_fenced(db, request_key, 'autostart_set_enabled', request, retry_of)
            old = _lookup_update_write(db, request_key, 'autostart_set_enabled', request, retry_of)
            if old is not None:
                return old['autostart']
            _claim_update_retry(db, request_key, 'autostart_set_enabled', request, retry_of, now)
            from dlt.autostart import AutostartError, set_enabled, status
            try:
                set_enabled(self.path, port, enabled)
            except AutostartError as exc:
                observed = status(self.path, port)
                if observed.get('state') in ('enabled', 'disabled') and observed['state'] == ('enabled' if enabled else 'disabled'):
                    pass  # The registry write may have succeeded before reporting an OS error.
                elif observed.get('state') == 'unavailable':
                    raise ScheduleUncertain('登录启动修改结果无法核验') from exc
                else:
                    raise ScheduleError(str(exc)) from exc
            observed = status(self.path, port)
            if observed.get('state') == 'unavailable':
                raise ScheduleUncertain('登录启动修改结果无法核验')
            expected_state = 'enabled' if enabled else 'disabled'
            if observed.get('state') != expected_state:
                raise ScheduleError(observed.get('reason') or '登录启动状态与请求不一致')
            result = {'autostart': observed}
            _save_update_write(db, request_key, 'autostart_set_enabled', request, result, now)
            return observed

    def fence_write(self, request_key, operation, request, *, retry_of=None, port=None):
        """Atomically fence an uncertain write against its late request."""
        request_key = _write_key(request_key)
        request = _validate_update_write(operation, request)
        if retry_of is not None:
            retry_of = _write_key(retry_of)
        self.initialize()
        digest = hashlib.sha256(_canonical({'operation': operation, 'request': request})).hexdigest()
        now = self._now().isoformat()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            accepted = db.execute('SELECT operation,request_json,result_json FROM update_write_requests WHERE request_key=?',
                                  (request_key,)).fetchone()
            if accepted:
                result = _lookup_update_write(db, request_key, operation, request, retry_of)
                if result is None:
                    raise ScheduleError('已接受请求无法核验')
                return {'request_key': request_key, 'operation': operation, 'request': request,
                        'result': result, 'status': 'accepted'}
            existing = db.execute('SELECT operation,request_json,request_sha256,created_at '
                                  'FROM update_write_fences WHERE request_key=?', (request_key,)).fetchone()
            if existing:
                if existing[:3] != (operation, _canonical(request), digest):
                    raise ScheduleError('封止请求标识已用于不同操作或内容')
                _claim_update_retry(db, request_key, operation, request, retry_of, existing[3])
                return {'request_key': request_key, 'operation': operation, 'request': request,
                        'request_sha256': digest, 'retry_of': retry_of,
                        'created_at': existing[3], 'status': 'fenced'}
            if operation == 'autostart_set_enabled':
                if request['port'] != port:
                    raise ScheduleError('更新启动服务端口已变化，原请求不能在新实例上封止')
                from dlt.autostart import status as read_autostart
                observed = read_autostart(self.path, port)
                if observed.get('state') == 'unavailable':
                    raise ScheduleUncertain('登录启动状态无法核验，不能封止请求')
                if observed.get('state') == ('enabled' if request['enabled'] else 'disabled'):
                    _claim_update_retry(db, request_key, operation, request, retry_of, now)
                    result = {'autostart': observed}
                    _save_update_write(db, request_key, operation, request, result, now)
                    return {'request_key': request_key, 'operation': operation, 'request': request,
                            'result': result, 'retry_of': retry_of, 'created_at': now, 'status': 'accepted'}
            _claim_update_retry(db, request_key, operation, request, retry_of, now)
            db.execute('INSERT INTO update_write_fences VALUES (?,?,?,?,?)',
                       (request_key, operation, _canonical(request), digest, now))
        return {'request_key': request_key, 'operation': operation, 'request': request,
                'request_sha256': digest, 'retry_of': retry_of, 'created_at': now, 'status': 'fenced'}

    def write_request(self, request_key):
        _write_key(request_key)
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT operation,request_json,result_json,created_at '
                             'FROM update_write_requests WHERE request_key=?', (request_key,)).fetchone()
            fence = db.execute('SELECT operation,request_json,request_sha256,created_at '
                               'FROM update_write_fences WHERE request_key=?', (request_key,)).fetchone()
            retry = db.execute('SELECT retry_of,operation,request_json FROM update_write_retries WHERE request_key=?',
                               (request_key,)).fetchone()
        if row and fence:
            raise ScheduleError('更新请求同时存在接受和封止记录')
        if row:
            operation, request_raw, result_raw, created_at = row
            request, result = json.loads(request_raw), json.loads(result_raw)
            if (operation not in UPDATE_WRITE_OPERATIONS or request_raw != _canonical(request)
                    or result_raw != _canonical(result) or not isinstance(request, dict) or not isinstance(result, dict)):
                raise ScheduleError('更新计划请求记录无法核验')
            retry_of = retry[0] if retry else None
            if retry and (retry[1] != operation or retry[2] != request_raw):
                raise ScheduleError('更新计划关联重试记录无法核验')
            return {'request_key': request_key, 'operation': operation, 'request': request,
                    'result': result, 'retry_of': retry_of, 'created_at': created_at, 'status': 'accepted'}
        if fence:
            operation, request_raw, digest, created_at = fence
            request = json.loads(request_raw)
            expected = hashlib.sha256(_canonical({'operation': operation, 'request': request})).hexdigest()
            if (operation not in UPDATE_WRITE_OPERATIONS or not isinstance(request, dict)
                    or request_raw != _canonical(request) or digest != expected):
                raise ScheduleError('更新计划封止记录无法核验')
            _validate_update_write(operation, request)
            retry_of = retry[0] if retry else None
            if retry and (retry[1] != operation or retry[2] != request_raw):
                raise ScheduleError('更新计划关联重试记录无法核验')
            return {'request_key': request_key, 'operation': operation, 'request': request,
                    'request_sha256': digest, 'retry_of': retry_of,
                    'created_at': created_at, 'status': 'fenced'}
        raise KeyError(request_key)

    def _finish_cancelled(self, claim):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            db.execute("UPDATE update_runs SET status='cancelled',ended_at=?,error=? "
                       "WHERE id=? AND token=? AND status='cancelling'",
                       (self._now().isoformat(), '用户取消；已提交的快照仍保留', claim['run_id'], claim['token']))

    def _check_claim(self, claim):
        with closing(sqlite3.connect(self.path)) as db:
            self._guard(claim)(db)

    def _wait_retry(self, claim, seconds, sleep):
        remaining = seconds
        while remaining > 0:
            self._check_claim(claim)
            step = min(1, remaining)
            sleep(step)
            remaining -= step
        self._check_claim(claim)

    def _record_attempt(self, claim, number, status, detail):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._guard(claim)(db)
            db.execute('INSERT INTO update_attempts VALUES (?, ?, ?, ?, ?)',
                       (claim['run_id'], number, self._now().isoformat(), status, str(detail)[:2000]))

    def _finish(self, claim, status, *, snapshot_id=None, error=None):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._guard(claim)(db)
            db.execute('UPDATE update_runs SET status=?, ended_at=?, snapshot_id=?, error=? WHERE id=?',
                       (status, self._now().isoformat(), snapshot_id, error, claim['run_id']))

    def _renew_lease(self, claim, *, wait_seconds=0):
        """Renew only a live owner before starting one bounded page request."""
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._guard(claim)(db)
            old = _parse(db.execute('SELECT lease_until FROM update_runs WHERE id=?', (claim['run_id'],)).fetchone()[0])
            until = max(old, self._now() + timedelta(seconds=wait_seconds + claim['config']['timeout_seconds'] + 30))
            db.execute('UPDATE update_runs SET lease_until=? WHERE id=?', (until.isoformat(), claim['run_id']))

    def _missing_older_targets(self, snapshot_id):
        """Find locally unobserved registered issues older than the latest page."""
        from dlt.prospective_settlement import PaperResults, PaperResultError
        paper = PaperResults(self.path, clock=self.clock)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='fixed_experiments'").fetchone():
                return {}
            oldest = db.execute('SELECT MIN(draw_date) FROM draw_versions WHERE snapshot_id=?', (snapshot_id,)).fetchone()[0]
            if not oldest:
                raise SourceError('latest snapshot has no draw rows')
            targets = {}
            for digest, raw in db.execute('SELECT content_sha256,content_json FROM fixed_experiments').fetchall():
                try:
                    if not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != digest:
                        continue  # follow-up reports damaged registrations separately
                    content = json.loads(raw)
                    issue = content['protocol']['target_issue']
                    calendar_id, cal, _ = paper.experiments._calendar_for_fixed(db, content)
                    date = cal['extracted']['draw_date']
                    cutoff = _parse(cal['extracted']['cutoff_at'])
                    now = self._now()
                    if date >= oldest or now < cutoff:
                        continue
                    found = False
                    for (sid,) in db.execute('SELECT snapshot_id FROM draw_versions WHERE issue=?', (issue,)).fetchall():
                        try:
                            paper._observed_draw(db, sid, issue, _parse(content['registered_at']), cutoff, now)
                            found = True
                            break
                        except PaperResultError:
                            continue
                    if not found:
                        token = hashlib.sha256(_canonical({'fixed': digest, 'calendar': calendar_id})).hexdigest()
                        targets.setdefault(issue, set()).add(token)
                except (ValueError, KeyError, TypeError, AttributeError):
                    continue  # follow-up will retain the problem rather than infer a target
            return {issue: sorted(tokens) for issue, tokens in sorted(targets.items())}

    def _page_identity(self, snapshot_id, page_no, page_size):
        """Bind a cursor anchor to retained source bytes and draw projections."""
        source = BASE + '?' + urlencode({'gameNo': 85, 'provinceId': 0, 'pageSize': page_size,
                                        'isVerify': 1, 'pageNo': page_no})
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            snapshot = db.execute('SELECT canonical_json,row_count FROM snapshots WHERE id=?', (snapshot_id,)).fetchone()
            observations = db.execute('SELECT raw_sha256,raw_payload FROM observations WHERE snapshot_id=? AND source=?',
                                      (snapshot_id, source)).fetchall()
            projected = db.execute('SELECT issue,row_json,draw_date FROM draw_versions WHERE snapshot_id=? ORDER BY issue',
                                   (snapshot_id,)).fetchall()
        if not snapshot or not isinstance(snapshot[0], bytes) or hashlib.sha256(snapshot[0]).hexdigest() != snapshot_id:
            raise SourceError('历史锚点快照损坏')
        for raw_hash, raw in observations:
            if not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != raw_hash:
                continue
            try:
                payload, rows = parse_snapshot(raw)
                expected = sorted((row['issue'], _canonical(row['raw_row']), row['draw_date']) for row in rows)
                if (_canonical(payload) == snapshot[0] and len(rows) == snapshot[1]
                        and expected == projected and len(rows) <= page_size):
                    signature = hashlib.sha256(_canonical([[issue, hashlib.sha256(item).hexdigest()]
                                                           for issue, item, _ in expected])).hexdigest()
                    dates = [day for _, _, day in expected]
                    return signature, {issue for issue, _, _ in expected}, min(dates), max(dates)
            except (ValueError, TypeError):
                continue
        raise SourceError('历史锚点原文、页码或开奖投影不一致')

    def _previous_cursor(self, claim, targets):
        root = claim['config'].get('parent_plan_id', claim['plan_id'])
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            plans = []
            for ident, raw in db.execute('SELECT id,config_json FROM update_plans').fetchall():
                try:
                    cfg = _validate(json.loads(raw))
                    if (ident == root or cfg.get('parent_plan_id') == root) and cfg['page_size'] == claim['config']['page_size']:
                        plans.append(ident)
                except (ValueError, TypeError):
                    continue
            rows = db.execute("SELECT r.plan_id,a.detail FROM update_runs r JOIN update_attempts a ON a.run_id=r.id "
                              "WHERE r.status='success' AND a.status='success' ORDER BY r.rowid DESC, a.number DESC").fetchall()
        for plan_id, detail in rows:
            if plan_id not in plans:
                continue
            try:
                cursor = json.loads(detail)['history_cursor']
                page = cursor['page_no']
                target_hash = hashlib.sha256(_canonical(targets)).hexdigest()
                if (cursor['version'] != 1 or cursor['targets_sha256'] != target_hash or type(page) is not int
                        or not 2 <= page < 1000 or cursor['page_size'] != claim['config']['page_size']):
                    return None
                fingerprint, issues, _, _ = self._page_identity(cursor['snapshot_id'], page, cursor['page_size'])
                return cursor if fingerprint == cursor['fingerprint'] and issues else None
            except (ValueError, KeyError, TypeError, AttributeError, SourceError):
                return None
        return None

    def _fetch_missing_pages(self, claim, latest, *, page_updater, sleep):
        config = claim['config']
        with closing(sqlite3.connect(self.path)) as db:
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='fixed_experiments'").fetchone()
            if not exists or not db.execute('SELECT 1 FROM fixed_experiments LIMIT 1').fetchone():
                return None, None
        try:
            _, seen, oldest, _ = self._page_identity(latest['snapshot_id'], 1, config['page_size'])
        except SourceError as exc:
            return f'最新页来源或内容核验失败，无法继续历史补查：{exc}', None
        targets = self._missing_older_targets(latest['snapshot_id'])
        missing = set(targets)
        if not missing:
            return None, None
        limit = config.get('max_pages', 10)
        budget = limit - 1  # latest page has already been requested

        def fetch(number):
            for attempt in range(config['retry_count'] + 1):
                self._renew_lease(claim)
                try:
                    result = page_updater(SnapshotStore(self.path), page_no=number,
                                          page_size=config['page_size'], timeout_seconds=config['timeout_seconds'],
                                          max_bytes=config['max_bytes'], transaction_guard=self._guard(claim))
                    fingerprint, issues, first_day, last_day = self._page_identity(result['snapshot_id'], number, config['page_size'])
                    return result, fingerprint, issues, first_day, last_day, None
                except (SourceError, InvalidSnapshot, OSError) as exc:
                    if attempt == config['retry_count']:
                        return None, None, None, None, None, f'历史第 {number} 页抓取或核验失败：{str(exc)[:500]}'
                    self._renew_lease(claim, wait_seconds=config['retry_wait_seconds'])
                    self._wait_retry(claim, config['retry_wait_seconds'], sleep)

        cursor = self._previous_cursor(claim, targets)
        next_page = 2
        if cursor and budget:
            number = cursor['page_no']
            result, fingerprint, issues, first_day, last_day, error = fetch(number)
            budget -= 1
            if error:
                return error, cursor
            if fingerprint == cursor['fingerprint'] and not (issues & seen) and last_day <= oldest:
                missing.difference_update(issues)
                if not missing:
                    return None, None
                seen.update(issues)
                oldest = first_day
                next_page = number + 1
            else:
                # The numbered page shifted or overlapped the latest page.
                # Do not trust the old cursor; start again at page two.
                cursor = None

        for number in range(next_page, 1001):
            if budget <= 0:
                break
            result, fingerprint, issues, first_day, last_day, error = fetch(number)
            budget -= 1
            if error:
                return error, cursor
            if issues & seen:
                return f'历史第 {number} 页与已抓取页面存在重复期号，列表可能发生变化', None
            if last_day > oldest:
                return f'历史第 {number} 页的日期顺序异常，列表可能发生变化', None
            seen.update(issues)
            oldest = first_day
            missing.difference_update(issues)
            if not missing:
                return None, None
            if len(issues) < config['page_size']:
                return f'历史第 {number} 页返回不足请求期数，停止本次补查；尚未查到：{", ".join(sorted(missing))}', None
            cursor = {'version': 1, 'page_no': number, 'page_size': config['page_size'],
                      'snapshot_id': result['snapshot_id'], 'fingerprint': fingerprint,
                      'targets_sha256': hashlib.sha256(_canonical(targets)).hexdigest()}
        if cursor and cursor['page_no'] < 1000:
            note = f'已查至第 {cursor["page_no"]} 页，下次将核对后继续'
            if limit < 3:
                note += '；继续推进需将单次上限设为至少 3 页'
        else:
            cursor, note = None, '下次将从前面重新检查'
        return f'已达到 {limit} 页补查上限，尚未查到：{", ".join(sorted(missing))}；{note}', cursor

    def execute(self, claim, *, updater=update_snapshot, page_updater=update_page, sleep=time.sleep):
        """Perform bounded attempts; only a valid owner can import or finish."""
        from dlt.recovery_guard import require_path_execution_allowed
        require_path_execution_allowed(self.path)
        try:
            return self._execute_active(claim, updater=updater, page_updater=page_updater, sleep=sleep)
        except ScheduleCancelled:
            self._finish_cancelled(claim)
            return None

    def _execute_active(self, claim, *, updater, page_updater, sleep):
        config = claim['config']
        for attempt in range(1, config['retry_count'] + 2):
            try:
                self._check_claim(claim)
                result = updater(SnapshotStore(self.path), page_size=config['page_size'],
                                 timeout_seconds=config['timeout_seconds'], max_bytes=config['max_bytes'],
                                 transaction_guard=self._guard(claim))
                warning, cursor = self._fetch_missing_pages(claim, result, page_updater=page_updater, sleep=sleep)
                detail = (_canonical({'snapshot_id': result['snapshot_id'], 'history_cursor': cursor}).decode('utf-8')
                          if cursor else result['snapshot_id'])
                self._record_attempt(claim, attempt, 'success', detail)
                self._finish(claim, 'success', snapshot_id=result['snapshot_id'], error=warning)
                result['history_warning'] = warning
                result['history_cursor'] = cursor
                return result
            except (SourceError, InvalidSnapshot, OSError) as exc:
                self._record_attempt(claim, attempt, 'failed', str(exc))
                if attempt > config['retry_count']:
                    self._finish(claim, 'failed', error=str(exc)[:2000])
                    return None
                self._wait_retry(claim, config['retry_wait_seconds'], sleep)

    def run_due(self):
        from dlt.recovery_guard import require_path_execution_allowed
        require_path_execution_allowed(self.path)
        # Corrections have their own durable, fenced follow-up queue. Recover
        # it even when there are no enabled data-update plans.
        from dlt.prospective_settlement import PaperResults
        PaperResults(self.path, clock=self.clock).recover_pending_source_corrections()
        self.process_pending_followups()
        results = []
        for plan in self.list_plans():
            claim = self.claim(plan['id'])
            if claim:
                results.append({'plan_id': plan['id'], 'run_id': claim['run_id'], 'result': self.execute(claim)})
        self.process_pending_followups()
        PaperResults(self.path, clock=self.clock).recover_pending_source_corrections()
        return results

    def process_pending_followups(self):
        """Resume post-update paper checks after a worker crash or restart.

        Completion is idempotent at the result group level. A failed dispatch is
        retained and retried when a later successful source update is recorded.
        """
        with closing(sqlite3.connect(self.path)) as db:
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            pending = db.execute("SELECT r.id, r.snapshot_id FROM update_runs r WHERE r.status='success' "
                                 "AND NOT EXISTS (SELECT 1 FROM update_followups f WHERE f.run_id=r.id)").fetchall()
            has_experiments = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='fixed_experiments'").fetchone()
        for run_id, snapshot_id in pending:
            if not has_experiments:
                status, details = 'skipped', {'reason': 'no fixed experiments'}
            else:
                try:
                    from dlt.prospective_settlement import PaperResults
                    outcomes = PaperResults(self.path, clock=self.clock).process_update_and_catchup(snapshot_id)
                    details = {key: {k: value[k] for k in ('status', 'reason', 'group_id', 'issue') if k in value}
                               for key, value in outcomes.items()}
                    status = ('incomplete' if any(value.get('status') in ('error', 'waiting_source')
                                                  for value in outcomes.values()) else 'complete')
                except Exception as exc:
                    # Fetch completion and paper settlement have separate outcomes.
                    status, details = 'failed', {'reason': str(exc)[:2000]}
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('PRAGMA foreign_keys=ON')
                db.execute('INSERT INTO update_followups(run_id, status, details_json, recorded_at) VALUES (?, ?, ?, ?)',
                           (run_id, status, _canonical(details), self._now().isoformat()))

    def list_runs(self, plan_id):
        with closing(sqlite3.connect(self.path)) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute('SELECT * FROM update_runs WHERE plan_id=? ORDER BY started_at, rowid', (plan_id,)).fetchall()
            has_configs = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='update_run_configs'").fetchone()
            def config_for(run_id):
                saved = db.execute('SELECT config_sha256,config_json FROM update_run_configs WHERE run_id=?', (run_id,)).fetchone() if has_configs else None
                if not saved:
                    return {'config_status': 'not_recorded', 'recorded_config': None}
                try:
                    if hashlib.sha256(saved[1]).hexdigest() != saved[0]:
                        raise ValueError('configuration digest mismatch')
                    return {'config_status': 'recorded', 'recorded_config': _validate(json.loads(saved[1]))}
                except (ValueError, TypeError):
                    return {'config_status': 'review_required', 'recorded_config': None}
            return [{**{key: row[key] for key in row.keys() if key != 'token'},
                     **config_for(row['id']),
                     'attempts': [dict(x) for x in db.execute(
                'SELECT number, at, status, detail FROM update_attempts WHERE run_id=? ORDER BY number', (row['id'],))],
                     'followups': [{'status': f[0], 'details': json.loads(f[1]), 'recorded_at': f[2]} for f in db.execute(
                         'SELECT status, details_json, recorded_at FROM update_followups WHERE run_id=? ORDER BY seq', (row['id'],))]}
                    for row in rows]
