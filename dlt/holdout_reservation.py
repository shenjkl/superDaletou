"""Immutable local exploration exclusions; not proof of independent holdout.

Only registered application paths are constrained. Local files, direct SQLite
access, prior viewing, and external programs remain outside this boundary.
"""
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import uuid

from dlt.candidates import _canonical
from dlt.holdout_audit import audit_holdout


class ReservationError(ValueError):
    pass


class ReservationFenced(ReservationError):
    def __init__(self, request_key, request_sha256, retry_of=None):
        super().__init__('该排除范围预留请求已封止，迟到请求不能保存')
        self.request_key = request_key
        self.request_sha256 = request_sha256
        self.retry_of = retry_of


def _has_table(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def reservation_lineage(db):
    """Validate original exclusions and explicit, unused successor versions."""
    if not _has_table(db, 'holdout_reservations'):
        return {}, {}
    records, successors, created = {}, {}, {}
    for ident, raw, created_at in db.execute('SELECT id,content_json,created_at FROM holdout_reservations'):
        try:
            content = json.loads(raw)
            issues = content['issues']
            valid = (hashlib.sha256(raw).hexdigest() == ident and content['version'] in ('local-exclusion-v1', 'local-exclusion-successor-v1')
                     and content['audit_status'] == 'no_recorded_use' and content['independence'] == 'not_verified'
                     and isinstance(issues, list) and 1 <= len(issues) <= 200 and len(set(issues)) == len(issues)
                     and all(isinstance(issue, str) and re.fullmatch(r'[0-9]{5}', issue) for issue in issues)
                     and issues[0] == content['start_issue'] and issues[-1] == content['end_issue'])
        except (ValueError, TypeError, KeyError, AttributeError):
            valid = False
        if not valid:
            raise ReservationError('排除范围记录损坏，不能启动研究任务')
        records[ident] = content
        created[ident] = created_at
    original_issues = set()
    for ident, content in records.items():
        if content['version'] != 'local-exclusion-v1':
            continue
        if 'supersedes' in content or original_issues.intersection(content['issues']):
            raise ReservationError('排除范围存在未声明的重叠')
        original_issues.update(content['issues'])
    for ident, content in records.items():
        if content['version'] != 'local-exclusion-successor-v1':
            continue
        parent = content.get('supersedes')
        if not isinstance(parent, str) or not re.fullmatch(r'[0-9a-f]{64}', parent):
            raise ReservationError('评估协议版本关联无效')
        predecessor = records.get(parent)
        if (not predecessor or parent == ident or parent in successors
                or any(content.get(key) != predecessor.get(key) for key in ('snapshot_id', 'start_issue', 'end_issue', 'issues'))
                or not isinstance(content.get('supersession_reason'), str) or not content['supersession_reason'].strip()
                or len(content['supersession_reason']) > 1000
                or not _has_table(db, 'fixed_evaluations') or not _has_table(db, 'fixed_evaluation_attempts')):
            raise ReservationError('评估协议版本关联无效')
        evaluation = db.execute('SELECT protocol_sha256,status,protocol_json,started_at,finished_at,error,report_sha256,report_json,created_at '
                                'FROM fixed_evaluations WHERE reservation_id=?', (parent,)).fetchone()
        if (not evaluation or evaluation[0] != content.get('supersedes_protocol_sha256') or evaluation[1] != 'pending'
                or any(value is not None for value in evaluation[3:8])
                or db.execute('SELECT 1 FROM fixed_evaluation_attempts WHERE reservation_id=?', (parent,)).fetchone()):
            raise ReservationError('已有运行记录的评估不能作为未使用版本替代')
        child = db.execute('SELECT protocol_sha256,protocol_json,created_at FROM fixed_evaluations WHERE reservation_id=?', (ident,)).fetchone()
        try:
            if (not child or hashlib.sha256(evaluation[2]).hexdigest() != evaluation[0]
                    or hashlib.sha256(child[1]).hexdigest() != child[0]):
                raise ValueError('protocol digest mismatch')
            times = [datetime.fromisoformat(x) for x in (created[parent], evaluation[8], content['audit_as_of_utc'], created[ident], child[2])]
            if (any(t.tzinfo is None or t.utcoffset() is None for t in times)
                    or not times[0] <= times[1] <= times[2] <= times[3] == times[4]):
                raise ValueError('invalid local version chronology')
            parent_spec, child_spec = json.loads(evaluation[2]), json.loads(child[1])
            metadata = {'reservation_id', 'environment', 'predecessor_reservation_id', 'predecessor_protocol_sha256'}
            if (parent_spec['reservation_id'] != parent or child_spec['reservation_id'] != ident
                    or child_spec['predecessor_reservation_id'] != parent
                    or child_spec['predecessor_protocol_sha256'] != evaluation[0]
                    or {k: v for k, v in child_spec.items() if k not in metadata}
                    != {k: v for k, v in parent_spec.items() if k not in metadata}):
                raise ValueError('protocol content changed')
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ReservationError('关联版本未完整保留原评估协议') from None
        successors[parent] = ident
    for ident in records:
        seen, current = set(), ident
        while current in successors:
            if current in seen:
                raise ReservationError('评估协议版本关联存在循环')
            seen.add(current); current = successors[current]
    return records, successors


def initialize(path):
    """Create the reservation ledger during service startup before serving data."""
    path = Path(path).resolve()
    if not path.is_file():
        raise ReservationError('研究数据库不存在')
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('PRAGMA foreign_keys=ON')
        db.executescript('''
            CREATE TABLE IF NOT EXISTS holdout_reservations (
                id TEXT PRIMARY KEY, content_json BLOB NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS holdout_reservations_no_update BEFORE UPDATE ON holdout_reservations
                BEGIN SELECT RAISE(ABORT, 'immutable holdout reservation'); END;
            CREATE TRIGGER IF NOT EXISTS holdout_reservations_no_delete BEFORE DELETE ON holdout_reservations
                BEGIN SELECT RAISE(ABORT, 'immutable holdout reservation'); END;
        ''')
        _ensure_write_tables(db)


def reserved_issues(db, *, require_table=False):
    if require_table and not _has_table(db, 'holdout_reservations'):
        raise ReservationError('排除范围记录表缺失，不能确认预留期')
    records, _ = reservation_lineage(db)
    return {issue for content in records.values() for issue in content['issues']}


def check_exploration_allowed(db, issues):
    if set(issues) & reserved_issues(db):
        raise ReservationError('所选奖期属于已固定的本机排除范围，不能用于探索任务')


def check_backtest_allowed(db, protocol):
    rows = db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue',
                      (protocol['snapshot_id'],)).fetchall()
    source = [row[0] for row in rows]
    used = set(protocol['targets'])
    for issue in protocol['targets']:
        if issue not in source:
            raise ReservationError('回测目标期不在所选快照')
        index = source.index(issue)
        used.update(source[max(0, index-protocol['history_periods']):index])
    check_exploration_allowed(db, used)


@contextmanager
def exploration_activity(path, *, kind, inputs, targets=()):
    """Record exposure before computation, under the same lock as reservation."""
    inputs, targets = sorted(set(inputs)), sorted(set(targets))
    content = {'version': 'exploration-activity-v1', 'kind': kind, 'inputs': inputs, 'targets': targets}
    payload = _canonical(content); ident = uuid.uuid4().hex
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript('''
            CREATE TABLE IF NOT EXISTS exploration_activities (
                id TEXT PRIMARY KEY, content_sha256 TEXT NOT NULL, content_json BLOB NOT NULL,
                status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT
            );
            CREATE TRIGGER IF NOT EXISTS exploration_activities_fixed BEFORE UPDATE OF id,content_sha256,content_json,started_at ON exploration_activities
                BEGIN SELECT RAISE(ABORT, 'immutable exploration activity'); END;
            CREATE TRIGGER IF NOT EXISTS exploration_activities_no_delete BEFORE DELETE ON exploration_activities
                BEGIN SELECT RAISE(ABORT, 'immutable exploration activity'); END;
        ''')
        db.execute('BEGIN IMMEDIATE')
        check_exploration_allowed(db, set(inputs) | set(targets))
        db.execute('INSERT INTO exploration_activities VALUES (?,?,?,\'running\',?,NULL)',
                   (ident, hashlib.sha256(payload).hexdigest(), payload, datetime.now(timezone.utc).isoformat()))
    try:
        yield
    finally:
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("UPDATE exploration_activities SET status='finished',finished_at=? WHERE id=? AND status='running'",
                       (datetime.now(timezone.utc).isoformat(), ident))


def reserve_range(path, snapshot_id, start_issue, end_issue, *, confirmed):
    if confirmed is not True:
        raise ReservationError('必须确认固定范围及其限制')
    path = Path(path).resolve()
    if not path.is_file():
        raise ReservationError('研究数据库不存在')
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('PRAGMA foreign_keys=ON')
        db.executescript('''
            CREATE TABLE IF NOT EXISTS holdout_reservations (
                id TEXT PRIMARY KEY, content_json BLOB NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS holdout_reservations_no_update BEFORE UPDATE ON holdout_reservations
                BEGIN SELECT RAISE(ABORT, 'immutable holdout reservation'); END;
            CREATE TRIGGER IF NOT EXISTS holdout_reservations_no_delete BEFORE DELETE ON holdout_reservations
                BEGIN SELECT RAISE(ABORT, 'immutable holdout reservation'); END;
        ''')
        db.execute('BEGIN IMMEDIATE')
        reserved_issues(db)
        for ident, raw, created_at in db.execute('SELECT id,content_json,created_at FROM holdout_reservations'):
            existing = json.loads(raw)
            if (existing['snapshot_id'], existing['start_issue'], existing['end_issue']) == (snapshot_id, start_issue, end_issue):
                return {'id': ident, 'content': existing, 'created_at': created_at}
        for table in ('training_jobs', 'backtest_jobs'):
            if _has_table(db, table) and db.execute(f"SELECT 1 FROM {table} WHERE status='running' LIMIT 1").fetchone():
                raise ReservationError('训练或回测仍在运行，请等待结束后重新核对')
        # An immediate write lock prevents jobs or draft publications from
        # being committed between this audit and the reservation commit.
        audit = audit_holdout(path, snapshot_id, start_issue, end_issue)
        if audit['status'] != 'no_recorded_use':
            raise ReservationError('本机记录显示范围已使用或无法核对，不能固定为未使用范围')
        issues = audit['issues']
        if set(issues) & reserved_issues(db):
            raise ReservationError('所选范围与已有固定范围重叠')
        content = {'version': 'local-exclusion-v1', 'snapshot_id': snapshot_id,
                   'start_issue': start_issue, 'end_issue': end_issue, 'issues': issues,
                   'audit_status': audit['status'], 'audit_as_of_utc': audit['as_of_utc'],
                   'independence': 'not_verified'}
        payload = _canonical(content)
        ident = hashlib.sha256(payload).hexdigest()
        now = datetime.now(timezone.utc).isoformat()
        db.execute('INSERT INTO holdout_reservations VALUES (?,?,?)', (ident, payload, now))
        return {'id': ident, 'content': content, 'created_at': now}


def _request(snapshot_id, start_issue, end_issue, confirmed):
    if (not isinstance(snapshot_id, str) or not re.fullmatch(r'[0-9a-f]{64}', snapshot_id)
            or not isinstance(start_issue, str) or not re.fullmatch(r'[0-9]{5}', start_issue)
            or not isinstance(end_issue, str) or not re.fullmatch(r'[0-9]{5}', end_issue)
            or confirmed is not True):
        raise ReservationError('快照、奖期或确认内容无效')
    return {'snapshot_id': snapshot_id, 'start_issue': start_issue,
            'end_issue': end_issue, 'confirmed': True}


def _request_key(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ReservationError('排除范围预留请求标识无效')
    return value


def _ensure_write_tables(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS holdout_write_requests (
            request_key TEXT PRIMARY KEY,
            reservation_id TEXT NOT NULL REFERENCES holdout_reservations(id),
            request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL,
            result_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS holdout_write_fences (
            request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL,
            request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS holdout_write_retries (
            request_key TEXT PRIMARY KEY,
            retry_of TEXT NOT NULL UNIQUE REFERENCES holdout_write_fences(request_key),
            request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS holdout_write_requests_no_update BEFORE UPDATE ON holdout_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable holdout write request'); END;
        CREATE TRIGGER IF NOT EXISTS holdout_write_requests_no_delete BEFORE DELETE ON holdout_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable holdout write request'); END;
        CREATE TRIGGER IF NOT EXISTS holdout_write_fences_no_update BEFORE UPDATE ON holdout_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable holdout write fence'); END;
        CREATE TRIGGER IF NOT EXISTS holdout_write_fences_no_delete BEFORE DELETE ON holdout_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable holdout write fence'); END;
        CREATE TRIGGER IF NOT EXISTS holdout_write_retries_no_update BEFORE UPDATE ON holdout_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable holdout write retry'); END;
        CREATE TRIGGER IF NOT EXISTS holdout_write_retries_no_delete BEFORE DELETE ON holdout_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable holdout write retry'); END;
        CREATE TRIGGER IF NOT EXISTS holdout_write_requests_no_fenced_insert BEFORE INSERT ON holdout_write_requests
            WHEN EXISTS (SELECT 1 FROM holdout_write_fences WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'fenced holdout reservation request'); END;
        CREATE TRIGGER IF NOT EXISTS holdout_write_fences_no_committed_insert BEFORE INSERT ON holdout_write_fences
            WHEN EXISTS (SELECT 1 FROM holdout_write_requests WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'committed holdout reservation request'); END;
    ''')


def _retry_state(db, request_key, request, retry_of, created_at=None):
    raw = _canonical(request)
    digest = hashlib.sha256(raw).hexdigest()
    existing = db.execute('SELECT retry_of,request_sha256,request_json FROM holdout_write_retries WHERE request_key=?',
                          (request_key,)).fetchone()
    if retry_of is None:
        if existing:
            raise ReservationError('该请求必须沿用已保存的关联重试关系')
        return
    _request_key(retry_of)
    if retry_of == request_key:
        raise ReservationError('排除范围预留不能关联自身重试')
    original = db.execute('SELECT request_sha256,request_json FROM holdout_write_fences WHERE request_key=?',
                          (retry_of,)).fetchone()
    if not original or original != (digest, raw):
        raise ReservationError('关联重试必须指向同内容且已封止的排除范围请求')
    if existing:
        if existing != (retry_of, digest, raw):
            raise ReservationError('请求标识已用于不同的排除范围重试')
        return
    if db.execute('SELECT 1 FROM holdout_write_retries WHERE retry_of=?', (retry_of,)).fetchone():
        raise ReservationError('原请求已有直接重试，请先核对该重试')
    if created_at is not None:
        db.execute('INSERT INTO holdout_write_retries VALUES (?,?,?,?,?)',
                   (request_key, retry_of, digest, raw, created_at))


def _write_state(db, request_key, request, retry_of=None):
    raw = _canonical(request)
    digest = hashlib.sha256(raw).hexdigest()
    saved = db.execute('SELECT reservation_id,request_sha256,request_json,result_json,created_at '
                       'FROM holdout_write_requests WHERE request_key=?', (request_key,)).fetchone()
    fence = db.execute('SELECT request_sha256,request_json,created_at FROM holdout_write_fences WHERE request_key=?',
                       (request_key,)).fetchone()
    retry = db.execute('SELECT retry_of,request_sha256,request_json FROM holdout_write_retries WHERE request_key=?',
                       (request_key,)).fetchone()
    if saved and fence:
        raise ReservationError('排除范围预留请求同时存在已接受和封止记录')
    if retry and not (saved or fence):
        raise ReservationError('排除范围关联重试缺少请求结果或封止记录')
    if retry and (retry_of != retry[0] or retry[1:] != (digest, raw)):
        raise ReservationError('排除范围关联重试记录与请求不一致')
    if not retry and retry_of is not None and (saved or fence):
        raise ReservationError('排除范围重试关系无法核验')
    if saved:
        reservation_id, saved_digest, saved_raw, result_raw, created_at = saved
        if saved_digest != digest or saved_raw != raw:
            raise ReservationError('请求标识已用于不同的排除范围预留')
        result = json.loads(result_raw)
        if (_canonical(result) != result_raw or result.get('id') != reservation_id
                or not db.execute('SELECT 1 FROM holdout_reservations WHERE id=?', (reservation_id,)).fetchone()):
            raise ReservationError('已保存的排除范围结果无法核验')
        content = result.get('content')
        if not isinstance(content, dict) or any(content.get(field) != request[field]
                for field in ('snapshot_id', 'start_issue', 'end_issue')):
            raise ReservationError('已接受范围与原预留请求不一致')
        return {'status': 'accepted', 'request_key': request_key, 'request_sha256': digest,
                'retry_of': retry[0] if retry else None, 'reservation': result}
    if fence:
        if fence[:2] != (digest, raw):
            raise ReservationError('请求标识已用于不同的封止内容')
        return {'status': 'fenced', 'request_key': request_key, 'request_sha256': digest,
                'retry_of': retry[0] if retry else None}
    return {'status': 'unknown', 'request_key': request_key, 'request_sha256': digest,
            'retry_of': retry[0] if retry else None}


def _store_request(db, request_key, request, retry_of, reservation, created_at):
    raw = _canonical(request)
    digest = hashlib.sha256(raw).hexdigest()
    result_raw = _canonical(reservation)
    db.execute('INSERT INTO holdout_write_requests VALUES (?,?,?,?,?,?)',
               (request_key, reservation['id'], digest, raw, result_raw, created_at))
    return {'status': 'accepted', 'request_key': request_key, 'request_sha256': digest,
            'retry_of': retry_of, 'reservation': reservation}


def reserve_range_write(path, snapshot_id, start_issue, end_issue, *, confirmed, request_key, retry_of=None):
    request_key = _request_key(request_key)
    request = _request(snapshot_id, start_issue, end_issue, confirmed)
    path = Path(path).resolve()
    if not path.is_file():
        raise ReservationError('研究数据库不存在')
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('PRAGMA foreign_keys=ON')
        db.executescript('''
            CREATE TABLE IF NOT EXISTS holdout_reservations (
                id TEXT PRIMARY KEY, content_json BLOB NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS holdout_reservations_no_update BEFORE UPDATE ON holdout_reservations
                BEGIN SELECT RAISE(ABORT, 'immutable holdout reservation'); END;
            CREATE TRIGGER IF NOT EXISTS holdout_reservations_no_delete BEFORE DELETE ON holdout_reservations
                BEGIN SELECT RAISE(ABORT, 'immutable holdout reservation'); END;
        ''')
        _ensure_write_tables(db)
        db.execute('BEGIN IMMEDIATE')
        state = _write_state(db, request_key, request, retry_of)
        if state['status'] == 'accepted':
            return state
        if state['status'] == 'fenced':
            raise ReservationFenced(request_key, state['request_sha256'], retry_of)
        now = datetime.now(timezone.utc).isoformat()
        _retry_state(db, request_key, request, retry_of, now)
        used = reserved_issues(db)
        for ident, raw, created_at in db.execute('SELECT id,content_json,created_at FROM holdout_reservations'):
            existing = json.loads(raw)
            if (existing['snapshot_id'], existing['start_issue'], existing['end_issue']) == (
                    snapshot_id, start_issue, end_issue):
                reservation = {'id': ident, 'content': existing, 'created_at': created_at}
                return _store_request(db, request_key, request, retry_of, reservation, now)
        for table in ('training_jobs', 'backtest_jobs'):
            if _has_table(db, table) and db.execute(
                    f"SELECT 1 FROM {table} WHERE status='running' LIMIT 1").fetchone():
                raise ReservationError('训练或回测仍在运行，请等待结束后重新核对')
        audit = audit_holdout(path, snapshot_id, start_issue, end_issue)
        if audit['status'] != 'no_recorded_use':
            raise ReservationError('本机记录显示范围已使用或无法核对，不能固定为未使用范围')
        issues = audit['issues']
        if set(issues) & used:
            raise ReservationError('所选范围与已有固定范围重叠')
        content = {'version': 'local-exclusion-v1', 'snapshot_id': snapshot_id,
                   'start_issue': start_issue, 'end_issue': end_issue, 'issues': issues,
                   'audit_status': audit['status'], 'audit_as_of_utc': audit['as_of_utc'],
                   'independence': 'not_verified'}
        payload = _canonical(content)
        ident = hashlib.sha256(payload).hexdigest()
        reservation = {'id': ident, 'content': content, 'created_at': now}
        db.execute('INSERT INTO holdout_reservations VALUES (?,?,?)', (ident, payload, now))
        return _store_request(db, request_key, request, retry_of, reservation, now)


def fence_range(path, *, request_key, request_sha256, snapshot_id, start_issue, end_issue,
                confirmed, retry_of=None):
    request_key = _request_key(request_key)
    request = _request(snapshot_id, start_issue, end_issue, confirmed)
    digest = hashlib.sha256(_canonical(request)).hexdigest()
    if request_sha256 != digest:
        raise ReservationError('排除范围预留请求摘要不匹配')
    path = Path(path).resolve()
    if not path.is_file():
        raise ReservationError('研究数据库不存在')
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('PRAGMA foreign_keys=ON')
        db.executescript('''
            CREATE TABLE IF NOT EXISTS holdout_reservations (
                id TEXT PRIMARY KEY, content_json BLOB NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS holdout_reservations_no_update BEFORE UPDATE ON holdout_reservations
                BEGIN SELECT RAISE(ABORT, 'immutable holdout reservation'); END;
            CREATE TRIGGER IF NOT EXISTS holdout_reservations_no_delete BEFORE DELETE ON holdout_reservations
                BEGIN SELECT RAISE(ABORT, 'immutable holdout reservation'); END;
        ''')
        _ensure_write_tables(db)
        db.execute('BEGIN IMMEDIATE')
        state = _write_state(db, request_key, request, retry_of)
        if state['status'] != 'unknown':
            return state
        now = datetime.now(timezone.utc).isoformat()
        _retry_state(db, request_key, request, retry_of, now)
        raw = _canonical(request)
        db.execute('INSERT INTO holdout_write_fences VALUES (?,?,?,?)',
                   (request_key, digest, raw, now))
        return {'status': 'fenced', 'request_key': request_key,
                'request_sha256': digest, 'retry_of': retry_of}


def reservation_write_status(path, *, request_key, request_sha256):
    request_key = _request_key(request_key)
    if not isinstance(request_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', request_sha256):
        raise ReservationError('排除范围预留请求摘要无效')
    with closing(sqlite3.connect(path)) as db:
        db.execute('BEGIN')
        present = [_has_table(db, table) for table in
                   ('holdout_write_requests', 'holdout_write_fences', 'holdout_write_retries')]
        if not any(present):
            return {'status': 'unknown', 'request_key': request_key, 'request_sha256': request_sha256,
                    'retry_of': None}
        if not all(present):
            raise ReservationError('排除范围预留写入日志结构不完整')
        saved = db.execute('SELECT request_json FROM holdout_write_requests WHERE request_key=?',
                           (request_key,)).fetchone()
        fence = db.execute('SELECT request_sha256,request_json FROM holdout_write_fences WHERE request_key=?',
                           (request_key,)).fetchone()
        retry = db.execute('SELECT retry_of,request_sha256,request_json FROM holdout_write_retries WHERE request_key=?',
                           (request_key,)).fetchone()
        if saved:
            request = json.loads(saved[0])
            normalized = _request(request.get('snapshot_id'), request.get('start_issue'),
                                  request.get('end_issue'), request.get('confirmed'))
            digest = hashlib.sha256(_canonical(normalized)).hexdigest()
            if normalized != request or digest != request_sha256:
                raise ReservationError('请求摘要与已保存排除范围预留不匹配')
            state = _write_state(db, request_key, request, retry[0] if retry else None)
            if state['status'] != 'accepted':
                raise ReservationError('已接受排除范围预留状态不一致')
            return state
        if not fence:
            if retry:
                raise ReservationError('排除范围关联重试缺少请求结果或封止记录')
            return {'status': 'unknown', 'request_key': request_key,
                    'request_sha256': request_sha256, 'retry_of': retry[0] if retry else None}
        if fence[0] != request_sha256 or hashlib.sha256(fence[1]).hexdigest() != request_sha256:
            raise ReservationError('封止请求摘要不匹配')
        request = json.loads(fence[1])
        normalized = _request(request.get('snapshot_id'), request.get('start_issue'),
                              request.get('end_issue'), request.get('confirmed'))
        if normalized != request or _canonical(request) != fence[1]:
            raise ReservationError('封止排除范围预留内容无法核验')
        state = _write_state(db, request_key, request, retry[0] if retry else None)
        if state['status'] != 'fenced':
            raise ReservationError('封止排除范围预留状态不一致')
        return state


def list_reservations(path):
    with closing(sqlite3.connect(path)) as db:
        db.execute('BEGIN')
        if not _has_table(db, 'holdout_reservations'):
            return []
        _, successors = reservation_lineage(db)
        from dlt.custom_origin import custom_origin_exposure
        output = []
        for row in db.execute('SELECT id,content_json,created_at FROM holdout_reservations ORDER BY created_at DESC,id'):
            content = json.loads(row[1])
            exposure = custom_origin_exposure(db, set(content['issues']))
            output.append({'id': row[0], 'content': content, 'created_at': row[2],
                           'superseded_by': successors.get(row[0]), 'custom_origin_exposure': exposure})
        return output
