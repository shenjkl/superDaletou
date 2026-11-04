"""Request recovery contract for closing fixed prospective experiments."""
from datetime import datetime, timezone
import hashlib
import json
import re

from dlt.candidates import _canonical


OPERATIONS = {'experiment_close', 'series_close'}


class ClosureFenced(ValueError):
    def __init__(self, request_key, operation, request_sha256, retry_of=None):
        super().__init__('该实验关闭请求已封止，迟到请求不能执行')
        self.request_key = request_key
        self.operation = operation
        self.request_sha256 = request_sha256
        self.retry_of = retry_of


def normalize(operation, request):
    if operation not in OPERATIONS or not isinstance(request, dict) or set(request) != {'object_key', 'reason'}:
        raise ValueError('实验关闭请求无效')
    key, reason = request.get('object_key'), request.get('reason')
    if not isinstance(key, str) or not key.strip() or len(key) > 128:
        raise ValueError('实验关闭对象无效')
    if not isinstance(reason, str) or not reason.strip() or len(reason.strip()) > 1000:
        raise ValueError('关闭原因必须为 1 至 1000 个字符')
    return {'object_key': key, 'reason': reason.strip()}


def digest(operation, request):
    return hashlib.sha256(_canonical(normalize(operation, request))).hexdigest()


def _request_key(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValueError('实验关闭请求标识无效')
    return value


def _state(db, request_key, operation, request, retry_of=None):
    raw = _canonical(request); sha = hashlib.sha256(raw).hexdigest(); object_key = request['object_key']
    accepted = db.execute('SELECT operation,object_key,request_sha256,request_json,result_json '
                          'FROM prospective_closure_write_requests WHERE request_key=?', (request_key,)).fetchone()
    fenced = db.execute('SELECT operation,object_key,request_sha256,request_json '
                        'FROM prospective_closure_write_fences WHERE request_key=?', (request_key,)).fetchone()
    retry = db.execute('SELECT retry_of,operation,object_key,request_sha256,request_json '
                       'FROM prospective_closure_write_retries WHERE request_key=?', (request_key,)).fetchone()
    if accepted and fenced:
        raise ValueError('实验关闭请求同时存在接受与封止记录')
    if retry and not (accepted or fenced):
        raise ValueError('实验关闭关联重试缺少接受或封止记录')
    if retry and (retry_of != retry[0] or retry[1:] != (operation, object_key, sha, raw)):
        raise ValueError('实验关闭关联重试与请求不一致')
    if not retry and retry_of is not None and (accepted or fenced):
        raise ValueError('实验关闭重试关系无法核验')
    if accepted:
        if accepted[:4] != (operation, object_key, sha, raw):
            raise ValueError('实验关闭请求标识已用于不同内容')
        result = json.loads(accepted[4])
        if (not isinstance(result, dict) or set(result) != {'closed_at', 'reason'}
                or result.get('reason') != request['reason'] or _canonical(result) != accepted[4]):
            raise ValueError('已接受的实验关闭结果无法核验')
        table, key_column = (('experiment_closures', 'experiment_key') if operation == 'experiment_close'
                             else ('series_closures', 'series_key'))
        closure = db.execute(f'SELECT closed_at,reason FROM {table} WHERE {key_column}=?',
                             (object_key,)).fetchone()
        if closure != (result['closed_at'], result['reason']):
            raise ValueError('已接受的实验关闭结果与关闭记录不一致')
        return {'status': 'accepted', 'request_key': request_key, 'operation': operation,
                'object_key': object_key, 'request_sha256': sha,
                'retry_of': retry[0] if retry else None, 'result': result}
    if fenced:
        if fenced != (operation, object_key, sha, raw):
            raise ValueError('实验关闭请求标识已用于不同封止内容')
        return {'status': 'fenced', 'request_key': request_key, 'operation': operation,
                'object_key': object_key, 'request_sha256': sha,
                'retry_of': retry[0] if retry else None}
    return {'status': 'unknown', 'request_key': request_key, 'operation': operation,
            'object_key': object_key, 'request_sha256': sha, 'retry_of': retry_of}


def _add_retry(db, request_key, operation, request, retry_of, created_at):
    raw = _canonical(request); sha = hashlib.sha256(raw).hexdigest(); object_key = request['object_key']
    existing = db.execute('SELECT retry_of,operation,object_key,request_sha256,request_json '
                          'FROM prospective_closure_write_retries WHERE request_key=?', (request_key,)).fetchone()
    if retry_of is None:
        if existing:
            raise ValueError('该实验关闭请求必须沿用已保存的关联重试')
        return
    _request_key(retry_of)
    if retry_of == request_key:
        raise ValueError('实验关闭请求不能关联自身重试')
    original = db.execute('SELECT operation,object_key,request_sha256,request_json '
                          'FROM prospective_closure_write_fences WHERE request_key=?', (retry_of,)).fetchone()
    if original != (operation, object_key, sha, raw):
        raise ValueError('实验关闭关联重试必须指向同操作、同对象、同内容的封止请求')
    wanted = (retry_of, operation, object_key, sha, raw)
    if existing:
        if existing != wanted:
            raise ValueError('实验关闭请求标识已用于不同关联重试')
        return
    if db.execute('SELECT 1 FROM prospective_closure_write_retries WHERE retry_of=?', (retry_of,)).fetchone():
        raise ValueError('该实验关闭请求已有直接重试')
    db.execute('INSERT INTO prospective_closure_write_retries VALUES (?,?,?,?,?,?,?)',
               (request_key, retry_of, operation, object_key, sha, raw, created_at))


def _save(db, request_key, operation, request, retry_of, result, created_at):
    raw = _canonical(request); sha = hashlib.sha256(raw).hexdigest()
    db.execute('INSERT INTO prospective_closure_write_requests VALUES (?,?,?,?,?,?,?)',
               (request_key, operation, request['object_key'], sha, raw, _canonical(result), created_at))
    return {'status': 'accepted', 'request_key': request_key, 'operation': operation,
            'object_key': request['object_key'], 'request_sha256': sha,
            'retry_of': retry_of, 'result': result}


def _reject_unresolved_fence(db, operation, object_key):
    pending = db.execute('SELECT 1 FROM prospective_closure_write_fences f WHERE f.operation=? AND f.object_key=? '
                         'AND NOT EXISTS (SELECT 1 FROM prospective_closure_write_retries r WHERE r.retry_of=f.request_key) '
                         'LIMIT 1', (operation, object_key)).fetchone()
    if pending:
        raise ValueError('该关闭对象已有结果待确认的请求，请先查询或关联重试')


def accept(db, request_key, operation, request, retry_of, *, closed_at, existing, insert):
    request_key = _request_key(request_key)
    request = normalize(operation, request)
    state = _state(db, request_key, operation, request, retry_of)
    if state['status'] == 'accepted':
        return state['result']
    if state['status'] == 'fenced':
        raise ClosureFenced(request_key, operation, state['request_sha256'], retry_of)
    created_at = datetime.now(timezone.utc).isoformat()
    _add_retry(db, request_key, operation, request, retry_of, created_at)
    _reject_unresolved_fence(db, operation, request['object_key'])
    old = existing()
    if old:
        if old[1] != request['reason']:
            raise ValueError('该实验已用不同原因关闭，不能改写原记录')
        result = {'closed_at': old[0], 'reason': old[1]}
        _save(db, request_key, operation, request, retry_of, result, created_at)
        return result
    result = {'closed_at': closed_at(), 'reason': request['reason']}
    _save(db, request_key, operation, request, retry_of, result, created_at)
    insert(result)
    return result


def fence(path, request_key, operation, request, request_sha256, retry_of=None):
    request_key = _request_key(request_key); request = normalize(operation, request)
    raw = _canonical(request); sha = hashlib.sha256(raw).hexdigest()
    if request_sha256 != sha:
        raise ValueError('实验关闭请求摘要不匹配')
    import sqlite3
    with sqlite3.connect(path) as db, db:
        db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
        state = _state(db, request_key, operation, request, retry_of)
        if state['status'] != 'unknown':
            return state
        table, key_column = (('fixed_experiments', 'request_key') if operation == 'experiment_close'
                             else ('experiment_series', 'series_key'))
        if not db.execute(f'SELECT 1 FROM {table} WHERE {key_column}=?', (request['object_key'],)).fetchone():
            raise ValueError('关闭对象不存在，不能封止实验关闭请求')
        created_at = datetime.now(timezone.utc).isoformat()
        _add_retry(db, request_key, operation, request, retry_of, created_at)
        _reject_unresolved_fence(db, operation, request['object_key'])
        db.execute('INSERT INTO prospective_closure_write_fences VALUES (?,?,?,?,?,?)',
                   (request_key, operation, request['object_key'], sha, raw, created_at))
        return {'status': 'fenced', 'request_key': request_key, 'operation': operation,
                'object_key': request['object_key'], 'request_sha256': sha, 'retry_of': retry_of}


def status(path, request_key, operation, request_sha256):
    request_key = _request_key(request_key)
    if operation not in OPERATIONS or not isinstance(request_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', request_sha256):
        raise ValueError('实验关闭状态查询参数无效')
    import sqlite3
    with sqlite3.connect(path) as db:
        db.execute('BEGIN')
        accepted = db.execute('SELECT operation,object_key,request_sha256,request_json,result_json '
                              'FROM prospective_closure_write_requests WHERE request_key=?', (request_key,)).fetchone()
        fenced = db.execute('SELECT operation,object_key,request_sha256,request_json '
                            'FROM prospective_closure_write_fences WHERE request_key=?', (request_key,)).fetchone()
        retry = db.execute('SELECT retry_of FROM prospective_closure_write_retries WHERE request_key=?', (request_key,)).fetchone()
        if accepted and fenced:
            raise ValueError('实验关闭请求同时存在接受与封止记录')
        if not accepted and not fenced:
            if retry:
                raise ValueError('实验关闭关联重试缺少接受或封止记录')
            return {'status': 'unknown', 'request_key': request_key, 'operation': operation,
                    'request_sha256': request_sha256}
        row = accepted or fenced
        if row[0] != operation or row[2] != request_sha256:
            raise ValueError('实验关闭请求类型或摘要不匹配')
        request = normalize(operation, json.loads(row[3]))
        if _canonical(request) != row[3]:
            raise ValueError('实验关闭请求内容无法核验')
        state = _state(db, request_key, operation, request, retry[0] if retry else None)
        return state
