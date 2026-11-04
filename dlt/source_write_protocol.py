"""Durable request coordination for locally reviewed source records.

The journal records whether a source write committed or was fenced. A committed
review/correction is separate from any later paper-result recalculation.
"""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3

from dlt.candidates import _canonical


class SourceWriteError(ValueError):
    pass


class SourceWriteFenced(SourceWriteError):
    def __init__(self, request_key, request_sha256, operation, retry_of=None):
        super().__init__('该来源写入请求已封止，迟到请求不能保存')
        self.request_key = request_key
        self.request_sha256 = request_sha256
        self.operation = operation
        self.retry_of = retry_of


def digest(request):
    return hashlib.sha256(_canonical(request)).hexdigest()


def _request_key(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', value, re.I):
        raise SourceWriteError('来源写入请求标识无效')
    return value


def object_key(operation, request):
    if operation == 'source_review':
        return f"{request['kind']}:{request['document_sha256']}"
    if operation == 'source_correction':
        scope = 'issue:' + request['issue'] if request.get('issue') is not None else 'global'
        return f"{scope}:{request['old_review_id']}"
    if operation in ('recovery_prepare', 'recovery_activate'):
        destination = Path(request['destination']).absolute()
        normalized = os.path.normcase(os.path.normpath(str(destination)))
        return operation + ':' + hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    raise SourceWriteError('来源写入操作无效')


def _state(db, request_key, operation, request_sha256):
    accepted = db.execute('SELECT operation,object_key,request_sha256,request_json,result_json,retry_of '
                          'FROM source_write_requests WHERE request_key=?', (request_key,)).fetchone()
    fence = db.execute('SELECT operation,object_key,request_sha256,request_json,created_at '
                       'FROM source_write_fences WHERE request_key=?', (request_key,)).fetchone()
    retry = db.execute('SELECT retry_of,operation,object_key,request_sha256,request_json '
                       'FROM source_write_retries WHERE request_key=?', (request_key,)).fetchone()
    if accepted and fence:
        raise SourceWriteError('来源写入请求同时存在接受与封止记录')
    if retry and not (accepted or fence):
        raise SourceWriteError('来源关联重试缺少接受或封止记录')
    if accepted:
        if accepted[0] != operation or accepted[2] != request_sha256:
            raise SourceWriteError('请求标识已用于不同的来源写入')
        raw, result_raw = accepted[3], accepted[4]
        try:
            request, result = json.loads(raw), json.loads(result_raw)
            calculated_key = object_key(accepted[0], request)
        except (TypeError, ValueError, KeyError) as exc:
            raise SourceWriteError('已接受来源写入内容无法解析') from exc
        if (not isinstance(raw, bytes) or not isinstance(result_raw, bytes)
                or hashlib.sha256(raw).hexdigest() != request_sha256 or _canonical(request) != raw
                or not isinstance(result, dict) or _canonical(result) != result_raw
                or calculated_key != accepted[1]):
            raise SourceWriteError('已接受来源写入摘要、对象或结果不一致')
        if ((retry[0] if retry else None) != accepted[5]
                or (retry and retry[1:] != (accepted[0], accepted[1], accepted[2], raw))):
            raise SourceWriteError('已接受来源写入的关联重试无法核验')
        try:
            validate_retry(db, request_key, accepted[0], accepted[1], accepted[2], raw, accepted[5])
        except SourceWriteError as exc:
            raise SourceWriteError('已接受来源写入的关联重试来源无法核验') from exc
        return {'status': 'accepted', 'request_key': request_key, 'operation': accepted[0],
                'object_key': accepted[1], 'request_sha256': accepted[2], 'result': result,
                'retry_of': accepted[5]}
    if fence:
        if fence[0] != operation or fence[2] != request_sha256:
            raise SourceWriteError('请求标识已用于不同的来源封止')
        raw = fence[3]
        try:
            request = json.loads(raw)
            calculated_key = object_key(fence[0], request)
        except (TypeError, ValueError, KeyError) as exc:
            raise SourceWriteError('已封止来源写入内容无法解析') from exc
        if (not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != request_sha256
                or _canonical(request) != raw or calculated_key != fence[1]):
            raise SourceWriteError('已封止来源写入摘要或对象不一致')
        retry_of = retry[0] if retry else None
        if retry and retry[1:] != (fence[0], fence[1], fence[2], raw):
            raise SourceWriteError('封止来源写入的关联重试无法核验')
        try:
            validate_retry(db, request_key, fence[0], fence[1], fence[2], raw, retry_of)
        except SourceWriteError as exc:
            raise SourceWriteError('封止来源写入的关联重试来源无法核验') from exc
        return {'status': 'fenced', 'request_key': request_key, 'operation': fence[0],
                'object_key': fence[1], 'request_sha256': fence[2], 'retry_of': retry_of}
    return {'status': 'unknown', 'request_key': request_key, 'operation': operation,
            'object_key': None, 'request_sha256': request_sha256}


def status(path, request_key, request_sha256):
    _request_key(request_key)
    if not isinstance(request_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', request_sha256):
        raise SourceWriteError('来源写入请求摘要无效')
    with closing(sqlite3.connect(Path(path))) as db:
        row = db.execute('SELECT operation FROM source_write_requests WHERE request_key=? UNION ALL '
                         'SELECT operation FROM source_write_fences WHERE request_key=? LIMIT 1',
                         (request_key, request_key)).fetchone()
        if not row:
            return {'status': 'unknown', 'request_key': request_key, 'request_sha256': request_sha256}
        return _state(db, request_key, row[0], request_sha256)


def validate_retry(db, request_key, operation, key, sha, raw, retry_of):
    prior = db.execute('SELECT retry_of,operation,object_key,request_sha256,request_json '
                       'FROM source_write_retries WHERE request_key=?', (request_key,)).fetchone()
    if retry_of is None:
        if prior:
            raise SourceWriteError('该请求必须沿用已记录的关联重试')
        return
    _request_key(retry_of)
    if retry_of == request_key:
        raise SourceWriteError('来源写入不能关联自身重试')
    original = db.execute('SELECT operation,object_key,request_sha256,request_json,created_at '
                          'FROM source_write_fences WHERE request_key=?', (retry_of,)).fetchone()
    same_content = bool(original and original[0] == operation)
    if same_content and operation in ('recovery_prepare', 'recovery_activate'):
        old_request, new_request = json.loads(original[3]), json.loads(raw)
        old_request.pop('destination', None)
        new_request.pop('destination', None)
        same_content = (_canonical(old_request) == _canonical(new_request)
                        and original[1] != key)
    else:
        same_content = same_content and original[:4] == (operation, key, sha, raw)
    if not same_content:
        raise SourceWriteError('关联重试必须使用相同内容并指向已封止的来源写入')
    wanted = (retry_of, operation, key, sha, raw)
    if prior:
        if prior != wanted:
            raise SourceWriteError('请求标识已用于不同的来源关联重试')
        return
    if db.execute('SELECT 1 FROM source_write_retries WHERE retry_of=?', (retry_of,)).fetchone():
        raise SourceWriteError('原来源写入已有直接重试，请先核对该重试')


def _retry(db, request_key, operation, key, sha, raw, retry_of, now):
    prior = db.execute('SELECT retry_of,operation,object_key,request_sha256,request_json '
                       'FROM source_write_retries WHERE request_key=?', (request_key,)).fetchone()
    validate_retry(db, request_key, operation, key, sha, raw, retry_of)
    if retry_of is None or prior:
        return
    db.execute('INSERT INTO source_write_retries VALUES (?,?,?,?,?,?,?)',
               (request_key, retry_of, operation, key, sha, raw, now))


def _check_object_fences(db, request_key, operation, key, retry_of):
    rows = db.execute('SELECT f.request_key FROM source_write_fences f '
                      'WHERE f.operation=? AND f.object_key=?', (operation, key)).fetchall()
    for (fenced_key,) in rows:
        if fenced_key == retry_of:
            continue
        retry = db.execute('SELECT r.request_key FROM source_write_retries r '
                           'WHERE r.retry_of=? AND (EXISTS (SELECT 1 FROM source_write_requests q '
                           'WHERE q.request_key=r.request_key) OR EXISTS (SELECT 1 FROM source_write_fences f2 '
                           'WHERE f2.request_key=r.request_key))', (fenced_key,)).fetchone()
        if not retry:
            raise SourceWriteError('同一资料对象存在尚未处理的待确认写入，请先查询或封止原请求')


def accept(db, *, request_key, operation, request, request_sha256, retry_of, insert):
    request_key = _request_key(request_key)
    if operation not in ('source_review', 'source_correction', 'recovery_prepare', 'recovery_activate'):
        raise SourceWriteError('来源写入操作无效')
    raw = _canonical(request)
    calculated = hashlib.sha256(raw).hexdigest()
    if request_sha256 != calculated:
        raise SourceWriteError('来源写入请求摘要不匹配')
    key = object_key(operation, request)
    current = _state(db, request_key, operation, calculated)
    if current['status'] == 'fenced':
        raise SourceWriteFenced(request_key, calculated, operation, retry_of)
    if current['status'] == 'accepted':
        row = db.execute('SELECT object_key,request_json FROM source_write_requests WHERE request_key=?',
                         (request_key,)).fetchone()
        if current['status'] == 'accepted':
            if row != (key, raw):
                raise SourceWriteError('已接受的来源写入内容无法核验')
            retry = db.execute('SELECT retry_of FROM source_write_retries WHERE request_key=?', (request_key,)).fetchone()
            if (retry[0] if retry else None) != retry_of:
                raise SourceWriteError('已接受来源写入的关联重试无法核验')
            return current
        row = db.execute('SELECT object_key,request_json FROM source_write_fences WHERE request_key=?',
                         (request_key,)).fetchone()
        if row != (key, raw):
            raise SourceWriteError('已封止的来源写入内容无法核验')
        retry = db.execute('SELECT retry_of,request_sha256,request_json FROM source_write_retries WHERE request_key=?',
                           (request_key,)).fetchone()
        if ((retry[0] if retry else None) != retry_of
                or (retry and retry != (retry_of, calculated, raw))):
            raise SourceWriteError('封止来源写入的关联重试无法核验')
        return current
    now = datetime.now(timezone.utc).isoformat()
    _retry(db, request_key, operation, key, calculated, raw, retry_of, now)
    _check_object_fences(db, request_key, operation, key, retry_of)
    result = insert()
    result_raw = _canonical(result)
    db.execute('INSERT INTO source_write_requests VALUES (?,?,?,?,?,?,?,?)',
               (request_key, operation, key, calculated, raw, result_raw, now, retry_of))
    return {'status': 'accepted', 'request_key': request_key, 'operation': operation,
            'object_key': key, 'request_sha256': calculated, 'retry_of': retry_of, 'result': result}


def fence(path, *, request_key, operation, request, request_sha256, retry_of=None):
    request_key = _request_key(request_key)
    if operation not in ('source_review', 'source_correction', 'recovery_prepare', 'recovery_activate'):
        raise SourceWriteError('来源写入操作无效')
    raw = _canonical(request)
    calculated = hashlib.sha256(raw).hexdigest()
    if request_sha256 != calculated:
        raise SourceWriteError('来源写入请求摘要不匹配')
    key = object_key(operation, request)
    with closing(sqlite3.connect(Path(path))) as db, db:
        db.execute('BEGIN IMMEDIATE')
        current = _state(db, request_key, operation, calculated)
        if current['status'] != 'unknown':
            row = db.execute('SELECT object_key,request_json FROM source_write_requests WHERE request_key=?',
                             (request_key,)).fetchone() if current['status'] == 'accepted' else db.execute(
                             'SELECT object_key,request_json FROM source_write_fences WHERE request_key=?',
                             (request_key,)).fetchone()
            if row != (key, raw):
                raise SourceWriteError('原来源请求内容与当前封止请求不符')
            return current
        now = datetime.now(timezone.utc).isoformat()
        _retry(db, request_key, operation, key, calculated, raw, retry_of, now)
        _check_object_fences(db, request_key, operation, key, retry_of)
        db.execute('INSERT INTO source_write_fences VALUES (?,?,?,?,?,?)',
                   (request_key, operation, key, calculated, raw, now))
        return {'status': 'fenced', 'request_key': request_key, 'operation': operation,
                'object_key': key, 'request_sha256': calculated, 'retry_of': retry_of}
