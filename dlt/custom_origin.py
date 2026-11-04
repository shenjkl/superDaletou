"""Append-only, explicitly unverified declarations for old custom scenarios."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from dlt.candidates import _canonical
from dlt.custom_scenarios import check_custom_scenario
from dlt.pair_repository import PairRepository


class OriginError(ValueError):
    pass


class OriginFenced(OriginError):
    def __init__(self, event_id, request_sha256, retry_of=None):
        super().__init__('该来源声明请求已封止，迟到请求不能保存')
        self.event_id = event_id
        self.request_sha256 = request_sha256
        self.retry_of = retry_of


def _table(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='custom_origin_declarations'").fetchone())


def _scenario(db, evaluation_key):
    row = db.execute('SELECT pair_key,request_json,result_sha256,result_json FROM draft_evaluations WHERE request_key=?', (evaluation_key,)).fetchone()
    if not row or hashlib.sha256(row[3]).hexdigest() != row[2]:
        raise OriginError('评估不存在或摘要不符')
    request, result = json.loads(row[1]), json.loads(row[3])
    scenario = request['scenario']
    digest = hashlib.sha256(_canonical(scenario)).hexdigest()
    if (request['pair_key'] != row[0] or result['scenario_sha256'] != digest
            or scenario.get('kind') != 'custom_payout_v1'
            or 'provenance' in scenario.get('custom_input', {})):
        raise OriginError('只有未声明来源的旧自定义评估可补录')
    pair = db.execute('SELECT pair_sha256,pair_json FROM draft_pairs WHERE request_key=?', (row[0],)).fetchone()
    if not pair:
        raise OriginError('评估关联的组合不存在')
    check_custom_scenario(PairRepository._decode(pair), scenario)
    return digest


def declarations(db, evaluation_key, scenario_sha):
    if not _table(db):
        return []
    output = []
    for event_id, digest, raw, created in db.execute(
            'SELECT event_id,content_sha256,content_json,created_at FROM custom_origin_declarations WHERE evaluation_key=? ORDER BY rowid', (evaluation_key,)):
        data = json.loads(raw)
        if (hashlib.sha256(raw).hexdigest() != digest or data.get('version') != 'custom-origin-v1'
                or data.get('event_id') != event_id or data.get('evaluation_key') != evaluation_key
                or data.get('scenario_sha256') != scenario_sha or data.get('created_at') != created
                or data.get('kind') not in ('synthetic', 'historical')
                or (data['kind'] == 'synthetic' and data.get('issue') is not None)
                or (data['kind'] == 'historical' and (not isinstance(data.get('issue'), str)
                    or not re.fullmatch(r'[0-9]{5}', data['issue'])))
                or not isinstance(data.get('reason'), str) or not data['reason'].strip()
                or len(data['reason']) > 1000 or data.get('assurance') != 'user_declared_not_verified'):
            raise OriginError('来源补录记录无效')
        when = datetime.fromisoformat(created)
        if when.tzinfo is None or when.utcoffset() is None:
            raise OriginError('来源补录时间无效')
        output.append(data)
    return output


def _request(evaluation_key, kind, issue, reason, confirmed):
    if (not isinstance(evaluation_key, str) or not evaluation_key.strip() or len(evaluation_key) > 128
            or kind not in ('synthetic', 'historical') or confirmed is not True
            or not isinstance(reason, str) or not reason.strip() or len(reason.strip()) > 1000
            or (kind == 'synthetic' and issue is not None)
            or (kind == 'historical' and (not isinstance(issue, str) or not re.fullmatch(r'[0-9]{5}', issue)))):
        raise OriginError('请填写来源、说明和确认；历史来源需五位期号')
    return {'evaluation_key': evaluation_key, 'kind': kind, 'issue': issue,
            'reason': reason.strip(), 'confirmed': True}


def _digest(request):
    return hashlib.sha256(_canonical(request)).hexdigest()


def _ensure_write_tables(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS custom_origin_write_fences (
            event_id TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS custom_origin_write_retries (
            event_id TEXT PRIMARY KEY, retry_of TEXT NOT NULL UNIQUE REFERENCES custom_origin_write_fences(event_id),
            request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS custom_origin_write_fences_no_update BEFORE UPDATE ON custom_origin_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable custom origin write fence'); END;
        CREATE TRIGGER IF NOT EXISTS custom_origin_write_fences_no_delete BEFORE DELETE ON custom_origin_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable custom origin write fence'); END;
        CREATE TRIGGER IF NOT EXISTS custom_origin_write_retries_no_update BEFORE UPDATE ON custom_origin_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable custom origin write retry'); END;
        CREATE TRIGGER IF NOT EXISTS custom_origin_write_retries_no_delete BEFORE DELETE ON custom_origin_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable custom origin write retry'); END;
        CREATE TRIGGER IF NOT EXISTS custom_origin_declarations_no_fenced_insert
            BEFORE INSERT ON custom_origin_declarations
            WHEN EXISTS (SELECT 1 FROM custom_origin_write_fences WHERE event_id=NEW.event_id)
            BEGIN SELECT RAISE(ABORT, 'fenced custom origin declaration'); END;
    ''')


def _validate_event_id(event_id):
    if not isinstance(event_id, str) or not event_id.strip() or len(event_id) > 128:
        raise OriginError('来源声明请求标识无效')
    return event_id


def _check_retry(db, event_id, request, retry_of, created_at):
    existing = db.execute('SELECT retry_of,request_sha256,request_json FROM custom_origin_write_retries WHERE event_id=?',
                          (event_id,)).fetchone()
    raw = _canonical(request)
    digest = hashlib.sha256(raw).hexdigest()
    if retry_of is None:
        if existing:
            raise OriginError('该请求必须继续使用已保存的关联重试')
        return
    _validate_event_id(retry_of)
    if retry_of == event_id:
        raise OriginError('来源声明不能关联自身重试')
    original = db.execute('SELECT request_sha256,request_json FROM custom_origin_write_fences WHERE event_id=?',
                          (retry_of,)).fetchone()
    if not original or original[0] != digest or original[1] != raw:
        raise OriginError('关联重试必须指向同内容且已封止的来源声明')
    if existing:
        if existing != (retry_of, digest, raw):
            raise OriginError('请求标识已用于不同的来源声明重试')
        return
    prior = db.execute('SELECT event_id FROM custom_origin_write_retries WHERE retry_of=?', (retry_of,)).fetchone()
    if prior:
        raise OriginError('原来源声明请求已有直接重试，请先核对该重试')
    db.execute('INSERT INTO custom_origin_write_retries VALUES (?,?,?,?,?)',
               (event_id, retry_of, digest, raw, created_at))


def _stored_declaration(db, event_id):
    row = db.execute('SELECT evaluation_key,content_sha256,content_json,created_at '
                     'FROM custom_origin_declarations WHERE event_id=?', (event_id,)).fetchone()
    if not row:
        return None
    data = json.loads(row[2])
    if hashlib.sha256(row[2]).hexdigest() != row[1] or data.get('event_id') != event_id:
        raise OriginError('已保存的来源声明无法核验')
    request = _request(row[0], data.get('kind'), data.get('issue'), data.get('reason'), True)
    return request, data


def _write_state(db, event_id, request, retry_of=None):
    digest = _digest(request)
    saved = _stored_declaration(db, event_id)
    if saved:
        if saved[0] != request:
            raise OriginError('请求标识已用于不同声明')
        scenario_digest = _scenario(db, request['evaluation_key'])
        if saved[1].get('scenario_sha256') != scenario_digest:
            raise OriginError('已保存来源声明的评估摘要无法核验')
        declarations(db, request['evaluation_key'], scenario_digest)
        retry = db.execute('SELECT retry_of,request_sha256,request_json FROM custom_origin_write_retries WHERE event_id=?',
                           (event_id,)).fetchone()
        if ((retry[0] if retry else None) != retry_of
                or (retry and retry != (retry_of, digest, _canonical(request)))):
            raise OriginError('已保存声明的关联重试无法核验')
        return {'status': 'accepted', 'event_id': event_id, 'request_sha256': digest, 'record': saved[1]}
    row = db.execute('SELECT request_sha256,request_json FROM custom_origin_write_fences WHERE event_id=?',
                     (event_id,)).fetchone()
    if row:
        if row != (digest, _canonical(request)):
            raise OriginError('请求标识已用于不同封止内容')
        retry = db.execute('SELECT retry_of,request_sha256,request_json FROM custom_origin_write_retries WHERE event_id=?',
                           (event_id,)).fetchone()
        if ((retry[0] if retry else None) != retry_of
                or (retry and retry != (retry_of, digest, _canonical(request)))):
            raise OriginError('封止请求的关联重试无法核验')
        return {'status': 'fenced', 'event_id': event_id, 'request_sha256': digest, 'retry_of': retry_of}
    return {'status': 'unknown', 'event_id': event_id, 'request_sha256': digest, 'retry_of': retry_of}


def record_declaration(path, *, event_id, evaluation_key, kind, issue, reason, confirmed, retry_of=None):
    event_id = _validate_event_id(event_id)
    request = _request(evaluation_key, kind, issue, reason, confirmed)
    path = Path(path)
    if not path.is_file():
        raise OriginError('研究数据库不存在')
    from dlt.recovery_guard import require_path_execution_allowed, require_execution_allowed
    require_path_execution_allowed(path)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('PRAGMA foreign_keys=ON')
        db.executescript('''
            CREATE TABLE IF NOT EXISTS custom_origin_declarations (
                event_id TEXT PRIMARY KEY, evaluation_key TEXT NOT NULL REFERENCES draft_evaluations(request_key),
                content_sha256 TEXT NOT NULL, content_json BLOB NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS custom_origin_declarations_no_update BEFORE UPDATE ON custom_origin_declarations
                BEGIN SELECT RAISE(ABORT, 'immutable custom origin declaration'); END;
            CREATE TRIGGER IF NOT EXISTS custom_origin_declarations_no_delete BEFORE DELETE ON custom_origin_declarations
                BEGIN SELECT RAISE(ABORT, 'immutable custom origin declaration'); END;
        ''')
        _ensure_write_tables(db)
        db.execute('BEGIN IMMEDIATE'); require_execution_allowed(db)
        state = _write_state(db, event_id, request, retry_of)
        if state['status'] == 'accepted':
            return state['record']
        if state['status'] == 'fenced':
            raise OriginFenced(event_id, state['request_sha256'], retry_of)
        now = datetime.now(timezone.utc).isoformat()
        _check_retry(db, event_id, request, retry_of, now)
        scenario_digest = _scenario(db, evaluation_key)
        declarations(db, evaluation_key, scenario_digest)
        record = {'version': 'custom-origin-v1', 'event_id': event_id, 'evaluation_key': evaluation_key,
                  'scenario_sha256': scenario_digest, 'kind': kind, 'issue': issue, 'reason': request['reason'],
                  'created_at': now, 'assurance': 'user_declared_not_verified'}
        payload = _canonical(record)
        db.execute('INSERT INTO custom_origin_declarations VALUES (?,?,?,?,?)',
                   (event_id, evaluation_key, hashlib.sha256(payload).hexdigest(), payload, now))
        return record


def fence_declaration(path, *, event_id, request_sha256, evaluation_key, kind, issue, reason, confirmed,
                      retry_of=None):
    event_id = _validate_event_id(event_id)
    request = _request(evaluation_key, kind, issue, reason, confirmed)
    digest = _digest(request)
    if request_sha256 != digest:
        raise OriginError('来源声明请求摘要不匹配')
    path = Path(path)
    if not path.is_file():
        raise OriginError('研究数据库不存在')
    from dlt.recovery_guard import require_path_execution_allowed, require_execution_allowed
    require_path_execution_allowed(path)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('''CREATE TABLE IF NOT EXISTS custom_origin_declarations (
            event_id TEXT PRIMARY KEY, evaluation_key TEXT NOT NULL REFERENCES draft_evaluations(request_key),
            content_sha256 TEXT NOT NULL, content_json BLOB NOT NULL, created_at TEXT NOT NULL)''')
        db.executescript('''
            CREATE TRIGGER IF NOT EXISTS custom_origin_declarations_no_update BEFORE UPDATE ON custom_origin_declarations
                BEGIN SELECT RAISE(ABORT, 'immutable custom origin declaration'); END;
            CREATE TRIGGER IF NOT EXISTS custom_origin_declarations_no_delete BEFORE DELETE ON custom_origin_declarations
                BEGIN SELECT RAISE(ABORT, 'immutable custom origin declaration'); END;
        ''')
        _ensure_write_tables(db)
        db.execute('BEGIN IMMEDIATE'); require_execution_allowed(db)
        state = _write_state(db, event_id, request, retry_of)
        if state['status'] != 'unknown':
            return state
        now = datetime.now(timezone.utc).isoformat()
        _check_retry(db, event_id, request, retry_of, now)
        raw = _canonical(request)
        db.execute('INSERT INTO custom_origin_write_fences VALUES (?,?,?,?)', (event_id, digest, raw, now))
        return {'status': 'fenced', 'event_id': event_id, 'request_sha256': digest, 'retry_of': retry_of}


def declaration_write_status(path, *, event_id, request_sha256):
    event_id = _validate_event_id(event_id)
    if not isinstance(request_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', request_sha256):
        raise OriginError('来源声明请求摘要无效')
    with closing(sqlite3.connect(path)) as db:
        db.execute('BEGIN')
        declaration_table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='custom_origin_declarations'").fetchone()
        if declaration_table:
            saved = _stored_declaration(db, event_id)
        else:
            saved = None
        if saved:
            digest = _digest(saved[0])
            if digest != request_sha256:
                raise OriginError('请求摘要与已保存来源声明不匹配')
            scenario_digest = _scenario(db, saved[0]['evaluation_key'])
            if saved[1].get('scenario_sha256') != scenario_digest:
                raise OriginError('已保存来源声明的评估摘要无法核验')
            declarations(db, saved[0]['evaluation_key'], scenario_digest)
            retry_table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='custom_origin_write_retries'").fetchone()
            retry = (db.execute('SELECT retry_of,request_sha256,request_json FROM custom_origin_write_retries WHERE event_id=?',
                                (event_id,)).fetchone() if retry_table else None)
            if retry and (retry[1] != digest or retry[2] != _canonical(saved[0])
                          or not db.execute('SELECT 1 FROM custom_origin_write_fences WHERE event_id=?',
                                            (retry[0],)).fetchone()):
                raise OriginError('来源声明关联重试记录无法核验')
            state = {'status': 'accepted', 'event_id': event_id, 'request_sha256': digest,
                     'record': saved[1], 'retry_of': retry[0] if retry else None}
        else:
            fences = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='custom_origin_write_fences'").fetchone()
            if not fences:
                state = {'status': 'unknown', 'event_id': event_id, 'request_sha256': request_sha256}
            else:
                row = db.execute('SELECT request_sha256,request_json FROM custom_origin_write_fences WHERE event_id=?',
                                 (event_id,)).fetchone()
                if not row:
                    state = {'status': 'unknown', 'event_id': event_id, 'request_sha256': request_sha256}
                elif row[0] != request_sha256 or hashlib.sha256(row[1]).hexdigest() != request_sha256:
                    raise OriginError('封止请求摘要不匹配')
                else:
                    request = json.loads(row[1])
                    normalized = _request(request.get('evaluation_key'), request.get('kind'), request.get('issue'),
                                          request.get('reason'), request.get('confirmed'))
                    if request != normalized or _canonical(request) != row[1]:
                        raise OriginError('封止来源声明内容无法核验')
                    retry = db.execute('SELECT retry_of,request_sha256,request_json FROM custom_origin_write_retries WHERE event_id=?',
                                       (event_id,)).fetchone()
                    if retry and (retry[1] != request_sha256 or retry[2] != row[1]
                                  or not db.execute('SELECT 1 FROM custom_origin_write_fences WHERE event_id=?',
                                                    (retry[0],)).fetchone()):
                        raise OriginError('来源声明关联重试记录无法核验')
                    state = {'status': 'fenced', 'event_id': event_id, 'request_sha256': request_sha256,
                             'retry_of': retry[0] if retry else None}
        return state


def list_legacy_custom(path):
    with closing(sqlite3.connect(path)) as db:
        db.execute('BEGIN')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='draft_evaluations'").fetchone():
            return []
        output = []
        for key, raw in db.execute('SELECT request_key,request_json FROM draft_evaluations ORDER BY rowid DESC'):
            try:
                scenario = json.loads(raw)['scenario']
                if scenario.get('kind') != 'custom_payout_v1' or 'provenance' in scenario.get('custom_input', {}):
                    continue
                digest = _scenario(db, key)
                output.append({'evaluation_key': key, 'custom_input': scenario['custom_input'],
                               'declarations': declarations(db, key, digest), 'status': 'available'})
            except (ValueError, TypeError, KeyError, AttributeError, sqlite3.Error):
                output.append({'evaluation_key': key, 'status': 'review_required'})
        return output


def custom_origin_exposure(db, issues):
    """Conservative exposure status; validates records but cannot verify claims."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='draft_evaluations'").fetchone():
        return []
    affected = []
    for key, pair_key, raw, result_sha, result_raw in db.execute('SELECT request_key,pair_key,request_json,result_sha256,result_json FROM draft_evaluations'):
        try:
            request = json.loads(raw)
            scenario = request['scenario']
            result = json.loads(result_raw)
            if (request['pair_key'] != pair_key or hashlib.sha256(result_raw).hexdigest() != result_sha
                    or result['scenario_sha256'] != hashlib.sha256(_canonical(scenario)).hexdigest()):
                raise OriginError('评估内容不一致')
            if scenario.get('kind') != 'custom_payout_v1':
                continue
            if 'provenance' in scenario.get('custom_input', {}):
                provenance = scenario['custom_input']['provenance']
                pair_row = db.execute('SELECT p.pair_sha256,p.pair_json FROM draft_pairs p JOIN draft_evaluations e ON p.request_key=e.pair_key WHERE e.request_key=?', (key,)).fetchone()
                if not pair_row:
                    raise OriginError('组合不存在')
                check_custom_scenario(PairRepository._decode(pair_row), scenario)
                declared = [provenance]
            else:
                declared = declarations(db, key, _scenario(db, key))
            if not declared:
                affected.append({'evaluation_key': key, 'reason': '未声明来源'})
            elif any(x['kind'] == 'historical' and x['issue'] in issues for x in declared):
                affected.append({'evaluation_key': key, 'reason': '本人声明参考该范围内奖期'})
        except (ValueError, TypeError, KeyError, AttributeError, sqlite3.Error):
            affected.append({'evaluation_key': key, 'reason': '来源记录无法核验'})
    return affected
