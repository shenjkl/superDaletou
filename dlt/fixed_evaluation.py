"""One locally fixed, multi-method descriptive evaluation per excluded range.

The range, protocol and scenarios are retained before execution. This cannot
prove historical availability, source authenticity, or researcher blindness.
"""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import sqlite3
import subprocess
import sys
import threading
import uuid

import numpy as np

from dlt.backtest import BacktestError, _validate
from dlt.backtest_limits import DEFAULT_BACKTEST_LIMITS
from dlt.candidates import _canonical
from dlt.historical_scenarios import backtest_scenarios
from dlt.holdout_reservation import reserved_issues, reservation_lineage
from dlt.holdout_audit import audit_holdout
from dlt.paired_description import describe_backtest


class EvaluationError(ValueError):
    pass


class FixedEvaluationFenced(EvaluationError):
    def __init__(self, request_key, operation, request_sha256, retry_of=None):
        super().__init__('该固定评估请求已封止，迟到请求不能执行')
        self.request_key = request_key
        self.operation = operation
        self.request_sha256 = request_sha256
        self.retry_of = retry_of


def _hash(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _without_amounts(report):
    """Project immutable report evidence while omitting replaceable calculations."""
    value = deepcopy(report)
    for method in value['methods']:
        result = method['result']
        result.pop('descriptive_mean_fraction')
        for period in result['periods']:
            period.pop('mean_difference_fraction')
            for replica in period['replicas']:
                if replica['status'] == 'complete':
                    for name in ('candidate_settlement', 'control_settlement', 'difference_yuan'):
                        replica.pop(name)
    return value


def _derive_amount_report(db, spec, original):
    """Rebuild fixed-report amounts from the original pairs; never train again."""
    from dlt.backtest_repository import BacktestRepository, _recheck_settlements
    if (original.get('version') != 'fixed-descriptive-report-v1'
            or original.get('reservation_id') != spec['reservation_id']
            or original.get('protocol_sha256') != _hash(spec)
            or not isinstance(original.get('methods'), list)
            or [item['id'] for item in original['methods']] != [item['id'] for item in spec['methods']]):
        raise EvaluationError('原报告与固定协议不一致，不能更正')
    derived = deepcopy(original)
    for item, fixed in zip(derived['methods'], spec['methods']):
        result = item['result']
        if (result.get('protocol') != fixed['protocol'] or result.get('scenarios') != spec['scenarios']
                or result.get('protocol_sha256') != _hash(fixed['protocol'])
                or result.get('scenarios_sha256') != _hash(spec['scenarios'])):
            raise EvaluationError('原报告的协议或奖金情景不一致，不能更正')
        BacktestRepository._check_dependencies(db, result, check_calculation=False)
        item['result'] = _recheck_settlements(db, result, rebuild=True)
    if _without_amounts(derived) != _without_amounts(original):
        raise EvaluationError('更正涉及金额以外的报告内容')
    return derived


def _check_original_attempt(db, reservation_id, base_sha, original):
    attempt = db.execute('SELECT status,report_sha256,report_json FROM fixed_evaluation_attempts '
                         'WHERE reservation_id=? ORDER BY attempt_no DESC LIMIT 1', (reservation_id,)).fetchone()
    if (not attempt or attempt[0] != 'complete' or attempt[1] != base_sha
            or not isinstance(attempt[2], bytes) or hashlib.sha256(attempt[2]).hexdigest() != base_sha
            or json.loads(attempt[2]) != original):
        raise EvaluationError('原报告与已完成运行记录不一致，不能更正')


def _checked_correction(db, spec, original, base_sha, row):
    ident, reservation_id, stored_base, report_sha, raw = row
    if not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != ident:
        raise EvaluationError('金额更正记录摘要不符')
    record = json.loads(raw)
    if (not isinstance(record, dict) or set(record) != {'version', 'reservation_id', 'base_report_sha256',
            'report_sha256', 'report', 'created_at', 'workflow_code_sha256', 'calculation_code_sha256', 'settlement_code_sha256'}
            or record['version'] != 'fixed-amount-correction-v1'
            or reservation_id != spec['reservation_id'] or record['reservation_id'] != reservation_id
            or stored_base != base_sha or record['base_report_sha256'] != base_sha
            or record['report_sha256'] != report_sha or _hash(record['report']) != report_sha
            or not all(isinstance(record[k], str) and re.fullmatch(r'[0-9a-f]{64}', record[k])
                       for k in ('workflow_code_sha256', 'calculation_code_sha256', 'settlement_code_sha256'))):
        raise EvaluationError('金额更正记录或原报告引用无效')
    created = datetime.fromisoformat(record['created_at'])
    if created.tzinfo is None or created.utcoffset() is None:
        raise EvaluationError('金额更正时间无效')
    _check_original_attempt(db, reservation_id, base_sha, original)
    if (_without_amounts(original) != _without_amounts(record['report'])
            or _derive_amount_report(db, spec, original) != record['report']):
        raise EvaluationError('金额更正结果无法按当前规则复核')
    return record


def _now():
    return datetime.now(timezone.utc).isoformat()


def _environment():
    root = Path(__file__).parent
    files = sorted(item.name for item in root.glob('*.py'))
    return {'python': platform.python_version(), 'numpy': np.__version__,
            'code': {name: hashlib.sha256((root/name).read_bytes()).hexdigest() for name in files}}


def _bounded(protocol):
    try:
        p = _validate(protocol)
    except BacktestError as exc:
        raise EvaluationError(str(exc)) from exc
    candidate = p['candidate']
    structure = candidate.get('structure'); front = candidate.get('front_count', 5); back = candidate.get('back_count', 2)
    if (structure not in ('singles', 'compound', 'dantuo')
            or (structure == 'compound' and (type(front) is not int or not 5 <= front <= 35
                                              or type(back) is not int or not 2 <= back <= 12))
            or any(not isinstance(candidate.get(key), str) or not candidate[key].strip() or len(candidate[key]) > 128
                   for key in ('rule_version', 'calendar_version'))):
        raise EvaluationError('组合结构或规则、日历版本无效')
    price = candidate['additional_price_fen'] if candidate['additional'] else candidate['base_price_fen']
    count = (candidate['budget_fen']//price if structure == 'singles' else
             math.comb(front, 5)*math.comb(back, 2) if structure == 'compound' else 18)
    if not 1 <= count <= candidate['max_tickets'] or count*price > candidate['budget_fen']:
        raise EvaluationError('预算或子注上限无法容纳组合结构')
    return p


def _load(db, reservation_id):
    row = db.execute('SELECT protocol_sha256,protocol_json FROM fixed_evaluations WHERE reservation_id=?', (reservation_id,)).fetchone()
    if not row or hashlib.sha256(row[1]).hexdigest() != row[0]:
        raise EvaluationError('固定评估协议不存在或摘要不符')
    try:
        spec = json.loads(row[1]); methods = spec['methods']
        if (spec['reservation_id'] != reservation_id or spec['version'] != 'fixed-descriptive-evaluation-v1'
                or spec['independence'] != 'not_verified' or spec['aggregation'] != 'arithmetic_mean_v1'
                or spec['primary_metric'] != 'after_tax_net_paired_difference'
                or spec['missing_policy'] != 'incomplete_period_unpublished'
                or not isinstance(spec['hypothesis'], str) or not spec['hypothesis'].strip() or len(spec['hypothesis']) > 500
                or type(spec['timeout']) is not int
                or not DEFAULT_BACKTEST_LIMITS.timeout_seconds[0] <= spec['timeout'] <= DEFAULT_BACKTEST_LIMITS.timeout_seconds[1]
                or not isinstance(spec['environment'], dict) or not isinstance(spec['scenarios'], dict)
                or not isinstance(methods, list) or not 1 <= len(methods) <= 5
                or any(not isinstance(item, dict) or set(item) != {'id', 'protocol'}
                       or not isinstance(item['id'], str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,40}', item['id'])
                       or not isinstance(item['protocol'], dict) for item in methods)
                or len({item['id'] for item in methods}) != len(methods)):
            raise EvaluationError('固定评估协议内容无效')
        return spec
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, EvaluationError):
            raise
        raise EvaluationError('固定评估协议内容无效') from exc


def _check_scope(db, reservation_id, protocol):
    reserved = db.execute('SELECT content_json FROM holdout_reservations WHERE id=?', (reservation_id,)).fetchone()
    if not reserved:
        raise EvaluationError('固定范围不存在')
    reservation = json.loads(reserved[0]); allowed = set(reservation['issues'])
    if protocol.get('snapshot_id') != reservation.get('snapshot_id') or protocol.get('targets') != reservation['issues']:
        raise EvaluationError('评估快照或目标期与固定范围不一致')
    all_reserved = reserved_issues(db)
    rows = db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue', (protocol['snapshot_id'],)).fetchall()
    issues = [row[0] for row in rows]; used = set(protocol['targets'])
    for issue in protocol['targets']:
        if issue not in issues:
            raise EvaluationError('目标期不在固定快照中')
        pos = issues.index(issue)
        used.update(issues[max(0, pos-protocol['history_periods']):pos])
    if used & (all_reserved-allowed):
        raise EvaluationError('评估历史窗口涉及其他固定排除范围')


def _validate_spec_methods(db, reservation_id, spec):
    """Recheck frozen execution constraints, including on recovered databases."""
    first = spec['methods'][0]['protocol']; baseline = first['candidate']
    total = 0
    for item in spec['methods']:
        protocol = item['protocol']
        if _bounded(protocol) != protocol:
            raise EvaluationError('固定评估方法配置无效')
        _check_scope(db, reservation_id, protocol)
        candidate = protocol['candidate']
        if (protocol['replicas'] != first['replicas'] or any(candidate.get(key) != baseline.get(key) for key in
                ('budget_fen', 'base_price_fen', 'additional_price_fen', 'additional', 'structure',
                 'front_count', 'back_count', 'max_tickets'))):
            raise EvaluationError('固定评估方法的副本、预算或结构不一致')
        total += len(protocol['targets'])*len(protocol['replicas'])
    if total > 100:
        raise EvaluationError('所有方法合计奖期与副本组合不能超过 100')


def _write_request(operation, request):
    if not isinstance(request, dict):
        raise EvaluationError('固定评估写入请求无效')
    if operation == 'freeze':
        if (set(request) != {'reservation_id', 'methods', 'hypothesis', 'timeout', 'confirmed'}
                or not isinstance(request.get('reservation_id'), str)
                or not re.fullmatch(r'[0-9a-f]{64}', request['reservation_id'])
                or not isinstance(request.get('hypothesis'), str) or not request['hypothesis'].strip()
                or len(request['hypothesis'].strip()) > 500 or type(request.get('timeout')) is not int
                or not DEFAULT_BACKTEST_LIMITS.timeout_seconds[0] <= request['timeout'] <= DEFAULT_BACKTEST_LIMITS.timeout_seconds[1]
                or request.get('confirmed') is not True
                or not isinstance(request.get('methods'), list) or not 1 <= len(request['methods']) <= 5):
            raise EvaluationError('固定评估协议请求无效')
        methods = []
        for item in request['methods']:
            if not isinstance(item, dict) or set(item) != {'id', 'protocol'} or not isinstance(item.get('id'), str):
                raise EvaluationError('固定评估方法请求无效')
            methods.append({'id': item['id'], 'protocol': _bounded(item['protocol'])})
        return {'reservation_id': request['reservation_id'], 'methods': methods,
                'hypothesis': request['hypothesis'].strip(), 'timeout': request['timeout'], 'confirmed': True}
    if operation == 'start':
        if (set(request) != {'reservation_id', 'retry'} or not isinstance(request.get('reservation_id'), str)
                or not re.fullmatch(r'[0-9a-f]{64}', request['reservation_id'])
                or type(request.get('retry')) is not bool):
            raise EvaluationError('固定评估启动请求无效')
        return {'reservation_id': request['reservation_id'], 'retry': request['retry']}
    if operation == 'supersede':
        if (set(request) != {'reservation_id', 'reason', 'confirmed'}
                or not isinstance(request.get('reservation_id'), str)
                or not re.fullmatch(r'[0-9a-f]{64}', request['reservation_id'])
                or not isinstance(request.get('reason'), str) or not request['reason'].strip()
                or len(request['reason'].strip()) > 1000 or request.get('confirmed') is not True):
            raise EvaluationError('固定评估新版本请求无效')
        return {'reservation_id': request['reservation_id'], 'reason': request['reason'].strip(), 'confirmed': True}
    if operation == 'recalculate_amounts':
        if (set(request) != {'reservation_id'} or not isinstance(request.get('reservation_id'), str)
                or not re.fullmatch(r'[0-9a-f]{64}', request['reservation_id'])):
            raise EvaluationError('固定评估金额更正请求无效')
        return {'reservation_id': request['reservation_id']}
    raise EvaluationError('未知的固定评估写入类型')


def _write_key(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise EvaluationError('固定评估请求标识无效')
    return value


def _write_tables(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS fixed_evaluation_write_requests (
            request_key TEXT PRIMARY KEY, operation TEXT NOT NULL,
            reservation_id TEXT NOT NULL REFERENCES holdout_reservations(id),
            request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, result_json BLOB NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fixed_evaluation_write_fences (
            request_key TEXT PRIMARY KEY, operation TEXT NOT NULL,
            reservation_id TEXT NOT NULL REFERENCES holdout_reservations(id),
            request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fixed_evaluation_write_retries (
            request_key TEXT PRIMARY KEY,
            retry_of TEXT NOT NULL UNIQUE REFERENCES fixed_evaluation_write_fences(request_key),
            operation TEXT NOT NULL, reservation_id TEXT NOT NULL,
            request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS fixed_evaluation_write_requests_no_update BEFORE UPDATE ON fixed_evaluation_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable fixed evaluation write request'); END;
        CREATE TRIGGER IF NOT EXISTS fixed_evaluation_write_requests_no_delete BEFORE DELETE ON fixed_evaluation_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable fixed evaluation write request'); END;
        CREATE TRIGGER IF NOT EXISTS fixed_evaluation_write_fences_no_update BEFORE UPDATE ON fixed_evaluation_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable fixed evaluation write fence'); END;
        CREATE TRIGGER IF NOT EXISTS fixed_evaluation_write_fences_no_delete BEFORE DELETE ON fixed_evaluation_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable fixed evaluation write fence'); END;
        CREATE TRIGGER IF NOT EXISTS fixed_evaluation_write_retries_no_update BEFORE UPDATE ON fixed_evaluation_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable fixed evaluation write retry'); END;
        CREATE TRIGGER IF NOT EXISTS fixed_evaluation_write_retries_no_delete BEFORE DELETE ON fixed_evaluation_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable fixed evaluation write retry'); END;
        CREATE TRIGGER IF NOT EXISTS fixed_evaluation_write_requests_no_fenced_insert BEFORE INSERT ON fixed_evaluation_write_requests
            WHEN EXISTS (SELECT 1 FROM fixed_evaluation_write_fences WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'fenced fixed evaluation write'); END;
        CREATE TRIGGER IF NOT EXISTS fixed_evaluation_write_fences_no_committed_insert BEFORE INSERT ON fixed_evaluation_write_fences
            WHEN EXISTS (SELECT 1 FROM fixed_evaluation_write_requests WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'committed fixed evaluation write'); END;
        CREATE TRIGGER IF NOT EXISTS fixed_evaluation_attempts_no_fenced_insert BEFORE INSERT ON fixed_evaluation_attempts
            WHEN EXISTS (SELECT 1 FROM fixed_evaluation_write_fences WHERE request_key=NEW.request_key AND operation='start')
            BEGIN SELECT RAISE(ABORT, 'fenced fixed evaluation start'); END;
    ''')


def _write_state(db, request_key, operation, request, retry_of=None):
    raw = _canonical(request); digest = hashlib.sha256(raw).hexdigest(); reservation_id = request['reservation_id']
    saved = db.execute('SELECT operation,reservation_id,request_sha256,request_json,result_json '
                       'FROM fixed_evaluation_write_requests WHERE request_key=?', (request_key,)).fetchone()
    fence = db.execute('SELECT operation,reservation_id,request_sha256,request_json FROM fixed_evaluation_write_fences '
                       'WHERE request_key=?', (request_key,)).fetchone()
    retry = db.execute('SELECT retry_of,operation,reservation_id,request_sha256,request_json '
                       'FROM fixed_evaluation_write_retries WHERE request_key=?', (request_key,)).fetchone()
    if saved and fence:
        raise EvaluationError('固定评估请求同时存在接受与封止记录')
    if retry and not (saved or fence):
        raise EvaluationError('固定评估关联重试缺少接受或封止记录')
    expected_retry = (operation, reservation_id, digest, raw)
    if retry and (retry_of != retry[0] or retry[1:] != expected_retry):
        raise EvaluationError('固定评估关联重试与请求不一致')
    if not retry and retry_of is not None and (saved or fence):
        raise EvaluationError('固定评估重试关系无法核验')
    if saved:
        if saved[:4] != (operation, reservation_id, digest, raw):
            raise EvaluationError('固定评估请求标识已用于不同内容')
        result = json.loads(saved[4])
        if _canonical(result) != saved[4] or not isinstance(result, dict):
            raise EvaluationError('已保存的固定评估请求结果无法核验')
        return {'status': 'accepted', 'request_key': request_key, 'operation': operation,
                'reservation_id': reservation_id, 'request_sha256': digest,
                'retry_of': retry[0] if retry else None, 'result': result}
    if fence:
        if fence != (operation, reservation_id, digest, raw):
            raise EvaluationError('固定评估请求标识已用于不同封止内容')
        return {'status': 'fenced', 'request_key': request_key, 'operation': operation,
                'reservation_id': reservation_id, 'request_sha256': digest,
                'retry_of': retry[0] if retry else None}
    return {'status': 'unknown', 'request_key': request_key, 'operation': operation,
            'reservation_id': reservation_id, 'request_sha256': digest,
            'retry_of': retry[0] if retry else None}


def _add_write_retry(db, request_key, operation, request, retry_of, created_at):
    raw = _canonical(request); digest = hashlib.sha256(raw).hexdigest(); reservation_id = request['reservation_id']
    existing = db.execute('SELECT retry_of,operation,reservation_id,request_sha256,request_json '
                          'FROM fixed_evaluation_write_retries WHERE request_key=?', (request_key,)).fetchone()
    if retry_of is None:
        if existing:
            raise EvaluationError('该固定评估请求必须沿用已保存的关联重试')
        return
    _write_key(retry_of)
    if retry_of == request_key:
        raise EvaluationError('固定评估请求不能关联自身重试')
    original = db.execute('SELECT operation,reservation_id,request_sha256,request_json '
                          'FROM fixed_evaluation_write_fences WHERE request_key=?', (retry_of,)).fetchone()
    if original != (operation, reservation_id, digest, raw):
        raise EvaluationError('固定评估关联重试必须指向同操作、同范围、同内容的封止请求')
    expected = (retry_of, operation, reservation_id, digest, raw)
    if existing:
        if existing != expected:
            raise EvaluationError('固定评估请求标识已用于不同关联重试')
        return
    if db.execute('SELECT 1 FROM fixed_evaluation_write_retries WHERE retry_of=?', (retry_of,)).fetchone():
        raise EvaluationError('原固定评估请求已有直接重试')
    db.execute('INSERT INTO fixed_evaluation_write_retries VALUES (?,?,?,?,?,?,?)',
               (request_key, retry_of, operation, reservation_id, digest, raw, created_at))


def _save_write(db, request_key, operation, request, retry_of, result):
    raw = _canonical(request); digest = hashlib.sha256(raw).hexdigest()
    db.execute('INSERT INTO fixed_evaluation_write_requests VALUES (?,?,?,?,?,?,?)',
               (request_key, operation, request['reservation_id'], digest, raw, _canonical(result), _now()))
    return {'status': 'accepted', 'request_key': request_key, 'operation': operation,
            'reservation_id': request['reservation_id'], 'request_sha256': digest,
            'retry_of': retry_of, 'result': result}


def validate_execution(db, reservation_id, attempt_id, method_id, protocol, scenarios):
    """Check the complete frozen authorization at each internal execution."""
    _, successors = reservation_lineage(db)
    if reservation_id in successors:
        raise EvaluationError('评估协议已被新版本替代，不能运行')
    spec = _load(db, reservation_id)
    _validate_spec_methods(db, reservation_id, spec)
    state = db.execute('SELECT status FROM fixed_evaluations WHERE reservation_id=?', (reservation_id,)).fetchone()
    attempt = db.execute('SELECT request_key,status FROM fixed_evaluation_attempts WHERE reservation_id=? ORDER BY attempt_no DESC LIMIT 1', (reservation_id,)).fetchone()
    matches = [item for item in spec['methods'] if item['id'] == method_id]
    if (not state or state[0] != 'running' or attempt != (attempt_id, 'running') or len(matches) != 1 or matches[0]['protocol'] != protocol
            or spec['scenarios'] != scenarios or spec['environment'] != _environment()):
        raise EvaluationError('固定评估任务、协议或执行环境不匹配')
    _check_scope(db, reservation_id, protocol)


class FixedEvaluations:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.threads = {}
        self.processes = {}

    def initialize(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS fixed_evaluations (
                    reservation_id TEXT PRIMARY KEY REFERENCES holdout_reservations(id),
                    protocol_sha256 TEXT NOT NULL, protocol_json BLOB NOT NULL, created_at TEXT NOT NULL,
                    status TEXT NOT NULL, started_at TEXT, finished_at TEXT, error TEXT,
                    report_sha256 TEXT, report_json BLOB
                );
                CREATE TRIGGER IF NOT EXISTS fixed_evaluations_fixed BEFORE UPDATE OF reservation_id,protocol_sha256,protocol_json,created_at ON fixed_evaluations
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation protocol'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_evaluations_no_delete BEFORE DELETE ON fixed_evaluations
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation protocol'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_evaluations_terminal BEFORE UPDATE ON fixed_evaluations WHEN OLD.status IN ('complete','failed')
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation result'); END;
                CREATE TABLE IF NOT EXISTS fixed_evaluation_attempts (
                    request_key TEXT PRIMARY KEY, reservation_id TEXT NOT NULL REFERENCES fixed_evaluations(reservation_id),
                    attempt_no INTEGER NOT NULL, retry_of TEXT REFERENCES fixed_evaluation_attempts(request_key),
                    retry_requested INTEGER NOT NULL, status TEXT NOT NULL, started_at TEXT NOT NULL,
                    finished_at TEXT, error TEXT, report_sha256 TEXT, report_json BLOB,
                    UNIQUE(reservation_id,attempt_no)
                );
                CREATE TRIGGER IF NOT EXISTS fixed_evaluation_attempts_fixed BEFORE UPDATE OF request_key,reservation_id,attempt_no,retry_of,retry_requested,started_at ON fixed_evaluation_attempts
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation attempt'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_evaluation_attempts_no_delete BEFORE DELETE ON fixed_evaluation_attempts
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation attempt'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_evaluation_attempts_terminal BEFORE UPDATE ON fixed_evaluation_attempts WHEN OLD.status IN ('complete','failed')
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation attempt result'); END;
                CREATE TABLE IF NOT EXISTS fixed_evaluation_corrections (
                    id TEXT PRIMARY KEY, reservation_id TEXT NOT NULL REFERENCES fixed_evaluations(reservation_id),
                    base_report_sha256 TEXT NOT NULL, report_sha256 TEXT NOT NULL, record_json BLOB NOT NULL,
                    UNIQUE(reservation_id,base_report_sha256,report_sha256)
                );
                CREATE TRIGGER IF NOT EXISTS fixed_evaluation_corrections_no_update BEFORE UPDATE ON fixed_evaluation_corrections
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation correction'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_evaluation_corrections_no_delete BEFORE DELETE ON fixed_evaluation_corrections
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation correction'); END;
            ''')
            _write_tables(db)
            db.execute('BEGIN IMMEDIATE')
            sql = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='fixed_evaluations_terminal'").fetchone()[0]
            normalized = ' '.join(sql.split())
            old = "CREATE TRIGGER fixed_evaluations_terminal BEFORE UPDATE ON fixed_evaluations WHEN OLD.status IN ('complete','failed') BEGIN SELECT RAISE(ABORT, 'immutable evaluation result'); END"
            new = "CREATE TRIGGER fixed_evaluations_terminal BEFORE UPDATE ON fixed_evaluations WHEN OLD.status='complete' BEGIN SELECT RAISE(ABORT, 'immutable evaluation result'); END"
            if normalized == old:
                db.execute('DROP TRIGGER fixed_evaluations_terminal')
                db.execute(new)
            elif normalized != new:
                raise EvaluationError('评估终态保护结构异常')
            for row in db.execute("SELECT reservation_id,status,started_at,finished_at,error,report_sha256,report_json FROM fixed_evaluations WHERE status!='pending'").fetchall():
                reservation_id, status, started, finished, error, report_sha, report = row
                if not db.execute('SELECT 1 FROM fixed_evaluation_attempts WHERE reservation_id=?', (reservation_id,)).fetchone():
                    db.execute('INSERT INTO fixed_evaluation_attempts VALUES (?,?,0,NULL,0,?,?,?,?,?,?)',
                               ('legacy-'+reservation_id, reservation_id, status, started, finished, error, report_sha, report))

    def freeze(self, reservation_id, methods, *, hypothesis, timeout, confirmed, request_key=None, retry_of=None):
        if (not isinstance(reservation_id, str) or not re.fullmatch(r'[0-9a-f]{64}', reservation_id)
                or not isinstance(hypothesis, str) or not hypothesis.strip() or len(hypothesis) > 500
                or type(timeout) is not int
                or not DEFAULT_BACKTEST_LIMITS.timeout_seconds[0] <= timeout <= DEFAULT_BACKTEST_LIMITS.timeout_seconds[1]
                or confirmed is not True
                or not isinstance(methods, list) or not 1 <= len(methods) <= 5):
            raise EvaluationError('请填写假设、方法清单和运行时限并确认固定')
        checked = _write_request('freeze', {'reservation_id': reservation_id, 'methods': methods,
                                             'hypothesis': hypothesis, 'timeout': timeout,
                                             'confirmed': confirmed})['methods']
        for item in checked:
            if not re.fullmatch(r'[a-zA-Z0-9_-]{1,40}', item['id']):
                raise EvaluationError('方法标识或协议无效')
        if len({item['id'] for item in checked}) != len(checked):
            raise EvaluationError('方法标识不能重复')
        if sum(len(x['protocol']['targets'])*len(x['protocol']['replicas']) for x in checked) > 100:
            raise EvaluationError('所有方法合计奖期与副本组合不能超过 100')
        write_request = {'reservation_id': reservation_id, 'methods': checked,
                         'hypothesis': hypothesis.strip(), 'timeout': timeout, 'confirmed': True}
        if request_key is not None:
            request_key = _write_key(request_key)
        with closing(sqlite3.connect(self.path)) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='holdout_reservations'").fetchone():
                raise EvaluationError('请先固定本机探索排除范围')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            if request_key is not None:
                state = _write_state(db, request_key, 'freeze', write_request, retry_of)
                if state['status'] == 'accepted':
                    return self._get(db, reservation_id)
                if state['status'] == 'fenced':
                    raise FixedEvaluationFenced(request_key, 'freeze', state['request_sha256'], retry_of)
                _add_write_retry(db, request_key, 'freeze', write_request, retry_of, _now())
            reserved_issues(db)
            row = db.execute('SELECT content_json FROM holdout_reservations WHERE id=?', (reservation_id,)).fetchone()
            if not row:
                raise EvaluationError('请先固定本机探索排除范围')
            reservation = json.loads(row[0]); first = checked[0]['protocol']; baseline = first['candidate']
            from dlt.custom_origin import custom_origin_exposure
            old = db.execute('SELECT protocol_sha256 FROM fixed_evaluations WHERE reservation_id=?', (reservation_id,)).fetchone()
            if old:
                previous = _load(db, reservation_id)
                if (previous['methods'] != checked or previous['hypothesis'] != hypothesis.strip()
                        or previous['timeout'] != timeout):
                    raise EvaluationError('该范围已有固定评估协议，不能替换或新增方法')
                result = self._get(db, reservation_id)
                if request_key is not None:
                    _save_write(db, request_key, 'freeze', write_request, retry_of, result)
                return result
            if custom_origin_exposure(db, set(reservation['issues'])):
                raise EvaluationError('自定义奖金来源可能涉及固定范围，请先复核')
            for item in checked:
                p = item['protocol']; c = p['candidate']
                if (p['snapshot_id'] != reservation['snapshot_id'] or p['targets'] != reservation['issues']
                        or p['replicas'] != first['replicas']
                        or any(c.get(key) != baseline.get(key) for key in
                               ('budget_fen', 'base_price_fen', 'additional_price_fen', 'additional', 'structure',
                                'front_count', 'back_count', 'max_tickets'))):
                    raise EvaluationError('各方法必须使用固定范围、相同副本及同预算和组合结构')
                _check_scope(db, reservation_id, p)
            scenarios = backtest_scenarios(self.path, first['snapshot_id'], first['targets'], additional=baseline['additional'])
            spec = {'version': 'fixed-descriptive-evaluation-v1', 'reservation_id': reservation_id,
                    'hypothesis': hypothesis.strip(), 'primary_metric': 'after_tax_net_paired_difference',
                    'aggregation': 'arithmetic_mean_v1', 'missing_policy': 'incomplete_period_unpublished',
                    'methods': checked, 'scenarios': scenarios, 'timeout': timeout,
                    'environment': _environment(), 'independence': 'not_verified',
                    'evidence': 'locally fixed historical reconstruction; not prospective or independently verified'}
            payload = _canonical(spec); digest = hashlib.sha256(payload).hexdigest()
            db.execute("INSERT INTO fixed_evaluations VALUES (?,?,?,?, 'pending',NULL,NULL,NULL,NULL,NULL)",
                       (reservation_id, digest, payload, _now()))
            result = self._get(db, reservation_id)
            if request_key is not None:
                _save_write(db, request_key, 'freeze', write_request, retry_of, result)
            return result

    def _get(self, db, reservation_id):
        cursor = db.cursor(); cursor.row_factory = sqlite3.Row
        row = cursor.execute('SELECT * FROM fixed_evaluations WHERE reservation_id=?', (reservation_id,)).fetchone()
        if not row:
            raise KeyError(reservation_id)
        value = dict(row); spec = _load(db, reservation_id)
        value.pop('protocol_json'); value['protocol'] = spec
        raw = value.pop('report_json')
        if raw is not None and hashlib.sha256(raw).hexdigest() != value['report_sha256']:
            raise EvaluationError('评估报告摘要不符')
        value['report'] = json.loads(raw) if raw is not None else None
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='fixed_evaluation_attempts'").fetchone():
            value['attempts'] = [dict(item) for item in cursor.execute(
                'SELECT request_key,attempt_no,retry_of,retry_requested,status,started_at,finished_at,error,report_sha256 '
                'FROM fixed_evaluation_attempts WHERE reservation_id=? ORDER BY attempt_no', (reservation_id,))]
        else:
            value['attempts'] = []
        _, successors = reservation_lineage(db)
        value['superseded_by'] = successors.get(reservation_id)
        value['environment_matches'] = spec['environment'] == _environment() if value['status'] == 'pending' else None
        value['can_supersede'] = (value['status'] == 'pending' and not value['attempts']
                                  and all(value[k] is None for k in ('started_at', 'finished_at', 'error', 'report_sha256', 'report'))
                                  and not value['superseded_by'] and not value['environment_matches'])
        value['validity'] = 'not_available'
        if value['status'] == 'complete':
            try:
                from dlt.backtest_repository import BacktestRepository
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='fixed_evaluation_corrections'").fetchone():
                    correction = db.execute('SELECT id,reservation_id,base_report_sha256,report_sha256,record_json '
                                            'FROM fixed_evaluation_corrections WHERE reservation_id=? ORDER BY rowid DESC LIMIT 1',
                                            (reservation_id,)).fetchone()
                    if correction:
                        original = value['report']
                        value['original_report'] = original
                        record = _checked_correction(db, spec, original, value['report_sha256'], correction)
                        value['report'] = record['report']
                        value['correction'] = {'id': correction[0], 'base_report_sha256': record['base_report_sha256'],
                                               'report_sha256': record['report_sha256'], 'created_at': record['created_at'],
                                               'workflow_code_sha256': record['workflow_code_sha256'],
                                               'calculation_code_sha256': record['calculation_code_sha256'],
                                               'settlement_code_sha256': record['settlement_code_sha256']}
                for item in value['report']['methods']:
                    BacktestRepository._check_dependencies(db, item['result'])
                protocol = spec['methods'][0]['protocol']
                scenarios = backtest_scenarios(self.path, protocol['snapshot_id'], protocol['targets'],
                                                additional=protocol['candidate']['additional'])
                value['validity'] = 'local_descriptive' if scenarios == spec['scenarios'] else 'review_required'
                if value['validity'] == 'review_required':
                    value['validity_reason'] = '奖金情景已变化，需要复核'
                if value['validity'] == 'local_descriptive':
                    value['paired_descriptions'] = {item['id']: describe_backtest(item['result'])
                                                    for item in value['report']['methods']}
            except (ValueError, TypeError, KeyError, sqlite3.Error) as exc:
                value['validity'] = 'review_required'
                value['validity_reason'] = str(exc)[:1000] or '报告或其依赖无法核验'
            reason = value.get('validity_reason', '')
            value['can_recalculate_amounts'] = (value['validity'] == 'review_required' and not value.get('original_report')
                and any(text in reason for text in ('精确计算不一致', 'settlement amounts are inconsistent',
                                                    'paired costs or difference mismatch', 'within-period mean mismatch',
                                                    'cross-period mean mismatch')))
        from dlt.custom_origin import custom_origin_exposure
        exposure = custom_origin_exposure(db, set(spec['methods'][0]['protocol']['targets']))
        value['custom_origin_exposure'] = exposure
        if exposure:
            value['can_supersede'] = False
            value['validity'] = 'review_required'
            value['validity_reason'] = '自定义奖金来源未明或本人声明涉及固定范围，历史结果需要复核'
            value['can_recalculate_amounts'] = False
            value.pop('paired_descriptions', None)
        return value

    def recalculate_amounts(self, reservation_id, *, request_key=None, retry_of=None):
        """Append a checked monetary correction while retaining the fixed report."""
        self.initialize()
        write_request = _write_request('recalculate_amounts', {'reservation_id': reservation_id})
        if request_key is not None:
            request_key = _write_key(request_key)
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            if request_key is not None:
                state = _write_state(db, request_key, 'recalculate_amounts', write_request, retry_of)
                if state['status'] == 'accepted':
                    return self._get(db, reservation_id)
                if state['status'] == 'fenced':
                    raise FixedEvaluationFenced(request_key, 'recalculate_amounts', state['request_sha256'], retry_of)
                _add_write_retry(db, request_key, 'recalculate_amounts', write_request, retry_of, _now())
            row = db.execute('SELECT status,protocol_sha256,report_sha256,report_json FROM fixed_evaluations '
                             'WHERE reservation_id=?', (reservation_id,)).fetchone()
            if not row or row[0] != 'complete' or row[3] is None or hashlib.sha256(row[3]).hexdigest() != row[2]:
                raise EvaluationError('已完成的原报告不存在或摘要不符，不能更正')
            spec = _load(db, reservation_id)
            original = json.loads(row[3])
            _check_original_attempt(db, reservation_id, row[2], original)
            protocol = spec['methods'][0]['protocol']
            actual = backtest_scenarios(self.path, protocol['snapshot_id'], protocol['targets'],
                                        additional=protocol['candidate']['additional'])
            if actual != spec['scenarios']:
                raise EvaluationError('奖金情景已变化，不能按原假设发布更正')
            derived = _derive_amount_report(db, spec, original)
            report_sha = _hash(derived)
            existing = db.execute('SELECT id,reservation_id,base_report_sha256,report_sha256,record_json '
                                  'FROM fixed_evaluation_corrections WHERE reservation_id=? AND base_report_sha256=? '
                                  'AND report_sha256=?', (reservation_id, row[2], report_sha)).fetchone()
            if existing:
                _checked_correction(db, spec, original, row[2], existing)
                current = self._get(db, reservation_id)
                if current['validity'] != 'local_descriptive' or current.get('correction', {}).get('id') != existing[0]:
                    raise EvaluationError('已有更正版本的当前状态需要复核，未发布新结果')
                if request_key is not None:
                    _save_write(db, request_key, 'recalculate_amounts', write_request, retry_of, current)
                return current
            if derived == original:
                raise EvaluationError('原报告金额与当前核算一致，无需更正')
            from dlt import backtest_repository, settlement
            record = {'version': 'fixed-amount-correction-v1', 'reservation_id': reservation_id,
                      'base_report_sha256': row[2], 'report_sha256': report_sha, 'report': derived, 'created_at': _now(),
                      'workflow_code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                      'calculation_code_sha256': hashlib.sha256(Path(backtest_repository.__file__).read_bytes()).hexdigest(),
                      'settlement_code_sha256': hashlib.sha256(Path(settlement.__file__).read_bytes()).hexdigest()}
            encoded = _canonical(record); ident = hashlib.sha256(encoded).hexdigest()
            db.execute('INSERT INTO fixed_evaluation_corrections VALUES (?,?,?,?,?)',
                       (ident, reservation_id, row[2], report_sha, encoded))
            current = self._get(db, reservation_id)
            if current['validity'] != 'local_descriptive' or current.get('correction', {}).get('id') != ident:
                raise EvaluationError('更正后的依赖核验未通过，未发布新结果')
            if request_key is not None:
                _save_write(db, request_key, 'recalculate_amounts', write_request, retry_of, current)
            return current

    def supersede_pending(self, reservation_id, *, reason, confirmed, request_key=None, retry_of=None):
        """Record an explicit successor only for a never-run, stale protocol."""
        write_request = _write_request('supersede', {'reservation_id': reservation_id,
                                   'reason': reason, 'confirmed': confirmed})
        reason = write_request['reason']
        if request_key is not None:
            request_key = _write_key(request_key)
        self.initialize()
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            if request_key is not None:
                state = _write_state(db, request_key, 'supersede', write_request, retry_of)
                if state['status'] == 'accepted':
                    return state['result']
                if state['status'] == 'fenced':
                    raise FixedEvaluationFenced(request_key, 'supersede', state['request_sha256'], retry_of)
                _add_write_retry(db, request_key, 'supersede', write_request, retry_of, _now())
            records, successors = reservation_lineage(db)
            if reservation_id not in records:
                raise EvaluationError('固定范围不存在')
            if reservation_id in successors:
                ident = successors[reservation_id]; content = records[ident]
                if content['supersession_reason'] != reason.strip():
                    raise EvaluationError('旧版本已有不同的新版本记录')
                row = db.execute('SELECT created_at FROM holdout_reservations WHERE id=?', (ident,)).fetchone()
                result = {'id': ident, 'content': content, 'created_at': row[0]}
                if request_key is not None:
                    _save_write(db, request_key, 'supersede', write_request, retry_of, result)
                return result
            old = self._get(db, reservation_id)
            if (old['status'] != 'pending' or old['attempts']
                    or any(old[k] is not None for k in ('started_at', 'finished_at', 'error', 'report_sha256', 'report'))):
                raise EvaluationError('仅从未运行的待执行协议可以创建新版本')
            if old['environment_matches']:
                raise EvaluationError('当前执行环境未变，无需创建新版本')
            original = records[reservation_id]
            for table in ('training_jobs', 'backtest_jobs'):
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() and db.execute(f"SELECT 1 FROM {table} WHERE status='running' LIMIT 1").fetchone():
                    raise EvaluationError('训练或回测仍在运行，请等待结束后重新核对')
            audit = audit_holdout(self.path, original['snapshot_id'], original['start_issue'], original['end_issue'])
            if audit['status'] != 'no_recorded_use' or audit['issues'] != original['issues']:
                raise EvaluationError('本机记录显示范围已使用或无法核对，不能作为未使用版本重新固定')
            prior_spec = old['protocol']
            # Do not allow results visible in a historical snapshot to influence
            # a new method selection under the guise of an environment upgrade.
            _validate_spec_methods(db, reservation_id, prior_spec)
            first = prior_spec['methods'][0]['protocol']
            actual = backtest_scenarios(self.path, first['snapshot_id'], first['targets'],
                                        additional=first['candidate']['additional'])
            if actual != prior_spec['scenarios']:
                raise EvaluationError('历史奖金情景已变化，不能创建保持原选择的新版本')
            content = {'version': 'local-exclusion-successor-v1', 'snapshot_id': original['snapshot_id'],
                       'start_issue': original['start_issue'], 'end_issue': original['end_issue'], 'issues': original['issues'],
                       'audit_status': audit['status'], 'audit_as_of_utc': audit['as_of_utc'],
                       'independence': 'not_verified', 'supersedes': reservation_id,
                       'supersedes_protocol_sha256': old['protocol_sha256'], 'supersession_reason': reason.strip()}
            payload = _canonical(content); ident = hashlib.sha256(payload).hexdigest(); now = _now()
            try:
                timestamps = [datetime.fromisoformat(x) for x in (old['created_at'], audit['as_of_utc'], now)]
                if any(t.tzinfo is None or t.utcoffset() is None for t in timestamps) or not timestamps[0] <= timestamps[1] <= timestamps[2]:
                    raise ValueError('clock order invalid')
            except (TypeError, ValueError):
                raise EvaluationError('本机时间顺序异常，不能创建关联版本') from None
            db.execute('INSERT INTO holdout_reservations VALUES (?,?,?)', (ident, payload, now))
            replacement = {**prior_spec, 'reservation_id': ident, 'environment': _environment(),
                           'predecessor_reservation_id': reservation_id,
                           'predecessor_protocol_sha256': old['protocol_sha256']}
            replacement_raw = _canonical(replacement)
            db.execute("INSERT INTO fixed_evaluations VALUES (?,?,?,?, 'pending',NULL,NULL,NULL,NULL,NULL)",
                       (ident, hashlib.sha256(replacement_raw).hexdigest(), replacement_raw, now))
            # Validate the entire graph under the same lock before committing.
            reservation_lineage(db)
            result = {'id': ident, 'content': content, 'created_at': now}
            if request_key is not None:
                _save_write(db, request_key, 'supersede', write_request, retry_of, result)
            return result

    def fence_write(self, request_key, operation, request, request_sha256, *, retry_of=None):
        request_key = _write_key(request_key)
        normalized = _write_request(operation, request)
        raw = _canonical(normalized); digest = hashlib.sha256(raw).hexdigest()
        if request_sha256 != digest:
            raise EvaluationError('固定评估请求摘要不匹配')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            state = _write_state(db, request_key, operation, normalized, retry_of)
            if state['status'] != 'unknown':
                return state
            now = _now()
            _add_write_retry(db, request_key, operation, normalized, retry_of, now)
            db.execute('INSERT INTO fixed_evaluation_write_fences VALUES (?,?,?,?,?,?)',
                       (request_key, operation, normalized['reservation_id'], digest, raw, now))
            return {'status': 'fenced', 'request_key': request_key, 'operation': operation,
                    'reservation_id': normalized['reservation_id'], 'request_sha256': digest,
                    'retry_of': retry_of}

    def write_status(self, request_key, operation, request_sha256):
        request_key = _write_key(request_key)
        if (operation not in ('freeze', 'start', 'supersede', 'recalculate_amounts')
                or not isinstance(request_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', request_sha256)):
            raise EvaluationError('固定评估状态查询参数无效')
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            present = [bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())
                       for table in ('fixed_evaluation_write_requests', 'fixed_evaluation_write_fences',
                                     'fixed_evaluation_write_retries')]
            if not any(present):
                return {'status': 'unknown', 'request_key': request_key, 'operation': operation,
                        'request_sha256': request_sha256}
            if not all(present):
                raise EvaluationError('固定评估写入日志结构不完整')
            saved = db.execute('SELECT operation,reservation_id,request_sha256,request_json,result_json '
                               'FROM fixed_evaluation_write_requests WHERE request_key=?', (request_key,)).fetchone()
            fence = db.execute('SELECT operation,reservation_id,request_sha256,request_json '
                               'FROM fixed_evaluation_write_fences WHERE request_key=?', (request_key,)).fetchone()
            retry = db.execute('SELECT retry_of FROM fixed_evaluation_write_retries WHERE request_key=?',
                               (request_key,)).fetchone()
            if not saved and not fence:
                if retry:
                    raise EvaluationError('固定评估关联重试缺少接受或封止记录')
                return {'status': 'unknown', 'request_key': request_key, 'operation': operation,
                        'request_sha256': request_sha256}
            if saved and fence:
                raise EvaluationError('固定评估请求同时存在接受与封止记录')
            row = saved or fence
            if row[0] != operation or row[2] != request_sha256:
                raise EvaluationError('固定评估请求类型或摘要不匹配')
            request = json.loads(row[3])
            normalized = _write_request(operation, request)
            if normalized != request or _canonical(request) != row[3]:
                raise EvaluationError('固定评估请求内容无法核验')
            state = _write_state(db, request_key, operation, request, retry[0] if retry else None)
            if state['status'] == 'accepted':
                if operation == 'supersede':
                    result = state.get('result')
                    records, successors = reservation_lineage(db)
                    successor_id = result.get('id') if isinstance(result, dict) else None
                    content = records.get(successor_id)
                    successor = (self._get(db, successor_id) if content is not None
                                 and db.execute('SELECT 1 FROM fixed_evaluations WHERE reservation_id=?',
                                                (successor_id,)).fetchone() else None)
                    row = db.execute('SELECT created_at FROM holdout_reservations WHERE id=?',
                                     (successor_id,)).fetchone() if content is not None else None
                    if (not isinstance(result, dict) or not content or not row or not successor
                            or result.get('content') != content or result.get('created_at') != row[0]
                            or content.get('supersedes') != state['reservation_id']
                            or successors.get(state['reservation_id']) != successor_id):
                        raise EvaluationError('固定评估关联版本回执与后继范围记录不一致')
                else:
                    result = state.get('result')
                    current = self._get(db, state['reservation_id'])
                    if (not isinstance(result, dict)
                            or result.get('reservation_id') != state['reservation_id']
                            or result.get('protocol') != current['protocol']):
                        raise EvaluationError('固定评估接受回执与当前固定协议不一致')
                    if operation == 'start':
                        attempt = next((item for item in current['attempts'] if item['request_key'] == request_key), None)
                        if not attempt:
                            raise EvaluationError('固定评估运行已接受，但关联运行记录缺失')
                        state['result'] = current
                        state['attempt'] = attempt
            return state

    def list(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='fixed_evaluations'").fetchone():
                return []
            keys = [row[0] for row in db.execute('SELECT reservation_id FROM fixed_evaluations ORDER BY created_at DESC,reservation_id')]
            return [self._get(db, key) for key in keys]

    def start(self, reservation_id, *, request_key=None, retry=False, retry_of=None):
        from dlt.recovery_guard import require_execution_allowed, require_path_execution_allowed
        require_path_execution_allowed(self.path)
        if request_key is None:
            request_key = uuid.uuid4().hex
        request_key = _write_key(request_key)
        write_request = _write_request('start', {'reservation_id': reservation_id, 'retry': retry})
        with closing(sqlite3.connect(self.path)) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='fixed_evaluations'").fetchone():
                raise KeyError(reservation_id)
        self.initialize()
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            if self.stopping.is_set():
                raise EvaluationError('本机服务正在停止，不能启动评估')
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE'); require_execution_allowed(db); current = self._get(db, reservation_id)
            write_state = _write_state(db, request_key, 'start', write_request, retry_of)
            if write_state['status'] == 'accepted':
                current['requested_attempt'] = next((x for x in current['attempts'] if x['request_key'] == request_key), None)
                if current['requested_attempt'] is None:
                    raise EvaluationError('已接受固定评估请求的运行记录缺失')
                return current
            if write_state['status'] == 'fenced':
                raise FixedEvaluationFenced(request_key, 'start', write_state['request_sha256'], retry_of)
            _add_write_retry(db, request_key, 'start', write_request, retry_of, _now())
            existing = db.execute('SELECT reservation_id,retry_requested FROM fixed_evaluation_attempts WHERE request_key=?', (request_key,)).fetchone()
            if existing:
                if existing != (reservation_id, int(retry)):
                    raise EvaluationError('请求标识已用于不同的评估或操作')
                current['requested_attempt'] = next(x for x in current['attempts'] if x['request_key'] == request_key)
                _save_write(db, request_key, 'start', write_request, retry_of, current)
                return current
            if current['custom_origin_exposure']:
                raise EvaluationError('自定义奖金来源未明或涉及固定范围，不能运行评估')
            if current['superseded_by']:
                raise EvaluationError('评估协议已被新版本替代，不能运行')
            if current['status'] not in ('pending', 'failed'):
                raise EvaluationError('评估已运行或完成；查询原任务状态，请勿新建运行')
            if (current['status'] == 'failed') != retry:
                raise EvaluationError('失败后必须显式重试，首次运行不能标为重试')
            if reservation_id in self.threads:
                raise EvaluationError('上次评估仍在清理，请稍后重试')
            prior = current['attempts'][-1] if current['attempts'] else None
            attempt_no = prior['attempt_no']+1 if prior else 0
            if attempt_no > 9:
                raise EvaluationError('重试次数已达上限')
            spec = current['protocol']
            if spec['environment'] != _environment():
                raise EvaluationError('执行环境与固定协议不一致，不能启动')
            _validate_spec_methods(db, reservation_id, spec)
            actual = backtest_scenarios(self.path, spec['methods'][0]['protocol']['snapshot_id'],
                                        spec['methods'][0]['protocol']['targets'],
                                        additional=spec['methods'][0]['protocol']['candidate']['additional'])
            if actual != spec['scenarios']:
                raise EvaluationError('历史奖金情景在固定后发生变化，不能启动')
            started = _now()
            db.execute("INSERT INTO fixed_evaluation_attempts VALUES (?,?,?,?,?,'running',?,NULL,NULL,NULL,NULL)",
                       (request_key, reservation_id, attempt_no, prior['request_key'] if prior else None, int(retry), started))
            db.execute("UPDATE fixed_evaluations SET status='running',started_at=?,finished_at=NULL,error=NULL WHERE reservation_id=?", (started, reservation_id))
            result = self._get(db, reservation_id)
            result['requested_attempt'] = next(x for x in result['attempts'] if x['request_key'] == request_key)
            _save_write(db, request_key, 'start', write_request, retry_of, result)
            thread = threading.Thread(target=self._execute, args=(reservation_id, spec, request_key), daemon=True)
            # Persist the accepted request before dispatching. If thread startup
            # fails, the receipt and attempt must remain visible for recovery.
            db.commit()
            self.threads[reservation_id] = thread
            try:
                thread.start()
            except Exception as exc:
                self.threads.pop(reservation_id, None)
                with closing(sqlite3.connect(self.path)) as failure_db, failure_db:
                    failure_db.execute('PRAGMA foreign_keys=ON')
                    failure_db.execute('BEGIN IMMEDIATE')
                    finished = _now()
                    error = '后台固定评估线程无法启动：' + str(exc)[-1000:]
                    changed = failure_db.execute(
                        "UPDATE fixed_evaluation_attempts SET status='failed',finished_at=?,error=? "
                        "WHERE request_key=? AND status='running'",
                        (finished, error, request_key)).rowcount
                    if changed:
                        failure_db.execute(
                            "UPDATE fixed_evaluations SET status='failed',finished_at=?,error=? "
                            "WHERE reservation_id=? AND status='running'",
                            (finished, error, reservation_id))
                result = self.get(reservation_id)
                result['requested_attempt'] = next(
                    x for x in result['attempts'] if x['request_key'] == request_key)
        return result

    def get(self, reservation_id):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            return self._get(db, reservation_id)

    def _execute(self, reservation_id, spec, attempt_id):
        try:
            process = subprocess.Popen([sys.executable, '-m', 'dlt.fixed_evaluation_worker'], stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                                       env={**os.environ, 'PYTHONIOENCODING': 'utf-8'})
            with self.lock:
                self.processes[reservation_id] = process
                if self.stopping.is_set():
                    process.kill()
            try:
                out, err = process.communicate(_canonical({'path': str(self.path), 'reservation_id': reservation_id, 'attempt_id': attempt_id}), timeout=spec['timeout'])
            except subprocess.TimeoutExpired:
                process.kill(); process.communicate(); raise EvaluationError('评估超过时间上限，未发布报告')
            if process.returncode:
                raise EvaluationError('评估进程未完成，未发布报告')
            report = json.loads(out)
            if (report.get('protocol_sha256') != _hash(spec) or report.get('reservation_id') != reservation_id
                    or [x['id'] for x in report.get('methods', [])] != [x['id'] for x in spec['methods']]):
                raise EvaluationError('评估报告与固定协议不符')
            for item, fixed in zip(report['methods'], spec['methods']):
                result = item['result']
                if (result.get('protocol') != fixed['protocol'] or result.get('scenarios') != spec['scenarios']
                        or result.get('protocol_sha256') != _hash(fixed['protocol'])
                        or result.get('scenarios_sha256') != _hash(spec['scenarios'])):
                    raise EvaluationError('方法报告与固定配置不符')
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('BEGIN IMMEDIATE'); current = self._get(db, reservation_id)
                if current['status'] != 'running':
                    return
                if current['custom_origin_exposure']:
                    raise EvaluationError('评估期间发现自定义奖金来源未明或涉及固定范围')
                for item in spec['methods']:
                    validate_execution(db, reservation_id, attempt_id, item['id'], item['protocol'], spec['scenarios'])
                actual = backtest_scenarios(self.path, spec['methods'][0]['protocol']['snapshot_id'],
                                            spec['methods'][0]['protocol']['targets'],
                                            additional=spec['methods'][0]['protocol']['candidate']['additional'])
                if actual != spec['scenarios']:
                    raise EvaluationError('评估期间历史奖金情景发生变化')
                payload = _canonical(report)
                finished = _now(); digest = hashlib.sha256(payload).hexdigest()
                changed = db.execute("UPDATE fixed_evaluation_attempts SET status='complete',finished_at=?,report_sha256=?,report_json=? WHERE request_key=? AND status='running'",
                                     (finished, digest, payload, attempt_id)).rowcount
                if changed != 1:
                    raise EvaluationError('评估执行权已失效')
                db.execute("UPDATE fixed_evaluations SET status='complete',finished_at=?,report_sha256=?,report_json=? WHERE reservation_id=?",
                           (finished, digest, payload, reservation_id))
        except Exception as exc:
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('BEGIN IMMEDIATE'); finished = _now(); error = str(exc)[-500:]
                changed = db.execute("UPDATE fixed_evaluation_attempts SET status='failed',finished_at=?,error=? WHERE request_key=? AND status='running'",
                                     (finished, error, attempt_id)).rowcount
                if changed:
                    db.execute("UPDATE fixed_evaluations SET status='failed',finished_at=?,error=? WHERE reservation_id=? AND status='running'",
                               (finished, error, reservation_id))
        finally:
            with self.lock:
                self.processes.pop(reservation_id, None); self.threads.pop(reservation_id, None)

    def recover_interrupted(self):
        from dlt.recovery_guard import require_execution_allowed, require_path_execution_allowed
        require_path_execution_allowed(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='fixed_evaluations'").fetchone():
                return
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE'); require_execution_allowed(db)
            finished = _now(); error = '服务中断，本次评估未发布报告；可按原协议显式重试'
            db.execute("UPDATE fixed_evaluation_attempts SET status='failed',finished_at=?,error=? WHERE status='running' "
                       "AND reservation_id IN (SELECT reservation_id FROM fixed_evaluations WHERE status='running')", (finished, error))
            db.execute("UPDATE fixed_evaluations SET status='failed',finished_at=?,error=? WHERE status='running'", (finished, error))

    def stop(self):
        with self.lock:
            self.stopping.set()
            for process in self.processes.values():
                if process.poll() is None:
                    process.kill()
            threads = list(self.threads.values())
        for thread in threads:
            thread.join(timeout=5)

