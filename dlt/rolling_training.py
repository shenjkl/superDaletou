"""Durable, explicitly enabled training plans for fixed rolling experiments.

This schedules training only. It neither generates nor registers portfolios.
Source observations and time remain personally retained local evidence.
"""
from contextlib import closing
from datetime import date, datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import platform
import re
import sqlite3
import threading

import numpy as np

from dlt.candidates import _canonical
from dlt.generation_replay import _read_history
from dlt.prospective import ProspectiveRepository, RegistrationError, _time
from dlt.prospective_series import _load
from dlt.training_jobs import JobError
from dlt.training_limits import DEFAULT_TRAINING_LIMITS


class RollingPlanError(ValueError):
    pass


class RollingPlanFenced(RollingPlanError):
    def __init__(self, request_key, operation, request, retry_of=None):
        super().__init__('该自动训练计划请求已封止，迟到请求不能执行')
        self.request_key, self.operation, self.request, self.retry_of = request_key, operation, request, retry_of


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


class RollingTraining:
    def __init__(self, path, jobs, *, clock=None):
        self.path = Path(path)
        self.jobs = jobs
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.lock = threading.RLock()

    def _now(self):
        now = self.clock()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise RollingPlanError('本机时间无效')
        return now.astimezone(timezone.utc)

    def initialize(self):
        ProspectiveRepository(self.path).initialize()
        self.jobs.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS rolling_training_plans (
                    id TEXT PRIMARY KEY, series_key TEXT NOT NULL REFERENCES experiment_series(series_key),
                    issue TEXT NOT NULL, request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL,
                    created_at TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL,
                    resolved_json BLOB, job_key TEXT, message TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(series_key,issue)
                );
                CREATE TABLE IF NOT EXISTS rolling_training_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, plan_id TEXT NOT NULL REFERENCES rolling_training_plans(id),
                    status TEXT NOT NULL, attempt INTEGER NOT NULL, job_key TEXT, message TEXT NOT NULL, at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS rolling_training_write_requests (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_json BLOB NOT NULL,
                    result_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS rolling_training_write_no_update BEFORE UPDATE ON rolling_training_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling training write'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_training_write_no_delete BEFORE DELETE ON rolling_training_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling training write'); END;
                CREATE TABLE IF NOT EXISTS rolling_training_write_fences (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS rolling_training_write_fence_no_update BEFORE UPDATE ON rolling_training_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling training write fence'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_training_write_fence_no_delete BEFORE DELETE ON rolling_training_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling training write fence'); END;
                CREATE TABLE IF NOT EXISTS rolling_training_write_retries (
                    request_key TEXT PRIMARY KEY, retry_of TEXT NOT NULL UNIQUE REFERENCES rolling_training_write_fences(request_key),
                    operation TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS rolling_training_write_retry_no_update BEFORE UPDATE ON rolling_training_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling training write retry'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_training_write_retry_no_delete BEFORE DELETE ON rolling_training_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling training write retry'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_plan_fixed BEFORE UPDATE OF id,series_key,issue,request_sha256,request_json,created_at ON rolling_training_plans
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling plan'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_plan_no_delete BEFORE DELETE ON rolling_training_plans
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling plan'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_events_no_update BEFORE UPDATE ON rolling_training_events
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling event'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_events_no_delete BEFORE DELETE ON rolling_training_events
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling event'); END;
            ''')

    @staticmethod
    def _event(db, ident, status, attempt, job_key, message, now):
        db.execute('INSERT INTO rolling_training_events(plan_id,status,attempt,job_key,message,at) VALUES (?,?,?,?,?,?)',
                   (ident, status, attempt, job_key, message, now.isoformat()))

    def create(self, *, series_key, issue, calendar_review_id, expected_cutoff_issue, start_at, timeout, confirmed,
               request_key=None, retry_of=None):
        if (not isinstance(series_key, str) or not 1 <= len(series_key) <= 128
                or any(not isinstance(x, str) or not re.fullmatch(r'[0-9]{5}', x) for x in (issue, expected_cutoff_issue))
                or issue == expected_cutoff_issue
                or not isinstance(calendar_review_id, str) or not re.fullmatch(r'[0-9a-f]{64}', calendar_review_id)
                or type(timeout) is not int
                or not DEFAULT_TRAINING_LIMITS.timeout_seconds[0] <= timeout <= DEFAULT_TRAINING_LIMITS.timeout_seconds[1]
                or confirmed is not True):
            raise RollingPlanError('请填写实验、目标期、日历、预期截止期和训练时限并确认')
        start = _time(start_at)
        request = {'series_key': series_key, 'issue': issue, 'calendar_review_id': calendar_review_id,
                   'expected_cutoff_issue': expected_cutoff_issue, 'start_at': start.isoformat(), 'timeout': timeout}
        ident = _digest({'version': 'rolling-training-plan-v1', 'series_key': series_key, 'issue': issue})
        if request_key is not None and (not isinstance(request_key, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I)):
            raise RollingPlanError('自动训练计划请求标识无效')
        self.initialize()
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            if request_key:
                saved = self._prior_write(db, request_key, 'create', request, retry_of)
                if saved:
                    return saved['plan']
            existing = db.execute('SELECT request_json FROM rolling_training_plans WHERE id=?', (ident,)).fetchone()
            if existing:
                if existing[0] != _canonical(request):
                    raise RollingPlanError('该实验和奖期已有不同的自动训练计划')
                result = self._get(db, ident)
                if request_key:
                    db.execute('INSERT INTO rolling_training_write_requests VALUES (?,?,?,?,?)',
                               (request_key, 'create', _canonical(request), _canonical({'plan': result}), self._now().isoformat()))
                return result
            series = _load(db, series_key)
            if series['manifest']['retraining_policy'] != 'rolling_retrain_v1' or issue not in series['manifest']['observation_issues']:
                raise RollingPlanError('目标期不属于逐期重训实验')
            if db.execute('SELECT 1 FROM series_closures WHERE series_key=?', (series_key,)).fetchone():
                raise RollingPlanError('实验版本已关闭')
            if db.execute('SELECT 1 FROM series_periods WHERE series_key=? AND issue=?', (series_key, issue)).fetchone():
                raise RollingPlanError('本期已登记，无需自动训练')
            calendar = ProspectiveRepository._get_review(db, calendar_review_id, 'calendar')
            if ProspectiveRepository._effective_review(db, calendar_review_id, 'calendar')[0] != calendar_review_id:
                raise RollingPlanError('日历资料已更正，请选择当前版本')
            extracted = calendar['extracted']; deadline = _time(extracted.get('cutoff_at'))
            if (extracted.get('issue') != issue or not extracted.get('version')
                    or deadline.astimezone(timezone(timedelta(hours=8))).date().isoformat() != extracted.get('draw_date')
                    or self._now() >= deadline or start >= deadline or _time(calendar['reviewed_at']) > self._now()
                    or db.execute('SELECT 1 FROM draw_versions WHERE issue=? LIMIT 1', (issue,)).fetchone()):
                raise RollingPlanError('日历、截止时间或目标期不符合自动训练条件')
            now = self._now()
            if now >= deadline:
                raise RollingPlanError('保存计划时已到截止时间')
            db.execute('INSERT INTO rolling_training_plans VALUES (?,?,?,?,?,?,?,0,NULL,NULL,?,?)',
                       (ident, series_key, issue, _digest(request), _canonical(request), now.isoformat(),
                        'waiting', '等待开始时间和预期历史数据', now.isoformat()))
            self._event(db, ident, 'waiting', 0, None, '计划已创建', now)
            result = self._get(db, ident)
            if request_key:
                db.execute('INSERT INTO rolling_training_write_requests VALUES (?,?,?,?,?)',
                           (request_key, 'create', _canonical(request), _canonical({'plan': result}), now.isoformat()))
            return result

    def write_status(self, request_key):
        if not isinstance(request_key, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I):
            raise RollingPlanError('自动训练计划请求标识无效')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT operation,request_json,result_json FROM rolling_training_write_requests WHERE request_key=?',
                             (request_key,)).fetchone()
            retry = db.execute('SELECT retry_of,operation,request_json FROM rolling_training_write_retries WHERE request_key=?',
                               (request_key,)).fetchone()
            fence = db.execute('SELECT operation,request_json FROM rolling_training_write_fences WHERE request_key=?',
                               (request_key,)).fetchone()
            if row and fence:
                raise RollingPlanError('自动训练计划请求同时存在接受与封止记录')
            if retry and not (row or fence):
                raise RollingPlanError('自动训练计划关联重试缺少接受或封止记录')
            if row:
                operation, raw, result_raw = row
                try:
                    request, result = json.loads(raw), json.loads(result_raw)
                except (TypeError, ValueError, UnicodeDecodeError) as exc:
                    raise RollingPlanError('自动训练计划接受记录无法解析') from exc
                plan = result.get('plan') if isinstance(result, dict) else None
                if (not isinstance(raw, bytes) or not isinstance(result_raw, bytes)
                        or operation not in ('create', 'cancel', 'retry')
                        or not isinstance(request, dict) or not isinstance(plan, dict)
                        or _canonical(request) != raw or _canonical(result) != result_raw):
                    raise RollingPlanError('自动训练计划接受记录格式或摘要不一致')
                if operation == 'create':
                    expected_id = hashlib.sha256(_canonical({'version': 'rolling-training-plan-v1',
                        'series_key': request.get('series_key'), 'issue': request.get('issue')})).hexdigest()
                    stored_plan = db.execute('SELECT request_sha256,request_json FROM rolling_training_plans WHERE id=?',
                                             (plan.get('id'),)).fetchone()
                    if (plan.get('id') != expected_id or plan.get('request') != request or not stored_plan
                            or stored_plan != (hashlib.sha256(raw).hexdigest(), raw)):
                        raise RollingPlanError('自动训练计划接受结果与计划记录不一致')
                elif (set(request) != {'id'} or request['id'] != plan.get('id')
                      or not db.execute('SELECT 1 FROM rolling_training_plans WHERE id=?',
                                        (request['id'],)).fetchone()):
                    raise RollingPlanError('自动训练计划操作结果与计划记录不一致')
                retry_of = retry[0] if retry else None
                if retry and (retry[1:] != (operation, raw)
                              or db.execute('SELECT operation,request_json FROM rolling_training_write_fences '
                                            'WHERE request_key=?', (retry_of,)).fetchone() != (operation, raw)):
                    raise RollingPlanError('自动训练计划接受记录的关联重试无法核验')
                return {'request_key': request_key, 'status': 'accepted', 'operation': operation,
                        'request': request, 'result': result, 'retry_of': retry_of}
            if fence:
                operation, raw = fence
                try:
                    request = json.loads(raw)
                except (TypeError, ValueError, UnicodeDecodeError) as exc:
                    raise RollingPlanError('自动训练计划封止记录无法解析') from exc
                if (not isinstance(raw, bytes) or operation not in ('create', 'cancel', 'retry')
                        or not isinstance(request, dict) or _canonical(request) != raw):
                    raise RollingPlanError('自动训练计划封止记录格式或摘要不一致')
                retry_of = retry[0] if retry else None
                if retry and (retry[1:] != (operation, raw)
                              or db.execute('SELECT operation,request_json FROM rolling_training_write_fences '
                                            'WHERE request_key=?', (retry_of,)).fetchone() != (operation, raw)):
                    raise RollingPlanError('自动训练计划封止记录的关联重试无法核验')
                return {'request_key': request_key, 'status': 'fenced', 'operation': operation,
                        'request': request, 'retry_of': retry_of}
            return {'request_key': request_key, 'status': 'unknown'}

    @staticmethod
    def _prior_write(db, request_key, operation, request, retry_of=None):
        if not request_key:
            return None
        if not isinstance(request_key, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I):
            raise RollingPlanError('自动训练计划请求标识无效')
        row = db.execute('SELECT operation,request_json,result_json FROM rolling_training_write_requests WHERE request_key=?',
                         (request_key,)).fetchone()
        fence = db.execute('SELECT operation,request_json FROM rolling_training_write_fences WHERE request_key=?',
                           (request_key,)).fetchone()
        if fence:
            if fence != (operation, _canonical(request)):
                raise RollingPlanError('自动训练计划请求标识已用于不同封止操作或内容')
            retry = db.execute('SELECT retry_of,operation,request_json FROM rolling_training_write_retries WHERE request_key=?',
                               (request_key,)).fetchone()
            if (retry[0] if retry else None) != retry_of:
                raise RollingPlanError('自动训练计划请求关联重试标识不匹配')
            raise RollingPlanFenced(request_key, operation, request, retry_of)
        if not row:
            if retry_of is not None:
                RollingTraining._claim_write_retry(db, request_key, operation, request, retry_of)
            return None
        if row[0] != operation or row[1] != _canonical(request):
            raise RollingPlanError('自动训练计划请求标识已用于不同操作或内容')
        retry = db.execute('SELECT retry_of,operation,request_json FROM rolling_training_write_retries WHERE request_key=?',
                           (request_key,)).fetchone()
        if (retry[0] if retry else None) != retry_of:
            raise RollingPlanError('自动训练计划请求关联重试标识不匹配')
        return json.loads(row[2])

    @staticmethod
    def _claim_write_retry(db, request_key, operation, request, retry_of):
        original = db.execute('SELECT operation,request_json FROM rolling_training_write_fences WHERE request_key=?',
                              (retry_of,)).fetchone()
        if not original or original[0] != operation:
            raise RollingPlanError('关联重试必须指向同类且已封止的请求')
        original_request = json.loads(original[1])
        if original_request != request:
            raise RollingPlanError('关联重试必须使用被封止请求的相同内容')
        try:
            db.execute('INSERT INTO rolling_training_write_retries VALUES (?,?,?,?,?)',
                       (request_key, retry_of, operation, _canonical(request), datetime.now(timezone.utc).isoformat()))
        except sqlite3.IntegrityError as exc:
            raise RollingPlanError('原请求已有关联重试或该请求键已被使用') from exc

    def fence_write(self, request_key, operation, request, *, retry_of=None):
        if operation not in ('create', 'cancel', 'retry') or not isinstance(request, dict):
            raise RollingPlanError('自动训练计划封止请求无效')
        if not isinstance(request_key, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I):
            raise RollingPlanError('自动训练计划请求标识无效')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            accepted = db.execute('SELECT operation,request_json,result_json FROM rolling_training_write_requests WHERE request_key=?',
                                  (request_key,)).fetchone()
            if accepted:
                if accepted[0] != operation or accepted[1] != _canonical(request):
                    raise RollingPlanError('自动训练计划请求标识已用于不同操作或内容')
                return {'request_key': request_key, 'status': 'accepted', 'operation': operation,
                        'request': json.loads(accepted[1]), 'result': json.loads(accepted[2]), 'retry_of': retry_of}
            prior = db.execute('SELECT operation,request_json FROM rolling_training_write_fences WHERE request_key=?',
                               (request_key,)).fetchone()
            if prior:
                if prior != (operation, _canonical(request)):
                    raise RollingPlanError('自动训练计划请求标识已用于不同封止操作或内容')
                return {'request_key': request_key, 'status': 'fenced', 'operation': operation,
                        'request': request, 'retry_of': retry_of}
            retry = db.execute('SELECT retry_of,operation,request_json FROM rolling_training_write_retries WHERE request_key=?',
                               (request_key,)).fetchone()
            if (retry[0] if retry else None) != retry_of:
                raise RollingPlanError('自动训练计划请求关联重试标识不匹配')
            if retry_of is not None and not retry:
                self._claim_write_retry(db, request_key, operation, request, retry_of)
            db.execute('INSERT INTO rolling_training_write_fences VALUES (?,?,?,?)',
                       (request_key, operation, _canonical(request), self._now().isoformat()))
            return {'request_key': request_key, 'status': 'fenced', 'operation': operation,
                    'request': request, 'retry_of': retry_of}

    def _save_write(self, db, request_key, operation, request, result):
        if request_key:
            db.execute('INSERT INTO rolling_training_write_requests VALUES (?,?,?,?,?)',
                       (request_key, operation, _canonical(request), _canonical({'plan': result}), self._now().isoformat()))

    @staticmethod
    def _get(db, ident):
        cursor = db.cursor()
        cursor.row_factory = sqlite3.Row
        row = cursor.execute('SELECT * FROM rolling_training_plans WHERE id=?', (ident,)).fetchone()
        if not row:
            raise KeyError(ident)
        value = dict(row); request = json.loads(value.pop('request_json'))
        if _digest(request) != value['request_sha256']:
            raise RollingPlanError('自动训练计划摘要不符')
        value['request'] = request
        resolved_raw = value.pop('resolved_json')
        value['resolved'] = json.loads(resolved_raw) if resolved_raw else None
        value['events'] = [dict(x) for x in cursor.execute('SELECT status,attempt,job_key,message,at FROM rolling_training_events WHERE plan_id=? ORDER BY seq', (ident,))]
        return value

    def list(self):
        with closing(sqlite3.connect(self.path)) as db:
            keys = [r[0] for r in db.execute('SELECT id FROM rolling_training_plans ORDER BY created_at DESC,id')]
            return [self._get(db, key) for key in keys]

    def _resolve(self, db, plan, now):
        request = plan['request']; series = _load(db, request['series_key'])
        issue = request['issue']; spec = series['rolling_spec']
        if db.execute('SELECT 1 FROM series_closures WHERE series_key=?', (request['series_key'],)).fetchone():
            return 'blocked', '实验版本已关闭', None
        if db.execute('SELECT 1 FROM series_periods WHERE series_key=? AND issue=?', (request['series_key'], issue)).fetchone():
            return 'blocked', '本期已登记', None
        calendar_id, calendar_review, _ = ProspectiveRepository._effective_review(db, request['calendar_review_id'], 'calendar')
        if calendar_id != request['calendar_review_id']:
            return 'blocked', '日历资料已更正，请重新核对计划', None
        calendar = calendar_review['extracted']
        deadline = _time(calendar['cutoff_at'])
        if now >= deadline or db.execute('SELECT 1 FROM draw_versions WHERE issue=? LIMIT 1', (issue,)).fetchone():
            return 'blocked', '目标期已开奖或已到截止时间', None
        if now < _time(request['start_at']):
            return 'waiting', '等待开始时间', None
        model_hash = hashlib.sha256(Path(__file__).with_name('models.py').read_bytes()).hexdigest()
        if spec['code_sha256'] != model_hash or spec['runtime'] != {'python': platform.python_version(), 'numpy': np.__version__}:
            return 'blocked', '模型代码或运行环境与固定协议不一致', None
        target_day = date.fromisoformat(calendar['draw_date'])
        observed = db.execute('SELECT d.snapshot_id,d.issue,d.draw_date,o.observed_at FROM draw_versions d '
                              'JOIN observations o ON o.snapshot_id=d.snapshot_id').fetchall()
        available = [(sid, number, day, _time(at)) for sid, number, day, at in observed if _time(at) <= now]
        grouped = {}
        for _, number, day, _ in available:
            if date.fromisoformat(day) < target_day:
                grouped.setdefault(day, set()).add(number)
        n = spec['training_periods']; days = sorted(grouped)[-n:]
        if len(days) < n:
            return 'waiting', '等待足够的历史开奖数据', None
        if any(len(grouped[day]) != 1 for day in days):
            return 'blocked', '历史数据存在同日奖期顺序歧义', None
        wanted = [(next(iter(grouped[day])), day) for day in days]
        expected = request['expected_cutoff_issue']
        if wanted[-1][0] != expected:
            known = any(number == expected for _, number, _, _ in available)
            return ('blocked', '已出现不同的更新历史期，请核对计划', None) if known else ('waiting', '等待预期截止期的数据', None)
        snapshots = {}
        for sid, _, _, at in available:
            snapshots[sid] = max(snapshots.get(sid, at), at)
        for sid in sorted(snapshots, key=lambda item: (snapshots[item], item), reverse=True):
            try:
                rows = _read_history(db, sid, wanted[0][0], wanted[-1][0])
            except (ValueError, KeyError, sqlite3.Error):
                continue
            if [(r['issue'], r['date']) for r in rows] != wanted:
                continue
            config = {**series['rolling_spec']['config_profile'], 'snapshot_id': sid,
                      'cutoff': wanted[-1][0], 'train_end': wanted[-1-spec['validation_periods']][0],
                      'rolling_issue': issue}
            return 'ready', '数据满足固定训练规则', {'config': config, 'first_issue': wanted[0][0],
                                                       'history_sha256': _digest(rows), 'deadline': deadline.isoformat()}
        return 'waiting', '等待一份覆盖完整训练窗口且无冲突的快照', None

    def _transition(self, db, plan, status, message, now, *, resolved=None, job_key=None):
        db.execute('UPDATE rolling_training_plans SET status=?,message=?,resolved_json=?,job_key=?,updated_at=? WHERE id=?',
                   (status, message, _canonical(resolved) if resolved is not None else None, job_key, now.isoformat(), plan['id']))
        self._event(db, plan['id'], status, plan['attempt'], job_key, message, now)

    def _completed_window_valid(self, db, plan, job):
        """Recheck locally observed inputs as of completion, not as of polling."""
        resolved = plan['resolved']; config = resolved['config']
        request = plan['request']; spec = _load(db, request['series_key'])['rolling_spec']
        calendar_id, calendar_review, _ = ProspectiveRepository._effective_review(db, request['calendar_review_id'], 'calendar')
        if calendar_id != request['calendar_review_id']:
            return False
        calendar = calendar_review['extracted']
        started, finished = _time(job['created_at']), _time(job['finished_at'])
        if (started > finished or job['request']['config'] != config
                or job['request']['first_issue'] != resolved['first_issue']):
            return False
        rows = _read_history(db, config['snapshot_id'], resolved['first_issue'], config['cutoff'])
        if _digest(rows) != resolved['history_sha256'] or len(rows) != spec['training_periods']:
            return False
        observed = db.execute('SELECT d.snapshot_id,d.issue,d.draw_date,o.observed_at FROM draw_versions d '
                              'JOIN observations o ON o.snapshot_id=d.snapshot_id').fetchall()
        observed = [(sid, issue, day, _time(at)) for sid, issue, day, at in observed]
        target_day = date.fromisoformat(calendar['draw_date'])
        wanted = [(row['issue'], row['date']) for row in rows]
        for instant in (started, finished):
            if not any(sid == config['snapshot_id'] and at <= instant for sid, _, _, at in observed):
                return False
            if any(issue == request['issue'] and at <= instant for _, issue, _, at in observed):
                return False
            grouped = {}
            for _, issue, day, at in observed:
                if at <= instant and date.fromisoformat(day) < target_day:
                    grouped.setdefault(day, set()).add(issue)
            days = sorted(grouped)[-spec['training_periods']:]
            if (len(days) != spec['training_periods'] or any(len(grouped[day]) != 1 for day in days)
                    or [(next(iter(grouped[day])), day) for day in days] != wanted):
                return False
        return True

    def tick(self):
        """Advance each plan once; a deterministic job key avoids duplicate launches."""
        from dlt.recovery_guard import require_path_execution_allowed
        require_path_execution_allowed(self.path)
        with self.lock:
            for item in self.list():
                if item['status'] not in ('waiting', 'dispatching', 'running'):
                    continue
                try:
                    self._advance(item['id'])
                except Exception:
                    # Unexpected errors do not mark a plan complete or expose user data.
                    with closing(sqlite3.connect(self.path)) as db, db:
                        db.execute('BEGIN IMMEDIATE'); current = self._get(db, item['id'])
                        if current['status'] in ('waiting', 'dispatching', 'running'):
                            self._transition(db, current, 'failed', '后台检查异常，请查看任务并处理后重试', self._now(),
                                             resolved=current['resolved'], job_key=current['job_key'])

    def _advance(self, ident):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE'); plan = self._get(db, ident); now = self._now()
            if plan['status'] == 'waiting':
                status, message, resolved = self._resolve(db, plan, now)
                if status == 'waiting':
                    if message != plan['message']:
                        self._transition(db, plan, 'waiting', message, now)
                    return
                if status == 'blocked':
                    self._transition(db, plan, status, message, now)
                    return
                job_key = 'rolling-'+ident+'-'+str(plan['attempt'])
                self._transition(db, plan, 'dispatching', '已固定本次训练输入，等待提交', now, resolved=resolved, job_key=job_key)
                plan = self._get(db, ident)
            if plan['status'] == 'dispatching':
                status, message, current = self._resolve(db, plan, now)
                if status != 'ready' or current != plan['resolved']:
                    self._transition(db, plan, 'blocked', '训练输入或资格已变化，请重新核对', now,
                                     resolved=plan['resolved'], job_key=plan['job_key'])
                    return
        if plan['status'] == 'dispatching':
            resolved = plan['resolved']; request = plan['request']
            def guard(db):
                current = self._get(db, ident)
                status, _, fresh = self._resolve(db, current, self._now())
                if current['status'] != 'dispatching' or status != 'ready' or fresh != resolved:
                    raise JobError('自动训练输入或资格在提交前发生变化')
            try:
                job = self.jobs.submit(plan['job_key'], name='滚动训练 '+request['issue']+' / 第 '+str(plan['attempt']+1)+' 次', config=resolved['config'],
                                       first_issue=resolved['first_issue'], timeout=request['timeout'], transaction_guard=guard)
            except JobError as exc:
                if '已有训练任务运行' in str(exc):
                    return
                raise
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('BEGIN IMMEDIATE'); current = self._get(db, ident)
                if current['status'] == 'dispatching':
                    self._transition(db, current, 'running', '训练任务已提交', self._now(), resolved=resolved, job_key=plan['job_key'])
            return
        if plan['status'] == 'running':
            try:
                job = self.jobs.get(plan['job_key'])
            except KeyError:
                raise RollingPlanError('训练任务记录缺失')
            if job['status'] == 'running':
                return
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('BEGIN IMMEDIATE'); current = self._get(db, ident)
                if current['status'] != 'running':
                    return
                if job['status'] == 'complete' and _time(job['finished_at']) < _time(plan['resolved']['deadline']):
                    calendar_id, _, _ = ProspectiveRepository._effective_review(db, current['request']['calendar_review_id'], 'calendar')
                    if calendar_id != current['request']['calendar_review_id']:
                        self._transition(db, current, 'blocked', '日历资料已更正，请重新核对计划', self._now(),
                                         resolved=plan['resolved'], job_key=plan['job_key'])
                        return
                    valid = self._completed_window_valid(db, current, job)
                    self._transition(db, current, 'complete' if valid else 'blocked',
                                     '训练完成；生成配对和登记仍须另行完成' if valid else '训练期间历史输入发生变化，请核对后重试',
                                     self._now(), resolved=plan['resolved'], job_key=plan['job_key'])
                else:
                    self._transition(db, current, 'failed', '训练失败、取消或超过截止时间', self._now(),
                                     resolved=plan['resolved'], job_key=plan['job_key'])

    def cancel(self, ident, *, request_key=None, retry_of=None):
        request = {'id': ident}
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            prior = self._prior_write(db, request_key, 'cancel', request, retry_of)
            if prior:
                plan = prior['plan']
            else:
                plan = self._get(db, ident)
                if plan['status'] == 'complete':
                    self._save_write(db, request_key, 'cancel', request, plan)
                    return plan
                if plan['status'] != 'cancelled':
                    self._transition(db, plan, 'cancelled', '计划已取消', self._now(), resolved=plan['resolved'], job_key=plan['job_key'])
                    plan = self._get(db, ident)
                self._save_write(db, request_key, 'cancel', request, plan)
        if plan['job_key']:
            try:
                self.jobs.cancel(plan['job_key'], 'rolling-cancel-'+str(plan['id']))
            except KeyError:
                pass
        with closing(sqlite3.connect(self.path)) as db:
            return self._get(db, ident)

    def retry(self, ident, *, request_key=None, retry_of=None):
        """Retry the same immutable plan with a new, auditable job attempt."""
        request = {'id': ident}
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            prior = self._prior_write(db, request_key, 'retry', request, retry_of)
            if prior:
                return prior['plan']
            plan = self._get(db, ident); now = self._now()
            if plan['status'] not in ('failed', 'blocked'):
                raise RollingPlanError('只有失败或阻断的计划可以重试')
            status, message, _ = self._resolve(db, plan, now)
            if status == 'blocked':
                raise RollingPlanError(message)
            if plan['attempt'] >= 9:
                raise RollingPlanError('重试次数已达上限')
            attempt = plan['attempt']+1
            db.execute("UPDATE rolling_training_plans SET status='waiting',attempt=?,resolved_json=NULL,job_key=NULL,"
                       "message=?,updated_at=? WHERE id=?", (attempt, '等待重新检查', now.isoformat(), ident))
            self._event(db, ident, 'waiting', attempt, None, '用户请求重试', now)
            result = self._get(db, ident)
            self._save_write(db, request_key, 'retry', request, result)
            return result
