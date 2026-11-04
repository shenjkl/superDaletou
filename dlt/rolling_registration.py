"""Durable paper registration following an explicitly enabled rolling plan.

No source authenticity or trusted time is asserted.  Drafts are deterministic
and resumable; publication and plan completion share one SQLite transaction.
"""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from dlt.candidates import _canonical
from dlt.pair_repository import PairRepository
from dlt.prospective import ProspectiveRepository, RegistrationError, _time
from dlt.prospective_series import SHARED_FIELDS, _load


class RollingRegistrationError(ValueError):
    pass


class RollingRegistrationFenced(RollingRegistrationError):
    def __init__(self, request_key, operation, request, retry_of=None):
        super().__init__('该自动登记计划请求已封止，迟到请求不能执行')
        self.request_key, self.operation, self.request, self.retry_of = request_key, operation, request, retry_of


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


class RollingRegistration:
    def __init__(self, path, training, *, clock=None):
        self.path = Path(path)
        self.training = training
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.repo = ProspectiveRepository(path, clock=self.clock)
        self.pairs = PairRepository(path)

    def _now(self):
        value = self.clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise RollingRegistrationError('本机时间无效')
        return value.astimezone(timezone.utc)

    def initialize(self):
        self.training.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS rolling_registration_plans (
                    id TEXT PRIMARY KEY, training_plan_id TEXT NOT NULL REFERENCES rolling_training_plans(id),
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL,
                    status TEXT NOT NULL, message TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS rolling_registration_one_active ON rolling_registration_plans(training_plan_id)
                    WHERE status IN ('waiting','complete');
                CREATE TABLE IF NOT EXISTS rolling_registration_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id TEXT NOT NULL REFERENCES rolling_registration_plans(id),
                    status TEXT NOT NULL, message TEXT NOT NULL, at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS rolling_registration_write_requests (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_json BLOB NOT NULL,
                    result_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS rolling_registration_write_no_update BEFORE UPDATE ON rolling_registration_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling registration write'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_registration_write_no_delete BEFORE DELETE ON rolling_registration_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling registration write'); END;
                CREATE TABLE IF NOT EXISTS rolling_registration_write_fences (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS rolling_registration_fence_no_update BEFORE UPDATE ON rolling_registration_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling registration fence'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_registration_fence_no_delete BEFORE DELETE ON rolling_registration_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling registration fence'); END;
                CREATE TABLE IF NOT EXISTS rolling_registration_write_retries (
                    request_key TEXT PRIMARY KEY, retry_of TEXT NOT NULL UNIQUE REFERENCES rolling_registration_write_fences(request_key),
                    operation TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS rolling_registration_retry_no_update BEFORE UPDATE ON rolling_registration_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling registration retry'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_registration_retry_no_delete BEFORE DELETE ON rolling_registration_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable rolling registration retry'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_registration_fixed BEFORE UPDATE OF id,training_plan_id,request_sha256,request_json,created_at ON rolling_registration_plans
                    BEGIN SELECT RAISE(ABORT, 'immutable registration plan'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_registration_no_delete BEFORE DELETE ON rolling_registration_plans
                    BEGIN SELECT RAISE(ABORT, 'immutable registration plan'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_registration_events_no_update BEFORE UPDATE ON rolling_registration_events
                    BEGIN SELECT RAISE(ABORT, 'immutable registration event'); END;
                CREATE TRIGGER IF NOT EXISTS rolling_registration_events_no_delete BEFORE DELETE ON rolling_registration_events
                    BEGIN SELECT RAISE(ABORT, 'immutable registration event'); END;
            ''')

    @staticmethod
    def _get(db, ident):
        cur = db.cursor(); cur.row_factory = sqlite3.Row
        row = cur.execute('SELECT * FROM rolling_registration_plans WHERE id=?', (ident,)).fetchone()
        if row is None:
            raise KeyError(ident)
        item = dict(row)
        request = json.loads(item.pop('request_json'))
        if _digest(request) != item['request_sha256'] or request['training_plan_id'] != item['training_plan_id']:
            raise RollingRegistrationError('自动登记计划摘要不符')
        item['request'] = request
        item['events'] = [dict(x) for x in cur.execute(
            'SELECT status,message,at FROM rolling_registration_events WHERE plan_id=? ORDER BY seq', (ident,))]
        return item

    @staticmethod
    def _transition(db, ident, status, message, now):
        at = now.isoformat()
        db.execute('UPDATE rolling_registration_plans SET status=?,message=?,updated_at=? WHERE id=?',
                   (status, message, at, ident))
        db.execute('INSERT INTO rolling_registration_events(plan_id,status,message,at) VALUES (?,?,?,?)',
                   (ident, status, message, at))

    def list(self):
        with closing(sqlite3.connect(self.path)) as db:
            return [self._get(db, row[0]) for row in db.execute(
                'SELECT id FROM rolling_registration_plans ORDER BY created_at DESC,id').fetchall()]

    def _check_sources(self, db, plan, now):
        request = plan['request']
        training = self.training._get(db, request['training_plan_id'])
        series = _load(db, training['series_key'])
        issue = training['issue']; tr = training['request']
        if db.execute('SELECT 1 FROM series_closures WHERE series_key=?', (training['series_key'],)).fetchone():
            raise RollingRegistrationError('实验版本已关闭')
        if db.execute('SELECT 1 FROM series_periods WHERE series_key=? AND issue=?', (training['series_key'], issue)).fetchone():
            raise RollingRegistrationError('该奖期已有固定实验')
        if db.execute('SELECT 1 FROM draw_versions WHERE issue=?', (issue,)).fetchone():
            raise RollingRegistrationError('目标期已出现，不能补造前瞻登记')
        refs = {'calendar': tr['calendar_review_id'], 'rule': request['rule_review_id'], 'tax': request['tax_review_id']}
        records = {}
        for kind, ident in refs.items():
            current, record, _ = self.repo._effective_review(db, ident, kind, issue=issue)
            if current != ident or _time(record['reviewed_at']) > now:
                raise RollingRegistrationError('资料已更正或核对时间无效')
            records[kind] = record
        cal, rule, tax = (records[k]['extracted'] for k in ('calendar', 'rule', 'tax'))
        internal, official = _time(request['internal_deadline']), _time(cal['cutoff_at'])
        if (cal.get('issue') != issue or not cal.get('draw_date') or not cal.get('version')
                or not rule.get('version') or rule.get('source') != records['rule']['source_url']
                or not isinstance(rule.get('applicable_issues'), list) or issue not in rule['applicable_issues']
                or not isinstance(tax.get('applicable_issues'), list) or issue not in tax['applicable_issues']
                or tax.get('tax') != rule.get('tax') or internal > official or now >= internal or now >= official):
            raise RollingRegistrationError('资料、适用期或登记截止不符合条件')
        return training, series, cal, rule

    def create(self, *, training_plan_id, rule_review_id, tax_review_id, internal_deadline, confirmed,
               request_key=None, retry_of=None):
        ids = (training_plan_id, rule_review_id, tax_review_id)
        if confirmed is not True or any(not isinstance(x, str) or not re.fullmatch(r'[0-9a-f]{64}', x) for x in ids):
            raise RollingRegistrationError('请核对训练计划、规则、税费与截止后确认')
        request = {'training_plan_id': training_plan_id, 'rule_review_id': rule_review_id,
                   'tax_review_id': tax_review_id, 'internal_deadline': _time(internal_deadline).isoformat()}
        ident = _digest({'version': 'rolling-registration-v1', 'request': request})
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            if request_key is not None:
                if not isinstance(request_key, str) or not re.fullmatch(
                        r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I):
                    raise RollingRegistrationError('自动登记计划请求标识无效')
                saved = self._prior_write(db, request_key, 'registration_create', request, retry_of)
                if saved: return saved
            old = db.execute('SELECT request_json FROM rolling_registration_plans WHERE id=?', (ident,)).fetchone()
            if old:
                if old[0] != _canonical(request):
                    raise RollingRegistrationError('该训练计划已有不同的自动登记计划')
                result = self._get(db, ident)
                if request_key: db.execute('INSERT INTO rolling_registration_write_requests VALUES (?,?,?,?,?)',
                    (request_key, 'registration_create', _canonical(request), _canonical({'plan': result}), self._now().isoformat()))
                return result
            if db.execute("SELECT 1 FROM rolling_registration_plans WHERE training_plan_id=? AND status IN ('waiting','complete')", (training_plan_id,)).fetchone():
                raise RollingRegistrationError('该训练计划已有正在执行或已完成的自动登记计划')
            now = self._now()
            plan = {'request': request}
            training, series, _, _ = self._check_sources(db, plan, now)
            if training['status'] not in ('waiting', 'dispatching', 'running', 'complete'):
                raise RollingRegistrationError('训练计划未处于可执行状态')
            if series['manifest']['retraining_policy'] != 'rolling_retrain_v1':
                raise RollingRegistrationError('仅逐期重训实验支持此计划')
            db.execute('INSERT INTO rolling_registration_plans VALUES (?,?,?,?,?,?,?,?)',
                       (ident, training_plan_id, _digest(request), _canonical(request), now.isoformat(),
                        'waiting', '等待训练完成', now.isoformat()))
            db.execute('INSERT INTO rolling_registration_events(plan_id,status,message,at) VALUES (?,?,?,?)',
                       (ident, 'waiting', '计划已确认', now.isoformat()))
            result = self._get(db, ident)
            if request_key: db.execute('INSERT INTO rolling_registration_write_requests VALUES (?,?,?,?,?)',
                (request_key, 'registration_create', _canonical(request), _canonical({'plan': result}), now.isoformat()))
            return result

    def write_status(self, request_key):
        self._check_write_key(request_key)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT operation,request_json,result_json FROM rolling_registration_write_requests WHERE request_key=?',
                             (request_key,)).fetchone()
            retry = db.execute('SELECT retry_of,operation,request_json FROM rolling_registration_write_retries WHERE request_key=?', (request_key,)).fetchone()
            fence = db.execute('SELECT operation,request_json FROM rolling_registration_write_fences WHERE request_key=?', (request_key,)).fetchone()
            if row and fence:
                raise RollingRegistrationError('自动登记计划请求同时存在接受与封止记录')
            if retry and not (row or fence):
                raise RollingRegistrationError('自动登记计划关联重试缺少接受或封止记录')
            if row:
                operation, raw, result_raw = row
                try:
                    request, result = json.loads(raw), json.loads(result_raw)
                except (TypeError, ValueError, UnicodeDecodeError) as exc:
                    raise RollingRegistrationError('自动登记计划接受记录无法解析') from exc
                plan = result.get('plan') if isinstance(result, dict) else None
                if (not isinstance(raw, bytes) or not isinstance(result_raw, bytes)
                        or operation not in ('registration_create', 'registration_cancel', 'registration_retry')
                        or not isinstance(request, dict) or not isinstance(plan, dict)
                        or _canonical(request) != raw or _canonical(result) != result_raw):
                    raise RollingRegistrationError('自动登记计划接受记录格式或摘要不一致')
                if operation == 'registration_create':
                    expected_id = hashlib.sha256(_canonical(
                        {'version': 'rolling-registration-v1', 'request': request})).hexdigest()
                    stored_plan = db.execute('SELECT training_plan_id,request_sha256,request_json '
                                             'FROM rolling_registration_plans WHERE id=?',
                                             (plan.get('id'),)).fetchone()
                    if (plan.get('id') != expected_id or plan.get('request') != request or not stored_plan
                            or stored_plan != (request.get('training_plan_id'), _digest(request), raw)):
                        raise RollingRegistrationError('自动登记计划接受结果与计划记录不一致')
                elif (set(request) != {'id'} or request['id'] != plan.get('id')
                      or not db.execute('SELECT 1 FROM rolling_registration_plans WHERE id=?',
                                        (request['id'],)).fetchone()):
                    raise RollingRegistrationError('自动登记计划操作结果与计划记录不一致')
                retry_of = retry[0] if retry else None
                if retry and (retry[1:] != (operation, raw)
                              or db.execute('SELECT operation,request_json FROM rolling_registration_write_fences '
                                            'WHERE request_key=?', (retry_of,)).fetchone() != (operation, raw)):
                    raise RollingRegistrationError('自动登记计划接受记录的关联重试无法核验')
                return {'request_key': request_key, 'status': 'accepted', 'operation': operation,
                        'request': request, 'result': result, 'retry_of': retry_of}
            if fence:
                operation, raw = fence
                try:
                    request = json.loads(raw)
                except (TypeError, ValueError, UnicodeDecodeError) as exc:
                    raise RollingRegistrationError('自动登记计划封止记录无法解析') from exc
                if (not isinstance(raw, bytes)
                        or operation not in ('registration_create', 'registration_cancel', 'registration_retry')
                        or not isinstance(request, dict) or _canonical(request) != raw):
                    raise RollingRegistrationError('自动登记计划封止记录格式或摘要不一致')
                retry_of = retry[0] if retry else None
                if retry and (retry[1:] != (operation, raw)
                              or db.execute('SELECT operation,request_json FROM rolling_registration_write_fences '
                                            'WHERE request_key=?', (retry_of,)).fetchone() != (operation, raw)):
                    raise RollingRegistrationError('自动登记计划封止记录的关联重试无法核验')
                return {'request_key': request_key, 'status': 'fenced', 'operation': operation,
                        'request': request, 'retry_of': retry_of}
            return {'request_key': request_key, 'status': 'unknown'}

    @staticmethod
    def _check_write_key(request_key):
        if not isinstance(request_key, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I):
            raise RollingRegistrationError('自动登记计划请求标识无效')

    def _prior_write(self, db, request_key, operation, request, retry_of=None):
        row = db.execute('SELECT operation,request_json,result_json FROM rolling_registration_write_requests WHERE request_key=?',
                         (request_key,)).fetchone()
        fence = db.execute('SELECT operation,request_json FROM rolling_registration_write_fences WHERE request_key=?',
                           (request_key,)).fetchone()
        retry = db.execute('SELECT retry_of,operation,request_json FROM rolling_registration_write_retries WHERE request_key=?',
                           (request_key,)).fetchone()
        if fence:
            if fence != (operation, _canonical(request)) or (retry[0] if retry else None) != retry_of:
                raise RollingRegistrationError('请求标识已用于不同封止操作或内容')
            raise RollingRegistrationFenced(request_key, operation, request, retry_of)
        if not row:
            if retry_of is not None: self._claim_retry(db, request_key, operation, request, retry_of)
            return None
        if row[0] != operation or row[1] != _canonical(request):
            raise RollingRegistrationError('自动登记计划请求标识已用于不同操作或内容')
        if (retry[0] if retry else None) != retry_of:
            raise RollingRegistrationError('自动登记计划请求关联重试标识不匹配')
        return json.loads(row[2])['plan']

    def _claim_retry(self, db, request_key, operation, request, retry_of):
        original = db.execute('SELECT operation,request_json FROM rolling_registration_write_fences WHERE request_key=?',
                              (retry_of,)).fetchone()
        if not original or original[0] != operation or original[1] != _canonical(request):
            raise RollingRegistrationError('关联重试必须使用同类已封止请求的相同内容')
        try:
            db.execute('INSERT INTO rolling_registration_write_retries VALUES (?,?,?,?,?)',
                       (request_key, retry_of, operation, _canonical(request), self._now().isoformat()))
        except sqlite3.IntegrityError as exc:
            raise RollingRegistrationError('原请求已有关联重试或新请求键已使用') from exc

    def _save_write(self, db, request_key, operation, request, plan):
        db.execute('INSERT INTO rolling_registration_write_requests VALUES (?,?,?,?,?)',
                   (request_key, operation, _canonical(request), _canonical({'plan': plan}), self._now().isoformat()))

    def fence_write(self, request_key, operation, request, *, retry_of=None):
        self._check_write_key(request_key)
        if operation not in ('registration_create', 'registration_cancel', 'registration_retry') or not isinstance(request, dict):
            raise RollingRegistrationError('自动登记计划封止请求无效')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT operation,request_json,result_json FROM rolling_registration_write_requests WHERE request_key=?',
                             (request_key,)).fetchone()
            if row:
                if row[0] != operation or row[1] != _canonical(request):
                    raise RollingRegistrationError('请求键已用于不同操作或内容')
                return {'request_key': request_key, 'status': 'accepted', 'operation': operation,
                        'request': json.loads(row[1]), 'result': json.loads(row[2]), 'retry_of': retry_of}
            old = db.execute('SELECT operation,request_json FROM rolling_registration_write_fences WHERE request_key=?',
                             (request_key,)).fetchone()
            if old:
                if old != (operation, _canonical(request)):
                    raise RollingRegistrationError('请求键已用于不同封止操作或内容')
                return {'request_key': request_key, 'status': 'fenced', 'operation': operation,
                        'request': request, 'retry_of': retry_of}
            retry = db.execute('SELECT retry_of,operation,request_json FROM rolling_registration_write_retries WHERE request_key=?',
                               (request_key,)).fetchone()
            if (retry[0] if retry else None) != retry_of:
                raise RollingRegistrationError('关联重试标识不匹配')
            if retry_of is not None and not retry:
                self._claim_retry(db, request_key, operation, request, retry_of)
            db.execute('INSERT INTO rolling_registration_write_fences VALUES (?,?,?,?)',
                       (request_key, operation, _canonical(request), self._now().isoformat()))
            return {'request_key': request_key, 'status': 'fenced', 'operation': operation,
                    'request': request, 'retry_of': retry_of}

    def cancel(self, ident, *, request_key=None, retry_of=None):
        self._check_write_key(request_key)
        request = {'id': ident}
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE'); plan = self._get(db, ident)
            prior = self._prior_write(db, request_key, 'registration_cancel', request, retry_of)
            if prior: return prior
            if plan['status'] in ('waiting', 'blocked', 'failed'):
                self._transition(db, ident, 'cancelled', '自动登记已取消', self._now())
            plan = self._get(db, ident); self._save_write(db, request_key, 'registration_cancel', request, plan)
            return plan

    def retry(self, ident, *, request_key=None, retry_of=None):
        self._check_write_key(request_key)
        request = {'id': ident}
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            prior = self._prior_write(db, request_key, 'registration_retry', request, retry_of)
            if prior: return prior
            plan = self._get(db, ident)
            if plan['status'] not in ('blocked', 'failed'):
                raise RollingRegistrationError('只有阻断或失败的计划可以重试')
            self._check_sources(db, plan, self._now())
            if db.execute("SELECT 1 FROM rolling_registration_plans WHERE training_plan_id=? AND status IN ('waiting','complete')", (plan['training_plan_id'],)).fetchone():
                raise RollingRegistrationError('该训练计划已有正在执行或已完成的自动登记计划')
            self._transition(db, ident, 'waiting', '等待重新检查', self._now())
            plan = self._get(db, ident); self._save_write(db, request_key, 'registration_retry', request, plan)
            return plan

    def tick(self):
        from dlt.recovery_guard import require_path_execution_allowed
        require_path_execution_allowed(self.path)
        for plan in self.list():
            if plan['status'] != 'waiting':
                continue
            try:
                self._advance(plan['id'])
            except Exception as exc:
                message = str(exc) if isinstance(exc, (RollingRegistrationError, RegistrationError)) else '自动生成或登记异常，请核对后重试'
                status = 'blocked' if isinstance(exc, (RollingRegistrationError, RegistrationError)) else 'failed'
                with closing(sqlite3.connect(self.path)) as db, db:
                    db.execute('BEGIN IMMEDIATE'); current = self._get(db, plan['id'])
                    if current['status'] == 'waiting':
                        self._transition(db, plan['id'], status, message, self._now())

    def _advance(self, ident):
        with closing(sqlite3.connect(self.path)) as db:
            plan = self._get(db, ident)
            if plan['status'] != 'waiting':
                return
            training, series, cal, rule = self._check_sources(db, plan, self._now())
            if training['status'] in ('waiting', 'dispatching', 'running'):
                return
            if training['status'] != 'complete' or not training['resolved'] or not training['job_key']:
                raise RollingRegistrationError('训练未合格完成，请核对训练计划')
        job = self.training.jobs.get(training['job_key'])
        if job['status'] != 'complete' or not job.get('artifact_id'):
            raise RollingRegistrationError('训练产物不可用')
        resolved = training['resolved']; request = plan['request']; issue = training['issue']
        replicas = []
        for replica in series['manifest']['replica_ids']:
            with closing(sqlite3.connect(self.path)) as db:
                current = self._get(db, ident)
                if current['status'] != 'waiting':
                    return
                self._check_sources(db, current, self._now())
            key = 'auto-pair-'+_digest({'plan': ident, 'replica': replica, 'artifact': job['artifact_id']})
            config = {**series['config_profile'], 'target_issue': issue, 'target_date': cal['draw_date'],
                      'snapshot_id': resolved['config']['snapshot_id'], 'rule_version': rule['version'],
                      'calendar_version': cal['version'], 'seed': series['manifest']['seeds'][issue][replica]}
            self.pairs.save_draft(key, config, history_first_issue=resolved['first_issue'],
                                  history_cutoff_issue=resolved['config']['cutoff'], artifact_id=job['artifact_id'])
            replicas.append({'replica': replica, 'pair_key': key})
        protocol = {key: series['manifest'][key] for key in SHARED_FIELDS}
        protocol.update(target_issue=issue, replicas=replicas, internal_deadline=request['internal_deadline'],
                        calendar_review_id=training['request']['calendar_review_id'],
                        rule_review_id=request['rule_review_id'], tax_review_id=request['tax_review_id'])
        def guard(db, now):
            current = self._get(db, ident)
            if current['status'] != 'waiting' or current['request'] != request:
                raise RollingRegistrationError('自动登记计划已取消或变化')
            latest, _, _, _ = self._check_sources(db, current, now)
            if latest['status'] != 'complete' or latest['job_key'] != training['job_key']:
                raise RollingRegistrationError('训练计划状态已变化')
        def complete(db, content):
            self._transition(db, ident, 'complete', '已固定本期候选与对照', _time(content['registered_at']))
        self.repo.freeze('auto-exp-'+ident, protocol, series_key=training['series_key'],
                         transaction_guard=guard, transaction_after_insert=complete)
