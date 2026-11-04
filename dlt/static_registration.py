"""Scheduled paper registration for fixed-model and untrained series.

The researcher explicitly fixes an issue, source reviews, history window and
expected cutoff. Locally observed data and local time are not independent proof.
"""
from contextlib import closing
from datetime import date, datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import platform
import re
import sqlite3

import numpy as np

from dlt.candidates import _canonical
from dlt.generation_replay import _read_history
from dlt.model_repository import ModelRepository, _assert_consistent_history
from dlt.models import TrainingError
from dlt.pair_repository import PairRepository
from dlt.prospective import ProspectiveRepository, RegistrationError, _time
from dlt.prospective_series import SHARED_FIELDS, _load


class StaticRegistrationError(ValueError):
    pass


class StaticRegistrationFenced(StaticRegistrationError):
    def __init__(self, request_key, operation, request, retry_of=None):
        super().__init__('静态登记计划请求已封止，迟到请求不能执行')
        self.request_key, self.operation, self.request, self.retry_of = request_key, operation, request, retry_of


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


class StaticRegistration:
    def __init__(self, path, *, clock=None):
        self.path = Path(path)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.repo = ProspectiveRepository(path, clock=self.clock)
        self.pairs = PairRepository(path)

    def _now(self):
        now = self.clock()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise StaticRegistrationError('本机时间无效')
        return now.astimezone(timezone.utc)

    def initialize(self):
        self.repo.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS static_registration_plans (
                    id TEXT PRIMARY KEY, series_key TEXT NOT NULL REFERENCES experiment_series(series_key),
                    issue TEXT NOT NULL, request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL,
                    created_at TEXT NOT NULL, status TEXT NOT NULL, resolved_json BLOB,
                    message TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS static_registration_one_active ON static_registration_plans(series_key,issue)
                    WHERE status IN ('waiting','complete');
                CREATE TABLE IF NOT EXISTS static_registration_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, plan_id TEXT NOT NULL REFERENCES static_registration_plans(id),
                    status TEXT NOT NULL, message TEXT NOT NULL, at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS static_registration_write_requests (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_json BLOB NOT NULL,
                    result_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS static_registration_write_no_update BEFORE UPDATE ON static_registration_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration write'); END;
                CREATE TRIGGER IF NOT EXISTS static_registration_write_no_delete BEFORE DELETE ON static_registration_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration write'); END;
                CREATE TABLE IF NOT EXISTS static_registration_write_fences (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS static_registration_fence_no_update BEFORE UPDATE ON static_registration_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration fence'); END;
                CREATE TRIGGER IF NOT EXISTS static_registration_fence_no_delete BEFORE DELETE ON static_registration_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration fence'); END;
                CREATE TABLE IF NOT EXISTS static_registration_write_retries (
                    request_key TEXT PRIMARY KEY, retry_of TEXT NOT NULL UNIQUE REFERENCES static_registration_write_fences(request_key),
                    operation TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS static_registration_retry_no_update BEFORE UPDATE ON static_registration_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration retry'); END;
                CREATE TRIGGER IF NOT EXISTS static_registration_retry_no_delete BEFORE DELETE ON static_registration_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration retry'); END;
                CREATE TRIGGER IF NOT EXISTS static_registration_fixed BEFORE UPDATE OF id,series_key,issue,request_sha256,request_json,created_at ON static_registration_plans
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration plan'); END;
                CREATE TRIGGER IF NOT EXISTS static_registration_no_delete BEFORE DELETE ON static_registration_plans
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration plan'); END;
                CREATE TRIGGER IF NOT EXISTS static_registration_events_no_update BEFORE UPDATE ON static_registration_events
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration event'); END;
                CREATE TRIGGER IF NOT EXISTS static_registration_events_no_delete BEFORE DELETE ON static_registration_events
                    BEGIN SELECT RAISE(ABORT, 'immutable static registration event'); END;
            ''')

    @staticmethod
    def _get(db, ident):
        cur = db.cursor(); cur.row_factory = sqlite3.Row
        row = cur.execute('SELECT * FROM static_registration_plans WHERE id=?', (ident,)).fetchone()
        if row is None:
            raise KeyError(ident)
        item = dict(row); request = json.loads(item.pop('request_json'))
        if (_digest(request) != item['request_sha256'] or request['series_key'] != item['series_key']
                or request['issue'] != item['issue']):
            raise StaticRegistrationError('自动登记计划摘要不符')
        item['request'] = request
        raw = item.pop('resolved_json'); item['resolved'] = json.loads(raw) if raw else None
        item['events'] = [dict(x) for x in cur.execute(
            'SELECT status,message,at FROM static_registration_events WHERE plan_id=? ORDER BY seq', (ident,))]
        return item

    @staticmethod
    def _transition(db, ident, status, message, now, resolved):
        at = now.isoformat()
        db.execute('UPDATE static_registration_plans SET status=?,message=?,updated_at=?,resolved_json=? WHERE id=?',
                   (status, message, at, _canonical(resolved) if resolved is not None else None, ident))
        db.execute('INSERT INTO static_registration_events(plan_id,status,message,at) VALUES (?,?,?,?)',
                   (ident, status, message, at))

    def list(self):
        with closing(sqlite3.connect(self.path)) as db:
            ids = db.execute('SELECT id FROM static_registration_plans ORDER BY created_at DESC,id').fetchall()
            return [self._get(db, row[0]) for row in ids]

    def _check_sources(self, db, plan, now):
        request = plan['request']; issue = request['issue']; key = request['series_key']
        series = _load(db, key)
        if (series['manifest']['retraining_policy'] != 'fixed_artifact_or_untrained_v1'
                or issue not in series['manifest']['observation_issues']):
            raise StaticRegistrationError('目标期不属于固定模型或无需训练的实验')
        if db.execute('SELECT 1 FROM series_closures WHERE series_key=?', (key,)).fetchone():
            raise StaticRegistrationError('实验版本已关闭')
        if db.execute('SELECT 1 FROM series_periods WHERE series_key=? AND issue=?', (key, issue)).fetchone():
            raise StaticRegistrationError('该奖期已有固定实验')
        if db.execute('SELECT 1 FROM draw_versions WHERE issue=?', (issue,)).fetchone():
            raise StaticRegistrationError('目标期已出现，不能补造前瞻登记')
        records = {}
        for kind in ('calendar', 'rule', 'tax'):
            ident = request[kind+'_review_id']
            current, record, _ = self.repo._effective_review(db, ident, kind, issue=issue)
            if current != ident or _time(record['reviewed_at']) > now:
                raise StaticRegistrationError('资料已更正或核对时间无效')
            records[kind] = record
        cal, rule, tax = (records[k]['extracted'] for k in ('calendar', 'rule', 'tax'))
        internal, cutoff, start = (_time(request['internal_deadline']), _time(cal['cutoff_at']), _time(request['start_at']))
        if (cal.get('issue') != issue or not cal.get('version') or not rule.get('version')
                or cutoff.astimezone(timezone(timedelta(hours=8))).date().isoformat() != cal.get('draw_date')
                or rule.get('source') != records['rule']['source_url']
                or not isinstance(rule.get('applicable_issues'), list) or issue not in rule['applicable_issues']
                or not isinstance(tax.get('applicable_issues'), list) or issue not in tax['applicable_issues']
                or tax.get('tax') != rule.get('tax') or internal > cutoff or start >= internal
                or now >= internal or now >= cutoff):
            raise StaticRegistrationError('资料、适用期或登记截止不符合条件')
        identity = series['generator_identity']
        if (identity != {'version': 'python-candidate-v1', 'runtime': platform.python_version(),
                         'code_sha256': hashlib.sha256(Path(__file__).with_name('candidates.py').read_bytes()).hexdigest()}):
            raise StaticRegistrationError('生成代码或运行环境与固定实验不一致')
        artifact_id = series['artifact_id']; method = series['config_profile']['method']
        if (method in ('tree', 'lstm')) != bool(artifact_id):
            raise StaticRegistrationError('固定模型产物与实验方法不一致')
        minimum = series['config_profile']['window']
        if artifact_id:
            artifact = ModelRepository(self.path).get(artifact_id)
            model_code = hashlib.sha256(Path(__file__).with_name('models.py').read_bytes()).hexdigest()
            if (artifact.get('code_sha256') != model_code
                    or artifact.get('runtime') != {'python': platform.python_version(), 'numpy': np.__version__}):
                raise StaticRegistrationError('固定模型代码或运行环境与产物不一致')
            minimum = max(minimum, artifact['config']['window'])
        if request['history_periods'] < minimum:
            raise StaticRegistrationError('历史窗口小于固定方法或模型所需长度')
        return series, cal, rule

    def create(self, *, series_key, issue, calendar_review_id, rule_review_id, tax_review_id,
               expected_cutoff_issue, history_periods, start_at, internal_deadline, confirmed, request_key=None, retry_of=None):
        ids = (calendar_review_id, rule_review_id, tax_review_id)
        if (confirmed is not True or not isinstance(series_key, str) or not 1 <= len(series_key) <= 128
                or any(not isinstance(x, str) or not re.fullmatch(r'[0-9]{5}', x) for x in (issue, expected_cutoff_issue))
                or issue == expected_cutoff_issue or any(not isinstance(x, str) or not re.fullmatch(r'[0-9a-f]{64}', x) for x in ids)
                or type(history_periods) is not int or not 1 <= history_periods <= 2000):
            raise StaticRegistrationError('请确认实验、奖期、资料、历史窗口与截止')
        request = {'series_key': series_key, 'issue': issue, 'calendar_review_id': calendar_review_id,
                   'rule_review_id': rule_review_id, 'tax_review_id': tax_review_id,
                   'expected_cutoff_issue': expected_cutoff_issue, 'history_periods': history_periods,
                   'start_at': _time(start_at).isoformat(), 'internal_deadline': _time(internal_deadline).isoformat()}
        ident = _digest({'version': 'static-registration-v1', 'request': request})
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            if request_key is not None:
                self._check_write_key(request_key)
                prior = self._prior_write(db, request_key, 'static_create', request, retry_of)
                if prior: return prior
            if db.execute('SELECT 1 FROM static_registration_plans WHERE id=?', (ident,)).fetchone():
                result = self._get(db, ident); self._save_write(db, request_key, 'static_create', request, result); return result
            if db.execute("SELECT 1 FROM static_registration_plans WHERE series_key=? AND issue=? AND status IN ('waiting','complete')", (series_key, issue)).fetchone():
                raise StaticRegistrationError('该实验奖期已有等待中或已完成的自动登记计划')
            now = self._now(); self._check_sources(db, {'request': request}, now)
            db.execute('INSERT INTO static_registration_plans VALUES (?,?,?,?,?,?,?,?,?,?)',
                       (ident, series_key, issue, _digest(request), _canonical(request), now.isoformat(),
                        'waiting', None, '等待开始时间和历史数据', now.isoformat()))
            db.execute('INSERT INTO static_registration_events(plan_id,status,message,at) VALUES (?,?,?,?)',
                       (ident, 'waiting', '计划已确认', now.isoformat()))
            result = self._get(db, ident); self._save_write(db, request_key, 'static_create', request, result); return result

    @staticmethod
    def _check_write_key(key):
        if not isinstance(key, str) or not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', key, re.I):
            raise StaticRegistrationError('自动登记计划请求标识无效')

    def _prior_write(self, db, key, operation, request, retry_of=None):
        row = db.execute('SELECT operation,request_json,result_json FROM static_registration_write_requests WHERE request_key=?', (key,)).fetchone()
        fence = db.execute('SELECT operation,request_json FROM static_registration_write_fences WHERE request_key=?',(key,)).fetchone()
        retry = db.execute('SELECT retry_of,operation,request_json FROM static_registration_write_retries WHERE request_key=?',(key,)).fetchone()
        if fence:
            if fence != (operation,_canonical(request)) or (retry[0] if retry else None)!=retry_of: raise StaticRegistrationError('请求标识已用于不同封止操作')
            raise StaticRegistrationFenced(key,operation,request,retry_of)
        if not row:
            if retry_of is not None:self._claim_retry(db,key,operation,request,retry_of)
            return None
        if row[0] != operation or row[1] != _canonical(request) or (retry[0] if retry else None)!=retry_of: raise StaticRegistrationError('请求标识已用于不同静态登记操作')
        return json.loads(row[2])['plan']

    def _claim_retry(self,db,key,operation,request,retry_of):
        original=db.execute('SELECT operation,request_json FROM static_registration_write_fences WHERE request_key=?',(retry_of,)).fetchone()
        if not original or original!=(operation,_canonical(request)):raise StaticRegistrationError('关联重试必须与已封止原请求同操作同内容')
        try:db.execute('INSERT INTO static_registration_write_retries VALUES (?,?,?,?,?)',(key,retry_of,operation,_canonical(request),self._now().isoformat()))
        except sqlite3.IntegrityError as exc:raise StaticRegistrationError('原请求已有重试或请求键已使用') from exc

    def _save_write(self, db, key, operation, request, plan):
        if key: db.execute('INSERT INTO static_registration_write_requests VALUES (?,?,?,?,?)',
                           (key, operation, _canonical(request), _canonical({'plan': plan}), self._now().isoformat()))

    def fence_write(self,key,operation,request,*,retry_of=None):
        self._check_write_key(key)
        if operation not in ('static_create','static_cancel','static_retry') or not isinstance(request,dict):raise StaticRegistrationError('静态登记封止请求无效')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db,db:
            db.execute('PRAGMA foreign_keys=ON');db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT operation,request_json,result_json FROM static_registration_write_requests WHERE request_key=?',(key,)).fetchone()
            if row:
                if row[0]!=operation or row[1]!=_canonical(request):raise StaticRegistrationError('请求键已用于不同写入')
                return {'request_key':key,'status':'accepted','operation':operation,'request':json.loads(row[1]),'result':json.loads(row[2]),'retry_of':retry_of}
            old=db.execute('SELECT operation,request_json FROM static_registration_write_fences WHERE request_key=?',(key,)).fetchone()
            if old:
                if old!=(operation,_canonical(request)):raise StaticRegistrationError('请求键已用于不同封止')
                return {'request_key':key,'status':'fenced','operation':operation,'request':request,'retry_of':retry_of}
            retry=db.execute('SELECT retry_of,operation,request_json FROM static_registration_write_retries WHERE request_key=?',(key,)).fetchone()
            if (retry[0] if retry else None)!=retry_of:raise StaticRegistrationError('关联重试不匹配')
            if retry_of is not None and not retry:self._claim_retry(db,key,operation,request,retry_of)
            db.execute('INSERT INTO static_registration_write_fences VALUES (?,?,?,?)',(key,operation,_canonical(request),self._now().isoformat()))
            return {'request_key':key,'status':'fenced','operation':operation,'request':request,'retry_of':retry_of}

    def write_status(self, key):
        self._check_write_key(key); self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT operation,request_json,result_json FROM static_registration_write_requests WHERE request_key=?', (key,)).fetchone()
            fence=db.execute('SELECT operation,request_json FROM static_registration_write_fences WHERE request_key=?',(key,)).fetchone()
            retry=db.execute('SELECT retry_of,operation,request_json FROM static_registration_write_retries WHERE request_key=?',(key,)).fetchone()
            if row and fence:raise StaticRegistrationError('静态登记请求同时存在接受与封止记录')
            if retry and not (row or fence):raise StaticRegistrationError('静态登记关联重试缺少接受或封止记录')
            if row:
                operation,raw,result_raw=row
                try: request,result=json.loads(raw),json.loads(result_raw)
                except (TypeError,ValueError,UnicodeDecodeError) as exc:raise StaticRegistrationError('静态登记接受记录无法解析') from exc
                plan=result.get('plan') if isinstance(result,dict) else None
                if (not isinstance(raw,bytes) or not isinstance(result_raw,bytes)
                        or operation not in ('static_create','static_cancel','static_retry')
                        or not isinstance(request,dict) or not isinstance(plan,dict)
                        or _canonical(request)!=raw or _canonical(result)!=result_raw):
                    raise StaticRegistrationError('静态登记接受记录格式或摘要不一致')
                if operation=='static_create':
                    expected=hashlib.sha256(_canonical({'version':'static-registration-v1','request':request})).hexdigest()
                    stored=db.execute('SELECT series_key,issue,request_sha256,request_json FROM static_registration_plans WHERE id=?',(plan.get('id'),)).fetchone()
                    if (plan.get('id')!=expected or plan.get('request')!=request or not stored
                            or stored!=(request.get('series_key'),request.get('issue'),_digest(request),raw)):
                        raise StaticRegistrationError('静态登记接受结果与计划记录不一致')
                else:
                    if set(request)!={'id'} or request['id']!=plan.get('id'):
                        raise StaticRegistrationError('静态登记操作请求与计划结果不一致')
                    stored=db.execute('SELECT request_json FROM static_registration_plans WHERE id=?',(request['id'],)).fetchone()
                    if not stored or json.loads(stored[0])!=plan.get('request'):
                        raise StaticRegistrationError('静态登记操作结果与计划记录不一致')
                retry_of=retry[0] if retry else None
                if retry and (retry[1:]!=(operation,raw)
                        or db.execute('SELECT operation,request_json FROM static_registration_write_fences WHERE request_key=?',(retry_of,)).fetchone()!=(operation,raw)):
                    raise StaticRegistrationError('静态登记接受记录的关联重试无法核验')
                return {'request_key':key,'status':'accepted','operation':operation,'request':request,'result':result,'retry_of':retry_of}
            if fence:
                operation,raw=fence
                try:request=json.loads(raw)
                except (TypeError,ValueError,UnicodeDecodeError) as exc:raise StaticRegistrationError('静态登记封止记录无法解析') from exc
                if (not isinstance(raw,bytes) or operation not in ('static_create','static_cancel','static_retry')
                        or not isinstance(request,dict) or _canonical(request)!=raw):
                    raise StaticRegistrationError('静态登记封止记录格式或摘要不一致')
                retry_of=retry[0] if retry else None
                if retry and (retry[1:]!=(operation,raw)
                        or db.execute('SELECT operation,request_json FROM static_registration_write_fences WHERE request_key=?',(retry_of,)).fetchone()!=(operation,raw)):
                    raise StaticRegistrationError('静态登记封止记录的关联重试无法核验')
                return {'request_key':key,'status':'fenced','operation':operation,'request':request,'retry_of':retry_of}
            return {'request_key':key,'status':'unknown'}

    def cancel(self, ident, *, request_key=None, retry_of=None):
        self._check_write_key(request_key); request={'id':ident}
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE'); plan = self._get(db, ident)
            prior=self._prior_write(db,request_key,'static_cancel',request,retry_of)
            if prior:return prior
            if plan['status'] in ('waiting', 'blocked', 'failed'):
                self._transition(db, ident, 'cancelled', '自动登记已取消', self._now(), plan['resolved'])
            plan=self._get(db,ident);self._save_write(db,request_key,'static_cancel',request,plan);return plan

    def retry(self, ident, *, request_key=None, retry_of=None):
        self._check_write_key(request_key); request={'id':ident}
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE'); prior=self._prior_write(db,request_key,'static_retry',request,retry_of)
            if prior:return prior
            plan = self._get(db, ident)
            if plan['status'] not in ('blocked', 'failed'):
                raise StaticRegistrationError('只有阻断或失败的计划可以重试')
            self._check_sources(db, plan, self._now())
            if db.execute("SELECT 1 FROM static_registration_plans WHERE series_key=? AND issue=? AND status IN ('waiting','complete')", (plan['series_key'], plan['issue'])).fetchone():
                raise StaticRegistrationError('该实验奖期已有等待中或已完成的自动登记计划')
            self._transition(db, ident, 'waiting', '等待重新检查', self._now(), plan['resolved'])
            plan=self._get(db,ident);self._save_write(db,request_key,'static_retry',request,plan);return plan

    def _resolve(self, db, plan, now):
        series, cal, rule = self._check_sources(db, plan, now)
        request = plan['request']
        if now < _time(request['start_at']):
            return None
        target_day = date.fromisoformat(cal['draw_date']); grouped = {}; snapshots = {}
        observed = db.execute('SELECT d.snapshot_id,d.issue,d.draw_date,o.observed_at FROM draw_versions d '
                              'JOIN observations o ON o.snapshot_id=d.snapshot_id').fetchall()
        for sid, issue, day, at in observed:
            at = _time(at)
            if at <= now:
                snapshots[sid] = max(snapshots.get(sid, at), at)
                if date.fromisoformat(day) < target_day:
                    grouped.setdefault(day, set()).add(issue)
        n = request['history_periods']; days = sorted(grouped)[-n:]
        if len(days) < n:
            return None
        if any(len(grouped[day]) != 1 for day in days):
            raise StaticRegistrationError('历史数据存在同日奖期顺序歧义')
        wanted = [(next(iter(grouped[day])), day) for day in days]
        if wanted[-1][0] != request['expected_cutoff_issue']:
            if any(request['expected_cutoff_issue'] in x for x in grouped.values()):
                raise StaticRegistrationError('已出现不同的更新历史期，请核对计划')
            return None
        try:
            _assert_consistent_history(db, {issue for issue, _ in wanted})
        except TrainingError as exc:
            raise StaticRegistrationError('固定历史窗口存在号码或日期冲突') from exc
        pinned = plan['resolved']
        if pinned and pinned['snapshot_id'] not in snapshots:
            raise StaticRegistrationError('固定历史快照尚无有效本机观察记录')
        candidates = [pinned['snapshot_id']] if pinned else sorted(snapshots, key=lambda x: (snapshots[x], x), reverse=True)
        for sid in candidates:
            try:
                rows = _read_history(db, sid, wanted[0][0], wanted[-1][0])
            except (ValueError, KeyError, sqlite3.Error):
                continue
            if [(r['issue'], r['date']) for r in rows] == wanted:
                resolved = {'snapshot_id': sid, 'first_issue': wanted[0][0], 'cutoff_issue': wanted[-1][0],
                            'history_sha256': _digest(rows)}
                if pinned and pinned != resolved:
                    raise StaticRegistrationError('固定历史窗口已变化')
                return resolved
        if pinned:
            raise StaticRegistrationError('固定历史快照不可用或不再覆盖最新窗口')
        return None

    def tick(self):
        from dlt.recovery_guard import require_path_execution_allowed
        require_path_execution_allowed(self.path)
        for plan in self.list():
            if plan['status'] != 'waiting':
                continue
            try:
                self._advance(plan['id'])
            except Exception as exc:
                known = isinstance(exc, (StaticRegistrationError, RegistrationError))
                with closing(sqlite3.connect(self.path)) as db, db:
                    db.execute('BEGIN IMMEDIATE'); current = self._get(db, plan['id'])
                    if current['status'] == 'waiting':
                        self._transition(db, plan['id'], 'blocked' if known else 'failed',
                                         str(exc) if known else '自动生成或登记异常，请核对后重试', self._now(), current['resolved'])

    def _advance(self, ident):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE'); plan = self._get(db, ident)
            if plan['status'] != 'waiting':
                return
            resolved = self._resolve(db, plan, self._now())
            if resolved is None:
                return
            if plan['resolved'] is None:
                db.execute('UPDATE static_registration_plans SET resolved_json=?,updated_at=? WHERE id=?',
                           (_canonical(resolved), self._now().isoformat(), ident))
            series, cal, rule = self._check_sources(db, plan, self._now())
        request = plan['request']; issue = request['issue']; replicas = []
        selection = None
        if series['artifact_id']:
            template_key = series['manifest'].get('template_pair_key')
            history = self.pairs.selection_history(template_key)
            if not history:
                raise StaticRegistrationError('固定模型模板缺少模型选择决策历史')
            selection = history[-1]['selection']
        for replica in series['manifest']['replica_ids']:
            with closing(sqlite3.connect(self.path)) as db:
                current = self._get(db, ident)
                if current['status'] != 'waiting':
                    return
                if self._resolve(db, current, self._now()) != resolved:
                    raise StaticRegistrationError('固定历史窗口已变化')
            key = 'static-pair-'+_digest({'plan': ident, 'replica': replica})
            config = {**series['config_profile'], 'target_issue': issue, 'target_date': cal['draw_date'],
                      'snapshot_id': resolved['snapshot_id'], 'rule_version': rule['version'],
                      'calendar_version': cal['version'], 'seed': series['manifest']['seeds'][issue][replica]}
            self.pairs.save_draft(key, config, history_first_issue=resolved['first_issue'],
                                  history_cutoff_issue=resolved['cutoff_issue'], artifact_id=series['artifact_id'],
                                  selection=selection)
            replicas.append({'replica': replica, 'pair_key': key})
        protocol = {key: series['manifest'][key] for key in SHARED_FIELDS}
        protocol.update(target_issue=issue, replicas=replicas, internal_deadline=request['internal_deadline'],
                        calendar_review_id=request['calendar_review_id'], rule_review_id=request['rule_review_id'],
                        tax_review_id=request['tax_review_id'])
        def guard(db, now):
            current = self._get(db, ident)
            if current['status'] != 'waiting' or current['request'] != request or current['resolved'] != resolved:
                raise StaticRegistrationError('自动登记计划已取消或变化')
            if self._resolve(db, current, now) != resolved:
                raise StaticRegistrationError('固定历史窗口已变化')
        def complete(db, content):
            self._transition(db, ident, 'complete', '已固定本期候选与对照', _time(content['registered_at']), resolved)
        self.repo.freeze('static-exp-'+ident, protocol, series_key=request['series_key'],
                         transaction_guard=guard, transaction_after_insert=complete)
