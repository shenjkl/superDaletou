"""Append-only personal judgments and linked, unregistered hypothesis drafts."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import secrets
import sqlite3

from dlt.candidates import _canonical
from dlt.recovery_guard import require_execution_allowed


class DecisionError(ValueError):
    pass


class StaleEvidence(DecisionError):
    pass


class ObjectWritePending(DecisionError):
    """Another request currently owns, or legacy requests conflict on, this object."""


def _sha(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _text(value, label, limit=1000):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise DecisionError(f'{label}不能为空，且不超过 {limit} 字')
    return value.strip()


def _id(value):
    return _text(value, '标识', 128)


def _decision_request(*, kind, key, choice, reason, limitations, evidence_sha256):
    if kind not in ('series', 'fixed_evaluation'):
        raise DecisionError('不支持的证据类型')
    if choice not in ('continue', 'not_participate', 'finish'):
        raise DecisionError('请选择个人判断')
    if not isinstance(evidence_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', evidence_sha256):
        raise DecisionError('证据摘要无效，请重新加载')
    return {'kind': kind, 'key': _id(key), 'choice': choice, 'reason': _text(reason, '判断理由'),
            'limitations': _text(limitations, '研究限制'), 'evidence_sha256': evidence_sha256}


def _draft_request(*, decision_key, hypothesis):
    return {'decision_key': _id(decision_key), 'hypothesis': _text(hypothesis, '新假设', 200)}


def _record(db, table, key):
    if table not in ('research_decisions', 'research_hypothesis_drafts'):
        raise ValueError('invalid record table')
    row = db.execute(f'SELECT request_json,record_sha256,record_json FROM {table} WHERE request_key=?', (_id(key),)).fetchone()
    if not row:
        raise KeyError(key)
    request_raw, digest, raw = row
    if not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != digest:
        raise DecisionError('记录摘要不符，请检查数据完整性')
    try:
        request, record = json.loads(request_raw), json.loads(raw)
        if (_canonical(request) != request_raw or _canonical(record) != raw
                or record['request_key'] != key or record['request'] != request):
            raise ValueError('invalid record')
        if table == 'research_decisions':
            evidence = record['evidence']
            if (record['version'] != 'research-decision-v1'
                    or _sha(evidence) != record['evidence_sha256']
                    or request['evidence_sha256'] != record['evidence_sha256']
                    or (evidence['kind'], evidence['key']) != (request['kind'], request['key'])
                    or request['choice'] not in ('continue', 'not_participate', 'finish')):
                raise ValueError('invalid evidence')
        elif record['version'] != 'research-hypothesis-draft-v1' or record['status'] != 'draft_not_preregistered':
            raise ValueError('invalid draft')
    except (TypeError, KeyError, ValueError) as exc:
        raise DecisionError('记录内容无法核验') from exc
    return record, digest


def load_draft(db, key):
    record, _ = _record(db, 'research_hypothesis_drafts', key)
    parent, parent_sha = _record(db, 'research_decisions', record['decision_key'])
    stored_parent = db.execute('SELECT decision_key FROM research_hypothesis_drafts WHERE request_key=?', (key,)).fetchone()
    if (not stored_parent or stored_parent[0] != record['decision_key']
            or record['decision_sha256'] != parent_sha or record['request']['decision_key'] != record['decision_key']
            or record['request']['hypothesis'] != record['hypothesis']):
        raise DecisionError('新假设与原判断的关联无法核验')
    return record


class ResearchDecisions:
    def __init__(self, path, *, clock=None):
        self.path = Path(path)
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def initialize(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            require_execution_allowed(db)
            db.executescript('''
                CREATE TABLE IF NOT EXISTS research_decisions (
                    request_key TEXT PRIMARY KEY, request_json BLOB NOT NULL,
                    record_sha256 TEXT NOT NULL, record_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS research_decisions_no_update BEFORE UPDATE ON research_decisions
                    BEGIN SELECT RAISE(ABORT, 'immutable research decision'); END;
                CREATE TRIGGER IF NOT EXISTS research_decisions_no_delete BEFORE DELETE ON research_decisions
                    BEGIN SELECT RAISE(ABORT, 'immutable research decision'); END;
                CREATE TABLE IF NOT EXISTS research_hypothesis_drafts (
                    request_key TEXT PRIMARY KEY, decision_key TEXT NOT NULL REFERENCES research_decisions(request_key),
                    request_json BLOB NOT NULL, record_sha256 TEXT NOT NULL, record_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS research_hypothesis_drafts_no_update BEFORE UPDATE ON research_hypothesis_drafts
                    BEGIN SELECT RAISE(ABORT, 'immutable hypothesis draft'); END;
                CREATE TRIGGER IF NOT EXISTS research_hypothesis_drafts_no_delete BEFORE DELETE ON research_hypothesis_drafts
                    BEGIN SELECT RAISE(ABORT, 'immutable hypothesis draft'); END;
                CREATE TABLE IF NOT EXISTS research_write_intents (
                    operation TEXT NOT NULL, request_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, object_kind TEXT NOT NULL, object_key TEXT NOT NULL,
                    retry_of TEXT, created_at TEXT NOT NULL,
                    PRIMARY KEY (operation, request_key), UNIQUE (operation, retry_of)
                );
                CREATE TRIGGER IF NOT EXISTS research_write_intents_no_update BEFORE UPDATE ON research_write_intents
                    BEGIN SELECT RAISE(ABORT, 'immutable research write intent'); END;
                CREATE TRIGGER IF NOT EXISTS research_write_intents_no_delete BEFORE DELETE ON research_write_intents
                    BEGIN SELECT RAISE(ABORT, 'immutable research write intent'); END;
                CREATE TABLE IF NOT EXISTS research_write_fences (
                    operation TEXT NOT NULL, request_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY (operation, request_key)
                );
                CREATE TRIGGER IF NOT EXISTS research_write_fences_no_update BEFORE UPDATE ON research_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable research write fence'); END;
                CREATE TRIGGER IF NOT EXISTS research_write_fences_no_delete BEFORE DELETE ON research_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable research write fence'); END;
                CREATE TABLE IF NOT EXISTS research_write_slots (
                    operation TEXT NOT NULL, object_kind TEXT NOT NULL, object_key TEXT NOT NULL,
                    request_key TEXT NOT NULL, slot_version INTEGER NOT NULL CHECK(slot_version > 0),
                    PRIMARY KEY (operation, object_kind, object_key),
                    UNIQUE (operation, request_key),
                    FOREIGN KEY (operation, request_key)
                        REFERENCES research_write_intents(operation, request_key)
                );
                CREATE TABLE IF NOT EXISTS research_write_acknowledgments (
                    operation TEXT NOT NULL, request_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, result_status TEXT NOT NULL CHECK(result_status IN ('committed','fenced')),
                    slot_version INTEGER NOT NULL CHECK(slot_version > 0), acknowledged_at TEXT NOT NULL,
                    PRIMARY KEY (operation, request_key),
                    FOREIGN KEY (operation, request_key)
                        REFERENCES research_write_intents(operation, request_key)
                );
                CREATE TRIGGER IF NOT EXISTS research_write_acknowledgments_no_update BEFORE UPDATE ON research_write_acknowledgments
                    BEGIN SELECT RAISE(ABORT, 'immutable research write acknowledgment'); END;
                CREATE TRIGGER IF NOT EXISTS research_write_acknowledgments_no_delete BEFORE DELETE ON research_write_acknowledgments
                    BEGIN SELECT RAISE(ABORT, 'immutable research write acknowledgment'); END;
                CREATE TRIGGER IF NOT EXISTS research_decisions_no_fenced_insert BEFORE INSERT ON research_decisions
                    WHEN EXISTS (SELECT 1 FROM research_write_fences WHERE operation='decision' AND request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'fenced research decision'); END;
                CREATE TRIGGER IF NOT EXISTS research_hypothesis_drafts_no_fenced_insert BEFORE INSERT ON research_hypothesis_drafts
                    WHEN EXISTS (SELECT 1 FROM research_write_fences WHERE operation='draft' AND request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'fenced research draft'); END;
            ''')

    def _now(self):
        value = self.clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise DecisionError('本机时间无时区，无法保存记录')
        return value.astimezone(timezone.utc).isoformat()

    def _context(self, db, kind, key):
        _id(key)
        if kind == 'series':
            from dlt.prospective_series import SeriesRepository, _load
            series = _load(db, key)
            report = SeriesRepository(self.path, clock=self.clock).describe(key, db=db)
            report.pop('as_of_utc', None)
            closure = db.execute('SELECT closed_at,reason FROM series_closures WHERE series_key=?', (key,)).fetchone()
            return {'kind': kind, 'key': key, 'hypothesis': series['manifest']['hypothesis'],
                    'protocol_sha256': _sha(series), 'report': report,
                    'closure': {'closed_at': closure[0], 'reason': closure[1]} if closure else None,
                    'limitations': ['描述性观察，不证明方法优于随机或盈利', '来源和时间为本机留存，未经独立认证']}
        if kind == 'fixed_evaluation':
            from dlt.fixed_evaluation import FixedEvaluations
            item = FixedEvaluations(self.path)._get(db, key)
            methods = []
            if item['validity'] == 'local_descriptive' and item['report']:
                for method in item['report']['methods']:
                    result = method['result']
                    methods.append({'id': method['id'], 'planned': result['planned_periods'],
                                    'valid': result['complete_periods'],
                                    'mean_difference_fraction': result['descriptive_mean_fraction'],
                                    'periods': [{'issue': period['target']['issue'], 'status': period['status'],
                                                 'mean_difference_fraction': period['mean_difference_fraction'],
                                                 'failures': [{'replica': r['replica'], 'reason': r['reason']}
                                                              for r in period['replicas'] if r['status'] != 'complete']}
                                                for period in result['periods']]})
            return {'kind': kind, 'key': key, 'hypothesis': item['protocol']['hypothesis'],
                    'observation_issues': item['protocol']['methods'][0]['protocol']['targets'],
                    'protocol_sha256': item['protocol_sha256'], 'status': item['status'],
                    'validity': item['validity'], 'validity_reason': item.get('validity_reason'),
                    'report_sha256': item['report_sha256'], 'correction': item.get('correction'),
                    'superseded_by': item['superseded_by'], 'methods': methods,
                    'limitations': ['历史重建，独立性及当时可用时间未经独立证明', '描述性观察，不证明方法优于随机或盈利']}
        raise DecisionError('不支持的证据类型')

    def context(self, kind, key):
        # _get for a fixed historical evaluation also reads scenarios using a
        # second connection; the reserved write lock keeps both views stable.
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            evidence = self._context(db, kind, key)
            return {'evidence': evidence, 'evidence_sha256': _sha(evidence)}

    def save(self, request_key, *, kind, key, choice, reason, limitations, evidence_sha256, require_intent=False):
        request_key = _id(request_key)
        request = _decision_request(kind=kind, key=key, choice=choice, reason=reason,
                                    limitations=limitations, evidence_sha256=evidence_sha256)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            self._reject_fenced(db, 'decision', request_key)
            intent = self._check_optional_intent(db, 'decision', request_key, _sha(request), require_intent)
            if db.execute('SELECT 1 FROM research_decisions WHERE request_key=?', (request_key,)).fetchone():
                previous, _ = _record(db, 'research_decisions', request_key)
                if previous['request'] != request:
                    raise DecisionError('请求标识已用于其他判断')
                return previous
            if intent:
                self._claim_slot(db, intent)
            elif self._slot(db, 'decision', kind, request['key']):
                raise DecisionError('该研究对象已有待确认写入，不能绕过准备记录')
            evidence = self._context(db, request['kind'], request['key'])
            if _sha(evidence) != evidence_sha256:
                raise StaleEvidence('证据已变化，请重新加载后再保存')
            record = {'version': 'research-decision-v1', 'request_key': request_key, 'request': request,
                      'evidence': evidence, 'evidence_sha256': evidence_sha256, 'created_at': self._now()}
            raw = _canonical(record)
            db.execute('INSERT INTO research_decisions VALUES (?,?,?,?)',
                       (request_key, _canonical(request), hashlib.sha256(raw).hexdigest(), raw))
            return record

    def draft(self, request_key, *, decision_key, hypothesis, require_intent=False):
        request_key = _id(request_key)
        request = _draft_request(decision_key=decision_key, hypothesis=hypothesis)
        decision_key, hypothesis = request['decision_key'], request['hypothesis']
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            self._reject_fenced(db, 'draft', request_key)
            intent = self._check_optional_intent(db, 'draft', request_key, _sha(request), require_intent)
            if db.execute('SELECT 1 FROM research_hypothesis_drafts WHERE request_key=?', (request_key,)).fetchone():
                previous = load_draft(db, request_key)
                if previous['request'] != request:
                    raise DecisionError('请求标识已用于其他草稿')
                return previous
            if intent:
                self._claim_slot(db, intent)
            elif self._slot(db, 'draft', 'decision', decision_key):
                raise DecisionError('该研究对象已有待确认写入，不能绕过准备记录')
            _, parent_sha = _record(db, 'research_decisions', decision_key)
            record = {'version': 'research-hypothesis-draft-v1', 'request_key': request_key, 'request': request,
                      'decision_key': decision_key, 'decision_sha256': parent_sha, 'hypothesis': hypothesis,
                      'created_at': self._now(), 'status': 'draft_not_preregistered'}
            raw = _canonical(record)
            db.execute('INSERT INTO research_hypothesis_drafts VALUES (?,?,?,?,?)',
                       (request_key, decision_key, _canonical(request), hashlib.sha256(raw).hexdigest(), raw))
            return record

    @staticmethod
    def _write_input(operation, request_key, request_sha256):
        if operation not in ('decision', 'draft'):
            raise DecisionError('不支持的写入类型')
        request_key = _id(request_key)
        if not isinstance(request_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', request_sha256):
            raise DecisionError('请求摘要无效')
        return operation, request_key, request_sha256

    @staticmethod
    def _reject_fenced(db, operation, request_key):
        if db.execute('SELECT 1 FROM research_write_fences WHERE operation=? AND request_key=?',
                      (operation, request_key)).fetchone():
            raise DecisionError('该请求已封止，不能提交；请核对后使用新请求')

    @staticmethod
    def _write_state(db, operation, request_key, request_sha256):
        table = 'research_decisions' if operation == 'decision' else 'research_hypothesis_drafts'
        present = db.execute(f'SELECT 1 FROM {table} WHERE request_key=?', (request_key,)).fetchone()
        fence = db.execute('SELECT request_sha256,created_at FROM research_write_fences '
                           'WHERE operation=? AND request_key=?', (operation, request_key)).fetchone()
        if present and fence:
            raise DecisionError('写入和封止记录冲突，无法核验')
        if present:
            record = (_record(db, table, request_key)[0] if operation == 'decision'
                      else load_draft(db, request_key))
            if not secrets.compare_digest(_sha(record['request']), request_sha256):
                raise DecisionError('请求标识与输入摘要不一致')
            return {'status': 'committed', 'operation': operation, 'request_key': request_key,
                    'request_sha256': request_sha256, 'record': record}
        if fence:
            if not isinstance(fence[0], str) or not re.fullmatch(r'[0-9a-f]{64}', fence[0]):
                raise DecisionError('封止记录无法核验')
            try:
                saved_at = datetime.fromisoformat(fence[1])
                if saved_at.tzinfo is None:
                    raise ValueError('missing timezone')
            except (TypeError, ValueError) as exc:
                raise DecisionError('封止记录无法核验') from exc
            if not secrets.compare_digest(fence[0], request_sha256):
                raise DecisionError('请求标识与输入摘要不一致')
            return {'status': 'fenced', 'operation': operation, 'request_key': request_key,
                    'request_sha256': request_sha256, 'fenced_at': fence[1]}
        return {'status': 'unknown', 'operation': operation, 'request_key': request_key,
                'request_sha256': request_sha256}

    @staticmethod
    def _intent(db, operation, request_key):
        row = db.execute('SELECT request_sha256,object_kind,object_key,retry_of,created_at '
                         'FROM research_write_intents WHERE operation=? AND request_key=?',
                         (operation, request_key)).fetchone()
        if not row:
            raise DecisionError('本数据库未找到对应的写入准备记录，不能确认原操作')
        digest, kind, key, retry_of, created_at = row
        ResearchDecisions._write_input(operation, request_key, digest)
        if ((operation == 'decision' and kind not in ('series', 'fixed_evaluation'))
                or (operation == 'draft' and kind != 'decision') or _id(key) != key
                or (retry_of is not None and (_id(retry_of) != retry_of or retry_of == request_key))):
            raise DecisionError('写入准备记录无法核验')
        try:
            if datetime.fromisoformat(created_at).tzinfo is None:
                raise ValueError('missing timezone')
        except (TypeError, ValueError) as exc:
            raise DecisionError('写入准备记录无法核验') from exc
        return {'operation': operation, 'request_key': request_key, 'request_sha256': digest,
                'object_kind': kind, 'object_key': key, 'retry_of': retry_of, 'created_at': created_at}

    @staticmethod
    def _require_intent(db, operation, request_key, request_sha256):
        intent = ResearchDecisions._intent(db, operation, request_key)
        if not secrets.compare_digest(intent['request_sha256'], request_sha256):
            raise DecisionError('写入准备记录与输入摘要不一致')
        return intent

    @staticmethod
    def _check_optional_intent(db, operation, request_key, request_sha256, required):
        exists = db.execute('SELECT 1 FROM research_write_intents WHERE operation=? AND request_key=?',
                            (operation, request_key)).fetchone()
        if required or exists:
            return ResearchDecisions._require_intent(db, operation, request_key, request_sha256)
        return None

    @staticmethod
    def _slot(db, operation, object_kind, object_key):
        return db.execute('SELECT request_key,slot_version FROM research_write_slots '
                          'WHERE operation=? AND object_kind=? AND object_key=?',
                          (operation, object_kind, object_key)).fetchone()

    @staticmethod
    def _claim_slot(db, intent):
        """Claim a legacy unresolved intent or verify the current request owns its slot."""
        slot = ResearchDecisions._slot(db, intent['operation'], intent['object_kind'], intent['object_key'])
        if slot:
            if slot[0] != intent['request_key']:
                raise ObjectWritePending('该研究对象已有另一项待确认写入，请先核对当前请求')
            rows = db.execute('SELECT request_key,request_sha256 FROM research_write_intents '
                              'WHERE operation=? AND object_kind=? AND object_key=? AND request_key<>?',
                              (intent['operation'], intent['object_kind'], intent['object_key'],
                               intent['request_key'])).fetchall()
            for key, digest in rows:
                if ResearchDecisions._write_state(db, intent['operation'], key, digest)['status'] == 'unknown':
                    raise ObjectWritePending('该研究对象存在未协调的旧请求，请逐项查询或封止后再继续')
            return slot[1]
        rows = db.execute('SELECT request_key,request_sha256 FROM research_write_intents '
                          'WHERE operation=? AND object_kind=? AND object_key=?',
                          (intent['operation'], intent['object_kind'], intent['object_key'])).fetchall()
        unresolved = []
        for key, digest in rows:
            state = ResearchDecisions._write_state(db, intent['operation'], key, digest)
            if state['status'] == 'unknown':
                unresolved.append(key)
        if unresolved and unresolved != [intent['request_key']]:
            raise ObjectWritePending('该研究对象存在未协调的旧请求，请逐项核对或封止后再继续')
        db.execute('INSERT INTO research_write_slots VALUES (?,?,?,?,?)',
                   (intent['operation'], intent['object_kind'], intent['object_key'], intent['request_key'], 1))
        return 1

    def prepare_write(self, operation, request_key, request, *, retry_of=None):
        request_key = _id(request_key)
        if operation not in ('decision', 'draft') or not isinstance(request, dict):
            raise DecisionError('不支持的写入类型或请求内容')
        names = ({'kind', 'key', 'choice', 'reason', 'limitations', 'evidence_sha256'} if operation == 'decision'
                 else {'decision_key', 'hypothesis'})
        if set(request) != names:
            raise DecisionError('写入准备字段不完整或包含多余字段')
        normalized = _decision_request(**request) if operation == 'decision' else _draft_request(**request)
        digest = _sha(normalized)
        kind, key = ((normalized['kind'], normalized['key']) if operation == 'decision'
                     else ('decision', normalized['decision_key']))
        if retry_of is not None:
            retry_of = _id(retry_of)
            if retry_of == request_key:
                raise DecisionError('重试不能关联自身')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE'); require_execution_allowed(db)
            existing = db.execute('SELECT 1 FROM research_write_intents WHERE operation=? AND request_key=?',
                                  (operation, request_key)).fetchone()
            if existing:
                intent = self._require_intent(db, operation, request_key, digest)
                if (intent['object_kind'], intent['object_key'], intent['retry_of']) != (kind, key, retry_of):
                    raise DecisionError('请求标识已用于其他写入准备')
                state = self._write_state(db, operation, request_key, digest)['status']
                slot = self._slot(db, operation, kind, key)
                acknowledged = db.execute('SELECT 1 FROM research_write_acknowledgments '
                                          'WHERE operation=? AND request_key=?', (operation, request_key)).fetchone()
                if not acknowledged and (not slot or slot[0] == request_key):
                    version = self._claim_slot(db, intent)
                    slot = (request_key, version)
                version = slot[1] if slot and slot[0] == request_key else None
                return {**intent, 'status': state, 'slot_version': version}
            if self._write_state(db, operation, request_key, digest)['status'] != 'unknown':
                raise DecisionError('请求标识已有结果或封止，不能重新准备')
            slot = self._slot(db, operation, kind, key)
            if retry_of:
                old = self._intent(db, operation, retry_of)
                if ((old['object_kind'], old['object_key']) != (kind, key)
                        or self._write_state(db, operation, retry_of, old['request_sha256'])['status'] != 'fenced'):
                    raise DecisionError('只能重试同一对象且已封止的请求')
                if db.execute('SELECT 1 FROM research_write_intents WHERE operation=? AND retry_of=?',
                              (operation, retry_of)).fetchone():
                    raise DecisionError('原请求已有重试，请先核对该重试')
                if not slot or slot[0] != retry_of:
                    raise DecisionError('原封止请求已确认或研究对象占用已变化，不能再关联重试')
            elif slot:
                raise ObjectWritePending('该研究对象已有待确认写入，请先核对当前请求')
            else:
                # Do not let a new client key race a prepared request created before
                # object slots were introduced. The holder must first be queried or fenced.
                old_rows = db.execute('SELECT request_key,request_sha256 FROM research_write_intents '
                                      'WHERE operation=? AND object_kind=? AND object_key=?',
                                      (operation, kind, key)).fetchall()
                if any(self._write_state(db, operation, old_key, old_digest)['status'] == 'unknown'
                       for old_key, old_digest in old_rows):
                    raise DecisionError('该研究对象存在未协调的旧请求，请先查询或封止原请求')
            created_at = self._now()
            db.execute('INSERT INTO research_write_intents VALUES (?,?,?,?,?,?,?)',
                       (operation, request_key, digest, kind, key, retry_of, created_at))
            if retry_of:
                db.execute('UPDATE research_write_slots SET request_key=?,slot_version=slot_version+1 '
                           'WHERE operation=? AND object_kind=? AND object_key=? AND request_key=?',
                           (request_key, operation, kind, key, retry_of))
                moved = db.execute('SELECT slot_version FROM research_write_slots '
                                   'WHERE operation=? AND object_kind=? AND object_key=? AND request_key=?',
                                   (operation, kind, key, request_key)).fetchone()
                if not moved:
                    raise DecisionError('原封止请求占用转移失败')
                slot_version = moved[0]
            else:
                db.execute('INSERT INTO research_write_slots VALUES (?,?,?,?,?)',
                           (operation, kind, key, request_key, 1))
                slot_version = 1
            return {'operation': operation, 'request_key': request_key, 'request_sha256': digest,
                    'object_kind': kind, 'object_key': key, 'retry_of': retry_of,
                    'created_at': created_at, 'status': 'unknown', 'slot_version': slot_version}

    def write_status(self, operation, request_key, request_sha256, *, require_intent=False):
        operation, request_key, request_sha256 = self._write_input(operation, request_key, request_sha256)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN')
            intent = self._check_optional_intent(db, operation, request_key, request_sha256, require_intent)
            state = self._write_state(db, operation, request_key, request_sha256)
            if intent:
                slot = self._slot(db, operation, intent['object_kind'], intent['object_key'])
                ack = db.execute('SELECT result_status,slot_version FROM research_write_acknowledgments '
                                 'WHERE operation=? AND request_key=?', (operation, request_key)).fetchone()
                state['slot_version'] = slot[1] if slot and slot[0] == request_key else (ack[1] if ack else None)
                state['acknowledged'] = ack is not None
            return state

    def fence_write(self, operation, request_key, request_sha256, *, require_intent=False):
        operation, request_key, request_sha256 = self._write_input(operation, request_key, request_sha256)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE'); require_execution_allowed(db)
            intent = self._check_optional_intent(db, operation, request_key, request_sha256, require_intent)
            current = self._write_state(db, operation, request_key, request_sha256)
            if current['status'] != 'unknown':
                if intent:
                    slot = self._slot(db, operation, intent['object_kind'], intent['object_key'])
                    current['slot_version'] = slot[1] if slot and slot[0] == request_key else None
                return current
            if intent:
                try:
                    self._claim_slot(db, intent)
                except ObjectWritePending:
                    # Fencing an identified request is safe even if another request
                    # owns the slot: the fence only rejects this key's late commit
                    # and never changes or releases the current owner's slot.
                    pass
            created_at = self._now()
            db.execute('INSERT INTO research_write_fences VALUES (?,?,?,?)',
                       (operation, request_key, request_sha256, created_at))
            slot = (self._slot(db, operation, intent['object_kind'], intent['object_key'])
                    if intent else None)
            return {'status': 'fenced', 'operation': operation, 'request_key': request_key,
                    'request_sha256': request_sha256, 'fenced_at': created_at,
                    **({'slot_version': slot[1] if slot and slot[0] == request_key else None} if intent else {})}

    def current_write(self, operation, object_kind, object_key):
        if (operation not in ('decision', 'draft') or
                (operation == 'decision' and object_kind not in ('series', 'fixed_evaluation')) or
                (operation == 'draft' and object_kind != 'decision')):
            raise DecisionError('研究对象类型无效')
        object_key = _id(object_key)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN')
            slot = self._slot(db, operation, object_kind, object_key)
            rows = db.execute('SELECT request_key,request_sha256,retry_of,created_at FROM research_write_intents '
                              'WHERE operation=? AND object_kind=? AND object_key=? ORDER BY created_at DESC,request_key',
                              (operation, object_kind, object_key)).fetchall()
            pending = []
            for key, digest, retry_of, created_at in rows:
                state = self._write_state(db, operation, key, digest)
                if state['status'] == 'unknown':
                    pending.append({'request_key': key, 'request_sha256': digest,
                                    'retry_of': retry_of, 'created_at': created_at,
                                    'status': 'unknown',
                                    **({'slot_version': slot[1]} if slot and slot[0] == key else {})})
            slot_conflict = bool(slot and any(item['request_key'] != slot[0] for item in pending))
            if slot_conflict:
                holder = next((item for item in rows if item[0] == slot[0]), None)
                if holder and not any(item['request_key'] == slot[0] for item in pending):
                    holder_state = self._write_state(db, operation, holder[0], holder[1])
                    pending.insert(0, {'request_key': holder[0], 'request_sha256': holder[1],
                                       'retry_of': holder[2], 'created_at': holder[3],
                                       'status': holder_state['status'], 'slot_version': slot[1]})
            if pending and (not slot or slot_conflict):
                return {'status': 'recovery_required', 'operation': operation, 'object_kind': object_kind,
                        'object_key': object_key, 'pending': pending}
            if slot:
                request_key, version = slot
                intent = self._intent(db, operation, request_key)
                state = self._write_state(db, operation, request_key, intent['request_sha256'])
                return {'status': state['status'], 'operation': operation, 'object_kind': object_kind,
                        'object_key': object_key, 'request_key': request_key,
                        'request_sha256': intent['request_sha256'], 'retry_of': intent['retry_of'],
                        'created_at': intent['created_at'], 'slot_version': version}
            return {'status': 'none', 'operation': operation, 'object_kind': object_kind,
                    'object_key': object_key}

    def acknowledge_write(self, operation, request_key, request_sha256, slot_version):
        operation, request_key, request_sha256 = self._write_input(operation, request_key, request_sha256)
        if type(slot_version) is not int or slot_version < 1:
            raise DecisionError('写入占用版本无效')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE'); require_execution_allowed(db)
            intent = self._require_intent(db, operation, request_key, request_sha256)
            prior = db.execute('SELECT request_sha256,result_status,slot_version,acknowledged_at '
                               'FROM research_write_acknowledgments WHERE operation=? AND request_key=?',
                               (operation, request_key)).fetchone()
            if prior:
                if prior[0] != request_sha256 or prior[2] != slot_version:
                    raise DecisionError('该写入已按不同占用版本确认')
                return {'status': 'acknowledged', 'operation': operation, 'request_key': request_key,
                        'request_sha256': request_sha256, 'result_status': prior[1],
                        'slot_version': prior[2], 'acknowledged_at': prior[3]}
            state = self._write_state(db, operation, request_key, request_sha256)
            if state['status'] not in ('committed', 'fenced'):
                raise DecisionError('只有已提交或已封止的结果可以确认')
            self._claim_slot(db, intent)
            slot = self._slot(db, operation, intent['object_kind'], intent['object_key'])
            if not slot or slot != (request_key, slot_version):
                raise DecisionError('当前对象占用已变化，不能确认或释放它')
            acknowledged_at = self._now()
            db.execute('INSERT INTO research_write_acknowledgments VALUES (?,?,?,?,?,?)',
                       (operation, request_key, request_sha256, state['status'], slot_version, acknowledged_at))
            changed = db.execute('DELETE FROM research_write_slots WHERE operation=? AND object_kind=? '
                                 'AND object_key=? AND request_key=? AND slot_version=?',
                                 (operation, intent['object_kind'], intent['object_key'], request_key, slot_version)).rowcount
            if changed != 1:
                raise DecisionError('当前对象占用已变化，不能释放它')
            return {'status': 'acknowledged', 'operation': operation, 'request_key': request_key,
                    'request_sha256': request_sha256, 'result_status': state['status'],
                    'slot_version': slot_version, 'acknowledged_at': acknowledged_at}

    def list(self, kind=None, key=None):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute('SELECT request_key FROM research_decisions ORDER BY rowid DESC').fetchall()
            decisions = []
            for (ident,) in rows:
                record, digest = _record(db, 'research_decisions', ident)
                if kind is not None and (record['request']['kind'], record['request']['key']) != (kind, key):
                    continue
                try:
                    current = self._context(db, record['request']['kind'], record['request']['key'])
                    status = 'current' if _sha(current) == record['evidence_sha256'] else 'changed'
                except (ValueError, KeyError, TypeError, sqlite3.Error):
                    status = 'unavailable'
                drafts = []
                for (draft_key,) in db.execute('SELECT request_key FROM research_hypothesis_drafts WHERE decision_key=? ORDER BY rowid DESC', (ident,)):
                    drafts.append(load_draft(db, draft_key))
                decisions.append({'record': record, 'record_sha256': digest, 'evidence_status': status, 'drafts': drafts})
                if len(decisions) >= 100:
                    break
            return decisions

    def drafts(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            keys = db.execute('SELECT request_key FROM research_hypothesis_drafts ORDER BY rowid DESC LIMIT 100').fetchall()
            results, statuses = [], {}
            for (key,) in keys:
                draft = load_draft(db, key)
                parent_key = draft['decision_key']
                if parent_key not in statuses:
                    parent, _ = _record(db, 'research_decisions', parent_key)
                    try:
                        current = self._context(db, parent['request']['kind'], parent['request']['key'])
                        statuses[parent_key] = 'current' if _sha(current) == parent['evidence_sha256'] else 'changed'
                    except (ValueError, KeyError, TypeError, sqlite3.Error):
                        statuses[parent_key] = 'unavailable'
                results.append({**draft, 'parent_evidence_status': statuses[parent_key]})
            return results
