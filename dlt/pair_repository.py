"""Atomic storage for research draft pairs; no prospective eligibility is claimed."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
from pathlib import Path

from dlt.candidate_service import generate_from_snapshot
from dlt.candidates import CandidateError, _canonical
from dlt.model_repository import ModelRepository, _assert_consistent_history
from dlt.models import TrainingError
from dlt.snapshots import SnapshotStore
from dlt.holdout_reservation import check_exploration_allowed
from dlt.pair_validation import validate_pair_structure, validate_pair_history, validate_random_control
from dlt.model_pair_validation import validate_model_candidate


def _validate_model_selection(db, selection, artifact_id, method, *, strict_ungrouped=False,
                              strict_context=False):
    required_fields = {'considered_attempts', 'selected_attempt', 'reason', 'trial_group_id'}
    allowed_fields = (required_fields, required_fields | {'external_attempts_note'},
                      required_fields | {'version'},
                      required_fields | {'external_attempts_note', 'version'})
    if not isinstance(selection, dict) or set(selection) not in allowed_fields:
        raise CandidateError('模型生成必须保留训练尝试比较清单和选择理由')
    attempts, selected = selection['considered_attempts'], selection['selected_attempt']
    reason, trial_group_id = selection['reason'], selection['trial_group_id']
    external_note = selection.get('external_attempts_note', '')
    version = selection.get('version')
    if (not isinstance(attempts, list) or not attempts or len(attempts) > 1000
            or any(not isinstance(key, str) or not key.strip() or len(key) > 128 for key in attempts)
            or len(set(attempts)) != len(attempts) or selected not in attempts
            or not isinstance(reason, str) or not reason.strip() or len(reason) > 1000
            or not isinstance(external_note, str) or len(external_note) > 2000
            or ('external_attempts_note' in selection and not external_note.strip())
            or (trial_group_id is not None and (not isinstance(trial_group_id, str)
                                                or not trial_group_id.strip() or len(trial_group_id) > 100))
            or (version is not None and version != 'model-selection-v1')):
        raise CandidateError('模型选择记录字段无效')
    strict_ungrouped = strict_ungrouped or version == 'model-selection-v1'
    strict_context = strict_context or version == 'model-selection-v1'
    if strict_ungrouped and trial_group_id is None and len(attempts) > 1:
        raise CandidateError('旧训练尝试没有共同试验组，不能合并比较；请在同一试验组内重新训练后选择')
    rows = {}
    requests = {}
    for key in attempts:
        row = db.execute('SELECT status,artifact_id,request_json FROM training_jobs WHERE request_key=?',
                         (key,)).fetchone()
        if not row:
            raise CandidateError('模型选择引用了不存在的训练尝试')
        try:
            request = json.loads(row[2])
            config = request['config']
        except (TypeError, KeyError, ValueError):
            raise CandidateError('模型选择引用的训练尝试无法核验') from None
        if (config.get('method') != method
                or request.get('trial_group_id') != trial_group_id):
            raise CandidateError('模型选择清单必须来自同一模型方法和试验组')
        rows[key] = row
        requests[key] = request
    chosen = rows[selected]
    if chosen[0] != 'complete' or chosen[1] != artifact_id:
        raise CandidateError('最终选择的训练尝试与候选模型产物不一致')
    if strict_context:
        context_fields = ('snapshot_id', 'availability_mode', 'train_end', 'cutoff',
                          'holdout_start', 'holdout_end', 'rolling_issue',
                          'algorithm_version', 'feature_schema_version', 'preprocessing_version')
        selected_request = requests[selected]
        selected_config = selected_request['config']
        context = (selected_request.get('first_issue'),
                   *(selected_config.get(field) for field in context_fields))
        if any((requests[key].get('first_issue'),
                *(requests[key]['config'].get(field) for field in context_fields)) != context
               for key in attempts):
            raise CandidateError('模型选择清单中的训练尝试必须使用相同快照、训练/验证范围及算法版本')


class PairRepository:
    def __init__(self, path):
        self.path = Path(path)

    def _initialize(self):
        if not self.path.is_file():
            raise CandidateError('stored snapshot not found')
        SnapshotStore(self.path).initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS draft_pairs (
                    request_key TEXT PRIMARY KEY, request_json BLOB NOT NULL,
                    pair_sha256 TEXT NOT NULL, pair_json BLOB NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS draft_pairs_no_update BEFORE UPDATE ON draft_pairs
                    BEGIN SELECT RAISE(ABORT, 'immutable draft pair'); END;
                CREATE TRIGGER IF NOT EXISTS draft_pairs_no_delete BEFORE DELETE ON draft_pairs
                    BEGIN SELECT RAISE(ABORT, 'immutable draft pair'); END;
                CREATE TABLE IF NOT EXISTS model_selection_decisions (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_key TEXT NOT NULL REFERENCES draft_pairs(request_key),
                    previous_seq INTEGER,
                    selection_sha256 TEXT NOT NULL,
                    selection_json BLOB NOT NULL,
                    request_key TEXT UNIQUE,
                    created_at TEXT NOT NULL,
                    UNIQUE(pair_key, seq)
                );
                CREATE TRIGGER IF NOT EXISTS model_selection_decisions_no_update BEFORE UPDATE ON model_selection_decisions
                    BEGIN SELECT RAISE(ABORT, 'immutable model selection decision'); END;
                CREATE TRIGGER IF NOT EXISTS model_selection_decisions_no_delete BEFORE DELETE ON model_selection_decisions
                    BEGIN SELECT RAISE(ABORT, 'immutable model selection decision'); END;
                CREATE TRIGGER IF NOT EXISTS model_selection_decisions_sequence BEFORE INSERT ON model_selection_decisions
                    WHEN NEW.previous_seq IS NOT (SELECT MAX(seq) FROM model_selection_decisions WHERE pair_key=NEW.pair_key)
                    BEGIN SELECT RAISE(ABORT, 'model selection decision sequence conflict'); END;
            ''')
            from dlt.pair_evaluation_writes import initialize as initialize_write_protocol
            initialize_write_protocol(db, pair=True)
            # Preserve the original saved choice as the first immutable history entry.
            rows = db.execute('SELECT request_key,request_json FROM draft_pairs').fetchall()
            for pair_key, request_raw in rows:
                try:
                    request_data = json.loads(request_raw)
                    selection = request_data.get('model_selection') if isinstance(request_data, dict) else None
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                if selection and not db.execute('SELECT 1 FROM model_selection_decisions WHERE pair_key=?', (pair_key,)).fetchone():
                    raw = _canonical(selection)
                    db.execute('INSERT INTO model_selection_decisions '
                               '(pair_key,previous_seq,selection_sha256,selection_json,request_key,created_at) '
                               'VALUES (?,NULL,?,?,NULL,?)',
                               (pair_key, hashlib.sha256(raw).hexdigest(), raw, datetime.now(timezone.utc).isoformat()))

    def selection_history(self, pair_key):
        self._initialize()
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT seq,previous_seq,selection_sha256,selection_json,request_key,created_at '
                              'FROM model_selection_decisions WHERE pair_key=? ORDER BY seq', (pair_key,)).fetchall()
            pair_row = db.execute('SELECT request_json FROM draft_pairs WHERE request_key=?', (pair_key,)).fetchone()
            if not pair_row:
                raise CandidateError('配对草稿不存在')
            pair_request = json.loads(pair_row[0])
            if pair_request.get('model_selection') is not None and not rows:
                raise CandidateError('模型选择决策历史缺少原始选择记录')
            result = []
            previous = None
            for seq, previous_seq, digest, raw, request_key, created_at in rows:
                try:
                    selection = json.loads(raw)
                    if (previous_seq != previous or not isinstance(selection, dict) or _canonical(selection) != raw
                            or hashlib.sha256(raw).hexdigest() != digest):
                        raise ValueError
                    parsed_time = datetime.fromisoformat(created_at)
                    if parsed_time.tzinfo is None or parsed_time.utcoffset() is None:
                        raise ValueError
                    _validate_model_selection(db, selection, pair_request['artifact_id'],
                                              pair_request['config']['method'])
                    if request_key is None:
                        if result or selection != pair_request.get('model_selection'):
                            raise ValueError
                    else:
                        from dlt.pair_evaluation_writes import status_in_transaction
                        receipt = status_in_transaction(db, 'selection_decision', request_key)
                        if receipt['status'] != 'accepted':
                            raise ValueError
                except (TypeError, KeyError, ValueError, json.JSONDecodeError, CandidateError):
                    raise CandidateError('模型选择决策历史完整性校验失败') from None
                result.append({'seq': seq, 'selection': selection, 'selection_sha256': digest,
                               'request_key': request_key, 'created_at': created_at})
                previous = seq
            return result

    def append_selection_decision(self, request_key, pair_key, selection, *, expected_previous_seq,
                                  retry_of=None, write_request=None):
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
            raise CandidateError('模型选择请求标识无效')
        if type(expected_previous_seq) is not int or expected_previous_seq < 0:
            raise CandidateError('模型选择历史序号无效')
        request = {'pair_key': pair_key, 'selection': selection,
                   'expected_previous_seq': expected_previous_seq}
        write_request = write_request or request
        self._initialize()
        try:
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('PRAGMA foreign_keys=ON')
                db.execute('BEGIN IMMEDIATE')
                from dlt.pair_evaluation_writes import record_acceptance
                existing = db.execute('SELECT seq,selection_sha256,selection_json FROM model_selection_decisions '
                                      'WHERE request_key=?', (request_key,)).fetchone()
                if existing:
                    seq, digest, raw = existing
                    result = {'request_key': request_key, 'request': write_request,
                              'decision_seq': seq, 'selection_sha256': digest}
                    record_acceptance(db, 'selection_decision', request_key, write_request, result,
                                      retry_of=retry_of)
                    return result
                row = db.execute('SELECT request_json FROM draft_pairs WHERE request_key=?', (pair_key,)).fetchone()
                if not row:
                    raise CandidateError('配对草稿不存在')
                pair_request = json.loads(row[0])
                artifact_id = pair_request.get('artifact_id')
                if not artifact_id:
                    raise CandidateError('非模型草稿不能追加模型选择记录')
                config = pair_request['config']
                if db.execute('SELECT 1 FROM selection_write_fences WHERE request_key=?', (request_key,)).fetchone():
                    raise CandidateError('模型选择请求已封止；迟到请求拒绝')
                _validate_model_selection(db, selection, artifact_id, config['method'],
                                          strict_ungrouped=True, strict_context=True)
                current = db.execute('SELECT MAX(seq) FROM model_selection_decisions WHERE pair_key=?',
                                     (pair_key,)).fetchone()[0]
                if expected_previous_seq != current:
                    raise CandidateError('模型选择历史已更新，请刷新后再追加')
                raw = _canonical(selection)
                digest = hashlib.sha256(raw).hexdigest()
                cursor = db.execute('INSERT INTO model_selection_decisions '
                                    '(pair_key,previous_seq,selection_sha256,selection_json,request_key,created_at) '
                                    'VALUES (?,?,?,?,?,?)',
                                    (pair_key, current, digest, raw, request_key,
                                     datetime.now(timezone.utc).isoformat()))
                seq = cursor.lastrowid
                result = {'request_key': request_key, 'request': write_request,
                          'decision_seq': seq, 'selection_sha256': digest}
                record_acceptance(db, 'selection_decision', request_key, write_request, result,
                                  retry_of=retry_of)
                return result
        except ValueError as exc:
            raise CandidateError(str(exc)) from exc

    @staticmethod
    def _decode(row):
        pair = json.loads(row[1])
        content = dict(pair)
        digest = content.pop('pair_sha256', None)
        if digest != row[0] or hashlib.sha256(_canonical(content)).hexdigest() != digest:
            raise CandidateError('stored pair integrity check failed')
        return pair

    def get(self, request_key):
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT pair_sha256, pair_json FROM draft_pairs WHERE request_key=?', (request_key,)).fetchone()
        if row is None:
            raise KeyError(request_key)
        return self._decode(row)

    def get_request(self, request_key):
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT request_json FROM draft_pairs WHERE request_key=?', (request_key,)).fetchone()
        if row is None:
            raise KeyError(request_key)
        return json.loads(row[0])

    def list(self, limit=100):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise CandidateError('invalid draft list limit')
        with closing(sqlite3.connect(self.path)) as db:
            keys = [row[0] for row in db.execute('SELECT request_key FROM draft_pairs ORDER BY created_at DESC, request_key DESC LIMIT ?', (limit,))]
        result = []
        for key in keys:
            inspected = self.inspect(key)
            pair = inspected['pair']
            result.append({'request_key': key, 'status': inspected['status'], 'reason': inspected['reason'],
                           'summary': {'pair_sha256': pair['pair_sha256'], 'config': pair['config'],
                                       'model_artifact_id': pair['model_artifact_id'], 'history_first': pair['history_first'],
                                       'history_last': pair['history_last'], 'candidate_count': pair['candidate']['count'],
                                       'cost_fen': pair['candidate']['cost_fen']}})
        return result

    def _checked_pair(self, db, request_key, *, require_replay=False, include_assurance=False):
        """Read a draft and verify dependent histories in the caller's transaction."""
        row = db.execute('SELECT request_json, pair_sha256, pair_json FROM draft_pairs WHERE request_key=?',
                         (request_key,)).fetchone()
        if row is None:
            raise KeyError(request_key)
        request = json.loads(row[0])
        pair = self._decode(row[1:])
        validate_pair_structure(pair, request['config'])
        history = validate_pair_history(db, pair, request)
        replayed = validate_random_control(pair, history)
        relevant = self._history(db, request['config']['snapshot_id'], request['history_first_issue'],
                                 request['history_cutoff_issue'])
        model_replayed = True
        if request['artifact_id']:
            if 'model_selection' in request:
                _validate_model_selection(db, request['model_selection'], request['artifact_id'],
                                          request['config']['method'])
            artifact = ModelRepository.get_in_transaction(db, request['artifact_id'])
            relevant |= self._history(db, artifact['config']['snapshot_id'], artifact['history_first_issue'],
                                      artifact['history_last_issue'])
            model_replayed = validate_model_candidate(db, pair, request, artifact)
        if require_replay and (not replayed or not model_replayed):
            raise CandidateError('原生成或模型代码、运行时与当前环境不同，无法重放候选及随机对照；请重新生成配对')
        try:
            _assert_consistent_history(db, relevant)
        except TrainingError as exc:
            raise CandidateError(str(exc)) from exc
        return (pair, replayed and model_replayed) if include_assurance else pair

    def inspect(self, request_key):
        """Historical drafts remain readable, but conflicting inputs require review."""
        pair = self.get(request_key)
        try:
            with closing(sqlite3.connect(self.path)) as db:
                _, replayed = self._checked_pair(db, request_key, include_assurance=True)
        except (CandidateError, TrainingError, KeyError) as exc:
            return {'pair': pair, 'status': 'review_required', 'reason': str(exc)}
        if not replayed:
            return {'pair': pair, 'status': 'review_required',
                    'reason': '原生成或模型代码、运行时不同，候选及随机对照无法完整重放；不能用于新登记'}
        return {'pair': pair, 'status': 'research_draft', 'reason': 'rule and calendar references not verified'}

    @staticmethod
    def _history(db, snapshot_id, first, last):
        rows = db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date, issue', (snapshot_id,)).fetchall()
        issues = [row[0] for row in rows]
        if first not in issues or last not in issues or issues.index(first) > issues.index(last):
            raise CandidateError('source history unavailable')
        return set(issues[issues.index(first):issues.index(last)+1])

    def save_draft(self, request_key, config, *, history_first_issue, history_cutoff_issue,
                   artifact_id=None, prediction_at=None, target_deadline=None, retry_of=None,
                   write_request=None, selection=None):
        """Save both alternatives as one immutable draft, not a registered experiment.

        Caller-provided rule/calendar references remain unverified. Failed runs
        have no durable task record; this method only deduplicates successful saves.
        """
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
            raise CandidateError('request key must be 1 to 128 characters')
        request_data = {'config': config, 'history_first_issue': history_first_issue,
                              'history_cutoff_issue': history_cutoff_issue, 'artifact_id': artifact_id,
                              'prediction_at': prediction_at, 'target_deadline': target_deadline}
        if selection is not None:
            request_data['model_selection'] = selection
        request = _canonical(request_data)
        self._initialize()
        write_request = write_request if write_request is not None else json.loads(request)
        with closing(sqlite3.connect(self.path)) as db:
            if db.execute('SELECT 1 FROM pair_write_fences WHERE request_key=?', (request_key,)).fetchone():
                raise CandidateError('draft request was fenced; late generation rejected')
            row = db.execute('SELECT request_json, pair_sha256, pair_json FROM draft_pairs WHERE request_key=?', (request_key,)).fetchone()
        if row:
            if row[0] != request:
                raise CandidateError('request key already used for different input')
            try:
                with closing(sqlite3.connect(self.path)) as db, db:
                    db.execute('PRAGMA foreign_keys=ON')
                    db.execute('BEGIN IMMEDIATE')
                    from dlt.pair_evaluation_writes import record_acceptance
                    record_acceptance(db, 'pair_create', request_key, write_request,
                                      {'request_key': request_key, 'request': write_request,
                                       'pair': self._decode(row[1:])}, retry_of=retry_of)
            except ValueError as exc:
                raise CandidateError(str(exc)) from exc
            return self._decode(row[1:])
        pair = generate_from_snapshot(self.path, config, history_first_issue=history_first_issue,
                                      history_cutoff_issue=history_cutoff_issue, artifact_id=artifact_id,
                                      prediction_at=prediction_at, target_deadline=target_deadline)
        model = ModelRepository(self.path).get(artifact_id) if artifact_id else None
        try:
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('PRAGMA foreign_keys=ON')
                db.execute('BEGIN IMMEDIATE')
                if db.execute('SELECT 1 FROM pair_write_fences WHERE request_key=?', (request_key,)).fetchone():
                    raise CandidateError('draft request was fenced; late generation rejected')
                row = db.execute('SELECT request_json, pair_sha256, pair_json FROM draft_pairs WHERE request_key=?', (request_key,)).fetchone()
                if row:
                    if row[0] != request:
                        raise CandidateError('request key already used for different input')
                    from dlt.pair_evaluation_writes import record_acceptance
                    record_acceptance(db, 'pair_create', request_key, write_request,
                                      {'request_key': request_key, 'request': write_request,
                                       'pair': self._decode(row[1:])}, retry_of=retry_of)
                    return self._decode(row[1:])
                relevant = self._history(db, config['snapshot_id'], history_first_issue, history_cutoff_issue)
                if model:
                    _validate_model_selection(db, selection, artifact_id, config['method'],
                                              strict_ungrouped=True, strict_context=True)
                    relevant |= self._history(db, model['config']['snapshot_id'], model['history_first_issue'], model['history_last_issue'])
                elif selection is not None:
                    raise CandidateError('non-model draft cannot include model selection')
                _assert_consistent_history(db, relevant)
                check_exploration_allowed(db, relevant | {config.get('target_issue')})
                validate_pair_structure(pair, config)
                history = validate_pair_history(db, pair, json.loads(request))
                if not validate_random_control(pair, history):
                    raise CandidateError('生成代码或运行时不一致，无法核验随机对照')
                if artifact_id:
                    current_artifact = ModelRepository.get_in_transaction(db, artifact_id)
                    if not validate_model_candidate(db, pair, json.loads(request), current_artifact):
                        raise CandidateError('模型代码或运行时不一致，无法核验模型候选')
                db.execute('INSERT INTO draft_pairs VALUES (?, ?, ?, ?, ?)',
                           (request_key, request, pair['pair_sha256'], _canonical(pair), datetime.now(timezone.utc).isoformat()))
                if selection is not None:
                    selection_raw = _canonical(selection)
                    db.execute('INSERT INTO model_selection_decisions '
                               '(pair_key,previous_seq,selection_sha256,selection_json,request_key,created_at) '
                               'VALUES (?,NULL,?,?,NULL,?)',
                               (request_key, hashlib.sha256(selection_raw).hexdigest(), selection_raw,
                                datetime.now(timezone.utc).isoformat()))
                from dlt.pair_evaluation_writes import record_acceptance
                record_acceptance(db, 'pair_create', request_key, write_request,
                                  {'request_key': request_key, 'request': write_request, 'pair': pair},
                                  retry_of=retry_of)
        except TrainingError as exc:
            raise CandidateError(str(exc)) from exc
        return pair
