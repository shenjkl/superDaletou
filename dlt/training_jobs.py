"""Cancellable, persistent local training jobs with atomic artifact publication."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading

from dlt.model_repository import ModelRepository, _assert_consistent_history
from dlt.models import (ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION,
                        PREPROCESSING_VERSION, TrainingError, _canonical)
from dlt.snapshots import SnapshotStore
from dlt.training_boundary import (check_holdout_fields, check_holdout_boundary, check_rolling_fields,
                                   check_rolling_target, TrainingBoundaryError)
from dlt.holdout_reservation import check_exploration_allowed
from dlt.recovery_guard import require_execution_allowed, require_path_execution_allowed
from dlt.task_cancellations import (initialize as initialize_cancellations,
                                    lookup as lookup_cancellation, save as save_cancellation)
from dlt.training_limits import DEFAULT_TRAINING_LIMITS


class JobError(ValueError):
    pass


class TrainingJobs:
    def __init__(self, path, *, worker_command=None):
        self.path = Path(path)
        self.worker_command = worker_command or [sys.executable, '-m', 'dlt.training_worker']
        self.processes = {}
        self.lock = threading.Lock()
        self.threads = {}

    def initialize(self):
        ModelRepository(self.path)._initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS training_jobs (
                    request_key TEXT PRIMARY KEY, request_json BLOB NOT NULL, name TEXT NOT NULL,
                    status TEXT NOT NULL, created_at TEXT NOT NULL, finished_at TEXT,
                    artifact_id TEXT REFERENCES model_artifacts(artifact_id), error TEXT
                );
            ''')
            initialize_cancellations(db)

    def recover_interrupted(self):
        """Call only after acquiring the application's singleton lock."""
        require_path_execution_allowed(self.path)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            db.execute("UPDATE training_jobs SET status='failed', finished_at=?, error=? WHERE status='running'",
                       (datetime.now(timezone.utc).isoformat(), '服务中断，训练任务未发布'))

    @staticmethod
    def _validate(name, config, first_issue, timeout, trial_group_id=None, retry_of=None):
        if not isinstance(name, str) or not name.strip() or len(name) > 60:
            raise JobError('任务名称须为 1 至 60 个字符')
        if not isinstance(config, dict) or config.get('availability_mode') != 'historical_reconstruction':
            raise JobError('必须选择历史重建模式')
        required = {'method', 'window', 'train_end', 'cutoff', 'availability_mode', 'snapshot_id',
                    'seed', 'algorithm_version', 'feature_schema_version',
                    'preprocessing_version', 'parameters'}
        allowed = required | {'holdout_start', 'holdout_end', 'rolling_issue'}
        if not required.issubset(config) or set(config) - allowed:
            raise JobError('训练配置字段缺失或包含未支持字段')
        try:
            check_holdout_fields(config)
            check_rolling_fields(config)
        except TrainingBoundaryError as exc:
            raise JobError(str(exc)) from exc
        limits = DEFAULT_TRAINING_LIMITS
        if type(timeout) is not int or not limits.timeout_seconds[0] <= timeout <= limits.timeout_seconds[1]:
            raise JobError(f'训练时间上限须为 {limits.timeout_seconds[0]} 至 {limits.timeout_seconds[1]} 秒')
        if not isinstance(first_issue, str) or not first_issue.isdigit() or len(first_issue) != 5:
            raise JobError('历史起始期格式无效')
        if (type(config.get('window')) is not int
                or not limits.window_periods[0] <= config['window'] <= limits.window_periods[1]):
            raise JobError(f'模型窗口须为 {limits.window_periods[0]} 至 {limits.window_periods[1]} 期')
        if type(config.get('seed')) is not int or not 0 <= config['seed'] <= 4294967295:
            raise JobError('随机种子须为 0 至 4294967295 的整数')
        params = config.get('parameters')
        if not isinstance(params, dict):
            raise JobError('模型参数缺失')
        if config.get('method') == 'tree':
            preprocessing_version = PREPROCESSING_VERSION['tree']
            parameter_fields = {'max_depth', 'min_leaf'}
            if set(params) != parameter_fields:
                raise JobError('树模型参数字段缺失或不受支持')
            if (type(params.get('max_depth')) is not int
                    or not limits.tree_max_depth[0] <= params['max_depth'] <= limits.tree_max_depth[1]
                    or type(params.get('min_leaf')) is not int
                    or not limits.tree_min_leaf[0] <= params['min_leaf'] <= limits.tree_min_leaf[1]):
                raise JobError('树模型参数超出支持范围')
        elif config.get('method') == 'lstm':
            preprocessing_version = PREPROCESSING_VERSION['lstm']
            parameter_fields = {'hidden_size', 'epochs', 'learning_rate'}
            if set(params) != parameter_fields:
                raise JobError('LSTM 参数字段缺失或不受支持')
            if (type(params.get('hidden_size')) is not int
                    or not limits.lstm_hidden_size[0] <= params['hidden_size'] <= limits.lstm_hidden_size[1]
                    or type(params.get('epochs')) is not int
                    or not limits.lstm_epochs[0] <= params['epochs'] <= limits.lstm_epochs[1]
                    or type(params.get('learning_rate')) not in (int, float)
                    or not limits.lstm_learning_rate[0] <= params['learning_rate'] <= limits.lstm_learning_rate[1]):
                raise JobError('LSTM 参数超出支持范围')
        else:
            raise JobError('只支持树模型和 LSTM')
        if (config.get('feature_schema_version') != FEATURE_SCHEMA_VERSION
                or config.get('algorithm_version') != ALGORITHM_VERSION[config['method']]
                or config.get('preprocessing_version') != preprocessing_version):
            raise JobError('算法、特征结构或预处理版本缺失或不受支持')
        try:
            request = {'name': name, 'config': config, 'first_issue': first_issue, 'timeout': timeout}
            if trial_group_id is not None:
                if (not isinstance(trial_group_id, str) or not trial_group_id.strip()
                        or len(trial_group_id) > 100):
                    raise JobError('训练试验组标识须为 1 至 100 个字符')
                request['trial_group_id'] = trial_group_id.strip()
            if retry_of is not None:
                if not isinstance(retry_of, str) or not retry_of.strip() or len(retry_of) > 128:
                    raise JobError('原训练任务标识无效')
                request['retry_of'] = retry_of.strip()
            return json.loads(_canonical(request))
        except JobError:
            raise
        except (ValueError, TypeError) as exc:
            raise JobError('训练配置必须是有效的有限数值') from exc

    def submit(self, request_key, *, name, config, first_issue, timeout, transaction_guard=None,
               trial_group_id=None, retry_of=None):
        require_path_execution_allowed(self.path)
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
            raise JobError('请求标识无效')
        data = self._validate(name, config, first_issue, timeout,
                              trial_group_id=trial_group_id, retry_of=retry_of)
        retry_of = data.get('retry_of')
        payload = _canonical(data)
        self.initialize()
        rows = SnapshotStore(self.path).get_draws(config.get('snapshot_id'))
        issues = [row['issue'] for row in rows]
        if first_issue not in issues or config.get('cutoff') not in issues or config.get('train_end') not in issues:
            raise JobError('起始期、训练截止期及验证切分必须存在于快照')
        start, cutoff, split = issues.index(first_issue), issues.index(config['cutoff']), issues.index(config['train_end'])
        limits = DEFAULT_TRAINING_LIMITS
        if (not start <= split < cutoff
                or not limits.history_rows[0] <= cutoff-start+1 <= limits.history_rows[1]
                or split-start < config['window']):
            raise JobError('历史窗口或训练与验证切分无效')
        try:
            check_holdout_boundary(issues, first_issue, config)
        except TrainingBoundaryError as exc:
            raise JobError(str(exc)) from exc
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            current_issues = [r[0] for r in db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue', (config['snapshot_id'],))]
            try:
                check_holdout_boundary(current_issues, first_issue, config)
            except TrainingBoundaryError as exc:
                raise JobError(str(exc)) from exc
            existing = db.execute('SELECT request_json FROM training_jobs WHERE request_key=?', (request_key,)).fetchone()
            if existing:
                if existing[0] != payload:
                    raise JobError('请求标识已用于不同配置')
                return self.get(request_key)
            if retry_of:
                original = db.execute('SELECT request_json,status FROM training_jobs WHERE request_key=?',
                                      (retry_of,)).fetchone()
                if not original:
                    raise JobError('关联的原训练任务不存在')
                if original[1] not in ('failed', 'cancelled'):
                    raise JobError('只有失败或已取消的训练任务可以关联重试')
                original_request = json.loads(original[0])
                retry_request = dict(data)
                retry_request.pop('retry_of', None)
                original_request.pop('retry_of', None)
                if retry_request != original_request:
                    raise JobError('关联重试必须沿用原训练任务的完整配置')
            try:
                check_rolling_target(db, config)
            except TrainingBoundaryError as exc:
                raise JobError(str(exc)) from exc
            check_exploration_allowed(db, set(current_issues[start:cutoff+1]) | {config.get('rolling_issue')})
            if db.execute("SELECT 1 FROM training_jobs WHERE status='running' LIMIT 1").fetchone():
                raise JobError('已有训练任务运行，请等待或取消')
            if transaction_guard is not None:
                transaction_guard(db)
            try:
                _assert_consistent_history(db, set(issues[start:cutoff+1]))
            except TrainingError as exc:
                raise JobError(str(exc)) from exc
            db.execute("INSERT INTO training_jobs VALUES (?, ?, ?, 'running', ?, NULL, NULL, NULL)",
                       (request_key, payload, name, datetime.now(timezone.utc).isoformat()))
        thread = threading.Thread(target=self._execute, args=(request_key, data), daemon=True)
        with self.lock:
            self.threads[request_key] = thread
            try:
                thread.start()
            except Exception as exc:
                self.threads.pop(request_key, None)
                with closing(sqlite3.connect(self.path)) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    db.execute("UPDATE training_jobs SET status='failed', error=?, finished_at=? "
                               "WHERE request_key=? AND status='running'",
                               ('后台训练线程无法启动：' + str(exc)[-1500:], datetime.now(timezone.utc).isoformat(),
                                request_key))
        return self.get(request_key)

    def _execute(self, key, data):
        process = None
        try:
            if self.get(key)['status'] != 'running':
                return
            process = subprocess.Popen(self.worker_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                                       env={**os.environ, 'PYTHONIOENCODING': 'utf-8'})
            with self.lock:
                self.processes[key] = process
                if self.get(key)['status'] != 'running':
                    process.kill()
            request = _canonical({'path': str(self.path), 'first_issue': data['first_issue'], 'config': data['config']})
            try:
                out, err = process.communicate(input=request, timeout=data['timeout'])
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise JobError('训练超过时间上限')
            if process.returncode != 0:
                raise JobError(err.decode('utf-8', 'replace')[-2000:] or '训练进程失败')
            artifact = json.loads(out)
            content = dict(artifact); ident = content.pop('artifact_sha256', None)
            if (not isinstance(ident, str) or ident != hashlib.sha256(_canonical(content)).hexdigest()
                    or artifact.get('config') != data['config'] or artifact.get('history_first_issue') != data['first_issue']
                    or artifact.get('history_last_issue') != data['config']['cutoff']):
                raise JobError('模型产物完整性或配置不匹配')
            rows = SnapshotStore(self.path).get_draws(data['config']['snapshot_id'])
            issues = [r['issue'] for r in rows]
            start, end = issues.index(data['first_issue']), issues.index(data['config']['cutoff'])
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('PRAGMA foreign_keys=ON')
                db.execute('BEGIN IMMEDIATE')
                status = db.execute('SELECT status FROM training_jobs WHERE request_key=?', (key,)).fetchone()
                if not status or status[0] != 'running':
                    return
                current_issues = [r[0] for r in db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue', (data['config']['snapshot_id'],))]
                check_holdout_boundary(current_issues, data['first_issue'], data['config'])
                check_rolling_target(db, data['config'])
                check_exploration_allowed(db, set(current_issues[start:end+1]) | {data['config'].get('rolling_issue')})
                _assert_consistent_history(db, set(issues[start:end+1]))
                previous = db.execute('SELECT artifact_json FROM model_artifacts WHERE artifact_id=?', (ident,)).fetchone()
                payload = _canonical(artifact)
                if previous and previous[0] != payload:
                    raise JobError('模型产物摘要冲突')
                db.execute('INSERT OR IGNORE INTO model_artifacts VALUES (?, ?, ?, ?, ?, ?, ?)',
                           (ident, data['config']['snapshot_id'], data['config']['method'], data['config']['cutoff'],
                            data['config']['availability_mode'], payload, datetime.now(timezone.utc).isoformat()))
                db.execute("UPDATE training_jobs SET status='complete', artifact_id=?, finished_at=? WHERE request_key=?",
                           (ident, datetime.now(timezone.utc).isoformat(), key))
        except Exception as exc:
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute("UPDATE training_jobs SET status='failed', error=?, finished_at=? WHERE request_key=? AND status='running'",
                           (str(exc)[-2000:], datetime.now(timezone.utc).isoformat(), key))
        finally:
            with self.lock:
                self.processes.pop(key, None)
                self.threads.pop(key, None)

    def cancel(self, key, cancel_request_key):
        require_path_execution_allowed(self.path)
        if not isinstance(cancel_request_key, str) or not cancel_request_key.strip() or len(cancel_request_key) > 128:
            raise JobError('取消请求标识无效')
        if not isinstance(key, str) or not key.strip() or len(key) > 128:
            raise JobError('训练任务标识无效')
        self.initialize()
        request = {'target_request_key': key}
        now = datetime.now(timezone.utc).isoformat()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            result = lookup_cancellation(db, cancel_request_key, 'training_cancel', request)
            if result is None:
                row = db.execute('SELECT status FROM training_jobs WHERE request_key=?', (key,)).fetchone()
                if not row:
                    result = {'request_key': cancel_request_key, 'operation': 'training_cancel', 'request': request,
                              'status': 'rejected', 'target_status': None, 'reason': 'target_not_found'}
                else:
                    status = row[0]
                    if status == 'running':
                        db.execute("UPDATE training_jobs SET status='cancelled', finished_at=? WHERE request_key=? AND status='running'",
                                   (now, key))
                        status = db.execute('SELECT status FROM training_jobs WHERE request_key=?', (key,)).fetchone()[0]
                    result = {'request_key': cancel_request_key, 'operation': 'training_cancel', 'request': request,
                              'status': 'accepted', 'target_status': status, 'reason': None}
                save_cancellation(db, cancel_request_key, 'training_cancel', request, result, now)
        if result['status'] == 'accepted' and result['target_status'] == 'cancelled':
            with self.lock:
                proc = self.processes.get(key)
                if proc and proc.poll() is None:
                    try:
                        proc.kill()
                    except OSError:
                        pass
        return result

    def get(self, key):
        with closing(sqlite3.connect(self.path)) as db:
            db.row_factory = sqlite3.Row
            row = db.execute('SELECT * FROM training_jobs WHERE request_key=?', (key,)).fetchone()
        if not row:
            raise KeyError(key)
        data = dict(row); data['request'] = json.loads(data.pop('request_json'))
        return data

    def list(self):
        with closing(sqlite3.connect(self.path)) as db:
            keys = [row[0] for row in db.execute('SELECT request_key FROM training_jobs ORDER BY created_at DESC, request_key DESC')]
        return [self.get(key) for key in keys]

    def stop(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE training_jobs SET status='failed', finished_at=?, error=? WHERE status='running'",
                       (datetime.now(timezone.utc).isoformat(), '服务停止，训练未发布'))
        with self.lock:
            for proc in self.processes.values():
                if proc.poll() is None:
                    proc.kill()
            threads = list(self.threads.values())
        for thread in threads:
            thread.join(timeout=5)
