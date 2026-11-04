"""Cancellable local reconstruction tasks and atomic report publication."""
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

from dlt.backtest import BacktestError, _validate
from dlt.backtest_limits import DEFAULT_BACKTEST_LIMITS
from dlt.backtest_repository import BacktestRepository
from dlt.candidates import _canonical
from dlt.historical_scenarios import backtest_scenarios
from dlt.snapshots import SnapshotStore
from dlt.training_jobs import JobError
from dlt.holdout_reservation import check_backtest_allowed
from dlt.recovery_guard import require_execution_allowed, require_path_execution_allowed
from dlt.task_cancellations import (initialize as initialize_cancellations,
                                    lookup as lookup_cancellation, save as save_cancellation)


def _now():
    return datetime.now(timezone.utc).isoformat()


class BacktestJobs:
    def __init__(self, path, worker_command=None):
        self.path = Path(path)
        self.worker_command = worker_command or [sys.executable, '-m', 'dlt.backtest_worker']
        self.lock = threading.Lock()
        self.processes = {}
        self.threads = {}

    def initialize(self):
        BacktestRepository(self.path)._initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS backtest_jobs (
                request_key TEXT PRIMARY KEY, request_json BLOB NOT NULL, status TEXT NOT NULL,
                created_at TEXT NOT NULL, finished_at TEXT, error TEXT)''')
            initialize_cancellations(db)

    def recover_interrupted(self):
        require_path_execution_allowed(self.path)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            db.execute("UPDATE backtest_jobs SET status='failed', finished_at=?, error=? WHERE status='running'",
                       (_now(), '服务中断，回测报告未发布'))

    def submit(self, key, protocol, timeout, retry_of=None):
        require_path_execution_allowed(self.path)
        if not isinstance(key, str) or not key.strip() or len(key) > 128:
            raise JobError('请求标识无效')
        if retry_of is not None and (not isinstance(retry_of, str) or not retry_of.strip() or len(retry_of) > 128):
            raise JobError('原回测任务标识无效')
        try:
            protocol = _validate(protocol)
        except BacktestError as exc:
            raise JobError(str(exc)) from exc
        limits = DEFAULT_BACKTEST_LIMITS
        if (type(timeout) is not int
                or not limits.timeout_seconds[0] <= timeout <= limits.timeout_seconds[1]):
            raise JobError(f'回测运行时限须为 {limits.timeout_seconds[0]} 至 {limits.timeout_seconds[1]} 秒')
        rows = SnapshotStore(self.path).get_draws(protocol['snapshot_id'])
        all_issues = [row['issue'] for row in rows]
        if any(issue not in all_issues for issue in protocol['targets']):
            raise JobError('目标期不在所选快照内')
        indices = [all_issues.index(issue) for issue in protocol['targets']]
        if indices != sorted(indices):
            raise JobError('目标期应按开奖顺序排列')
        data = {'protocol': protocol, 'timeout': timeout}
        if retry_of is not None:
            data['retry_of'] = retry_of.strip()
        request = _canonical(data)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            existing = db.execute('SELECT request_json FROM backtest_jobs WHERE request_key=?', (key,)).fetchone()
            if existing:
                if existing[0] != request:
                    raise JobError('请求标识已用于不同配置')
                return self.get(key)
            if retry_of:
                original = db.execute('SELECT request_json,status FROM backtest_jobs WHERE request_key=?',
                                      (retry_of,)).fetchone()
                if not original:
                    raise JobError('关联的原回测任务不存在')
                if original[1] not in ('failed', 'cancelled'):
                    raise JobError('只有失败或已取消的回测任务可以关联重试')
                original_request = json.loads(original[0])
                retry_request = dict(data)
                retry_request.pop('retry_of', None)
                original_request.pop('retry_of', None)
                if retry_request != original_request:
                    raise JobError('关联重试必须沿用原回测任务的完整配置')
            check_backtest_allowed(db, protocol)
            if db.execute("SELECT 1 FROM backtest_jobs WHERE status='running' LIMIT 1").fetchone():
                raise JobError('已有回测运行，请等待或取消')
            db.execute("INSERT INTO backtest_jobs VALUES (?, ?, 'running', ?, NULL, NULL)", (key, request, _now()))
        thread = threading.Thread(target=self._execute, args=(key, data), daemon=True)
        with self.lock:
            self.threads[key] = thread
            try:
                thread.start()
            except Exception as exc:
                self.threads.pop(key, None)
                with closing(sqlite3.connect(self.path)) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    db.execute("UPDATE backtest_jobs SET status='failed', error=?, finished_at=? "
                               "WHERE request_key=? AND status='running'",
                               ('后台回测线程无法启动：' + str(exc)[-1500:], _now(), key))
        return self.get(key)

    def _execute(self, key, data):
        try:
            protocol = data['protocol']
            scenarios = backtest_scenarios(self.path, protocol['snapshot_id'], protocol['targets'],
                                           additional=protocol['candidate']['additional'])
            if self.get(key)['status'] != 'running':
                return
            proc = subprocess.Popen(self.worker_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                                    env={**os.environ, 'PYTHONIOENCODING': 'utf-8'})
            with self.lock:
                self.processes[key] = proc
                if self.get(key)['status'] != 'running':
                    proc.kill()
            try:
                out, err = proc.communicate(_canonical({'path': str(self.path), 'protocol': protocol,
                                                        'scenarios': scenarios}), timeout=data['timeout'])
            except subprocess.TimeoutExpired:
                proc.kill(); proc.communicate()
                raise JobError('回测超过时间上限，未发布报告')
            if proc.returncode:
                raise JobError(err.decode('utf-8', 'replace')[-2000:] or '回测进程失败')
            report = json.loads(out)
            if (report.get('protocol') != protocol or report.get('scenarios') != scenarios
                    or report.get('protocol_sha256') != hashlib.sha256(_canonical(protocol)).hexdigest()
                    or report.get('scenarios_sha256') != hashlib.sha256(_canonical(scenarios)).hexdigest()):
                raise JobError('报告配置或摘要不匹配')
            payload = _canonical(report)
            request = _canonical({'protocol': protocol, 'scenarios': scenarios})
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('BEGIN IMMEDIATE')
                status = db.execute('SELECT status FROM backtest_jobs WHERE request_key=?', (key,)).fetchone()
                if not status or status[0] != 'running':
                    return
                check_backtest_allowed(db, protocol)
                # Preserve a report even if dependencies changed; inspect marks it
                # review_required rather than publishing it as research-ready.
                db.execute('INSERT INTO backtest_reports VALUES (?, ?, ?, ?)',
                           (key, request, hashlib.sha256(payload).hexdigest(), payload))
                db.execute("UPDATE backtest_jobs SET status='complete', finished_at=? WHERE request_key=?", (_now(), key))
        except Exception as exc:
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute("UPDATE backtest_jobs SET status='failed', finished_at=?, error=? WHERE request_key=? AND status='running'",
                           (_now(), str(exc)[-2000:], key))
        finally:
            with self.lock:
                self.processes.pop(key, None)
                self.threads.pop(key, None)

    def cancel(self, key, cancel_request_key):
        require_path_execution_allowed(self.path)
        if not isinstance(cancel_request_key, str) or not cancel_request_key.strip() or len(cancel_request_key) > 128:
            raise JobError('取消请求标识无效')
        if not isinstance(key, str) or not key.strip() or len(key) > 128:
            raise JobError('回测任务标识无效')
        self.initialize()
        request = {'target_request_key': key}
        now = _now()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            require_execution_allowed(db)
            result = lookup_cancellation(db, cancel_request_key, 'backtest_cancel', request)
            if result is None:
                row = db.execute('SELECT status FROM backtest_jobs WHERE request_key=?', (key,)).fetchone()
                if not row:
                    result = {'request_key': cancel_request_key, 'operation': 'backtest_cancel', 'request': request,
                              'status': 'rejected', 'target_status': None, 'reason': 'target_not_found'}
                else:
                    status = row[0]
                    if status == 'running':
                        db.execute("UPDATE backtest_jobs SET status='cancelled', finished_at=? WHERE request_key=? AND status='running'",
                                   (now, key))
                        status = db.execute('SELECT status FROM backtest_jobs WHERE request_key=?', (key,)).fetchone()[0]
                    result = {'request_key': cancel_request_key, 'operation': 'backtest_cancel', 'request': request,
                              'status': 'accepted', 'target_status': status, 'reason': None}
                save_cancellation(db, cancel_request_key, 'backtest_cancel', request, result, now)
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
            row = db.execute('SELECT * FROM backtest_jobs WHERE request_key=?', (key,)).fetchone()
        if not row:
            raise KeyError(key)
        value = dict(row)
        value['request'] = json.loads(value.pop('request_json'))
        if value['status'] == 'complete':
            value['result'] = BacktestRepository(self.path).inspect(key)
        return value

    def list(self):
        with closing(sqlite3.connect(self.path)) as db:
            keys = [r[0] for r in db.execute('SELECT request_key FROM backtest_jobs ORDER BY created_at DESC, request_key DESC')]
        return [self.get(key) for key in keys]

    def stop(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE backtest_jobs SET status='failed', finished_at=?, error=? WHERE status='running'",
                       (_now(), '服务停止，回测报告未发布'))
        with self.lock:
            for proc in self.processes.values():
                if proc.poll() is None:
                    proc.kill()
            threads = list(self.threads.values())
        for thread in threads:
            thread.join(timeout=5)
