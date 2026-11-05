"""Loopback-only browser service for the local research workspace."""
import argparse
import base64
from contextlib import closing
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import mimetypes
import os
from pathlib import Path
import re
import secrets
import sqlite3
import sys
import threading
from urllib.parse import parse_qs, quote, urlsplit

from dlt.snapshots import InvalidSnapshot, SnapshotStore
from dlt.model_repository import ModelRepository
from dlt.pair_repository import PairRepository
from dlt.candidates import CandidateError
from dlt.training_jobs import JobError, TrainingJobs
from dlt.training_limits import DEFAULT_TRAINING_LIMITS
from dlt.backtest_limits import DEFAULT_BACKTEST_LIMITS
from dlt.evaluation_repository import EvaluationRepository
from dlt.evaluation import EvaluationError
from dlt.scenarios import historical_scenario
from dlt.custom_scenarios import custom_scenario
from dlt.backtest_jobs import BacktestJobs
from dlt.scheduler import ScheduleError, ScheduleFenced, Scheduler
from dlt.prospective import ProspectiveRepository, RegistrationError
from dlt.prospective_settlement import PaperResultError, PaperResults
from dlt.integrity import check_database
from dlt.recovery import (RecoveryError, prepare_runtime_database_path,
                          reject_database_reparse_path,
                          verify_runtime_database_permissions)
from dlt.prospective_series import SeriesRepository
from dlt.prospective_closure_writes import (ClosureFenced, digest as _closure_write_digest,
                                            fence as fence_closure_write, status as closure_write_status)
from dlt.source_write_protocol import (SourceWriteFenced, digest as _source_write_digest,
                                       accept as accept_source_write, fence as fence_source_write,
                                       status as source_write_status)
from dlt.holdout_audit import audit_holdout
from dlt.holdout_reservation import (ReservationError, ReservationFenced, fence_range, list_reservations,
                                    initialize as initialize_holdout_reservations,
                                    reservation_write_status, reserve_range_write, reserved_issues)
from dlt.fixed_evaluation import FixedEvaluationFenced, FixedEvaluations, _write_request
from dlt.research_decisions import ObjectWritePending, ResearchDecisions, StaleEvidence
from dlt.generation_replay import build_generation_replay
from dlt.paper_replay import build_paper_replay
from dlt.source_preview import SourcePreviewError, preview_source
from dlt.candidates import _canonical
from dlt.rolling_training import RollingTraining, RollingPlanFenced
from dlt.rolling_registration import RollingRegistration, RollingRegistrationFenced
from dlt.static_registration import StaticRegistration, StaticRegistrationFenced
from dlt.autostart import status as autostart_status
from dlt.task_cancellations import get as get_task_cancellation
from dlt.custom_origin import OriginFenced


MAX_POST_BODY_BYTES = 1024 * 1024
MAX_SOURCE_REVIEW_BODY_BYTES = 8 * 1024 * 1024


ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT/'prototype'
ALLOWED_FILES = {'index.html', 'styles.css', 'data.js', 'calc-engine.js', 'candidate-engine.js',
                 'research-write-journal.js', 'fixed-experiment-journal.js', 'task-write-journal.js',
                 'update-write-journal.js', 'declaration-write-journal.js',
                 'holdout-write-journal.js', 'fixed-evaluation-write-journal.js',
                 'prospective-closure-write-journal.js', 'source-write-journal.js',
                 'paper-write-journal.js', 'pair-evaluation-journal.js', 'app.js'}


def _pair_wire(inspected):
    """Preserve unsigned 64-bit seeds exactly across JavaScript's number limit."""
    result = deepcopy(inspected)
    for label in ('candidate', 'control'):
        result['pair'][label]['seed'] = str(result['pair'][label]['seed'])
    result['seed_encoding'] = 'decimal_string_uint64'
    result['hash_basis'] = 'canonical stored JSON with integer seeds; wire representation uses decimal strings'
    return result


def _evaluation_wire(value):
    result = deepcopy(value)
    if result and isinstance(result.get('result'), dict):
        portfolios = result['result'].get('portfolios')
        for portfolio in portfolios.values() if isinstance(portfolios, dict) else ():
            mean = portfolio.get('mean_gross_tax_before', {}) if isinstance(portfolio, dict) else {}
            if not isinstance(mean, dict):
                continue
            exact = mean.get('fraction')
            if isinstance(exact, list):
                mean['fraction'] = [str(x) for x in exact]
    return result


def _masked_bundled_research_data(source, hidden_issues):
    """Filter reserved draw outcomes from the browser's bundled data payload."""
    prefix, separator, raw = source.partition('=')
    if not separator or prefix.strip() != 'window.RESEARCH_DATA':
        raise ValueError('静态研究数据格式无效')
    payload = json.loads(raw.strip().rstrip(';'))
    if not isinstance(payload, dict):
        raise ValueError('静态研究数据结构无效')
    for row in payload.get('draws', []):
        if isinstance(row, dict) and row.get('issue') in hidden_issues:
            row['front'] = []
            row['back'] = []
            row['numbers_hidden'] = True
    history = payload.get('generationHistory', [])
    if isinstance(history, list):
        overlaps_history = any(isinstance(row, dict) and row.get('issue') in hidden_issues for row in history)
        payload['generationHistory'] = [row for row in history
                                        if not isinstance(row, dict) or row.get('issue') not in hidden_issues]
        historical_issues = payload.get('historicalIssues')
        overlaps_static_evaluation = (any(issue in hidden_issues for issue in historical_issues)
                                      if isinstance(historical_issues, list) else overlaps_history)
        if overlaps_static_evaluation:
            # These bundled aggregate results cannot be recomputed by issue, so
            # do not present them as usable evidence while their range is held out.
            payload['historical'] = None
            payload['historical_hidden_due_to_reservation'] = True
        history_bytes = json.dumps(payload['generationHistory'], sort_keys=True,
                                   separators=(',', ':')).encode('utf-8')
        payload['generationHistorySha256'] = hashlib.sha256(history_bytes).hexdigest()
    return (prefix + ' = ' + json.dumps(payload, ensure_ascii=False, separators=(',', ':')) + ';').encode('utf-8')


def _safe_integers(value):
    """Keep report integers exact when JSON crosses JavaScript's number limit."""
    if type(value) is int and abs(value) > 9007199254740991:
        return str(value)
    if isinstance(value, dict):
        return {key: _safe_integers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_safe_integers(item) for item in value]
    return value


def _fixed_evaluation_write_digest(operation, request):
    normalized = _write_request(operation, request)
    return hashlib.sha256(_canonical(normalized)).hexdigest()


def _series_manifest_wire(manifest):
    """Accept exact decimal seeds without passing through JS floating point."""
    result = deepcopy(manifest)
    if not isinstance(result, dict) or not isinstance(result.get('seeds'), dict):
        raise ValueError('需要完整的奖期与副本种子清单')
    for issue, replicas in result['seeds'].items():
        if not isinstance(replicas, dict):
            raise ValueError('副本种子清单无效')
        for replica, seed in replicas.items():
            if isinstance(seed, str) and seed.isascii() and seed.isdigit() and len(seed) <= 20:
                replicas[replica] = int(seed)
            elif type(seed) is not int or seed > 9007199254740991:
                raise ValueError('大整数种子必须使用十进制字符串')
    return result


class SingleInstance:
    def __init__(self, path):
        self.path = Path(str(path)+'.app.lock')
        self.file = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(self.path, 'a+b')
        self.file.seek(0); self.file.write(b'1'); self.file.flush(); self.file.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise RuntimeError('该数据库已有本机服务运行')
        return self

    def __exit__(self, *args):
        if os.name == 'nt':
            import msvcrt
            self.file.seek(0); msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
        self.file.close()


class UpdateLoop:
    """Poll enabled plans while the local service runs, independent of the browser."""
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.stop_event = threading.Event()
        self.thread = None
        self.state = 'not_started'
        self.last_check = None
        self.error = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        thread = threading.Thread(target=self._run, daemon=True)
        self.thread = thread
        try:
            thread.start()
        except Exception as exc:
            self.thread = None
            self.state = 'failed'
            self.error = '后台更新线程无法启动：' + str(exc)[:500]

    def _run(self):
        while not self.stop_event.is_set():
            try:
                # The CLI worker shares this OS lock. Lease fencing remains the
                # second line of defence against a crashed prior execution.
                with SingleInstance(str(self.scheduler.path)+'.update'):
                    self.state = 'running'
                    while not self.stop_event.is_set():
                        try:
                            self.scheduler.run_due()
                            self.error = None
                        except Exception as exc:
                            self.error = str(exc)[:500]
                        self.last_check = datetime.now(timezone.utc).isoformat()
                        self.stop_event.wait(1)
            except RuntimeError:
                self.state = 'occupied'
                self.error = '另一个更新进程持有执行锁，等待其退出'
                self.stop_event.wait(5)
            except Exception as exc:
                self.state = 'failed'
                self.error = str(exc)[:500]
                self.stop_event.wait(5)
        self.state = 'stopped'

    def status(self):
        state = 'failed' if self.thread and not self.thread.is_alive() and self.state == 'running' else self.state
        return {'state': state, 'last_check': self.last_check, 'error': self.error}

    def stop(self):
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1)


class RollingTrainingLoop:
    """Run explicitly created rolling training plans while the service lives."""
    def __init__(self, coordinator, registration=None, static_registration=None):
        self.coordinator = coordinator
        self.registration = registration
        self.static_registration = static_registration
        self.stop_event = threading.Event()
        self.thread = None
        self.state = 'not_started'
        self.last_check = None
        self.error = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        thread = threading.Thread(target=self._run, daemon=True)
        self.thread = thread
        try:
            thread.start()
        except Exception as exc:
            self.thread = None
            self.state = 'failed'
            self.error = '后台训练计划线程无法启动：' + str(exc)[:500]

    def _run(self):
        while not self.stop_event.is_set():
            try:
                with SingleInstance(str(self.coordinator.path)+'.rolling'):
                    self.state = 'running'
                    while not self.stop_event.is_set():
                        try:
                            self.coordinator.tick()
                            if self.registration is not None:
                                self.registration.tick()
                            if self.static_registration is not None:
                                self.static_registration.tick()
                            self.error = None
                        except Exception:
                            self.error = '后台训练计划检查失败'
                        self.last_check = datetime.now(timezone.utc).isoformat()
                        self.stop_event.wait(1)
            except RuntimeError:
                self.state = 'occupied'
                self.error = '另一个进程正在处理自动训练计划'
                self.stop_event.wait(5)
            except Exception:
                self.state = 'failed'
                self.error = '后台训练线程异常'
                self.stop_event.wait(5)
        self.state = 'stopped'

    def status(self):
        state = 'failed' if self.thread and not self.thread.is_alive() and self.state == 'running' else self.state
        return {'state': state, 'last_check': self.last_check, 'error': self.error}

    def stop(self):
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join()


class LocalServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, port, db_path):
        self.db_path, self._database_existed_at_start = prepare_runtime_database_path(db_path)
        self.jobs = TrainingJobs(self.db_path)
        self.backtests = BacktestJobs(self.db_path)
        self.scheduler = Scheduler(self.db_path)
        self.update_loop = UpdateLoop(self.scheduler)
        self.experiments = ProspectiveRepository(self.db_path)
        SnapshotStore(self.db_path).initialize()
        initialize_holdout_reservations(self.db_path)
        PairRepository(self.db_path)._initialize()
        EvaluationRepository(self.db_path)._initialize()
        self.backtests.initialize()
        self.scheduler.initialize()
        self.experiments.initialize()
        PaperResults(self.db_path).initialize()
        verify_runtime_database_permissions(self.db_path,
                                           existed_before_start=self._database_existed_at_start)
        self.rolling_training = RollingTraining(self.db_path, self.jobs, clock=lambda: self.experiments._now())
        self.rolling_training.initialize()
        self.rolling_registration = RollingRegistration(self.db_path, self.rolling_training, clock=lambda: self.experiments._now())
        self.rolling_registration.initialize()
        self.static_registration = StaticRegistration(self.db_path, clock=lambda: self.experiments._now())
        self.static_registration.initialize()
        self.rolling_loop = RollingTrainingLoop(self.rolling_training, self.rolling_registration, self.static_registration)
        self.fixed_evaluations = FixedEvaluations(self.db_path)
        self.research_decisions = ResearchDecisions(self.db_path, clock=lambda: self.experiments._now())
        self.research_decisions.initialize()
        verify_runtime_database_permissions(self.db_path,
                                           existed_before_start=self._database_existed_at_start)
        self.evaluation_lock = threading.BoundedSemaphore(1)
        self.integrity_lock = threading.BoundedSemaphore(1)
        self.series_report_lock = threading.BoundedSemaphore(1)
        self.holdout_audit_lock = threading.BoundedSemaphore(1)
        self.replay_export_lock = threading.BoundedSemaphore(1)
        self.backup_lock = threading.BoundedSemaphore(1)
        self.restore_lock = threading.BoundedSemaphore(1)
        self.rolling_preview_lock = threading.BoundedSemaphore(1)
        self.source_preview_lock = threading.BoundedSemaphore(1)
        self.session = secrets.token_urlsafe(32)
        self.csrf = secrets.token_urlsafe(32)
        super().__init__(('127.0.0.1', port), Handler)

    @property
    def origin(self):
        return f'http://127.0.0.1:{self.server_port}'

    def handle_error(self, request, client_address):
        # Navigation or closing a browser may cancel an in-flight response.
        # Preserve tracebacks for every other unexpected server error.
        exc_type = sys.exc_info()[0]
        if exc_type and issubclass(exc_type, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
            return
        super().handle_error(request, client_address)


class Handler(BaseHTTPRequestHandler):
    server_version = 'LocalResearch/1'

    def log_message(self, format, *args):
        pass

    def _headers(self, status, content_type, size, *, cookie=False, download_name=None):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(size))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('X-Frame-Options', 'DENY')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Content-Security-Policy', "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; worker-src 'self' blob:; frame-ancestors 'none'")
        if download_name is not None:
            self.send_header('Content-Disposition', "attachment; filename*=UTF-8''" + quote(download_name, safe=''))
        if cookie:
            self.send_header('Set-Cookie', f'dlt_session={self.server.session}; Path=/; HttpOnly; SameSite=Strict')
        self.end_headers()

    def _json(self, status, data, *, cookie=False):
        payload = json.dumps(data, ensure_ascii=False, allow_nan=False).encode('utf-8')
        self._headers(status, 'application/json; charset=utf-8', len(payload), cookie=cookie)
        self.wfile.write(payload)

    def _validate_host(self):
        try:
            local = ipaddress.ip_address(self.client_address[0]).is_loopback
        except ValueError:
            return False
        if not local or self.headers.get('Host') != f'127.0.0.1:{self.server.server_port}':
            return False
        origin = self.headers.get('Origin')
        return not origin or origin == self.server.origin

    def _session(self):
        try:
            cookie = SimpleCookie(); cookie.load(self.headers.get('Cookie', ''))
            return 'dlt_session' in cookie and secrets.compare_digest(cookie['dlt_session'].value, self.server.session)
        except Exception:
            return False

    def _discard_bounded_body(self):
        # On Windows, closing a socket with unread request bytes can reset it
        # before the client receives the rejection. Never drain an unbounded
        # or transfer-encoded body, and bound the wait for an incomplete body.
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if self.headers.get('Transfer-Encoding') or not 0 <= length <= MAX_SOURCE_REVIEW_BODY_BYTES:
                return
            self.connection.settimeout(2)
            self.rfile.read(length)
        except (ValueError, OSError, TimeoutError):
            pass
        finally:
            self.connection.settimeout(None)

    def do_GET(self):
        if not self._validate_host():
            return self._json(403, {'error': '仅支持本机访问'})
        path = urlsplit(self.path).path
        if path == '/api/session':
            return self._json(200, {'csrf': self.server.csrf}, cookie=True)
        if path.startswith('/api/') and not self._session():
            return self._json(401, {'error': '本机会话已失效'})
        if path == '/api/custom-origin-declarations':
            from dlt.custom_origin import list_legacy_custom
            return self._json(200, {'records': list_legacy_custom(self.server.db_path)})
        if path == '/api/training-limits':
            from dlt.models import (ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION,
                                    PREPROCESSING_VERSION)
            return self._json(200, {
                'schema': 'training-limits-v1',
                'limits': asdict(DEFAULT_TRAINING_LIMITS),
                'backtest': asdict(DEFAULT_BACKTEST_LIMITS),
                'versions': {'algorithm': ALGORITHM_VERSION,
                             'feature_schema': FEATURE_SCHEMA_VERSION,
                             'preprocessing': PREPROCESSING_VERSION},
            })
        if path == '/api/custom-origin-declaration-writes':
            from dlt.custom_origin import declaration_write_status
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if any(len(query.get(name, [])) != 1 for name in ('event_id', 'request_sha256')):
                return self._json(400, {'error': '需要来源声明请求标识和摘要'})
            try:
                write = declaration_write_status(self.server.db_path, event_id=query['event_id'][0],
                                                 request_sha256=query['request_sha256'][0])
                return self._json(200, {'write': write})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '来源声明请求状态无法核验'})
        if path == '/api/integrity':
            if not self.server.integrity_lock.acquire(blocking=False):
                return self._json(409, {'error': '已有完整性检查正在运行，请稍后重试'})
            try:
                check = check_database(self.server.db_path)
            finally:
                self.server.integrity_lock.release()
            return self._json(200, {'check': check})
        if path == '/api/holdout-reservations':
            return self._json(200, {'reservations': list_reservations(self.server.db_path)})
        if path == '/api/holdout-reservation-writes':
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if any(len(query.get(name, [])) != 1 for name in ('request_key', 'request_sha256')):
                return self._json(400, {'error': '需要排除范围预留请求标识和摘要'})
            try:
                write = reservation_write_status(self.server.db_path, request_key=query['request_key'][0],
                                                 request_sha256=query['request_sha256'][0])
                return self._json(200, {'write': _safe_integers(write)})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '排除范围预留请求状态无法核验'})
        if path == '/api/fixed-evaluations':
            return self._json(200, {'evaluations': _safe_integers(self.server.fixed_evaluations.list())})
        if path == '/api/fixed-evaluation-writes':
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if any(len(query.get(name, [])) != 1 for name in ('request_key', 'operation', 'request_sha256')):
                return self._json(400, {'error': '需要固定评估请求标识、操作类型和摘要'})
            try:
                write = self.server.fixed_evaluations.write_status(
                    query['request_key'][0], query['operation'][0], query['request_sha256'][0])
                return self._json(200, {'write': _safe_integers(write)})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '固定评估请求状态无法核验'})
        if path == '/api/research-writes/status':
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if any(len(query.get(name, [])) != 1 for name in ('operation', 'request_key', 'request_sha256')):
                return self._json(400, {'error': '需要写入类型、请求标识和请求摘要'})
            if len(query.get('require_intent', [])) > 1 or query.get('require_intent', ['0'])[0] not in ('0', '1'):
                return self._json(400, {'error': '准备记录参数无效'})
            try:
                state = self.server.research_decisions.write_status(
                    query['operation'][0], query['request_key'][0], query['request_sha256'][0],
                    require_intent=query.get('require_intent', ['0'])[0] == '1')
                return self._json(200, {'write': _safe_integers(state)})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '请求状态无法核验，请检查标识、摘要和记录完整性'})
        if path == '/api/research-writes/current':
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            names = ('operation', 'object_kind', 'object_key')
            if any(len(query.get(name, [])) != 1 for name in names):
                return self._json(400, {'error': '需要写入类型和研究对象标识'})
            try:
                current = self.server.research_decisions.current_write(*(query[name][0] for name in names))
                return self._json(200, {'write': _safe_integers(current)})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '当前研究对象的写入状态无法核验'})
        if path in ('/api/research-decision-context', '/api/research-decisions', '/api/research-drafts'):
            if not self.server.series_report_lock.acquire(blocking=False):
                return self._json(409, {'error': '已有证据汇总正在计算，请稍后重试'})
            try:
                if path == '/api/research-drafts':
                    return self._json(200, {'drafts': self.server.research_decisions.drafts()})
                query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
                kinds, keys = query.get('kind'), query.get('key')
                if not kinds or len(kinds) != 1 or not keys or len(keys) != 1:
                    return self._json(400, {'error': '需要证据类型和标识'})
                if path == '/api/research-decision-context':
                    return self._json(200, _safe_integers(self.server.research_decisions.context(kinds[0], keys[0])))
                return self._json(200, {'decisions': _safe_integers(self.server.research_decisions.list(kinds[0], keys[0]))})
            except KeyError:
                return self._json(404, {'error': '证据记录不存在'})
            except (ValueError, TypeError, sqlite3.Error):
                return self._json(409, {'error': '证据或判断记录无法核验，请检查数据完整性'})
            finally:
                self.server.series_report_lock.release()
        if path == '/api/rolling-training-plans':
            return self._json(200, {'plans': self.server.rolling_training.list(), 'worker': self.server.rolling_loop.status()})
        if path == '/api/rolling-training-plan-writes':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if not keys or len(keys) != 1:
                return self._json(400, {'error': '需要自动训练计划请求标识'})
            return self._json(200, self.server.rolling_training.write_status(keys[0]))
        if path == '/api/rolling-registration-plans':
            return self._json(200, {'plans': self.server.rolling_registration.list()})
        if path == '/api/rolling-registration-plan-writes':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if not keys or len(keys) != 1: return self._json(400, {'error': '需要自动登记计划请求标识'})
            return self._json(200, self.server.rolling_registration.write_status(keys[0]))
        if path == '/api/static-registration-plans':
            return self._json(200, {'plans': self.server.static_registration.list()})
        if path == '/api/static-registration-plan-writes':
            keys=parse_qs(urlsplit(self.path).query,keep_blank_values=True).get('key')
            if not keys or len(keys)!=1:return self._json(400,{'error':'需要静态登记计划请求标识'})
            return self._json(200,self.server.static_registration.write_status(keys[0]))
        if path == '/api/snapshots':
            with closing(sqlite3.connect(self.server.db_path)) as db:
                rows = db.execute('SELECT id, row_count FROM snapshots ORDER BY rowid DESC').fetchall()
            output = []
            for ident, count in rows:
                draws = SnapshotStore(self.server.db_path).get_draws(ident)
                output.append({'id': ident, 'row_count': count, 'first_issue': draws[0]['issue'],
                               'last_issue': draws[-1]['issue'], 'issues': [r['issue'] for r in draws]})
            return self._json(200, {'snapshots': output})
        if path == '/api/source-quarantine':
            return self._json(200, {'records': SnapshotStore(self.server.db_path).list_quarantines()})
        if path == '/api/source-quarantine-document':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('id')
            if keys is None or len(keys) != 1 or not re.fullmatch(r'[0-9a-f]{64}', keys[0]):
                return self._json(400, {'error': '隔离来源标识无效'})
            try:
                document = SnapshotStore(self.server.db_path).quarantine_document(keys[0])
            except InvalidSnapshot as exc:
                return self._json(409, {'error': str(exc)})
            if document is None:
                return self._json(404, {'error': '隔离来源不存在'})
            raw = document['raw']
            self._headers(200, 'application/octet-stream', len(raw),
                          download_name=f'lottery-quarantine-{keys[0][:12]}.bin')
            self.wfile.write(raw)
            return
        if path == '/api/snapshots/import-bundled-status':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is None or len(keys) != 1:
                return self._json(400, {'error': '需要快照导入请求标识'})
            try:
                return self._json(200, SnapshotStore(self.server.db_path).bundled_import_status(keys[0]))
            except InvalidSnapshot as exc:
                return self._json(400, {'error': str(exc)})
        if path == '/api/draws':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('snapshot')
            if keys is None or len(keys) != 1 or len(keys[0]) != 64:
                return self._json(400, {'error': '快照标识无效'})
            store = SnapshotStore(self.server.db_path)
            draws = store.get_draws(keys[0])
            if not draws:
                return self._json(404, {'error': '快照不存在'})
            with closing(sqlite3.connect(self.server.db_path)) as db:
                latest = db.execute('SELECT source, observed_at FROM observations WHERE snapshot_id=? '
                                    'ORDER BY observed_at DESC LIMIT 1', (keys[0],)).fetchone()
                try:
                    hidden_issues = reserved_issues(db, require_table=True)
                except ReservationError as exc:
                    return self._json(503, {'error': f'排除范围状态无法核验，快照明细暂不可用：{exc}'})
            rows = [{'issue': row['issue'], 'draw_date': row['draw_date'],
                     'front': [] if row['issue'] in hidden_issues else row['front'],
                     'back': [] if row['issue'] in hidden_issues else row['back'],
                     'numbers_hidden': row['issue'] in hidden_issues,
                     'numbers_usable': bool(row['numbers_usable']), 'prize_fields_present': bool(row['prize_fields_present']),
                     'settlement_ready': bool(row['settlement_ready']), 'settlement_reason': row['settlement_reason'],
                     'conflict': bool(store.conflicts(row['issue']))} for row in reversed(draws[-12:])]
            return self._json(200, {'snapshot_id': keys[0], 'count': len(draws), 'draws': rows,
                                    'reserved_issues': sorted(hidden_issues),
                                    'observation': {'source': latest[0], 'observed_at': latest[1]} if latest else None})
        if path == '/api/draw-conflicts':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('issue')
            if keys is None or len(keys) != 1:
                return self._json(400, {'error': '需要一个奖期'})
            try:
                with closing(sqlite3.connect(self.server.db_path)) as db:
                    try:
                        is_reserved = keys[0] in reserved_issues(db, require_table=True)
                    except ReservationError as exc:
                        return self._json(503, {'error': f'排除范围状态无法核验，号码版本暂不可用：{exc}'})
                    if is_reserved:
                        return self._json(409, {'error': '该奖期属于已固定排除范围，号码版本已隐藏'})
                return self._json(200, {'review': PaperResults(self.server.db_path).inspect_draw_versions(keys[0])})
            except PaperResultError as exc:
                return self._json(400, {'error': str(exc)})
        if path == '/api/jobs':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is not None:
                if len(keys) != 1 or not keys[0]:
                    return self._json(400, {'error': '训练任务标识无效'})
                try:
                    job = self.server.jobs.get(keys[0])
                except KeyError:
                    return self._json(404, {'error': '训练任务不存在'})
                if job['artifact_id']:
                    try:
                        check = ModelRepository(self.server.db_path).inspect(job['artifact_id'])
                        job['artifact_status'], job['artifact_reason'] = check['status'], check['reason']
                    except Exception:
                        job['artifact_status'], job['artifact_reason'] = 'review_required', '模型产物或来源无法核验'
                return self._json(200, {'job': job})
            jobs = self.server.jobs.list()
            for job in jobs:
                if job['artifact_id']:
                    try:
                        check = ModelRepository(self.server.db_path).inspect(job['artifact_id'])
                        job['artifact_status'], job['artifact_reason'] = check['status'], check['reason']
                    except Exception:
                        job['artifact_status'], job['artifact_reason'] = 'review_required', '模型产物或来源无法核验'
            return self._json(200, {'jobs': jobs})
        if path == '/api/task-cancellations':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is None or len(keys) != 1:
                return self._json(400, {'error': '需要取消请求标识'})
            try:
                return self._json(200, {'cancellation': get_task_cancellation(self.server.db_path, keys[0])})
            except KeyError:
                return self._json(404, {'error': '取消请求结果尚无可核验记录'})
            except (ValueError, TypeError, sqlite3.Error):
                return self._json(409, {'error': '取消请求记录无法核验'})
        if path == '/api/backtests':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is not None:
                if len(keys) != 1 or not keys[0]:
                    return self._json(400, {'error': '回测标识无效'})
                try:
                    return self._json(200, {'job': _safe_integers(self.server.backtests.get(keys[0]))})
                except KeyError:
                    return self._json(404, {'error': '回测不存在'})
            return self._json(200, {'jobs': _safe_integers(self.server.backtests.list())})
        if path == '/api/update-plans':
            plans = self.server.scheduler.list_plans()
            return self._json(200, {'plans': [{**plan, 'runs': self.server.scheduler.list_runs(plan['id']),
                                              'changes': self.server.scheduler.list_changes(plan['id'])}
                                             for plan in plans], 'worker': self.server.update_loop.status(),
                                    'autostart': autostart_status(self.server.db_path, urlsplit(self.server.origin).port)})
        if path == '/api/update-write-requests':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is None or len(keys) != 1:
                return self._json(400, {'error': '需要更新计划请求标识'})
            try:
                return self._json(200, {'write': self.server.scheduler.write_request(keys[0])})
            except KeyError:
                return self._json(404, {'error': '更新计划请求结果尚无可核验记录'})
            except (ValueError, TypeError, sqlite3.Error):
                return self._json(409, {'error': '更新计划请求记录无法核验'})
        if path == '/api/source-reviews':
            with closing(sqlite3.connect(self.server.db_path)) as db:
                rows = db.execute('SELECT id, record_json FROM reviewed_sources ORDER BY rowid DESC').fetchall()
                corrections = dict(db.execute('SELECT old_review_id,new_review_id FROM source_corrections').fetchall())
                records = []
                for ident, raw in rows:
                    try:
                        record = json.loads(raw)
                        verified = self.server.experiments._get_review(db, ident, record.get('kind'))
                        records.append({'review_id': ident, 'status': 'personally_reviewed',
                                        'superseded_by': corrections.get(ident), **verified})
                    except (ValueError, KeyError, TypeError, RegistrationError):
                        records.append({'review_id': ident, 'status': 'review_required', 'reason': '资料完整性核验失败'})
            return self._json(200, {'reviews': records})
        if path == '/api/source-review-document':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('id')
            if keys is None or len(keys) != 1 or len(keys[0]) != 64:
                return self._json(400, {'error': '资料标识无效'})
            with closing(sqlite3.connect(self.server.db_path)) as db:
                row = db.execute('SELECT record_json,document FROM reviewed_sources WHERE id=?', (keys[0],)).fetchone()
                if not row:
                    return self._json(404, {'error': '资料不存在'})
                try:
                    kind = json.loads(row[0])['kind']
                    record = self.server.experiments._get_review(db, keys[0], kind)
                except (ValueError, KeyError, TypeError, RegistrationError):
                    return self._json(409, {'error': '资料完整性核验失败'})
            # An uploaded file may contain arbitrary credentials or machine
            # paths, including inside opaque PDFs or archives. Returning it as
            # an export cannot satisfy the PRD's exclusion requirement.
            # Retain the original bytes and their digest for local auditing.
            return self._json(403, {'error': '来源原文保留在本机；原样下载暂不可用，请使用本人保存的原文件核对'})
        if path == '/api/source-corrections':
            return self._json(200, {'corrections': self.server.experiments.list_source_corrections()})
        if path == '/api/experiment-series/report':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is None or len(keys) != 1 or not keys[0]:
                return self._json(400, {'error': '需要实验版本标识'})
            if not self.server.series_report_lock.acquire(blocking=False):
                return self._json(409, {'error': '已有实验汇总正在计算，请稍后重试'})
            try:
                report = SeriesRepository(self.server.db_path, clock=self.server.experiments.clock).describe(keys[0])
                return self._json(200, {'report': _safe_integers(report)})
            except KeyError:
                return self._json(404, {'error': '实验版本不存在'})
            except (ValueError, sqlite3.Error):
                return self._json(409, {'error': '实验版本或数据完整性异常，无法生成汇总'})
            finally:
                self.server.series_report_lock.release()
        if path == '/api/experiment-series':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            repository = SeriesRepository(self.server.db_path, clock=self.server.experiments.clock)
            if keys is not None:
                if len(keys) != 1 or not keys[0]:
                    return self._json(400, {'error': '需要实验版本标识'})
                try:
                    return self._json(200, {'series': _safe_integers(repository.get(keys[0]))})
                except KeyError:
                    return self._json(404, {'error': '实验版本不存在'})
            with closing(sqlite3.connect(self.server.db_path)) as db:
                identifiers = [row[0] for row in db.execute('SELECT series_key FROM experiment_series ORDER BY rowid DESC')]
            return self._json(200, {'series': [{'series_key': key, **_safe_integers(repository.get(key))}
                                              for key in identifiers]})
        if path == '/api/experiment-series/write-status':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is None or len(keys) != 1:
                return self._json(400, {'error': '需要实验版本请求标识'})
            try:
                repository = SeriesRepository(self.server.db_path, clock=self.server.experiments.clock)
                return self._json(200, {'write': repository.create_status(keys[0])})
            except RegistrationError as exc:
                return self._json(400, {'error': str(exc)})
        if path == '/api/experiments':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is not None:
                if len(keys) != 1 or not keys[0]:
                    return self._json(400, {'error': '实验标识无效'})
                try:
                    result = self.server.experiments.inspect(keys[0])
                    result['paper_result'] = PaperResults(self.server.db_path).inspect_current(keys[0])
                    return self._json(200, {'experiment': _safe_integers(result)})
                except KeyError:
                    return self._json(404, {'error': '实验不存在'})
            with closing(sqlite3.connect(self.server.db_path)) as db:
                ids = [row[0] for row in db.execute('SELECT request_key FROM fixed_experiments ORDER BY rowid DESC')]
            output = []
            for key in ids:
                entry = self.server.experiments.inspect(key)
                content = entry['content']
                result = PaperResults(self.server.db_path).inspect_current(key)
                output.append({'request_key': key, 'status': entry['status'], 'reason': entry['reason'],
                               'target_issue': content['protocol']['target_issue'],
                               'target_date': entry.get('effective_calendar', {}).get('draw_date'),
                               'registered_at': content['registered_at'], 'replicas': len(content['pairs']),
                               'hypothesis': content['protocol']['hypothesis'], 'paper_status': result['status']})
            return self._json(200, {'experiments': output})
        if path == '/api/experiments/write-status':
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if any(len(query.get(name, [])) != 1 for name in ('request_key', 'request_sha256')):
                return self._json(400, {'error': '需要请求标识和请求摘要'})
            try:
                return self._json(200, {'write': self.server.experiments.write_status(
                    query['request_key'][0], query['request_sha256'][0])})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '实验请求状态无法核验'})
        if path == '/api/prospective-closure-writes':
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if any(len(query.get(name, [])) != 1 for name in ('request_key', 'operation', 'request_sha256')):
                return self._json(400, {'error': '需要实验关闭请求标识、操作类型和摘要'})
            try:
                write = closure_write_status(self.server.db_path, query['request_key'][0],
                                             query['operation'][0], query['request_sha256'][0])
                return self._json(200, {'write': _safe_integers(write)})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '实验关闭请求状态无法核验'})
        if path == '/api/source-write-status':
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if any(len(query.get(name, [])) != 1 for name in ('request_key', 'request_sha256')):
                return self._json(400, {'error': '需要来源写入请求标识和摘要'})
            try:
                self.server.experiments.initialize()
                write = source_write_status(self.server.db_path, query['request_key'][0], query['request_sha256'][0])
                return self._json(200, {'write': write})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '来源写入请求状态无法核验'})
        if path == '/api/recovery-write-status':
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if any(len(query.get(name, [])) != 1 for name in ('request_key', 'operation', 'request_sha256')):
                return self._json(400, {'error': '需要恢复写入请求标识、操作类型和摘要'})
            try:
                self.server.experiments.initialize()
                operation = query['operation'][0]
                if operation not in ('recovery_prepare', 'recovery_activate'):
                    raise ValueError('invalid operation')
                write = source_write_status(self.server.db_path, query['request_key'][0], query['request_sha256'][0])
                if write['status'] != 'unknown' and write.get('operation') != operation:
                    raise ValueError('operation mismatch')
                return self._json(200, {'write': write})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '恢复写入请求状态无法核验'})
        if path == '/api/paper-results/write-status':
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if any(len(query.get(name, [])) != 1 for name in ('request_key', 'operation', 'request_sha256')):
                return self._json(400, {'error': '需要纸面结果请求标识、操作类型和摘要'})
            try:
                paper = PaperResults(self.server.db_path)
                paper.initialize()
                write = paper.write_status(
                    query['request_key'][0], query['operation'][0], query['request_sha256'][0])
                return self._json(200, {'write': write})
            except (ValueError, TypeError, KeyError, sqlite3.Error):
                return self._json(409, {'error': '纸面结果请求状态无法核验'})
        if path == '/api/experiment-history':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is None or len(keys) != 1 or not keys[0]:
                return self._json(400, {'error': '需要实验标识'})
            try:
                paper = PaperResults(self.server.db_path)
                paper.experiments.get(keys[0])
                history = paper.list_history(keys[0])
                history['publications'] = [{**item, 'result': paper.get_group(item['group_id'])}
                                           for item in history['publications']]
                current = paper.inspect_current(keys[0])
                history['current'] = {'status': current['status'], 'group_id': current.get('group_id'),
                                      'reason': current.get('reason')}
                return self._json(200, {'history': _safe_integers(history)})
            except KeyError:
                return self._json(404, {'error': '实验或历史结果不存在'})
            except ValueError as exc:
                return self._json(409, {'error': str(exc)})
        if path == '/api/pairs':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if keys is not None:
                if len(keys) != 1 or not keys[0]:
                    return self._json(400, {'error': '草稿标识无效'})
                try:
                    repository = PairRepository(self.server.db_path)
                    return self._json(200, {'request_key': keys[0], 'request': repository.get_request(keys[0]),
                                            **_pair_wire(repository.inspect(keys[0])),
                                            'selection_history': repository.selection_history(keys[0])})
                except KeyError:
                    return self._json(404, {'error': '草稿不存在'})
            return self._json(200, {'pairs': _safe_integers(PairRepository(self.server.db_path).list())})
        if path == '/api/pair-evaluation-writes':
            params = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            operations, keys = params.get('operation'), params.get('key')
            if operations is None or len(operations) != 1 or operations[0] not in ('pair_create', 'evaluation', 'selection_decision') or keys is None or len(keys) != 1:
                return self._json(400, {'error': '需要有效的生成或评估请求标识'})
            from dlt.pair_evaluation_writes import status_in_transaction
            repository = PairRepository(self.server.db_path) if operations[0] in ('pair_create', 'selection_decision') else EvaluationRepository(self.server.db_path)
            repository._initialize()
            try:
                with closing(sqlite3.connect(self.server.db_path)) as db:
                    write = status_in_transaction(db, operations[0], keys[0])
            except (ValueError, sqlite3.Error) as exc:
                return self._json(409, {'error': str(exc)[:1000]})
            if write['status'] != 'unknown':
                return self._json(200, {'write': write})
            try:
                if operations[0] == 'selection_decision':
                    return self._json(200, {'write': write})
                if operations[0] == 'pair_create':
                    request = repository.get_request(keys[0])
                    result = {'request_key': keys[0], 'request': request, **_pair_wire(repository.inspect(keys[0]))}
                    request_raw = _canonical(request)
                else:
                    evaluation_request = repository.get_request(keys[0])
                    scenario = evaluation_request['scenario']
                    dependency = scenario.get('source_dependency', {})
                    write_request = ({'pair_key': evaluation_request['pair_key'], 'scenario_kind': 'custom_payout_v1',
                                      'custom_input': scenario['custom_input'], 'samples': evaluation_request['samples'],
                                      'seed': evaluation_request['seed']} if scenario.get('kind') == 'custom_payout_v1'
                                     else {'pair_key': evaluation_request['pair_key'],
                                           'scenario_snapshot_id': dependency.get('snapshot_id'),
                                           'scenario_issue': dependency.get('issue'),
                                           'samples': evaluation_request['samples'], 'seed': evaluation_request['seed']})
                    result = {'request_key': keys[0], 'request': evaluation_request,
                              **_evaluation_wire(repository.inspect(keys[0]))}
                    request_raw = _canonical(write_request)
                write = {'status': 'accepted', 'legacy': True, 'request_key': keys[0],
                         'request_sha256': hashlib.sha256(request_raw).hexdigest(),
                         'request': write_request if operations[0] == 'evaluation' else request,
                         'result': result, 'retry_of': None}
                return self._json(200, {'write': write})
            except KeyError:
                return self._json(200, {'write': write})
        if path == '/api/evaluations/comparison-options':
            try:
                return self._json(200, {'options': _safe_integers(EvaluationRepository(self.server.db_path).comparison_options())})
            except ValueError as exc:
                return self._json(409, {'error': str(exc)[:1000]})
        if path == '/api/evaluations/compare':
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key', [])
            try:
                comparison = EvaluationRepository(self.server.db_path).compare_pairs(keys)
                comparison['entries'] = [_evaluation_wire(entry) for entry in comparison['entries']]
                return self._json(200, _safe_integers(comparison))
            except (ValueError, KeyError) as exc:
                return self._json(409, {'error': str(exc)[:1000]})
        if path == '/api/evaluations':
            request_keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('key')
            if request_keys is not None:
                if len(request_keys) != 1 or not request_keys[0]:
                    return self._json(400, {'error': '评估请求标识无效'})
                key = request_keys[0]
                repository = EvaluationRepository(self.server.db_path)
                try:
                    request = repository.get_request(key)
                except KeyError:
                    return self._json(404, {'error': '评估结果尚未保存'})
                return self._json(200, {'request_key': key, 'request': request,
                                        **_evaluation_wire(repository.inspect(key))})
            keys = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get('pair')
            if keys is None or len(keys) != 1 or not keys[0]:
                return self._json(400, {'error': '需要组合草稿标识'})
            history = [_evaluation_wire(item) for item in EvaluationRepository(self.server.db_path).list_for_pair(keys[0])]
            return self._json(200, {'evaluation': history[0] if history else None, 'evaluations': history})
        name = 'index.html' if path == '/' else path.lstrip('/')
        if name not in ALLOWED_FILES:
            return self._json(404, {'error': '未找到页面'})
        file = STATIC/name
        data = file.read_bytes()
        if name == 'data.js':
            with closing(sqlite3.connect(self.server.db_path)) as db:
                try:
                    hidden_issues = reserved_issues(db, require_table=True)
                except ReservationError as exc:
                    return self._json(503, {'error': f'排除范围状态无法核验，静态研究数据暂不可用：{exc}'})
            try:
                data = _masked_bundled_research_data(data.decode('utf-8'), hidden_issues)
            except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
                return self._json(503, {'error': '静态研究数据无法安全筛选，暂不可用'})
        content_type = mimetypes.guess_type(file.name)[0] or 'application/octet-stream'
        self._headers(200, content_type+'; charset=utf-8', len(data))
        self.wfile.write(data)

    def do_POST(self):
        path = urlsplit(self.path).path
        if (not self._validate_host() or self.headers.get('Origin') != self.server.origin
                or self.headers.get('Sec-Fetch-Site', 'same-origin') not in ('same-origin', 'none')
                or not self._session() or not secrets.compare_digest(self.headers.get('X-DLT-CSRF', ''), self.server.csrf)):
            self._discard_bounded_body()
            return self._json(403, {'error': '本机操作校验失败'})
        if self.headers.get('Content-Type', '').split(';')[0].strip().lower() != 'application/json' or self.headers.get('Transfer-Encoding'):
            self._discard_bounded_body()
            return self._json(415, {'error': '请求格式不受支持'})
        try:
            length = int(self.headers.get('Content-Length', ''))
            body_limit = (MAX_SOURCE_REVIEW_BODY_BYTES if path == '/api/source-reviews'
                          else MAX_POST_BODY_BYTES)
            if not 0 <= length <= body_limit:
                raise ValueError('请求过大')
            body = json.loads(self.rfile.read(length).decode('utf-8'))
            if not isinstance(body, dict):
                raise ValueError('请求必须是对象')
            if path == '/api/autostart':
                port = urlsplit(self.server.origin).port
                if body['port'] != port:
                    raise ScheduleError('登录启动请求端口与当前服务不一致')
                state = self.server.scheduler.set_autostart_enabled(body['enabled'], body['port'],
                                    request_key=body['request_key'], retry_of=body.get('retry_of'))
                return self._json(200, {'autostart': state,
                                        'write': self.server.scheduler.write_request(body['request_key'])})
            if path == '/api/custom-origin-declarations':
                from dlt.custom_origin import record_declaration
                record = record_declaration(self.server.db_path, event_id=body['event_id'],
                            evaluation_key=body['evaluation_key'], kind=body['kind'], issue=body.get('issue'),
                            reason=body['reason'], confirmed=body.get('confirmed'), retry_of=body.get('retry_of'))
                from dlt.custom_origin import _request, _digest
                request = _request(body['evaluation_key'], body['kind'], body.get('issue'),
                                   body['reason'], body.get('confirmed'))
                return self._json(200, {'declaration': record,
                                        'write': {'status': 'accepted', 'event_id': body['event_id'],
                                                  'request_sha256': _digest(request), 'record': record}})
            if path == '/api/custom-origin-declarations/fence':
                from dlt.custom_origin import fence_declaration
                write = fence_declaration(self.server.db_path, event_id=body['event_id'],
                            request_sha256=body['request_sha256'], evaluation_key=body['evaluation_key'],
                            kind=body['kind'], issue=body.get('issue'), reason=body['reason'],
                            confirmed=body.get('confirmed'), retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': write, **({'error': '来源声明请求已封止，迟到提交将被拒绝'}
                                                     if write['status'] == 'fenced' else {})})
            if path == '/api/update-plans/edit':
                result = self.server.scheduler.edit_plan(body['plan_id'], body['config'],
                                            expected_sha256=body['expected_sha256'], request_key=body['request_key'],
                                            retry_of=body.get('retry_of'))
                return self._json(200, {'edit': result, 'write': self.server.scheduler.write_request(body['request_key'])})
            if path == '/api/update-runs/cancel':
                result = self.server.scheduler.cancel_run(body['plan_id'], body['run_id'],
                                            request_key=body['request_key'], retry_of=body.get('retry_of'))
                return self._json(200, {'run': result, 'write': self.server.scheduler.write_request(body['request_key'])})
            if path == '/api/update-write-requests/fence':
                write = self.server.scheduler.fence_write(body['request_key'], body['operation'], body['request'],
                                      retry_of=body.get('retry_of'), port=urlsplit(self.server.origin).port)
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': write, **({'error': '更新请求已封止，迟到请求将被拒绝'}
                                                     if write['status'] == 'fenced' else {})})
            if path == '/api/research-decisions':
                required = body.get('require_intent', False)
                if not isinstance(required, bool):
                    raise ValueError('准备记录参数无效')
                if not required:
                    raise ValueError('保存判断前必须先准备并占用研究对象')
                try:
                    record = self.server.research_decisions.save(
                        body['request_key'], kind=body['kind'], key=body['key'], choice=body['choice'],
                        reason=body['reason'], limitations=body['limitations'], evidence_sha256=body['evidence_sha256'],
                        require_intent=required)
                except StaleEvidence as exc:
                    return self._json(409, {'error': str(exc)})
                return self._json(200, {'decision': _safe_integers(record)})
            if path == '/api/research-writes/fence':
                required = body.get('require_intent', False)
                if not isinstance(required, bool):
                    raise ValueError('准备记录参数无效')
                state = self.server.research_decisions.fence_write(
                    body['operation'], body['request_key'], body['request_sha256'], require_intent=required)
                return self._json(200, {'write': _safe_integers(state)})
            if path == '/api/research-writes/acknowledge':
                result = self.server.research_decisions.acknowledge_write(
                    body['operation'], body['request_key'], body['request_sha256'], body['slot_version'])
                return self._json(200, {'acknowledgment': result})
            if path == '/api/research-writes/prepare':
                try:
                    intent = self.server.research_decisions.prepare_write(
                        body['operation'], body['request_key'], body['request'], retry_of=body.get('retry_of'))
                except ObjectWritePending as exc:
                    operation, request = body.get('operation'), body.get('request')
                    if operation == 'decision' and isinstance(request, dict):
                        object_kind, object_key = request.get('kind'), request.get('key')
                    elif operation == 'draft' and isinstance(request, dict):
                        object_kind, object_key = 'decision', request.get('decision_key')
                    else:
                        raise
                    current = self.server.research_decisions.current_write(operation, object_kind, object_key)
                    return self._json(409, {'code': 'object_occupied', 'error': str(exc),
                                            'write': _safe_integers(current)})
                return self._json(200, {'intent': intent})
            if path == '/api/research-drafts':
                required = body.get('require_intent', False)
                if not isinstance(required, bool):
                    raise ValueError('准备记录参数无效')
                if not required:
                    raise ValueError('保存新假设前必须先准备并占用研究对象')
                record = self.server.research_decisions.draft(body['request_key'], decision_key=body['decision_key'],
                                                               hypothesis=body['hypothesis'], require_intent=required)
                return self._json(200, {'draft': record})
            if path == '/api/rolling-training-plans':
                try:
                    result = self.server.rolling_training.create(
                        series_key=body['series_key'], issue=body['issue'], calendar_review_id=body['calendar_review_id'],
                        expected_cutoff_issue=body['expected_cutoff_issue'], start_at=body['start_at'],
                        timeout=body['timeout'], confirmed=body['confirmed'], request_key=body['request_key'],
                        retry_of=body.get('retry_of'))
                except RollingPlanFenced as exc:
                    return self._json(409, {'error': str(exc), 'write': self.server.rolling_training.write_status(exc.request_key)})
                return self._json(200, {'plan': result, 'write': self.server.rolling_training.write_status(body['request_key'])})
            if path == '/api/rolling-training-plans/cancel':
                try: plan = self.server.rolling_training.cancel(body['id'], request_key=body['request_key'], retry_of=body.get('retry_of'))
                except RollingPlanFenced as exc: return self._json(409, {'error': str(exc), 'write': self.server.rolling_training.write_status(exc.request_key)})
                return self._json(200, {'plan': plan, 'write': self.server.rolling_training.write_status(body['request_key'])})
            if path == '/api/rolling-training-plans/retry':
                try: plan = self.server.rolling_training.retry(body['id'], request_key=body['request_key'], retry_of=body.get('retry_of'))
                except RollingPlanFenced as exc: return self._json(409, {'error': str(exc), 'write': self.server.rolling_training.write_status(exc.request_key)})
                return self._json(200, {'plan': plan, 'write': self.server.rolling_training.write_status(body['request_key'])})
            if path == '/api/rolling-training-plan-writes/fence':
                write = self.server.rolling_training.fence_write(body['request_key'], body['operation'], body['request'],
                                                                  retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': write, **({'error': '原自动训练计划请求已封止'} if write['status'] == 'fenced' else {})})
            if path == '/api/rolling-registration-plans':
                try: plan = self.server.rolling_registration.create(
                    training_plan_id=body['training_plan_id'], rule_review_id=body['rule_review_id'],
                    tax_review_id=body['tax_review_id'], internal_deadline=body['internal_deadline'],
                    confirmed=body['confirmed'], request_key=body['request_key'], retry_of=body.get('retry_of'))
                except RollingRegistrationFenced as exc: return self._json(409, {'error': str(exc), 'write': self.server.rolling_registration.write_status(exc.request_key)})
                return self._json(200, {'plan': plan, 'write': self.server.rolling_registration.write_status(body['request_key'])})
            if path == '/api/rolling-registration-plans/cancel':
                try: plan = self.server.rolling_registration.cancel(body['id'], request_key=body['request_key'], retry_of=body.get('retry_of'))
                except RollingRegistrationFenced as exc: return self._json(409, {'error': str(exc), 'write': self.server.rolling_registration.write_status(exc.request_key)})
                return self._json(200, {'plan': plan, 'write': self.server.rolling_registration.write_status(body['request_key'])})
            if path == '/api/rolling-registration-plans/retry':
                try: plan = self.server.rolling_registration.retry(body['id'], request_key=body['request_key'], retry_of=body.get('retry_of'))
                except RollingRegistrationFenced as exc: return self._json(409, {'error': str(exc), 'write': self.server.rolling_registration.write_status(exc.request_key)})
                return self._json(200, {'plan': plan, 'write': self.server.rolling_registration.write_status(body['request_key'])})
            if path == '/api/rolling-registration-plan-writes/fence':
                write = self.server.rolling_registration.fence_write(body['request_key'], body['operation'], body['request'], retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': write, **({'error': '原自动登记计划请求已封止'} if write['status'] == 'fenced' else {})})
            if path == '/api/static-registration-plans':
                try: plan = self.server.static_registration.create(
                    series_key=body['series_key'], issue=body['issue'], calendar_review_id=body['calendar_review_id'],
                    rule_review_id=body['rule_review_id'], tax_review_id=body['tax_review_id'],
                    expected_cutoff_issue=body['expected_cutoff_issue'], history_periods=body['history_periods'],
                    start_at=body['start_at'], internal_deadline=body['internal_deadline'], confirmed=body['confirmed'],
                    request_key=body['request_key'], retry_of=body.get('retry_of'))
                except StaticRegistrationFenced as exc: return self._json(409, {'error': str(exc), 'write': self.server.static_registration.write_status(exc.request_key)})
                return self._json(200, {'plan': plan, 'write': self.server.static_registration.write_status(body['request_key'])})
            if path == '/api/static-registration-plans/cancel':
                try: plan=self.server.static_registration.cancel(body['id'],request_key=body['request_key'],retry_of=body.get('retry_of'))
                except StaticRegistrationFenced as exc: return self._json(409, {'error': str(exc), 'write': self.server.static_registration.write_status(exc.request_key)})
                return self._json(200, {'plan':plan,'write':self.server.static_registration.write_status(body['request_key'])})
            if path == '/api/static-registration-plans/retry':
                try: plan=self.server.static_registration.retry(body['id'],request_key=body['request_key'],retry_of=body.get('retry_of'))
                except StaticRegistrationFenced as exc: return self._json(409, {'error': str(exc), 'write': self.server.static_registration.write_status(exc.request_key)})
                return self._json(200, {'plan':plan,'write':self.server.static_registration.write_status(body['request_key'])})
            if path == '/api/static-registration-plan-writes/fence':
                write=self.server.static_registration.fence_write(body['request_key'],body['operation'],body['request'],retry_of=body.get('retry_of'))
                return self._json(200 if write['status']=='accepted' else 409,{'write':write,**({'error':'静态登记请求已封止'} if write['status']=='fenced' else {})})
            if path == '/api/encrypted-backup':
                from dlt.encrypted_backup import encrypted_snapshot_bytes
                if set(body) != {'password', 'acknowledge'} or body['acknowledge'] is not True:
                    raise ValueError('请确认加密快照包含原始资料，且口令遗失后无法恢复')
                if not self.server.backup_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有加密快照正在生成，请稍后重试'})
                try:
                    password = body.pop('password')
                    payload = encrypted_snapshot_bytes(self.server.db_path, password)
                    del password
                    try:
                        self._headers(200, 'application/octet-stream', len(payload), download_name='research-snapshot.dltbak')
                        self.wfile.write(payload)
                    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                        pass
                    return
                finally:
                    self.server.backup_lock.release()
            if path in ('/api/encrypted-backup/prepare-candidate',
                        '/api/encrypted-backup/inspect',
                        '/api/encrypted-backup/verify-candidate',
                        '/api/encrypted-backup/verify-restored-copy',
                        '/api/encrypted-backup/activate-candidate'):
                from dlt.encrypted_backup import prepare_encrypted_snapshot, inspect_encrypted_snapshot
                from dlt.recovery import (verify_isolated_candidate, verify_restored_copy,
                                          activate_isolated_candidate)
                recovery_operation = ('recovery_prepare' if path.endswith('/prepare-candidate') else
                                      'recovery_activate' if path.endswith('/activate-candidate') else None)
                if recovery_operation:
                    self.server.experiments.initialize()
                    if recovery_operation == 'recovery_prepare':
                        required = {'source', 'destination', 'password', 'acknowledge', 'request_key', 'request_sha256'}
                        request_names = {'source', 'destination', 'acknowledge'}
                    else:
                        required = {'candidate', 'destination', 'acknowledge', 'enable_restored_plans',
                                    'request_key', 'request_sha256'}
                        request_names = {'candidate', 'destination', 'acknowledge', 'enable_restored_plans'}
                    allowed = required | {'retry_of'}
                    if set(body) - allowed or required - set(body):
                        raise ValueError('恢复写入请求字段无效')
                    request = {name: body[name] for name in request_names}
                    if any(not isinstance(request[name], str) or not request[name].strip()
                           or len(request[name]) > 32767 or not Path(request[name]).is_absolute()
                           for name in ('destination', 'source' if recovery_operation == 'recovery_prepare' else 'candidate')):
                        raise ValueError('请填写有效的本机绝对路径')
                    if request['acknowledge'] is not True or (recovery_operation == 'recovery_activate'
                            and request['enable_restored_plans'] is not True):
                        raise ValueError('必须明确确认恢复明文资料及计划运行风险')
                    if body['request_sha256'] != _source_write_digest(request):
                        raise ValueError('恢复写入请求摘要不匹配')
                    receipt = {'request_key': body['request_key'], 'operation': recovery_operation,
                               'request_sha256': body['request_sha256']}
                    if not self.server.restore_lock.acquire(blocking=False):
                        return self._json(409, {'error': '已有恢复操作正在进行，请查询本请求状态'})
                    try:
                        current = source_write_status(self.server.db_path, body['request_key'], body['request_sha256'])
                        if current['status'] == 'accepted':
                            if current.get('operation') != recovery_operation:
                                raise ValueError('恢复请求标识已用于其他操作')
                            return self._json(200, {'result': current['result'], 'write': current})
                        if current['status'] == 'fenced':
                            return self._json(409, {'error': '恢复请求已封止，不能执行', 'write': current})
                        from dlt.source_write_protocol import (_check_object_fences,
                                                               object_key as source_object_key,
                                                               validate_retry)
                        with closing(sqlite3.connect(self.server.db_path)) as db:
                            key = source_object_key(recovery_operation, request)
                            validate_retry(db, body['request_key'], recovery_operation, key,
                                           body['request_sha256'], _canonical(request), body.get('retry_of'))
                            _check_object_fences(db, body['request_key'], recovery_operation, key,
                                                 body.get('retry_of'))
                        target = request['destination']
                        reconciled = None
                        try:
                            reconciled = (verify_isolated_candidate(target, write_receipt=receipt)
                                          if recovery_operation == 'recovery_prepare'
                                          else verify_restored_copy(target, write_receipt=receipt))
                        except Exception:
                            reconciled = None
                        if reconciled is not None:
                            result = {'status': 'prepared' if recovery_operation == 'recovery_prepare'
                                      else 'restored_copy_ready', 'check': reconciled['check'],
                                      'reconciled': True}
                        elif recovery_operation == 'recovery_prepare':
                            password = body.pop('password')
                            try:
                                result = prepare_encrypted_snapshot(request['source'], request['destination'], password,
                                    acknowledge_plaintext=True, write_receipt=receipt)
                            finally:
                                del password
                        else:
                            result = activate_isolated_candidate(request['candidate'], request['destination'],
                                acknowledge_plaintext=True, enable_restored_plans=True, write_receipt=receipt)
                        with closing(sqlite3.connect(self.server.db_path)) as db, db:
                            write = accept_source_write(db, request_key=body['request_key'], operation=recovery_operation,
                                request=request, request_sha256=body['request_sha256'], retry_of=body.get('retry_of'),
                                insert=lambda: result)
                        return self._json(200, {'result': result, 'write': write})
                    finally:
                        if 'password' in locals():
                            del password
                        self.server.restore_lock.release()
                if not self.server.restore_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有恢复操作正在进行，请稍后重试'})
                try:
                    if path.endswith('/prepare-candidate'):
                        if set(body) != {'source', 'destination', 'password', 'acknowledge'}:
                            raise ValueError('恢复候选请求字段无效')
                        if any(not isinstance(body[name], str) or not body[name].strip()
                               or len(body[name]) > 32767 or not Path(body[name]).is_absolute()
                               for name in ('source', 'destination')):
                            raise ValueError('请填写有效的本机绝对路径')
                        password = body.pop('password')
                        if body['acknowledge'] is not True:
                            raise ValueError('请确认将创建包含明文敏感资料的隔离候选')
                        result = prepare_encrypted_snapshot(body['source'], body['destination'], password,
                                                            acknowledge_plaintext=True)
                        del password
                        return self._json(200, result)
                    if path.endswith('/inspect'):
                        if set(body) != {'source', 'password'}:
                            raise ValueError('快照检查请求字段无效')
                        if (not isinstance(body['source'], str) or not body['source'].strip()
                                or len(body['source']) > 32767 or not Path(body['source']).is_absolute()):
                            raise ValueError('请填写有效的本机绝对路径')
                        password = body.pop('password')
                        result = inspect_encrypted_snapshot(body['source'], password)
                        del password
                        return self._json(200, result)
                    if path.endswith('/verify-candidate'):
                        if set(body) != {'candidate'}:
                            raise ValueError('候选复核请求字段无效')
                        if (not isinstance(body['candidate'], str) or not body['candidate'].strip()
                                or len(body['candidate']) > 32767 or not Path(body['candidate']).is_absolute()):
                            raise ValueError('请填写有效的本机绝对路径')
                        return self._json(200, verify_isolated_candidate(body['candidate']))
                    if path.endswith('/verify-restored-copy'):
                        if set(body) != {'candidate'}:
                            raise ValueError('恢复副本复核请求字段无效')
                        if (not isinstance(body['candidate'], str) or not body['candidate'].strip()
                                or len(body['candidate']) > 32767 or not Path(body['candidate']).is_absolute()):
                            raise ValueError('请填写有效的本机绝对路径')
                        candidate_path = Path(body['candidate']).resolve()
                        if (candidate_path == self.server.db_path
                                or (candidate_path.exists() and os.path.samefile(candidate_path, self.server.db_path))):
                            raise ValueError('当前运行数据库不能作为恢复副本核验目标')
                        return self._json(200, verify_restored_copy(body['candidate']))
                    if set(body) != {'candidate', 'destination', 'acknowledge', 'enable_restored_plans'}:
                        raise ValueError('恢复副本请求字段无效')
                    if any(not isinstance(body[name], str) or not body[name].strip()
                           or len(body[name]) > 32767 or not Path(body[name]).is_absolute()
                           for name in ('candidate', 'destination')):
                        raise ValueError('请填写有效的本机绝对路径')
                    if body['acknowledge'] is not True or body['enable_restored_plans'] is not True:
                        raise ValueError('必须分别确认明文副本及其中已启用计划可能开始执行')
                    destination = Path(body['destination']).absolute()
                    if destination.resolve() == self.server.db_path:
                        raise ValueError('恢复目标不能是当前运行数据库')
                    result = activate_isolated_candidate(
                        body['candidate'], destination, acknowledge_plaintext=True,
                        enable_restored_plans=True)
                    return self._json(200, result)
                finally:
                    if 'password' in locals():
                        del password
                    self.server.restore_lock.release()
            if path == '/api/generation-replay':
                if not self.server.replay_export_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有重放包正在生成，请稍后重试'})
                try:
                    bundle = build_generation_replay(self.server.db_path, body['pair_key'])
                    encoded = base64.b64encode(_canonical(bundle)).decode('ascii')
                finally:
                    self.server.replay_export_lock.release()
                return self._json(200, {'package_base64': encoded, 'bundle_sha256': bundle['bundle_sha256']})
            if path == '/api/paper-replay':
                if not self.server.replay_export_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有重放包正在生成，请稍后重试'})
                try:
                    bundle = build_paper_replay(self.server.db_path, body['experiment_key'])
                    encoded = base64.b64encode(_canonical(bundle)).decode('ascii')
                finally:
                    self.server.replay_export_lock.release()
                return self._json(200, {'package_base64': encoded, 'bundle_sha256': bundle['bundle_sha256']})
            if path == '/api/holdout-audit':
                if not self.server.holdout_audit_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有范围核对正在运行，请稍后重试'})
                try:
                    result = audit_holdout(self.server.db_path, body['snapshot_id'], body['start_issue'], body['end_issue'])
                finally:
                    self.server.holdout_audit_lock.release()
                return self._json(200, {'audit': result})
            if path == '/api/holdout-reservations':
                if not self.server.holdout_audit_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有范围核对或固定操作正在运行，请稍后重试'})
                try:
                    result = reserve_range_write(self.server.db_path, body['snapshot_id'], body['start_issue'],
                                                 body['end_issue'], confirmed=body.get('confirmed'),
                                                 request_key=body['request_key'], retry_of=body.get('retry_of'))
                finally:
                    self.server.holdout_audit_lock.release()
                return self._json(200, {'reservation': result['reservation'], 'write': result})
            if path == '/api/holdout-reservations/fence':
                if not self.server.holdout_audit_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有范围核对或固定操作正在运行，请稍后重试'})
                try:
                    write = fence_range(self.server.db_path, request_key=body['request_key'],
                                        request_sha256=body['request_sha256'], snapshot_id=body['snapshot_id'],
                                        start_issue=body['start_issue'], end_issue=body['end_issue'],
                                        confirmed=body.get('confirmed'), retry_of=body.get('retry_of'))
                finally:
                    self.server.holdout_audit_lock.release()
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': write, **({'error': '排除范围预留请求已封止，迟到提交将被拒绝'}
                                                     if write['status'] == 'fenced' else {})})
            if path == '/api/fixed-evaluations':
                request = {'reservation_id': body['reservation_id'], 'methods': body['methods'],
                           'hypothesis': body['hypothesis'], 'timeout': body['timeout'],
                           'confirmed': body.get('confirmed')}
                if body['request_sha256'] != _fixed_evaluation_write_digest('freeze', request):
                    raise ValueError('固定评估请求摘要不匹配')
                result = self.server.fixed_evaluations.freeze(body['reservation_id'], body['methods'],
                           hypothesis=body['hypothesis'], timeout=body['timeout'], confirmed=body.get('confirmed'),
                           request_key=body['request_key'], retry_of=body.get('retry_of'))
                write = self.server.fixed_evaluations.write_status(body['request_key'], 'freeze',
                                                                   body['request_sha256'])
                return self._json(200, {'evaluation': _safe_integers(result),
                                        'write': _safe_integers(write)})
            if path == '/api/fixed-evaluations/start':
                request = {'reservation_id': body['reservation_id'], 'retry': body.get('retry', False)}
                if body['request_sha256'] != _fixed_evaluation_write_digest('start', request):
                    raise ValueError('固定评估请求摘要不匹配')
                result = self.server.fixed_evaluations.start(body['reservation_id'], request_key=body['request_key'],
                            retry=body.get('retry', False), retry_of=body.get('retry_of'))
                write = self.server.fixed_evaluations.write_status(body['request_key'], 'start', body['request_sha256'])
                return self._json(200, {'evaluation': _safe_integers(result), 'write': _safe_integers(write)})
            if path == '/api/fixed-evaluations/recalculate-amounts':
                request = {'reservation_id': body['reservation_id']}
                if body['request_sha256'] != _fixed_evaluation_write_digest('recalculate_amounts', request):
                    raise ValueError('固定评估请求摘要不匹配')
                result = self.server.fixed_evaluations.recalculate_amounts(body['reservation_id'],
                            request_key=body['request_key'], retry_of=body.get('retry_of'))
                write = self.server.fixed_evaluations.write_status(body['request_key'], 'recalculate_amounts',
                                                                    body['request_sha256'])
                return self._json(200, {'evaluation': _safe_integers(result), 'write': _safe_integers(write)})
            if path == '/api/fixed-evaluations/supersede':
                request = {'reservation_id': body['reservation_id'], 'reason': body['reason'],
                           'confirmed': body.get('confirmed')}
                if body['request_sha256'] != _fixed_evaluation_write_digest('supersede', request):
                    raise ValueError('固定评估请求摘要不匹配')
                result = self.server.fixed_evaluations.supersede_pending(
                    body['reservation_id'], reason=body['reason'], confirmed=body.get('confirmed'),
                    request_key=body['request_key'], retry_of=body.get('retry_of'))
                write = self.server.fixed_evaluations.write_status(body['request_key'], 'supersede',
                                                                    body['request_sha256'])
                return self._json(200, {'reservation': result, 'write': _safe_integers(write)})
            if path == '/api/fixed-evaluation-writes/fence':
                write = self.server.fixed_evaluations.fence_write(body['request_key'], body['operation'],
                            body['request'], body['request_sha256'], retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': _safe_integers(write), **({'error': '固定评估请求已封止，迟到请求将被拒绝'}
                                                                      if write['status'] == 'fenced' else {})})
            if path == '/api/snapshots/import-bundled':
                source = ROOT/'research'/'data'/'official_page1.json'
                result = SnapshotStore(self.server.db_path).import_official(source.read_bytes(),
                          'local historical research file; source not revalidated', datetime.now(timezone.utc).isoformat(),
                          request_key=body['request_key'], retry_of=body.get('retry_of'))
                status = SnapshotStore(self.server.db_path).bundled_import_status(body['request_key'])
                return self._json(200, {**result, 'request_key': body['request_key'], 'write': status})
            if path == '/api/snapshots/import-bundled-fence':
                source = ROOT/'research'/'data'/'official_page1.json'
                write = SnapshotStore(self.server.db_path).fence_bundled_import(
                    body['request_key'], source.read_bytes(),
                    'local historical research file; source not revalidated', retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': write, **({'error': '本地快照导入请求已封止，迟到请求将被拒绝'}
                                                      if write['status'] == 'fenced' else {})})
            if path == '/api/jobs':
                result = self.server.jobs.submit(body['request_key'], name=body['name'], config=body['config'],
                                                 first_issue=body['first_issue'], timeout=body['timeout'],
                                                 trial_group_id=body.get('trial_group_id'),
                                                 retry_of=body.get('retry_of'))
                return self._json(200, result)
            if path == '/api/jobs/cancel':
                result = self.server.jobs.cancel(body['request_key'], body['cancel_request_key'])
                if result['status'] == 'rejected':
                    return self._json(404, {'error': '目标训练任务不存在', 'cancellation': result})
                return self._json(200, {'cancellation': result})
            if path == '/api/backtests':
                result = self.server.backtests.submit(body['request_key'], body['protocol'], body['timeout'],
                                                       retry_of=body.get('retry_of'))
                return self._json(200, _safe_integers(result))
            if path == '/api/backtests/cancel':
                result = self.server.backtests.cancel(body['request_key'], body['cancel_request_key'])
                if result['status'] == 'rejected':
                    return self._json(404, {'error': '目标回测任务不存在', 'cancellation': result})
                return self._json(200, {'cancellation': result})
            if path == '/api/update-plans':
                ident = self.server.scheduler.create_plan(body['config'], request_key=body['request_key'],
                                                          retry_of=body.get('retry_of'))
                return self._json(200, {'id': ident, 'enabled': False,
                                        'write': self.server.scheduler.write_request(body['request_key'])})
            if path == '/api/update-plans/set-enabled':
                result = self.server.scheduler.set_enabled(body['plan_id'], body['enabled'],
                                            request_key=body['request_key'], retry_of=body.get('retry_of'))
                return self._json(200, {'id': body['plan_id'], 'enabled': body['enabled'],
                                        'write': self.server.scheduler.write_request(body['request_key'])})
            if path == '/api/update-plans/run-now':
                ident = self.server.scheduler.create_one_off(body['plan_id'], request_key=body['request_key'],
                                                             retry_of=body.get('retry_of'))
                return self._json(200, {'id': ident, 'status': 'queued',
                                        'write': self.server.scheduler.write_request(body['request_key'])})
            if path == '/api/source-reviews':
                try:
                    document = base64.b64decode(body['document_base64'], validate=True)
                except (ValueError, TypeError) as exc:
                    raise ValueError('来源文件必须为有效 Base64') from exc
                request = {'kind': body['kind'], 'source_url': body['source_url'],
                           'document_sha256': body['document_sha256'], 'document_size': body['document_size'],
                           'document_name': body.get('document_name'), 'extracted': body['extracted'],
                           'reviewer_note': body['reviewer_note'], 'confirmed': body['confirmed']}
                if (len(document) != body['document_size']
                        or hashlib.sha256(document).hexdigest() != body['document_sha256']
                        or body['request_sha256'] != _source_write_digest(request)):
                    raise ValueError('来源原文或写入请求摘要与本机待确认记录不符')
                write = self.server.experiments.record_source_review(
                    kind=body['kind'], source_url=body['source_url'], document=document,
                    extracted=body['extracted'], reviewer_note=body['reviewer_note'], confirmed=body['confirmed'],
                    document_name=body.get('document_name'), write_key=body['request_key'],
                    request_sha256=body['request_sha256'], retry_of=body.get('retry_of'))
                return self._json(200, {'review_id': write['result']['review_id'], 'write': write})
            if path == '/api/source-review-capture':
                from dlt.prospective import _source_url
                from dlt.source_capture import SourceCaptureError, capture_document_bounded
                kind, source_url = body.get('kind'), body.get('source_url')
                if kind not in ('calendar', 'rule', 'tax') or not _source_url(source_url, kind):
                    raise ValueError('请使用允许的官方 HTTPS 来源网址')
                try:
                    captured = capture_document_bounded(source_url, kind)
                except SourceCaptureError as exc:
                    return self._json(502, {'error': str(exc)})
                return self._json(200, captured)
            if path == '/api/source-writes/fence':
                if body.get('operation') not in ('source_review', 'source_correction'):
                    raise ValueError('该来源写入接口不接受此操作类型')
                write = fence_source_write(self.server.db_path, request_key=body['request_key'],
                    operation=body['operation'], request=body['request'], request_sha256=body['request_sha256'],
                    retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                    {'write': write, **({'error': '来源写入请求已封止，迟到提交将被拒绝'}
                                       if write['status'] == 'fenced' else {})})
            if path == '/api/recovery-writes/fence':
                self.server.restore_lock.acquire()
                try:
                    self.server.experiments.initialize()
                    operation, request = body['operation'], body['request']
                    if operation not in ('recovery_prepare', 'recovery_activate'):
                        raise ValueError('恢复写入操作无效')
                    receipt = {'request_key': body['request_key'], 'operation': operation,
                               'request_sha256': body['request_sha256']}
                    target = request.get('destination')
                    recovered = None
                    if isinstance(target, str) and Path(target).is_absolute():
                        from dlt.recovery import verify_isolated_candidate, verify_restored_copy
                        try:
                            recovered = (verify_isolated_candidate(target, write_receipt=receipt)
                                         if operation == 'recovery_prepare'
                                         else verify_restored_copy(target, write_receipt=receipt))
                        except Exception:
                            recovered = None
                    with closing(sqlite3.connect(self.server.db_path)) as db, db:
                        if recovered is not None:
                            write = accept_source_write(db, request_key=body['request_key'], operation=operation,
                                request=request, request_sha256=body['request_sha256'], retry_of=body.get('retry_of'),
                                insert=lambda: {'status': 'prepared' if operation == 'recovery_prepare'
                                                else 'restored_copy_ready', 'check': recovered['check']})
                        else:
                            write = fence_source_write(self.server.db_path, request_key=body['request_key'],
                                operation=operation, request=request, request_sha256=body['request_sha256'],
                                retry_of=body.get('retry_of'))
                    return self._json(200 if write['status'] == 'accepted' else 409,
                        {'write': write, **({'error': '恢复写入请求已封止，迟到提交将被拒绝'}
                                            if write['status'] == 'fenced' else {})})
                finally:
                    self.server.restore_lock.release()
            if path == '/api/source-review-preview':
                if not self.server.source_preview_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有资料预览正在处理，请稍后重试'})
                try:
                    page = preview_source(self.server.db_path, body.get('review_id'), body.get('offset', 0))
                except SourcePreviewError as exc:
                    return self._json(exc.status, {'error': str(exc)})
                finally:
                    self.server.source_preview_lock.release()
                return self._json(200, {'preview': page})
            if path == '/api/source-corrections':
                if body.get('issue') is None:
                    write = self.server.experiments.declare_source_correction(
                        body['old_review_id'], body['new_review_id'], note=body['note'], confirmed=body['confirmed'],
                        write_key=body['request_key'], request_sha256=body['request_sha256'], retry_of=body.get('retry_of'))
                else:
                    write = self.server.experiments.declare_scoped_source_correction(
                        body['old_review_id'], body['new_review_id'], issue=body['issue'], note=body['note'],
                        confirmed=body['confirmed'], write_key=body['request_key'],
                        request_sha256=body['request_sha256'], retry_of=body.get('retry_of'))
                correction = write['result']
                try:
                    followup = PaperResults(self.server.db_path).process_source_correction(correction['correction_id'], scope=correction['scope'])
                except Exception as exc:
                    # The correction is already durable; the background queue
                    # will recover it. Do not report the declaration as rolled back.
                    followup = {'status': 'pending_or_unknown', 'reason': str(exc)[:1000]}
                return self._json(200, {'correction': correction, 'followup': followup, 'write': write})
            if path == '/api/source-corrections/retry':
                paper = PaperResults(self.server.db_path)
                result = paper.retry_source_correction(body['correction_id'], body.get('scope', 'global'),
                    request_key=body['request_key'], request_sha256=body['request_sha256'],
                    retry_of=body.get('retry_of'))
                return self._json(200, result)
            if path == '/api/experiments':
                protocol = body['protocol']
                if (not isinstance(protocol, dict) or protocol.get('observation_issues') != [protocol.get('target_issue')]
                        or not isinstance(protocol.get('replicas'), list) or not 1 <= len(protocol['replicas']) <= 100):
                    raise ValueError('当前仅支持一个目标奖期和 1 至 100 个副本')
                required = body.get('require_intent', False)
                if not isinstance(required, bool):
                    raise ValueError('准备记录参数无效')
                if not required:
                    raise ValueError('固定实验提交前必须先准备并保存本机待确认记录')
                content = self.server.experiments.freeze(body['request_key'], protocol, require_intent=True)
                digest = self.server.experiments._write_digest(self.server.experiments._protocol(protocol), None)
                return self._json(200, {'experiment': _safe_integers(content),
                                        'write': {'request_key': body['request_key'],
                                                  'request_sha256': digest, 'status': 'committed'}})
            if path == '/api/experiments/prepare':
                intent = self.server.experiments.prepare_write(body['request_key'], body['protocol'],
                                                               series_key=body.get('series_key'),
                                                               retry_of=body.get('retry_of'))
                return self._json(200, {'intent': intent})
            if path == '/api/experiments/fence':
                result = self.server.experiments.fence_write(body['request_key'], body['request_sha256'])
                return self._json(200, {'write': result})
            if path == '/api/experiment-series/fence':
                repository = SeriesRepository(self.server.db_path, clock=self.server.experiments.clock)
                write = repository.fence_create(body['request_key'], _series_manifest_wire(body['manifest']),
                                                retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'committed' else 409,
                                  {'write': write, **({'error': '实验版本请求已封止，迟到请求将被拒绝'}
                                                      if write['status'] == 'fenced' else {})})
            if path == '/api/experiment-series':
                content = SeriesRepository(self.server.db_path, clock=self.server.experiments.clock).create(body['series_key'],
                                                                        _series_manifest_wire(body['manifest']),
                                                                        retry_of=body.get('retry_of'))
                write = SeriesRepository(self.server.db_path, clock=self.server.experiments.clock).create_status(body['series_key'])
                return self._json(200, {'series': _safe_integers(content), 'write': write})
            if path == '/api/experiment-series/rolling-preview':
                if not self.server.rolling_preview_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有滚动训练预览正在运行，请稍后重试'})
                try:
                    spec = SeriesRepository(self.server.db_path, clock=self.server.experiments.clock).preview_rolling(body['pair_key'])
                finally:
                    self.server.rolling_preview_lock.release()
                return self._json(200, {'spec': spec})
            if path == '/api/experiment-series/periods':
                protocol = body['protocol']
                if (not isinstance(body.get('series_key'), str) or not body['series_key'].strip()
                        or not isinstance(protocol, dict) or not isinstance(protocol.get('replicas'), list)
                        or not 1 <= len(protocol['replicas']) <= 100):
                    raise ValueError('每期需要 1 至 100 个副本')
                required = body.get('require_intent', False)
                if not isinstance(required, bool):
                    raise ValueError('准备记录参数无效')
                if not required:
                    raise ValueError('固定系列期次前必须先准备并保存本机待确认记录')
                content = self.server.experiments.freeze(body['request_key'], protocol, series_key=body['series_key'],
                                                         require_intent=True)
                digest = self.server.experiments._write_digest(
                    self.server.experiments._protocol(protocol), body['series_key'])
                return self._json(200, {'experiment': _safe_integers(content),
                                        'write': {'request_key': body['request_key'],
                                                  'request_sha256': digest, 'status': 'committed'}})
            if path == '/api/experiment-series/close':
                request = {'object_key': body['object_key'], 'reason': body['reason']}
                operation = 'series_close'
                if body['request_sha256'] != _closure_write_digest(operation, request):
                    raise ValueError('实验关闭请求摘要不匹配')
                closure = SeriesRepository(self.server.db_path, clock=self.server.experiments.clock).close(
                    body['object_key'], body['reason'], write_key=body['request_key'], retry_of=body.get('retry_of'))
                write = {'status': 'accepted', 'request_key': body['request_key'], 'operation': operation,
                         'object_key': body['object_key'], 'request_sha256': body['request_sha256'],
                         'retry_of': body.get('retry_of'), 'result': closure}
                return self._json(200, {'closure': closure, 'write': write})
            if path == '/api/experiments/close':
                request = {'object_key': body['object_key'], 'reason': body['reason']}
                operation = 'experiment_close'
                if body['request_sha256'] != _closure_write_digest(operation, request):
                    raise ValueError('实验关闭请求摘要不匹配')
                closure = self.server.experiments.close(body['object_key'], body['reason'],
                    write_key=body['request_key'], retry_of=body.get('retry_of'))
                write = {'status': 'accepted', 'request_key': body['request_key'], 'operation': operation,
                         'object_key': body['object_key'], 'request_sha256': body['request_sha256'],
                         'retry_of': body.get('retry_of'), 'result': closure}
                return self._json(200, {'closure': closure, 'write': write})
            if path == '/api/prospective-closure-writes/fence':
                write = fence_closure_write(self.server.db_path, body['request_key'], body['operation'],
                    body['request'], body['request_sha256'], retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                    {'write': _safe_integers(write), **({'error': '实验关闭请求已封止，迟到提交将被拒绝'}
                                                       if write['status'] == 'fenced' else {})})
            if path == '/api/draw-conflicts/resolve':
                paper = PaperResults(self.server.db_path)
                resolution = paper.resolve_draw(body['issue'], body['snapshot_id'],
                                                expected_variants_sha256=body['variants_sha256'],
                                                expected_row_sha256=body['row_sha256'],
                                                note=body['note'], confirmed=body['confirmed'],
                                                request_key=body.get('request_key'),
                                                request_sha256=body.get('request_sha256'),
                                                retry_of=body.get('retry_of'))
                previous = (paper.write_status(body['request_key'], 'draw_resolve', body['request_sha256'])
                            if body.get('request_key') else {})
                saved = previous.get('result')
                if isinstance(saved, dict) and saved.get('resolution') == resolution and isinstance(saved.get('recalculations'), dict):
                    outcomes = saved['recalculations']
                else:
                    try:
                        outcomes = paper.process_issue(body['issue'], body['snapshot_id'])
                    except Exception as exc:
                        outcomes = {'dispatch': {'status': 'error', 'reason': str(exc)[:1000], 'recorded': False}}
                if body.get('request_key') and not (isinstance(saved, dict) and 'recalculations' in saved):
                    paper.record_draw_followup(body['request_key'], body['request_sha256'], resolution, outcomes)
                return self._json(200, {'resolution': resolution, 'recalculations': outcomes,
                                        'write': paper.write_status(body['request_key'], 'draw_resolve', body['request_sha256'])
                                        if body.get('request_key') else None,
                                        'assurance': 'personal review; origin not independently authenticated'})
            if path == '/api/paper-results/recalculate-issue':
                paper = PaperResults(self.server.db_path)
                review = paper.inspect_draw_versions(body['issue'])
                selected = review['resolution']
                variants = review['variants']
                single = (len(variants) == 1 and any(s['snapshot_id'] == body['snapshot_id']
                                                     for s in variants[0]['snapshots']))
                reviewed = selected and selected['selected_snapshot'] == body['snapshot_id']
                if not (single or reviewed):
                    raise PaperResultError('reviewed selection is missing or outdated')
                return self._json(200, {'recalculations': paper.recalculate_issue(
                    body['issue'], body['snapshot_id'], request_key=body.get('request_key'),
                    request_sha256=body.get('request_sha256'), retry_of=body.get('retry_of')),
                    **({'write': paper.write_status(body['request_key'], 'issue_recalculate', body['request_sha256'])}
                       if body.get('request_key') else {})})
            if path == '/api/paper-results/write-fence':
                write = PaperResults(self.server.db_path).fence_write(body['request_key'], body['operation'],
                    body['request'], body['request_sha256'], retry_of=body.get('retry_of'))
                return self._json(200, {'write': write})
            if path == '/api/pairs/fence':
                from dlt.pair_evaluation_writes import fence as fence_pair_evaluation_write, record_acceptance, status_in_transaction
                repo = PairRepository(self.server.db_path)
                repo._initialize()
                with closing(sqlite3.connect(self.server.db_path)) as db, db:
                    db.execute('PRAGMA foreign_keys=ON')
                    db.execute('BEGIN IMMEDIATE')
                    existing = db.execute('SELECT request_json,pair_sha256,pair_json FROM draft_pairs WHERE request_key=?',
                                          (body['request_key'],)).fetchone()
                    if existing:
                        request = json.loads(existing[0])
                        if request != body['request']:
                            raise CandidateError('request key already accepted for different draft input')
                        result = {'request_key': body['request_key'], 'request': request,
                                  'pair': PairRepository._decode(existing[1:])}
                        record_acceptance(db, 'pair_create', body['request_key'], request, result,
                                          retry_of=body.get('retry_of'))
                        write = status_in_transaction(db, 'pair_create', body['request_key'])
                    else:
                        write = fence_pair_evaluation_write(db, 'pair_create', body['request_key'],
                                                            body['request'], retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': write, **({'error': '组合生成请求已封止，迟到请求将被拒绝'}
                                                      if write['status'] == 'fenced' else {})})
            if path == '/api/evaluations/fence':
                from dlt.pair_evaluation_writes import fence as fence_pair_evaluation_write, record_acceptance, status_in_transaction
                repo = EvaluationRepository(self.server.db_path)
                repo._initialize()
                with closing(sqlite3.connect(self.server.db_path)) as db, db:
                    db.execute('PRAGMA foreign_keys=ON')
                    db.execute('BEGIN IMMEDIATE')
                    existing = db.execute('SELECT pair_key,request_json,result_json FROM draft_evaluations WHERE request_key=?',
                                          (body['request_key'],)).fetchone()
                    if existing:
                        evaluation_request = json.loads(existing[1])
                        scenario = evaluation_request['scenario']
                        dependency = scenario.get('source_dependency', {})
                        request = ({'pair_key': evaluation_request['pair_key'], 'scenario_kind': 'custom_payout_v1',
                                    'custom_input': scenario['custom_input'], 'samples': evaluation_request['samples'],
                                    'seed': evaluation_request['seed']} if scenario.get('kind') == 'custom_payout_v1'
                                 else {'pair_key': evaluation_request['pair_key'],
                                       'scenario_snapshot_id': dependency.get('snapshot_id'),
                                       'scenario_issue': dependency.get('issue'),
                                       'samples': evaluation_request['samples'], 'seed': evaluation_request['seed']})
                        if request != body['request']:
                            raise EvaluationError('request key already accepted for different evaluation input')
                        result = {'request_key': body['request_key'], 'request': evaluation_request,
                                  'result': json.loads(existing[2])}
                        record_acceptance(db, 'evaluation', body['request_key'], request, result,
                                          retry_of=body.get('retry_of'))
                        write = status_in_transaction(db, 'evaluation', body['request_key'])
                    else:
                        write = fence_pair_evaluation_write(db, 'evaluation', body['request_key'],
                                                            body['request'], retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': write, **({'error': '收益评估请求已封止，迟到请求将被拒绝'}
                                                      if write['status'] == 'fenced' else {})})
            if path == '/api/model-selection-decisions/fence':
                from dlt.pair_evaluation_writes import fence as fence_pair_evaluation_write, status_in_transaction
                repo = PairRepository(self.server.db_path)
                repo._initialize()
                with closing(sqlite3.connect(self.server.db_path)) as db, db:
                    db.execute('PRAGMA foreign_keys=ON')
                    db.execute('BEGIN IMMEDIATE')
                    write = fence_pair_evaluation_write(db, 'selection_decision', body['request_key'],
                                                        body['request'], retry_of=body.get('retry_of'))
                return self._json(200 if write['status'] == 'accepted' else 409,
                                  {'write': write, **({'error': '模型选择记录请求已封止，迟到请求将被拒绝'}
                                                      if write['status'] == 'fenced' else {})})
            if path == '/api/pairs':
                config = body['config']
                if (not isinstance(config, dict) or type(config.get('max_tickets')) is not int
                        or not 1 <= config['max_tickets'] <= 500
                        or type(config.get('max_sampling_attempts')) is not int
                        or not 1 <= config['max_sampling_attempts'] <= 20000
                        or type(config.get('budget_fen')) is not int or not 1 <= config['budget_fen'] <= 100000
                        or type(config.get('window')) is not int or not 1 <= config['window'] <= 200):
                    raise JobError('预算或生成资源限制超出支持范围')
                repo = PairRepository(self.server.db_path)
                repo.save_draft(body['request_key'], config, history_first_issue=body['history_first_issue'],
                                history_cutoff_issue=body['history_cutoff_issue'], artifact_id=body.get('artifact_id'),
                                prediction_at=body.get('prediction_at'), target_deadline=body.get('target_deadline'),
                                retry_of=body.get('retry_of'), selection=body.get('model_selection'),
                                write_request={'config': config, 'history_first_issue': body['history_first_issue'],
                                               'history_cutoff_issue': body['history_cutoff_issue'],
                                               'artifact_id': body.get('artifact_id'),
                                               'prediction_at': body.get('prediction_at'),
                                               'target_deadline': body.get('target_deadline'),
                                               **({'model_selection': body['model_selection']}
                                                  if body.get('model_selection') is not None else {})})
                return self._json(200, {'request_key': body['request_key'], **_pair_wire(repo.inspect(body['request_key'])),
                                       'selection_history': repo.selection_history(body['request_key'])})
            if path == '/api/model-selection-decisions':
                key, pair_key = body['request_key'], body['pair_key']
                result = PairRepository(self.server.db_path).append_selection_decision(
                    key, pair_key, body['selection'], expected_previous_seq=body.get('expected_previous_seq'),
                    retry_of=body.get('retry_of'), write_request={
                        'pair_key': pair_key, 'selection': body['selection'],
                        'expected_previous_seq': body.get('expected_previous_seq')})
                return self._json(200, result)
            if path == '/api/evaluations':
                key, pair_key = body['request_key'], body['pair_key']
                kind = body.get('scenario_kind', 'historical')
                snapshot_id = body.get('scenario_snapshot_id')
                samples, seed = body['samples'], body['seed']
                scenario_issue = body.get('scenario_issue', '26123')
                if (not isinstance(key, str) or not key.strip() or len(key) > 128
                        or not isinstance(pair_key, str) or not pair_key.strip() or len(pair_key) > 128
                        or kind not in ('historical', 'custom_payout_v1')
                        or (kind == 'historical' and (not isinstance(snapshot_id, str) or len(snapshot_id) != 64
                            or not isinstance(scenario_issue, str) or not re.fullmatch(r'\d{5}', scenario_issue)))
                        or type(samples) is not int or not 1000 <= samples <= 20000
                        or type(seed) is not int or not 0 <= seed <= 4294967295):
                    raise JobError('评估标识、快照、模拟次数或种子无效')
                repo = EvaluationRepository(self.server.db_path)
                try:
                    old = repo.get_request(key)
                except KeyError:
                    old = None
                if old:
                    dependency = old['scenario'].get('source_dependency', {})
                    if kind == 'custom_payout_v1':
                        pair = PairRepository(self.server.db_path).get(pair_key)
                        same_scenario = old['scenario'] == custom_scenario(pair, body.get('custom_input'))
                    else:
                        same_scenario = (dependency.get('snapshot_id') == snapshot_id and dependency.get('issue') == scenario_issue)
                    if (old['pair_key'] != pair_key or old['samples'] != samples or old['seed'] != seed
                            or not same_scenario):
                        raise JobError('请求标识已用于不同的评估配置')
                    return self._json(200, {'request_key': key, 'request': old, **_evaluation_wire(repo.inspect(key))})
                if not self.server.evaluation_lock.acquire(blocking=False):
                    return self._json(409, {'error': '已有评估正在计算，请稍后重试'})
                try:
                    pair = PairRepository(self.server.db_path).get(pair_key)
                    scenario = (custom_scenario(pair, body.get('custom_input')) if kind == 'custom_payout_v1'
                                else historical_scenario(self.server.db_path, snapshot_id, pair, scenario_issue))
                    dependency = scenario.get('source_dependency', {})
                    write_request = ({'pair_key': pair_key, 'scenario_kind': 'custom_payout_v1',
                                     'custom_input': scenario['custom_input'], 'samples': samples, 'seed': seed}
                                    if scenario.get('kind') == 'custom_payout_v1'
                                    else {'pair_key': pair_key,
                                          'scenario_snapshot_id': dependency.get('snapshot_id'),
                                          'scenario_issue': dependency.get('issue'),
                                          'samples': samples, 'seed': seed})
                    repo.run(key, pair_key, scenario=scenario, samples=samples, seed=seed,
                             max_samples=20000, max_ticket_draws=1000000,
                             retry_of=body.get('retry_of'), write_request=write_request)
                    return self._json(200, {'request_key': key, 'request': repo.get_request(key),
                                            **_evaluation_wire(repo.inspect(key))})
                finally:
                    self.server.evaluation_lock.release()
            return self._json(404, {'error': '未找到操作'})
        except ScheduleFenced as exc:
            write = {'request_key': exc.request_key, 'operation': exc.operation, 'request': exc.request,
                     'retry_of': exc.retry_of, 'status': 'fenced'}
            return self._json(409, {'error': str(exc), 'write': write})
        except OriginFenced as exc:
            write = {'status': 'fenced', 'event_id': exc.event_id,
                     'request_sha256': exc.request_sha256, 'retry_of': exc.retry_of}
            return self._json(409, {'error': str(exc), 'write': write})
        except SourceWriteFenced as exc:
            write = {'status': 'fenced', 'request_key': exc.request_key, 'operation': exc.operation,
                     'request_sha256': exc.request_sha256, 'retry_of': exc.retry_of}
            return self._json(409, {'error': str(exc), 'write': write})
        except ClosureFenced as exc:
            write = {'status': 'fenced', 'request_key': exc.request_key, 'operation': exc.operation,
                     'request_sha256': exc.request_sha256, 'retry_of': exc.retry_of}
            return self._json(409, {'error': str(exc), 'write': write})
        except ReservationFenced as exc:
            write = {'status': 'fenced', 'request_key': exc.request_key,
                     'request_sha256': exc.request_sha256, 'retry_of': exc.retry_of}
            return self._json(409, {'error': str(exc), 'write': write})
        except FixedEvaluationFenced as exc:
            write = {'status': 'fenced', 'request_key': exc.request_key,
                     'operation': exc.operation, 'request_sha256': exc.request_sha256,
                     'retry_of': exc.retry_of}
            return self._json(409, {'error': str(exc), 'write': write})
        except KeyError as exc:
            return self._json(400, {'error': f'缺少或无效的字段：{exc}'})
        except (ValueError, JobError) as exc:
            return self._json(400, {'error': str(exc)[:1000]})
        except Exception:
            return self._json(500, {'error': '本机服务执行失败，请检查任务记录'})


def main():
    parser = argparse.ArgumentParser(description='Start the local research browser service.')
    parser.add_argument('--db', default=str(ROOT/'runtime'/'research.sqlite3'))
    parser.add_argument('--port', type=int, default=8766)
    args = parser.parse_args()
    supplied_path = Path(args.db)
    reject_database_reparse_path(supplied_path)
    path = supplied_path.resolve()
    # Reject quarantined or exposed databases before creating the single-instance
    # lock file. LocalServer repeats this check before schema initialization.
    prepare_runtime_database_path(supplied_path)
    with SingleInstance(path):
        server = LocalServer(args.port, supplied_path)
        server.jobs.recover_interrupted()
        server.backtests.recover_interrupted()
        server.fixed_evaluations.recover_interrupted()
        server.update_loop.start()
        server.rolling_loop.start()
        print(f'本机研究服务：{server.origin}', flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.update_loop.stop(); server.rolling_loop.stop(); server.shutdown(); server.server_close(); server.jobs.stop(); server.backtests.stop(); server.fixed_evaluations.stop()


if __name__ == '__main__':
    main()
