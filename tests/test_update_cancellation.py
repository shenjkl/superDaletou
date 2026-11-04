from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dlt.official import SourceError
from dlt.recovery_guard import RecoveryQuarantined
from dlt.scheduler import Scheduler
from test_backtest import source_data
from test_scheduler import configuration


class UpdateCancellationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'research.sqlite3'
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.scheduler = Scheduler(self.path, clock=lambda: self.now)
        self.plan = self.scheduler.create_plan(configuration())
        self.scheduler.set_enabled(self.plan, True)
        self.claim = self.scheduler.claim(self.plan)
        self.raw = json.dumps(source_data()).encode()

    def saved(self):
        return self.scheduler.list_runs(self.plan)[0]

    def import_fixture(self, store, *, transaction_guard, **kwargs):
        return store.import_official(self.raw, 'synthetic source', self.now.isoformat(),
                                     transaction_guard=transaction_guard)

    def test_cancel_before_fetch_is_idempotent_and_keeps_schedule_enabled(self):
        run_id = self.claim['run_id']
        self.assertEqual(self.scheduler.cancel_run(self.plan, run_id)['status'], 'cancelling')
        self.assertEqual(self.scheduler.cancel_run(self.plan, run_id)['status'], 'cancelling')
        with patch('socket.socket', side_effect=AssertionError('no socket')):
            self.assertIsNone(self.scheduler.execute(self.claim, updater=lambda *a, **k: self.fail('must not fetch')))
        self.assertEqual(self.saved()['status'], 'cancelled')
        self.assertEqual(self.scheduler.cancel_run(self.plan, run_id)['status'], 'cancelled')
        self.assertTrue(self.scheduler.list_plans()[0]['enabled'])
        self.assertEqual(self.saved()['attempts'], [])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0], 0)

    def test_cancel_during_fetch_prevents_snapshot_publication(self):
        entered, release = threading.Event(), threading.Event()
        def delayed(store, *, transaction_guard, **kwargs):
            entered.set()
            self.assertTrue(release.wait(10))
            return self.import_fixture(store, transaction_guard=transaction_guard)
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(self.scheduler.execute, self.claim, updater=delayed)
            self.assertTrue(entered.wait(10))
            self.scheduler.cancel_run(self.plan, self.claim['run_id'])
            self.assertIsNone(self.scheduler.claim(self.plan))
            release.set()
            self.assertIsNone(result.result(timeout=10))
        self.assertEqual(self.saved()['status'], 'cancelled')
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0], 0)

    def test_cancel_after_snapshot_commit_preserves_it_without_success_followup(self):
        original = self.scheduler._record_attempt
        def record_then_cancel(claim, number, status, detail):
            original(claim, number, status, detail)
            self.scheduler.cancel_run(self.plan, claim['run_id'])
        with patch.object(self.scheduler, '_record_attempt', side_effect=record_then_cancel):
            self.assertIsNone(self.scheduler.execute(self.claim, updater=self.import_fixture))
        self.scheduler.process_pending_followups()
        run = self.saved()
        self.assertEqual(run['status'], 'cancelled')
        self.assertEqual(run['attempts'][0]['status'], 'success')
        self.assertFalse(run['followups'])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0], 1)

    def test_completed_run_is_not_overwritten_by_late_cancel(self):
        result = self.scheduler.execute(self.claim, updater=self.import_fixture)
        self.assertTrue(result['snapshot_id'])
        self.assertEqual(self.scheduler.cancel_run(self.plan, self.claim['run_id'])['status'], 'success')
        self.assertEqual(self.saved()['status'], 'success')

    def test_cancel_interrupts_retry_wait_before_next_fetch(self):
        calls, sleeps = [], []
        def failed(*args, **kwargs):
            calls.append(1)
            raise SourceError('fixture')
        def sleep(seconds):
            sleeps.append(seconds)
            self.now += timedelta(seconds=seconds)
            self.scheduler.cancel_run(self.plan, self.claim['run_id'])
        self.assertIsNone(self.scheduler.execute(self.claim, updater=failed, sleep=sleep))
        self.assertEqual(len(calls), 1)
        self.assertEqual(sleeps, [1])
        self.assertEqual(self.saved()['status'], 'cancelled')
        self.assertEqual(self.saved()['attempts'][0]['status'], 'failed')

    def test_crashed_cancelled_worker_is_finalized_after_lease_without_oneoff_retry(self):
        one = self.scheduler.create_one_off(self.plan)
        claim = self.scheduler.claim(one)
        self.scheduler.cancel_run(one, claim['run_id'])
        self.assertIsNone(self.scheduler.claim(one))
        self.now += timedelta(seconds=100)
        self.assertIsNone(self.scheduler.claim(one))
        runs = self.scheduler.list_runs(one)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]['status'], 'cancelled')
        self.assertFalse(next(x for x in self.scheduler.list_plans() if x['id'] == one)['enabled'])

    def test_cancellation_cannot_cross_plan_or_execute_on_quarantined_copy(self):
        with self.assertRaises(KeyError):
            self.scheduler.cancel_run('wrong-plan', self.claim['run_id'])
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE recovery_quarantine(id INTEGER)')
        with self.assertRaises(RecoveryQuarantined):
            self.scheduler.cancel_run(self.plan, self.claim['run_id'])

    def test_http_handler_requires_origin_session_and_csrf(self):
        from dlt.app import Handler
        def call(csrf):
            body = json.dumps({'plan_id': self.plan, 'run_id': self.claim['run_id']}).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', scheduler=self.scheduler)
            handler.path = '/api/update-runs/cancel'
            handler.headers = {'Origin': handler.server.origin, 'X-DLT-CSRF': csrf,
                               'Content-Type': 'application/json', 'Content-Length': str(len(body))}
            handler.rfile = BytesIO(body)
            handler._validate_host = lambda: True
            handler._session = lambda: True
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        self.assertEqual(call('bad')[0], 403)
        self.assertEqual(self.saved()['status'], 'running')
        self.assertEqual(call('secret')[1]['run']['status'], 'cancelling')


if __name__ == '__main__':
    unittest.main()
