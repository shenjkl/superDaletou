from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from dlt.official import SourceError, fetch_latest, fetch_latest_bounded
from dlt.scheduler import ScheduleError, Scheduler
from test_backtest import source_data


RAW = json.dumps(source_data()).encode()


class Response:
    def __init__(self, request, raw=RAW, status=200, url=None, encoding='identity'):
        self.status = status
        self.headers = {'Content-Encoding': encoding}
        self.url = url or request.full_url
        self.stream = io.BytesIO(raw)
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def geturl(self): return self.url
    def read(self, size): return self.stream.read(size)


class Opener:
    def __init__(self, **kwargs): self.kwargs = kwargs
    def open(self, request, timeout): return Response(request, **self.kwargs)


def configuration():
    return {'start_at': '2026-01-01T00:00:00+00:00', 'interval_seconds': 60, 'timezone_offset_minutes': 480,
            'page_size': 20, 'timeout_seconds': 2, 'max_bytes': 100000, 'retry_count': 2, 'retry_wait_seconds': 1}


class AdapterTests(unittest.TestCase):
    def test_valid_response_is_bounded_and_validated(self):
        result = fetch_latest(page_size=20, timeout_seconds=2, max_bytes=100000, opener=Opener())
        self.assertEqual(result['raw'], RAW)
        self.assertEqual(result['row_count'], 20)
        self.assertIn('no completeness', result['scope'])

    def test_redirect_oversize_invalid_and_bad_encoding_fail(self):
        for opener, limit in [(Opener(url='https://example.com/'), 100000), (Opener(), 5),
                              (Opener(raw=b'bad json'), 100000), (Opener(encoding='gzip'), 100000)]:
            with self.subTest(opener=opener.kwargs), self.assertRaises(SourceError):
                fetch_latest(page_size=20, timeout_seconds=2, max_bytes=limit, opener=opener)

    def test_overall_network_process_deadline_is_enforced(self):
        with patch('dlt.official.subprocess.run', side_effect=subprocess.TimeoutExpired('source', 2)):
            with self.assertRaisesRegex(SourceError, 'timed out'):
                fetch_latest_bounded(page_size=20, timeout_seconds=2, max_bytes=100000)


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'data.sqlite3'
        self.current = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.scheduler = Scheduler(self.path, clock=lambda: self.current)
        self.plan = self.scheduler.create_plan(configuration())

    def advance(self, seconds): self.current += timedelta(seconds=seconds)

    def update_fixture(self, store, *, transaction_guard, **kwargs):
        return store.import_official(RAW, 'synthetic fixture', self.current.isoformat(), transaction_guard=transaction_guard)

    def test_paused_by_default_coalesces_missed_and_deduplicates_claims(self):
        self.assertIsNone(self.scheduler.claim(self.plan))
        self.advance(300)
        self.scheduler.set_enabled(self.plan, True)
        with ThreadPoolExecutor(max_workers=6) as pool:
            claims = list(pool.map(lambda _: self.scheduler.claim(self.plan), range(6)))
        claimed = [c for c in claims if c]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(self.scheduler.list_plans()[0]['next_due'], '2026-01-01T00:06:00+00:00')
        result = self.scheduler.execute(claimed[0], updater=self.update_fixture, sleep=self.advance)
        self.assertTrue(result['snapshot_id'])
        self.assertIsNone(self.scheduler.claim(self.plan))
        self.assertEqual(len(self.scheduler.list_runs(self.plan)), 1)

    def test_retries_record_failures_and_exhaustion_without_disabling_plan(self):
        self.scheduler.set_enabled(self.plan, True)
        claim = self.scheduler.claim(self.plan)
        def fail(*args, **kwargs): raise SourceError('offline')
        self.assertIsNone(self.scheduler.execute(claim, updater=fail, sleep=self.advance))
        run = self.scheduler.list_runs(self.plan)[0]
        self.assertEqual(run['status'], 'failed')
        self.assertEqual([r['status'] for r in run['attempts']], ['failed']*3)
        self.assertTrue(self.scheduler.list_plans()[0]['enabled'])
        self.advance(58)
        self.assertIsNotNone(self.scheduler.claim(self.plan))

    def test_expired_worker_is_fenced_from_snapshot_import(self):
        self.scheduler.set_enabled(self.plan, True)
        old = self.scheduler.claim(self.plan)
        self.advance(100)
        replacement = self.scheduler.claim(self.plan)
        self.assertTrue(replacement['recovered'])
        with self.assertRaisesRegex(ScheduleError, 'ownership'):
            self.scheduler.execute(old, updater=self.update_fixture, sleep=self.advance)
        self.scheduler.execute(replacement, updater=self.update_fixture, sleep=self.advance)
        self.assertEqual([r['status'] for r in self.scheduler.list_runs(self.plan)], ['interrupted', 'success'])

    def test_pausing_prevents_dispatch_and_missing_fields_are_rejected(self):
        bad = configuration(); del bad['timeout_seconds']
        with self.assertRaisesRegex(ScheduleError, 'timeout_seconds'):
            self.scheduler.create_plan(bad)
        self.scheduler.set_enabled(self.plan, True)
        self.scheduler.set_enabled(self.plan, False)
        self.advance(100)
        self.assertIsNone(self.scheduler.claim(self.plan))

    def test_manual_one_off_is_durable_and_does_not_change_paused_plan(self):
        before = self.scheduler.list_plans()[0]
        manual = self.scheduler.create_one_off(self.plan)
        plans = {p['id']: p for p in self.scheduler.list_plans()}
        self.assertFalse(plans[self.plan]['enabled'])
        self.assertEqual(plans[self.plan]['next_due'], before['next_due'])
        self.assertTrue(plans[manual]['enabled'])
        self.assertEqual(plans[manual]['config']['parent_plan_id'], self.plan)
        claim = self.scheduler.claim(manual)
        self.assertFalse({p['id']: p for p in self.scheduler.list_plans()}[manual]['enabled'])
        self.scheduler.execute(claim, updater=self.update_fixture, sleep=self.advance)
        self.advance(1000)
        self.assertIsNone(self.scheduler.claim(manual))
        self.assertEqual(self.scheduler.list_runs(manual)[0]['status'], 'success')

    def test_manual_one_off_recovers_expired_worker_without_rescheduling(self):
        manual = self.scheduler.create_one_off(self.plan)
        abandoned = self.scheduler.claim(manual)
        self.advance(100)
        replacement = self.scheduler.claim(manual)
        self.assertTrue(replacement['recovered'])
        with self.assertRaisesRegex(ScheduleError, 'ownership'):
            self.scheduler.execute(abandoned, updater=self.update_fixture, sleep=self.advance)
        self.scheduler.execute(replacement, updater=self.update_fixture, sleep=self.advance)
        self.assertEqual([r['status'] for r in self.scheduler.list_runs(manual)], ['interrupted', 'success'])
        self.advance(100)
        self.assertIsNone(self.scheduler.claim(manual))

    def test_separate_worker_runs_without_browser_and_persists_outcome(self):
        cfg = {**configuration(), 'start_at': (datetime.now(timezone.utc)-timedelta(seconds=2)).isoformat(),
               'retry_count': 0}
        real = Scheduler(self.path)
        plan = real.create_plan(cfg)
        real.set_enabled(plan, True)
        process = subprocess.Popen([sys.executable, '-m', 'dlt.worker', '--db', str(self.path), 'run'],
                                   cwd=str(Path(__file__).resolve().parents[1]),
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic()+15
            runs = []
            while time.monotonic() < deadline:
                self.assertIsNone(process.poll(), 'background worker exited unexpectedly')
                runs = real.list_runs(plan)
                if runs and runs[-1]['status'] in ('success', 'failed'):
                    break
                time.sleep(.1)
            self.assertTrue(runs and runs[-1]['status'] in ('success', 'failed'), 'no terminal update run')
            self.assertTrue(runs[-1]['attempts'])
        finally:
            process.terminate()
            process.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
