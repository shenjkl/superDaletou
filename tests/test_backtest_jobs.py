import json
from pathlib import Path
import sys
import tempfile
import time
import unittest

from dlt.backtest_jobs import BacktestJobs
from dlt.snapshots import SnapshotStore
from dlt.training_jobs import JobError
from test_backtest import protocol


class BacktestJobTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'db.sqlite3'
        raw = (Path(__file__).resolve().parents[1] / 'research/data/official_page1.json').read_bytes()
        self.snapshot = SnapshotStore(self.path).import_official(raw, 'local history',
                                                               '2026-10-29T12:00:00+08:00')['snapshot_id']
        self.jobs = BacktestJobs(self.path)
        self.jobs.initialize()
        self.addCleanup(self.jobs.stop)
        self.cfg = protocol(self.snapshot)
        self.cfg.update(targets=['26122', '26123'], replicas=['a', 'b'], history_periods=12)
        self.cfg['candidate'].update(budget_fen=400, max_tickets=10)

    def wait(self, key):
        for _ in range(200):
            job = self.jobs.get(key)
            if job['status'] != 'running':
                return job
            time.sleep(.05)
        self.fail('job did not complete')

    def test_real_worker_persists_matched_period_results_and_idempotence(self):
        self.jobs.submit('first', self.cfg, 30)
        result = self.wait('first')
        self.assertEqual(result['status'], 'complete', result.get('error'))
        report = result['result']['report']
        self.assertEqual((report['planned_periods'], report['complete_periods']), (2, 2))
        self.assertEqual(result['result']['status'], 'research_only')
        self.assertEqual(self.jobs.submit('first', self.cfg, 30)['status'], 'complete')
        with self.assertRaisesRegex(JobError, '不同配置'):
            self.jobs.submit('first', self.cfg, 29)

    def test_cancel_and_restart_never_publish_report(self):
        self.jobs.worker_command = [sys.executable, '-c', 'import time; time.sleep(5)']
        self.jobs.submit('cancelled', self.cfg, 30)
        cancellation = self.jobs.cancel('cancelled', 'cancel-request-1')
        self.assertEqual((cancellation['status'], cancellation['target_status']), ('accepted', 'cancelled'))
        time.sleep(.2)
        self.assertEqual(self.jobs.get('cancelled')['status'], 'cancelled')
        with self.assertRaises(KeyError):
            from dlt.backtest_repository import BacktestRepository
            BacktestRepository(self.path).get('cancelled')
        self.jobs.submit('interrupted', self.cfg, 30)
        self.jobs.recover_interrupted()
        self.assertEqual(self.jobs.get('interrupted')['status'], 'failed')

    def test_rejects_unbounded_work_and_holdout_claim(self):
        with self.assertRaises(JobError):
            self.jobs.submit('holdout', {**self.cfg, 'selection_role': 'holdout'}, 30)
        bad = json.loads(json.dumps(self.cfg)); bad['limits']['max_targets'] = 100000
        with self.assertRaises(JobError):
            self.jobs.submit('limits', bad, 30)

    def test_stop_waits_for_worker_cleanup(self):
        self.jobs.worker_command = [sys.executable, '-c', 'import time; time.sleep(10)']
        self.jobs.submit('stop-running', self.cfg, 30)
        self.jobs.stop()
        self.assertEqual(self.jobs.get('stop-running')['status'], 'failed')
        with self.jobs.lock:
            self.assertFalse(self.jobs.threads)
            self.assertFalse(self.jobs.processes)


if __name__ == '__main__':
    unittest.main()
