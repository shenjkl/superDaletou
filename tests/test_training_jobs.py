import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from dlt.model_repository import ModelRepository
from dlt.models import ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION
from dlt.snapshots import SnapshotStore
from dlt.training_jobs import JobError, TrainingJobs
from dlt.training_boundary import check_holdout_boundary, TrainingBoundaryError
from test_backtest import source_data


class TrainingJobTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'研究.sqlite3'
        self.source = source_data()
        self.snapshot = SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'fixture',
                                                                '2026-02-01T12:00:00+08:00')['snapshot_id']
        self.config = {'method': 'tree', 'window': 3, 'train_end': '26014', 'cutoff': '26020',
                       'availability_mode': 'historical_reconstruction', 'snapshot_id': self.snapshot, 'seed': 42,
                       'algorithm_version': ALGORITHM_VERSION['tree'],
                       'feature_schema_version': FEATURE_SCHEMA_VERSION,
                       'preprocessing_version': PREPROCESSING_VERSION['tree'],
                       'parameters': {'max_depth': 2, 'min_leaf': 2}}
        self.jobs = TrainingJobs(self.path)
        self.jobs.initialize()
        self.addCleanup(self.jobs.stop)

    def wait_terminal(self, key):
        deadline = time.monotonic()+20
        while time.monotonic() < deadline:
            result = self.jobs.get(key)
            if result['status'] != 'running': return result
            time.sleep(.05)
        self.fail('training did not reach a terminal state')

    def test_real_training_produces_durable_artifact_and_reuses_key(self):
        args = {'name': '树模型研究', 'config': self.config, 'first_issue': '26001', 'timeout': 15}
        self.jobs.submit('job-1', **args)
        result = self.wait_terminal('job-1')
        self.assertEqual(result['status'], 'complete', result.get('error'))
        self.assertEqual(ModelRepository(self.path).get(result['artifact_id'])['config'], self.config)
        self.assertEqual(self.jobs.submit('job-1', **args)['artifact_id'], result['artifact_id'])
        with self.assertRaisesRegex(JobError, '不同配置'):
            self.jobs.submit('job-1', **{**args, 'name': '其他名称'})

    def test_cancel_kills_worker_and_prevents_artifact_publication(self):
        self.jobs.worker_command = [sys.executable, '-c', 'import time; time.sleep(20)']
        self.jobs.submit('job-2', name='任务', config=self.config, first_issue='26001', timeout=15)
        deadline = time.monotonic()+4
        while time.monotonic() < deadline:
            with self.jobs.lock:
                if 'job-2' in self.jobs.processes: break
            time.sleep(.02)
        cancellation = self.jobs.cancel('job-2', 'cancel-job-2')
        self.assertEqual((cancellation['status'], cancellation['target_status']), ('accepted', 'cancelled'))
        time.sleep(.2)
        self.assertIsNone(self.jobs.get('job-2')['artifact_id'])

    def test_single_active_task_and_timeout_are_enforced(self):
        self.jobs.worker_command = [sys.executable, '-c', 'import time; time.sleep(20)']
        self.jobs.submit('slow', name='慢任务', config=self.config, first_issue='26001', timeout=1)
        with self.assertRaisesRegex(JobError, '已有训练任务'):
            self.jobs.submit('other', name='其他', config=self.config, first_issue='26001', timeout=5)
        result = self.wait_terminal('slow')
        self.assertEqual(result['status'], 'failed')
        self.assertIn('时间上限', result['error'])
        self.assertIsNone(result['artifact_id'])

    def test_stop_waits_for_worker_cleanup(self):
        self.jobs.worker_command = [sys.executable, '-c', 'import time; time.sleep(10)']
        self.jobs.submit('stopping', name='训练', config=self.config, first_issue='26001', timeout=15)
        self.jobs.stop()
        self.assertEqual(self.jobs.get('stopping')['status'], 'failed')
        with self.jobs.lock:
            self.assertFalse(self.jobs.threads)
            self.assertFalse(self.jobs.processes)

    def test_conflict_during_training_prevents_publishing(self):
        self.jobs.worker_command = [sys.executable, '-c',
            "import time,runpy; time.sleep(1); runpy.run_module('dlt.training_worker',run_name='__main__')"]
        self.jobs.submit('conflict', name='冲突检查', config=self.config, first_issue='26001', timeout=15)
        self.source['value']['list'][0]['lotteryDrawResult'] = '10 11 12 13 14 03 04'
        SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'fixture2',
                                                 '2026-02-02T12:00:00+08:00')
        result = self.wait_terminal('conflict')
        self.assertEqual(result['status'], 'failed')
        self.assertIn('conflicts', result['error'])
        self.assertIsNone(result['artifact_id'])

    def test_declared_holdout_excludes_future_range_and_is_saved(self):
        cfg = {**self.config, 'cutoff': '26017', 'holdout_start': '26018', 'holdout_end': '26020'}
        self.jobs.submit('excluded', name='训练', config=cfg, first_issue='26001', timeout=15)
        result = self.wait_terminal('excluded')
        self.assertEqual(result['status'], 'complete', result.get('error'))
        artifact = ModelRepository(self.path).get(result['artifact_id'])
        self.assertEqual(artifact['config']['holdout_start'], '26018')
        self.assertEqual(artifact['config']['holdout_end'], '26020')
        self.assertTrue({'26018', '26019', '26020'}.isdisjoint(artifact['train_targets']))
        self.assertTrue({'26018', '26019', '26020'}.isdisjoint(artifact['validation_targets']))

    def test_invalid_or_leaking_declared_holdout_is_rejected_before_task(self):
        variants = [
            {'holdout_start': '26018'},
            {'holdout_start': '26018', 'holdout_end': '26020'},
            {'cutoff': '26017', 'holdout_start': '26020', 'holdout_end': '26018'},
            {'cutoff': '26017', 'holdout_start': '26018', 'holdout_end': '26999'},
        ]
        for index, changes in enumerate(variants):
            with self.subTest(changes=changes), self.assertRaises(JobError):
                self.jobs.submit('invalid-'+str(index), name='训练', config={**self.config, **changes},
                                 first_issue='26001', timeout=15)
        self.assertEqual(self.jobs.list(), [])

    def test_direct_model_repository_checks_declared_holdout(self):
        leaking = {**self.config, 'holdout_start': '26018', 'holdout_end': '26020'}
        with self.assertRaisesRegex(ValueError, '排除'):
            ModelRepository(self.path).train_from_snapshot(self.snapshot, leaking, first_issue='26001')

    def test_worker_rejects_leak_even_when_called_directly(self):
        leaking = {**self.config, 'holdout_start': '26018', 'holdout_end': '26020'}
        payload = json.dumps({'path': str(self.path), 'first_issue': '26001', 'config': leaking}).encode()
        process = subprocess.run([sys.executable, '-m', 'dlt.training_worker'], input=payload,
                                 capture_output=True, timeout=10)
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(process.stdout, b'')

    def test_holdout_boundary_uses_snapshot_order_across_year_rollover(self):
        issues = ['99122', '99123', '00001', '00002']
        check_holdout_boundary(issues, '99122', {'cutoff': '99123', 'holdout_start': '00001', 'holdout_end': '00002'})
        with self.assertRaises(TrainingBoundaryError):
            check_holdout_boundary(issues, '99122', {'cutoff': '00001', 'holdout_start': '00001', 'holdout_end': '00002'})


if __name__ == '__main__':
    unittest.main()
