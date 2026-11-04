from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import sys
import time
import unittest
from unittest.mock import patch

from dlt.integrity import check_database
from dlt.rolling_training import RollingPlanError, RollingTraining
from dlt.snapshots import SnapshotStore
from dlt.training_jobs import TrainingJobs
import test_rolling_policy as fixtures

ROLLING_TRAINING_SERIES_KEY = '88888888-8888-4888-8888-888888888888'


class RollingTrainingTests(unittest.TestCase):
    setUpBase = fixtures.RollingPolicyTests.setUp
    review = fixtures.RollingPolicyTests.review
    import_draw = fixtures.RollingPolicyTests.import_draw
    _db = fixtures.RollingPolicyTests._db
    prepare_second = fixtures.RollingPolicyTests.prepare_second
    train = fixtures.RollingPolicyTests.train

    def setUp(self):
        self.setUpBase()
        self.series.create(ROLLING_TRAINING_SERIES_KEY, self.manifest)
        self.scheduler = RollingTraining(self.path, self.jobs, clock=lambda: self.now)
        self.scheduler.initialize()
        self.args = {'series_key': ROLLING_TRAINING_SERIES_KEY, 'issue': '26017', 'calendar_review_id': self.cal_id,
                     'expected_cutoff_issue': '26016', 'start_at': (self.now-timedelta(minutes=1)).isoformat(),
                     'timeout': 15, 'confirmed': True}

    def drive(self, ident, limit=20):
        deadline = time.monotonic()+limit
        while time.monotonic() < deadline:
            self.scheduler.tick()
            plan = next(x for x in self.scheduler.list() if x['id'] == ident)
            if plan['status'] not in ('waiting', 'dispatching', 'running'):
                return plan
            time.sleep(.05)
        self.fail('plan did not reach a terminal state')

    def test_plan_trains_exact_fixed_window_without_browser(self):
        plan = self.scheduler.create(**self.args)
        self.assertEqual(plan['status'], 'waiting')
        self.assertEqual(self.scheduler.create(**self.args)['id'], plan['id'])
        final = self.drive(plan['id'])
        self.assertEqual(final['status'], 'complete', final['message'])
        job = self.jobs.get(final['job_key'])
        self.assertEqual(job['request']['config']['rolling_issue'], '26017')
        self.assertEqual(job['request']['first_issue'], '26001')
        self.assertEqual(job['request']['config']['train_end'], '26012')
        self.assertEqual(job['request']['config']['cutoff'], '26016')
        self.assertEqual([x['status'] for x in final['events']], ['waiting', 'dispatching', 'running', 'complete'])
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_waiting_missing_expected_data_and_stop_at_deadline(self):
        plan = self.scheduler.create(**{**self.args, 'expected_cutoff_issue': '26099'})
        self.scheduler.tick()
        self.assertEqual(self.scheduler.list()[0]['status'], 'waiting')
        self.now = self.deadline
        self.scheduler.tick()
        result = self.scheduler.list()[0]
        self.assertEqual(result['status'], 'blocked')
        self.assertIsNone(result['job_key'])
        with self.assertRaises(RollingPlanError):
            self.scheduler.retry(plan['id'])

    def test_target_result_appearing_before_start_blocks_without_job(self):
        plan = self.scheduler.create(**self.args)
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '26017',
            'lotteryDrawTime': self.calendar['draw_date'], 'lotteryDrawResult': '01 02 03 04 15 01 02',
            'prizeLevelList': []})
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'draw appeared', self.now.isoformat())
        self.scheduler.tick()
        current = self.scheduler.list()[0]
        self.assertEqual(current['status'], 'blocked')
        self.assertIsNone(current['job_key'])
        self.assertEqual(current['id'], plan['id'])

    def test_cancel_and_plan_conflict_never_launch_new_job(self):
        plan = self.scheduler.create(**self.args)
        with self.assertRaises(RollingPlanError):
            self.scheduler.create(**{**self.args, 'timeout': 10})
        self.assertEqual(self.scheduler.cancel(plan['id'], request_key='77777777-7777-4777-8777-777777777777')['status'], 'cancelled')
        self.scheduler.tick()
        self.assertIsNone(self.scheduler.list()[0]['job_key'])

    def test_outdated_expected_cutoff_is_blocked_not_silently_changed(self):
        plan = self.scheduler.create(**{**self.args, 'expected_cutoff_issue': '26015'})
        self.scheduler.tick()
        result = self.scheduler.list()[0]
        self.assertEqual(result['status'], 'blocked')
        self.assertIn('更新历史期', result['message'])
        self.assertIsNone(result['job_key'])
        self.assertEqual(result['id'], plan['id'])

    def test_input_change_before_job_insert_is_rechecked_in_transaction(self):
        plan = self.scheduler.create(**self.args)
        original = self.scheduler._resolve
        calls = 0
        def changed(db, current, now):
            nonlocal calls
            calls += 1
            if calls == 3:
                return 'blocked', 'fixture change', None
            return original(db, current, now)
        with patch.object(self.scheduler, '_resolve', side_effect=changed):
            self.scheduler.tick()
        result = self.scheduler.list()[0]
        self.assertEqual(result['status'], 'failed')
        self.assertIsNotNone(result['job_key'])
        with self.assertRaises(KeyError):
            self.jobs.get(result['job_key'])
        self.assertEqual(result['id'], plan['id'])

    def test_interruption_requires_explicit_retry_and_preserves_attempt(self):
        self.jobs.worker_command = [sys.executable, '-c', 'import time; time.sleep(10)']
        plan = self.scheduler.create(**self.args)
        self.scheduler.tick()
        running = self.scheduler.list()[0]
        self.assertEqual(running['status'], 'running')
        old_job = running['job_key']
        self.jobs.stop()
        self.scheduler.tick()
        self.assertEqual(self.scheduler.list()[0]['status'], 'failed')
        self.jobs = TrainingJobs(self.path)
        self.jobs.recover_interrupted()
        self.addCleanup(self.jobs.stop)
        self.scheduler = RollingTraining(self.path, self.jobs, clock=lambda: self.now)
        retry = self.scheduler.retry(plan['id'])
        self.assertEqual(retry['attempt'], 1)
        result = self.drive(plan['id'])
        self.assertEqual(result['status'], 'complete', result['message'])
        self.assertNotEqual(result['job_key'], old_job)
        self.assertEqual(self.jobs.get(old_job)['status'], 'failed')
        self.assertTrue(any(e['job_key'] == old_job for e in result['events']))
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def _observe_newer_issue(self, observed_at):
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '25999',
            'lotteryDrawTime': (self.deadline-timedelta(days=1)).date().isoformat(),
            'lotteryDrawResult': '01 02 03 04 35 01 02', 'prizeLevelList': []})
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'new history fixture', observed_at.isoformat())

    def test_new_history_during_training_blocks_completed_job(self):
        self.jobs.worker_command = [sys.executable, '-c',
            "import time,runpy; time.sleep(1); runpy.run_module('dlt.training_worker',run_name='__main__')"]
        plan = self.scheduler.create(**self.args)
        self.scheduler.tick()
        self.assertEqual(self.scheduler.list()[0]['status'], 'running')
        self._observe_newer_issue(datetime.now(timezone.utc))
        result = self.drive(plan['id'])
        self.assertEqual(self.jobs.get(result['job_key'])['status'], 'complete')
        self.assertEqual(result['status'], 'blocked')
        self.assertIn('历史输入发生变化', result['message'])

    def test_history_after_completion_does_not_rewrite_training_time(self):
        plan = self.scheduler.create(**self.args)
        self.scheduler.tick()
        job_key = self.scheduler.list()[0]['job_key']
        until = time.monotonic()+20
        while time.monotonic() < until:
            job = self.jobs.get(job_key)
            if job['status'] != 'running':
                break
            time.sleep(.05)
        self.assertEqual(job['status'], 'complete', job.get('error'))
        self._observe_newer_issue(datetime.fromisoformat(job['finished_at'])+timedelta(seconds=1))
        self.scheduler.tick()
        self.assertEqual(self.scheduler.list()[0]['status'], 'complete')


if __name__ == '__main__':
    unittest.main()
