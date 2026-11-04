from contextlib import closing
from copy import deepcopy
import json
import sqlite3
import sys
import unittest

from dlt.integrity import check_database
from dlt.recovery import RecoveryError, prepare_isolated_copy
from dlt.rolling_training import RollingTraining
from dlt.snapshots import SnapshotStore
from dlt.training_jobs import TrainingJobs
import test_rolling_training as fixtures


class RollingIntegrityTests(unittest.TestCase):
    setUpBase = fixtures.RollingTrainingTests.setUpBase
    review = fixtures.RollingTrainingTests.review
    import_draw = fixtures.RollingTrainingTests.import_draw
    _db = fixtures.RollingTrainingTests._db
    prepare_second = fixtures.RollingTrainingTests.prepare_second
    train = fixtures.RollingTrainingTests.train
    drive = fixtures.RollingTrainingTests.drive

    def setUp(self):
        fixtures.RollingTrainingTests.setUp(self)

    def completed(self):
        plan = self.scheduler.create(**self.args)
        result = self.drive(plan['id'])
        self.assertEqual(result['status'], 'complete', result['message'])
        return result

    def assert_rejected(self):
        result = check_database(self.path)
        self.assertEqual(result['status'], 'failed', result)
        self.assertTrue(any(item['area'] == 'rolling_training_plans' for item in result['errors']))
        target = self.path.with_name('recovery-rolling.sqlite3')
        with self.assertRaises(RecoveryError):
            prepare_isolated_copy(self.path, target)
        self.assertFalse(target.exists())

    def test_waiting_running_and_completed_are_valid(self):
        plan = self.scheduler.create(**self.args)
        self.assertEqual(check_database(self.path)['status'], 'passed')
        self.scheduler.tick()
        self.assertEqual(check_database(self.path)['status'], 'passed')
        self.assertEqual(self.drive(plan['id'])['status'], 'complete')
        result = check_database(self.path)
        self.assertEqual(result['status'], 'passed', result)

    def test_status_changed_without_event_is_rejected(self):
        self.scheduler.create(**self.args)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE rolling_training_plans SET status='complete'")
        self.assert_rejected()

    def test_completed_job_reference_missing_is_rejected(self):
        plan = self.completed()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DELETE FROM training_jobs WHERE request_key=?', (plan['job_key'],))
        self.assert_rejected()

    def test_history_hash_or_training_configuration_changed_is_rejected(self):
        plan = self.completed()
        resolved = plan['resolved']
        resolved['history_sha256'] = '0'*64
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE rolling_training_plans SET resolved_json=? WHERE id=?',
                       (json.dumps(resolved).encode(), plan['id']))
        self.assert_rejected()

    def test_job_request_must_match_fixed_plan(self):
        plan = self.completed()
        with closing(sqlite3.connect(self.path)) as db, db:
            raw = db.execute('SELECT request_json FROM training_jobs WHERE request_key=?', (plan['job_key'],)).fetchone()[0]
            task = json.loads(raw)
            task['config']['seed'] += 1
            db.execute('UPDATE training_jobs SET request_json=? WHERE request_key=?',
                       (json.dumps(task).encode(), plan['job_key']))
        self.assert_rejected()

    def test_invalid_event_sequence_is_rejected_even_if_guard_is_restored(self):
        plan = self.completed()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER rolling_events_no_update')
            db.execute("UPDATE rolling_training_events SET status='cancelled' WHERE plan_id=? AND status='running'", (plan['id'],))
            db.execute("CREATE TRIGGER rolling_events_no_update BEFORE UPDATE ON rolling_training_events "
                       "BEGIN SELECT RAISE(ABORT, 'immutable rolling event'); END")
        self.assert_rejected()

    def test_missing_historical_retry_job_is_rejected(self):
        self.jobs.worker_command = [sys.executable, '-c', 'import time; time.sleep(10)']
        plan = self.scheduler.create(**self.args)
        self.scheduler.tick()
        old_key = self.scheduler.list()[0]['job_key']
        self.jobs.stop()
        self.scheduler.tick()
        self.jobs = TrainingJobs(self.path)
        self.addCleanup(self.jobs.stop)
        self.scheduler = RollingTraining(self.path, self.jobs, clock=lambda: self.now)
        self.scheduler.retry(plan['id'])
        self.assertEqual(self.drive(plan['id'])['status'], 'complete')
        self.assertEqual(check_database(self.path)['status'], 'passed')
        with closing(sqlite3.connect(self.path)) as db, db:
            original = db.execute('SELECT request_json FROM training_jobs WHERE request_key=?', (old_key,)).fetchone()[0]
            changed = json.loads(original)
            changed['config']['seed'] += 1
            db.execute('UPDATE training_jobs SET request_json=? WHERE request_key=?', (json.dumps(changed).encode(), old_key))
        self.assert_rejected()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE training_jobs SET request_json=? WHERE request_key=?', (original, old_key))
            db.execute('DELETE FROM training_jobs WHERE request_key=?', (old_key,))
        self.assert_rejected()

    def test_later_conflicting_observation_does_not_rewrite_saved_history(self):
        self.completed()
        source = deepcopy(self.source)
        source['value']['list'][-1]['lotteryDrawResult'] = '01 02 03 04 35 01 02'
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'later conflicting observation', self.now.isoformat())
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)


if __name__ == '__main__':
    unittest.main()
