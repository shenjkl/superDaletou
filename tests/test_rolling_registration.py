from contextlib import closing
from datetime import timedelta
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch

from dlt.rolling_registration import RollingRegistration, RollingRegistrationError
from dlt.integrity import check_database
from dlt.snapshots import SnapshotStore
import test_rolling_training as fixtures


class RollingRegistrationTests(unittest.TestCase):
    setUpBase = fixtures.RollingTrainingTests.setUpBase
    setUp = fixtures.RollingTrainingTests.setUp
    review = fixtures.RollingTrainingTests.review
    import_draw = fixtures.RollingTrainingTests.import_draw
    _db = fixtures.RollingTrainingTests._db
    prepare_second = fixtures.RollingTrainingTests.prepare_second
    train = fixtures.RollingTrainingTests.train
    drive = fixtures.RollingTrainingTests.drive

    def make_plans(self):
        train = self.scheduler.create(**self.args)
        service = RollingRegistration(self.path, self.scheduler, clock=lambda: self.now)
        plan = service.create(training_plan_id=train['id'], rule_review_id=self.rule_id, tax_review_id=self.tax_id,
                              internal_deadline=(self.deadline-timedelta(hours=1)).isoformat(), confirmed=True)
        return train, service, plan

    def test_completed_training_generates_all_fixed_replicas_and_registers_once(self):
        train, service, plan = self.make_plans()
        service.tick()
        self.assertEqual(service.list()[0]['status'], 'waiting')
        self.assertEqual(self.drive(train['id'])['status'], 'complete')
        service.tick()
        self.assertEqual(service.list()[0]['status'], 'complete')
        result = self.repo.get('auto-exp-'+plan['id'])
        self.assertEqual([p['replica'] for p in result['pairs']], ['a', 'b'])
        self.assertEqual([p['pair']['config']['seed'] for p in result['pairs']], [1, 2])
        self.assertEqual(len({p['pair']['model_artifact_id'] for p in result['pairs']}), 1)
        self.assertEqual(self.series.get(fixtures.ROLLING_TRAINING_SERIES_KEY)['periods'][0]['experiment_key'], 'auto-exp-'+plan['id'])
        service.tick()
        self.assertEqual(len(service.list()[0]['events']), 2)
        check = check_database(self.path)
        self.assertEqual(check['status'], 'passed', check)

    def test_missing_confirmation_and_late_deadline_do_not_create_plan(self):
        train = self.scheduler.create(**self.args)
        service = RollingRegistration(self.path, self.scheduler, clock=lambda: self.now)
        args = dict(training_plan_id=train['id'], rule_review_id=self.rule_id, tax_review_id=self.tax_id,
                    internal_deadline=(self.deadline-timedelta(hours=1)).isoformat())
        with self.assertRaises(RollingRegistrationError):
            service.create(**args, confirmed=False)
        self.now = self.deadline
        with self.assertRaises(RollingRegistrationError):
            service.create(**args, confirmed=True)
        self.assertEqual(service.list(), [])

    def test_target_observed_before_registration_blocks_without_fixed_result(self):
        train, service, plan = self.make_plans()
        self.assertEqual(self.drive(train['id'])['status'], 'complete')
        source = self.source.copy(); source['value'] = dict(source['value'])
        source['value']['list'] = list(source['value']['list'])+[{'lotteryGameNum': '85', 'lotteryDrawNum': '26017',
            'lotteryDrawTime': self.calendar['draw_date'], 'lotteryDrawResult': '01 02 03 04 15 01 02', 'prizeLevelList': []}]
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'observed target fixture', self.now.isoformat())
        service.tick()
        self.assertEqual(service.list()[0]['status'], 'blocked')
        with self.assertRaises(KeyError):
            self.repo.get('auto-exp-'+plan['id'])

    def test_cancel_during_generation_prevents_publication(self):
        train, service, plan = self.make_plans()
        self.assertEqual(self.drive(train['id'])['status'], 'complete')
        started, release = threading.Event(), threading.Event()
        original = service.pairs.save_draft
        def pause(*args, **kwargs):
            started.set(); self.assertTrue(release.wait(10))
            return original(*args, **kwargs)
        with patch.object(service.pairs, 'save_draft', side_effect=pause), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(service.tick)
            self.assertTrue(started.wait(10))
            self.assertEqual(service.cancel(plan['id'], request_key='55555555-5555-4555-8555-555555555555')['status'], 'cancelled')
            release.set(); future.result(timeout=20)
        self.assertEqual(service.list()[0]['status'], 'cancelled')
        with self.assertRaises(KeyError):
            self.repo.get('auto-exp-'+plan['id'])

    def test_completion_write_failure_rolls_back_registration(self):
        train, service, plan = self.make_plans()
        self.assertEqual(self.drive(train['id'])['status'], 'complete')
        with patch.object(service, '_transition', side_effect=RuntimeError('injected')):
            with self.assertRaises(RuntimeError):
                service._advance(plan['id'])
        with self.assertRaises(KeyError):
            self.repo.get('auto-exp-'+plan['id'])
        self.assertEqual(service.list()[0]['status'], 'waiting')
        service.tick()
        self.assertEqual(service.list()[0]['status'], 'complete')

    def test_cancel_after_last_pair_is_checked_inside_publication_transaction(self):
        train, service, plan = self.make_plans()
        self.assertEqual(self.drive(train['id'])['status'], 'complete')
        original = service.pairs.save_draft
        count = 0
        def cancel_after_second(*args, **kwargs):
            nonlocal count
            result = original(*args, **kwargs)
            count += 1
            if count == 2:
                service.cancel(plan['id'], request_key='66666666-6666-4666-8666-666666666666')
            return result
        with patch.object(service.pairs, 'save_draft', side_effect=cancel_after_second):
            service.tick()
        self.assertEqual(service.list()[0]['status'], 'cancelled')
        with self.assertRaises(KeyError):
            self.repo.get('auto-exp-'+plan['id'])

    def test_new_rule_invalidates_plan_until_user_chooses_new_plan(self):
        train, service, plan = self.make_plans()
        self.assertEqual(self.drive(train['id'])['status'], 'complete')
        rule = dict(self.repo._get_review(self._db(), self.rule_id, 'rule')['extracted'])
        rule['version'] = 'later-correction'
        replacement = self.review('rule', rule)
        self.repo.declare_scoped_source_correction(self.rule_id, replacement, issue='26017', note='fixture', confirmed=True)
        service.tick()
        self.assertEqual(service.list()[0]['status'], 'blocked')
        with self.assertRaises(RollingRegistrationError):
            service.retry(plan['id'])
        with self.assertRaises(KeyError):
            self.repo.get('auto-exp-'+plan['id'])
        new_plan = service.create(training_plan_id=train['id'], rule_review_id=replacement, tax_review_id=self.tax_id,
                                  internal_deadline=(self.deadline-timedelta(hours=1)).isoformat(), confirmed=True)
        self.assertNotEqual(plan['id'], new_plan['id'])
        service.tick()
        self.assertEqual(next(p for p in service.list() if p['id'] == new_plan['id'])['status'], 'complete')
        self.assertEqual(self.repo.get('auto-exp-'+new_plan['id'])['protocol']['rule_review_id'], replacement)

    def test_restart_reuses_drafts_after_interruption(self):
        train, service, plan = self.make_plans()
        self.assertEqual(self.drive(train['id'])['status'], 'complete')
        original = service.pairs.save_draft
        calls = 0
        def interrupt(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError('simulated process interruption')
            return original(*args, **kwargs)
        with patch.object(service.pairs, 'save_draft', side_effect=interrupt):
            with self.assertRaises(RuntimeError):
                service._advance(plan['id'])
        restarted = RollingRegistration(self.path, self.scheduler, clock=lambda: self.now)
        restarted.initialize(); restarted.tick()
        self.assertEqual(restarted.list()[0]['status'], 'complete')
        self.assertEqual(len(self.repo.get('auto-exp-'+plan['id'])['pairs']), 2)


if __name__ == '__main__':
    unittest.main()
