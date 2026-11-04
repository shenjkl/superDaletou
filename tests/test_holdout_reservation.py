from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
import json
import sqlite3
import sys
import threading
import unittest
from unittest.mock import patch

from dlt.backtest import run_from_snapshot
from dlt.backtest_jobs import BacktestJobs
from dlt.backtest_repository import BacktestRepository
from dlt.candidate_service import generate_from_snapshot
from dlt.holdout_reservation import ReservationError, list_reservations, reserve_range
from dlt.integrity import check_database
from dlt.model_repository import ModelRepository
from dlt.pair_repository import PairRepository
from dlt.recovery import RecoveryError, prepare_isolated_copy
from dlt.snapshots import SnapshotStore
from dlt.training_jobs import TrainingJobs
import test_backtest as backtest_fixtures
import test_candidates as candidate_fixtures
import test_holdout_audit as audit_fixtures
import test_model_repository as model_fixtures


class ReservationTests(unittest.TestCase):
    setUp = audit_fixtures.HoldoutAuditTests.setUp

    def reserve(self, first='26018', last='26019'):
        return reserve_range(self.path, self.snapshot, first, last, confirmed=True)

    def test_reservation_is_persistent_checked_and_immutable(self):
        with self.assertRaises(ReservationError):
            reserve_range(self.path, self.snapshot, '26018', '26019', confirmed=False)
        created = self.reserve()
        self.assertEqual(created['content']['issues'], ['26018', '26019'])
        self.assertEqual(created['content']['independence'], 'not_verified')
        self.assertEqual(list_reservations(self.path)[0]['id'], created['id'])
        self.assertEqual(self.reserve(), created)
        self.assertEqual(check_database(self.path)['status'], 'passed')
        with self.assertRaises(ReservationError):
            self.reserve('26019', '26020')
        with closing(sqlite3.connect(self.path)) as db, self.assertRaises(sqlite3.IntegrityError):
            db.execute('DELETE FROM holdout_reservations')

    def test_training_and_generation_reject_reserved_input_and_target(self):
        self.reserve()
        config = model_fixtures.config(self.snapshot)
        overlapping = {**config, 'train_end': '26017', 'cutoff': '26020'}
        jobs = TrainingJobs(self.path); self.addCleanup(jobs.stop)
        with self.assertRaisesRegex(ValueError, '排除范围'):
            jobs.submit('blocked', name='fixture', config=overlapping, first_issue='26001', timeout=10)
        with self.assertRaisesRegex(ValueError, '排除范围'):
            ModelRepository(self.path).train_from_snapshot(self.snapshot, overlapping, first_issue='26001')
        candidate = {**candidate_fixtures.config(), 'snapshot_id': self.snapshot, 'budget_fen': 400}
        with self.assertRaisesRegex(ValueError, '排除范围'):
            generate_from_snapshot(self.path, candidate, history_first_issue='26001', history_cutoff_issue='26020')
        with self.assertRaisesRegex(ValueError, '排除范围'):
            PairRepository(self.path).save_draft('blocked-pair', {**candidate, 'target_issue': '26018'},
                                                history_first_issue='26001', history_cutoff_issue='26016')
        pair = PairRepository(self.path).save_draft('allowed-pair', candidate,
                                                   history_first_issue='26001', history_cutoff_issue='26016')
        self.assertTrue(pair['candidate']['tickets'])

    def test_exploratory_backtest_rejects_reserved_target_or_history(self):
        self.reserve()
        protocol = backtest_fixtures.protocol(self.snapshot)
        protocol['candidate']['max_tickets'] = 100
        jobs = BacktestJobs(self.path); jobs.initialize(); self.addCleanup(jobs.stop)
        with self.assertRaisesRegex(ValueError, '排除范围'):
            jobs.submit('blocked-target', protocol, 10)
        with self.assertRaisesRegex(ValueError, '排除范围'):
            run_from_snapshot(self.path, protocol, backtest_fixtures.scenarios(protocol['targets']))
        protocol['targets'] = ['26020']
        with self.assertRaisesRegex(ValueError, '排除范围'):
            BacktestRepository(self.path).run('blocked-history', protocol, backtest_fixtures.scenarios(protocol['targets']))

    def test_other_snapshot_does_not_bypass_same_issue_exclusion(self):
        self.reserve()
        source = deepcopy(backtest_fixtures.source_data())
        source['note'] = 'a different snapshot with the same draws'
        other = SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'second',
                                                        '2026-02-02T12:00:00+08:00')['snapshot_id']
        candidate = {**candidate_fixtures.config(), 'snapshot_id': other, 'budget_fen': 400}
        with self.assertRaisesRegex(ValueError, '排除范围'):
            generate_from_snapshot(self.path, candidate, history_first_issue='26001', history_cutoff_issue='26020')

    def test_existing_or_unresolved_use_cannot_be_reserved(self):
        ModelRepository(self.path).train_from_snapshot(self.snapshot, model_fixtures.config(self.snapshot), first_issue='26001')
        with self.assertRaisesRegex(ReservationError, '已使用'):
            self.reserve('26015', '26016')
        jobs = TrainingJobs(self.path); jobs.initialize(); self.addCleanup(jobs.stop)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO training_jobs VALUES ('broken',x'7b','fixture','failed','2026',NULL,NULL,'x')")
        with self.assertRaisesRegex(ReservationError, '无法核对'):
            self.reserve()

    def test_running_jobs_block_reservation(self):
        jobs = TrainingJobs(self.path, worker_command=[sys.executable, '-c', 'import time; time.sleep(5)'])
        self.addCleanup(jobs.stop)
        jobs.submit('running', name='fixture', config=model_fixtures.config(self.snapshot), first_issue='26001', timeout=10)
        with self.assertRaisesRegex(ReservationError, '仍在运行'):
            self.reserve()

    def test_reservation_and_job_submit_are_serialized(self):
        from dlt.holdout_reservation import audit_holdout
        entered, release = threading.Event(), threading.Event()
        def delayed(*args):
            entered.set()
            self.assertTrue(release.wait(5))
            return audit_holdout(*args)
        config = {**model_fixtures.config(self.snapshot), 'train_end': '26017', 'cutoff': '26020'}
        jobs = TrainingJobs(self.path); jobs.initialize(); self.addCleanup(jobs.stop)
        with patch('dlt.holdout_reservation.audit_holdout', side_effect=delayed), ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.reserve)
            self.assertTrue(entered.wait(5))
            second = pool.submit(jobs.submit, 'race', name='fixture', config=config, first_issue='26001', timeout=10)
            release.set()
            self.assertEqual(first.result(timeout=10)['content']['start_issue'], '26018')
            with self.assertRaisesRegex(ValueError, '排除范围'):
                second.result(timeout=10)

    def test_inflight_direct_generation_is_recorded_before_range_is_fixed(self):
        from dlt.candidate_service import generate_pair
        entered, release = threading.Event(), threading.Event()
        def delayed(**kwargs):
            entered.set()
            self.assertTrue(release.wait(5))
            return generate_pair(**kwargs)
        candidate = {**candidate_fixtures.config(), 'snapshot_id': self.snapshot, 'budget_fen': 400}
        with patch('dlt.candidate_service.generate_pair', side_effect=delayed), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(generate_from_snapshot, self.path, candidate,
                                 history_first_issue='26001', history_cutoff_issue='26016')
            self.assertTrue(entered.wait(5))
            try:
                with self.assertRaisesRegex(ReservationError, '已使用'):
                    self.reserve('26015', '26016')
            finally:
                release.set()
            self.assertTrue(future.result(timeout=10)['candidate']['tickets'])
        with self.assertRaisesRegex(ReservationError, '已使用'):
            self.reserve('26015', '26016')

    def test_corrupt_reservation_blocks_new_tasks_and_isolated_copy(self):
        self.reserve()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER holdout_reservations_no_update')
            db.execute("UPDATE holdout_reservations SET content_json=x'7b7d'")
            db.execute("CREATE TRIGGER holdout_reservations_no_update BEFORE UPDATE ON holdout_reservations "
                       "BEGIN SELECT RAISE(ABORT, 'immutable holdout reservation'); END")
        self.assertEqual(check_database(self.path)['status'], 'failed')
        with self.assertRaisesRegex(ReservationError, '记录损坏'):
            ModelRepository(self.path).train_from_snapshot(self.snapshot, model_fixtures.config(self.snapshot), first_issue='26001')
        with self.assertRaises(RecoveryError):
            prepare_isolated_copy(self.path, self.path.with_name('bad-copy.sqlite3'))


if __name__ == '__main__':
    unittest.main()
