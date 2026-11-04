from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from dlt.evaluation_repository import EvaluationRepository
from dlt.holdout_audit import audit_holdout
from dlt.holdout_reservation import ReservationError, reserve_range
from dlt.pair_repository import PairRepository
from dlt.scenarios import _historical_scenario, historical_scenario
from dlt.snapshots import SnapshotStore
from dlt.custom_scenarios import custom_scenario, PAYOUT_KEYS
from dlt.evaluation import EvaluationError
from test_candidates import config


class HistoricalScenarioExclusionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'research.sqlite3'
        raw = (Path(__file__).resolve().parents[1] / 'research/data/official_page1.json').read_bytes()
        self.snapshot = SnapshotStore(self.path).import_official(raw, 'local fixture',
                                '2026-10-29T12:00:00+08:00')['snapshot_id']
        rows = SnapshotStore(self.path).get_draws(self.snapshot)
        cfg = {**config(), 'snapshot_id': self.snapshot, 'target_issue': '26124',
               'target_date': '2026-10-31', 'budget_fen': 400}
        self.pair = PairRepository(self.path).save_draft('pair', cfg, history_first_issue=rows[0]['issue'],
                                                       history_cutoff_issue='26119')
        self.evaluation = EvaluationRepository(self.path)

    def reserve(self):
        return reserve_range(self.path, self.snapshot, '26122', '26122', confirmed=True)

    def scenario(self):
        # Emulate a scenario constructed by a previous version without activity records.
        return _historical_scenario(self.path, self.snapshot, self.pair, '26122')

    def args(self):
        return {'scenario': self.scenario(), 'samples': 100, 'seed': 7,
                'max_samples': 1000, 'max_ticket_draws': 10000}

    def test_existing_saved_scenario_counts_as_prior_use(self):
        # Emulate a historical record from before activity recording existed.
        self.evaluation._run_new('old', 'pair', **self.args())
        audit = audit_holdout(self.path, self.snapshot, '26122', '26122')
        self.assertEqual(audit['status'], 'known_use')
        matches = [x for x in audit['findings'] if x['kind'] == 'portfolio_scenario_evaluation']
        self.assertEqual([(x['record_key'], x['target_issues']) for x in matches], [('old', ['26122'])])
        with self.assertRaisesRegex(ReservationError, '已使用'):
            self.reserve()

    def test_reserved_issue_cannot_be_used_as_scenario_or_for_evaluation(self):
        args = self.args()
        self.reserve()
        with self.assertRaisesRegex(ReservationError, '排除范围'):
            historical_scenario(self.path, self.snapshot, self.pair, '26122')
        with self.assertRaisesRegex(ReservationError, '排除范围'):
            self.evaluation.run('blocked', 'pair', **args)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM draft_evaluations WHERE request_key='blocked'").fetchone())

    def test_unverified_issue_label_is_conservatively_counted_and_blocked(self):
        from test_evaluation import scenario
        synthetic = scenario()
        synthetic['source_issue'] = '26122'
        synthetic['rule']['applicable_issues'] = ['26124']
        args = {'scenario': synthetic, 'samples': 100, 'seed': 7, 'max_samples': 1000, 'max_ticket_draws': 10000}
        self.evaluation._run_new('legacy-unverified', 'pair', **args)
        audit = audit_holdout(self.path, self.snapshot, '26122', '26122')
        self.assertEqual(audit['status'], 'known_use')
        self.assertTrue(any(x['kind'] == 'portfolio_scenario_unverified' for x in audit['findings']))
        with self.assertRaisesRegex(ReservationError, '已使用'):
            self.reserve()

    def test_reserved_issue_blocks_unverified_scenario_label(self):
        from test_evaluation import scenario
        synthetic = scenario()
        synthetic['source_issue'] = '26122'
        synthetic['rule']['applicable_issues'] = ['26124']
        self.reserve()
        with self.assertRaisesRegex(ReservationError, '排除范围'):
            self.evaluation.run('blocked', 'pair', scenario=synthetic, samples=100, seed=7,
                                max_samples=1000, max_ticket_draws=10000)

    def test_inflight_scenario_read_blocks_later_reservation(self):
        entered, release = threading.Event(), threading.Event()
        def delayed(*args):
            entered.set()
            self.assertTrue(release.wait(10))
            return _historical_scenario(*args)
        with patch('dlt.scenarios._historical_scenario', side_effect=delayed), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(historical_scenario, self.path, self.snapshot, self.pair, '26122')
            self.assertTrue(entered.wait(10))
            try:
                with self.assertRaisesRegex(ReservationError, '已使用'):
                    self.reserve()
            finally:
                release.set()
            self.assertEqual(future.result(timeout=10)['source_issue'], '26122')

    def test_failed_scenario_attempt_is_retained_as_possible_use(self):
        with patch('dlt.scenarios._historical_scenario', side_effect=ValueError('fixture failure')):
            with self.assertRaisesRegex(ValueError, 'fixture failure'):
                historical_scenario(self.path, self.snapshot, self.pair, '26122')
        audit = audit_holdout(self.path, self.snapshot, '26122', '26122')
        self.assertEqual(audit['status'], 'known_use')
        self.assertTrue(any(x['kind'] == 'exploration_activity' and x['status'] == 'finished'
                            and x['target_issues'] == ['26122'] for x in audit['findings']))
        with self.assertRaisesRegex(ReservationError, '已使用'):
            self.reserve()

    def test_inflight_evaluation_blocks_later_reservation(self):
        from dlt.evaluation import evaluate_pair
        entered, release = threading.Event(), threading.Event()
        def delayed(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(10))
            return evaluate_pair(*args, **kwargs)
        with patch('dlt.evaluation_repository.evaluate_pair', side_effect=delayed), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.evaluation.run, 'running', 'pair', **self.args())
            self.assertTrue(entered.wait(10))
            try:
                with self.assertRaisesRegex(ReservationError, '已使用'):
                    self.reserve()
            finally:
                release.set()
            self.assertTrue(future.result(timeout=10)['portfolios'])

    def test_damaged_saved_evaluation_prevents_claiming_unused(self):
        self.evaluation._run_new('old', 'pair', **self.args())
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draft_evaluations_no_update')
            db.execute("UPDATE draft_evaluations SET request_json=? WHERE request_key='old'", (b'{',))
        audit = audit_holdout(self.path, self.snapshot, '26122', '26122')
        self.assertEqual(audit['status'], 'unresolved')
        self.assertTrue(any(x['kind'] == 'portfolio_scenario_evaluation' for x in audit['unresolved']))
        with self.assertRaisesRegex(ReservationError, '无法核对'):
            self.reserve()

    def custom(self, provenance=None):
        data = {'label': '研究假设', 'payouts': {key: '100' for key in PAYOUT_KEYS}}
        if provenance is not None:
            data['provenance'] = provenance
        return custom_scenario(self.pair, data)

    def test_user_declared_historical_custom_amount_counts_as_possible_use(self):
        scenario = self.custom({'kind': 'historical', 'issue': '26122'})
        self.evaluation.run('custom-historical', 'pair', **{**self.args(), 'scenario': scenario})
        audit = audit_holdout(self.path, self.snapshot, '26122', '26122')
        self.assertEqual(audit['status'], 'known_use')
        self.assertTrue(any(x['record_key'] == 'custom-historical' and x['status'] == 'user_declared'
                            for x in audit['findings']))
        with self.assertRaises(ReservationError):
            self.reserve()

    def test_reserved_issue_blocks_custom_historical_and_keeps_attempt_unpublished(self):
        scenario = self.custom({'kind': 'historical', 'issue': '26122'})
        self.reserve()
        with self.assertRaisesRegex(ReservationError, '排除范围'):
            self.evaluation.run('custom-blocked', 'pair', **{**self.args(), 'scenario': scenario})
        with self.assertRaises(KeyError):
            self.evaluation.get('custom-blocked')

    def test_declared_synthetic_does_not_count_as_specific_historical_use(self):
        scenario = self.custom({'kind': 'synthetic'})
        self.evaluation.run('custom-synthetic', 'pair', **{**self.args(), 'scenario': scenario})
        audit = audit_holdout(self.path, self.snapshot, '26122', '26122')
        self.assertEqual(audit['status'], 'no_recorded_use', audit)
        self.assertEqual(audit['independence'], 'not_verified')

    def test_legacy_unknown_origin_is_unresolved_and_new_unknown_is_blocked(self):
        scenario = self.custom()
        with self.assertRaisesRegex(EvaluationError, '需声明来源'):
            self.evaluation.run('new-unknown', 'pair', **{**self.args(), 'scenario': scenario})
        self.evaluation._run_new('legacy-unknown', 'pair', **{**self.args(), 'scenario': scenario})
        audit = audit_holdout(self.path, self.snapshot, '26122', '26122')
        self.assertEqual(audit['status'], 'unresolved')
        self.assertTrue(any(x['kind'] == 'custom_scenario_origin' for x in audit['unresolved']))
        with self.assertRaisesRegex(ReservationError, '无法核对'):
            self.reserve()

    def test_custom_historical_inflight_prevents_reservation(self):
        from dlt.evaluation import evaluate_pair
        entered, release = threading.Event(), threading.Event()
        scenario = self.custom({'kind': 'historical', 'issue': '26122'})
        args = {**self.args(), 'scenario': scenario}
        def delayed(*values, **options):
            entered.set()
            self.assertTrue(release.wait(10))
            return evaluate_pair(*values, **options)
        with patch('dlt.evaluation_repository.evaluate_pair', side_effect=delayed), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.evaluation.run, 'custom-running', 'pair', **args)
            self.assertTrue(entered.wait(10))
            try:
                with self.assertRaisesRegex(ReservationError, '已使用'):
                    self.reserve()
            finally:
                release.set()
            self.assertTrue(future.result(timeout=10)['portfolios'])


if __name__ == '__main__':
    unittest.main()
