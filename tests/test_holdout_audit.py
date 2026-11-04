from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from dlt.backtest_repository import BacktestRepository
from dlt.holdout_audit import HoldoutAuditError, audit_holdout
from dlt.model_repository import ModelRepository
from dlt.pair_repository import PairRepository
from dlt.snapshots import SnapshotStore
from dlt.training_jobs import TrainingJobs
import test_backtest as backtest_fixtures
import test_candidates as candidate_fixtures
import test_model_repository as model_fixtures


class HoldoutAuditTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.path = Path(temp.name)/'research.sqlite3'
        raw = json.dumps(backtest_fixtures.source_data()).encode()
        self.snapshot = SnapshotStore(self.path).import_official(raw, 'synthetic', '2026-02-01T12:00:00+08:00')['snapshot_id']

    def test_no_local_records_does_not_prove_independence(self):
        audit = audit_holdout(self.path, self.snapshot, '26018', '26020')
        self.assertEqual(audit['issues'], ['26018', '26019', '26020'])
        self.assertEqual(audit['status'], 'no_recorded_use')
        self.assertEqual(audit['independence'], 'not_verified')
        self.assertTrue(audit['limitations'])
        single = audit_holdout(self.path, self.snapshot, '26018', '26018')
        self.assertEqual(single['issues'], ['26018'])
        with self.assertRaises(HoldoutAuditError):
            audit_holdout(self.path, self.snapshot, '26020', '26018')

    def test_detects_training_generation_and_historical_backtest_without_conflating_ranges(self):
        model = ModelRepository(self.path)
        model.train_from_snapshot(self.snapshot, model_fixtures.config(self.snapshot), first_issue='26001')
        pair = PairRepository(self.path)
        pair.save_draft('candidate', {**candidate_fixtures.config(), 'snapshot_id': self.snapshot, 'budget_fen': 400},
                        history_first_issue='26001', history_cutoff_issue='26016')
        proto = backtest_fixtures.protocol(self.snapshot)
        BacktestRepository(self.path).run('backtest', proto, backtest_fixtures.scenarios(proto['targets']))
        audit = audit_holdout(self.path, self.snapshot, '26015', '26019')
        self.assertEqual(audit['status'], 'known_use')
        items = {x['kind']: x for x in audit['findings']}
        self.assertEqual(items['model_artifact']['input_issues'], ['26015', '26016'])
        self.assertEqual(items['candidate_generation']['input_issues'], ['26015', '26016'])
        self.assertEqual(items['backtest_report']['target_issues'], ['26018', '26019'])
        self.assertEqual(items['backtest_report']['input_issues'], ['26015', '26016', '26017', '26018', '26019'])
        self.assertEqual(audit['independence'], 'not_verified')

    def test_failed_attempt_and_corrupt_record_are_conservatively_reported(self):
        TrainingJobs(self.path).initialize()
        request = {'config': model_fixtures.config(self.snapshot), 'first_issue': '26001'}
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO training_jobs VALUES (?,?,?,'failed',?,NULL,NULL,?)",
                       ('failed', json.dumps(request).encode(), 'fixture', '2026-01-01', 'failed'))
            db.execute("INSERT INTO training_jobs VALUES (?,?,?,'failed',?,NULL,NULL,?)",
                       ('unreadable', b'{', 'fixture', '2026-01-01', 'failed'))
        audit = audit_holdout(self.path, self.snapshot, '26015', '26016')
        self.assertEqual(audit['status'], 'unresolved')
        self.assertEqual(audit['findings'][0]['record_key'], 'failed')
        self.assertEqual(audit['findings'][0]['status'], 'failed')
        self.assertEqual(audit['unresolved'][0]['record_key'], 'unreadable')

    def test_snapshot_corruption_blocks_audit(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draws_no_update')
            db.execute("UPDATE draw_versions SET draw_date='2099-01-01' WHERE issue='26018'")
        with self.assertRaises(HoldoutAuditError):
            audit_holdout(self.path, self.snapshot, '26018', '26020')


if __name__ == '__main__':
    unittest.main()
