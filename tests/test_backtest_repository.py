from contextlib import closing
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from dlt.backtest import BacktestError, run_from_snapshot
from dlt.backtest_repository import BacktestRepository, _recheck_settlements
from dlt.candidates import _canonical
from dlt.snapshots import SnapshotStore
from test_backtest import protocol, scenarios, source_data


class BacktestRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'db.sqlite3'
        self.source = source_data()
        sid = SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'fixture',
                                                      '2026-02-01T12:00:00+08:00')['snapshot_id']
        self.protocol = protocol(sid)
        self.scenarios = scenarios(self.protocol['targets'])
        self.repo = BacktestRepository(self.path)

    def change_source(self):
        self.source['value']['list'][6]['lotteryDrawResult'] = '10 11 12 13 14 03 04'
        SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'other',
                                                '2026-02-02T12:00:00+08:00')

    def test_partial_report_persists_with_all_failures_and_idempotency(self):
        del self.scenarios['26019']
        first = self.repo.run('run-1', self.protocol, self.scenarios)
        self.assertEqual(first['status'], 'research_only')
        self.assertEqual(first['report']['completeness'], 'partial')
        self.assertEqual(first['paired_description']['included'], first['report']['complete_periods'])
        self.assertEqual(first['paired_description']['missing'], len(first['report']['incomplete_issues']))
        self.assertEqual(first, BacktestRepository(self.path).run('run-1', self.protocol, self.scenarios))
        self.assertEqual(first['report'], self.repo.get('run-1'))
        with self.assertRaisesRegex(BacktestError, 'different input'):
            self.repo.run('run-1', {**self.protocol, 'seed': 1}, self.scenarios)
        with closing(sqlite3.connect(self.path)) as db, db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("DELETE FROM backtest_reports WHERE request_key='run-1'")

    def test_later_source_conflict_marks_report_review_without_destroying_history(self):
        report = self.repo.run('run-2', self.protocol, self.scenarios)['report']
        self.change_source()
        state = self.repo.inspect('run-2')
        self.assertEqual(state['status'], 'review_required')
        self.assertEqual(self.repo.get('run-2'), report)
        self.assertNotIn('paired_description', state)

    def test_rehashed_report_with_mismatched_control_is_withdrawn(self):
        report = deepcopy(self.repo.run('unfair', self.protocol, self.scenarios)['report'])
        period = next(p for p in report['periods'] if p['status'] == 'complete')
        pair = period['replicas'][0]['pair']
        pair['control']['cost_fen'] += 1
        contents = {key: value for key, value in pair.items() if key != 'pair_sha256'}
        pair['pair_sha256'] = hashlib.sha256(_canonical(contents)).hexdigest()
        raw = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER backtest_reports_no_update')
            db.execute('UPDATE backtest_reports SET report_json=?,report_sha256=? WHERE request_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), 'unfair'))
            db.execute("CREATE TRIGGER backtest_reports_no_update BEFORE UPDATE ON backtest_reports BEGIN SELECT RAISE(ABORT, 'immutable backtest report'); END")
        result = self.repo.inspect('unfair')
        self.assertEqual(result['status'], 'review_required')
        self.assertIn('配对结构', result['reason'])
        self.assertNotIn('paired_description', result)
        self.assertEqual(self.repo.get('unfair'), report)
        from dlt.integrity import check_database
        check = check_database(self.path)
        self.assertTrue(any(item['area'] == 'backtest_reports' for item in check['errors']), check)

    def test_rehashed_legal_control_that_does_not_replay_is_withdrawn(self):
        report = deepcopy(self.repo.run('unreplayable', self.protocol, self.scenarios)['report'])
        period = next(p for p in report['periods'] if p['status'] == 'complete')
        pair = period['replicas'][0]['pair']
        ticket = pair['control']['tickets'][0]
        old = set(ticket['front'])
        ticket['front'] = sorted(ticket['front'][1:] + [next(v for v in range(1,36) if v not in old)])
        pair['pair_sha256'] = hashlib.sha256(_canonical({k: v for k, v in pair.items() if k != 'pair_sha256'})).hexdigest()
        raw = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER backtest_reports_no_update')
            db.execute('UPDATE backtest_reports SET report_json=?,report_sha256=? WHERE request_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), 'unreplayable'))
        result = self.repo.inspect('unreplayable')
        self.assertEqual(result['status'], 'review_required')
        self.assertIn('重放', result['reason'])
        self.assertEqual(self.repo.get('unreplayable'), report)

    def test_rehashed_wrong_model_candidate_is_not_a_current_result(self):
        for method in ('tree', 'lstm'):
            with self.subTest(method=method):
                cfg = protocol(self.protocol['snapshot_id'], method)
                key = 'wrong-model-' + method
                report = deepcopy(self.repo.run(key, cfg, self.scenarios)['report'])
                period = next(p for p in report['periods'] if p['status'] == 'complete')
                pair = period['replicas'][0]['pair']
                ticket = pair['candidate']['tickets'][0]
                old = set(ticket['front'])
                ticket['front'] = sorted(ticket['front'][1:] + [next(n for n in range(1,36) if n not in old)])
                pair['pair_sha256'] = hashlib.sha256(_canonical({k:v for k,v in pair.items() if k != 'pair_sha256'})).hexdigest()
                raw = _canonical(report)
                with closing(sqlite3.connect(self.path)) as db, db:
                    db.execute('DROP TRIGGER backtest_reports_no_update')
                    db.execute('UPDATE backtest_reports SET report_json=?,report_sha256=? WHERE request_key=?',
                               (raw, hashlib.sha256(raw).hexdigest(), key))
                    db.execute("CREATE TRIGGER backtest_reports_no_update BEFORE UPDATE ON backtest_reports BEGIN SELECT RAISE(ABORT, 'immutable backtest report'); END")
                result = self.repo.inspect(key)
                self.assertEqual(result['status'], 'review_required')
                self.assertIn('模型候选', result['reason'])
                self.assertEqual(self.repo.get(key), report)

    def test_legacy_report_without_tax_unit_is_not_current(self):
        scenarios_without_unit = deepcopy(self.scenarios)
        for item in scenarios_without_unit.values():
            del item['rule']['tax']['unit']
        with patch('dlt.settlement.require_tax_unit'):
            report = self.repo.run('legacy-tax', self.protocol, scenarios_without_unit)['report']
        state = self.repo.inspect('legacy-tax')
        self.assertEqual(state['status'], 'review_required')
        self.assertIn('计税单位', state['reason'])
        self.assertEqual(self.repo.get('legacy-tax'), report)
        self.assertNotIn('paired_description', state)

    def test_inconsistent_period_arithmetic_is_review_required_without_rewriting_report(self):
        report = self.repo.run('tampered', self.protocol, self.scenarios)['report']
        period = next(p for p in report['periods'] if p['status'] == 'complete')
        period['replicas'][0]['difference_yuan'] = '987654321'
        raw = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER backtest_reports_no_update')
            db.execute('UPDATE backtest_reports SET report_json=?,report_sha256=? WHERE request_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), 'tampered'))
        result = self.repo.inspect('tampered')
        self.assertEqual(result['status'], 'review_required')
        self.assertNotIn('paired_description', result)
        self.assertEqual(self.repo.get('tampered'), report)

    def test_precise_legacy_report_is_rechecked_without_retraining(self):
        report = self.repo.run('legacy-good', self.protocol, self.scenarios)['report']
        report.pop('calculation_version'); report.pop('settlement_code_sha256')
        report['code_sha256'] = '0' * 64
        raw = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER backtest_reports_no_update')
            db.execute('UPDATE backtest_reports SET report_json=?,report_sha256=? WHERE request_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), 'legacy-good'))
        with patch('dlt.backtest._generate', side_effect=AssertionError('must not retrain')):
            state = self.repo.inspect('legacy-good')
        self.assertEqual(state['status'], 'research_only', state)
        self.assertEqual(state['report'], report)

    def test_wrong_legacy_gross_and_tax_are_withdrawn_even_when_net_is_consistent(self):
        report = self.repo.run('legacy-wrong', self.protocol, self.scenarios)['report']
        report.pop('calculation_version'); report.pop('settlement_code_sha256')
        report['code_sha256'] = '0' * 64
        replica = next(p for p in report['periods'] if p['status'] == 'complete')['replicas'][0]
        row = replica['candidate_settlement']
        row['gross'] = str(Decimal(row['gross']) + 1)
        row['tax'] = str(Decimal(row['tax']) + 1)  # net, paired difference and means remain unchanged
        raw = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER backtest_reports_no_update')
            db.execute('UPDATE backtest_reports SET report_json=?,report_sha256=? WHERE request_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), 'legacy-wrong'))
        state = self.repo.inspect('legacy-wrong')
        self.assertEqual(state['status'], 'review_required', state)
        self.assertIn('精确计算不一致', state['reason'])
        self.assertNotIn('paired_description', state)
        fresh = self.repo.run('recalculated', self.protocol, self.scenarios)
        self.assertEqual(fresh['status'], 'research_only')
        self.assertEqual(self.repo.get('legacy-wrong'), report)

    def test_rebuild_changes_only_amounts_using_frozen_tickets(self):
        report = self.repo.run('base', self.protocol, self.scenarios)['report']
        replica = report['periods'][0]['replicas'][0]
        original = replica['candidate_settlement']['gross']
        replica['candidate_settlement']['gross'] = str(Decimal(original) + 1)
        replica['candidate_settlement']['tax'] = str(Decimal(replica['candidate_settlement']['tax']) + 1)
        wrong = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, patch('dlt.backtest._generate', side_effect=AssertionError('must not train')):
            db.execute('BEGIN')
            corrected = _recheck_settlements(db, report, rebuild=True)
            _recheck_settlements(db, corrected)
        self.assertEqual(_canonical(report), wrong)
        self.assertEqual(corrected['periods'][0]['replicas'][0]['candidate_settlement']['gross'], original)
        self.assertEqual(corrected['periods'][0]['replicas'][0]['pair'], replica['pair'])
        self.assertEqual(corrected['protocol'], report['protocol'])
        self.assertEqual(corrected['scenarios'], report['scenarios'])

    def test_rebuild_rejects_ticket_or_snapshot_tampering(self):
        report = self.repo.run('base-tamper', self.protocol, self.scenarios)['report']
        report['periods'][0]['replicas'][0]['pair']['candidate']['tickets'][0]['front'][0] = 35
        with closing(sqlite3.connect(self.path)) as db:
            with self.assertRaisesRegex(BacktestError, 'digest mismatch'):
                _recheck_settlements(db, report, rebuild=True)

    def test_rebuild_preserves_incomplete_periods_without_filling_zero(self):
        del self.scenarios['26019']
        report = self.repo.run('partial-rebuild', self.protocol, self.scenarios)['report']
        incomplete = next(period for period in report['periods'] if period['target']['issue'] == '26019')
        self.assertEqual(incomplete['status'], 'incomplete')
        first = report['periods'][0]['replicas'][0]['candidate_settlement']
        first['gross'] = str(Decimal(first['gross']) + 1)
        with closing(sqlite3.connect(self.path)) as db:
            corrected = _recheck_settlements(db, report, rebuild=True)
        updated = next(period for period in corrected['periods'] if period['target']['issue'] == '26019')
        self.assertEqual(updated, incomplete)
        self.assertIsNone(updated['mean_difference_fraction'])
        self.assertEqual(corrected['incomplete_issues'], ['26019'])

    def test_missing_historical_protocol_is_reported_for_review(self):
        report = self.repo.run('missing-protocol', self.protocol, self.scenarios)['report']
        report['protocol'] = None
        raw = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER backtest_reports_no_update')
            db.execute('UPDATE backtest_reports SET report_json=?,report_sha256=? WHERE request_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), 'missing-protocol'))
        result = self.repo.inspect('missing-protocol')
        self.assertEqual(result['status'], 'review_required')
        self.assertNotIn('paired_description', result)

    def test_conflict_during_execution_persists_only_as_review_required(self):
        def compute_then_conflict(*args, **kwargs):
            report = run_from_snapshot(*args, **kwargs)
            self.change_source()
            return report
        with patch('dlt.backtest_repository.run_from_snapshot', side_effect=compute_then_conflict):
            result = self.repo.run('run-3', self.protocol, self.scenarios)
        self.assertEqual(result['status'], 'review_required')
        self.assertEqual(self.repo.get('run-3'), result['report'])

    def test_repository_never_publishes_unverified_holdout_role(self):
        with self.assertRaisesRegex(BacktestError, 'holdout independence'):
            self.repo.run('not-independent', {**self.protocol, 'selection_role': 'holdout'}, self.scenarios)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='backtest_reports'").fetchone())

    def test_legacy_holdout_report_remains_auditable_but_requires_review(self):
        report = self.repo.run('old', self.protocol, self.scenarios)['report']
        report['protocol']['selection_role'] = 'holdout'
        report['protocol_sha256'] = hashlib.sha256(_canonical(report['protocol'])).hexdigest()
        raw = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER backtest_reports_no_update')
            db.execute('UPDATE backtest_reports SET report_json=?,report_sha256=? WHERE request_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), 'old'))
        inspected = self.repo.inspect('old')
        self.assertEqual(inspected['status'], 'review_required')
        self.assertEqual(inspected['report']['protocol']['selection_role'], 'holdout')
        with self.assertRaises(BacktestError):
            self.repo.run('old', report['protocol'], self.scenarios)


if __name__ == '__main__':
    unittest.main()
