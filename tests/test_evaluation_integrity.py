"""Cross-record checks before publishing an isolated SQLite copy."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from dlt.candidates import _canonical
from dlt.custom_scenarios import custom_scenario, PAYOUT_KEYS
from dlt.integrity import check_database
from dlt.recovery import RecoveryError, prepare_isolated_copy
import test_scenarios as fixtures


class EvaluationIntegrityTests(unittest.TestCase):
    conflict = fixtures.ScenarioTests.conflict

    def setUp(self):
        fixtures.ScenarioTests.setUp(self)
        self.evaluations.run('historical', 'pair', **self.args)
        custom = custom_scenario(self.pair, {'label': '用户假设', 'payouts': {key: '10' for key in PAYOUT_KEYS}, 'provenance': {'kind': 'synthetic'}})
        self.evaluations.run('custom', 'pair', **{**self.args, 'scenario': custom})
        workspace = Path(__file__).resolve().parents[1]
        self.directory = tempfile.TemporaryDirectory(dir=workspace)
        self.addCleanup(self.directory.cleanup)
        self.target = Path(self.directory.name) / 'candidate.sqlite3'

    def mutate(self, key, change):
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute('DROP TRIGGER draft_evaluations_no_update')
            request_raw, result_raw = db.execute('SELECT request_json,result_json FROM draft_evaluations WHERE request_key=?', (key,)).fetchone()
            request, result = json.loads(request_raw), json.loads(result_raw)
            change(request, result)
            payload = _canonical(result)
            db.execute('UPDATE draft_evaluations SET request_json=?,result_json=?,result_sha256=? WHERE request_key=?',
                       (_canonical(request), payload, hashlib.sha256(payload).hexdigest(), key))
            db.execute("CREATE TRIGGER draft_evaluations_no_update BEFORE UPDATE ON draft_evaluations BEGIN SELECT RAISE(ABORT, 'immutable evaluation'); END")

    def assert_rejected(self):
        report = check_database(self.db)
        self.assertEqual(report['status'], 'failed', report)
        self.assertTrue(any(item['area'] == 'draft_evaluations' for item in report['errors']), report)
        with self.assertRaises(RecoveryError):
            prepare_isolated_copy(self.db, self.target)
        self.assertFalse(self.target.exists())

    def test_healthy_and_conflicted_history_can_be_preserved_without_being_declared_current(self):
        self.assertEqual(check_database(self.db)['status'], 'passed')
        self.conflict()
        self.assertEqual(self.evaluations.inspect('historical')['status'], 'review_required')
        report = check_database(self.db)
        self.assertEqual(report['status'], 'passed', report)
        result = prepare_isolated_copy(self.db, self.target)
        self.assertEqual(result['status'], 'prepared')
        self.assertEqual(result['check']['counts']['draft_evaluations'], 2)

    def test_request_seed_mismatch_blocks_candidate(self):
        self.mutate('custom', lambda request, _result: request.__setitem__('seed', request['seed'] + 1))
        self.assert_rejected()

    def test_custom_rule_tampering_is_detected_even_after_rehash(self):
        def alter(request, result):
            request['scenario']['rule']['tax']['rate'] = '0.10'
            result['scenario_sha256'] = hashlib.sha256(_canonical(request['scenario'])).hexdigest()
        self.mutate('custom', alter)
        self.assert_rejected()

    def test_historical_dependency_tampering_is_detected_even_after_rehash(self):
        def alter(request, result):
            request['scenario']['source_dependency']['row_sha256'] = '0' * 64
            result['scenario_sha256'] = hashlib.sha256(_canonical(request['scenario'])).hexdigest()
        self.mutate('historical', alter)
        self.assert_rejected()

    def test_forged_result_description_and_rehashed_cost_are_rejected(self):
        for field in ('description', 'cost'):
            with self.subTest(field=field):
                def alter(_request, result):
                    if field == 'description':
                        result['scenario']['reference'] = 'incorrect assumption'
                    else:
                        result['portfolios']['candidate']['cost_yuan'] = '999'
                self.mutate('custom', alter)
                self.assert_rejected()
                # Each case starts from a valid, independently saved result.
                if field == 'description':
                    with closing(sqlite3.connect(self.db)) as db, db:
                        db.execute('DROP TRIGGER draft_evaluations_no_delete')
                        db.execute("DELETE FROM draft_evaluations WHERE request_key='custom'")
                        db.execute("CREATE TRIGGER draft_evaluations_no_delete BEFORE DELETE ON draft_evaluations BEGIN SELECT RAISE(ABORT, 'immutable evaluation'); END")
                    custom = custom_scenario(self.pair, {'label': '用户假设', 'payouts': {key: '10' for key in PAYOUT_KEYS}, 'provenance': {'kind': 'synthetic'}})
                    self.evaluations.run('custom', 'pair', **{**self.args, 'scenario': custom})

    def test_rehashed_forged_theoretical_mean_is_detected(self):
        def alter(_request, result):
            result['portfolios']['candidate']['mean_gross_tax_before']['fraction'] = [1, 1]
            result['portfolios']['candidate']['mean_gross_tax_before']['yuan'] = '1'
        self.mutate('custom', alter)
        self.assert_rejected()

    def test_rehashed_forged_coverage_is_detected(self):
        def alter(_request, result):
            result['portfolios']['candidate']['jackpot_coverage']['unique_combinations'] = 0
            result['portfolios']['candidate']['jackpot_coverage']['probability'] = 0
        self.mutate('custom', alter)
        self.assert_rejected()

    def test_invalid_historical_payout_fails_closed_instead_of_raising(self):
        def alter(request, result):
            request['scenario']['payouts']['101'] = 'not-money'
            result['scenario_sha256'] = hashlib.sha256(_canonical(request['scenario'])).hexdigest()
        self.mutate('historical', alter)
        self.assert_rejected()


if __name__ == '__main__':
    unittest.main()
