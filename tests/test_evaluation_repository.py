import json
from copy import deepcopy
import hashlib
from pathlib import Path
from contextlib import closing
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from dlt.evaluation import evaluate_pair, EvaluationError
from dlt.evaluation_repository import EvaluationRepository, _legacy_default_precision_safe
from dlt.candidates import _canonical
from dlt.pair_repository import PairRepository
from dlt.snapshots import SnapshotStore
from test_candidates import config, input_rows
from test_evaluation import scenario


class EvaluationRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'data.sqlite3'
        self.source = {'success': True, 'errorCode': '0', 'value': {'list': [
            {'lotteryGameNum': '85', 'lotteryDrawNum': r['issue'], 'lotteryDrawTime': r['date'],
             'lotteryDrawResult': ' '.join(map(str, r['front'] + r['back'])), 'prizeLevelList': []}
            for r in input_rows()]}}
        snapshot = SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'fixture',
                                                             '2026-01-17T12:00:00+08:00')['snapshot_id']
        PairRepository(self.path).save_draft('pair', {**config(), 'snapshot_id': snapshot, 'budget_fen': 400},
                                             history_first_issue='26001', history_cutoff_issue='26016')
        self.repo = EvaluationRepository(self.path)
        self.args = {'scenario': scenario(), 'samples': 100, 'seed': 7,
                     'max_samples': 1000, 'max_ticket_draws': 10000}

    def change_source(self):
        self.source['value']['list'][0]['lotteryDrawResult'] = '10 11 12 13 14 01 02'
        SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'other',
                                                '2026-01-18T12:00:00+08:00')

    def test_restart_and_idempotency(self):
        result = self.repo.run('evaluation', 'pair', **self.args)
        self.assertEqual(result, EvaluationRepository(self.path).get('evaluation'))
        self.assertEqual(result, self.repo.run('evaluation', 'pair', **self.args))
        self.assertEqual(self.repo.inspect('evaluation')['status'], 'research_draft')
        with self.assertRaisesRegex(EvaluationError, 'different input'):
            self.repo.run('evaluation', 'pair', **{**self.args, 'seed': 8})

    def test_existing_result_requires_review_after_new_conflict(self):
        prior = self.repo.run('evaluation', 'pair', **self.args)
        self.change_source()
        self.assertEqual(self.repo.get('evaluation'), prior)
        self.assertEqual(self.repo.inspect('evaluation')['status'], 'review_required')
        with self.assertRaisesRegex(EvaluationError, 'conflicts'):
            self.repo.run('another', 'pair', **self.args)

    def test_conflict_during_calculation_rejected_before_commit(self):
        def evaluate_then_conflict(*args, **kwargs):
            result = evaluate_pair(*args, **kwargs)
            self.change_source()
            return result
        with patch('dlt.evaluation_repository.evaluate_pair', side_effect=evaluate_then_conflict):
            with self.assertRaisesRegex(EvaluationError, 'conflicts'):
                self.repo.run('evaluation', 'pair', **self.args)
        with self.assertRaises(KeyError):
            self.repo.get('evaluation')

    def _make_legacy(self, key):
        result = self.repo.get(key)
        result['sampling'].pop('calculation_version')
        result['sampling'].pop('settlement_code_sha256')
        result['sampling']['code_sha256'] = '0' * 64
        raw = _canonical(result)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draft_evaluations_no_update')
            db.execute('UPDATE draft_evaluations SET result_json=?,result_sha256=? WHERE request_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), key))
        return result

    def test_ordinary_legacy_amounts_are_readable_under_default_precision_bound(self):
        self.repo.run('legacy-normal', 'pair', **self.args)
        original = self._make_legacy('legacy-normal')
        state = self.repo.inspect('legacy-normal')
        self.assertEqual(state['status'], 'research_draft', state)
        self.assertEqual(state['result'], original)

    def test_legacy_result_without_tax_unit_is_not_current(self):
        args = deepcopy(self.args)
        del args['scenario']['rule']['tax']['unit']
        with patch('dlt.settlement.require_tax_unit'):
            original = self.repo.run('legacy-tax', 'pair', **args)
        state = self.repo.inspect('legacy-tax')
        self.assertEqual(state['status'], 'review_required')
        self.assertIn('tax unit', state['reason'])
        self.assertEqual(self.repo.get('legacy-tax'), original)

    def test_large_legacy_evaluation_is_withdrawn_and_new_run_preserves_old(self):
        args = {**self.args, 'scenario': scenario()}
        args['scenario']['payouts']['first'] = '12345678901234567890123456.78'
        self.repo.run('legacy-large', 'pair', **args)
        original = self._make_legacy('legacy-large')
        state = self.repo.inspect('legacy-large')
        self.assertEqual(state['status'], 'review_required')
        self.assertIn('重新评估', state['reason'])
        with self.assertRaisesRegex(EvaluationError, '需要复核'):
            self.repo.compare_pairs(['legacy-large', 'missing'])
        result = self.repo.run('precise-new', 'pair', **args)
        self.assertEqual(result['sampling']['calculation_version'], 'exact-money-v1')
        self.assertEqual(self.repo.inspect('precise-new')['status'], 'research_draft')
        self.assertEqual(self.repo.get('legacy-large'), original)

    def test_legacy_precision_bound_rejects_extreme_scale_and_multiplier(self):
        pair = PairRepository(self.path).get('pair')
        sc = scenario()
        self.assertTrue(_legacy_default_precision_safe(pair, sc))
        sc['rule']['tax']['rate'] = '0.00000000000000000000001'
        self.assertFalse(_legacy_default_precision_safe(pair, sc))
        sc = scenario(); sc['payouts']['first'] = '0.' + '0' * 29 + '1'
        self.assertFalse(_legacy_default_precision_safe(pair, sc))
        sc = scenario(); pair['candidate']['tickets'][0]['multiplier'] = 1000001
        self.assertFalse(_legacy_default_precision_safe(pair, sc))


if __name__ == '__main__':
    unittest.main()
