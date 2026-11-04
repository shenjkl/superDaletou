"""Cross-pair historical comparisons without opening a network socket."""
from contextlib import closing
from copy import deepcopy
import json
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

from dlt.app import Handler
from dlt.evaluation import EvaluationError
from dlt.evaluation_repository import EvaluationRepository
from dlt.pair_repository import PairRepository
from dlt.scenarios import historical_scenario
import test_scenarios as fixtures


class PortfolioComparisonTests(unittest.TestCase):
    conflict = fixtures.ScenarioTests.conflict

    def setUp(self):
        fixtures.ScenarioTests.setUp(self)
        cfg = {**self.pair['config'], 'budget_fen': 600, 'seed': 17}
        self.pair2 = PairRepository(self.db).save_draft('pair-two', cfg,
                           history_first_issue=self.pair['history_first'], history_cutoff_issue=self.pair['history_last'])
        self.evaluations.run('one', 'pair', **self.args)
        scenario = historical_scenario(self.db, self.snapshot, self.pair2, '26123')
        self.evaluations.run('two', 'pair-two', **{**self.args, 'scenario': scenario})

    def test_different_costs_and_full_pair_results_are_preserved(self):
        result = self.evaluations.compare_pairs(['one', 'two'])
        self.assertEqual([entry['pair_key'] for entry in result['entries']], ['pair', 'pair-two'])
        self.assertEqual([entry['pair']['candidate']['cost_fen'] for entry in result['entries']], [400, 600])
        self.assertEqual([entry['result']['scenario']['source_issue'] for entry in result['entries']], ['26123', '26123'])
        self.assertIn('not an adoption recommendation', result['assurance'])
        self.assertEqual({entry['request_key'] for entry in self.evaluations.comparison_options()}, {'one', 'two'})

    def test_mismatched_scenario_or_repeated_pair_is_rejected(self):
        different = historical_scenario(self.db, self.snapshot, self.pair2, '26122')
        self.evaluations.run('different', 'pair-two', **{**self.args, 'scenario': different})
        with self.assertRaisesRegex(EvaluationError, '假设不一致'):
            self.evaluations.compare_pairs(['one', 'different'])
        self.evaluations.run('same-pair', 'pair', **self.args)
        with self.assertRaisesRegex(EvaluationError, '不同的组合'):
            self.evaluations.compare_pairs(['one', 'same-pair'])
        for keys in (['one'], ['one', 'one'], ['one', 'missing'], ['one'] * 4):
            with self.subTest(keys=keys), self.assertRaises(EvaluationError):
                self.evaluations.compare_pairs(keys)

    def test_different_tax_assumption_or_target_is_rejected(self):
        tax = historical_scenario(self.db, self.snapshot, self.pair2, '26123')
        tax['rule']['tax']['rate'] = '0.1'
        self.evaluations.run('tax', 'pair-two', **{**self.args, 'scenario': tax})
        with self.assertRaisesRegex(EvaluationError, '假设不一致'):
            self.evaluations.compare_pairs(['one', 'tax'])
        config = {**self.pair2['config'], 'target_issue': '26125', 'target_date': '2026-11-02', 'seed': 19}
        future = PairRepository(self.db).save_draft('future', config, history_first_issue=self.pair['history_first'],
                                                     history_cutoff_issue=self.pair['history_last'])
        scenario = historical_scenario(self.db, self.snapshot, future, '26123')
        self.evaluations.run('future-eval', 'future', **{**self.args, 'scenario': scenario})
        with self.assertRaisesRegex(EvaluationError, '目标期'):
            self.evaluations.compare_pairs(['one', 'future-eval'])

    def test_different_ticket_prices_remain_visible_as_separate_cost_assumptions(self):
        config = {**self.pair2['config'], 'base_price_fen': 300, 'additional_price_fen': 400, 'seed': 29}
        priced = PairRepository(self.db).save_draft('priced', config, history_first_issue=self.pair['history_first'],
                                                     history_cutoff_issue=self.pair['history_last'])
        scenario = historical_scenario(self.db, self.snapshot, priced, '26123')
        self.evaluations.run('priced-eval', 'priced', **{**self.args, 'scenario': scenario})
        result = self.evaluations.compare_pairs(['one', 'priced-eval'])
        prices = [entry['pair']['config']['base_price_fen'] for entry in result['entries']]
        self.assertEqual(prices, [200, 300])
        self.assertEqual([entry['pair']['candidate']['cost_fen'] for entry in result['entries']], [400, 600])

    def test_source_conflict_blocks_the_whole_comparison(self):
        self.conflict()
        with self.assertRaisesRegex(EvaluationError, '需要复核'):
            self.evaluations.compare_pairs(['one', 'two'])
        self.assertTrue(all(item['status'] == 'review_required' for item in self.evaluations.comparison_options()))

    def test_comparison_reads_a_consistent_snapshot_during_new_conflict(self):
        with closing(sqlite3.connect(self.db)) as db:
            db.execute('PRAGMA journal_mode=WAL')
        original, calls = EvaluationRepository._check_scenario, []
        def check_then_conflict(db, scenario):
            original(db, scenario)
            calls.append(scenario['source_issue'])
            if len(calls) == 1:
                self.conflict()
        with patch.object(EvaluationRepository, '_check_scenario', side_effect=check_then_conflict):
            result = self.evaluations.compare_pairs(['one', 'two'])
        self.assertEqual(len(result['entries']), 2)
        with self.assertRaisesRegex(EvaluationError, '需要复核'):
            self.evaluations.compare_pairs(['one', 'two'])

    def test_session_protected_routes_return_compatible_results(self):
        def get(path, *, session=True):
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(db_path=self.db)
            handler.path = path
            handler._validate_host = lambda: True
            handler._session = lambda: session
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_GET()
        status, data = get('/api/evaluations/comparison-options')
        self.assertEqual(status, 200)
        self.assertEqual({item['request_key'] for item in data['options']}, {'one', 'two'})
        query = urlencode([('key', 'one'), ('key', 'two')])
        status, data = get('/api/evaluations/compare?' + query)
        self.assertEqual(status, 200)
        self.assertEqual(len(data['entries']), 2)
        self.assertEqual(get('/api/evaluations/compare?' + query, session=False)[0], 401)
        self.assertEqual(get('/api/evaluations/compare?key=one')[0], 409)


if __name__ == '__main__':
    unittest.main()
