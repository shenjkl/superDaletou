"""Custom fixed-payout research scenarios; no network or live service."""
from contextlib import closing
from copy import deepcopy
from io import BytesIO
import hashlib
import json
import sqlite3
import threading
from types import SimpleNamespace
import unittest

from dlt.app import Handler
from dlt.candidates import _canonical
from dlt.custom_scenarios import custom_scenario, PAYOUT_KEYS
from dlt.evaluation import EvaluationError
from dlt.pair_repository import PairRepository
from dlt.scenarios import ScenarioError
from dlt.settlement import settle_portfolio
import test_scenarios as fixtures


def inputs():
    return {'label': '固定奖金', 'payouts': {key: str(index * 10) for index, key in enumerate(PAYOUT_KEYS, 1)},
            'provenance': {'kind': 'synthetic'}}


class CustomScenarioTests(unittest.TestCase):
    def setUp(self):
        fixtures.ScenarioTests.setUp(self)

    def test_validation_and_normalization(self):
        sc = custom_scenario(self.pair, inputs())
        self.assertEqual(sc['payouts']['101'], '10.00')
        self.assertEqual(sc['rule']['additional_groups']['101'], '201')
        self.assertEqual(sc['source_issue'], 'custom')
        self.assertNotIn('source_dependency', sc)
        self.assertIn('not a verified', sc['assurance'])
        for value in ('', '-1', '1e3', '1.001', '01', '9'*19, 10, None):
            bad = inputs(); bad['payouts']['101'] = value
            with self.subTest(value=value), self.assertRaises(ScenarioError):
                custom_scenario(self.pair, bad)
        for bad in ({}, {'label': 'x', 'payouts': {}}, {'label': ' ', 'payouts': inputs()['payouts']},
                    {**inputs(), 'source_dependency': {}}):
            with self.subTest(bad=bad), self.assertRaises(ScenarioError):
                custom_scenario(self.pair, bad)

    def test_additional_payout_and_tax_with_independent_example(self):
        data = inputs(); data['payouts']['101'] = '12000'; data['payouts']['201'] = '6000'
        sc = custom_scenario(self.pair, data)
        draw = {'issue': '26124', 'front': [1, 2, 3, 4, 5], 'back': [1, 2], 'promotion': False}
        ticket = {'front': [1, 2, 3, 4, 5], 'back': [1, 2], 'additional': True, 'multiplier': 1}
        settled = settle_portfolio(issue='26124', draw=draw, tickets=[ticket], payouts=sc['payouts'],
                                   rule=sc['rule'], duplicate_policy='reject')
        self.assertEqual(settled['gross'], '18000.00')
        self.assertEqual(settled['tax'], '3600.00')
        self.assertEqual(settled['cost'], '3')
        self.assertEqual(settled['net'], '14397.00')

    def test_persist_reopen_and_compare_pairs_under_same_assumptions(self):
        sc = custom_scenario(self.pair, inputs())
        first = self.evaluations.run('custom-one', 'pair', **{**self.args, 'scenario': sc})
        self.assertEqual(first['scenario']['source_issue'], 'custom')
        self.assertEqual(self.evaluations.inspect('custom-one')['status'], 'research_draft')
        config = {**self.pair['config'], 'budget_fen': 600, 'seed': 17}
        second = PairRepository(self.db).save_draft('pair-two', config,
                    history_first_issue=self.pair['history_first'], history_cutoff_issue=self.pair['history_last'])
        renamed = inputs(); renamed['label'] = '另一名称'
        self.evaluations.run('custom-two', 'pair-two', **{**self.args, 'scenario': custom_scenario(second, renamed)})
        comparison = self.evaluations.compare_pairs(['custom-one', 'custom-two'])
        self.assertEqual(len(comparison['entries']), 2)
        self.assertEqual({o['scenario_kind'] for o in self.evaluations.comparison_options()}, {'custom_payout_v1'})
        changed = inputs(); changed['payouts']['101'] = '100'
        self.evaluations.run('different', 'pair-two', **{**self.args, 'scenario': custom_scenario(second, changed)})
        with self.assertRaisesRegex(EvaluationError, '假设不一致'):
            self.evaluations.compare_pairs(['custom-one', 'different'])

    def test_modified_scenario_even_with_rehashed_request_and_result_is_withdrawn(self):
        sc = custom_scenario(self.pair, inputs())
        self.evaluations.run('custom-one', 'pair', **{**self.args, 'scenario': sc})
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute('DROP TRIGGER draft_evaluations_no_update')
            request_raw, result_raw = db.execute('SELECT request_json,result_json FROM draft_evaluations WHERE request_key=?', ('custom-one',)).fetchone()
            request, result = json.loads(request_raw), json.loads(result_raw)
            request['scenario']['rule']['tax']['rate'] = '0.10'
            result['scenario_sha256'] = hashlib.sha256(_canonical(request['scenario'])).hexdigest()
            payload = _canonical(result)
            db.execute('UPDATE draft_evaluations SET request_json=?,result_json=?,result_sha256=? WHERE request_key=?',
                       (_canonical(request), payload, hashlib.sha256(payload).hexdigest(), 'custom-one'))
        checked = self.evaluations.inspect('custom-one')
        self.assertEqual(checked['status'], 'review_required')
        self.assertIn('固定输入不一致', checked['reason'])

    def test_route_idempotency_validation_and_origin(self):
        def call(body, origin='http://127.0.0.1:8766'):
            encoded = json.dumps(body).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.db,
                                             evaluation_lock=threading.Lock())
            handler.path = '/api/evaluations'
            handler.headers = {'Origin': origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': 'secret',
                               'Content-Type': 'application/json', 'Content-Length': str(len(encoded))}
            handler.rfile = BytesIO(encoded)
            handler._validate_host = lambda: True
            handler._session = lambda: True
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        body = {'request_key': 'route-custom', 'pair_key': 'pair', 'scenario_kind': 'custom_payout_v1',
                'custom_input': inputs(), 'samples': 1000, 'seed': 3}
        status, created = call(body)
        self.assertEqual(status, 200, created)
        self.assertEqual(created['request']['scenario']['source_issue'], 'custom')
        self.assertEqual(call(body)[1]['request'], created['request'])
        changed = deepcopy(body); changed['custom_input']['payouts']['101'] = '123'
        self.assertEqual(call(changed)[0], 400)
        invalid = deepcopy(body); invalid['request_key'] = 'bad'; invalid['custom_input']['payouts']['101'] = '-1'
        self.assertEqual(call(invalid)[0], 400)
        self.assertEqual(call({**body, 'request_key': 'cross'}, origin='https://example.invalid')[0], 403)


if __name__ == '__main__':
    unittest.main()
