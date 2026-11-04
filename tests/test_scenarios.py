from copy import deepcopy
from contextlib import closing
from io import BytesIO
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dlt.evaluation import EvaluationError, evaluate_pair
from dlt.evaluation_repository import EvaluationRepository
from dlt.pair_repository import PairRepository
from dlt.scenarios import ScenarioError, historical_26123, historical_scenario
from dlt.snapshots import SnapshotStore
from dlt.app import Handler
from test_candidates import config


class ScenarioTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name)/'data.sqlite3'
        raw = (Path(__file__).resolve().parents[1]/'research/data/official_page1.json').read_bytes()
        self.source = json.loads(raw)
        self.snapshot = SnapshotStore(self.db).import_official(raw, 'local historical fixture',
                                                               '2026-10-29T12:00:00+08:00')['snapshot_id']
        rows = SnapshotStore(self.db).get_draws(self.snapshot)
        cfg = {**config(), 'snapshot_id': self.snapshot, 'target_issue': '26124', 'target_date': '2026-10-31',
               'budget_fen': 400}
        self.repo = PairRepository(self.db)
        self.pair = self.repo.save_draft('pair', cfg, history_first_issue=rows[0]['issue'], history_cutoff_issue=rows[-1]['issue'])
        self.scenario = historical_26123(self.db, self.snapshot, self.pair)
        self.evaluations = EvaluationRepository(self.db)
        self.args = {'scenario': self.scenario, 'samples': 100, 'seed': 7, 'max_samples': 1000, 'max_ticket_draws': 10000}

    def conflict(self):
        changed = deepcopy(self.source)
        changed['value']['list'][0]['prizeLevelList'][0]['stakeAmount'] = '1'
        SnapshotStore(self.db).import_official(json.dumps(changed).encode(), 'synthetic changed payout',
                                              '2026-10-30T12:00:00+08:00')

    def test_historical_payouts_and_explicit_assumptions(self):
        self.assertEqual(self.scenario['payouts']['101'], '5,750,462')
        self.assertIn('不重算', self.scenario['adjustment_assumption'])
        self.assertIn('not a verified', self.scenario['assurance'])
        result = self.evaluations.run('evaluation', 'pair', **self.args)
        self.assertEqual(result['pair_sha256'], self.pair['pair_sha256'])
        self.assertEqual(self.evaluations.latest_for_pair('pair')['status'], 'research_draft')

    def test_prize_only_conflict_invalidates_existing_result(self):
        self.evaluations.run('evaluation', 'pair', **self.args)
        self.conflict()
        self.assertEqual(self.repo.inspect('pair')['status'], 'research_draft')
        self.assertEqual(self.evaluations.inspect('evaluation')['status'], 'review_required')
        with self.assertRaisesRegex(ScenarioError, '来源存在差异'):
            historical_26123(self.db, self.snapshot, self.pair)

    def test_conflict_during_simulation_blocks_publication(self):
        def evaluate_then_conflict(*args, **kwargs):
            result = evaluate_pair(*args, **kwargs)
            self.conflict()
            return result
        with patch('dlt.evaluation_repository.evaluate_pair', side_effect=evaluate_then_conflict):
            with self.assertRaisesRegex(EvaluationError, '来源有冲突'):
                self.evaluations.run('evaluation', 'pair', **self.args)
        with self.assertRaises(KeyError):
            self.evaluations.get('evaluation')

    def test_multiple_historical_scenarios_persist_separately(self):
        other = historical_scenario(self.db, self.snapshot, self.pair, '26122')
        self.assertEqual(other['source_dependency']['issue'], '26122')
        self.assertEqual(other['rule']['applicable_issues'], ['26124'])
        self.assertEqual(historical_scenario(self.db, self.snapshot, self.pair, '26123'), self.scenario)
        first = self.evaluations.run('first', 'pair', **self.args)
        second = self.evaluations.run('second', 'pair', **{**self.args, 'scenario': other})
        self.assertNotEqual(first['scenario_sha256'], second['scenario_sha256'])
        history = self.evaluations.list_for_pair('pair')
        self.assertEqual([item['request_key'] for item in history], ['second', 'first'])
        self.assertEqual([item['result']['scenario']['source_issue'] for item in history], ['26122', '26123'])
        self.assertEqual(self.evaluations.run('first', 'pair', **self.args), first)

    def test_scenario_range_and_promotion_are_blocked(self):
        for issue in ('26013', '26124', 'x'):
            with self.subTest(issue=issue), self.assertRaises(ScenarioError):
                historical_scenario(self.db, self.snapshot, self.pair, issue)
        changed = deepcopy(self.source)
        next(row for row in changed['value']['list'] if row['lotteryDrawNum'] == '26122')['lotteryPromotionFlag'] = 1
        sid = SnapshotStore(self.db).import_official(json.dumps(changed).encode(), 'changed fixture',
                                                      '2026-10-30T12:00:00+08:00')['snapshot_id']
        with self.assertRaises(ScenarioError):
            historical_scenario(self.db, sid, self.pair, '26122')

    def test_scenario_label_must_match_its_source_issue(self):
        scenario = historical_scenario(self.db, self.snapshot, self.pair, '26122')
        scenario['source_issue'] = '26123'
        with self.assertRaisesRegex(EvaluationError, '期号与来源不一致'):
            self.evaluations.run('mismatch', 'pair', **{**self.args, 'scenario': scenario})

    def test_conflict_invalidates_only_its_historical_scenario(self):
        self.evaluations.run('first', 'pair', **self.args)
        other = historical_scenario(self.db, self.snapshot, self.pair, '26122')
        self.evaluations.run('second', 'pair', **{**self.args, 'scenario': other})
        changed = deepcopy(self.source)
        row = next(row for row in changed['value']['list'] if row['lotteryDrawNum'] == '26122')
        row['prizeLevelList'][0]['stakeAmount'] = '1'
        SnapshotStore(self.db).import_official(json.dumps(changed).encode(), 'conflicting fixture',
                                              '2026-10-30T12:00:00+08:00')
        history = {item['request_key']: item for item in self.evaluations.list_for_pair('pair')}
        self.assertEqual(history['first']['status'], 'research_draft')
        self.assertEqual(history['second']['status'], 'review_required')

    def test_modified_request_is_review_required_and_history_remains_readable(self):
        self.evaluations.run('first', 'pair', **self.args)
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute('DROP TRIGGER draft_evaluations_no_update')
            request = json.loads(db.execute('SELECT request_json FROM draft_evaluations WHERE request_key=?', ('first',)).fetchone()[0])
            request['scenario']['payouts']['101'] = '1'
            db.execute('UPDATE draft_evaluations SET request_json=? WHERE request_key=?', (json.dumps(request).encode(), 'first'))
        inspected = self.evaluations.inspect('first')
        self.assertEqual(inspected['status'], 'review_required')
        self.assertIn('不一致', inspected['reason'])
        self.assertEqual(self.evaluations.list_for_pair('pair')[0]['status'], 'review_required')

    def test_malformed_result_is_review_required_even_if_digest_is_recomputed(self):
        self.evaluations.run('first', 'pair', **self.args)
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute('DROP TRIGGER draft_evaluations_no_update')
            result = json.loads(db.execute('SELECT result_json FROM draft_evaluations WHERE request_key=?', ('first',)).fetchone()[0])
            result['portfolios']['candidate']['any_prize'] = None
            raw = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
            db.execute('UPDATE draft_evaluations SET result_json=?, result_sha256=? WHERE request_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), 'first'))
        inspected = self.evaluations.inspect('first')
        self.assertEqual(inspected['status'], 'review_required')
        self.assertIn('字段损坏', inspected['reason'])

    def test_history_checks_all_scenarios_in_one_database_snapshot(self):
        self.evaluations.run('first', 'pair', **self.args)
        other = historical_scenario(self.db, self.snapshot, self.pair, '26122')
        self.evaluations.run('second', 'pair', **{**self.args, 'scenario': other})
        with closing(sqlite3.connect(self.db)) as db:
            db.execute('PRAGMA journal_mode=WAL')
        original = EvaluationRepository._check_scenario
        calls = []
        def check_then_import(db, scenario):
            original(db, scenario)
            calls.append(scenario['source_issue'])
            if len(calls) == 1:
                self.conflict()
        with patch.object(EvaluationRepository, '_check_scenario', side_effect=check_then_import):
            observed = self.evaluations.list_for_pair('pair')
        self.assertEqual([entry['status'] for entry in observed], ['research_draft', 'research_draft'])
        current = {entry['request_key']: entry['status'] for entry in self.evaluations.list_for_pair('pair')}
        self.assertEqual(current, {'first': 'review_required', 'second': 'research_draft'})

    def test_inconsistent_probability_or_pair_cost_requires_review(self):
        for field in ('probability', 'cost'):
            key = 'bad-' + field
            self.evaluations.run(key, 'pair', **self.args)
            with closing(sqlite3.connect(self.db)) as db, db:
                db.execute('DROP TRIGGER IF EXISTS draft_evaluations_no_update')
                result = json.loads(db.execute('SELECT result_json FROM draft_evaluations WHERE request_key=?', (key,)).fetchone()[0])
                if field == 'probability':
                    result['portfolios']['candidate']['any_prize']['estimate'] = 0.987654
                else:
                    result['portfolios']['candidate']['cost_yuan'] = '999'
                raw = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
                db.execute('UPDATE draft_evaluations SET result_json=?, result_sha256=? WHERE request_key=?',
                           (raw, hashlib.sha256(raw).hexdigest(), key))
            self.assertEqual(self.evaluations.inspect(key)['status'], 'review_required')

    def test_evaluation_route_respects_issue_and_legacy_default(self):
        def call(body, *, csrf='secret', origin='http://127.0.0.1:8766'):
            encoded = json.dumps(body).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.db,
                                             evaluation_lock=threading.Lock())
            handler.path = '/api/evaluations'
            handler.headers = {'Origin': origin, 'Sec-Fetch-Site': 'same-origin',
                               'X-DLT-CSRF': csrf, 'Content-Type': 'application/json',
                               'Content-Length': str(len(encoded))}
            handler.rfile = BytesIO(encoded)
            handler._validate_host = lambda: True
            handler._session = lambda: True
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        base = {'request_key': 'route', 'pair_key': 'pair', 'scenario_snapshot_id': self.snapshot,
                'samples': 1000, 'seed': 3}
        status, body = call({**base, 'scenario_issue': '26122'})
        self.assertEqual(status, 200)
        self.assertEqual(body['result']['scenario']['source_issue'], '26122')
        self.assertEqual(call({**base, 'scenario_issue': '26123'})[0], 400)
        status, legacy = call({**base, 'request_key': 'legacy'})
        self.assertEqual(status, 200)
        self.assertEqual(legacy['result']['scenario']['source_issue'], '26123')
        self.assertEqual(call({**base, 'request_key': 'bad-csrf'}, csrf='wrong')[0], 403)
        self.assertEqual(call({**base, 'request_key': 'bad-origin'}, origin='https://example.invalid')[0], 403)
        with self.assertRaises(KeyError):
            self.evaluations.get('bad-origin')

    def test_history_route_handles_empty_table_and_returns_only_requested_pair(self):
        def get(pair, *, session=True):
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(db_path=self.db)
            handler.path = '/api/evaluations?pair=' + pair
            handler._validate_host = lambda: True
            handler._session = lambda: session
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_GET()
        status, empty = get('pair')
        self.assertEqual((status, empty['evaluation'], empty['evaluations']), (200, None, []))
        self.evaluations.run('first', 'pair', **self.args)
        other = historical_scenario(self.db, self.snapshot, self.pair, '26122')
        self.evaluations.run('second', 'pair', **{**self.args, 'scenario': other})
        status, history = get('pair')
        self.assertEqual(status, 200)
        self.assertEqual([e['request_key'] for e in history['evaluations']], ['second', 'first'])
        self.assertEqual(history['evaluation']['result']['scenario']['source_issue'], '26122')
        self.assertEqual(get('another')[1]['evaluations'], [])
        self.assertEqual(get('pair', session=False)[0], 401)


if __name__ == '__main__':
    unittest.main()
