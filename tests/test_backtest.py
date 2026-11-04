from copy import deepcopy
from fractions import Fraction
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from dlt.backtest import BacktestError, run_from_snapshot
from dlt.candidates import CandidateError
from dlt.models import ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION
from dlt.prize_rules import (DLT_2026_ADDITIONAL_GROUPS, DLT_2026_HIT_GROUPS,
                             DLT_2026_OFFICIAL_SORT_CODES)
from dlt.snapshots import SnapshotStore
from test_candidates import config, input_rows
from test_settlement import fixture


def source_data():
    prize_levels = [{'sort': code, 'stakeAmount': '6000', 'awardType': 0}
                    for code in sorted(DLT_2026_OFFICIAL_SORT_CODES)]
    return {'success': True, 'errorCode': '0', 'value': {'list': [
        {'lotteryGameNum': '85', 'lotteryDrawNum': row['issue'], 'lotteryDrawTime': row['date'],
         'lotteryDrawResult': ' '.join(map(str, row['front']+row['back'])),
         'lotteryPromotionFlag': 0, 'prizeLevelList': prize_levels}
        for row in input_rows(20)]}}


def scenarios(issues):
    result = {}
    for issue in issues:
        source = fixture()
        rule = source['rule']
        rule.update(version='synthetic-dlt-2026-v1', applicable_issues=[issue],
                    payout_sort_map={code: code for code in sorted(DLT_2026_OFFICIAL_SORT_CODES)},
                    hit_groups={outcome: (str(rank) if rank is not None else None)
                                for outcome, rank in DLT_2026_HIT_GROUPS.items()},
                    additional_groups={str(base): str(extra)
                                       for base, extra in DLT_2026_ADDITIONAL_GROUPS.items()})
        payouts = {group: '6000' for group in set(rule['hit_groups'].values()) if group is not None}
        result[issue] = {'source_ref': 'synthetic fixture', 'promotion': False,
                         'rule': rule, 'payouts': payouts}
    return result


def protocol(snapshot, method='hot'):
    candidate = {**config(method), 'budget_fen': 400, 'base_price_fen': 200,
                 'additional_price_fen': 300, 'additional': False, 'max_tickets': 100}
    return {'mode': 'historical_reconstruction', 'analysis': 'descriptive', 'aggregation': 'arithmetic_mean_v1',
            'selection_role': 'exploration', 'snapshot_id': snapshot, 'targets': ['26018', '26019', '26020'],
            'replicas': ['a', 'b', 'c'], 'seed': 42, 'history_periods': 12,
            'limits': {'max_targets': 10, 'max_replicas': 10, 'max_history': 20}, 'candidate': candidate,
            'model': {'window': 3, 'validation_periods': 2,
                      'algorithm_version': ALGORITHM_VERSION.get(method),
                      'feature_schema_version': FEATURE_SCHEMA_VERSION,
                      'preprocessing_version': PREPROCESSING_VERSION.get(method),
                      'parameters': {'max_depth': 2, 'min_leaf': 2} if method == 'tree' else
                                    {'hidden_size': 3, 'epochs': 1, 'learning_rate': 0.1}}}


class BacktestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'db.sqlite3'
        self.source = source_data()
        self.snapshot = SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'fixture',
                                                               '2026-02-01T12:00:00+08:00')['snapshot_id']

    def test_rolling_all_methods_and_per_period_aggregation(self):
        for method in ('random', 'hot', 'cold', 'tree', 'lstm'):
            with self.subTest(method=method):
                cfg = protocol(self.snapshot, method)
                result = run_from_snapshot(self.path, cfg, scenarios(cfg['targets']))
                self.assertEqual(result, run_from_snapshot(self.path, cfg, scenarios(cfg['targets'])))
                self.assertEqual((result['planned_periods'], result['complete_periods']), (3, 3))
                for period in result['periods']:
                    self.assertEqual(len(period['replicas']), 3)
                    expected = sum(Fraction(item['difference_yuan']) for item in period['replicas']) / 3
                    self.assertEqual(Fraction(*period['mean_difference_fraction']), expected)
                    for item in period['replicas']:
                        self.assertEqual(item['pair']['history_last'],
                                         f"26{int(period['target']['issue'])-26001:03}")
                        if method in ('tree', 'lstm'):
                            self.assertNotIn(period['target']['issue'], item['artifact']['train_targets'])
                            self.assertNotIn(period['target']['issue'], item['artifact']['validation_targets'])
                self.assertIn('not verified', result['evidence'])

    def test_target_and_future_outcomes_cannot_change_candidate_or_fit(self):
        cfg = protocol(self.snapshot, 'tree')
        original = run_from_snapshot(self.path, cfg, scenarios(cfg['targets']))['periods'][0]['replicas']
        changed = deepcopy(self.source)
        for row in changed['value']['list'][-3:]:
            row['lotteryDrawResult'] = '10 11 12 13 14 03 04'
        other = Path(self.tmp.name)/'other.sqlite3'
        sid = SnapshotStore(other).import_official(json.dumps(changed).encode(), 'fixture',
                                                  '2026-02-01T12:00:00+08:00')['snapshot_id']
        updated = run_from_snapshot(other, protocol(sid, 'tree'), scenarios(cfg['targets']))['periods'][0]['replicas']
        for old, new in zip(original, updated):
            self.assertEqual(old['pair']['candidate']['tickets'], new['pair']['candidate']['tickets'])
            self.assertEqual(old['pair']['control']['tickets'], new['pair']['control']['tickets'])
            self.assertEqual(old['artifact']['model'], new['artifact']['model'])

    def test_missing_scenario_or_replica_failure_is_not_silently_dropped(self):
        cfg = protocol(self.snapshot)
        sc = scenarios(cfg['targets']); del sc['26019']
        result = run_from_snapshot(self.path, cfg, sc)
        self.assertEqual(result['complete_periods'], 2)
        self.assertEqual(result['incomplete_issues'], ['26019'])
        self.assertIsNone(result['periods'][1]['mean_difference_fraction'])
        self.assertTrue(all(r['pair'] is not None for r in result['periods'][1]['replicas']))
        import dlt.backtest as module
        original = module._generate
        def fail_one(history, target, p, replica):
            if target['issue'] == '26018' and replica == 'b':
                raise CandidateError('injected generation failure')
            return original(history, target, p, replica)
        with patch('dlt.backtest._generate', side_effect=fail_one):
            result = run_from_snapshot(self.path, cfg, scenarios(cfg['targets']))
        self.assertEqual(result['periods'][0]['status'], 'incomplete')
        self.assertIsNone(result['periods'][0]['mean_difference_fraction'])
        self.assertEqual(result['periods'][0]['replicas'][1]['reason'], 'injected generation failure')
        self.assertEqual(result['completeness'], 'partial')

    def test_bad_protocol_and_scenario_cost_are_blocked(self):
        cfg = protocol(self.snapshot)
        with self.assertRaisesRegex(BacktestError, 'aggregation'):
            run_from_snapshot(self.path, {**cfg, 'aggregation': 'unknown'}, {})
        with self.assertRaisesRegex(BacktestError, 'chronology'):
            run_from_snapshot(self.path, {**cfg, 'targets': list(reversed(cfg['targets']))}, {})
        sc = scenarios(cfg['targets']); sc['26018']['rule']['prices']['base'] = '3'
        result = run_from_snapshot(self.path, cfg, sc)
        self.assertTrue(all('costs conflict' in r['reason'] for r in result['periods'][0]['replicas']))
        self.assertIsNone(result['periods'][0]['mean_difference_fraction'])

    def test_offline_engine_rejects_unverified_holdout_label(self):
        cfg = {**protocol(self.snapshot), 'selection_role': 'holdout'}
        with patch('dlt.backtest.SnapshotStore.get_draws') as read:
            with self.assertRaisesRegex(BacktestError, 'holdout independence'):
                run_from_snapshot(self.path, cfg, scenarios(cfg['targets']))
            read.assert_not_called()

    def test_existing_historical_snapshot_rejects_synthetic_payouts(self):
        path = Path(self.tmp.name)/'historical.sqlite3'
        raw = (Path(__file__).resolve().parents[1]/'research/data/official_page1.json').read_bytes()
        sid = SnapshotStore(path).import_official(raw, 'existing local snapshot',
                                                  '2026-10-29T12:00:00+08:00')['snapshot_id']
        rows = SnapshotStore(path).get_draws(sid)
        targets = [row['issue'] for row in rows[-3:]]
        cfg = {**protocol(sid, 'tree'), 'targets': targets, 'replicas': ['a', 'b']}
        result = run_from_snapshot(path, cfg, scenarios(targets))
        self.assertEqual((result['planned_periods'], result['complete_periods']), (3, 0))
        self.assertIn('payout scenario differs from retained draw fields',
                      result['periods'][0]['replicas'][0]['reason'])
        self.assertEqual(result['evidence'], 'descriptive reconstruction only; source and holdout independence not verified')


if __name__ == '__main__':
    unittest.main()
