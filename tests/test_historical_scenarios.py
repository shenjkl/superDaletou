from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from dlt.backtest import run_from_snapshot
from dlt.historical_scenarios import backtest_scenarios
from dlt.snapshots import SnapshotStore
from test_backtest import protocol


class HistoricalScenarioTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'history.sqlite3'
        raw = (Path(__file__).resolve().parents[1] / 'research/data/official_page1.json').read_bytes()
        self.source = json.loads(raw)
        self.snapshot = SnapshotStore(self.path).import_official(
            raw, 'local research file', '2026-10-29T12:00:00+08:00')['snapshot_id']

    def test_actual_issue_prizes_and_missing_additional_prize(self):
        issues = ['26121', '26122', '26123']
        standard = backtest_scenarios(self.path, self.snapshot, issues)
        self.assertEqual(standard['26123']['payouts']['101'], '5,750,462')
        self.assertEqual(standard['26122']['payouts']['101'], '7,995,227')
        self.assertNotIn('blocked_reason', standard['26121'])
        extra = backtest_scenarios(self.path, self.snapshot, issues, additional=True)
        self.assertIn('奖金缺失', extra['26121']['blocked_reason'])
        self.assertNotIn('blocked_reason', extra['26122'])
        self.assertIn('未经独立核验', standard['26123']['assumptions'])

    def test_unsupported_and_conflicting_issues_remain_blocked(self):
        result = backtest_scenarios(self.path, self.snapshot, ['26050', '26124'])
        self.assertIn('派奖', result['26050']['blocked_reason'])
        self.assertIn('范围', result['26124']['blocked_reason'])
        changed = deepcopy(self.source)
        changed['value']['list'][0]['prizeLevelList'][0]['stakeAmount'] = '1'
        SnapshotStore(self.path).import_official(json.dumps(changed).encode(), 'different source',
                                                '2026-10-30T12:00:00+08:00')
        result = backtest_scenarios(self.path, self.snapshot, ['26123'])
        self.assertIn('存在差异', result['26123']['blocked_reason'])

    def test_engine_keeps_blocked_issue_and_does_not_fill_zero(self):
        issues = ['26121', '26122', '26123']
        cfg = protocol(self.snapshot)
        cfg.update(targets=issues, replicas=['a'], history_periods=12)
        cfg['candidate']['additional'] = True
        cfg['candidate']['budget_fen'] = 600
        result = run_from_snapshot(self.path, cfg, backtest_scenarios(self.path, self.snapshot, issues, additional=True))
        self.assertEqual((result['planned_periods'], result['complete_periods']), (3, 2))
        self.assertEqual(result['incomplete_issues'], ['26121'])
        self.assertIsNone(result['periods'][0]['mean_difference_fraction'])
        self.assertIn('奖金缺失', result['periods'][0]['replicas'][0]['reason'])


if __name__ == '__main__':
    unittest.main()
