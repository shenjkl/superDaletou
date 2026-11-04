from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from datetime import timedelta
from fractions import Fraction
import hashlib
import sqlite3
import threading
import unittest
from unittest.mock import patch

from dlt.candidates import _canonical
from dlt.pair_repository import PairRepository
from dlt.prospective_settlement import PaperResults
import dlt.prospective_series as module
import test_prospective_series as fixtures


class SeriesReportTests(unittest.TestCase):
    setUp = fixtures.SeriesTests.setUp
    _db = fixtures.SeriesTests._db
    review = fixtures.SeriesTests.review
    prepare_second = fixtures.SeriesTests.prepare_second
    import_draw = fixtures.SeriesTests.import_draw

    def start(self):
        self.series.create('series', self.manifest)
        self.repo.freeze('period-1', self.protocol, series_key='series')
        self.paper = PaperResults(self.path, clock=lambda: self.now)

    def settle(self, key, issue, date, pair_key):
        ticket = PairRepository(self.path).get(pair_key)['candidate']['tickets'][0]
        draw = {'lotteryGameNum': '85', 'lotteryDrawNum': issue, 'lotteryDrawTime': date,
                'lotteryDrawResult': ' '.join(map(str, ticket['front']+ticket['back'])),
                'lotteryPromotionFlag': 0, 'verify': 1,
                'prizeLevelList': [{'sort': 101, 'stakeAmount': '6000'}, {'sort': 201, 'stakeAmount': '100'},
                                   {'sort': 301, 'stakeAmount': '5000'}, {'sort': 401, 'stakeAmount': '200'}]}
        sid = self.import_draw(draw)
        result = self.paper.process_one(key, sid)
        self.assertEqual(result['status'], 'complete', result)
        return sid, result

    def test_empty_partial_complete_mean_counts_only_independent_issues(self):
        self.start()
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        empty = self.series.describe('series')
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        self.assertEqual((empty['planned'], empty['valid'], empty['missing']), (2, 0, 2))
        self.assertIsNone(empty['mean_difference_fraction'])
        self.assertEqual(empty['outcomes'], {'candidate': None, 'control': None})
        self.assertEqual([x['status'] for x in empty['periods']], ['waiting', 'unregistered'])
        second = self.prepare_second()
        self.repo.freeze('period-2', second, series_key='series')
        _, first = self.settle('period-1', '26017', self.calendar['draw_date'], 'pair-1')
        partial = self.series.describe('series')
        self.assertEqual((partial['completeness'], partial['valid'], partial['missing']), ('partial', 1, 1))
        self.assertEqual(partial['mean_difference_fraction'], first['result']['mean_difference_fraction'])
        self.now = self.deadline + timedelta(days=1, hours=2)
        _, other = self.settle('period-2', '26018', (self.deadline+timedelta(days=1)).date().isoformat(), 'next-3')
        report = self.series.describe('series')
        expected = (Fraction(*first['result']['mean_difference_fraction']) + Fraction(*other['result']['mean_difference_fraction']))/2
        self.assertEqual(report['mean_difference_fraction'], [expected.numerator, expected.denominator])
        self.assertEqual((report['completeness'], report['valid'], report['replicas_per_issue']), ('complete', 2, 2))
        self.assertEqual(report['outcomes']['candidate']['cost_total_fraction'], [8, 1])
        expected_net = sum((Fraction(row['candidate']['net']) for item in (first, other)
                            for row in item['result']['replicas']), Fraction(0))/2
        self.assertEqual(report['outcomes']['candidate']['net_total_fraction'], [expected_net.numerator, expected_net.denominator])

    def test_auxiliary_metrics_average_alternative_replicas_before_periods(self):
        self.start(); self.now = self.deadline+timedelta(hours=2)
        _, saved = self.settle('period-1', '26017', self.calendar['draw_date'], 'pair-1')
        series, fixed = self.series.get('series')['content'], self.repo.get('period-1')
        result = deepcopy(saved['result'])
        # Synthetic arithmetic example: candidate nets 14/-4, control nets -4/5.
        values = [
            ({'gross':'20','tax':'2','cost':'4','net':'14','winning_tickets':1},
             {'gross':'0','tax':'0','cost':'4','net':'-4','winning_tickets':0}),
            ({'gross':'0','tax':'0','cost':'4','net':'-4','winning_tickets':0},
             {'gross':'10','tax':'1','cost':'4','net':'5','winning_tickets':1}),
        ]
        for row, (candidate, control) in zip(result['replicas'], values):
            row['candidate'].update(candidate); row['control'].update(control)
            row['difference_yuan'] = str(int(candidate['net'])-int(control['net']))
        result['mean_difference_fraction'] = [9, 2]
        first = module._period_difference(series, fixed, result)
        for row in result['replicas']:
            for label in ('candidate','control'):
                row[label].update(gross='0', tax='0', cost='4', net='-4', winning_tickets=0)
            row['difference_yuan'] = '0'
        result['mean_difference_fraction'] = [0, 1]
        second = module._period_difference(series, fixed, result)
        candidate = module._side_summary([('26017', first), ('26018', second)], 'candidate')
        control = module._side_summary([('26017', first), ('26018', second)], 'control')
        self.assertEqual(candidate['cost_total_fraction'], [8, 1])
        self.assertEqual(candidate['net_total_fraction'], [1, 1])
        self.assertEqual(candidate['roi_fraction'], [1, 8])
        self.assertEqual(candidate['win_fraction'], [1, 4])
        self.assertEqual(candidate['profit_fraction'], [1, 4])
        self.assertEqual((candidate['net_p25_fraction'], candidate['net_p50_fraction'], candidate['net_p75_fraction']),
                         ([-4, 1], [-4, 1], [5, 1]))
        self.assertEqual((candidate['largest_gross_issue'], candidate['largest_gross_share_fraction']), ('26017', [1, 1]))
        self.assertEqual(control['net_total_fraction'], [-7, 2])
        self.assertEqual(control['roi_fraction'], [-7, 16])

    def test_zero_cost_and_no_prizes_have_no_roi_or_prize_concentration(self):
        side = {key: Fraction(0) for key in ('gross','tax','cost','net','won','profit')}
        summary = module._side_summary([('26017', {'sides': {'candidate': side}})], 'candidate')
        self.assertIsNone(summary['roi_fraction'])
        self.assertIsNone(summary['largest_gross_issue'])
        self.assertIsNone(summary['largest_gross_share_fraction'])
        with self.assertRaises(ValueError):
            module._money('1e-1000000')

    def test_missing_replica_blocks_whole_period_even_with_valid_result_hash(self):
        self.start(); self.now = self.deadline+timedelta(hours=2)
        sid, old = self.settle('period-1', '26017', self.calendar['draw_date'], 'pair-1')
        incomplete = deepcopy(old['result']); incomplete['replicas'].pop()
        payload = _canonical(incomplete); digest = hashlib.sha256(payload).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('INSERT INTO paper_groups VALUES (?,?,?,?,?)', (digest, 'period-1', '26017', incomplete['row_sha256'], payload))
            db.execute('UPDATE paper_current SET group_id=? WHERE experiment_key=?', (digest, 'period-1'))
            db.execute('INSERT INTO paper_publications(experiment_key,previous_group,group_id,snapshot_id,published_at) '
                       'VALUES (?,?,?,?,?)', ('period-1', old['group_id'], digest, sid, self.now.isoformat()))
        report = self.series.describe('series')
        self.assertEqual(report['valid'], 0)
        self.assertIsNone(report['mean_difference_fraction'])
        self.assertEqual(report['periods'][0]['status'], 'review_required')

    def test_rule_correction_invalidates_old_group_before_recalculation(self):
        self.start(); self.now = self.deadline+timedelta(hours=2)
        self.settle('period-1', '26017', self.calendar['draw_date'], 'pair-1')
        self.assertEqual(self.series.describe('series')['valid'], 1)
        tax = deepcopy(self.repo.get('period-1')['tax_review']['extracted']); tax['tax']['threshold'] = '1'
        next_id = self.review('tax', tax)
        self.repo.declare_source_correction(self.tax_id, next_id, note='synthetic change', confirmed=True)
        report = self.series.describe('series')
        self.assertEqual((report['valid'], report['counts']['review_required']), (0, 1))
        self.assertIsNone(report['mean_difference_fraction'])
        self.assertEqual(report['outcomes'], {'candidate': None, 'control': None})

    def test_unregistered_issue_after_draw_is_marked_missed(self):
        self.start(); self.now = self.deadline+timedelta(days=1, hours=2)
        ticket = PairRepository(self.path).get('pair-1')['candidate']['tickets'][0]
        draw = {'lotteryGameNum': '85', 'lotteryDrawNum': '26018',
                'lotteryDrawTime': (self.deadline+timedelta(days=1)).date().isoformat(),
                'lotteryDrawResult': ' '.join(map(str, ticket['front']+ticket['back'])),
                'lotteryPromotionFlag': 0, 'verify': 1, 'prizeLevelList': []}
        self.import_draw(draw)
        report = self.series.describe('series')
        self.assertEqual(report['periods'][1]['status'], 'missed_registration')
        self.assertEqual(report['counts']['missed_registration'], 1)

    def test_report_uses_one_snapshot_when_correction_commits_concurrently(self):
        self.start(); self.now = self.deadline+timedelta(hours=2)
        self.settle('period-1', '26017', self.calendar['draw_date'], 'pair-1')
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('PRAGMA journal_mode=WAL')
        tax = deepcopy(self.repo.get('period-1')['tax_review']['extracted']); tax['tax']['threshold'] = '1'
        next_id = self.review('tax', tax)
        entered, release = threading.Event(), threading.Event()
        original = module._period_difference
        def delayed(*args):
            entered.set()
            if not release.wait(10): raise RuntimeError('test timeout')
            return original(*args)
        with patch.object(module, '_period_difference', side_effect=delayed), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.series.describe, 'series')
            self.assertTrue(entered.wait(10))
            self.repo.declare_source_correction(self.tax_id, next_id, note='synthetic change', confirmed=True)
            release.set()
            self.assertEqual(future.result(timeout=10)['valid'], 1)
        self.assertEqual(self.series.describe('series')['valid'], 0)


if __name__ == '__main__':
    unittest.main()
