from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import hashlib
import sqlite3
import threading
import unittest

from dlt.integrity import check_database
from dlt.candidates import _canonical
from dlt.pair_repository import PairRepository
from dlt.prospective import RegistrationError
from dlt.prospective_series import SeriesRepository, validate_period
from dlt.prospective_settlement import PaperResults
import test_prospective as fixtures
import test_prospective_settlement as paper_fixtures


class SeriesTests(unittest.TestCase):
    review = fixtures.ProspectiveTests.review
    import_draw = paper_fixtures.PaperResultsTests.import_draw

    def setUp(self):
        fixtures.ProspectiveTests.setUp(self)
        rule = deepcopy(self.repo._get_review(self._db(), self.rule_id, 'rule')['extracted'])
        rule['applicable_issues'] = ['26017', '26018']
        self.rule_id = self.review('rule', rule)
        self.tax_id = self.review('tax', {'tax': rule['tax'], 'applicable_issues': ['26017', '26018']})
        self.protocol.update(rule_review_id=self.rule_id, tax_review_id=self.tax_id,
                             observation_issues=['26017', '26018'], retraining_policy='fixed_artifact_or_untrained_v1')
        self.manifest = {key: self.protocol[key] for key in ('hypothesis', 'primary_metric', 'observation_issues',
                         'aggregation', 'exclusion_rule', 'retraining_policy', 'selection_rule', 'time_assurance',
                         'other_winnings_assumption')}
        self.manifest.update(replica_ids=['a', 'b'], seeds={'26017': {'a': 1, 'b': 2}, '26018': {'a': 3, 'b': 4}},
                             template_pair_key='pair-1', first_calendar_review_id=self.cal_id)
        self.series = SeriesRepository(self.path, clock=lambda: self.now)

    def _db(self):
        db = sqlite3.connect(self.path)
        self.addCleanup(db.close)
        return db

    def prepare_second(self):
        self.now = self.deadline + timedelta(hours=1)
        cal = {**self.calendar, 'issue': '26018', 'draw_date': (self.deadline+timedelta(days=1)).date().isoformat(),
               'cutoff_at': (self.deadline+timedelta(days=1)).isoformat()}
        cal_id = self.review('calendar', cal)
        repo = PairRepository(self.path)
        for seed in (3, 4):
            repo.save_draft(f'next-{seed}', {**self.config, 'target_issue': '26018', 'target_date': cal['draw_date'],
                                            'seed': seed}, history_first_issue='26001', history_cutoff_issue='26016')
        return {**self.protocol, 'target_issue': '26018', 'calendar_review_id': cal_id,
                'internal_deadline': (self.deadline+timedelta(days=1, hours=-1)).isoformat(),
                'replicas': [{'replica': 'a', 'pair_key': 'next-3'}, {'replica': 'b', 'pair_key': 'next-4'}]}

    def test_two_periods_preserve_protocol_and_close_without_cancelling_existing(self):
        first = self.series.create('series', self.manifest)
        self.assertEqual(first, self.series.create('series', self.manifest))
        self.repo.freeze('period-1', self.protocol, series_key='series')
        second = self.prepare_second()
        self.repo.freeze('period-2', second, series_key='series')
        self.assertEqual({x['issue'] for x in self.series.get('series')['periods']}, {'26017', '26018'})
        self.series.close('series', 'end of observation')
        self.assertEqual(self.repo.freeze('period-2', second, series_key='series')['protocol'], second)
        with self.assertRaisesRegex(RegistrationError, 'closed'):
            self.repo.freeze('duplicate', second, series_key='series')
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)
        self.assertEqual(report['counts']['series_periods'], 2)

    def test_series_period_write_can_be_prepared_fenced_and_retried_by_original_key(self):
        self.series.create('series', self.manifest)
        original = self.repo.prepare_write('period-original', self.protocol, series_key='series')
        self.assertEqual(self.repo.write_status('period-original', original['request_sha256'])['status'], 'unknown')
        self.assertEqual(self.repo.fence_write('period-original', original['request_sha256'])['status'], 'fenced')
        with self.assertRaisesRegex(RegistrationError, 'fenced'):
            self.repo.freeze('period-original', self.protocol, series_key='series', require_intent=True)
        retry = self.repo.prepare_write('period-retry', self.protocol, series_key='series', retry_of='period-original')
        saved = self.repo.freeze('period-retry', self.protocol, series_key='series', require_intent=True)
        self.assertEqual(saved['protocol'], self.protocol)
        self.assertEqual(self.repo.write_status('period-retry', retry['request_sha256'])['status'], 'committed')
        self.assertEqual(self.series.get('series')['periods'], [{'issue': '26017', 'experiment_key': 'period-retry'}])
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_fixed_manifest_rejects_changed_parameters_seeds_and_duplicate(self):
        self.series.create('series', self.manifest)
        bad = {**self.protocol, 'replicas': list(reversed(self.protocol['replicas']))}
        with self.assertRaisesRegex(RegistrationError, 'replica manifest'):
            self.repo.freeze('bad-order', bad, series_key='series')
        self.repo.freeze('period-1', self.protocol, series_key='series')
        with self.assertRaisesRegex(RegistrationError, 'already has'):
            self.repo.freeze('other-key', self.protocol, series_key='series')
        second = self.prepare_second()
        pairs = PairRepository(self.path)
        pairs.save_draft('wrong-seed', {**self.config, 'target_issue': '26018',
                         'target_date': (self.deadline+timedelta(days=1)).date().isoformat(), 'seed': 5},
                         history_first_issue='26001', history_cutoff_issue='26016')
        wrong = {**second, 'replicas': [{'replica': 'a', 'pair_key': 'wrong-seed'}, second['replicas'][1]]}
        with self.assertRaisesRegex(RegistrationError, 'seed differs'):
            self.repo.freeze('bad-seed', wrong, series_key='series')
        with self.assertRaises(KeyError):
            self.repo.get('bad-seed')
        with self.assertRaisesRegex(RegistrationError, 'protocol differs'):
            self.repo.freeze('bad-hypothesis', {**second, 'hypothesis': 'changed'}, series_key='series')

    def test_generator_code_and_runtime_are_part_of_fixed_method(self):
        self.series.create('series', self.manifest)
        first = PairRepository(self.path).get('pair-1')
        second = PairRepository(self.path).get('pair-2')
        for field in ('code_sha256', 'runtime', 'version'):
            changed = deepcopy(first); changed[field] = 'changed'
            with self.subTest(field=field), closing(sqlite3.connect(self.path)) as db:
                with self.assertRaisesRegex(RegistrationError, 'configuration or seed'):
                    validate_period(db, 'series', self.protocol,
                                    [{'replica': 'a', 'pair': changed}, {'replica': 'b', 'pair': second}])

    def test_concurrent_close_and_registration_have_one_consistent_order(self):
        self.series.create('series', self.manifest)
        barrier = threading.Barrier(2)
        def register():
            barrier.wait()
            try:
                self.repo.freeze('period-1', self.protocol, series_key='series')
                return 'registered'
            except RegistrationError as exc:
                self.assertIn('closed', str(exc))
                return 'closed'
        def close():
            barrier.wait()
            return self.series.close('series', 'stop')
        with ThreadPoolExecutor(max_workers=2) as pool:
            a, b = pool.submit(register), pool.submit(close)
            result = a.result(timeout=15); b.result(timeout=15)
        info = self.series.get('series')
        self.assertIsNotNone(info['closure'])
        self.assertEqual(len(info['periods']), 1 if result == 'registered' else 0)
        if result == 'closed':
            with self.assertRaises(KeyError):
                self.repo.get('period-1')

    def test_creation_rejects_observed_issue_and_invalid_seed_schedule(self):
        bad = deepcopy(self.manifest); bad['seeds']['26018']['b'] = 3
        with self.assertRaisesRegex(RegistrationError, 'distinct'):
            self.series.create('bad', bad)
        self.now = self.deadline + timedelta(hours=2)
        self.paper = PaperResults(self.path, clock=lambda: self.now)
        ticket = PairRepository(self.path).get('pair-1')['candidate']['tickets'][0]
        self.draw = {'lotteryGameNum': '85', 'lotteryDrawNum': '26017', 'lotteryDrawTime': self.calendar['draw_date'],
                     'lotteryDrawResult': ' '.join(map(str, ticket['front']+ticket['back'])),
                     'lotteryPromotionFlag': 0, 'verify': 1, 'prizeLevelList': []}
        self.import_draw(self.draw)
        with self.assertRaisesRegex(RegistrationError, 'already observed'):
            self.series.create('late', self.manifest)

    def test_new_schedule_rejects_unusable_seeds_before_fixing_protocol(self):
        for bad_seed in (2**32, 2**64-1):
            manifest = deepcopy(self.manifest)
            manifest['seeds']['26018']['b'] = bad_seed
            with self.subTest(seed=bad_seed), self.assertRaisesRegex(RegistrationError, '4294967295'):
                self.series.create('invalid-seed', manifest)
            with self.assertRaises(KeyError):
                self.series.get('invalid-seed')
        valid = deepcopy(self.manifest)
        valid['seeds']['26018']['b'] = 2**32-1
        self.series.create('maximum-valid', valid)
        self.assertEqual(self.series.get('maximum-valid')['unsupported_seed_issues'], [])

    def test_legacy_oversized_seed_is_readable_and_idempotent(self):
        content = self.series.create('legacy', self.manifest)
        original = deepcopy(content)
        original['manifest']['seeds']['26018']['b'] = 2**64-1
        raw = _canonical(original)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER experiment_series_no_update')
            db.execute('UPDATE experiment_series SET content_sha256=?,content_json=? WHERE series_key=?',
                       (hashlib.sha256(raw).hexdigest(), raw, 'legacy'))
            db.execute("CREATE TRIGGER experiment_series_no_update BEFORE UPDATE ON experiment_series BEGIN SELECT RAISE(ABORT, 'immutable experiment series'); END")
        info = self.series.get('legacy')
        self.assertEqual(info['unsupported_seed_issues'], ['26018'])
        self.assertEqual(info['content'], original)
        self.assertEqual(self.series.create('legacy', original['manifest']), original)
        with self.assertRaisesRegex(RegistrationError, 'different protocol'):
            self.series.create('legacy', self.manifest)
        self.assertEqual(self.series.get('legacy')['content'], original)

    def test_largest_valid_seed_generates_and_registers_future_pair(self):
        manifest = deepcopy(self.manifest)
        manifest['seeds']['26018']['b'] = 2**32-1
        self.series.create('maximum-seed', manifest)
        self.repo.freeze('period-1', self.protocol, series_key='maximum-seed')
        second = self.prepare_second()
        PairRepository(self.path).save_draft('maximum-pair', {**self.config, 'target_issue': '26018',
             'target_date': (self.deadline+timedelta(days=1)).date().isoformat(), 'seed': 2**32-1},
             history_first_issue='26001', history_cutoff_issue='26016')
        second['replicas'][1]['pair_key'] = 'maximum-pair'
        result = self.repo.freeze('period-2', second, series_key='maximum-seed')
        self.assertEqual(result['pairs'][1]['pair']['config']['seed'], 2**32-1)

    def test_first_deadline_is_rechecked_before_series_commit(self):
        times = iter([self.now, self.deadline])
        changing = SeriesRepository(self.path, clock=lambda: next(times))
        with self.assertRaisesRegex(RegistrationError, 'deadline reached'):
            changing.create('late-commit', self.manifest)
        with self.assertRaises(KeyError):
            self.series.get('late-commit')

    def test_closed_series_still_settles_its_already_fixed_period(self):
        self.series.create('series', self.manifest)
        self.repo.freeze('period-1', self.protocol, series_key='series')
        self.series.close('series', 'no further registrations')
        self.now = self.deadline + timedelta(hours=2)
        self.paper = PaperResults(self.path, clock=lambda: self.now)
        ticket = PairRepository(self.path).get('pair-1')['candidate']['tickets'][0]
        self.draw = {'lotteryGameNum': '85', 'lotteryDrawNum': '26017', 'lotteryDrawTime': self.calendar['draw_date'],
                     'lotteryDrawResult': ' '.join(map(str, ticket['front']+ticket['back'])),
                     'lotteryPromotionFlag': 0, 'verify': 1,
                     'prizeLevelList': [{'sort': 101, 'stakeAmount': '6000'}, {'sort': 201, 'stakeAmount': '100'},
                                        {'sort': 301, 'stakeAmount': '5000'}, {'sort': 401, 'stakeAmount': '200'}]}
        snapshot = self.import_draw(self.draw)
        results = self.paper.process_snapshot(snapshot)
        self.assertEqual(results['period-1']['status'], 'complete')
        self.assertEqual(self.paper.inspect_current('period-1')['status'], 'locally_attested')
        self.assertEqual(self.series.get('series')['closure']['reason'], 'no further registrations')
        previous = results['period-1']['group_id']
        rule = deepcopy(self.repo.get('period-1')['rule_review']['extracted'])
        rule['version'] = 'corrected-v2'
        rule['payout_sort_map'] = {'first': '301', 'first-extra': '401', 'second': '101', 'second-extra': '201'}
        replacement = self.review('rule', rule)
        correction = self.repo.declare_source_correction(self.rule_id, replacement, note='synthetic fix', confirmed=True)
        self.assertEqual(self.paper.inspect_current('period-1')['status'], 'review_required')
        outcome = self.paper.process_source_correction(correction['correction_id'])
        self.assertEqual(outcome['details']['period-1']['status'], 'complete')
        self.assertNotEqual(previous, self.paper.inspect_current('period-1')['group_id'])
        self.assertEqual(len(self.paper.list_history('period-1')['publications']), 2)
        self.assertIsNotNone(self.series.get('series')['closure'])


if __name__ == '__main__':
    unittest.main()
