from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import json
import threading
import unittest
from unittest.mock import patch

from dlt.integrity import check_database
from dlt.pair_repository import PairRepository
from dlt.snapshots import SnapshotStore
from dlt.static_registration import StaticRegistration, StaticRegistrationError
import test_prospective_series as fixtures
import test_rolling_policy as model_fixtures

STATIC_SERIES_KEY = '11111111-1111-4111-8111-111111111111'
STATIC_MODEL_SERIES_KEY = '22222222-2222-4222-8222-222222222222'
STATIC_LSTM_SERIES_KEY = '33333333-3333-4333-8333-333333333333'


class StaticRegistrationTests(unittest.TestCase):
    setUp = fixtures.SeriesTests.setUp
    review = fixtures.SeriesTests.review
    import_draw = fixtures.SeriesTests.import_draw
    _db = fixtures.SeriesTests._db
    prepare_second = fixtures.SeriesTests.prepare_second

    def create_plan(self, **changes):
        self.series.create(STATIC_SERIES_KEY, self.manifest)
        self.service = StaticRegistration(self.path, clock=lambda: self.now)
        args = dict(series_key=STATIC_SERIES_KEY, issue='26017', calendar_review_id=self.cal_id,
                    rule_review_id=self.rule_id, tax_review_id=self.tax_id, expected_cutoff_issue='26016',
                    history_periods=6, start_at=(self.now-timedelta(minutes=1)).isoformat(),
                    internal_deadline=(self.deadline-timedelta(hours=1)).isoformat(), confirmed=True)
        self.args = args
        return self.service.create(**{**args, **changes})

    def test_untrained_method_generates_precommitted_pairs_and_registers(self):
        plan = self.create_plan()
        self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'complete')
        fixed = self.repo.get('static-exp-'+plan['id'])
        self.assertEqual([item['pair']['config']['seed'] for item in fixed['pairs']], [1, 2])
        self.assertEqual([item['pair']['history_first'] for item in fixed['pairs']], ['26011', '26011'])
        self.assertEqual(self.service.list()[0]['resolved']['cutoff_issue'], '26016')
        self.service.tick()
        self.assertEqual(len(self.service.list()[0]['events']), 2)
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_later_conflicting_source_does_not_falsify_historical_integrity(self):
        plan = self.create_plan(); self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'complete')
        source = deepcopy(self.source)
        source['value']['list'][-1]['lotteryDrawResult'] = '01 02 03 04 35 01 02'
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'later conflict', self.now.isoformat())
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)
        self.assertEqual(self.repo.inspect('static-exp-'+plan['id'])['status'], 'review_required')

    def test_waits_for_start_and_expected_cutoff(self):
        plan = self.create_plan(start_at=(self.now+timedelta(minutes=1)).isoformat())
        self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'waiting')
        self.assertIsNone(self.service.list()[0]['resolved'])
        self.now += timedelta(minutes=2)
        self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'complete')
        self.assertEqual(self.repo.get('static-exp-'+plan['id'])['protocol']['target_issue'], '26017')

    def test_next_issue_uses_newly_observed_snapshot_and_fixed_seeds(self):
        self.create_plan(); self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'complete')
        second = self.prepare_second()
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '26017',
            'lotteryDrawTime': self.calendar['draw_date'], 'lotteryDrawResult': '01 02 03 04 15 01 02', 'prizeLevelList': []})
        snapshot = SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'next draw fixture',
                            (self.now-timedelta(seconds=5)).isoformat())['snapshot_id']
        plan = self.service.create(**{**self.args, 'issue': '26018', 'calendar_review_id': second['calendar_review_id'],
                            'expected_cutoff_issue': '26017', 'start_at': (self.now-timedelta(minutes=1)).isoformat(),
                            'internal_deadline': second['internal_deadline']})
        with patch('dlt.pair_repository.datetime') as clock:
            clock.now.return_value = self.now-timedelta(seconds=1)
            self.service.tick()
        current = next(p for p in self.service.list() if p['id'] == plan['id'])
        self.assertEqual(current['status'], 'complete', current['message'])
        self.assertEqual(current['resolved']['snapshot_id'], snapshot)
        result = self.repo.get('static-exp-'+plan['id'])
        self.assertEqual([p['pair']['config']['seed'] for p in result['pairs']], [3, 4])
        self.assertEqual([p['pair']['history_last'] for p in result['pairs']], ['26017', '26017'])

    def test_newer_local_history_blocks_stale_expected_cutoff(self):
        plan = self.create_plan()
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '25999',
             'lotteryDrawTime': (self.deadline-timedelta(days=1)).date().isoformat(),
             'lotteryDrawResult': '01 02 03 04 35 01 02', 'prizeLevelList': []})
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'newer fixture', self.now.isoformat())
        self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'blocked')
        with self.assertRaises(KeyError):
            self.repo.get('static-exp-'+plan['id'])

    def test_conflicting_history_is_blocked(self):
        self.create_plan()
        source = deepcopy(self.source)
        source['value']['list'][-1]['lotteryDrawResult'] = '01 02 03 04 35 01 02'
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'conflicting fixture', self.now.isoformat())
        self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'blocked')
        self.assertIn('冲突', self.service.list()[0]['message'])

    def test_cancel_after_last_pair_prevents_publication(self):
        plan = self.create_plan(); original = self.service.pairs.save_draft; count = 0
        def cancel_after_second(*args, **kwargs):
            nonlocal count
            result = original(*args, **kwargs); count += 1
            if count == 2:
                self.service.cancel(plan['id'], request_key='44444444-4444-4444-8444-444444444444')
            return result
        with patch.object(self.service.pairs, 'save_draft', side_effect=cancel_after_second):
            self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'cancelled', self.service.list()[0]['message'])
        with self.assertRaises(KeyError):
            self.repo.get('static-exp-'+plan['id'])

    def test_new_history_during_generation_invalidates_fixed_window(self):
        plan = self.create_plan(); original = self.service.pairs.save_draft; count = 0
        def change_after_second(*args, **kwargs):
            nonlocal count
            result = original(*args, **kwargs); count += 1
            if count == 2:
                source = deepcopy(self.source)
                source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '25999',
                    'lotteryDrawTime': (self.deadline-timedelta(days=1)).date().isoformat(),
                    'lotteryDrawResult': '01 02 03 04 35 01 02', 'prizeLevelList': []})
                SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'newer fixture', self.now.isoformat())
            return result
        with patch.object(self.service.pairs, 'save_draft', side_effect=change_after_second):
            self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'blocked')
        with self.assertRaises(KeyError):
            self.repo.get('static-exp-'+plan['id'])

    def test_target_result_appearing_after_last_pair_blocks_publication(self):
        plan = self.create_plan(); original = self.service.pairs.save_draft; count = 0
        def observe_after_second(*args, **kwargs):
            nonlocal count
            result = original(*args, **kwargs); count += 1
            if count == 2:
                source = deepcopy(self.source)
                source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '26017',
                    'lotteryDrawTime': self.calendar['draw_date'], 'lotteryDrawResult': '01 02 03 04 15 01 02',
                    'prizeLevelList': []})
                SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'observed target', self.now.isoformat())
            return result
        with patch.object(self.service.pairs, 'save_draft', side_effect=observe_after_second):
            self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'blocked')
        with self.assertRaises(KeyError):
            self.repo.get('static-exp-'+plan['id'])

    def test_deadline_reached_after_last_pair_blocks_publication(self):
        plan = self.create_plan(); original = self.service.pairs.save_draft; count = 0
        def expire_after_second(*args, **kwargs):
            nonlocal count
            result = original(*args, **kwargs); count += 1
            if count == 2:
                self.now = self.deadline-timedelta(hours=1)
            return result
        with patch.object(self.service.pairs, 'save_draft', side_effect=expire_after_second):
            self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'blocked')
        with self.assertRaises(KeyError):
            self.repo.get('static-exp-'+plan['id'])

    def test_rule_corrected_after_last_pair_blocks_publication(self):
        plan = self.create_plan(); original = self.service.pairs.save_draft; count = 0
        def correct_after_second(*args, **kwargs):
            nonlocal count
            result = original(*args, **kwargs); count += 1
            if count == 2:
                old = self.repo._get_review(self._db(), self.rule_id, 'rule')['extracted']
                replacement = self.review('rule', {**old, 'version': 'corrected-static'})
                self.repo.declare_scoped_source_correction(self.rule_id, replacement, issue='26017', note='fixture', confirmed=True)
            return result
        with patch.object(self.service.pairs, 'save_draft', side_effect=correct_after_second):
            self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'blocked')
        with self.assertRaises(KeyError):
            self.repo.get('static-exp-'+plan['id'])

    def test_interruption_reuses_pinned_snapshot_and_partial_drafts(self):
        plan = self.create_plan(); original = self.service.pairs.save_draft; count = 0
        def interrupt(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                raise RuntimeError('fixture interruption')
            return original(*args, **kwargs)
        with patch.object(self.service.pairs, 'save_draft', side_effect=interrupt):
            with self.assertRaises(RuntimeError):
                self.service._advance(plan['id'])
        pinned = self.service.list()[0]['resolved']
        restarted = StaticRegistration(self.path, clock=lambda: self.now); restarted.initialize(); restarted.tick()
        self.assertEqual(restarted.list()[0]['status'], 'complete')
        self.assertEqual(restarted.list()[0]['resolved'], pinned)
        self.assertEqual(len(self.repo.get('static-exp-'+plan['id'])['pairs']), 2)

    def test_completion_write_failure_rolls_back_experiment_and_link(self):
        plan = self.create_plan()
        with patch.object(self.service, '_transition', side_effect=RuntimeError('injected')):
            with self.assertRaises(RuntimeError):
                self.service._advance(plan['id'])
        with self.assertRaises(KeyError):
            self.repo.get('static-exp-'+plan['id'])
        self.assertEqual(self.series.get(STATIC_SERIES_KEY)['periods'], [])
        self.assertEqual(self.service.list()[0]['status'], 'waiting')
        self.service.tick()
        self.assertEqual(self.service.list()[0]['status'], 'complete')

    def test_rejects_unconfirmed_or_too_short_history(self):
        with self.assertRaises(StaticRegistrationError):
            self.create_plan(confirmed=False)
        with self.assertRaises(StaticRegistrationError):
            self.service.create(**{**self.args, 'history_periods': 3})

    def test_hot_and_cold_use_fixed_method_and_matched_control(self):
        pairs = PairRepository(self.path)
        for method in ('hot', 'cold'):
            with self.subTest(method=method):
                pair_key = method+'-template'
                series_key = '44444444-4444-4444-8444-' + ('444444444444' if method == 'hot' else '555555555555')
                pairs.save_draft(pair_key, {**self.config, 'method': method, 'seed': 1},
                                 history_first_issue='26001', history_cutoff_issue='26016')
                self.series.create(series_key, {**self.manifest, 'template_pair_key': pair_key})
                service = StaticRegistration(self.path, clock=lambda: self.now)
                plan = service.create(series_key=series_key, issue='26017', calendar_review_id=self.cal_id,
                                      rule_review_id=self.rule_id, tax_review_id=self.tax_id,
                                      expected_cutoff_issue='26016', history_periods=6,
                                      start_at=(self.now-timedelta(minutes=1)).isoformat(),
                                      internal_deadline=(self.deadline-timedelta(hours=1)).isoformat(), confirmed=True)
                service.tick()
                self.assertEqual(next(p for p in service.list() if p['id'] == plan['id'])['status'], 'complete')
                fixed = self.repo.get('static-exp-'+plan['id'])
                for item in fixed['pairs']:
                    pair = item['pair']
                    self.assertEqual(pair['config']['method'], method)
                    self.assertEqual(pair['candidate']['cost_fen'], pair['control']['cost_fen'])


class StaticModelRegistrationTests(unittest.TestCase):
    setUp = model_fixtures.RollingPolicyTests.setUp
    review = model_fixtures.RollingPolicyTests.review
    import_draw = model_fixtures.RollingPolicyTests.import_draw
    _db = model_fixtures.RollingPolicyTests._db
    prepare_second = model_fixtures.RollingPolicyTests.prepare_second
    train = model_fixtures.RollingPolicyTests.train

    def test_changed_model_code_or_runtime_blocks_automatic_plan(self):
        self.manifest['retraining_policy'] = 'fixed_artifact_or_untrained_v1'
        self.series.create(STATIC_MODEL_SERIES_KEY, self.manifest)
        service = StaticRegistration(self.path, clock=lambda: self.now)
        args = dict(series_key=STATIC_MODEL_SERIES_KEY, issue='26017', calendar_review_id=self.cal_id,
                    rule_review_id=self.rule_id, tax_review_id=self.tax_id, expected_cutoff_issue='26016',
                    history_periods=6, start_at=(self.now-timedelta(minutes=1)).isoformat(),
                    internal_deadline=(self.deadline-timedelta(hours=1)).isoformat(), confirmed=True)
        artifact_id = self.series.get(STATIC_MODEL_SERIES_KEY)['content']['artifact_id']
        from dlt.model_repository import ModelRepository
        artifact = ModelRepository(self.path).get(artifact_id)
        for field, value in (('code_sha256', '0'*64), ('runtime', {'python': 'other', 'numpy': 'other'})):
            with self.subTest(field=field), patch.object(ModelRepository, 'get', return_value={**artifact, field: value}):
                with self.assertRaisesRegex(StaticRegistrationError, '模型代码或运行环境'):
                    service.create(**args)

    def test_fixed_tree_artifact_is_used_for_all_replicas(self):
        self.manifest['retraining_policy'] = 'fixed_artifact_or_untrained_v1'
        self.series.create(STATIC_MODEL_SERIES_KEY, self.manifest)
        service = StaticRegistration(self.path, clock=lambda: self.now)
        plan = service.create(series_key=STATIC_MODEL_SERIES_KEY, issue='26017', calendar_review_id=self.cal_id,
                              rule_review_id=self.rule_id, tax_review_id=self.tax_id, expected_cutoff_issue='26016',
                              history_periods=6, start_at=(self.now-timedelta(minutes=1)).isoformat(),
                              internal_deadline=(self.deadline-timedelta(hours=1)).isoformat(), confirmed=True)
        service.tick()
        self.assertEqual(service.list()[0]['status'], 'complete', service.list()[0]['message'])
        fixed = self.repo.get('static-exp-'+plan['id'])
        expected = self.series.get(STATIC_MODEL_SERIES_KEY)['content']['artifact_id']
        self.assertEqual([item['pair']['model_artifact_id'] for item in fixed['pairs']], [expected, expected])

    def test_fixed_lstm_artifact_is_used_for_all_replicas(self):
        config = {**self.train_config, 'method': 'lstm',
                  'algorithm_version': model_fixtures.ALGORITHM_VERSION['lstm'],
                  'preprocessing_version': model_fixtures.PREPROCESSING_VERSION['lstm'],
                  'parameters': {'hidden_size': 3, 'epochs': 1, 'learning_rate': 0.1}}
        artifact = self.train('static-lstm-model', config, '26001')
        selection = {'considered_attempts': ['static-lstm-model'], 'selected_attempt': 'static-lstm-model',
                     'reason': 'single-model fixture selection', 'trial_group_id': None}
        lstm_pair_config = {**self.config, 'method': 'lstm', 'seed': 1,
                            'algorithm_version': model_fixtures.ALGORITHM_VERSION['lstm'],
                            'preprocessing_version': model_fixtures.PREPROCESSING_VERSION['lstm']}
        PairRepository(self.path).save_draft('static-lstm-template', lstm_pair_config,
                            history_first_issue='26001', history_cutoff_issue='26016', artifact_id=artifact,
                            selection=selection)
        manifest = {**self.manifest, 'retraining_policy': 'fixed_artifact_or_untrained_v1',
                    'template_pair_key': 'static-lstm-template'}
        self.series.create(STATIC_LSTM_SERIES_KEY, manifest)
        service = StaticRegistration(self.path, clock=lambda: self.now)
        plan = service.create(series_key=STATIC_LSTM_SERIES_KEY, issue='26017', calendar_review_id=self.cal_id,
                              rule_review_id=self.rule_id, tax_review_id=self.tax_id, expected_cutoff_issue='26016',
                              history_periods=6, start_at=(self.now-timedelta(minutes=1)).isoformat(),
                              internal_deadline=(self.deadline-timedelta(hours=1)).isoformat(), confirmed=True)
        service.tick()
        self.assertEqual(service.list()[0]['status'], 'complete', service.list()[0]['message'])
        fixed = self.repo.get('static-exp-'+plan['id'])
        self.assertEqual([item['pair']['model_artifact_id'] for item in fixed['pairs']], [artifact, artifact])


if __name__ == '__main__':
    unittest.main()
