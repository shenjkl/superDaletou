from contextlib import closing
from copy import deepcopy
from datetime import timedelta
import json
import sqlite3
import sys
import time
import unittest
from unittest.mock import patch

from dlt.integrity import check_database
from dlt.generation_replay import build_generation_replay, verify_generation_replay
from dlt.candidates import CandidateError
from dlt.models import ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION
from dlt.pair_repository import PairRepository
from dlt.prospective import RegistrationError
from dlt.snapshots import SnapshotStore
from dlt.training_jobs import TrainingJobs
import test_prospective_series as series_fixtures

ROLLING_SERIES_KEY = series_fixtures.SERIES_KEY


class RollingPolicyTests(unittest.TestCase):
    review = series_fixtures.SeriesTests.review
    import_draw = series_fixtures.SeriesTests.import_draw
    _db = series_fixtures.SeriesTests._db
    prepare_second = series_fixtures.SeriesTests.prepare_second

    def setUp(self):
        series_fixtures.SeriesTests.setUp(self)
        with closing(sqlite3.connect(self.path)) as db:
            self.fixture_observed_at = db.execute(
                'SELECT MIN(observed_at) FROM observations WHERE snapshot_id=?', (self.snapshot,)
            ).fetchone()[0]
        self.jobs = TrainingJobs(self.path)
        self.jobs.initialize()
        self.addCleanup(self.jobs.stop)
        self.train_config = {'method': 'tree', 'window': 3, 'train_end': '26012', 'cutoff': '26016',
                             'availability_mode': 'historical_reconstruction', 'snapshot_id': self.snapshot,
                             'seed': 42, 'rolling_issue': '26017',
                             'algorithm_version': ALGORITHM_VERSION['tree'],
                             'feature_schema_version': FEATURE_SCHEMA_VERSION,
                             'preprocessing_version': PREPROCESSING_VERSION['tree'],
                             'parameters': {'max_depth': 2, 'min_leaf': 2}}
        artifact = self.train('first-model', self.train_config, '26001')
        self.tree_config = {**self.config, 'method': 'tree'}
        pairs = PairRepository(self.path)
        selection = {'considered_attempts': ['first-model'], 'selected_attempt': 'first-model',
                     'reason': 'single-model fixture selection', 'trial_group_id': None}
        for seed in (1, 2):
            pairs.save_draft('first-tree-'+str(seed), {**self.tree_config, 'seed': seed},
                             history_first_issue='26001', history_cutoff_issue='26016', artifact_id=artifact,
                             selection=selection)
        self.protocol.update(retraining_policy='rolling_retrain_v1',
                             replicas=[{'replica': 'a', 'pair_key': 'first-tree-1'},
                                       {'replica': 'b', 'pair_key': 'first-tree-2'}])
        self.manifest.update(retraining_policy='rolling_retrain_v1', template_pair_key='first-tree-1')

    def train(self, key, config, first):
        self.jobs.submit(key, name='rolling fixture', config=config, first_issue=first, timeout=20)
        until = time.monotonic()+20
        while time.monotonic() < until:
            job = self.jobs.get(key)
            if job['status'] != 'running':
                self.assertEqual(job['status'], 'complete', job.get('error'))
                return job['artifact_id']
            time.sleep(.05)
        self.fail('training did not complete')

    def test_fixed_rolling_contract_accepts_separately_trained_next_period(self):
        content = self.series.create(ROLLING_SERIES_KEY, self.manifest)
        self.assertEqual(content['rolling_spec']['training_periods'], 16)
        self.assertEqual(content['rolling_spec']['validation_periods'], 4)
        self.repo.freeze('period-1', self.protocol, series_key=ROLLING_SERIES_KEY)
        second = self.prepare_second()
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '26017',
            'lotteryDrawTime': self.calendar['draw_date'], 'lotteryDrawResult': '01 02 03 04 15 01 02',
            'prizeLevelList': []})
        new_snapshot = SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'next fixture',
                            (self.now-timedelta(minutes=5)).isoformat())['snapshot_id']
        self.assertEqual(self.series.create(ROLLING_SERIES_KEY, self.manifest), content)
        replay = build_generation_replay(self.path, 'first-tree-1')
        self.assertEqual(replay['model_training']['config']['rolling_issue'], '26017')
        self.assertEqual(verify_generation_replay(replay, retrain=True)['training'], 'matched')
        next_cfg = {**self.train_config, 'snapshot_id': new_snapshot, 'train_end': '26013',
                    'cutoff': '26017', 'rolling_issue': '26018'}
        with patch('dlt.training_jobs.datetime') as fake_clock:
            fake_clock.now.return_value = self.now-timedelta(minutes=2)
            artifact = self.train('second-model', next_cfg, '26002')
            bad_artifact = self.train('changed-seed-model', {**next_cfg, 'seed': 43}, '26002')
        with patch('dlt.pair_repository.datetime') as fake_clock:
            fake_clock.now.return_value = self.now-timedelta(minutes=1)
            for seed in (3, 4):
                PairRepository(self.path).save_draft('next-tree-'+str(seed),
                    {**self.tree_config, 'snapshot_id': new_snapshot, 'target_issue': '26018',
                     'target_date': (self.deadline+timedelta(days=1)).date().isoformat(), 'seed': seed},
                    history_first_issue='26002', history_cutoff_issue='26017', artifact_id=artifact,
                    selection={'considered_attempts': ['second-model'], 'selected_attempt': 'second-model',
                               'reason': 'single-model fixture selection', 'trial_group_id': None})
            for seed in (3, 4):
                PairRepository(self.path).save_draft('changed-seed-pair-'+str(seed),
                    {**self.tree_config, 'snapshot_id': new_snapshot, 'target_issue': '26018',
                     'target_date': (self.deadline+timedelta(days=1)).date().isoformat(), 'seed': seed},
                    history_first_issue='26002', history_cutoff_issue='26017', artifact_id=bad_artifact,
                    selection={'considered_attempts': ['changed-seed-model'],
                               'selected_attempt': 'changed-seed-model',
                               'reason': 'single-model fixture selection', 'trial_group_id': None})
        second['replicas'] = [{'replica': 'a', 'pair_key': 'next-tree-3'}, {'replica': 'b', 'pair_key': 'next-tree-4'}]
        changed = deepcopy(second)
        changed['replicas'][0]['pair_key'] = 'changed-seed-pair-3'
        changed['replicas'][1]['pair_key'] = 'changed-seed-pair-4'
        with self.assertRaisesRegex(RegistrationError, '固定滚动训练协议'):
            self.repo.freeze('invalid-period', changed, series_key=ROLLING_SERIES_KEY)
        fixed = self.repo.freeze('period-2', second, series_key=ROLLING_SERIES_KEY)
        self.assertNotEqual(content['artifact_id'], fixed['pairs'][0]['pair']['model_artifact_id'])
        self.assertEqual(self.series.get(ROLLING_SERIES_KEY)['periods'][-1]['issue'], '26018')
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_template_requires_completed_training_job(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("DELETE FROM training_jobs WHERE request_key='first-model'")
        with self.assertRaises((RegistrationError, CandidateError)):
            self.series.create(ROLLING_SERIES_KEY, self.manifest)

    def test_fixed_experiment_remains_readable_when_original_model_code_is_missing(self):
        self.series.create(ROLLING_SERIES_KEY, self.manifest)
        fixed = self.repo.freeze('period-1', self.protocol, series_key=ROLLING_SERIES_KEY)
        other = self.path.with_name('different_model_version.py')
        other.write_text('# different model version', encoding='utf-8')
        with patch('dlt.models.__file__', str(other)):
            self.assertEqual(self.repo.inspect('period-1')['status'], 'locally_attested')
            self.assertEqual(self.repo.freeze('period-1', self.protocol, series_key=ROLLING_SERIES_KEY), fixed)
            self.assertEqual(PairRepository(self.path).inspect('first-tree-1')['status'], 'review_required')

    def test_snapshot_must_be_observed_before_training_starts(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            observed = db.execute('SELECT observed_at FROM observations WHERE snapshot_id=?', (self.snapshot,)).fetchone()[0]
            from datetime import datetime
            earlier = (datetime.fromisoformat(observed)-timedelta(seconds=1)).isoformat()
            db.execute("UPDATE training_jobs SET created_at=? WHERE request_key='first-model'", (earlier,))
        with self.assertRaisesRegex(RegistrationError, '训练开始时'):
            self.series.create(ROLLING_SERIES_KEY, self.manifest)

    def test_preexisting_newer_local_observation_blocks_stale_template(self):
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '25999',
            'lotteryDrawTime': (self.deadline-timedelta(days=1)).date().isoformat(),
            'lotteryDrawResult': '01 02 03 04 35 01 02', 'prizeLevelList': []})
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'newer fixture',
                                                self.fixture_observed_at)
        with self.assertRaisesRegex(RegistrationError, '最新.*历史期'):
            self.series.create(ROLLING_SERIES_KEY, self.manifest)

    def test_newer_observation_between_training_start_and_pair_blocks_template(self):
        from datetime import datetime
        with closing(sqlite3.connect(self.path)) as db:
            started = datetime.fromisoformat(db.execute("SELECT created_at FROM training_jobs WHERE request_key='first-model'").fetchone()[0])
            created = datetime.fromisoformat(db.execute("SELECT created_at FROM draft_pairs WHERE request_key='first-tree-1'").fetchone()[0])
        self.assertGreater(created, started)
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '25999',
            'lotteryDrawTime': (self.deadline-timedelta(days=1)).date().isoformat(),
            'lotteryDrawResult': '01 02 03 04 35 01 02', 'prizeLevelList': []})
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'arrived mid-training',
                                                (started+(created-started)/2).isoformat())
        with self.assertRaisesRegex(RegistrationError, '最新.*历史期'):
            self.series.create(ROLLING_SERIES_KEY, self.manifest)

    def test_template_rejects_gap_in_latest_observed_training_window(self):
        source = deepcopy(self.source)
        rows = source['value']['list']
        first_date = self.deadline.date()-timedelta(days=19)
        rows.pop(7)
        rows.append({'lotteryGameNum': '85', 'lotteryDrawNum': '25999',
                     'lotteryDrawTime': first_date.isoformat(),
                     'lotteryDrawResult': '01 02 03 04 35 01 02', 'prizeLevelList': []})
        sid = SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'gapped source',
                                                       self.fixture_observed_at)['snapshot_id']
        config = {**self.train_config, 'snapshot_id': sid}
        artifact = self.train('gapped-model', config, '25999')
        PairRepository(self.path).save_draft('gapped-pair', {**self.tree_config, 'snapshot_id': sid, 'seed': 1},
                                            history_first_issue='25999', history_cutoff_issue='26016', artifact_id=artifact,
                                            selection={'considered_attempts': ['gapped-model'],
                                                       'selected_attempt': 'gapped-model',
                                                       'reason': 'single-model fixture selection',
                                                       'trial_group_id': None})
        with self.assertRaisesRegex(RegistrationError, '最新.*历史期'):
            self.series.create(ROLLING_SERIES_KEY, {**self.manifest, 'template_pair_key': 'gapped-pair'})

    def test_same_date_distinct_issues_cannot_be_silently_ordered(self):
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '25999',
            'lotteryDrawTime': source['value']['list'][-1]['lotteryDrawTime'],
            'lotteryDrawResult': '01 02 03 04 35 01 02', 'prizeLevelList': []})
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'same date',
                                                self.fixture_observed_at)
        with self.assertRaisesRegex(RegistrationError, '同日奖期顺序'):
            self.series.create(ROLLING_SERIES_KEY, self.manifest)

    def test_already_observed_target_cannot_start_rolling_training(self):
        config = {**self.train_config, 'rolling_issue': '26016'}
        with self.assertRaisesRegex(ValueError, '已开奖'):
            self.jobs.submit('late-model', name='late', config=config, first_issue='26001', timeout=20)

    def test_lstm_template_can_fix_a_rolling_training_contract(self):
        config = {**self.train_config, 'method': 'lstm',
                  'algorithm_version': ALGORITHM_VERSION['lstm'],
                  'preprocessing_version': PREPROCESSING_VERSION['lstm'],
                  'parameters': {'hidden_size': 3, 'epochs': 1, 'learning_rate': 0.1}}
        artifact = self.train('first-lstm', config, '26001')
        lstm_pair_config = {**self.config, 'method': 'lstm', 'seed': 1}
        PairRepository(self.path).save_draft('first-lstm-pair', lstm_pair_config,
                             history_first_issue='26001', history_cutoff_issue='26016', artifact_id=artifact,
                             selection={'considered_attempts': ['first-lstm'], 'selected_attempt': 'first-lstm',
                                        'reason': 'single-model fixture selection', 'trial_group_id': None})
        manifest = {**self.manifest, 'template_pair_key': 'first-lstm-pair'}
        result = self.series.create(ROLLING_SERIES_KEY, manifest)
        self.assertEqual(result['rolling_spec']['config_profile']['method'], 'lstm')
        self.assertEqual(result['rolling_spec']['config_profile']['parameters']['hidden_size'], 3)

    def test_rolling_target_must_be_explicit_and_never_use_invalid_format(self):
        for target in ('2601', 'not-an-issue', ''):
            with self.subTest(target=target), self.assertRaises(ValueError):
                self.jobs.submit('bad-target-'+target, name='bad', config={**self.train_config, 'rolling_issue': target},
                                 first_issue='26001', timeout=20)

    def test_unknown_template_parameters_cannot_be_hidden_in_fixed_rule(self):
        from dlt.training_jobs import JobError
        config = {**self.train_config, 'parameters': {**self.train_config['parameters'], 'extra': 123}}
        with self.assertRaisesRegex(JobError, '模型参数字段缺失或不受支持'):
            self.train('hidden-param-model', config, '26001')

    def test_target_appearing_while_training_prevents_artifact_publication(self):
        self.jobs.worker_command = [sys.executable, '-c',
            "import time,runpy; time.sleep(1); runpy.run_module('dlt.training_worker',run_name='__main__')"]
        cfg = {**self.train_config, 'seed': 44}
        self.jobs.submit('pending-when-drawn', name='rolling', config=cfg, first_issue='26001', timeout=15)
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '26017',
            'lotteryDrawTime': self.calendar['draw_date'], 'lotteryDrawResult': '01 02 03 04 15 01 02',
            'prizeLevelList': []})
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'draw appeared', self.now.isoformat())
        until = time.monotonic()+20
        while time.monotonic() < until:
            job = self.jobs.get('pending-when-drawn')
            if job['status'] != 'running':
                break
            time.sleep(.05)
        self.assertEqual(job['status'], 'failed')
        self.assertIsNone(job['artifact_id'])

    def test_completed_training_retry_after_target_is_idempotent_not_a_new_run(self):
        original = self.jobs.get('first-model')
        source = deepcopy(self.source)
        source['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '26017',
            'lotteryDrawTime': self.calendar['draw_date'], 'lotteryDrawResult': '01 02 03 04 15 01 02',
            'prizeLevelList': []})
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'draw appeared', self.now.isoformat())
        result = self.jobs.submit('first-model', name='rolling fixture', config=self.train_config,
                                  first_issue='26001', timeout=20)
        self.assertEqual(result, original)
        with self.assertRaisesRegex(ValueError, '已开奖'):
            self.jobs.submit('new-late-job', name='rolling fixture', config=self.train_config,
                             first_issue='26001', timeout=20)


if __name__ == '__main__':
    unittest.main()
