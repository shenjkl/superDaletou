from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from dlt.candidates import _canonical
from dlt.generation_replay import ReplayError, build_generation_replay, verify_generation_replay
from dlt.model_repository import ModelRepository
from dlt.pair_repository import PairRepository
from dlt.snapshots import SnapshotStore
import test_candidates as candidate_fixtures
import test_model_repository as model_fixtures


class GenerationReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'research.sqlite3'
        self.snapshot = SnapshotStore(self.path).import_official(model_fixtures.payload(),
            'source credential: SECRET_SOURCE', '2026-01-20T12:00:00+08:00')['snapshot_id']
        self.pairs = PairRepository(self.path)

    def save(self, method='random', artifact_id=None, key='private/path/SECRET_REQUEST'):
        config = {**candidate_fixtures.config(method), 'snapshot_id': self.snapshot, 'budget_fen': 400,
                  'rule_version': r'C:\Users\private\SECRET_RULE',
                  'calendar_version': 'Bearer SECRET_CALENDAR',
                  'unrecognized': {'token': 'SECRET_TOKEN'}}
        return self.pairs.save_draft(key, config, history_first_issue='26001',
                                     history_cutoff_issue='26016', artifact_id=artifact_id)

    def test_generation_replay_redacts_free_text_and_reproduces_all_methods(self):
        for method in ('random', 'hot', 'cold', 'tree', 'lstm'):
            with self.subTest(method=method):
                artifact_id = None
                if method in ('tree', 'lstm'):
                    artifact_id = ModelRepository(self.path).train_from_snapshot(
                        self.snapshot, model_fixtures.config(self.snapshot, method), first_issue='26001')
                key = 'private/path/SECRET_REQUEST-' + method
                original = self.save(method, artifact_id, key)
                bundle = build_generation_replay(self.path, key)
                self.assertEqual(bundle['candidate'], original['candidate'])
                self.assertEqual(bundle['control'], original['control'])
                self.assertEqual(verify_generation_replay(bundle)['status'], 'matched')
                encoded = json.dumps(bundle, ensure_ascii=False)
                for secret in ('SECRET_SOURCE', 'SECRET_REQUEST', 'SECRET_RULE', 'SECRET_CALENDAR', 'SECRET_TOKEN', 'C:\\\\Users'):
                    self.assertNotIn(secret, encoded)
                self.assertNotIn('unrecognized', encoded)
                self.assertNotIn('artifact_json', encoded)

    def test_digest_or_candidate_tampering_fails_replay(self):
        self.save()
        bundle = build_generation_replay(self.path, 'private/path/SECRET_REQUEST')
        altered = deepcopy(bundle)
        altered['candidate']['tickets'][0]['front'][0] = 35
        with self.assertRaises(ReplayError):
            verify_generation_replay(altered)
        altered['bundle_sha256'] = hashlib.sha256(_canonical({k: v for k, v in altered.items() if k != 'bundle_sha256'})).hexdigest()
        with self.assertRaises(ReplayError):
            verify_generation_replay(altered)
        with patch('dlt.generation_replay.Path.read_bytes', return_value=b'different code'):
            with self.assertRaises(ReplayError):
                verify_generation_replay(bundle)

    def test_changed_snapshot_projection_or_conflict_blocks_export(self):
        self.save()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draws_no_update')
            db.execute("UPDATE draw_versions SET front_json='[1,2,3,4,35]' WHERE issue='26015'")
        with self.assertRaises(ReplayError):
            build_generation_replay(self.path, 'private/path/SECRET_REQUEST')

    def test_conflicting_history_blocks_export(self):
        self.save()
        source = json.loads(model_fixtures.payload())
        source['value']['list'][14]['lotteryDrawResult'] = '01 02 03 04 35 01 02'
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'alternate fixture',
                                                '2026-01-20T12:01:00+08:00')
        with self.assertRaises(ReplayError):
            build_generation_replay(self.path, 'private/path/SECRET_REQUEST')

    def test_modified_original_code_identity_blocks_export(self):
        self.save()
        with patch('dlt.generation_replay.Path.read_bytes', return_value=b'different code'):
            with self.assertRaises(ReplayError):
                build_generation_replay(self.path, 'private/path/SECRET_REQUEST')

    def test_compound_replay_does_not_require_unused_sampling_limit(self):
        config = {**candidate_fixtures.config('hot', 'compound'), 'snapshot_id': self.snapshot}
        del config['max_sampling_attempts']
        pair = self.pairs.save_draft('compound', config, history_first_issue='26001', history_cutoff_issue='26016')
        bundle = build_generation_replay(self.path, 'compound')
        self.assertEqual(bundle['candidate'], pair['candidate'])
        self.assertEqual(verify_generation_replay(bundle)['status'], 'matched')

    def test_tree_and_lstm_training_can_be_replayed_without_free_text(self):
        for method in ('tree', 'lstm'):
            with self.subTest(method=method):
                training_config = model_fixtures.config(self.snapshot, method)
                training_config['secret'] = r'C:\private\SECRET_TRAINING'
                training_config['parameters']['token'] = 'SECRET_PARAMETER'
                artifact = ModelRepository(self.path).train_from_snapshot(self.snapshot, training_config, first_issue='26001')
                key = 'model-' + method
                original = self.save(method, artifact, key)
                bundle = build_generation_replay(self.path, key)
                self.assertEqual(bundle['format'], 'dlt-generation-replay-v2')
                self.assertEqual(bundle['candidate'], original['candidate'])
                self.assertEqual(verify_generation_replay(bundle)['training'], 'not_run')
                self.assertEqual(verify_generation_replay(bundle, retrain=True)['training'], 'matched')
                encoded = json.dumps(bundle, ensure_ascii=False)
                self.assertNotIn('SECRET_TRAINING', encoded)
                self.assertNotIn('SECRET_PARAMETER', encoded)

    def test_training_weight_tampering_and_runtime_mismatch_are_rejected(self):
        artifact = ModelRepository(self.path).train_from_snapshot(
            self.snapshot, model_fixtures.config(self.snapshot, 'lstm'), first_issue='26001')
        self.save('lstm', artifact, 'lstm-pair')
        bundle = build_generation_replay(self.path, 'lstm-pair')
        altered = deepcopy(bundle)
        altered['model_training']['model']['weights']['by'][0] += 1
        altered['model_training']['model_sha256'] = hashlib.sha256(_canonical(altered['model_training']['model'])).hexdigest()
        altered['bundle_sha256'] = hashlib.sha256(_canonical({k: v for k, v in altered.items() if k != 'bundle_sha256'})).hexdigest()
        with self.assertRaisesRegex(ReplayError, '评分'):
            verify_generation_replay(altered)
        with patch('dlt.generation_replay.platform.python_version', return_value='0.0.0'):
            with self.assertRaisesRegex(ReplayError, '版本'):
                verify_generation_replay(bundle, retrain=True)

    def test_v1_packages_remain_usable_but_model_retraining_is_unavailable(self):
        artifact = ModelRepository(self.path).train_from_snapshot(
            self.snapshot, model_fixtures.config(self.snapshot, 'tree'), first_issue='26001')
        self.save('tree', artifact, 'old-pair')
        bundle = build_generation_replay(self.path, 'old-pair')
        del bundle['model_training']
        bundle['format'] = 'dlt-generation-replay-v1'
        bundle['bundle_sha256'] = hashlib.sha256(_canonical({k: v for k, v in bundle.items() if k != 'bundle_sha256'})).hexdigest()
        self.assertEqual(verify_generation_replay(bundle)['training'], 'not_available')
        with self.assertRaisesRegex(ReplayError, '旧版'):
            verify_generation_replay(bundle, retrain=True)

    def test_retraining_timeout_and_resource_limit_are_reported(self):
        artifact = ModelRepository(self.path).train_from_snapshot(
            self.snapshot, model_fixtures.config(self.snapshot, 'lstm'), first_issue='26001')
        self.save('lstm', artifact, 'bounded-model')
        bundle = build_generation_replay(self.path, 'bounded-model')
        with patch('dlt.generation_replay.subprocess.run', side_effect=subprocess.TimeoutExpired('fixture', 120)):
            with self.assertRaisesRegex(ReplayError, '120 秒'):
                verify_generation_replay(bundle, retrain=True)
        altered = deepcopy(bundle)
        altered['model_training']['config']['parameters']['hidden_size'] = 10000
        altered['bundle_sha256'] = hashlib.sha256(_canonical({k: v for k, v in altered.items() if k != 'bundle_sha256'})).hexdigest()
        with self.assertRaisesRegex(ReplayError, '上限'):
            verify_generation_replay(altered, retrain=True)

    def test_declared_training_exclusion_is_preserved_in_model_replay(self):
        from test_backtest import source_data
        snapshot = SnapshotStore(self.path).import_official(json.dumps(source_data()).encode(), 'fixture',
                    '2026-02-01T12:00:00+08:00')['snapshot_id']
        cfg = {**model_fixtures.config(snapshot), 'train_end': '26014', 'cutoff': '26017',
               'holdout_start': '26018', 'holdout_end': '26020'}
        artifact = ModelRepository(self.path).train_from_snapshot(snapshot, cfg, first_issue='26001')
        candidate = {**candidate_fixtures.config('tree'), 'snapshot_id': snapshot,
                     'target_issue': '26021', 'target_date': '2026-01-21', 'budget_fen': 400}
        self.pairs.save_draft('excluded-model', candidate, history_first_issue='26001',
                              history_cutoff_issue='26017', artifact_id=artifact)
        bundle = build_generation_replay(self.path, 'excluded-model')
        self.assertEqual(bundle['model_training']['config']['holdout_start'], '26018')
        self.assertEqual(bundle['model_training']['config']['holdout_end'], '26020')
        self.assertEqual(verify_generation_replay(bundle, retrain=True)['training'], 'matched')


if __name__ == '__main__':
    unittest.main()
