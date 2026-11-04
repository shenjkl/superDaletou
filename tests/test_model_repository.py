from contextlib import closing
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from dlt.model_repository import ModelRepository
from dlt.models import (ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION,
                        TrainingError, predict, train)
from dlt.models import _canonical
from dlt.snapshots import SnapshotStore


def payload(count=16):
    result = []
    for i in range(count):
        day = date(2026, 1, 1) + timedelta(days=i)
        result.append({'lotteryGameNum': '85', 'lotteryDrawNum': f'26{i+1:03}',
                       'lotteryDrawTime': day.isoformat(),
                       'lotteryDrawResult': f'01 02 03 04 {5+i%6:02} 01 {2+i%4:02}',
                       'prizeLevelList': []})
    return json.dumps({'success': True, 'errorCode': '0', 'value': {'list': result}}).encode()


def config(snapshot_id, method='tree'):
    return {'method': method, 'window': 3, 'train_end': '26012', 'cutoff': '26016',
            'availability_mode': 'historical_reconstruction', 'snapshot_id': snapshot_id, 'seed': 42,
            'algorithm_version': ALGORITHM_VERSION[method],
            'feature_schema_version': FEATURE_SCHEMA_VERSION,
            'preprocessing_version': PREPROCESSING_VERSION[method],
            'parameters': {'max_depth': 2, 'min_leaf': 2} if method == 'tree'
            else {'hidden_size': 3, 'epochs': 2, 'learning_rate': 0.1}}


class ModelRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'research.sqlite3'
        self.store = SnapshotStore(self.path)
        self.snapshot_id = self.store.import_official(payload(), 'fixture', '2026-01-20T12:00:00+08:00')['snapshot_id']
        self.repo = ModelRepository(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_real_training_artifact_survives_reopen_and_replays(self):
        for method in ('tree', 'lstm'):
            with self.subTest(method=method):
                artifact_id = self.repo.train_from_snapshot(self.snapshot_id, config(self.snapshot_id, method), first_issue='26001')
                self.assertEqual(artifact_id, self.repo.train_from_snapshot(self.snapshot_id, config(self.snapshot_id, method), first_issue='26001'))
                artifact = ModelRepository(self.path).get_for_prediction(artifact_id)
                self.assertEqual(artifact['config']['snapshot_id'], self.snapshot_id)
                self.assertEqual(artifact['config']['availability_mode'], 'historical_reconstruction')
                draws = self.store.get_draws(self.snapshot_id)[-3:]
                history = [{'issue': d['issue'], 'date': d['draw_date'], 'front': d['front'], 'back': d['back']} for d in draws]
                self.assertEqual(len(predict(artifact, history, target_issue='26017', target_date='2026-01-17')['front_scores']), 35)
                with closing(sqlite3.connect(self.path)) as db:
                    with self.assertRaises(sqlite3.IntegrityError):
                        db.execute('DELETE FROM model_artifacts WHERE artifact_id=?', (artifact_id,))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM model_artifacts').fetchone()[0], 2)

    def test_missing_snapshot_bad_window_or_unproven_strict_replay_do_not_publish(self):
        for cfg, first in [(config('f'*64), '26001'), (config(self.snapshot_id), '26016'),
                           ({**config(self.snapshot_id), 'availability_mode': 'strict_replay'}, '26001')]:
            with self.subTest(cfg=cfg, first=first), self.assertRaises(TrainingError):
                self.repo.train_from_snapshot(self.snapshot_id, cfg, first_issue=first)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='model_artifacts'").fetchone())

    def test_conflicting_numbers_block_training_but_prize_only_difference_does_not(self):
        changed = json.loads(payload()); changed['value']['list'][0]['prizeLevelList'] = [{'sort': '101', 'stakeAmount': '12'}]
        self.store.import_official(json.dumps(changed).encode(), 'fixture', '2026-01-20T12:01:00+08:00')
        artifact = self.repo.train_from_snapshot(self.snapshot_id, config(self.snapshot_id), first_issue='26001')
        changed = json.loads(payload()); changed['value']['list'][1]['lotteryDrawResult'] = '01 02 03 04 20 01 03'
        self.store.import_official(json.dumps(changed).encode(), 'fixture', '2026-01-20T12:02:00+08:00')
        with self.assertRaisesRegex(TrainingError, 'unresolved number or date conflicts'):
            self.repo.train_from_snapshot(self.snapshot_id, config(self.snapshot_id), first_issue='26001')
        self.assertEqual(self.repo.get(artifact)['config']['snapshot_id'], self.snapshot_id)
        self.assertEqual(self.repo.inspect(artifact)['status'], 'review_required')
        with self.assertRaisesRegex(TrainingError, 'requires review'):
            self.repo.get_for_prediction(artifact)
        # A conflicting later observation requires review, but does not itself
        # prove that the retained original snapshot or artifact was damaged.
        from dlt.integrity import check_database
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_conflict_arriving_during_training_blocks_atomic_publication(self):
        def train_then_import(rows, cfg):
            artifact = train(rows, cfg)
            changed = json.loads(payload())
            changed['value']['list'][1]['lotteryDrawResult'] = '01 02 03 04 20 01 03'
            self.store.import_official(json.dumps(changed).encode(), 'fixture', '2026-01-20T12:03:00+08:00')
            return artifact
        with patch('dlt.model_repository.train', side_effect=train_then_import):
            with self.assertRaisesRegex(TrainingError, 'unresolved number or date conflicts'):
                self.repo.train_from_snapshot(self.snapshot_id, config(self.snapshot_id), first_issue='26001')
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM model_artifacts').fetchone()[0], 0)

    def test_model_is_not_ready_if_snapshot_projection_is_damaged(self):
        ident = self.repo.train_from_snapshot(self.snapshot_id, config(self.snapshot_id), first_issue='26001')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draws_no_update')
            db.execute("UPDATE draw_versions SET front_json='[1,2,3,4,35]' WHERE issue='26010'")
        state = self.repo.inspect(ident)
        self.assertEqual(state['status'], 'review_required')
        self.assertIn('投影', state['reason'])
        self.assertEqual(self.repo.get(ident)['artifact_sha256'], ident)

    def test_rehashed_artifact_with_wrong_training_targets_is_not_ready(self):
        ident = self.repo.train_from_snapshot(self.snapshot_id, config(self.snapshot_id), first_issue='26001')
        artifact = deepcopy(self.repo.get(ident))
        artifact['train_targets'] = list(reversed(artifact['train_targets']))
        artifact['artifact_sha256'] = hashlib.sha256(_canonical({k:v for k,v in artifact.items() if k != 'artifact_sha256'})).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER model_artifacts_no_update')
            db.execute('UPDATE model_artifacts SET artifact_id=?,artifact_json=? WHERE artifact_id=?',
                       (artifact['artifact_sha256'], _canonical(artifact), ident))
        state = self.repo.inspect(artifact['artifact_sha256'])
        self.assertEqual(state['status'], 'review_required')
        self.assertIn('训练目标', state['reason'])
        from dlt.integrity import check_database
        report = check_database(self.path)
        self.assertTrue(any(item['area'] == 'model_artifacts' for item in report['errors']), report)


if __name__ == '__main__':
    unittest.main()
