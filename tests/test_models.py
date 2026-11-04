from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path
import hashlib
import json
import unittest

import numpy as np

from dlt.models import (ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION,
                        TrainingError, _lstm_loss_grad, _weights_init, predict, prepare_samples, train)
from dlt.snapshots import parse_snapshot


def rows(count=16):
    result = []
    for i in range(count):
        day = date(2026, 1, 1) + timedelta(days=i)
        result.append({'issue': f'26{i+1:03}', 'date': day.isoformat(),
                       'front': sorted({1, 2, 3, 4, 5+i % 6}), 'back': [1, 2+i % 4],
                       'available_at': f'{day.isoformat()}T22:00:00+08:00',
                       'prediction_at': f'{day.isoformat()}T18:00:00+08:00'})
    return result


def config(method):
    return {'method': method, 'window': 3, 'train_end': '26012', 'cutoff': '26016',
            'availability_mode': 'historical_reconstruction', 'snapshot_id': 'a'*64, 'seed': 42,
            'algorithm_version': ALGORITHM_VERSION[method],
            'feature_schema_version': FEATURE_SCHEMA_VERSION,
            'preprocessing_version': PREPROCESSING_VERSION[method],
            'parameters': {'max_depth': 2, 'min_leaf': 2} if method == 'tree'
            else {'hidden_size': 3, 'epochs': 2, 'learning_rate': 0.1}}


class ModelTests(unittest.TestCase):
    def test_lstm_bptt_matches_numerical_gradients(self):
        rng = np.random.default_rng(9)
        sequence = rng.normal(size=(3, 47)); target = (rng.random(47) > .7).astype(float)
        weights = _weights_init(3, 7)
        _, _, grads = _lstm_loss_grad(sequence, target, weights)
        for name, index in [('w', (0, 0)), ('w', (47, 7)), ('b', (3,)), ('wy', (1, 4)), ('by', (8,))]:
            original = weights[name][index]; eps = 1e-6
            weights[name][index] = original + eps
            high = _lstm_loss_grad(sequence, target, weights)[0]
            weights[name][index] = original - eps
            low = _lstm_loss_grad(sequence, target, weights)[0]
            weights[name][index] = original
            self.assertAlmostEqual(grads[name][index], (high-low)/(2*eps), places=7)

    def test_real_training_and_reproducible_inference_for_each_model(self):
        history = rows()
        for method in ('tree', 'lstm'):
            with self.subTest(method=method):
                artifact = train(history, config(method))
                self.assertEqual(artifact, train(history, config(method)))
                result = predict(artifact, history[-3:], target_issue='26017', target_date='2026-01-17')
                self.assertEqual(len(result['front_scores']), 35)
                self.assertEqual(len(result['back_scores']), 12)
                self.assertIn('not a winning probability', result['semantics'])
                self.assertEqual(result, predict(artifact, history[-3:], target_issue='26017', target_date='2026-01-17'))
                if method == 'lstm':
                    self.assertFalse(np.array_equal(np.asarray(artifact['model']['weights']['wy']), _weights_init(3, 42)['wy']))

    def test_validation_outcomes_do_not_change_fitted_weights(self):
        history = rows(); changed = deepcopy(history)
        changed[-1]['front'] = [10, 11, 12, 13, 14]
        for method in ('tree', 'lstm'):
            with self.subTest(method=method):
                first, second = train(history, config(method)), train(changed, config(method))
                self.assertEqual(first['model'], second['model'])
                self.assertNotEqual(first['training_rows_sha256'], second['training_rows_sha256'])

    def test_strict_replay_rejects_late_or_unknown_availability(self):
        history = rows(); cfg = {**config('tree'), 'availability_mode': 'strict_replay'}
        self.assertTrue(train(history, cfg))
        history[2]['available_at'] = '2026-02-01T00:00:00+08:00'
        with self.assertRaisesRegex(TrainingError, 'not available'):
            train(history, cfg)
        history = rows(); del history[2]['available_at']
        with self.assertRaisesRegex(TrainingError, 'timezone-aware'):
            train(history, cfg)

    def test_strict_replay_rejects_a_prediction_after_target_availability(self):
        history = rows(); cfg = {**config('tree'), 'availability_mode': 'strict_replay'}
        history[-1]['prediction_at'] = history[-1]['available_at']
        with self.assertRaisesRegex(TrainingError, 'target outcome was already available'):
            train(history, cfg)

    def test_future_rows_and_target_window_are_blocked(self):
        history = rows(); cfg = config('tree')
        with self.assertRaisesRegex(TrainingError, 'end at the explicit cutoff'):
            train(history + rows(17)[-1:], cfg)
        artifact = train(history, cfg)
        with self.assertRaisesRegex(TrainingError, 'chronological'):
            predict(artifact, history[-3:], target_issue='26016', target_date='2026-01-17')
        with self.assertRaisesRegex(TrainingError, 'chronological'):
            predict(artifact, history[-3:], target_issue='26017', target_date='2026-01-16')

    def test_strict_prediction_rejects_late_prediction_and_late_inputs(self):
        history = rows(); artifact = train(history, {**config('tree'), 'availability_mode': 'strict_replay'})
        args = {'target_issue': '26017', 'target_date': '2026-01-17',
                'prediction_at': '2026-01-17T17:00:00+08:00', 'target_deadline': '2026-01-17T18:00:00+08:00'}
        self.assertTrue(predict(artifact, history[-3:], **args))
        with self.assertRaisesRegex(TrainingError, 'deadline'):
            predict(artifact, history[-3:], **{**args, 'prediction_at': args['target_deadline']})
        with self.assertRaisesRegex(TrainingError, 'deadline does not match'):
            predict(artifact, history[-3:], **{**args, 'target_deadline': '2026-01-18T18:00:00+08:00'})
        later = deepcopy(history[-3:]); later[-1]['available_at'] = '2026-01-17T17:00:00+08:00'
        with self.assertRaisesRegex(TrainingError, 'not available'):
            predict(artifact, later, **args)

    def test_tampered_artifact_is_rejected(self):
        history = rows(); artifact = train(history, config('tree'))
        artifact['config']['seed'] = 1
        with self.assertRaisesRegex(TrainingError, 'integrity'):
            predict(artifact, history[-3:], target_issue='26017', target_date='2026-01-17')

    def test_tree_and_lstm_train_on_reviewed_official_snapshot(self):
        raw = (Path(__file__).resolve().parents[1] / 'research/data/official_page1.json').read_bytes()
        payload, draws = parse_snapshot(raw)
        history = [{'issue': r['issue'], 'date': r['draw_date'], 'front': r['front'], 'back': r['back']}
                   for r in sorted(draws, key=lambda x: x['draw_date'])]
        source_id = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                               separators=(',', ':')).encode()).hexdigest()
        for method in ('tree', 'lstm'):
            cfg = {'method': method, 'window': 5, 'train_end': history[-11]['issue'],
                   'cutoff': history[-1]['issue'], 'availability_mode': 'historical_reconstruction',
                   'snapshot_id': source_id, 'seed': 42,
                   'algorithm_version': ALGORITHM_VERSION[method],
                   'feature_schema_version': FEATURE_SCHEMA_VERSION,
                   'preprocessing_version': PREPROCESSING_VERSION[method],
                   'parameters': {'max_depth': 2, 'min_leaf': 2} if method == 'tree'
                   else {'hidden_size': 3, 'epochs': 1, 'learning_rate': 0.1}}
            with self.subTest(method=method):
                artifact = train(history, cfg)
                self.assertEqual(artifact['config']['snapshot_id'], source_id)
                self.assertEqual(len(artifact['validation_targets']), 10)
                self.assertIn('not a winning probability', artifact['score_semantics'])
                self.assertEqual(len(artifact['artifact_sha256']), 64)


if __name__ == '__main__':
    unittest.main()
