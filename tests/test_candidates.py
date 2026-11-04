from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from dlt.candidate_service import generate_from_snapshot
from dlt.candidates import CandidateError, generate_pair
from dlt.model_repository import ModelRepository
from dlt.models import ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION
from dlt.snapshots import SnapshotStore


def input_rows(count=16):
    data = []
    for i in range(count):
        day = date(2026, 1, 1)+timedelta(days=i)
        data.append({'issue': f'26{i+1:03}', 'date': day.isoformat(),
                     'front': [1, 2, 3, 4, 5+i%6], 'back': [1, 2+i%4]})
    return data


def config(method='random', structure='singles'):
    return {'method': method, 'structure': structure, 'target_issue': '26017', 'target_date': '2026-01-17',
            'snapshot_id': 'a'*64, 'rule_version': 'synthetic-price-v1', 'calendar_version': 'synthetic-calendar-v1',
            'budget_fen': 12600, 'base_price_fen': 200, 'additional_price_fen': 300,
            'additional': False, 'window': 6, 'seed': 42, 'front_count': 7, 'back_count': 3,
            'max_tickets': 180, 'max_sampling_attempts': 10000, 'score_temperature': 1.0}


class CandidateTests(unittest.TestCase):
    def test_all_methods_structures_and_additional_cost_are_reproducible(self):
        history = input_rows()
        scores = {'front_scores': [float(i)/35 for i in range(35)], 'back_scores': [float(i)/12 for i in range(12)],
                  'artifact_sha256': 'b'*64}
        for method in ('random', 'hot', 'cold', 'tree', 'lstm'):
            for structure in ('singles', 'compound', 'dantuo'):
                for additional in (False, True):
                    with self.subTest(method=method, structure=structure, additional=additional):
                        cfg = {**config(method, structure), 'additional': additional,
                               'budget_fen': 18900 if structure == 'compound' and additional else 12600}
                        model = scores if method in ('tree', 'lstm') else None
                        pair = generate_pair(history=history, config=cfg, model_scores=model)
                        self.assertEqual(pair, generate_pair(history=history, config=cfg, model_scores=model))
                        a, b = pair['candidate'], pair['control']
                        self.assertEqual((a['count'], a['cost_fen']), (b['count'], b['cost_fen']))
                        self.assertNotEqual(a['seed'], b['seed'])
                        self.assertLessEqual(a['cost_fen'], cfg['budget_fen'])
                        for p in (a, b):
                            self.assertEqual(len(p['tickets']), p['count'])
                            self.assertEqual(len({(tuple(t['front']), tuple(t['back'])) for t in p['tickets']}), p['count'])
                            for t in p['tickets']:
                                self.assertEqual((len(set(t['front'])), len(set(t['back']))), (5, 2))
                                self.assertTrue(all(1 <= n <= 35 for n in t['front']))
                                self.assertTrue(all(1 <= n <= 12 for n in t['back']))
                                self.assertEqual(t['additional'], additional)
                                if structure == 'dantuo': self.assertTrue(set(p['pool']['dan']) <= set(t['front']))
                        self.assertEqual(pair['evidence'], 'pending')

    def test_invalid_boundary_missing_model_and_excessive_limits_are_blocked(self):
        history = input_rows()
        for change in ({'budget_fen': 100}, {'target_issue': '26016'}, {'target_date': '2026-01-16'},
                       {'max_tickets': 3}, {'seed': -1}, {'max_sampling_attempts': 1}):
            with self.subTest(change=change), self.assertRaises(CandidateError):
                generate_pair(history=history, config={**config(), **change})
        with self.assertRaisesRegex(CandidateError, 'model scores'):
            generate_pair(history=history, config=config('tree'))
        with self.assertRaisesRegex(CandidateError, 'must not be supplied'):
            generate_pair(history=history, config=config(), model_scores={'artifact_sha256': 'b'*64})

    def test_collapsed_model_scores_terminate_without_partial_pair(self):
        scores = {'front_scores': [1e4]*5+[0.0]*30, 'back_scores': [1e4]*2+[0.0]*10,
                  'artifact_sha256': 'b'*64}
        with self.assertRaisesRegex(CandidateError, 'sampling-attempt limit'):
            generate_pair(history=input_rows(), config={**config('tree'), 'max_sampling_attempts': 100}, model_scores=scores)

    def test_persisted_real_model_produces_candidate_with_matched_random_control(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp)/'data.sqlite3'; history = input_rows()
            source = {'success': True, 'errorCode': '0', 'value': {'list': [
                {'lotteryGameNum': '85', 'lotteryDrawNum': r['issue'], 'lotteryDrawTime': r['date'],
                 'lotteryDrawResult': ' '.join(map(str, r['front']+r['back'])), 'prizeLevelList': []}
                for r in history]}}
            snap = SnapshotStore(db).import_official(json.dumps(source).encode(), 'fixture', '2026-01-17T12:00:00+08:00')['snapshot_id']
            cfg = {'method': 'tree', 'window': 3, 'train_end': '26012', 'cutoff': '26016',
                   'availability_mode': 'historical_reconstruction', 'snapshot_id': snap, 'seed': 42,
                   'algorithm_version': ALGORITHM_VERSION['tree'],
                   'feature_schema_version': FEATURE_SCHEMA_VERSION,
                   'preprocessing_version': PREPROCESSING_VERSION['tree'],
                   'parameters': {'max_depth': 2, 'min_leaf': 2}}
            artifact = ModelRepository(db).train_from_snapshot(snap, cfg, first_issue='26001')
            candidate_cfg = {**config('tree', 'compound'), 'snapshot_id': snap}
            result = generate_from_snapshot(db, candidate_cfg, history_first_issue='26001',
                                            history_cutoff_issue='26016', artifact_id=artifact)
            self.assertEqual(result['model_artifact_id'], artifact)
            self.assertEqual(result['candidate']['count'], 63)
            self.assertEqual(result['candidate']['cost_fen'], result['control']['cost_fen'])
            with self.assertRaisesRegex(CandidateError, 'artifact required'):
                generate_from_snapshot(db, candidate_cfg, history_first_issue='26001', history_cutoff_issue='26016')


if __name__ == '__main__':
    unittest.main()
