from contextlib import closing
from copy import deepcopy
import hashlib
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

from dlt.candidates import CandidateError, _canonical
from dlt.model_repository import ModelRepository
import test_generation_replay as fixtures
import test_model_repository as model_fixtures


def alter_candidate(pair):
    pair = deepcopy(pair)
    ticket = pair['candidate']['tickets'][0]
    old = set(ticket['front'])
    ticket['front'] = sorted(ticket['front'][1:] + [next(v for v in range(1,36) if v not in old)])
    pair['pair_sha256'] = hashlib.sha256(_canonical({k:v for k,v in pair.items() if k != 'pair_sha256'})).hexdigest()
    return pair


class ModelPairValidationTests(unittest.TestCase):
    setUp = fixtures.GenerationReplayTests.setUp
    save = fixtures.GenerationReplayTests.save

    def test_tree_and_lstm_model_candidates_must_match_retained_weights(self):
        for method in ('tree', 'lstm'):
            with self.subTest(method=method):
                artifact = ModelRepository(self.path).train_from_snapshot(
                    self.snapshot, model_fixtures.config(self.snapshot, method), first_issue='26001')
                key = 'model-' + method
                original = self.save(method, artifact, key)
                self.assertEqual(self.pairs.inspect(key)['status'], 'research_draft')
                altered = alter_candidate(original)
                with closing(sqlite3.connect(self.path)) as db, db:
                    db.execute('DROP TRIGGER draft_pairs_no_update')
                    db.execute('UPDATE draft_pairs SET pair_sha256=?,pair_json=? WHERE request_key=?',
                               (altered['pair_sha256'], _canonical(altered), key))
                    db.execute("CREATE TRIGGER draft_pairs_no_update BEFORE UPDATE ON draft_pairs BEGIN SELECT RAISE(ABORT, 'immutable draft pair'); END")
                result = self.pairs.inspect(key)
                self.assertEqual(result['status'], 'review_required')
                self.assertIn('模型候选', result['reason'])
                self.assertEqual(self.pairs.get(key), altered)
                from dlt.integrity import check_database
                report = check_database(self.path)
                self.assertTrue(any(item['area'] == 'draft_pairs' for item in report['errors']), report)

    def test_missing_original_model_code_blocks_new_use_but_not_historical_read(self):
        artifact = ModelRepository(self.path).train_from_snapshot(
            self.snapshot, model_fixtures.config(self.snapshot, 'tree'), first_issue='26001')
        pair = self.save('tree', artifact, 'model')
        other = self.path.with_name('other_models.py')
        other.write_text('# another model version', encoding='utf-8')
        with patch('dlt.models.__file__', str(other)):
            self.assertEqual(self.pairs.get('model'), pair)
            self.assertEqual(self.pairs.inspect('model')['status'], 'review_required')
            with closing(sqlite3.connect(self.path)) as db:
                with self.assertRaisesRegex(CandidateError, '无法重放'):
                    self.pairs._checked_pair(db, 'model', require_replay=True)

    def test_save_rejects_legal_but_wrong_model_candidate_before_commit(self):
        from dlt.candidate_service import generate_from_snapshot
        artifact = ModelRepository(self.path).train_from_snapshot(
            self.snapshot, model_fixtures.config(self.snapshot, 'tree'), first_issue='26001')
        def altered(*args, **kwargs):
            return alter_candidate(generate_from_snapshot(*args, **kwargs))
        with patch('dlt.pair_repository.generate_from_snapshot', side_effect=altered):
            with self.assertRaisesRegex(CandidateError, '比较清单'):
                self.save('tree', artifact, 'never-published')
        with self.assertRaises(KeyError):
            self.pairs.get('never-published')
