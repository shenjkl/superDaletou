import json
from copy import deepcopy
import hashlib
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from dlt.candidate_service import generate_from_snapshot
from dlt.candidates import CandidateError
from dlt.candidates import _canonical
from dlt.pair_repository import PairRepository
from dlt.snapshots import SnapshotStore
from test_candidates import config, input_rows


class PairRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'data.sqlite3'
        self.source = {'success': True, 'errorCode': '0', 'value': {'list': [
            {'lotteryGameNum': '85', 'lotteryDrawNum': r['issue'], 'lotteryDrawTime': r['date'],
             'lotteryDrawResult': ' '.join(map(str, r['front'] + r['back'])), 'prizeLevelList': []}
            for r in input_rows()]}}
        snapshot = SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'fixture',
                                                             '2026-01-17T12:00:00+08:00')['snapshot_id']
        self.config = {**config(), 'snapshot_id': snapshot}
        self.repo = PairRepository(self.path)
        self.bounds = {'history_first_issue': '26001', 'history_cutoff_issue': '26016'}

    def test_restart_idempotence_and_input_collision(self):
        pair = self.repo.save_draft('request-1', self.config, **self.bounds)
        self.assertEqual(pair, PairRepository(self.path).get('request-1'))
        self.assertEqual(pair, self.repo.save_draft('request-1', self.config, **self.bounds))
        self.assertEqual(pair['evidence'], 'pending')
        with self.assertRaisesRegex(CandidateError, 'different input'):
            self.repo.save_draft('request-1', {**self.config, 'seed': 99}, **self.bounds)
        with closing(sqlite3.connect(self.path)) as db, db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("DELETE FROM draft_pairs WHERE request_key='request-1'")

    def test_conflict_arriving_after_generation_blocks_commit(self):
        def generate_then_conflict(*args, **kwargs):
            pair = generate_from_snapshot(*args, **kwargs)
            self.source['value']['list'][0]['lotteryDrawResult'] = '10 11 12 13 14 01 02'
            SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'other',
                                                     '2026-01-18T12:00:00+08:00')
            return pair
        with patch('dlt.pair_repository.generate_from_snapshot', side_effect=generate_then_conflict):
            with self.assertRaisesRegex(CandidateError, 'conflicts'):
                self.repo.save_draft('request-2', self.config, **self.bounds)
        with self.assertRaises(KeyError):
            self.repo.get('request-2')

    def test_failed_insert_leaves_no_partial_pair(self):
        self.repo._initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("CREATE TRIGGER fail_pair BEFORE INSERT ON draft_pairs BEGIN SELECT RAISE(ABORT, 'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.save_draft('request-3', self.config, **self.bounds)
        with self.assertRaises(KeyError):
            self.repo.get('request-3')

    def test_forged_consistent_digest_cannot_publish_unmatched_control(self):
        def invalid_pair(*args, **kwargs):
            pair = generate_from_snapshot(*args, **kwargs)
            pair['control']['cost_fen'] += 1
            payload = {key: value for key, value in pair.items() if key != 'pair_sha256'}
            pair['pair_sha256'] = hashlib.sha256(_canonical(payload)).hexdigest()
            return pair
        with patch('dlt.pair_repository.generate_from_snapshot', side_effect=invalid_pair):
            with self.assertRaisesRegex(CandidateError, '成本'):
                self.repo.save_draft('bad-pair', self.config, **self.bounds)
        with self.assertRaises(KeyError):
            self.repo.get('bad-pair')

    def test_changed_history_digest_keeps_old_record_but_blocks_use(self):
        pair = deepcopy(self.repo.save_draft('old-pair', self.config, **self.bounds))
        pair['history_sha256'] = 'f'*64
        payload = {key: value for key, value in pair.items() if key != 'pair_sha256'}
        pair['pair_sha256'] = hashlib.sha256(_canonical(payload)).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draft_pairs_no_update')
            db.execute('UPDATE draft_pairs SET pair_sha256=?, pair_json=? WHERE request_key=?',
                       (pair['pair_sha256'], _canonical(pair), 'old-pair'))
        self.assertEqual(self.repo.get('old-pair'), pair)
        state = self.repo.inspect('old-pair')
        self.assertEqual(state['status'], 'review_required')
        self.assertIn('历史窗口', state['reason'])

    def test_legal_unique_control_with_rehashed_content_must_replay(self):
        pair = deepcopy(self.repo.save_draft('changed-control', self.config, **self.bounds))
        ticket = pair['control']['tickets'][0]
        old = set(ticket['front'])
        ticket['front'] = sorted(ticket['front'][1:] + [next(v for v in range(1, 36) if v not in old)])
        payload = {key: value for key, value in pair.items() if key != 'pair_sha256'}
        pair['pair_sha256'] = hashlib.sha256(_canonical(payload)).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draft_pairs_no_update')
            db.execute('UPDATE draft_pairs SET pair_sha256=?,pair_json=? WHERE request_key=?',
                       (pair['pair_sha256'], _canonical(pair), 'changed-control'))
        self.assertEqual(self.repo.get('changed-control'), pair)
        result = self.repo.inspect('changed-control')
        self.assertEqual(result['status'], 'review_required')
        self.assertIn('重放', result['reason'])

    def test_different_generator_version_is_readable_but_not_a_ready_draft(self):
        pair = deepcopy(self.repo.save_draft('old-code', self.config, **self.bounds))
        pair['code_sha256'] = 'f'*64
        pair['pair_sha256'] = hashlib.sha256(_canonical({key: value for key, value in pair.items()
                                                         if key != 'pair_sha256'})).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draft_pairs_no_update')
            db.execute('UPDATE draft_pairs SET pair_sha256=?,pair_json=? WHERE request_key=?',
                       (pair['pair_sha256'], _canonical(pair), 'old-code'))
        self.assertEqual(self.repo.get('old-code'), pair)
        state = self.repo.inspect('old-code')
        self.assertEqual(state['status'], 'review_required')
        self.assertIn('无法完整重放', state['reason'])


if __name__ == '__main__':
    unittest.main()
