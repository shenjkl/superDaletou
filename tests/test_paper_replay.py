from copy import deepcopy
from contextlib import closing
import base64
import hashlib
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from dlt.candidates import _canonical
from dlt.paper_replay import PaperReplayError, _clean_rule, build_paper_replay, verify_paper_replay
from dlt.app import Handler
from dlt.prospective_settlement import PaperResults
from test_settlement import fixture as settlement_fixture
import test_prospective_settlement as fixtures


class PaperReplayTests(unittest.TestCase):
    review = fixtures.PaperResultsTests.review
    import_draw = fixtures.PaperResultsTests.import_draw

    def setUp(self):
        fixtures.PaperResultsTests.setUp(self)
        self.secret = 'PRIVATE_TOKEN_X7y98z_secret'
        self.path_marker = r'C:\Users\private\local-secret.txt'
        self.draw['arbitraryNote'] = self.secret + ' ' + self.path_marker + ' https://private.invalid/data'
        self.snapshot = self.import_draw(self.draw)
        self.paper.process_one('exp', self.snapshot)

    def test_export_is_whitelisted_and_independently_recalculates(self):
        with closing(sqlite3.connect(self.path)) as db:
            raw = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?',
                             (self.snapshot, '26017')).fetchone()[0]
        self.assertIn(self.secret.encode(), raw)
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        bundle = build_paper_replay(self.path, 'exp', clock=lambda: self.now)
        result = verify_paper_replay(bundle)
        encoded = _canonical(bundle)
        self.assertEqual(result['status'], 'matched')
        self.assertTrue(result['calculator_matches_export'])
        self.assertEqual(result['replicas'], 2)
        self.assertNotIn(self.secret.encode(), encoded)
        self.assertNotIn(self.path_marker.encode(), encoded)
        self.assertNotIn(b'https://', encoded)
        self.assertNotIn(b'source_url', encoded)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        for key in bundle['payouts']:
            self.assertRegex(key, r'^g\d+$')

    def test_calculator_version_mismatch_is_explicit_not_a_false_version_match(self):
        bundle = build_paper_replay(self.path, 'exp', clock=lambda: self.now)
        other = self.path.with_name('different-calculator.py')
        other.write_text('# different calculator', encoding='utf-8')
        with patch('dlt.paper_replay.settlement_module.__file__', str(other)):
            result = verify_paper_replay(bundle)
        self.assertEqual(result['status'], 'matched')
        self.assertFalse(result['calculator_matches_export'])

    def test_cli_verifies_file_and_detects_tampering(self):
        bundle = build_paper_replay(self.path, 'exp', clock=lambda: self.now)
        file = self.path.with_name('replay.json')
        file.write_bytes(_canonical(bundle))
        run = subprocess.run([sys.executable, '-m', 'dlt.paper_replay', str(file)], capture_output=True, text=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(json.loads(run.stdout)['status'], 'matched')
        bundle['replicas'][0]['candidate']['expected']['gross'] = '999'
        with self.assertRaisesRegex(PaperReplayError, '摘要'):
            verify_paper_replay(bundle)
        body = {key: value for key, value in bundle.items() if key != 'bundle_sha256'}
        bundle['bundle_sha256'] = hashlib.sha256(_canonical(body)).hexdigest()
        with self.assertRaisesRegex(PaperReplayError, '不一致'):
            verify_paper_replay(bundle)
        file.write_text('{"format":"one","format":"two"}', encoding='utf-8')
        failed = subprocess.run([sys.executable, '-m', 'dlt.paper_replay', str(file)], capture_output=True, text=True)
        self.assertEqual(failed.returncode, 2)
        self.assertIn('重复字段', json.loads(failed.stdout)['error'])

    def test_independent_hand_calculated_additional_and_tax_example(self):
        hits = {f'{front},{back}': None for front in range(6) for back in range(3)}
        hits['5,2'] = 'g0'
        winner = {'front': [1, 2, 3, 4, 5], 'back': [1, 2], 'additional': True, 'multiplier': 2}
        loser = {'front': [6, 7, 8, 9, 10], 'back': [3, 4], 'additional': False, 'multiplier': 1}
        body = {'format': 'dlt-paper-settlement-replay-v1', 'issue': '26123',
                'draw': {'issue': '26123', 'front': [1, 2, 3, 4, 5], 'back': [1, 2], 'promotion': False},
                'rule': {'hit_groups': hits, 'additional_groups': {'g0': 'g1'},
                         'prices': {'base': '2', 'additional': '3'},
                         'tax': {'threshold': '10000', 'rate': '0.2', 'quantum': '0.01',
                                 'comparison': 'gt', 'rounding': 'down'}},
                'payouts': {'g0': '6000', 'g1': '1000'},
                'assumptions': {'alternatives': 'separate_no_other_same_issue_winnings',
                                'payout': 'fixed_published_amounts_without_added_winner_adjustment',
                                'provenance': 'local_evidence_not_independently_authenticated'},
                'replicas': [{'candidate': {'tickets': [winner], 'expected': {'gross': '14000', 'tax': '2800',
                              'cost': '6', 'net': '11194', 'winning_tickets': 2}},
                              'control': {'tickets': [loser], 'expected': {'gross': '0', 'tax': '0',
                              'cost': '2', 'net': '-2', 'winning_tickets': 0}}, 'difference_yuan': '11196'}],
                'mean_difference_fraction': [11196, 1],
                'source_hashes': {key: 'a'*64 for key in ('experiment', 'result', 'snapshot', 'draw_row',
                                  'workflow_at_settlement', 'calculator_at_export')}}
        bundle = {**body, 'bundle_sha256': hashlib.sha256(_canonical(body)).hexdigest()}
        result = verify_paper_replay(bundle)
        self.assertEqual((result['status'], result['replicas'], result['tickets']), ('matched', 1, 2))
        self.assertFalse(result['calculator_matches_export'])
        self.assertEqual(result['tax_unit_evidence'], 'legacy_implicit_calculation_only')

    def test_extra_fields_and_invalid_data_fail_closed(self):
        bundle = build_paper_replay(self.path, 'exp', clock=lambda: self.now)
        for mutate in ('extra', 'mapping', 'fraction', 'assumption'):
            altered = deepcopy(bundle)
            if mutate == 'extra':
                altered['replicas'][0]['candidate']['tickets'][0]['token'] = self.secret
            elif mutate == 'mapping':
                altered['rule']['hit_groups']['0,0'] = {'not': 'a group'}
            elif mutate == 'fraction':
                altered['mean_difference_fraction'] = [True, 1]
            else:
                altered['assumptions']['provenance'] = 'officially-certified'
            body = {key: value for key, value in altered.items() if key != 'bundle_sha256'}
            altered['bundle_sha256'] = hashlib.sha256(_canonical(body)).hexdigest()
            with self.subTest(mutate=mutate), self.assertRaises(PaperReplayError):
                verify_paper_replay(altered)

    def test_rule_labels_and_free_text_are_replaced_by_aliases(self):
        values = settlement_fixture()
        rule, payouts = values['rule'], values['payouts']
        original = 'first'
        rule['hit_groups'] = {key: self.secret if value == original else value for key, value in rule['hit_groups'].items()}
        rule['additional_groups'] = {self.secret if key == original else key: value
                                     for key, value in rule['additional_groups'].items()}
        payouts[self.secret] = payouts.pop(original)
        rule['source'] = self.path_marker
        clean, clean_payouts = _clean_rule(rule, payouts)
        encoded = _canonical({'rule': clean, 'payouts': clean_payouts})
        self.assertNotIn(self.secret.encode(), encoded)
        self.assertNotIn(self.path_marker.encode(), encoded)

    def test_export_route_requires_session_origin_and_releases_lock(self):
        lock = threading.Lock()
        def call(*, origin='http://127.0.0.1:8766', csrf='secret'):
            encoded = json.dumps({'experiment_key': 'exp'}).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret',
                                             db_path=self.path, replay_export_lock=lock)
            handler.path = '/api/paper-replay'
            handler.headers = {'Origin': origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': csrf,
                               'Content-Type': 'application/json', 'Content-Length': str(len(encoded))}
            handler.rfile = BytesIO(encoded)
            handler._validate_host = lambda: True
            handler._session = lambda: True
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        self.assertEqual(call(origin='https://example.invalid')[0], 403)
        self.assertEqual(call(csrf='wrong')[0], 403)
        self.assertTrue(lock.acquire(blocking=False))
        self.assertEqual(call()[0], 409)
        lock.release()
        with patch('dlt.app.build_paper_replay', side_effect=lambda path, key: build_paper_replay(path, key, clock=lambda: self.now)):
            status, response = call()
        self.assertEqual(status, 200)
        raw = base64.b64decode(response['package_base64'], validate=True)
        self.assertNotIn(self.secret.encode(), raw)
        self.assertEqual(verify_paper_replay(json.loads(raw))['status'], 'matched')
        self.assertFalse(lock.locked())
        with patch('dlt.app.build_paper_replay', side_effect=PaperReplayError('需要复核')):
            self.assertEqual(call()[0], 400)
        self.assertFalse(lock.locked())

    def test_source_conflict_prevents_export_and_does_not_modify_existing_result(self):
        group_before = self.paper.inspect_current('exp')['group_id']
        changed = deepcopy(self.draw)
        changed['prizeLevelList'][0]['stakeAmount'] = '1'
        self.import_draw(changed)
        with self.assertRaisesRegex(PaperReplayError, '复核'):
            build_paper_replay(self.path, 'exp', clock=lambda: self.now)
        self.assertEqual(self.paper.get_group(group_before)['issue'], '26017')

    def test_export_requires_original_observation_and_a_nonfuture_clock(self):
        with self.assertRaisesRegex(PaperReplayError, '观察时间'):
            build_paper_replay(self.path, 'exp', clock=lambda: self.deadline)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER observations_no_delete')
            db.execute('DELETE FROM observations WHERE snapshot_id=?', (self.snapshot,))
        with self.assertRaisesRegex(PaperReplayError, '观察记录缺失'):
            build_paper_replay(self.path, 'exp', clock=lambda: self.now)

    def test_damaged_stored_snapshot_blocks_export_even_when_raw_observation_survives(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER snapshots_no_update')
            db.execute('UPDATE snapshots SET canonical_json=? WHERE id=?', (b'{}', self.snapshot))
        with self.assertRaisesRegex(PaperReplayError, '观察与当前结果不一致'):
            build_paper_replay(self.path, 'exp', clock=lambda: self.now)

    def test_rehashed_but_inconsistent_stored_result_is_not_exported(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER paper_groups_no_update')
            db.execute('DROP TRIGGER paper_publications_no_update')
            old, raw = db.execute('SELECT id,content_json FROM paper_groups').fetchone()
            group = json.loads(raw)
            group['replicas'][0]['candidate']['gross'] = '1'
            encoded = _canonical(group)
            new = hashlib.sha256(encoded).hexdigest()
            db.execute('UPDATE paper_groups SET id=?,content_json=? WHERE id=?', (new, encoded, old))
            db.execute('UPDATE paper_current SET group_id=? WHERE group_id=?', (new, old))
            db.execute('UPDATE paper_publications SET group_id=? WHERE group_id=?', (new, old))
        with self.assertRaisesRegex(PaperReplayError, '重算金额不一致'):
            build_paper_replay(self.path, 'exp', clock=lambda: self.now)

    def test_export_reads_one_snapshot_when_source_conflicts_concurrently(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('PRAGMA journal_mode=WAL')
        original = PaperResults.inspect_current_in_transaction
        def inspect_then_conflict(paper, db, key):
            state = original(paper, db, key)
            changed = deepcopy(self.draw)
            changed['prizeLevelList'][0]['stakeAmount'] = '1'
            self.import_draw(changed)
            return state
        with patch.object(PaperResults, 'inspect_current_in_transaction', autospec=True, side_effect=inspect_then_conflict):
            bundle = build_paper_replay(self.path, 'exp', clock=lambda: self.now)
        self.assertEqual(verify_paper_replay(bundle)['status'], 'matched')
        with self.assertRaisesRegex(PaperReplayError, '复核'):
            build_paper_replay(self.path, 'exp', clock=lambda: self.now)

    def test_waiting_or_missing_experiment_cannot_export(self):
        with self.assertRaises(PaperReplayError):
            build_paper_replay(self.path, 'missing', clock=lambda: self.now)
        missing_file = self.path.with_name('missing.sqlite3')
        with self.assertRaises(PaperReplayError):
            build_paper_replay(missing_file, 'exp')
        self.assertFalse(missing_file.exists())
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DELETE FROM paper_current WHERE experiment_key=?', ('exp',))
        with self.assertRaisesRegex(PaperReplayError, '不可用'):
            build_paper_replay(self.path, 'exp', clock=lambda: self.now)


if __name__ == '__main__':
    unittest.main()
