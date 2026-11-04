from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from dlt.candidates import _canonical
from dlt.integrity import check_database
from dlt.pair_repository import PairRepository
from dlt.prospective import ProspectiveRepository, RegistrationError
from dlt.snapshots import SnapshotStore
from test_candidates import config, input_rows
from test_settlement import fixture
from dlt.prize_rules import (DLT_2026_ADDITIONAL_GROUPS, DLT_2026_HIT_GROUPS,
                             DLT_2026_OFFICIAL_SORT_CODES)


def complete_current_rule(rule, payout_sort_map=None):
    """Keep prospective fixtures aligned with the complete current prize contract."""
    codes = sorted(DLT_2026_OFFICIAL_SORT_CODES)
    names = {'101': 'first', '201': 'first-extra', '301': 'second', '401': 'second-extra'}
    names.update({code: f'prize-{code}' for code in codes if code not in names})
    payout_sort_map = payout_sort_map or {names[code]: code for code in codes}
    code_by_group = {group: str(code) for group, code in payout_sort_map.items()}
    group_by_code = {code: group for group, code in code_by_group.items()}
    rule['hit_groups'] = {outcome: group_by_code.get(code) for outcome, code in DLT_2026_HIT_GROUPS.items()}
    rule['additional_groups'] = {group_by_code[base]: group_by_code[extra]
                                 for base, extra in DLT_2026_ADDITIONAL_GROUPS.items()}
    rule['payout_sort_map'] = payout_sort_map
    return rule


def swap_first_second_prize_codes(rule):
    mapping = dict(rule['payout_sort_map'])
    mapping['first'], mapping['second'] = mapping['second'], mapping['first']
    mapping['first-extra'], mapping['second-extra'] = mapping['second-extra'], mapping['first-extra']
    return complete_current_rule(rule, mapping)


class ProspectiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'db.sqlite3'
        real = datetime.now(timezone.utc)
        # Training fixtures create real subprocesses before their first frozen
        # record. Leave enough headroom under concurrent or slow test runs.
        self.now = real + timedelta(seconds=30)
        china = timezone(timedelta(hours=8))
        target_day = (self.now.astimezone(china)+timedelta(days=2)).date()
        self.deadline = datetime.combine(target_day, datetime.min.time(), tzinfo=china)+timedelta(hours=20)
        history = input_rows()
        for i, row in enumerate(history):
            row['date'] = (target_day-timedelta(days=18-i)).isoformat()
        self.source = {'success': True, 'errorCode': '0', 'value': {'list': [
            {'lotteryGameNum': '85', 'lotteryDrawNum': r['issue'], 'lotteryDrawTime': r['date'],
             'lotteryDrawResult': ' '.join(map(str, r['front']+r['back'])), 'prizeLevelList': []} for r in history]}}
        self.snapshot = SnapshotStore(self.path).import_official(json.dumps(self.source).encode(), 'synthetic fixture',
                                                               (real-timedelta(seconds=1)).isoformat())['snapshot_id']
        self.config = {**config(), 'snapshot_id': self.snapshot, 'target_date': target_day.isoformat(), 'budget_fen': 400}
        pair_repo = PairRepository(self.path)
        for i in (1, 2):
            pair_repo.save_draft(f'pair-{i}', {**self.config, 'seed': i}, history_first_issue='26001',
                                 history_cutoff_issue='26016')
        self.repo = ProspectiveRepository(self.path, clock=lambda: self.now)
        rule = complete_current_rule(fixture()['rule']); rule['version'] = self.config['rule_version']; rule['applicable_issues'] = ['26017']
        rule['source'] = 'https://www.sporttery.cn/synthetic-test'
        self.calendar = {'issue': '26017', 'draw_date': target_day.isoformat(), 'cutoff_at': self.deadline.isoformat(),
                         'version': self.config['calendar_version']}
        self.cal_id = self.review('calendar', self.calendar)
        self.rule_id = self.review('rule', rule)
        self.tax_id = self.review('tax', {'tax': rule['tax'], 'applicable_issues': ['26017']})
        self.protocol = {'hypothesis': 'synthetic test hypothesis', 'primary_metric': 'paired_net_difference',
                         'target_issue': '26017', 'observation_issues': ['26017'],
                         'replicas': [{'replica': 'a', 'pair_key': 'pair-1'}, {'replica': 'b', 'pair_key': 'pair-2'}],
                         'aggregation': 'arithmetic_mean_v1', 'exclusion_rule': 'record failed pairs',
                         'retraining_policy': 'none', 'selection_rule': 'fixed before observation',
                         'time_assurance': 'local_clock', 'internal_deadline': (self.deadline-timedelta(hours=1)).isoformat(),
                         'calendar_review_id': self.cal_id, 'rule_review_id': self.rule_id, 'tax_review_id': self.tax_id,
                         'other_winnings_assumption': 'no_other_same_issue_winnings'}

    def review(self, kind, extracted):
        return self.repo.record_source_review(kind=kind, source_url='https://www.sporttery.cn/synthetic-test',
                                              document=b'synthetic test document', extracted=extracted,
                                              reviewer_note='synthetic fixture; not an official document', confirmed=True)

    def test_freeze_preserves_pairs_and_idempotency_after_deadline(self):
        before = PairRepository(self.path).get('pair-1')
        result = self.repo.freeze('exp', self.protocol)
        self.assertEqual(result['pairs'][0]['pair'], before)
        self.assertEqual(result['status'], 'locally_attested')
        self.assertIn('not independently certified', result['time_evidence'])
        self.now = self.deadline + timedelta(days=1)
        self.assertEqual(self.repo.freeze('exp', self.protocol), result)
        with self.assertRaisesRegex(RegistrationError, 'different protocol'):
            self.repo.freeze('exp', {**self.protocol, 'hypothesis': 'changed'})
        self.assertEqual(ProspectiveRepository(self.path).get('exp'), result)
        with closing(sqlite3.connect(self.path)) as db, db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("DELETE FROM fixed_experiments WHERE request_key='exp'")

    def test_prepared_fixed_write_is_queryable_idempotent_and_fence_blocks_late_commit(self):
        intent = self.repo.prepare_write('pending-exp', self.protocol)
        self.assertEqual(intent['status'], 'unknown')
        self.assertEqual(self.repo.prepare_write('pending-exp', self.protocol), intent)
        self.assertEqual(self.repo.write_status('pending-exp', intent['request_sha256'])['status'], 'unknown')
        with self.assertRaisesRegex(RegistrationError, 'different input'):
            self.repo.prepare_write('pending-exp', {**self.protocol, 'hypothesis': 'different'})
        fenced = self.repo.fence_write('pending-exp', intent['request_sha256'])
        self.assertEqual(fenced['status'], 'fenced')
        self.assertEqual(self.repo.write_status('pending-exp', intent['request_sha256'])['status'], 'fenced')
        with self.assertRaisesRegex(RegistrationError, 'fenced'):
            self.repo.freeze('pending-exp', self.protocol, require_intent=True)
        with closing(sqlite3.connect(self.path)) as db, db:
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'fenced fixed experiment'):
                db.execute('INSERT INTO fixed_experiments VALUES (?, ?, ?, ?)',
                           ('pending-exp', _canonical(self.protocol), '0'*64, b'{}'))
        self.assertEqual(self.repo.write_status('pending-exp', intent['request_sha256'])['status'], 'fenced')
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_prepared_fixed_write_commit_returns_same_record_after_response_loss(self):
        intent = self.repo.prepare_write('prepared-exp', self.protocol)
        saved = self.repo.freeze('prepared-exp', self.protocol, require_intent=True)
        self.assertEqual(self.repo.write_status('prepared-exp', intent['request_sha256'])['status'], 'committed')
        self.assertEqual(self.repo.freeze('prepared-exp', self.protocol, require_intent=True), saved)
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_fixed_experiment_commit_and_fence_have_one_atomic_winner(self):
        intent = self.repo.prepare_write('racing-exp', self.protocol)
        barrier = threading.Barrier(2)
        def commit():
            barrier.wait()
            try:
                self.repo.freeze('racing-exp', self.protocol, require_intent=True)
                return 'committed'
            except RegistrationError:
                return 'rejected'
        def fence():
            barrier.wait()
            return self.repo.fence_write('racing-exp', intent['request_sha256'])['status']
        with ThreadPoolExecutor(max_workers=2) as pool:
            commit_future = pool.submit(commit)
            fence_future = pool.submit(fence)
            committed, fenced = commit_future.result(), fence_future.result()
        final = self.repo.write_status('racing-exp', intent['request_sha256'])['status']
        self.assertIn((committed, fenced, final), {
            ('committed', 'committed', 'committed'),
            ('rejected', 'fenced', 'fenced'),
        })
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_fixed_write_retry_must_follow_fenced_request_and_is_linked(self):
        first = self.repo.prepare_write('first-exp', self.protocol)
        with self.assertRaisesRegex(RegistrationError, 'fenced request'):
            self.repo.prepare_write('retry-exp', self.protocol, retry_of='first-exp')
        self.repo.fence_write('first-exp', first['request_sha256'])
        retry = self.repo.prepare_write('retry-exp', self.protocol, retry_of='first-exp')
        self.assertEqual(retry['status'], 'unknown')
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_missing_fixed_write_intent_remains_unknown_and_cannot_be_fenced(self):
        protocol = self.repo._protocol(self.protocol)
        digest = self.repo._write_digest(protocol, None)
        self.assertEqual(self.repo.write_status('lost-preparation', digest)['status'], 'unknown')
        with self.assertRaisesRegex(RegistrationError, 'preparation'):
            self.repo.fence_write('lost-preparation', digest)

    def test_new_registration_rejects_pair_from_unavailable_generator_version(self):
        import hashlib
        from dlt.candidates import _canonical
        pair = deepcopy(self.repo.pairs.get('pair-1'))
        pair['code_sha256'] = 'f'*64
        pair['pair_sha256'] = hashlib.sha256(_canonical({key: value for key, value in pair.items()
                                                         if key != 'pair_sha256'})).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draft_pairs_no_update')
            db.execute('UPDATE draft_pairs SET pair_sha256=?,pair_json=? WHERE request_key=?',
                       (pair['pair_sha256'], _canonical(pair), 'pair-1'))
        with self.assertRaisesRegex(RegistrationError, '无法重放'):
            self.repo.freeze('cannot-verify', self.protocol)

    def test_deadline_equality_rejected_and_no_partial_freeze(self):
        self.now = self.deadline-timedelta(hours=1)
        with self.assertRaisesRegex(RegistrationError, 'deadline'):
            self.repo.freeze('exp', self.protocol)
        with self.assertRaises(KeyError):
            self.repo.get('exp')

    def test_deadline_or_clock_changes_inside_transaction_are_rejected(self):
        initial = self.now
        times = iter([initial, self.deadline])
        repo = ProspectiveRepository(self.path, clock=lambda: next(times))
        with self.assertRaisesRegex(RegistrationError, 'deadline reached'):
            repo.freeze('exp', self.protocol)

    def test_target_outcome_existing_anywhere_blocks_backfill(self):
        changed = deepcopy(self.source)
        changed['value']['list'].append({'lotteryGameNum': '85', 'lotteryDrawNum': '26017',
                                        'lotteryDrawTime': self.calendar['draw_date'],
                                        'lotteryDrawResult': '01 02 03 04 05 01 02', 'prizeLevelList': []})
        SnapshotStore(self.path).import_official(json.dumps(changed).encode(), 'synthetic fixture', self.now.isoformat())
        with self.assertRaisesRegex(RegistrationError, 'target outcome'):
            self.repo.freeze('exp', self.protocol)

    def test_review_and_protocol_must_be_explicit_and_prices_match(self):
        with self.assertRaises(RegistrationError):
            self.repo.record_source_review(kind='calendar', source_url='https://example.com/x', document=b'x',
                                           extracted=self.calendar, reviewer_note='test', confirmed=True)
        with self.assertRaisesRegex(RegistrationError, 'local time evidence'):
            self.repo.freeze('exp', {**self.protocol, 'time_assurance': 'third_party'})
        without_tax = dict(self.protocol); del without_tax['tax_review_id']
        with self.assertRaisesRegex(RegistrationError, 'incomplete protocol'):
            self.repo.freeze('exp', without_tax)
        with self.assertRaisesRegex(RegistrationError, 'no-other-winnings'):
            self.repo.freeze('exp', {**self.protocol, 'other_winnings_assumption': 'unknown'})
        with self.assertRaisesRegex(RegistrationError, 'distinct replica'):
            self.repo.freeze('exp', {**self.protocol, 'replicas': [self.protocol['replicas'][0]]*2})
        wrong_rule = complete_current_rule(fixture()['rule'])
        wrong_rule['version'] = self.config['rule_version']
        wrong_rule['applicable_issues'] = ['26017']; wrong_rule['prices']['base'] = '3'
        wrong_rule['source'] = 'https://www.sporttery.cn/synthetic-test'
        rid = self.review('rule', wrong_rule)
        with self.assertRaisesRegex(RegistrationError, 'ticket prices must match'):
            self.repo.freeze('exp', {**self.protocol, 'rule_review_id': rid})

    def test_china_lottery_official_domain_is_allowed_but_lookalikes_are_not(self):
        for url in ('https://lottery.gov.cn/rule', 'https://www.lottery.gov.cn/rule',
                    'https://m.lottery.gov.cn/rule'):
            ident = self.repo.record_source_review(kind='rule', source_url=url, document=b'synthetic',
                                                   extracted={'fixture': True}, reviewer_note='local fixture', confirmed=True)
            self.assertEqual(len(ident), 64)
        for url in ('https://lottery.gov.cn.attacker.invalid/rule', 'https://m.lottery.gov.cn@attacker.invalid/rule',
                    'http://m.lottery.gov.cn/rule'):
            with self.assertRaises(RegistrationError):
                self.repo.record_source_review(kind='rule', source_url=url, document=b'synthetic',
                                               extracted={'fixture': True}, reviewer_note='local fixture', confirmed=True)

    def test_source_conflicts_mark_frozen_experiment_for_review(self):
        self.repo.freeze('exp', self.protocol)
        changed = deepcopy(self.source)
        changed['value']['list'][0]['lotteryDrawResult'] = '10 11 12 13 14 03 04'
        SnapshotStore(self.path).import_official(json.dumps(changed).encode(), 'fixture2', self.now.isoformat())
        self.assertEqual(self.repo.inspect('exp')['status'], 'review_required')
        self.assertEqual(self.repo.get('exp')['status'], 'locally_attested')

    def test_closing_retains_frozen_content(self):
        frozen = self.repo.freeze('exp', self.protocol)
        first = self.repo.close('exp', 'finished observation')
        self.assertEqual(self.repo.close('exp', 'ignored duplicate'), first)
        self.assertEqual(self.repo.inspect('exp')['status'], 'closed')
        self.assertEqual(self.repo.get('exp'), frozen)

    def test_replica_reuse_and_changed_method_configuration_are_rejected(self):
        pairs = PairRepository(self.path)
        pairs.save_draft('same-seed', {**self.config, 'seed': 1}, history_first_issue='26001', history_cutoff_issue='26016')
        manifest = [{'replica': 'a', 'pair_key': 'pair-1'}, {'replica': 'b', 'pair_key': 'same-seed'}]
        with self.assertRaisesRegex(RegistrationError, 'distinct pairs and seeds'):
            self.repo.freeze('exp', {**self.protocol, 'replicas': manifest})
        pairs.save_draft('other-method', {**self.config, 'seed': 3, 'window': 5},
                         history_first_issue='26001', history_cutoff_issue='26016')
        manifest[1]['pair_key'] = 'other-method'
        with self.assertRaisesRegex(RegistrationError, 'replicas must share'):
            self.repo.freeze('exp', {**self.protocol, 'replicas': manifest})


if __name__ == '__main__':
    unittest.main()
