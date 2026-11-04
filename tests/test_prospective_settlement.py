from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from io import BytesIO
import json
import hashlib
import sqlite3
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dlt.pair_repository import PairRepository
from dlt.candidates import _canonical
from dlt.prospective import RegistrationError
from dlt.prospective_settlement import PaperResultError, PaperResults
from dlt.scheduler import Scheduler
from dlt.snapshots import SnapshotStore
from test_scheduler import configuration
import test_prospective as fixtures


OFFICIAL_ENDPOINT = 'https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry?gameNo=85&provinceId=0&pageSize=20&isVerify=1&pageNo=1'


class PaperResultsTests(unittest.TestCase):
    review = fixtures.ProspectiveTests.review

    def setUp(self):
        fixtures.ProspectiveTests.setUp(self)
        self.frozen = self.repo.freeze('exp', self.protocol)
        self.paper = PaperResults(self.path, clock=lambda: self.now)
        self.now = self.deadline + timedelta(hours=2)
        ticket = PairRepository(self.path).get('pair-1')['candidate']['tickets'][0]
        numbers = ' '.join(map(str, ticket['front']+ticket['back']))
        self.draw = {'lotteryGameNum': '85', 'lotteryDrawNum': '26017', 'lotteryDrawTime': self.calendar['draw_date'],
                     'lotteryDrawResult': numbers, 'lotteryPromotionFlag': 0, 'verify': 1,
                     'prizeLevelList': [{'sort': 101, 'stakeAmount': '6000'}, {'sort': 201, 'stakeAmount': '100'},
                                        {'sort': 301, 'stakeAmount': '5000'}, {'sort': 401, 'stakeAmount': '200'}]}

    def import_draw(self, row, *, source=OFFICIAL_ENDPOINT, observed=None):
        payload = {'success': True, 'errorCode': '0', 'value': {'list': [row]}}
        return SnapshotStore(self.path).import_official(json.dumps(payload).encode(), source,
                                                       (observed or (self.now-timedelta(minutes=1))).isoformat())['snapshot_id']

    def resolve(self, snapshot_id):
        review = self.paper.inspect_draw_versions('26017')
        variant = next(item for item in review['variants']
                       if any(s['snapshot_id'] == snapshot_id for s in item['snapshots']))
        return self.paper.resolve_draw('26017', snapshot_id, expected_variants_sha256=review['variants_sha256'],
                                       expected_row_sha256=variant['row_sha256'],
                                       note='synthetic correction reviewed', confirmed=True)

    def test_complete_idempotent_pair_groups_and_close(self):
        self.repo.close('exp', 'closed to new registrations')
        sid = self.import_draw(self.draw)
        first = self.paper.process_one('exp', sid)
        second = self.paper.process_one('exp', sid)
        self.assertEqual(first['status'], 'complete')
        self.assertEqual(first['group_id'], second['group_id'])
        self.assertEqual(len(first['result']['replicas']), 2)
        self.assertEqual(first['result']['source_snapshot_id'], sid)
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'locally_attested')
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM paper_groups').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM paper_publications').fetchone()[0], 1)

    def test_inconsistent_fixed_request_withdraws_current_result_and_blocks_recalculation(self):
        snapshot = self.import_draw(self.draw)
        first = self.paper.process_one('exp', snapshot)
        self.assertEqual(first['status'], 'complete')
        changed = deepcopy(self.frozen['protocol'])
        changed['hypothesis'] = 'different request'
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_experiments_no_update')
            db.execute("UPDATE fixed_experiments SET request_json=? WHERE request_key='exp'", (_canonical(changed),))
            db.execute("CREATE TRIGGER fixed_experiments_no_update BEFORE UPDATE ON fixed_experiments BEGIN SELECT RAISE(ABORT, 'immutable fixed experiment'); END")
        self.assertEqual(self.repo.get('exp'), self.frozen)
        self.assertEqual(self.repo.inspect('exp')['status'], 'review_required')
        current = self.paper.inspect_current('exp')
        self.assertEqual(current['status'], 'review_required')
        self.assertIsNone(current['result'])
        self.assertEqual(self.paper.process_one('exp', snapshot)['status'], 'blocked')
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT group_id FROM paper_current WHERE experiment_key='exp'").fetchone()[0], first['group_id'])
            self.assertEqual(db.execute("SELECT count(*) FROM paper_publications WHERE experiment_key='exp'").fetchone()[0], 1)

    def test_old_fixed_result_survives_when_generation_code_is_unavailable(self):
        snapshot = self.import_draw(self.draw)
        saved = self.paper.process_one('exp', snapshot)
        self.assertEqual(saved['status'], 'complete')
        other = self.path.with_name('different_candidate_version.py')
        other.write_text('# another version', encoding='utf-8')
        with patch('dlt.pair_validation.candidate_module.__file__', str(other)):
            self.assertEqual(self.paper.inspect_current('exp')['status'], 'locally_attested')
            self.assertEqual(PairRepository(self.path).inspect('pair-1')['status'], 'review_required')

    def test_large_paper_payout_keeps_exact_replica_differences(self):
        draw = deepcopy(self.draw)
        draw['prizeLevelList'][0]['stakeAmount'] = '12345678901234567890123456.78'
        outcome = self.paper.process_one('exp', self.import_draw(draw))
        self.assertEqual(outcome['status'], 'complete')
        for replica in outcome['result']['replicas']:
            for label in ('candidate', 'control'):
                row = replica[label]
                self.assertEqual(Fraction(Decimal(row['net'])),
                                 Fraction(Decimal(row['gross'])) - Fraction(Decimal(row['tax'])) - Fraction(Decimal(row['cost'])))
            self.assertEqual(Fraction(Decimal(replica['difference_yuan'])),
                             Fraction(Decimal(replica['candidate']['net'])) - Fraction(Decimal(replica['control']['net'])))

    def _install_legacy_group(self, result, transform):
        legacy = deepcopy(result['result'])
        legacy.pop('calculation_version', None)
        legacy.pop('settlement_code_sha256', None)
        legacy['code_sha256'] = '0' * 64
        transform(legacy)
        encoded = _canonical(legacy); ident = hashlib.sha256(encoded).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER paper_groups_no_update')
            db.execute('DROP TRIGGER paper_publications_no_update')
            db.execute('UPDATE paper_groups SET id=?,content_json=? WHERE id=?', (ident, encoded, result['group_id']))
            db.execute('UPDATE paper_current SET group_id=? WHERE experiment_key=?', (ident, 'exp'))
            db.execute('UPDATE paper_publications SET group_id=? WHERE experiment_key=?', (ident, 'exp'))
        return ident, legacy

    def test_legacy_precise_result_is_rechecked_and_reused_idempotently(self):
        sid = self.import_draw(self.draw)
        original = self.paper.process_one('exp', sid)
        legacy_id, _ = self._install_legacy_group(original, lambda _: None)
        current = self.paper.inspect_current('exp')
        self.assertEqual((current['status'], current['group_id']), ('locally_attested', legacy_id))
        self.assertEqual(self.paper.process_one('exp', sid)['group_id'], legacy_id)
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 1)

    def test_legacy_wrong_amount_is_withdrawn_then_versioned_on_recalculation(self):
        draw = deepcopy(self.draw)
        draw['prizeLevelList'][0]['stakeAmount'] = '12345678901234567890123456.78'
        sid = self.import_draw(draw)
        original = self.paper.process_one('exp', sid)
        def introduce_old_rounding(legacy):
            legacy['replicas'][0]['candidate']['gross'] = '12345678901234567890123457'
        legacy_id, legacy = self._install_legacy_group(original, introduce_old_rounding)
        current = self.paper.inspect_current('exp')
        self.assertEqual(current['status'], 'review_required')
        self.assertIsNone(current['result'])
        self.assertIn('重新计算', current['reason'])
        updated = self.paper.process_one('exp', sid)
        self.assertEqual(updated['status'], 'complete')
        self.assertNotEqual(updated['group_id'], legacy_id)
        self.assertEqual(self.paper.get_group(legacy_id), legacy)
        self.assertEqual(self.paper.inspect_current('exp')['group_id'], updated['group_id'])
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 2)
        self.assertEqual(self.paper.process_one('exp', sid)['group_id'], updated['group_id'])

    def test_recalculate_route_accepts_single_version_and_rejects_unreviewed_conflict(self):
        from dlt.app import Handler
        sid = self.import_draw(self.draw)
        def call(snapshot, *, csrf='secret', origin='http://127.0.0.1:8766'):
            body = json.dumps({'issue': '26017', 'snapshot_id': snapshot}).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.path)
            handler.path = '/api/paper-results/recalculate-issue'
            handler.headers = {'Origin': origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': csrf,
                               'Content-Type': 'application/json', 'Content-Length': str(len(body))}
            handler.rfile = BytesIO(body)
            handler._validate_host = lambda: True
            handler._session = lambda: True
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            with patch('dlt.app.PaperResults', return_value=self.paper):
                return handler.do_POST()
        self.assertEqual(call(sid, csrf='wrong')[0], 403)
        self.assertEqual(call(sid, origin='https://example.invalid')[0], 403)
        self.assertEqual(call('0' * 64)[0], 400)
        status, response = call(sid)
        self.assertEqual(status, 200)
        self.assertEqual(response['recalculations']['exp']['status'], 'complete')
        changed = deepcopy(self.draw); changed['prizeLevelList'][0]['stakeAmount'] = '7000'
        newer = self.import_draw(changed)
        self.assertEqual(call(sid)[0], 400)
        self.resolve(newer)
        self.assertEqual(call(sid)[0], 400)
        self.assertEqual(call(newer)[1]['recalculations']['exp']['status'], 'complete')

    def test_missing_payout_and_promotion_block_without_result(self):
        draw = deepcopy(self.draw); draw['prizeLevelList'].pop()
        sid = self.import_draw(draw)
        result = self.paper.process_one('exp', sid)
        self.assertEqual(result['status'], 'blocked')
        self.assertIn('payout is missing', result['reason'])
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'blocked')

    def test_conflict_review_correction_replaces_current_not_history(self):
        original = self.import_draw(self.draw)
        old = self.paper.process_one('exp', original)
        corrected = deepcopy(self.draw); corrected['prizeLevelList'][0]['stakeAmount'] = '7000'
        new = self.import_draw(corrected)
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        self.assertEqual(self.paper.process_one('exp', new)['status'], 'blocked')
        self.resolve(new)
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        replacement = self.paper.process_one('exp', new)
        self.assertEqual(replacement['status'], 'complete')
        self.assertNotEqual(old['group_id'], replacement['group_id'])
        self.assertEqual(self.paper.inspect_current('exp')['group_id'], replacement['group_id'])
        self.assertEqual(self.paper.get_group(old['group_id']), old['result'])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM paper_groups').fetchone()[0], 2)
            history = db.execute('SELECT previous_group, group_id FROM paper_publications ORDER BY seq').fetchall()
            self.assertEqual(history[-1], (old['group_id'], replacement['group_id']))

    def test_unofficial_or_early_source_is_blocked(self):
        sid = self.import_draw(self.draw, source='https://example.com/not-official')
        self.assertIn('no consistent official-source', self.paper.process_one('exp', sid)['reason'])
        # Same content may later acquire a qualifying official observation.
        self.import_draw(self.draw, observed=self.deadline-timedelta(seconds=1))
        self.assertEqual(self.paper.process_one('exp', sid)['status'], 'blocked')
        self.import_draw(self.draw)
        self.assertEqual(self.paper.process_one('exp', sid)['status'], 'complete')

    def test_promotion_is_blocked_even_if_rule_claims_included(self):
        draw = deepcopy(self.draw); draw['lotteryPromotionFlag'] = 1
        sid = self.import_draw(draw)
        self.assertIn('promotion eligibility', self.paper.process_one('exp', sid)['reason'])

    def test_unverified_draw_is_blocked(self):
        draw = deepcopy(self.draw); draw['verify'] = 0
        sid = self.import_draw(draw)
        self.assertIn('verification flag', self.paper.process_one('exp', sid)['reason'])

    def test_background_update_followup_recovers_after_worker_restart(self):
        scheduler = Scheduler(self.path, clock=lambda: self.now)
        cfg = {**configuration(), 'start_at': (self.now-timedelta(seconds=5)).isoformat()}
        plan = scheduler.create_plan(cfg)
        scheduler.set_enabled(plan, True)
        claim = scheduler.claim(plan)
        payload = json.dumps({'success': True, 'errorCode': '0', 'value': {'list': [self.draw]}}).encode()
        def updater(store, *, transaction_guard, **kwargs):
            return store.import_official(payload, OFFICIAL_ENDPOINT, (self.now-timedelta(minutes=1)).isoformat(),
                                         transaction_guard=transaction_guard)
        scheduler.execute(claim, updater=updater)
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'waiting')
        restarted = Scheduler(self.path, clock=lambda: self.now)
        restarted.process_pending_followups()
        result = self.paper.inspect_current('exp')
        self.assertEqual(result['status'], 'locally_attested')
        run = restarted.list_runs(plan)[0]
        self.assertEqual(run['followups'][0]['status'], 'complete')
        self.assertEqual(run['followups'][0]['details']['exp']['status'], 'complete')
        restarted.process_pending_followups()
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM paper_groups').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM update_followups').fetchone()[0], 1)

    def test_update_catches_up_from_retained_older_snapshot_without_duplicate_result(self):
        older = self.import_draw(self.draw)
        newer_draw = deepcopy(self.draw)
        newer_draw['lotteryDrawNum'] = '26018'
        latest = self.import_draw(newer_draw)
        results = self.paper.process_update_and_catchup(latest)
        self.assertEqual(results['exp']['status'], 'complete')
        self.assertEqual(results['exp']['result']['source_snapshot_id'], older)
        self.assertNotIn('exp', self.paper.process_update_and_catchup(latest))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM paper_groups').fetchone()[0], 1)

    def test_update_reports_missing_older_target_and_does_not_treat_future_as_missing(self):
        newer_draw = deepcopy(self.draw)
        newer_draw['lotteryDrawNum'] = '26018'
        latest = self.import_draw(newer_draw)
        self.now = self.deadline - timedelta(minutes=1)
        self.assertEqual(self.paper.process_update_and_catchup(latest), {})
        self.now = self.deadline + timedelta(hours=2)
        outcome = self.paper.process_update_and_catchup(latest)['exp']
        self.assertEqual((outcome['status'], outcome['issue']), ('waiting_source', '26017'))
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'waiting')

    def test_catchup_still_rejects_unofficial_or_early_observation(self):
        self.import_draw(self.draw, source='https://example.com/not-official')
        self.import_draw(self.draw, observed=self.deadline-timedelta(seconds=1))
        newer_draw = deepcopy(self.draw)
        newer_draw['lotteryDrawNum'] = '26018'
        latest = self.import_draw(newer_draw)
        self.assertEqual(self.paper.process_update_and_catchup(latest)['exp']['status'], 'blocked')
        self.import_draw(self.draw)
        self.assertEqual(self.paper.process_update_and_catchup(latest)['exp']['status'], 'complete')

    def test_catchup_does_not_hide_existing_result_with_new_conflict(self):
        original = self.import_draw(self.draw)
        self.assertEqual(self.paper.process_one('exp', original)['status'], 'complete')
        changed = deepcopy(self.draw)
        changed['prizeLevelList'][0]['stakeAmount'] = '7000'
        self.import_draw(changed)
        newer = deepcopy(self.draw)
        newer['lotteryDrawNum'] = '26018'
        latest = self.import_draw(newer)
        outcome = self.paper.process_update_and_catchup(latest)['exp']
        self.assertEqual(outcome['status'], 'blocked')
        self.assertIn('conflicting', outcome['reason'])
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')

    def test_update_isolates_damaged_registration_and_keeps_valid_result(self):
        latest = self.import_draw(self.draw)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('INSERT INTO fixed_experiments VALUES (?, ?, ?, ?)',
                       ('damaged', b'{}', 'not-a-digest', b'{}'))
        outcomes = self.paper.process_update_and_catchup(latest)
        self.assertEqual(outcomes['exp']['status'], 'complete')
        self.assertEqual(outcomes['damaged']['status'], 'error')

    def test_background_followup_marks_missing_target_incomplete(self):
        scheduler = Scheduler(self.path, clock=lambda: self.now)
        plan = scheduler.create_plan({**configuration(), 'start_at': (self.now-timedelta(seconds=5)).isoformat()})
        scheduler.set_enabled(plan, True)
        claim = scheduler.claim(plan)
        newer_draw = deepcopy(self.draw)
        newer_draw['lotteryDrawNum'] = '26018'
        payload = json.dumps({'success': True, 'errorCode': '0', 'value': {'list': [newer_draw]}}).encode()
        def updater(store, *, transaction_guard, **kwargs):
            return store.import_official(payload, OFFICIAL_ENDPOINT, self.now.isoformat(), transaction_guard=transaction_guard)
        scheduler.execute(claim, updater=updater)
        scheduler.process_pending_followups()
        followup = scheduler.list_runs(plan)[0]['followups'][0]
        self.assertEqual(followup['status'], 'incomplete')
        self.assertEqual(followup['details']['exp']['status'], 'waiting_source')

    def test_background_followup_uses_retained_snapshot_for_missing_latest_target(self):
        old_snapshot = self.import_draw(self.draw)
        scheduler = Scheduler(self.path, clock=lambda: self.now)
        plan = scheduler.create_plan({**configuration(), 'start_at': (self.now-timedelta(seconds=5)).isoformat()})
        scheduler.set_enabled(plan, True)
        claim = scheduler.claim(plan)
        newer_draw = deepcopy(self.draw)
        newer_draw['lotteryDrawNum'] = '26018'
        payload = json.dumps({'success': True, 'errorCode': '0', 'value': {'list': [newer_draw]}}).encode()
        def updater(store, *, transaction_guard, **kwargs):
            return store.import_official(payload, OFFICIAL_ENDPOINT, self.now.isoformat(), transaction_guard=transaction_guard)
        scheduler.execute(claim, updater=updater)
        scheduler.process_pending_followups()
        followup = scheduler.list_runs(plan)[0]['followups'][0]
        self.assertEqual(followup['status'], 'complete')
        self.assertEqual(followup['details']['exp']['status'], 'complete')
        self.assertEqual(self.paper.inspect_current('exp')['result']['source_snapshot_id'], old_snapshot)

    def test_failed_publication_rolls_back_whole_pair_group(self):
        sid = self.import_draw(self.draw)
        self.paper.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("CREATE TRIGGER injected_failure BEFORE INSERT ON paper_current BEGIN SELECT RAISE(ABORT, 'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.paper.process_one('exp', sid)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM paper_groups').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT count(*) FROM paper_current').fetchone()[0], 0)

    def test_correction_missing_prize_remains_review_required(self):
        old_id = self.import_draw(self.draw)
        old = self.paper.process_one('exp', old_id)
        correction = deepcopy(self.draw); correction['prizeLevelList'].pop()
        new_id = self.import_draw(correction)
        self.resolve(new_id)
        self.assertEqual(self.paper.process_one('exp', new_id)['status'], 'blocked')
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        self.assertEqual(self.paper.get_group(old['group_id']), old['result'])

    def test_review_rejects_unseen_variant_or_changed_selected_row(self):
        self.import_draw(self.draw)
        second = deepcopy(self.draw); second['prizeLevelList'][0]['stakeAmount'] = '7000'
        selected = self.import_draw(second)
        review = self.paper.inspect_draw_versions('26017')
        self.assertEqual(len(review['variants']), 2)
        variant = next(x for x in review['variants'] if any(s['snapshot_id'] == selected for s in x['snapshots']))
        observation = variant['snapshots'][0]['observations'][0]
        self.assertTrue(observation['official_url_format'])
        self.assertTrue(observation['bytes_and_row_match'])
        with self.assertRaisesRegex(PaperResultError, 'selected row differs'):
            self.paper.resolve_draw('26017', selected, expected_variants_sha256=review['variants_sha256'],
                                    expected_row_sha256='0'*64, note='reviewed', confirmed=True)
        third = deepcopy(self.draw); third['prizeLevelList'][0]['stakeAmount'] = '8000'
        self.import_draw(third)
        with self.assertRaisesRegex(PaperResultError, 'changed since review'):
            self.paper.resolve_draw('26017', selected, expected_variants_sha256=review['variants_sha256'],
                                    expected_row_sha256=variant['row_sha256'], note='reviewed', confirmed=True)
        self.assertIsNone(self.paper.inspect_draw_versions('26017')['resolution'])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM paper_resolutions').fetchone()[0], 0)

    def test_unexpected_recalculation_failure_is_recorded_and_not_waiting(self):
        sid = self.import_draw(self.draw)
        with patch.object(self.paper, 'process_one', side_effect=RuntimeError('injected failure')):
            outcome = self.paper.process_issue('26017', sid)
        self.assertEqual(outcome['exp']['status'], 'error')
        self.assertTrue(outcome['exp']['recorded'])
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'blocked')
        self.assertIn('injected failure', self.paper.inspect_current('exp')['reason'])
        self.assertEqual(self.paper.list_history('exp')['events'][-1]['status'], 'error')

    def test_invalid_stored_draw_is_rejected_during_review(self):
        sid = self.import_draw(self.draw)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER draws_no_update')
            db.execute('UPDATE draw_versions SET row_json=? WHERE snapshot_id=? AND issue=?',
                       ('not a canonical byte record', sid, '26017'))
        with self.assertRaisesRegex(PaperResultError, 'stored draw record is invalid'):
            self.paper.inspect_draw_versions('26017')

    def test_rule_correction_invalidates_then_recalculates_same_draw(self):
        sid = self.import_draw(self.draw)
        old = self.paper.process_one('exp', sid)
        corrected = deepcopy(self.frozen['rule_review']['extracted'])
        corrected['version'] = 'corrected-rule-v2'
        corrected['payout_sort_map'] = {'first': '301', 'first-extra': '401', 'second': '101', 'second-extra': '201'}
        new_id = self.review('rule', corrected)
        resolution = self.repo.declare_source_correction(self.rule_id, new_id, note='corrected mapping', confirmed=True)
        self.assertEqual(resolution['kind'], 'rule')
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        result = self.paper.process_one('exp', sid)
        self.assertEqual(result['status'], 'complete')
        self.assertNotEqual(old['group_id'], result['group_id'])
        self.assertEqual(result['result']['original_rule_review_id'], self.rule_id)
        self.assertEqual(result['result']['rule_review_id'], new_id)
        self.assertEqual(result['result']['rule_correction_chain'], [self.rule_id, new_id])
        self.assertEqual(result['result']['payouts']['first'], '5000')
        self.assertEqual(self.paper.get_group(old['group_id']), old['result'])
        self.assertEqual(self.repo.get('exp'), self.frozen)
        self.assertEqual(self.paper.inspect_current('exp')['group_id'], result['group_id'])
        self.assertEqual(self.paper.process_one('exp', sid)['group_id'], result['group_id'])
        history = self.paper.list_history('exp')['publications']
        self.assertEqual(len(history), 2)
        self.assertEqual(history[-1]['previous_group'], old['group_id'])
        with self.assertRaisesRegex(RegistrationError, 'superseded'):
            self.repo.freeze('new-experiment', self.protocol)
        self.assertEqual(self.repo.freeze('exp', self.protocol), self.frozen)

    def test_tax_and_rule_corrections_must_agree_before_new_result(self):
        sid = self.import_draw(self.draw)
        old = self.paper.process_one('exp', sid)
        new_tax = deepcopy(self.frozen['tax_review']['extracted'])
        new_tax['tax']['threshold'] = '1'
        tax_id = self.review('tax', new_tax)
        self.repo.declare_source_correction(self.tax_id, tax_id, note='tax correction', confirmed=True)
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        blocked = self.paper.process_one('exp', sid)
        self.assertEqual(blocked['status'], 'blocked')
        self.assertIn('tax review does not cover', blocked['reason'])
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 1)
        new_rule = deepcopy(self.frozen['rule_review']['extracted'])
        new_rule['version'] = 'corrected-tax-v2'
        new_rule['tax'] = new_tax['tax']
        rule_id = self.review('rule', new_rule)
        self.repo.declare_source_correction(self.rule_id, rule_id, note='matching tax correction', confirmed=True)
        corrected = self.paper.process_one('exp', sid)
        self.assertEqual(corrected['status'], 'complete')
        self.assertNotEqual(old['group_id'], corrected['group_id'])
        self.assertEqual(corrected['result']['tax_review_id'], tax_id)
        self.assertEqual(corrected['result']['rule_review_id'], rule_id)
        self.assertNotEqual(old['result']['replicas'][0]['candidate']['tax'],
                            corrected['result']['replicas'][0]['candidate']['tax'])

    def test_invalid_or_conflicting_source_corrections_are_rejected(self):
        new_rule = deepcopy(self.frozen['rule_review']['extracted'])
        new_rule['version'] = 'corrected-v2'
        replacement = self.review('rule', new_rule)
        with self.assertRaisesRegex(RegistrationError, 'explicit confirmation'):
            self.repo.declare_source_correction(self.rule_id, replacement, note='reviewed', confirmed=False)
        with self.assertRaises(RegistrationError):
            self.repo.declare_source_correction(self.rule_id, self.tax_id, note='different kind', confirmed=True)
        self.repo.declare_source_correction(self.rule_id, replacement, note='reviewed', confirmed=True)
        with self.assertRaisesRegex(RegistrationError, 'already superseded'):
            self.repo.declare_source_correction(self.rule_id, replacement, note='stale decision', confirmed=True)
        with self.assertRaisesRegex(RegistrationError, 'already in a correction chain'):
            self.repo.declare_source_correction(replacement, self.rule_id, note='cycle', confirmed=True)
        with closing(sqlite3.connect(self.path)) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM source_corrections')

    def test_corrected_price_or_inapplicable_rule_does_not_replace_old_result(self):
        for invalid in ('price', 'issue'):
            with self.subTest(invalid=invalid):
                # Each case uses its own database and already fixed result.
                case = PaperResultsTests(methodName='test_complete_idempotent_pair_groups_and_close')
                case.setUp()
                try:
                    sid = case.import_draw(case.draw)
                    old = case.paper.process_one('exp', sid)
                    rule = deepcopy(case.frozen['rule_review']['extracted'])
                    if invalid == 'price': rule['prices']['base'] = '3'
                    else: rule['applicable_issues'] = ['26018']
                    rule_id = case.review('rule', rule)
                    case.repo.declare_source_correction(case.rule_id, rule_id, note='reviewed', confirmed=True)
                    result = case.paper.process_one('exp', sid)
                    self.assertEqual(result['status'], 'blocked')
                    self.assertEqual(case.paper.inspect_current('exp')['status'], 'review_required')
                    self.assertEqual(case.paper.get_group(old['group_id']), old['result'])
                finally:
                    case.doCleanups()

    def test_correction_queue_recovers_after_interruption_without_duplicate_publication(self):
        sid = self.import_draw(self.draw)
        old = self.paper.process_one('exp', sid)
        rule = deepcopy(self.frozen['rule_review']['extracted'])
        rule['version'] = 'recovered-rule-v2'
        rule['payout_sort_map'] = {'first': '301', 'first-extra': '401', 'second': '101', 'second-extra': '201'}
        new_id = self.review('rule', rule)
        correction = self.repo.declare_source_correction(self.rule_id, new_id, note='reviewed', confirmed=True)
        ident = correction['correction_id']
        self.assertEqual(self.repo.list_source_corrections()[0]['followup_status'], 'pending')
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE source_correction_followups SET status='running',token='old',lease_until=?,attempts=1 "
                       "WHERE correction_id=?", ((self.now-timedelta(seconds=1)).isoformat(), ident))
        scheduler = Scheduler(self.path, clock=lambda: self.now)
        scheduler.initialize()
        scheduler.run_due()
        current = self.paper.inspect_current('exp')
        self.assertEqual(current['status'], 'locally_attested')
        self.assertNotEqual(current['group_id'], old['group_id'])
        self.assertEqual(current['result']['rule_review_id'], new_id)
        record = self.repo.list_source_corrections()[0]
        self.assertEqual((record['followup_status'], record['attempts']), ('complete', 2))
        self.assertEqual(record['details']['exp']['status'], 'complete')
        scheduler.run_due()
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 2)
        self.assertEqual(self.paper.process_source_correction(ident, retry=True)['status'], 'complete')
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 2)

    def test_followup_blocks_until_tax_and_rule_corrections_are_consistent(self):
        sid = self.import_draw(self.draw)
        self.paper.process_one('exp', sid)
        tax = deepcopy(self.frozen['tax_review']['extracted'])
        tax['tax']['threshold'] = '1'
        tax_id = self.review('tax', tax)
        first = self.repo.declare_source_correction(self.tax_id, tax_id, note='correct tax', confirmed=True)
        outcome = self.paper.process_source_correction(first['correction_id'])
        self.assertEqual(outcome['status'], 'complete')
        self.assertEqual(outcome['details']['exp']['status'], 'blocked')
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        rule = deepcopy(self.frozen['rule_review']['extracted']); rule['tax'] = tax['tax']
        rule['version'] = 'matching-v2'
        rule_id = self.review('rule', rule)
        second = self.repo.declare_source_correction(self.rule_id, rule_id, note='matching rule', confirmed=True)
        outcome = self.paper.process_source_correction(second['correction_id'])
        self.assertEqual(outcome['details']['exp']['status'], 'complete')
        self.assertEqual(self.paper.inspect_current('exp')['result']['tax_review_id'], tax_id)

    def test_source_correction_unexpected_failure_is_recorded_and_retryable(self):
        draw = deepcopy(self.draw)
        draw['prizeLevelList'].pop()
        snapshot = self.import_draw(draw)
        self.assertEqual(self.paper.process_one('exp', snapshot)['status'], 'blocked')
        rule = deepcopy(self.frozen['rule_review']['extracted'])
        rule['version'] = 'corrected-v2'
        rule_id = self.review('rule', rule)
        correction = self.repo.declare_source_correction(self.rule_id, rule_id, note='correct rule', confirmed=True)
        with patch.object(self.paper, 'process_one', side_effect=RuntimeError('injected failure')):
            result = self.paper.process_source_correction(correction['correction_id'])
        self.assertEqual(result['status'], 'failed')
        self.assertTrue(result['details']['exp']['recorded'])
        current = self.paper.inspect_current('exp')
        self.assertEqual(current['status'], 'blocked')
        self.assertIn('injected failure', current['reason'])
        retried = self.paper.process_source_correction(correction['correction_id'], retry=True)
        self.assertEqual(retried['details']['exp']['status'], 'blocked')

    def test_correction_during_calculation_invalidates_old_result(self):
        sid = self.import_draw(self.draw)
        rule = deepcopy(self.frozen['rule_review']['extracted']); rule['version'] = 'parallel-v2'
        new_id = self.review('rule', rule)
        entered, release, submitted = threading.Event(), threading.Event(), threading.Event()
        from dlt import prospective_settlement as module
        original = module.settle_portfolio
        def slow_settlement(*args, **kwargs):
            entered.set()
            if not release.wait(5): raise RuntimeError('test timed out')
            return original(*args, **kwargs)
        def declare():
            submitted.set()
            return self.repo.declare_source_correction(self.rule_id, new_id, note='concurrent review', confirmed=True)
        with patch.object(module, 'settle_portfolio', side_effect=slow_settlement), ThreadPoolExecutor(max_workers=2) as pool:
            calculation = pool.submit(self.paper.process_one, 'exp', sid)
            self.assertTrue(entered.wait(5))
            correction = pool.submit(declare)
            self.assertTrue(submitted.wait(5))
            release.set()
            self.assertEqual(calculation.result(timeout=10)['status'], 'complete')
            correction.result(timeout=10)
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 1)
        self.assertEqual(self.paper.recover_pending_source_corrections()[0]['details']['exp']['status'], 'complete')
        self.assertEqual(self.paper.inspect_current('exp')['result']['rule_review_id'], new_id)

    def test_simultaneous_correction_followups_have_one_active_owner(self):
        sid = self.import_draw(self.draw); self.paper.process_one('exp', sid)
        rule = deepcopy(self.frozen['rule_review']['extracted']); rule['version'] = 'owner-v2'
        new_id = self.review('rule', rule)
        ident = self.repo.declare_source_correction(self.rule_id, new_id, note='reviewed', confirmed=True)['correction_id']
        entered, release = threading.Event(), threading.Event()
        original = self.paper.process_one
        def slow_process(*args, **kwargs):
            entered.set()
            if not release.wait(5): raise RuntimeError('test timed out')
            return original(*args, **kwargs)
        with patch.object(self.paper, 'process_one', side_effect=slow_process), ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.paper.process_source_correction, ident)
            self.assertTrue(entered.wait(5))
            self.assertEqual(self.paper.process_source_correction(ident)['status'], 'running')
            release.set()
            self.assertEqual(first.result(timeout=10)['status'], 'complete')
        self.assertEqual(self.repo.list_source_corrections()[0]['attempts'], 1)
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 2)

    def test_expired_correction_owner_cannot_publish_and_recovery_is_idempotent(self):
        sid = self.import_draw(self.draw)
        old = self.paper.process_one('exp', sid)
        rule = deepcopy(self.frozen['rule_review']['extracted']); rule['version'] = 'lease-v2'
        new_id = self.review('rule', rule)
        ident = self.repo.declare_source_correction(self.rule_id, new_id, note='reviewed', confirmed=True)['correction_id']
        from dlt import prospective_settlement as module
        original, advanced = module.settle_portfolio, []
        def expire_during_calculation(*args, **kwargs):
            if not advanced:
                self.now += timedelta(seconds=181)
                advanced.append(True)
            return original(*args, **kwargs)
        with patch.object(module, 'settle_portfolio', side_effect=expire_during_calculation):
            result = self.paper.process_source_correction(ident)
        self.assertEqual(result['status'], 'superseded')
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 1)
        self.assertEqual(self.paper.get_group(old['group_id']), old['result'])
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        recovered = self.paper.recover_pending_source_corrections()
        self.assertEqual(recovered[0]['details']['exp']['status'], 'complete')
        self.assertEqual(self.paper.inspect_current('exp')['result']['rule_review_id'], new_id)
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 2)

    def test_new_experiment_can_reference_corrected_rule_version(self):
        corrected = deepcopy(self.frozen['rule_review']['extracted'])
        corrected['version'] = 'corrected-v2'
        corrected['applicable_issues'] = ['26017', '26018']
        rule_id = self.review('rule', corrected)
        self.repo.declare_source_correction(self.rule_id, rule_id, note='reviewed', confirmed=True)
        china = timezone(timedelta(hours=8))
        day = (self.now.astimezone(china) + timedelta(days=2)).date()
        deadline = datetime.combine(day, datetime.min.time(), tzinfo=china) + timedelta(hours=20)
        calendar = {'issue': '26018', 'draw_date': day.isoformat(), 'cutoff_at': deadline.isoformat(),
                    'version': 'new-calendar-v2'}
        calendar_id = self.review('calendar', calendar)
        tax = {'tax': corrected['tax'], 'applicable_issues': ['26018']}
        tax_id = self.review('tax', tax)
        cfg = {**self.config, 'target_issue': '26018', 'target_date': day.isoformat(),
               'rule_version': 'corrected-v2', 'calendar_version': 'new-calendar-v2', 'seed': 7}
        PairRepository(self.path).save_draft('corrected-pair', cfg, history_first_issue='26001',
                                             history_cutoff_issue='26016')
        protocol = {**self.protocol, 'target_issue': '26018', 'observation_issues': ['26018'],
                    'replicas': [{'replica': 'one', 'pair_key': 'corrected-pair'}],
                    'internal_deadline': (deadline-timedelta(hours=1)).isoformat(),
                    'calendar_review_id': calendar_id, 'rule_review_id': rule_id, 'tax_review_id': tax_id}
        frozen = self.repo.freeze('corrected-future-experiment', protocol)
        self.assertEqual(frozen['pairs'][0]['pair']['config']['rule_version'], 'corrected-v2')
        self.assertEqual(frozen['protocol']['rule_review_id'], rule_id)


if __name__ == '__main__':
    unittest.main()
