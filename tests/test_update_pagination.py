from contextlib import closing
import base64
from copy import deepcopy
from datetime import date, timedelta
import json
import sqlite3
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from dlt.official import SourceError, fetch_page, fetch_page_bounded
from dlt.pair_repository import PairRepository
from dlt.prospective_settlement import PaperResults
from dlt.prize_rules import DLT_2026_OFFICIAL_SORT_CODES
from dlt.scheduler import ScheduleError, Scheduler
from test_scheduler import Opener, RAW, configuration
import test_prospective as fixtures


class AdapterPaginationTests(unittest.TestCase):
    def test_page_number_is_bounded_and_in_source(self):
        result = fetch_page(page_no=2, page_size=20, timeout_seconds=2, max_bytes=100000, opener=Opener())
        self.assertEqual(parse_qs(urlsplit(result['source']).query)['pageNo'], ['2'])
        for value in (0, 1001, True, '2'):
            with self.subTest(value=value), self.assertRaises(SourceError):
                fetch_page(page_no=value, page_size=20, timeout_seconds=2, max_bytes=100000, opener=Opener())

    def test_child_cannot_claim_another_page(self):
        class Process:
            returncode = 0
            stderr = b''
            stdout = json.dumps({'source': 'https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry?'
                                          'gameNo=85&provinceId=0&pageSize=20&isVerify=1&pageNo=1',
                                 'observed_at': '2026-01-01T00:00:00+00:00',
                                 'raw_base64': base64.b64encode(RAW).decode()}).encode()
        with patch('dlt.official.subprocess.run', return_value=Process()):
            with self.assertRaisesRegex(SourceError, 'invalid source'):
                fetch_page_bounded(page_no=2, page_size=20, timeout_seconds=2, max_bytes=100000)


class PaginationTests(unittest.TestCase):
    review = fixtures.ProspectiveTests.review

    def setUp(self):
        fixtures.ProspectiveTests.setUp(self)
        self.repo.freeze('exp', self.protocol)
        ticket = PairRepository(self.path).get('pair-1')['candidate']['tickets'][0]
        published_levels = [{'sort': int(code), 'stakeAmount': '6000', 'awardType': 0}
                            for code in sorted(DLT_2026_OFFICIAL_SORT_CODES)]
        self.draw = {'lotteryGameNum': '85', 'lotteryDrawNum': '26017', 'lotteryDrawTime': self.calendar['draw_date'],
                     'lotteryDrawResult': ' '.join(map(str, ticket['front']+ticket['back'])),
                     'lotteryPromotionFlag': 0, 'verify': 1,
                     'prizeLevelList': published_levels}
        self.now = self.deadline + timedelta(days=10)
        self.scheduler = Scheduler(self.path, clock=lambda: self.now)

    def newer(self, issue, days):
        return {**deepcopy(self.draw), 'lotteryDrawNum': issue,
                'lotteryDrawTime': (date.fromisoformat(self.calendar['draw_date'])+timedelta(days=days)).isoformat()}

    def import_row(self, store, row, guard, number):
        payload = json.dumps({'success': True, 'errorCode': '0', 'value': {'list': [row]}}).encode()
        source = ('https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry?'
                  f'gameNo=85&provinceId=0&pageSize=1&isVerify=1&pageNo={number}')
        return store.import_official(payload, source, self.now.isoformat(), transaction_guard=guard)

    def run_update(self, page_updater, *, max_pages=4, retry_count=0):
        cfg = {**configuration(), 'start_at': (self.now-timedelta(seconds=5)).isoformat(),
               'page_size': 1, 'retry_count': retry_count, 'max_pages': max_pages}
        plan = self.scheduler.create_plan(cfg)
        self.scheduler.set_enabled(plan, True)
        claim = self.scheduler.claim(plan)
        def latest(store, *, transaction_guard, **kwargs):
            return self.import_row(store, self.newer('26020', 3), transaction_guard, 1)
        result = self.scheduler.execute(claim, updater=latest, page_updater=page_updater, sleep=lambda _: None)
        self.scheduler.process_pending_followups()
        return result, self.scheduler.list_runs(plan)[0]

    def test_multi_page_import_and_automatic_settlement(self):
        called = []
        def page(store, *, page_no, transaction_guard, **kwargs):
            called.append(page_no)
            row = self.newer('26019', 2) if page_no == 2 else self.draw
            return self.import_row(store, row, transaction_guard, page_no)
        result, run = self.run_update(page)
        self.assertEqual(called, [2, 3])
        self.assertIsNone(result['history_warning'])
        self.assertEqual(run['followups'][0]['details']['exp']['status'], 'complete')
        settled = PaperResults(self.path, clock=lambda: self.now).inspect_current('exp')['result']
        self.assertIn('pageNo=3', settled['source_url'])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM observations WHERE source LIKE '%pageNo=3'").fetchone()[0], 1)

    def test_cancel_during_older_page_keeps_committed_latest_without_settlement(self):
        cfg = {**configuration(), 'start_at': (self.now-timedelta(seconds=5)).isoformat(),
               'page_size': 1, 'retry_count': 0, 'max_pages': 4}
        plan = self.scheduler.create_plan(cfg)
        self.scheduler.set_enabled(plan, True)
        claim = self.scheduler.claim(plan)
        def latest(store, *, transaction_guard, **kwargs):
            return self.import_row(store, self.newer('26020', 3), transaction_guard, 1)
        def page(store, *, page_no, transaction_guard, **kwargs):
            self.scheduler.cancel_run(plan, claim['run_id'])
            return self.import_row(store, self.draw, transaction_guard, page_no)
        self.assertIsNone(self.scheduler.execute(claim, updater=latest, page_updater=page))
        self.scheduler.process_pending_followups()
        run = self.scheduler.list_runs(plan)[0]
        self.assertEqual(run['status'], 'cancelled')
        self.assertFalse(run['followups'])
        self.assertEqual(self.paper_status(), 'waiting')
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM observations WHERE source LIKE '%pageNo=1'").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM observations WHERE source LIKE '%pageNo=2'").fetchone()[0], 0)

    def test_limit_and_failure_leave_unresolved_target_visible(self):
        for limit, fail in ((1, False), (3, True)):
            with self.subTest(limit=limit):
                calls = []
                def page(*args, **kwargs):
                    calls.append(kwargs['page_no'])
                    raise SourceError('offline')
                result, run = self.run_update(page, max_pages=limit, retry_count=1)
                self.assertTrue(result['history_warning'])
                self.assertEqual(run['followups'][0]['status'], 'incomplete')
                self.assertEqual(run['followups'][0]['details']['exp']['status'], 'waiting_source')
                self.assertEqual(len(calls), 2 if fail else 0)

    def test_overlapping_page_reports_incomplete(self):
        def page(store, *, page_no, transaction_guard, **kwargs):
            return self.import_row(store, self.newer('26020', 3), transaction_guard, page_no)
        result, run = self.run_update(page)
        self.assertIn('重复期号', result['history_warning'])
        self.assertEqual(run['followups'][0]['status'], 'incomplete')

    def test_out_of_order_page_is_not_saved_as_a_resume_anchor(self):
        def page(store, *, page_no, transaction_guard, **kwargs):
            return self.import_row(store, self.newer('26021', 9), transaction_guard, page_no)
        result, run = self.run_update(page)
        self.assertIn('日期顺序异常', result['history_warning'])
        self.assertIsNone(result['history_cursor'])
        self.assertEqual(run['followups'][0]['status'], 'incomplete')

    def test_transient_page_failure_retries_without_losing_latest_page(self):
        calls = []
        def page(store, *, page_no, transaction_guard, **kwargs):
            calls.append(page_no)
            if len(calls) == 1:
                raise SourceError('temporary outage')
            return self.import_row(store, self.draw, transaction_guard, page_no)
        result, run = self.run_update(page, retry_count=1)
        self.assertEqual(calls, [2, 2])
        self.assertIsNone(result['history_warning'])
        self.assertEqual(run['followups'][0]['details']['exp']['status'], 'complete')

    def test_later_update_can_use_page_retained_before_failure(self):
        # First run saves the desired page, but reports a failure before the
        # caller can treat its fetch sequence as complete.
        calls = []
        def partial(store, *, page_no, transaction_guard, **kwargs):
            calls.append(page_no)
            self.import_row(store, self.draw, transaction_guard, page_no)
            raise SourceError('connection lost after import')
        _, first = self.run_update(partial)
        self.assertTrue(first['error'])
        self.assertEqual(first['followups'][0]['details']['exp']['status'], 'complete')
        def unexpected(*args, **kwargs):
            self.fail('already settled target should not trigger more pages')
        result, second = self.run_update(unexpected)
        self.assertIsNone(result['history_warning'])
        self.assertEqual(second['followups'][0]['status'], 'complete')

    def test_expired_page_worker_is_fenced(self):
        def page(store, *, page_no, transaction_guard, **kwargs):
            self.now += timedelta(seconds=1000)
            return self.import_row(store, self.draw, transaction_guard, page_no)
        with self.assertRaisesRegex(ScheduleError, 'ownership'):
            self.run_update(page)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertIsNone(db.execute("SELECT issue FROM draw_versions WHERE issue='26017'").fetchone())

    def test_invalid_page_limits_and_corrupted_saved_plan_are_rejected(self):
        for value in (0, 1001, True, '10'):
            with self.subTest(value=value), self.assertRaises(ScheduleError):
                self.scheduler.create_plan({**configuration(), 'max_pages': value})
        plan = self.scheduler.create_plan({**configuration(), 'max_pages': 3})
        self.scheduler.set_enabled(plan, True)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE update_plans SET config_json=? WHERE id=?',
                       (json.dumps({**configuration(), 'max_pages': 1001}).encode(), plan))
        with self.assertRaisesRegex(ScheduleError, 'saved plan configuration is invalid'):
            self.scheduler.claim(plan)

    def test_resume_after_limit_verifies_anchor_and_manual_run_uses_parent_cursor(self):
        plan = self.scheduler.create_plan({**configuration(), 'start_at': (self.now-timedelta(seconds=1)).isoformat(),
                                           'page_size': 1, 'max_pages': 3, 'retry_count': 0})
        self.scheduler.set_enabled(plan, True)
        calls = []
        def latest(store, *, transaction_guard, **kwargs):
            return self.import_row(store, self.newer('26020', 4), transaction_guard, 1)
        def page(store, *, page_no, transaction_guard, **kwargs):
            calls.append(page_no)
            rows = {2: self.newer('26019', 3), 3: self.newer('26018', 2), 4: self.draw}
            return self.import_row(store, rows[page_no], transaction_guard, page_no)
        first = self.scheduler.execute(self.scheduler.claim(plan), updater=latest, page_updater=page)
        self.assertEqual(calls, [2, 3])
        self.assertEqual(first['history_cursor']['page_no'], 3)
        self.assertIn('下次将核对后继续', first['history_warning'])
        self.scheduler.process_pending_followups()
        self.assertEqual(self.paper_status(), 'waiting')
        self.now += timedelta(seconds=61)
        manual = self.scheduler.create_one_off(plan)
        calls.clear()
        second = self.scheduler.execute(self.scheduler.claim(manual), updater=latest, page_updater=page)
        self.assertEqual(calls, [3, 4])
        self.assertIsNone(second['history_warning'])
        self.scheduler.process_pending_followups()
        self.assertEqual(self.paper_status(), 'locally_attested')

    def paper_status(self):
        return PaperResults(self.path, clock=lambda: self.now).inspect_current('exp')['status']

    def test_shifted_anchor_restarts_without_skipping_earlier_pages(self):
        plan = self.scheduler.create_plan({**configuration(), 'start_at': (self.now-timedelta(seconds=1)).isoformat(),
                                           'page_size': 1, 'max_pages': 3, 'retry_count': 0})
        self.scheduler.set_enabled(plan, True)
        calls, changed = [], [False]
        def latest(store, *, transaction_guard, **kwargs):
            return self.import_row(store, self.newer('26020', 4), transaction_guard, 1)
        def page(store, *, page_no, transaction_guard, **kwargs):
            calls.append(page_no)
            row = (self.newer('26021', 5) if changed[0] and page_no == 3 else
                   self.newer('26019', 3) if page_no == 2 else self.newer('26018', 2))
            return self.import_row(store, row, transaction_guard, page_no)
        first = self.scheduler.execute(self.scheduler.claim(plan), updater=latest, page_updater=page)
        self.assertEqual(first['history_cursor']['page_no'], 3)
        self.now += timedelta(seconds=61)
        changed[0] = True; calls.clear()
        second = self.scheduler.execute(self.scheduler.claim(plan), updater=latest, page_updater=page)
        self.assertEqual(calls, [3, 2])
        self.assertEqual(second['history_cursor']['page_no'], 2)
        self.assertIn('尚未查到', second['history_warning'])

    def test_invalid_anchor_and_changed_targets_cannot_be_resumed(self):
        plan = self.scheduler.create_plan({**configuration(), 'start_at': (self.now-timedelta(seconds=1)).isoformat(),
                                           'page_size': 1, 'max_pages': 2, 'retry_count': 0})
        self.scheduler.set_enabled(plan, True)
        def latest(store, *, transaction_guard, **kwargs):
            return self.import_row(store, self.newer('26020', 4), transaction_guard, 1)
        def page(store, *, page_no, transaction_guard, **kwargs):
            return self.import_row(store, self.newer('26019', 3), transaction_guard, page_no)
        first = self.scheduler.execute(self.scheduler.claim(plan), updater=latest, page_updater=page)
        self.now += timedelta(seconds=61)
        claim = self.scheduler.claim(plan)
        targets = self.scheduler._missing_older_targets(first['snapshot_id'])
        self.assertIsNotNone(self.scheduler._previous_cursor(claim, targets))
        self.assertIsNone(self.scheduler._previous_cursor(claim, {**targets, '26016': ['new']}))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER observations_no_update')
            db.execute("UPDATE observations SET raw_payload=? WHERE source LIKE '%pageNo=2'", (b'{}',))
        self.assertIsNone(self.scheduler._previous_cursor(claim, targets))

    def test_interrupted_continuation_reuses_last_completed_cursor(self):
        plan = self.scheduler.create_plan({**configuration(), 'start_at': (self.now-timedelta(seconds=1)).isoformat(),
                                           'page_size': 1, 'max_pages': 3, 'retry_count': 0})
        self.scheduler.set_enabled(plan, True)
        calls, expire = [], [False]
        def latest(store, *, transaction_guard, **kwargs):
            return self.import_row(store, self.newer('26020', 4), transaction_guard, 1)
        def page(store, *, page_no, transaction_guard, **kwargs):
            calls.append(page_no)
            if expire[0] and page_no == 4:
                self.now += timedelta(seconds=1000)
            rows = {2: self.newer('26019', 3), 3: self.newer('26018', 2), 4: self.draw}
            return self.import_row(store, rows[page_no], transaction_guard, page_no)
        first = self.scheduler.execute(self.scheduler.claim(plan), updater=latest, page_updater=page)
        self.assertEqual(first['history_cursor']['page_no'], 3)
        self.now += timedelta(seconds=61)
        expire[0] = True; calls.clear()
        with self.assertRaisesRegex(ScheduleError, 'ownership'):
            self.scheduler.execute(self.scheduler.claim(plan), updater=latest, page_updater=page)
        self.assertEqual(calls, [3, 4])
        expire[0] = False; calls.clear()
        recovered = self.scheduler.claim(plan)
        self.assertTrue(recovered['recovered'])
        result = self.scheduler.execute(recovered, updater=latest, page_updater=page)
        self.assertEqual(calls, [3, 4])
        self.assertIsNone(result['history_warning'])
        self.scheduler.process_pending_followups()
        self.assertEqual(self.paper_status(), 'locally_attested')
        self.assertEqual([run['status'] for run in self.scheduler.list_runs(plan)], ['success', 'interrupted', 'success'])


if __name__ == '__main__':
    unittest.main()
