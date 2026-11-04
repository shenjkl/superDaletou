from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import sqlite3
import sys
import threading
import unittest
from unittest.mock import patch

from dlt.integrity import check_database
from dlt.candidates import _canonical
from dlt.pair_repository import PairRepository
from dlt.prospective import RegistrationError
import test_prospective_settlement as fixtures
import test_rolling_training as rolling_fixtures
import test_source_correction_http as http_fixtures
import test_series_report as series_fixtures


class CalendarCorrectionTests(unittest.TestCase):
    setUp = fixtures.PaperResultsTests.setUp
    review = fixtures.PaperResultsTests.review
    import_draw = fixtures.PaperResultsTests.import_draw

    def correction(self, *, cutoff=None, draw_date=None, issue=None):
        calendar = deepcopy(self.calendar)
        calendar['version'] = 'calendar-corrected-v2'
        if cutoff is not None: calendar['cutoff_at'] = cutoff.isoformat()
        if draw_date is not None: calendar['draw_date'] = draw_date
        if issue is not None: calendar['issue'] = issue
        ident = self.review('calendar', calendar)
        correction = self.repo.declare_source_correction(self.cal_id, ident, note='synthetic calendar correction', confirmed=True)
        return ident, correction

    def test_later_cutoff_with_same_date_recalculates_and_keeps_history(self):
        sid = self.import_draw(self.draw)
        old = self.paper.process_one('exp', sid)
        self.assertEqual(old['status'], 'complete')
        new_id, correction = self.correction(cutoff=self.deadline+timedelta(minutes=30))
        self.assertEqual(self.repo.inspect('exp')['effective_calendar'],
                         {'review_id': new_id, 'draw_date': self.calendar['draw_date']})
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        updated = self.paper.process_source_correction(correction['correction_id'])
        self.assertEqual(updated['details']['exp']['status'], 'complete', updated)
        current = self.paper.inspect_current('exp')
        self.assertEqual(current['status'], 'locally_attested')
        self.assertEqual(current['result']['calendar_review_id'], new_id)
        self.assertNotEqual(current['group_id'], old['group_id'])
        self.assertEqual(self.paper.get_group(old['group_id']), old['result'])
        self.paper.process_source_correction(correction['correction_id'], retry=True)
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 2)
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_legacy_result_without_calendar_fields_remains_readable_and_correctable(self):
        sid = self.import_draw(self.draw)
        original = self.paper.process_one('exp', sid)
        legacy = deepcopy(original['result'])
        for name in ('calendar_review_id', 'original_calendar_review_id', 'calendar_correction_chain'):
            legacy.pop(name)
        encoded = _canonical(legacy); ident = hashlib.sha256(encoded).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER paper_groups_no_update')
            db.execute('DROP TRIGGER paper_publications_no_update')
            db.execute('UPDATE paper_groups SET id=?,content_json=? WHERE id=?', (ident, encoded, original['group_id']))
            db.execute('UPDATE paper_current SET group_id=? WHERE experiment_key=?', (ident, 'exp'))
            db.execute('UPDATE paper_publications SET group_id=? WHERE experiment_key=?', (ident, 'exp'))
            db.execute("CREATE TRIGGER paper_groups_no_update BEFORE UPDATE ON paper_groups BEGIN SELECT RAISE(ABORT, 'immutable paper group'); END")
            db.execute("CREATE TRIGGER paper_publications_no_update BEFORE UPDATE ON paper_publications BEGIN SELECT RAISE(ABORT, 'immutable publication'); END")
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'locally_attested')
        self.assertEqual(check_database(self.path)['status'], 'passed')
        new_id, correction = self.correction(cutoff=self.deadline+timedelta(minutes=30))
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        self.assertEqual(self.paper.process_source_correction(correction['correction_id'])['details']['exp']['status'], 'complete')
        self.assertEqual(self.paper.inspect_current('exp')['result']['calendar_review_id'], new_id)
        self.assertEqual(self.paper.get_group(ident), legacy)

    def test_earlier_cutoff_equal_to_registration_withdraws_result(self):
        self.now = self.deadline-timedelta(minutes=10)
        protocol = {**self.protocol, 'internal_deadline': (self.deadline-timedelta(minutes=5)).isoformat()}
        late = self.repo.freeze('late', protocol)
        registered = self.now
        self.now = self.deadline+timedelta(hours=2)
        sid = self.import_draw(self.draw)
        old = self.paper.process_one('late', sid)
        self.assertEqual(old['status'], 'complete')
        _, correction = self.correction(cutoff=registered)
        self.assertEqual(self.repo.inspect('late')['status'], 'review_required')
        invalid = self.paper.inspect_current('late')
        self.assertEqual(invalid['status'], 'review_required')
        self.assertIn('at or after corrected', invalid['reason'])
        outcome = self.paper.process_source_correction(correction['correction_id'])
        self.assertEqual(outcome['details']['late']['status'], 'blocked')
        self.assertIsNone(self.paper.inspect_current('late')['result'])
        self.assertEqual(self.paper.get_group(old['group_id']), old['result'])
        self.assertEqual(self.repo.get('late'), late)

    def test_later_cutoff_requires_an_observation_after_new_cutoff(self):
        sid = self.import_draw(self.draw, observed=self.deadline+timedelta(minutes=1))
        old = self.paper.process_one('exp', sid)
        self.assertEqual(old['status'], 'complete')
        _, correction = self.correction(cutoff=self.deadline+timedelta(minutes=30))
        followup = self.paper.process_source_correction(correction['correction_id'])
        self.assertEqual(followup['details']['exp']['status'], 'blocked')
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        self.import_draw(self.draw, observed=self.deadline+timedelta(minutes=31))
        followup = self.paper.process_source_correction(correction['correction_id'], retry=True)
        self.assertEqual(followup['details']['exp']['status'], 'complete')
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'locally_attested')
        self.assertEqual(self.paper.get_group(old['group_id']), old['result'])

    def test_changed_draw_date_keeps_old_result_under_review(self):
        sid = self.import_draw(self.draw)
        old = self.paper.process_one('exp', sid)
        shifted = self.deadline+timedelta(days=1)
        _, correction = self.correction(cutoff=shifted, draw_date=shifted.date().isoformat())
        self.assertEqual(self.repo.inspect('exp')['status'], 'review_required')
        self.assertNotIn('effective_calendar', self.repo.inspect('exp'))
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        outcome = self.paper.process_source_correction(correction['correction_id'])
        self.assertEqual(outcome['details']['exp']['status'], 'blocked')
        self.assertIn('target date', outcome['details']['exp']['reason'])
        self.assertEqual(self.paper.get_group(old['group_id']), old['result'])
        self.assertIsNone(self.paper.inspect_current('exp')['result'])

    def test_different_issue_or_invalid_cutoff_cannot_be_declared(self):
        with self.assertRaisesRegex(RegistrationError, 'same issue'):
            self.correction(issue='26018')
        with self.assertRaisesRegex(RegistrationError, 'same issue'):
            self.correction(cutoff=self.deadline+timedelta(days=1))
        self.assertEqual(self.repo.list_source_corrections(), [])

    def test_superseded_calendar_cannot_be_used_for_new_registration(self):
        self.now = self.deadline-timedelta(hours=3)
        new_id, _ = self.correction(cutoff=self.deadline-timedelta(minutes=30))
        with self.assertRaisesRegex(RegistrationError, 'calendar review has been superseded'):
            self.repo.freeze('new-using-old-calendar', self.protocol)
        # Exact retry of the existing fixed request is still idempotent.
        self.assertEqual(self.repo.freeze('exp', self.protocol), self.frozen)
        cfg = {**self.config, 'calendar_version': 'calendar-corrected-v2', 'seed': 99}
        PairRepository(self.path).save_draft('new-calendar-pair', cfg, history_first_issue='26001', history_cutoff_issue='26016')
        protocol = {**self.protocol, 'calendar_review_id': new_id,
                    'replicas': [{'replica': 'one', 'pair_key': 'new-calendar-pair'}]}
        result = self.repo.freeze('new-calendar-experiment', protocol)
        self.assertEqual(result['protocol']['calendar_review_id'], new_id)

    def test_calendar_correction_during_calculation_withdraws_old_result(self):
        sid = self.import_draw(self.draw)
        new_id = self.review('calendar', {**self.calendar, 'version': 'concurrent-v2',
                                         'cutoff_at': (self.deadline+timedelta(minutes=30)).isoformat()})
        entered, release, submitted = threading.Event(), threading.Event(), threading.Event()
        from dlt import prospective_settlement as module
        original = module.settle_portfolio
        def slow(*args, **kwargs):
            entered.set()
            if not release.wait(5): raise RuntimeError('test timed out')
            return original(*args, **kwargs)
        def declare():
            submitted.set()
            return self.repo.declare_source_correction(self.cal_id, new_id, note='concurrent fixture', confirmed=True)
        with patch.object(module, 'settle_portfolio', side_effect=slow), ThreadPoolExecutor(max_workers=2) as pool:
            calculation = pool.submit(self.paper.process_one, 'exp', sid)
            self.assertTrue(entered.wait(5))
            correction = pool.submit(declare)
            self.assertTrue(submitted.wait(5))
            release.set()
            self.assertEqual(calculation.result(timeout=10)['status'], 'complete')
            correction.result(timeout=10)
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        self.assertEqual(len(self.paper.list_history('exp')['publications']), 1)
        recovered = self.paper.recover_pending_source_corrections()
        self.assertEqual(recovered[0]['details']['exp']['status'], 'complete')
        self.assertEqual(self.paper.inspect_current('exp')['result']['calendar_review_id'], new_id)


class RollingCalendarCorrectionTests(unittest.TestCase):
    setUpBase = rolling_fixtures.RollingTrainingTests.setUpBase
    setUp = rolling_fixtures.RollingTrainingTests.setUp
    review = rolling_fixtures.RollingTrainingTests.review
    import_draw = rolling_fixtures.RollingTrainingTests.import_draw
    _db = rolling_fixtures.RollingTrainingTests._db
    prepare_second = rolling_fixtures.RollingTrainingTests.prepare_second
    train = rolling_fixtures.RollingTrainingTests.train
    drive = rolling_fixtures.RollingTrainingTests.drive

    def correct_calendar(self):
        changed = {**self.calendar, 'version': 'corrected-calendar',
                   'cutoff_at': (self.deadline-timedelta(minutes=30)).isoformat()}
        new_id = self.review('calendar', changed)
        self.repo.declare_source_correction(self.cal_id, new_id, note='synthetic calendar correction', confirmed=True)
        return new_id

    def test_corrected_calendar_blocks_waiting_plan_and_new_old_calendar_plan(self):
        plan = self.scheduler.create(**self.args)
        self.correct_calendar()
        self.scheduler.tick()
        result = self.scheduler.list()[0]
        self.assertEqual(result['status'], 'blocked')
        self.assertIn('日历资料已更正', result['message'])
        self.assertEqual(result['id'], plan['id'])
        with self.assertRaisesRegex(ValueError, '日历资料已更正'):
            self.scheduler.retry(plan['id'])

    def test_calendar_corrected_during_training_blocks_plan_completion(self):
        self.jobs.worker_command = [sys.executable, '-c',
            "import time,runpy; time.sleep(1); runpy.run_module('dlt.training_worker',run_name='__main__')"]
        plan = self.scheduler.create(**self.args)
        self.scheduler.tick()
        self.assertEqual(self.scheduler.list()[0]['status'], 'running')
        self.correct_calendar()
        result = self.drive(plan['id'])
        self.assertEqual(result['status'], 'blocked')


class CalendarCorrectionHttpTests(unittest.TestCase):
    setUp = http_fixtures.SourceCorrectionHttpTests.setUp
    review = http_fixtures.SourceCorrectionHttpTests.review
    import_draw = http_fixtures.SourceCorrectionHttpTests.import_draw
    start_server = http_fixtures.SourceCorrectionHttpTests.start_server

    def test_calendar_correction_http_recalculates_and_keeps_history(self):
        sid = self.import_draw(self.draw)
        old = self.paper.process_one('exp', sid)
        replacement = {**self.calendar, 'version': 'calendar-http-v2',
                       'cutoff_at': (self.deadline+timedelta(minutes=30)).isoformat()}
        new_id = self.review('calendar', replacement)
        body = {'old_review_id': self.cal_id, 'new_review_id': new_id, 'note': 'synthetic', 'confirmed': True}
        request, read, write = self.start_server()
        with patch('dlt.app.PaperResults', return_value=self.paper):
            self.assertEqual(request('POST', '/api/source-corrections', json.dumps(body), {**write, 'Origin': 'https://evil.invalid'})[0], 403)
            status, _, response = request('POST', '/api/source-corrections', json.dumps(body), write)
            self.assertEqual(status, 200, response)
            self.assertEqual(response['correction']['kind'], 'calendar')
            self.assertEqual(response['followup']['details']['exp']['status'], 'complete')
            _, _, history = request('GET', '/api/experiment-history?key=exp', headers=read)
            self.assertEqual(len(history['history']['publications']), 2)
            self.assertEqual(history['history']['publications'][0]['group_id'], old['group_id'])


class CalendarCorrectionSeriesTests(unittest.TestCase):
    setUp = series_fixtures.SeriesReportTests.setUp
    _db = series_fixtures.SeriesReportTests._db
    review = series_fixtures.SeriesReportTests.review
    prepare_second = series_fixtures.SeriesReportTests.prepare_second
    import_draw = series_fixtures.SeriesReportTests.import_draw
    start = series_fixtures.SeriesReportTests.start
    settle = series_fixtures.SeriesReportTests.settle

    def test_cross_period_summary_withdraws_old_calendar_result(self):
        self.start(); self.now = self.deadline+timedelta(hours=2)
        self.settle('period-1', '26017', self.calendar['draw_date'], 'pair-1')
        self.assertEqual(self.series.describe('series')['valid'], 1)
        new = self.review('calendar', {**self.calendar, 'version': 'series-calendar-v2',
                                       'cutoff_at': (self.deadline+timedelta(minutes=30)).isoformat()})
        correction = self.repo.declare_source_correction(self.cal_id, new, note='fixture', confirmed=True)
        report = self.series.describe('series')
        self.assertEqual((report['valid'], report['counts']['review_required']), (0, 1))
        self.paper.process_source_correction(correction['correction_id'])
        self.assertEqual(self.series.describe('series')['valid'], 1)


if __name__ == '__main__':
    unittest.main()
