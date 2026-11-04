from contextlib import closing
from copy import deepcopy
from datetime import timedelta
import sqlite3
import unittest
from unittest.mock import patch

from dlt.integrity import check_database
from dlt.pair_repository import PairRepository
from dlt.prospective import RegistrationError
from dlt.recovery import RecoveryError, prepare_isolated_copy
from dlt.scheduler import Scheduler
import test_series_report as fixtures


class ScopedCorrectionTests(unittest.TestCase):
    setUp = fixtures.SeriesReportTests.setUp
    _db = fixtures.SeriesReportTests._db
    review = fixtures.SeriesReportTests.review
    prepare_second = fixtures.SeriesReportTests.prepare_second
    import_draw = fixtures.SeriesReportTests.import_draw
    start = fixtures.SeriesReportTests.start
    settle = fixtures.SeriesReportTests.settle

    def complete_both(self):
        self.start()
        second = self.prepare_second()
        self.repo.freeze('period-2', second, series_key='series')
        self.now = self.deadline+timedelta(days=1, hours=2)
        _, first = self.settle('period-1', '26017', self.calendar['draw_date'], 'pair-1')
        _, second_result = self.settle('period-2', '26018', (self.deadline+timedelta(days=1)).date().isoformat(), 'next-3')
        return first, second_result

    def changed_rule(self, old_id=None, *, version='scoped-v2'):
        old_id = old_id or self.rule_id
        rule = deepcopy(self.repo._get_review(self._db(), old_id, 'rule')['extracted'])
        rule['version'] = version
        rule['payout_sort_map'] = {'first': '301', 'first-extra': '401', 'second': '101', 'second-extra': '201'}
        return self.review('rule', rule)

    def test_rule_correction_affects_only_selected_issue_and_preserves_history(self):
        first, second = self.complete_both()
        new = self.changed_rule()
        correction = self.repo.declare_scoped_source_correction(self.rule_id, new, issue='26017', note='fixture', confirmed=True)
        self.assertEqual(correction['scope'], 'issue')
        self.assertEqual(self.paper.inspect_current('period-1')['status'], 'review_required')
        self.assertEqual(self.paper.inspect_current('period-2')['group_id'], second['group_id'])
        report = self.series.describe('series')
        self.assertEqual((report['valid'], report['counts']['review_required']), (1, 1))
        processed = self.paper.process_source_correction(correction['correction_id'], scope='issue')
        self.assertEqual(set(processed['details']), {'period-1'})
        self.assertEqual(processed['details']['period-1']['status'], 'complete')
        current = self.paper.inspect_current('period-1')
        self.assertEqual(current['result']['rule_review_id'], new)
        self.assertEqual(self.paper.get_group(first['group_id']), first['result'])
        self.assertEqual(self.paper.inspect_current('period-2')['group_id'], second['group_id'])
        self.assertEqual(self.series.describe('series')['valid'], 2)
        self.paper.process_source_correction(correction['correction_id'], scope='issue', retry=True)
        self.assertEqual(len(self.paper.list_history('period-1')['publications']), 2)
        self.assertEqual(len(self.paper.list_history('period-2')['publications']), 1)
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_same_source_can_be_corrected_differently_for_each_issue(self):
        self.complete_both()
        first, second = self.changed_rule(version='issue-17'), self.changed_rule(version='issue-18')
        a = self.repo.declare_scoped_source_correction(self.rule_id, first, issue='26017', note='fixture', confirmed=True)
        b = self.repo.declare_scoped_source_correction(self.rule_id, second, issue='26018', note='fixture', confirmed=True)
        self.paper.process_source_correction(a['correction_id'], scope='issue')
        self.paper.process_source_correction(b['correction_id'], scope='issue')
        self.assertEqual(self.paper.inspect_current('period-1')['result']['rule_review_id'], first)
        self.assertEqual(self.paper.inspect_current('period-2')['result']['rule_review_id'], second)
        self.assertEqual(len([x for x in self.repo.list_source_corrections() if x['scope'] == 'issue']), 2)

    def test_scoped_tax_mismatch_blocks_only_one_issue_until_matching_rule(self):
        _, second = self.complete_both()
        tax = deepcopy(self.repo._get_review(self._db(), self.tax_id, 'tax')['extracted'])
        tax['tax']['threshold'] = '1'
        new_tax = self.review('tax', tax)
        change = self.repo.declare_scoped_source_correction(self.tax_id, new_tax, issue='26017', note='fixture', confirmed=True)
        outcome = self.paper.process_source_correction(change['correction_id'], scope='issue')
        self.assertEqual(outcome['details']['period-1']['status'], 'blocked')
        self.assertEqual(self.paper.inspect_current('period-1')['status'], 'review_required')
        self.assertEqual(self.paper.inspect_current('period-2')['group_id'], second['group_id'])
        rule = deepcopy(self.repo._get_review(self._db(), self.rule_id, 'rule')['extracted'])
        rule['tax'] = tax['tax']; rule['version'] = 'matching-scoped-tax'
        new_rule = self.review('rule', rule)
        correction = self.repo.declare_scoped_source_correction(self.rule_id, new_rule, issue='26017', note='fixture', confirmed=True)
        self.assertEqual(self.paper.process_source_correction(correction['correction_id'], scope='issue')['details']['period-1']['status'], 'complete')
        self.assertEqual(self.paper.inspect_current('period-1')['result']['tax_review_id'], new_tax)
        self.assertEqual(self.paper.inspect_current('period-2')['group_id'], second['group_id'])

    def test_global_then_issue_correction_follows_single_chain(self):
        self.complete_both()
        globally = self.changed_rule(version='global-v2')
        first = self.repo.declare_source_correction(self.rule_id, globally, note='fixture', confirmed=True)
        self.paper.process_source_correction(first['correction_id'])
        other_before = self.paper.inspect_current('period-2')['group_id']
        scoped = self.changed_rule(globally, version='scoped-after-global')
        second = self.repo.declare_scoped_source_correction(globally, scoped, issue='26017', note='fixture', confirmed=True)
        self.paper.process_source_correction(second['correction_id'], scope='issue')
        result = self.paper.inspect_current('period-1')['result']
        self.assertEqual(result['rule_correction_chain'], [self.rule_id, globally, scoped])
        self.assertEqual(self.paper.inspect_current('period-2')['group_id'], other_before)
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_issue_then_global_correction_only_reaches_its_descendants(self):
        _, other = self.complete_both()
        scoped = self.changed_rule(version='scoped-before-global')
        first = self.repo.declare_scoped_source_correction(self.rule_id, scoped, issue='26017', note='fixture', confirmed=True)
        self.paper.process_source_correction(first['correction_id'], scope='issue')
        globally = self.changed_rule(scoped, version='global-after-scoped')
        second = self.repo.declare_source_correction(scoped, globally, note='fixture', confirmed=True)
        self.paper.process_source_correction(second['correction_id'])
        self.assertEqual(self.paper.inspect_current('period-1')['result']['rule_correction_chain'], [self.rule_id, scoped, globally])
        self.assertEqual(self.paper.inspect_current('period-2')['group_id'], other['group_id'])
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_ambiguous_global_and_scoped_branches_are_rejected(self):
        self.complete_both()
        scoped = self.changed_rule(version='scoped')
        self.repo.declare_scoped_source_correction(self.rule_id, scoped, issue='26017', note='fixture', confirmed=True)
        new = self.changed_rule(version='global')
        with self.assertRaisesRegex(RegistrationError, 'issue-scoped corrections'):
            self.repo.declare_source_correction(self.rule_id, new, note='fixture', confirmed=True)
        with self.assertRaisesRegex(RegistrationError, 'superseded'):
            self.repo.declare_scoped_source_correction(self.rule_id, new, issue='26017', note='fixture', confirmed=True)
        with self.assertRaisesRegex(RegistrationError, 'cover the selected issue'):
            self.repo.declare_scoped_source_correction(self.rule_id, new, issue='26999', note='fixture', confirmed=True)

    def test_background_recovers_scoped_followup_without_browser(self):
        self.complete_both()
        new = self.changed_rule()
        correction = self.repo.declare_scoped_source_correction(self.rule_id, new, issue='26017', note='fixture', confirmed=True)
        self.assertEqual(self.paper.inspect_current('period-1')['status'], 'review_required')
        scheduler = Scheduler(self.path, clock=lambda: self.now)
        scheduler.initialize(); scheduler.run_due()
        current = self.paper.inspect_current('period-1')
        self.assertEqual(current['status'], 'locally_attested')
        self.assertEqual(current['result']['rule_review_id'], new)
        item = next(x for x in self.repo.list_source_corrections() if x['scope'] == 'issue')
        self.assertEqual(item['seq'], correction['correction_id'])
        self.assertEqual(item['followup_status'], 'complete')

    def test_missing_durable_followup_is_detected_before_recovery_copy(self):
        self.complete_both()
        new = self.changed_rule()
        correction = self.repo.declare_scoped_source_correction(self.rule_id, new, issue='26017', note='fixture', confirmed=True)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DELETE FROM scoped_correction_followups WHERE correction_id=?', (correction['correction_id'],))
        check = check_database(self.path)
        self.assertEqual(check['status'], 'failed', check)
        self.assertTrue(any(item['area'] == 'scoped_correction_followups' for item in check['errors']))
        with self.assertRaises(RecoveryError):
            prepare_isolated_copy(self.path, self.path.with_name('broken-scoped.sqlite3'))

    def test_scoped_review_is_enforced_when_freezing_new_experiment(self):
        self.start()
        new = self.changed_rule()
        self.repo.declare_scoped_source_correction(self.rule_id, new, issue='26017', note='fixture', confirmed=True)
        with self.assertRaisesRegex(RegistrationError, 'superseded'):
            self.repo.freeze('new-old-rule', self.protocol)
        rule = self.repo._get_review(self._db(), new, 'rule')['extracted']
        cfg = {**self.config, 'seed': 15, 'rule_version': rule['version']}
        PairRepository(self.path).save_draft('new-rule-pair', cfg, history_first_issue='26001', history_cutoff_issue='26016')
        current = {**self.protocol, 'rule_review_id': new, 'replicas': [{'replica': 'one', 'pair_key': 'new-rule-pair'}]}
        self.assertEqual(self.repo.freeze('new-current-rule', current)['protocol']['rule_review_id'], new)


if __name__ == '__main__':
    unittest.main()
