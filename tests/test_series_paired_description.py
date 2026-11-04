import unittest
from contextlib import closing
from copy import deepcopy
import hashlib
import sqlite3

import test_scoped_correction as fixtures
from dlt.candidates import _canonical


class SeriesPairedDescriptionTests(unittest.TestCase):
    setUp = fixtures.ScopedCorrectionTests.setUp
    _db = fixtures.ScopedCorrectionTests._db
    review = fixtures.ScopedCorrectionTests.review
    prepare_second = fixtures.ScopedCorrectionTests.prepare_second
    import_draw = fixtures.ScopedCorrectionTests.import_draw
    start = fixtures.ScopedCorrectionTests.start
    settle = fixtures.ScopedCorrectionTests.settle
    complete_both = fixtures.ScopedCorrectionTests.complete_both
    changed_rule = fixtures.ScopedCorrectionTests.changed_rule

    def test_current_summary_withdraws_corrected_period_until_recomputed(self):
        self.complete_both()
        report = self.series.describe('series')
        summary = report['paired_description']
        self.assertEqual((summary['planned'], summary['included'], summary['missing']), (2, 2, 0))
        self.assertEqual(summary['mean_fraction'], report['mean_difference_fraction'])
        self.assertEqual(summary['positive']+summary['zero']+summary['negative'], 2)
        new = self.changed_rule()
        correction = self.repo.declare_scoped_source_correction(self.rule_id, new, issue='26017', note='fixture', confirmed=True)
        partial = self.series.describe('series')['paired_description']
        self.assertEqual((partial['included'], partial['missing'], partial['completeness']), (1, 1, 'partial'))
        self.assertIsNone(partial['leave_one_out_min_fraction'])
        self.paper.process_source_correction(correction['correction_id'], scope='issue')
        self.assertEqual(self.series.describe('series')['paired_description']['included'], 2)

    def test_inaccurate_legacy_period_is_excluded_until_new_result(self):
        first, _ = self.complete_both()
        legacy = deepcopy(first['result'])
        legacy.pop('calculation_version'); legacy.pop('settlement_code_sha256')
        legacy['code_sha256'] = '0' * 64
        legacy['replicas'][0]['candidate']['gross'] = '999999.01'
        encoded = _canonical(legacy); ident = hashlib.sha256(encoded).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER paper_groups_no_update')
            db.execute('DROP TRIGGER paper_publications_no_update')
            db.execute('UPDATE paper_groups SET id=?,content_json=? WHERE id=?', (ident, encoded, first['group_id']))
            db.execute('UPDATE paper_current SET group_id=? WHERE experiment_key=?', (ident, 'period-1'))
            db.execute('UPDATE paper_publications SET group_id=? WHERE experiment_key=?', (ident, 'period-1'))
        report = self.series.describe('series')
        self.assertEqual((report['valid'], report['counts']['review_required']), (1, 1))
        self.assertEqual(report['paired_description']['included'], 1)
        self.assertEqual(self.paper.process_one('period-1', legacy['source_snapshot_id'])['status'], 'complete')
        self.assertEqual(self.series.describe('series')['paired_description']['included'], 2)
        self.assertEqual(self.paper.get_group(ident), legacy)


if __name__ == '__main__':
    unittest.main()
