from contextlib import closing
from copy import deepcopy
import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from dlt.integrity import check_database
from dlt.candidates import _canonical
import test_prospective_settlement as fixtures
import test_model_repository as model_fixtures
import test_evaluation_repository as evaluation_fixtures
import test_backtest_repository as backtest_fixtures


class IntegrityTests(unittest.TestCase):
    review = fixtures.PaperResultsTests.review
    import_draw = fixtures.PaperResultsTests.import_draw

    def setUp(self):
        fixtures.PaperResultsTests.setUp(self)
        self.snapshot = self.import_draw(self.draw)
        self.paper.process_one('exp', self.snapshot)

    def alter(self, trigger, sql, args=()):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER ' + trigger)
            db.execute(sql, args)

    def test_healthy_fixed_results_and_corrections_are_read_only(self):
        rule = deepcopy(self.frozen['rule_review']['extracted'])
        rule['version'] = 'corrected-v2'
        ident = self.review('rule', rule)
        correction = self.repo.declare_source_correction(self.rule_id, ident, note='fixture', confirmed=True)
        self.paper.process_source_correction(correction['correction_id'])
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)
        self.assertEqual(report['counts']['fixed_experiments'], 1)
        self.assertEqual(report['counts']['paper_groups'], 2)
        self.assertEqual(report['counts']['source_corrections'], 1)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)

    def test_draw_projection_tampering_is_detected(self):
        self.alter('draws_no_update', "UPDATE draw_versions SET front_json='[1,2,3,4,35]' WHERE snapshot_id=?", (self.snapshot,))
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed')
        self.assertIn('snapshots', [item['area'] for item in report['errors']])

    def test_snapshot_without_original_observation_is_detected(self):
        self.alter('observations_no_delete', 'DELETE FROM observations WHERE snapshot_id=?', (self.snapshot,))
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed')
        self.assertIn('snapshots', [item['area'] for item in report['errors']])

    def test_document_and_frozen_pair_tampering_are_detected(self):
        self.alter('reviewed_sources_no_update', "UPDATE reviewed_sources SET document=x'00' WHERE id=?", (self.rule_id,))
        self.alter('draft_pairs_no_update', "UPDATE draft_pairs SET pair_json=x'7b7d' WHERE request_key='pair-1'")
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed')
        self.assertTrue({'reviewed_sources', 'draft_pairs', 'fixed_experiments'} <= {x['area'] for x in report['errors']})

    def test_fixed_request_and_replica_manifest_are_checked_even_after_rehash(self):
        original = deepcopy(self.frozen)
        for kind in ('request', 'replica', 'missing_pair', 'duplicate_replica'):
            with self.subTest(kind=kind):
                content = deepcopy(original)
                request = deepcopy(content['protocol'])
                if kind == 'request':
                    request['hypothesis'] = 'different fixed request'
                elif kind == 'replica':
                    content['pairs'][0]['replica'] = 'wrong replica'
                elif kind == 'missing_pair':
                    content['pairs'].pop()
                else:
                    content['pairs'][1]['replica'] = content['pairs'][0]['replica']
                    content['protocol']['replicas'][1]['replica'] = content['protocol']['replicas'][0]['replica']
                    request = deepcopy(content['protocol'])
                raw = _canonical(content)
                with closing(sqlite3.connect(self.path)) as db, db:
                    db.execute('DROP TRIGGER fixed_experiments_no_update')
                    db.execute('UPDATE fixed_experiments SET request_json=?,content_sha256=?,content_json=? WHERE request_key=?',
                               (_canonical(request), hashlib.sha256(raw).hexdigest(), raw, 'exp'))
                    db.execute("CREATE TRIGGER fixed_experiments_no_update BEFORE UPDATE ON fixed_experiments BEGIN SELECT RAISE(ABORT, 'immutable fixed experiment'); END")
                report = check_database(self.path)
                self.assertEqual(report['status'], 'failed', report)
                self.assertTrue(any(item['area'] == 'fixed_experiments' for item in report['errors']), report)

    def test_missing_current_reference_is_detected_even_when_fk_was_disabled(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE paper_current SET group_id='missing'")
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed')
        self.assertTrue({'references', 'paper_current'} <= {x['area'] for x in report['errors']})

    def test_publication_chain_tampering_is_detected(self):
        self.alter('paper_publications_no_update', "UPDATE paper_publications SET previous_group='not-the-previous-result'")
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed')
        self.assertIn('paper_publications', [item['area'] for item in report['errors']])

    def test_legacy_missing_raw_bytes_is_incomplete(self):
        # Legacy schema acquired this nullable column through ALTER TABLE.
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER observations_no_update')
            db.execute('DROP TRIGGER observations_no_delete')
            db.execute('ALTER TABLE observations RENAME TO old_observations')
            db.execute('CREATE TABLE observations (snapshot_id TEXT NOT NULL REFERENCES snapshots(id), source TEXT NOT NULL, observed_at TEXT NOT NULL, raw_sha256 TEXT NOT NULL, raw_payload BLOB, PRIMARY KEY(snapshot_id,source,observed_at))')
            db.execute('INSERT INTO observations SELECT snapshot_id,source,observed_at,raw_sha256,NULL FROM old_observations')
            db.execute('DROP TABLE old_observations')
            db.execute("CREATE TRIGGER observations_no_update BEFORE UPDATE ON observations BEGIN SELECT RAISE(ABORT, 'immutable observation'); END")
            db.execute("CREATE TRIGGER observations_no_delete BEFORE DELETE ON observations BEGIN SELECT RAISE(ABORT, 'immutable observation'); END")
        report = check_database(self.path)
        self.assertEqual(report['status'], 'incomplete', report)
        self.assertTrue(report['limitations'])

    def test_missing_or_replaced_guards_fail_even_if_data_are_unchanged(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_experiments_no_update')
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed', report)
        self.assertIn('schema', [item['area'] for item in report['errors']])
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TRIGGER fixed_experiments_no_update BEFORE UPDATE ON fixed_experiments BEGIN SELECT 1; END')
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed', report)
        self.assertIn('schema', [item['area'] for item in report['errors']])

    def test_missing_followup_trigger_and_unknown_trigger_are_not_passed(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER source_corrections_queue')
            db.execute('CREATE TRIGGER custom_trigger AFTER INSERT ON paper_groups BEGIN SELECT 1; END')
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed', report)
        self.assertIn('schema', [item['area'] for item in report['errors']])
        self.assertIn('unknown triggers are present and were not checked', report['limitations'])

    def test_unknown_trigger_alone_makes_check_incomplete(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TRIGGER custom_trigger AFTER INSERT ON paper_groups BEGIN SELECT 1; END')
        report = check_database(self.path)
        self.assertEqual(report['status'], 'incomplete', report)

    def test_reconstructed_table_without_foreign_key_is_detected(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER observations_no_update')
            db.execute('DROP TRIGGER observations_no_delete')
            db.execute('ALTER TABLE observations RENAME TO old_observations')
            db.execute('CREATE TABLE observations (snapshot_id TEXT NOT NULL, source TEXT NOT NULL, observed_at TEXT NOT NULL, raw_sha256 TEXT NOT NULL, raw_payload BLOB NOT NULL, PRIMARY KEY(snapshot_id,source,observed_at))')
            db.execute('INSERT INTO observations SELECT * FROM old_observations')
            db.execute('DROP TABLE old_observations')
            db.execute("CREATE TRIGGER observations_no_update BEFORE UPDATE ON observations BEGIN SELECT RAISE(ABORT, 'immutable observation'); END")
            db.execute("CREATE TRIGGER observations_no_delete BEFORE DELETE ON observations BEGIN SELECT RAISE(ABORT, 'immutable observation'); END")
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed', report)
        self.assertTrue(any(item['reason'] == 'primary or foreign key contract differs' for item in report['errors']))

    def test_missing_unique_constraint_is_detected(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER series_periods_no_update')
            db.execute('DROP TRIGGER series_periods_no_delete')
            db.execute('DROP TABLE series_periods')
            db.execute('CREATE TABLE series_periods (series_key TEXT NOT NULL REFERENCES experiment_series(series_key), issue TEXT NOT NULL, experiment_key TEXT NOT NULL REFERENCES fixed_experiments(request_key), PRIMARY KEY(series_key,issue))')
            db.execute("CREATE TRIGGER series_periods_no_update BEFORE UPDATE ON series_periods BEGIN SELECT RAISE(ABORT, 'immutable series period'); END")
            db.execute("CREATE TRIGGER series_periods_no_delete BEFORE DELETE ON series_periods BEGIN SELECT RAISE(ABORT, 'immutable series period'); END")
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed', report)
        self.assertTrue(any(item['reason'] == 'required uniqueness constraint missing' for item in report['errors']))

    def test_unknown_schema_and_non_database_never_claim_success(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE future_feature (x TEXT)')
        self.assertEqual(check_database(self.path)['status'], 'incomplete')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'invalid.sqlite3'
            path.write_bytes(b'not a database')
            self.assertEqual(check_database(path)['status'], 'failed')
            self.assertEqual(check_database(path.with_name('missing'))['status'], 'failed')


class OtherRecordIntegrityTests(unittest.TestCase):
    def test_trained_artifact_evaluation_and_backtest_records(self):
        model = model_fixtures.ModelRepositoryTests('test_real_training_artifact_survives_reopen_and_replays')
        model.setUp()
        self.addCleanup(model.tearDown)
        model.repo.train_from_snapshot(model.snapshot_id, model_fixtures.config(model.snapshot_id), first_issue='26001')
        result = check_database(model.path)
        self.assertEqual(result['status'], 'passed', result)
        self.assertEqual(result['counts']['model_artifacts'], 1)

        evaluation = evaluation_fixtures.EvaluationRepositoryTests('test_restart_and_idempotency')
        evaluation.setUp()
        self.addCleanup(evaluation.doCleanups)
        evaluation.repo.run('evaluation', 'pair', **evaluation.args)
        result = check_database(evaluation.path)
        self.assertEqual(result['status'], 'passed', result)
        self.assertEqual(result['counts']['draft_evaluations'], 1)

        backtest = backtest_fixtures.BacktestRepositoryTests('test_partial_report_persists_with_all_failures_and_idempotency')
        backtest.setUp()
        self.addCleanup(backtest.doCleanups)
        backtest.repo.run('run-1', backtest.protocol, backtest.scenarios)
        result = check_database(backtest.path)
        self.assertEqual(result['status'], 'passed', result)
        self.assertEqual(result['counts']['backtest_reports'], 1)


if __name__ == '__main__':
    unittest.main()
