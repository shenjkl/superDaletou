from contextlib import closing
import hashlib
from pathlib import Path
import sqlite3
import shutil
import tempfile
import subprocess
import sys
import os
import json
from io import StringIO
from contextlib import redirect_stderr
import unittest
from unittest.mock import patch

from dlt.integrity import check_database
from dlt.recovery import RecoveryError, _publish_staged, prepare_isolated_copy
from dlt.recovery_guard import RecoveryQuarantined, require_path_execution_allowed
from dlt.scheduler import Scheduler
import test_prospective_settlement as fixtures
from test_scheduler import configuration


class IsolatedRecoveryTests(unittest.TestCase):
    review = fixtures.PaperResultsTests.review
    import_draw = fixtures.PaperResultsTests.import_draw

    def setUp(self):
        fixtures.PaperResultsTests.setUp(self)
        self.snapshot = self.import_draw(self.draw)
        self.paper.process_one('exp', self.snapshot)
        workspace = Path(__file__).resolve().parents[1]
        self.destination_dir = tempfile.TemporaryDirectory(dir=workspace)
        root = Path(self.destination_dir.name).resolve()
        self.assertTrue(root.is_relative_to(workspace))
        self.addCleanup(self.destination_dir.cleanup)
        self.target = root / 'isolated.sqlite3'

    def test_snapshot_preserves_results_without_modifying_source(self):
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        report = prepare_isolated_copy(self.path, self.target)
        self.assertEqual(report['status'], 'prepared')
        self.assertEqual(report['check']['counts']['paper_groups'], 1)
        self.assertEqual(check_database(self.target)['status'], 'passed')
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        with closing(sqlite3.connect(self.target)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM paper_current').fetchone()[0], 1)
        self.assertEqual(list(self.target.parent.glob('.dlt-recovery-*.sqlite3')), [])

    def test_real_publication_within_workspace(self):
        workspace = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir=workspace) as directory:
            root = Path(directory).resolve()
            self.assertTrue(root.is_relative_to(workspace))
            target = root / 'isolated.sqlite3'
            before = hashlib.sha256(self.path.read_bytes()).hexdigest()
            report = prepare_isolated_copy(self.path, target)
            self.assertEqual(report['status'], 'prepared')
            self.assertEqual(check_database(target)['status'], 'passed')
            self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
            self.assertEqual(list(root.glob('.dlt-recovery-*.sqlite3')), [])

    def test_cli_prepares_checked_copy_and_refuses_second_publication(self):
        command = [sys.executable, '-m', 'dlt.recovery', str(self.path), str(self.target)]
        environment = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
        result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', env=environment, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'prepared')
        before = hashlib.sha256(self.target.read_bytes()).hexdigest()
        repeated = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', env=environment, timeout=30)
        self.assertEqual(repeated.returncode, 2, repeated.stderr)
        self.assertEqual(json.loads(repeated.stdout)['status'], 'failed')
        self.assertEqual(hashlib.sha256(self.target.read_bytes()).hexdigest(), before)

    def test_copy_retains_enabled_plan_but_does_not_execute_it(self):
        scheduler = Scheduler(self.path)
        plan = scheduler.create_plan(configuration())
        scheduler.set_enabled(plan, True)
        report = prepare_isolated_copy(self.path, self.target)
        self.assertEqual(report['status'], 'prepared')
        with closing(sqlite3.connect(self.path)) as source, closing(sqlite3.connect(self.target)) as copy:
            self.assertEqual(copy.execute('SELECT enabled FROM update_plans WHERE id=?', (plan,)).fetchone(), (1,))
            self.assertEqual(source.execute('SELECT count(*) FROM update_runs').fetchone()[0], 0)
            self.assertEqual(copy.execute('SELECT count(*) FROM update_runs').fetchone()[0], 0)
        self.assertTrue(any('no schedules' in note for note in report['limitations']))

    def test_candidate_blocks_service_workers_and_plan_execution_before_network(self):
        from dlt.app import LocalServer
        from dlt.rolling_training import RollingTraining
        from dlt.rolling_registration import RollingRegistration
        from dlt.static_registration import StaticRegistration
        scheduler = Scheduler(self.path)
        plan = scheduler.create_plan(configuration())
        scheduler.set_enabled(plan, True)
        prepare_isolated_copy(self.path, self.target)
        copy_scheduler = Scheduler(self.target)
        with patch('socket.socket', side_effect=AssertionError('socket must not be opened')):
            with self.assertRaises(RecoveryQuarantined):
                LocalServer(0, self.target)
            for action in (lambda: copy_scheduler.claim(plan), copy_scheduler.run_due,
                           copy_scheduler.process_pending_followups,
                           lambda: copy_scheduler.execute({'config': {}}),
                           lambda: copy_scheduler.set_enabled(plan, True),
                           lambda: copy_scheduler.create_one_off(plan),
                           RollingTraining(self.target, object()).tick,
                           RollingRegistration(self.target, object()).tick,
                           StaticRegistration(self.target).tick):
                with self.subTest(action=action), self.assertRaises(RecoveryQuarantined):
                    action()
        with closing(sqlite3.connect(self.target)) as db:
            self.assertEqual(db.execute('SELECT enabled FROM update_plans WHERE id=?', (plan,)).fetchone(), (1,))
            self.assertEqual(db.execute('SELECT count(*) FROM update_runs').fetchone()[0], 0)
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM recovery_quarantine')
        self.assertEqual(check_database(self.target)['status'], 'passed')

    def test_empty_or_damaged_quarantine_marker_still_blocks_execution(self):
        prepare_isolated_copy(self.path, self.target)
        with closing(sqlite3.connect(self.target)) as db, db:
            db.execute('DROP TRIGGER recovery_quarantine_no_delete')
            db.execute('DELETE FROM recovery_quarantine')
        with self.assertRaises(RecoveryQuarantined):
            require_path_execution_allowed(self.target)
        self.assertEqual(check_database(self.target)['status'], 'failed')

    def test_worker_cli_rejects_quarantine_without_starting_worker(self):
        from dlt import worker
        prepare_isolated_copy(self.path, self.target)
        with patch('sys.argv', ['worker', '--db', str(self.target), 'run']), \
                patch('socket.socket', side_effect=AssertionError('socket must not be opened')):
            with self.assertRaises(RecoveryQuarantined):
                worker.main()

    def test_quarantine_blocks_direct_job_entries_and_interruption_recovery(self):
        from dlt.training_jobs import TrainingJobs
        from dlt.backtest_jobs import BacktestJobs
        from dlt.fixed_evaluation import FixedEvaluations
        from dlt.model_repository import ModelRepository
        prepare_isolated_copy(self.path, self.target)
        before = hashlib.sha256(self.target.read_bytes()).hexdigest()
        training = TrainingJobs(self.target)
        backtests = BacktestJobs(self.target)
        evaluations = FixedEvaluations(self.target)
        actions = (lambda: training.submit('x', name='x', config={}, first_issue='26001', timeout=10),
                   training.recover_interrupted, lambda: backtests.submit('x', {}, 10),
                   backtests.recover_interrupted, lambda: evaluations.start('x'),
                   evaluations.recover_interrupted,
                   lambda: ModelRepository(self.target).train_from_snapshot('x', {}, first_issue='26001'))
        with patch('subprocess.Popen', side_effect=AssertionError('worker must not start')):
            for action in actions:
                with self.subTest(action=action), self.assertRaises(RecoveryQuarantined):
                    action()
        self.assertEqual(hashlib.sha256(self.target.read_bytes()).hexdigest(), before)
        self.assertEqual(check_database(self.target)['status'], 'passed')

    def test_quarantine_blocks_direct_computation_workers(self):
        from dlt import training_worker, backtest_worker, fixed_evaluation_worker
        prepare_isolated_copy(self.path, self.target)
        request = json.dumps({'path': str(self.target)})
        with patch('sys.stdin', StringIO(request)), redirect_stderr(StringIO()) as error:
            self.assertEqual(training_worker.main(), 1)
            self.assertIn('隔离恢复候选', error.getvalue())
        for module in (backtest_worker, fixed_evaluation_worker):
            with self.subTest(worker=module.__name__), patch('sys.stdin', StringIO(request)), \
                    self.assertRaises(RecoveryQuarantined):
                module.main()

    def test_interrupted_recovery_does_not_rewrite_retained_running_jobs(self):
        from dlt.training_jobs import TrainingJobs
        from dlt.backtest_jobs import BacktestJobs
        TrainingJobs(self.path).initialize()
        BacktestJobs(self.path).initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO training_jobs VALUES ('retained-train',?, 'fixture', 'running', '2026-01-01', NULL, NULL, NULL)", (b'{}',))
            db.execute("INSERT INTO backtest_jobs VALUES ('retained-backtest',?, 'running', '2026-01-01', NULL, NULL)", (b'{}',))
        prepare_isolated_copy(self.path, self.target)
        for jobs in (TrainingJobs(self.target), BacktestJobs(self.target)):
            with self.assertRaises(RecoveryQuarantined):
                jobs.recover_interrupted()
        with closing(sqlite3.connect(self.target)) as db:
            self.assertEqual(db.execute("SELECT status,finished_at,error FROM training_jobs WHERE request_key='retained-train'").fetchone(), ('running', None, None))
            self.assertEqual(db.execute("SELECT status,finished_at,error FROM backtest_jobs WHERE request_key='retained-backtest'").fetchone(), ('running', None, None))

    def test_app_cli_rejects_quarantine_before_lock_or_socket(self):
        from dlt import app
        prepare_isolated_copy(self.path, self.target)
        with patch('sys.argv', ['app', '--db', str(self.target), '--port', '8766']), \
                patch('socket.socket', side_effect=AssertionError('socket must not be opened')):
            with self.assertRaises(RecoveryQuarantined):
                app.main()
        self.assertFalse(Path(str(self.target)+'.app.lock').exists())

    def test_uncheckpointed_wal_is_included_in_consistent_copy(self):
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
            db.execute("INSERT INTO paper_events VALUES ('wal-event','exp',?,'fixture','test','2026-01-01')", (self.snapshot,))
            db.commit()
            self.assertTrue(Path(str(self.path)+'-wal').exists())
            report = prepare_isolated_copy(self.path, self.target)
            self.assertEqual(report['status'], 'prepared')
            with closing(sqlite3.connect(self.target)) as copied:
                self.assertEqual(copied.execute("SELECT count(*) FROM paper_events WHERE id='wal-event'").fetchone()[0], 1)

    def test_cleanup_failure_is_reported_after_publication(self):
        real_unlink = Path.unlink
        def refuse_staging(path, *args, **kwargs):
            if path.name.startswith('.dlt-recovery-'):
                raise PermissionError('fixture')
            return real_unlink(path, *args, **kwargs)
        def retain_staging(src, dst):
            shutil.copyfile(src, dst)  # Simulate a successful publication that retains staging.
        with patch('dlt.recovery._publish_staged', side_effect=retain_staging), patch('dlt.recovery.Path.unlink', refuse_staging):
            report = prepare_isolated_copy(self.path, self.target)
        self.assertEqual(report['status'], 'prepared')
        self.assertEqual(report['staging_cleanup'], 'failed')
        self.assertTrue(self.target.exists())
        for path in self.target.parent.glob('.dlt-recovery-*.sqlite3'):
            path.unlink()

    def test_existing_target_is_never_overwritten(self):
        self.target.write_bytes(b'existing target')
        with self.assertRaises(RecoveryError):
            prepare_isolated_copy(self.path, self.target)
        self.assertEqual(self.target.read_bytes(), b'existing target')

    def test_actual_atomic_publication_does_not_replace_existing_file(self):
        staged = self.target.parent / 'existing-staged.sqlite3'
        staged.write_bytes(b'candidate')
        self.target.write_bytes(b'preexisting')
        with self.assertRaises(FileExistsError):
            _publish_staged(staged, self.target)
        self.assertEqual(self.target.read_bytes(), b'preexisting')
        self.assertEqual(staged.read_bytes(), b'candidate')

    def test_existing_wal_or_other_sidecar_blocks_copy(self):
        for suffix in ('-wal', '-shm', '-journal'):
            sidecar = Path(str(self.target) + suffix)
            sidecar.write_bytes(b'preexisting log')
            with self.assertRaises(RecoveryError):
                prepare_isolated_copy(self.path, self.target)
            self.assertFalse(self.target.exists())
            self.assertEqual(sidecar.read_bytes(), b'preexisting log')
            sidecar.unlink()

    def test_incomplete_source_is_not_published(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE unsupported_feature (value TEXT)')
        with self.assertRaises(RecoveryError):
            prepare_isolated_copy(self.path, self.target)
        self.assertFalse(self.target.exists())

    def test_corrupt_or_unrecognized_source_is_not_published(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_experiments_no_update')
        with self.assertRaises(RecoveryError) as error:
            prepare_isolated_copy(self.path, self.target)
        self.assertFalse(self.target.exists())
        self.assertEqual(error.exception.check['status'], 'failed')
        self.assertTrue(any(x['area'] == 'schema' for x in error.exception.check['errors']))
        self.assertEqual(list(self.target.parent.glob('.dlt-recovery-*.sqlite3')), [])

    def test_inconsistent_fixed_replica_blocks_isolated_publication_even_after_rehash(self):
        from dlt.candidates import _canonical
        content = dict(self.frozen)
        content['pairs'] = [dict(item) for item in content['pairs']]
        content['pairs'][0]['replica'] = 'inconsistent'
        raw = _canonical(content)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_experiments_no_update')
            db.execute('UPDATE fixed_experiments SET content_sha256=?,content_json=? WHERE request_key=?',
                       (hashlib.sha256(raw).hexdigest(), raw, 'exp'))
            db.execute("CREATE TRIGGER fixed_experiments_no_update BEFORE UPDATE ON fixed_experiments BEGIN SELECT RAISE(ABORT, 'immutable fixed experiment'); END")
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        with self.assertRaises(RecoveryError) as error:
            prepare_isolated_copy(self.path, self.target)
        self.assertFalse(self.target.exists())
        self.assertTrue(any(item['area'] == 'fixed_experiments' for item in error.exception.check['errors']))
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)

    def test_size_limit_stops_publication(self):
        with patch('dlt.recovery.MAX_DATABASE_BYTES', 1):
            with self.assertRaises(RecoveryError):
                prepare_isolated_copy(self.path, self.target)
        self.assertFalse(self.target.exists())

    def test_target_created_during_publication_is_not_replaced(self):
        def concurrent_create(staged, destination):
            Path(destination).write_bytes(b'concurrent file')
            _publish_staged(staged, destination)
        with patch('dlt.recovery._publish_staged', side_effect=concurrent_create):
            with self.assertRaises(RecoveryError):
                prepare_isolated_copy(self.path, self.target)
        self.assertEqual(self.target.read_bytes(), b'concurrent file')
        self.assertEqual(list(self.target.parent.glob('.dlt-recovery-*.sqlite3')), [])

    def test_denied_publication_keeps_source_and_leaves_no_partial_target(self):
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        with patch('dlt.recovery._publish_staged', side_effect=PermissionError('denied')):
            with self.assertRaises(RecoveryError):
                prepare_isolated_copy(self.path, self.target)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        self.assertFalse(self.target.exists())
        self.assertEqual(list(self.target.parent.glob('.dlt-recovery-*.sqlite3')), [])


if __name__ == '__main__':
    unittest.main()
