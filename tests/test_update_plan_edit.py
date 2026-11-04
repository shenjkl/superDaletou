from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from io import BytesIO
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dlt.integrity import check_database
from dlt.recovery_guard import RecoveryQuarantined
from dlt.scheduler import ScheduleError, Scheduler
from test_scheduler import configuration


class UpdatePlanEditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'research.sqlite3'
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.scheduler = Scheduler(self.path, clock=lambda: self.now)
        self.plan = self.scheduler.create_plan(configuration())

    def current(self):
        return next(p for p in self.scheduler.list_plans() if p['id'] == self.plan)

    def changed(self, **values):
        return {**configuration(), 'max_pages': 10, **values}

    def edit(self, config, sha=None):
        return self.scheduler.edit_plan(self.plan, config, expected_sha256=sha or self.current()['config_sha256'])

    def test_paused_edit_records_versions_and_old_and_new_run_configs(self):
        self.scheduler.set_enabled(self.plan, True)
        first = self.scheduler.claim(self.plan)
        self.scheduler._record_attempt(first, 1, 'failed', 'fixture')
        self.scheduler._finish(first, 'failed', error='fixture')
        self.scheduler.set_enabled(self.plan, False)
        old = self.current()
        new = self.changed(interval_seconds=120, max_pages=24)
        self.assertTrue(self.edit(new)['changed'])
        self.assertFalse(self.current()['enabled'])
        self.assertEqual(self.current()['next_due'], '2026-01-01T00:00:00+00:00')
        self.assertFalse(self.edit(new)['changed'])
        self.scheduler.set_enabled(self.plan, True)
        second = self.scheduler.claim(self.plan)
        self.assertEqual(second['config']['max_pages'], 24)
        self.scheduler._record_attempt(second, 1, 'failed', 'fixture')
        self.scheduler._finish(second, 'failed', error='fixture')
        runs = {r['id']: r for r in self.scheduler.list_runs(self.plan)}
        self.assertEqual({r['config_status'] for r in runs.values()}, {'recorded'})
        self.assertEqual(runs[first['run_id']]['recorded_config']['interval_seconds'], 60)
        self.assertEqual(runs[second['run_id']]['recorded_config']['interval_seconds'], 120)
        with closing(sqlite3.connect(self.path)) as db:
            change = db.execute('SELECT old_sha256,old_config,new_sha256,new_config FROM update_plan_changes').fetchone()
        self.assertEqual(change[0], old['config_sha256'])
        self.assertEqual(json.loads(change[1])['interval_seconds'], 60)
        self.assertEqual(json.loads(change[3])['interval_seconds'], 120)
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_rejects_enabled_running_stale_and_one_off_edits(self):
        sha = self.current()['config_sha256']
        self.scheduler.set_enabled(self.plan, True)
        with self.assertRaisesRegex(ScheduleError, '先暂停'):
            self.edit(self.changed(), sha)
        run = self.scheduler.claim(self.plan)
        self.scheduler.set_enabled(self.plan, False)
        with self.assertRaisesRegex(ScheduleError, '运行中'):
            self.edit(self.changed(), sha)
        self.scheduler._finish(run, 'failed', error='fixture')
        self.edit(self.changed(interval_seconds=120), sha)
        with self.assertRaisesRegex(ScheduleError, '已变化'):
            self.edit(self.changed(interval_seconds=180), sha)
        one = self.scheduler.create_one_off(self.plan)
        one_plan = next(p for p in self.scheduler.list_plans() if p['id'] == one)
        with self.assertRaisesRegex(ScheduleError, '一次性'):
            self.scheduler.edit_plan(one, self.changed(), expected_sha256=one_plan['config_sha256'])

    def test_two_simultaneous_edits_only_publish_one_revision(self):
        sha = self.current()['config_sha256']
        def edit(interval):
            try: return self.edit(self.changed(interval_seconds=interval), sha)
            except ScheduleError: return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(edit, (120, 180)))
        self.assertEqual(sum(item is not None for item in results), 1)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM update_plan_changes').fetchone()[0], 1)
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_active_run_config_damage_prevents_publishing(self):
        self.scheduler.set_enabled(self.plan, True)
        claim = self.scheduler.claim(self.plan)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER update_run_configs_no_update')
            db.execute("UPDATE update_run_configs SET config_json=x'7b7d' WHERE run_id=?", (claim['run_id'],))
        self.assertEqual(self.scheduler.list_runs(self.plan)[0]['config_status'], 'review_required')
        with self.assertRaisesRegex(ScheduleError, '运行配置记录不一致'):
            self.scheduler._finish(claim, 'success')

    def test_edit_and_change_record_roll_back_together(self):
        before = self.current()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("CREATE TRIGGER reject_edit BEFORE UPDATE ON update_plans BEGIN SELECT RAISE(ABORT, 'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.edit(self.changed(interval_seconds=120))
        self.assertEqual(self.current(), before)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM update_plan_changes').fetchone()[0], 0)

    def test_edited_plan_and_run_config_survive_isolated_copy_without_execution(self):
        from dlt.recovery import prepare_isolated_copy, protect_private_directory
        self.edit(self.changed(interval_seconds=120))
        self.scheduler.set_enabled(self.plan, True)
        claim = self.scheduler.claim(self.plan)
        self.scheduler._record_attempt(claim, 1, 'failed', 'fixture')
        self.scheduler._finish(claim, 'failed', error='fixture')
        protect_private_directory(self.path.parent)
        workspace = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir=workspace) as directory:
            from dlt.recovery import protect_private_directory
            protect_private_directory(directory)
            target = Path(directory) / 'candidate.sqlite3'
            before = hashlib.sha256(self.path.read_bytes()).hexdigest()
            self.assertEqual(prepare_isolated_copy(self.path, target)['status'], 'prepared')
            copy = Scheduler(target, clock=lambda: self.now)
            self.assertEqual(copy.list_changes(self.plan)[0]['new_config']['interval_seconds'], 120)
            self.assertEqual(copy.list_runs(self.plan)[0]['recorded_config']['interval_seconds'], 120)
            with self.assertRaises(RecoveryQuarantined):
                copy.claim(self.plan)
            self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)

    def test_rejects_bad_fields_and_recovery_quarantine_before_changes(self):
        for config in ({**self.changed(), 'interval_seconds': 0},
                       {**self.changed(), 'run_once': True},
                       {k:v for k,v in self.changed().items() if k != 'page_size'}):
            with self.subTest(config=config), self.assertRaises(ScheduleError):
                self.edit(config)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE recovery_quarantine(id INTEGER)')
        with self.assertRaises(RecoveryQuarantined):
            self.edit(self.changed())
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM update_plan_changes').fetchone()[0], 0)

    def test_missing_legacy_run_config_is_not_fabricated_and_corruption_is_detected(self):
        self.scheduler.set_enabled(self.plan, True)
        claim = self.scheduler.claim(self.plan)
        self.scheduler._finish(claim, 'failed', error='fixture')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER update_run_configs_no_delete')
            db.execute('DELETE FROM update_run_configs')
            db.execute("CREATE TRIGGER update_run_configs_no_delete BEFORE DELETE ON update_run_configs BEGIN SELECT RAISE(ABORT, 'immutable update run config'); END")
        self.assertEqual(self.scheduler.list_runs(self.plan)[0]['config_status'], 'not_recorded')
        self.scheduler.set_enabled(self.plan, False)
        self.edit(self.changed(interval_seconds=120))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER update_plan_changes_no_update')
            db.execute("UPDATE update_plan_changes SET new_config=x'7b7d'")
            db.execute("CREATE TRIGGER update_plan_changes_no_update BEFORE UPDATE ON update_plan_changes BEGIN SELECT RAISE(ABORT, 'immutable update plan change'); END")
        report = check_database(self.path)
        self.assertEqual(report['status'], 'failed')
        self.assertTrue(any(x['area'] == 'update_plan_changes' for x in report['errors']))
        self.assertEqual(self.scheduler.list_changes(self.plan)[0]['status'], 'review_required')
        with self.assertRaisesRegex(ScheduleError, '变更记录不一致'):
            self.scheduler.create_one_off(self.plan)
        self.scheduler.set_enabled(self.plan, True)
        with self.assertRaisesRegex(ScheduleError, '变更记录不一致'):
            self.scheduler.claim(self.plan)

    def test_route_enforces_local_session_and_csrf(self):
        from dlt.app import Handler
        def call(csrf):
            body = json.dumps({'plan_id': self.plan, 'config': self.changed(interval_seconds=120),
                               'expected_sha256': self.current()['config_sha256'],
                               'request_key': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'}).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', scheduler=self.scheduler)
            handler.path = '/api/update-plans/edit'
            handler.headers = {'Origin': handler.server.origin, 'X-DLT-CSRF': csrf,
                               'Content-Type': 'application/json', 'Content-Length': str(len(body))}
            handler.rfile = BytesIO(body)
            handler._validate_host = lambda: True
            handler._session = lambda: True
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        self.assertEqual(call('wrong')[0], 403)
        self.assertEqual(self.current()['config']['interval_seconds'], 60)
        self.assertTrue(call('secret')[1]['edit']['changed'])

    def test_plan_read_exposes_recorded_changes_without_starting_service(self):
        from dlt.app import Handler
        self.edit(self.changed(interval_seconds=120))
        handler = object.__new__(Handler)
        handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', db_path=self.path,
            scheduler=self.scheduler, update_loop=SimpleNamespace(status=lambda: {'state': 'running'}))
        handler.path = '/api/update-plans'
        handler._validate_host = lambda: True
        handler._session = lambda: True
        handler._json = lambda status, payload, **kwargs: (status, payload)
        with patch('dlt.app.autostart_status', return_value={'state': 'disabled'}):
            status, payload = handler.do_GET()
        self.assertEqual(status, 200)
        change = payload['plans'][0]['changes'][0]
        self.assertEqual(change['status'], 'recorded')
        self.assertEqual(change['old_config']['interval_seconds'], 60)
        self.assertEqual(change['new_config']['interval_seconds'], 120)


if __name__ == '__main__':
    unittest.main()
