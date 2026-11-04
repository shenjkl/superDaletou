from contextlib import closing
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dlt import autostart
from dlt.recovery_guard import RecoveryQuarantined
from dlt.recovery import protect_private_directory
from dlt.scheduler import Scheduler


class Key:
    def __enter__(self): return self
    def __exit__(self, *args): return False


class Registry:
    HKEY_CURRENT_USER = object()
    KEY_QUERY_VALUE, KEY_SET_VALUE, REG_SZ = 1, 2, 1
    def __init__(self): self.entry = None; self.exists = False; self.writes = 0
    def OpenKey(self, root, path, reserved, access):
        if not self.exists: raise FileNotFoundError()
        return Key()
    def CreateKeyEx(self, root, path, reserved, access):
        self.exists = True
        return Key()
    def QueryValueEx(self, key, name):
        if self.entry is None: raise FileNotFoundError()
        return self.entry
    def SetValueEx(self, key, name, reserved, kind, value):
        self.entry = (value, kind); self.writes += 1
    def DeleteValue(self, key, name):
        self.entry = None; self.writes += 1


class AutostartTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        protect_private_directory(self.tmp.name)
        self.db = Path(self.tmp.name) / '研究 数据.sqlite3'
        self.scheduler = Scheduler(self.db)
        self.scheduler.initialize()
        self.registry = Registry()
        self.reg_patch = patch('dlt.autostart._registry', return_value=self.registry)
        self.reg_patch.start(); self.addCleanup(self.reg_patch.stop)
        self.avail_patch = patch('dlt.autostart._available', return_value=True)
        self.avail_patch.start(); self.addCleanup(self.avail_patch.stop)

    def test_off_by_default_enable_and_disable_are_idempotent(self):
        self.assertEqual(autostart.status(self.db, 8766)['state'], 'disabled')
        self.assertEqual(self.registry.writes, 0)
        self.assertEqual(autostart.set_enabled(self.db, 8766, True)['state'], 'enabled')
        command = self.registry.entry[0]
        self.assertIn('pythonw.exe', command)
        self.assertIn('start-local.pyw', command)
        self.assertIn('研究 数据.sqlite3', command)
        self.assertEqual(autostart.status(self.db, 8766)['state'], 'enabled')
        autostart.set_enabled(self.db, 8766, True)
        self.assertEqual(self.registry.writes, 1)
        autostart.set_enabled(self.db, 8766, False)
        autostart.set_enabled(self.db, 8766, False)
        self.assertEqual(self.registry.writes, 2)
        self.assertEqual(autostart.status(self.db, 8766)['state'], 'disabled')

    def test_existing_other_configuration_is_never_overwritten_or_deleted(self):
        self.registry.exists = True; self.registry.entry = ('other command', self.registry.REG_SZ)
        self.assertEqual(autostart.status(self.db, 8766)['state'], 'conflict')
        for enabled in (True, False):
            with self.assertRaisesRegex(autostart.AutostartError, '其他启动配置'):
                autostart.set_enabled(self.db, 8766, enabled)
        self.assertEqual(self.registry.entry[0], 'other command')
        self.assertEqual(self.registry.writes, 0)

    def test_missing_launcher_blocks_enable_but_existing_owned_entry_can_be_removed(self):
        autostart.set_enabled(self.db, 8766, True)
        with patch('dlt.autostart._available', return_value=False):
            state = autostart.status(self.db, 8766)
            self.assertEqual(state['state'], 'enabled')
            self.assertIn('缺失', state['reason'])
            autostart.set_enabled(self.db, 8766, False)
            self.assertFalse(autostart.status(self.db, 8766)['can_enable'])
            with self.assertRaises(autostart.AutostartError):
                autostart.set_enabled(self.db, 8766, True)

    def test_quarantined_database_and_invalid_inputs_do_not_change_registry(self):
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute('CREATE TABLE recovery_quarantine (id INTEGER)')
        with self.assertRaises(RecoveryQuarantined):
            autostart.set_enabled(self.db, 8766, True)
        with self.assertRaises(autostart.AutostartError):
            autostart.set_enabled(self.db, 8766, 'true')
        self.assertEqual(self.registry.writes, 0)

    def test_permission_errors_are_reported_without_claiming_success(self):
        with patch.object(self.registry, 'CreateKeyEx', side_effect=PermissionError('fixture')):
            with self.assertRaisesRegex(autostart.AutostartError, '未保存'):
                autostart.set_enabled(self.db, 8766, True)
        with patch.object(self.registry, 'OpenKey', side_effect=PermissionError('fixture')):
            self.assertEqual(autostart.status(self.db, 8766)['state'], 'unavailable')

    def test_windows_run_command_length_limit_blocks_enable_without_writing(self):
        with patch('dlt.autostart._command', return_value='x' * 261):
            state = autostart.status(self.db, 8766)
            self.assertEqual(state['state'], 'disabled')
            self.assertFalse(state['can_enable'])
            self.assertIn('260', state['reason'])
            with self.assertRaisesRegex(autostart.AutostartError, '260'):
                autostart.set_enabled(self.db, 8766, True)
        self.assertEqual(self.registry.writes, 0)

    def test_launcher_delegates_to_app_with_existing_arguments_and_no_socket(self):
        launcher = Path(__file__).resolve().parents[1] / 'start-local.pyw'
        argv = [str(launcher), '--db', str(self.db), '--port', '8766']
        with patch.object(sys, 'argv', argv), patch.object(sys, 'path', list(sys.path)), \
                patch('dlt.app.main') as main, \
                patch('socket.socket', side_effect=AssertionError('socket must not be opened')):
            runpy.run_path(str(launcher), run_name='__main__')
            main.assert_called_once_with()
            self.assertEqual(sys.argv, argv)

    def test_route_checks_session_origin_and_csrf_before_setting(self):
        from dlt.app import Handler
        def call(enabled=True, *, origin='http://127.0.0.1:8766', csrf='secret', session=True):
            body = json.dumps({'enabled': enabled, 'port': 8766,
                               'request_key': ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' if enabled
                                               else 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb')}).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.db,
                                             scheduler=self.scheduler)
            handler.path = '/api/autostart'
            handler.headers = {'Origin': origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': csrf,
                               'Content-Type': 'application/json', 'Content-Length': str(len(body))}
            handler.rfile = BytesIO(body)
            handler._validate_host = lambda: True
            handler._session = lambda: session
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        for args in ({'csrf': 'bad'}, {'origin': 'https://example.com'}, {'session': False}):
            self.assertEqual(call(**args)[0], 403)
        self.assertEqual(self.registry.writes, 0)
        self.assertEqual(call('true')[0], 400)
        self.assertEqual(call(True)[1]['autostart']['state'], 'enabled')
        self.assertEqual(call(False)[1]['autostart']['state'], 'disabled')

    def test_plan_read_includes_startup_state_only_for_local_session(self):
        from dlt.app import Handler
        handler = object.__new__(Handler)
        handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', db_path=self.db,
            scheduler=SimpleNamespace(list_plans=lambda: [], list_runs=lambda _: []),
            update_loop=SimpleNamespace(status=lambda: {'state': 'running'}))
        handler.path = '/api/update-plans'
        handler._validate_host = lambda: True
        handler._session = lambda: False
        handler._json = lambda status, payload, **kwargs: (status, payload)
        self.assertEqual(handler.do_GET()[0], 401)
        handler._session = lambda: True
        status, payload = handler.do_GET()
        self.assertEqual(status, 200)
        self.assertEqual(payload['autostart']['state'], 'disabled')
        self.assertEqual(self.registry.writes, 0)


if __name__ == '__main__':
    unittest.main()
