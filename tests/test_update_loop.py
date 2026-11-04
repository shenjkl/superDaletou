from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from dlt.app import SingleInstance, UpdateLoop
from dlt.scheduler import Scheduler
from test_backtest import source_data


class UpdateLoopTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.scheduler = Scheduler(Path(self.tmp.name)/'research.sqlite3')
        self.scheduler.initialize()

    def test_runs_without_browser_and_stops(self):
        called = threading.Event()
        loop = UpdateLoop(self.scheduler)
        with patch.object(self.scheduler, 'run_due', side_effect=lambda: called.set()):
            loop.start()
            self.assertTrue(called.wait(5))
            self.assertEqual(loop.status()['state'], 'running')
            for _ in range(100):
                if loop.status()['last_check']:
                    break
                time.sleep(.01)
            self.assertIsNotNone(loop.status()['last_check'])
            loop.stop()
        self.assertEqual(loop.status()['state'], 'stopped')

    def test_existing_worker_lock_prevents_duplicate_loop(self):
        called = threading.Event()
        loop = UpdateLoop(self.scheduler)
        with SingleInstance(str(self.scheduler.path)+'.update'):
            with patch.object(self.scheduler, 'run_due', side_effect=lambda: called.set()):
                loop.start()
                for _ in range(100):
                    if loop.status()['state'] == 'occupied':
                        break
                    time.sleep(.01)
                self.assertEqual(loop.status()['state'], 'occupied')
                self.assertFalse(called.is_set())
                loop.stop()
        self.assertEqual(loop.status()['state'], 'stopped')

    def test_enabled_plan_imports_and_records_without_browser(self):
        config = {'start_at': (datetime.now(timezone.utc)-timedelta(seconds=2)).isoformat(),
                  'interval_seconds': 3600, 'timezone_offset_minutes': 480, 'page_size': 20,
                  'timeout_seconds': 2, 'max_bytes': 1048576, 'retry_count': 0, 'retry_wait_seconds': 1}
        plan = self.scheduler.create_plan(config)
        self.scheduler.set_enabled(plan, True)
        original_execute = self.scheduler.execute
        def fake_source(store, *, page_size, timeout_seconds, max_bytes, transaction_guard):
            return store.import_official(json.dumps(source_data()).encode(), 'synthetic automatic update',
                                         datetime.now(timezone.utc).isoformat(), transaction_guard=transaction_guard)
        loop = UpdateLoop(self.scheduler)
        with patch.object(self.scheduler, 'execute', side_effect=lambda claim: original_execute(claim, updater=fake_source)):
            try:
                loop.start()
                for _ in range(200):
                    runs = self.scheduler.list_runs(plan)
                    if runs and runs[-1]['status'] == 'success' and runs[-1]['followups']:
                        break
                    time.sleep(.02)
                self.assertTrue(runs)
                self.assertEqual(runs[-1]['status'], 'success')
                self.assertTrue(runs[-1]['snapshot_id'])
                self.assertEqual(runs[-1]['followups'][0]['status'], 'skipped')
            finally:
                loop.stop()


if __name__ == '__main__':
    unittest.main()
