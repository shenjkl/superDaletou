from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
import sqlite3
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch

from dlt.candidates import _canonical
from dlt.fixed_evaluation import EvaluationError
from dlt.holdout_reservation import ReservationError, list_reservations, reserved_issues
from dlt.holdout_audit import audit_holdout
from dlt.integrity import check_database
import test_fixed_evaluation as fixtures


class EvaluationSupersessionTests(unittest.TestCase):
    setUp = fixtures.FixedEvaluationTests.setUp
    freeze = fixtures.FixedEvaluationTests.freeze
    wait = fixtures.FixedEvaluationTests.wait

    def stale(self):
        with patch('dlt.fixed_evaluation._environment', return_value={'old': True}):
            return self.freeze()

    def supersede(self, reason='代码升级，重新核对协议'):
        return self.service.supersede_pending(self.reservation, reason=reason, confirmed=True)

    def test_stale_never_run_protocol_can_create_auditable_successor(self):
        old = self.stale()
        self.assertTrue(self.service.get(self.reservation)['can_supersede'])
        successor = self.supersede(); ident = successor['id']
        self.assertNotEqual(ident, self.reservation)
        self.assertEqual(successor['content']['supersedes'], self.reservation)
        self.assertEqual(successor['content']['supersedes_protocol_sha256'], old['protocol_sha256'])
        self.assertEqual(self.supersede()['id'], ident)
        with self.assertRaisesRegex(EvaluationError, '不同的新版本'):
            self.supersede('another reason')
        self.assertEqual(self.service.get(self.reservation)['superseded_by'], ident)
        self.assertEqual(self.service.get(self.reservation)['protocol_sha256'], old['protocol_sha256'])
        with patch('dlt.fixed_evaluation._environment', return_value={'old': True}):
            with self.assertRaisesRegex(EvaluationError, '已被新版本替代'):
                self.service.start(self.reservation, request_key='old-run')
        saved = self.service.get(ident)
        self.assertEqual(saved['status'], 'pending')
        self.assertEqual(saved['protocol']['methods'], old['protocol']['methods'])
        self.assertEqual(saved['protocol']['hypothesis'], old['protocol']['hypothesis'])
        self.assertEqual(saved['protocol']['scenarios'], old['protocol']['scenarios'])
        with self.assertRaisesRegex(EvaluationError, '不能替换'):
            self.service.freeze(ident, self.methods, hypothesis='不允许改变假设', timeout=30, confirmed=True)
        changed_methods = deepcopy(self.methods)
        changed_methods[0]['protocol']['seed'] += 1
        with self.assertRaisesRegex(EvaluationError, '不能替换'):
            self.service.freeze(ident, changed_methods, hypothesis=old['protocol']['hypothesis'], timeout=30, confirmed=True)
        self.service.start(ident, request_key='new-run')
        deadline = time.monotonic()+35
        while time.monotonic() < deadline:
            result = self.service.get(ident)
            if result['status'] != 'running':
                break
            time.sleep(.05)
        self.assertEqual(result['status'], 'complete', result['error'])
        self.assertEqual(result['validity'], 'local_descriptive')
        self.assertEqual(self.service.get(self.reservation)['status'], 'pending')
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_no_confirmation_or_unchanged_environment_cannot_create_successor(self):
        self.freeze()
        with self.assertRaises(EvaluationError):
            self.service.supersede_pending(self.reservation, reason='change', confirmed=False)
        with self.assertRaisesRegex(EvaluationError, '环境未变'):
            self.service.supersede_pending(self.reservation, reason='change', confirmed=True)
        self.assertEqual(len(list_reservations(self.path)), 1)

    def test_consumed_or_failed_evaluation_is_never_reclassified_unused(self):
        self.freeze(); self.service.start(self.reservation, request_key='first')
        final = self.wait()
        self.assertEqual(final['status'], 'complete', final['error'])
        with self.assertRaisesRegex(EvaluationError, '从未运行'):
            self.supersede()
        self.assertEqual(len(list_reservations(self.path)), 1)

    def test_failed_attempt_cannot_be_reclassified_as_unused(self):
        self.freeze()
        original = subprocess.Popen
        def fail_worker(*args, **kwargs):
            return original([sys.executable, '-c', 'raise SystemExit(1)'], **kwargs)
        with patch('dlt.fixed_evaluation.subprocess.Popen', side_effect=fail_worker):
            self.service.start(self.reservation, request_key='failed-run')
            result = self.wait()
        self.assertEqual(result['status'], 'failed')
        with self.assertRaisesRegex(EvaluationError, '从未运行'):
            self.supersede()
        self.assertEqual(len(list_reservations(self.path)), 1)

    def test_unknown_local_use_blocks_even_if_original_is_pending(self):
        self.stale()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS exploration_activities (id TEXT PRIMARY KEY, content_sha256 TEXT NOT NULL, content_json BLOB NOT NULL, status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT)')
            db.execute("INSERT INTO exploration_activities VALUES ('bad','bad',x'00','finished','2026-01-01','2026-01-01')")
        with self.assertRaisesRegex(EvaluationError, '已使用或无法核对'):
            self.supersede()
        self.assertEqual(len(list_reservations(self.path)), 1)

    def test_corrupt_pending_protocol_cannot_be_silently_ignored_in_audit(self):
        self.freeze()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_evaluations_fixed')
            db.execute("UPDATE fixed_evaluations SET protocol_json=x'00' WHERE reservation_id=?", (self.reservation,))
        audit = audit_holdout(self.path, self.snapshot, '26122', '26123')
        self.assertEqual(audit['status'], 'unresolved')
        self.assertTrue(any(item['kind'] == 'fixed_evaluation' for item in audit['unresolved']))

    def test_tampered_pending_targets_cannot_escape_fixed_scope(self):
        self.stale(); spec = deepcopy(self.service.get(self.reservation)['protocol'])
        spec['methods'][0]['protocol']['targets'] = ['26121', '26123']
        raw = _canonical(spec)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_evaluations_fixed')
            db.execute('UPDATE fixed_evaluations SET protocol_json=?,protocol_sha256=? WHERE reservation_id=?',
                       (raw, hashlib.sha256(raw).hexdigest(), self.reservation))
        audit = audit_holdout(self.path, self.snapshot, '26122', '26123')
        self.assertEqual(audit['status'], 'unresolved')
        with self.assertRaisesRegex(EvaluationError, '已使用或无法核对'):
            self.supersede()
        with patch('dlt.fixed_evaluation._environment', return_value={'old': True}):
            with self.assertRaisesRegex(EvaluationError, '固定范围不一致'):
                self.service.start(self.reservation, request_key='bad-scope')

    def test_tampered_budget_or_metric_cannot_start_or_count_as_unused(self):
        self.freeze(); original = self.service.get(self.reservation)['protocol']
        for field in ('budget', 'metric'):
            spec = deepcopy(original)
            if field == 'budget':
                spec['methods'][1]['protocol']['candidate']['budget_fen'] += 200
            else:
                spec['primary_metric'] = 'different_metric'
            raw = _canonical(spec)
            with self.subTest(field=field), closing(sqlite3.connect(self.path)) as db, db:
                db.execute('DROP TRIGGER IF EXISTS fixed_evaluations_fixed')
                db.execute('UPDATE fixed_evaluations SET protocol_json=?,protocol_sha256=? WHERE reservation_id=?',
                           (raw, hashlib.sha256(raw).hexdigest(), self.reservation))
            self.assertEqual(audit_holdout(self.path, self.snapshot, '26122', '26123')['status'], 'unresolved')
            with self.assertRaises(EvaluationError):
                self.service.start(self.reservation, request_key='bad-'+field)

    def test_pending_status_with_an_attempt_is_not_treated_as_unused(self):
        self.stale()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO fixed_evaluation_attempts VALUES (?, ?, 0, NULL, 0, 'failed', ?, ?, ?, NULL, NULL)",
                       ('unexpected-attempt', self.reservation, '2026-01-01T00:00:00+00:00',
                        '2026-01-01T00:00:01+00:00', 'synthetic failure'))
        audit = audit_holdout(self.path, self.snapshot, '26122', '26123')
        self.assertEqual(audit['status'], 'unresolved')
        with self.assertRaisesRegex(EvaluationError, '从未运行'):
            self.supersede()
        self.assertEqual(len(list_reservations(self.path)), 1)

    def test_pending_status_with_started_timestamp_is_not_treated_as_unused(self):
        self.stale()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE fixed_evaluations SET started_at=? WHERE reservation_id=?',
                       ('2026-01-01T00:00:00+00:00', self.reservation))
        self.assertFalse(self.service.get(self.reservation)['can_supersede'])
        self.assertEqual(audit_holdout(self.path, self.snapshot, '26122', '26123')['status'], 'unresolved')
        with self.assertRaisesRegex(EvaluationError, '从未运行'):
            self.supersede()

    def test_lineage_rejects_parent_changed_to_have_start_time(self):
        self.stale(); self.supersede()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE fixed_evaluations SET started_at=? WHERE reservation_id=?',
                       ('2026-01-01T00:00:00+00:00', self.reservation))
        with closing(sqlite3.connect(self.path)) as db:
            with self.assertRaises(ReservationError):
                reserved_issues(db)

    def test_sqlite_backup_retains_valid_version_chain(self):
        self.stale(); child = self.supersede()
        destination = self.path.with_name('isolated-copy.sqlite3')
        with closing(sqlite3.connect(self.path)) as source, closing(sqlite3.connect(destination)) as target:
            source.backup(target)
        copied = {x['id']: x for x in list_reservations(destination)}
        self.assertEqual(copied[self.reservation]['superseded_by'], child['id'])
        self.assertEqual(check_database(destination)['status'], 'passed')

    def test_tampered_successor_lineage_blocks_use_and_integrity(self):
        self.stale(); child = self.supersede()
        corrupt = deepcopy(child['content']); corrupt['issues'] = ['26123']
        corrupt['start_issue'] = '26123'
        raw = _canonical(corrupt); ident = hashlib.sha256(raw).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('INSERT INTO holdout_reservations VALUES (?,?,?)', (ident, raw, '2026-01-01T00:00:00+00:00'))
        with closing(sqlite3.connect(self.path)) as db:
            with self.assertRaises(ReservationError):
                reserved_issues(db)
        self.assertEqual(check_database(self.path)['status'], 'failed')

    def test_chained_unrun_successors_remain_auditable(self):
        self.stale(); first = self.supersede()
        with patch('dlt.fixed_evaluation._environment', return_value={'newer': True}):
            second = self.service.supersede_pending(first['id'], reason='再次升级', confirmed=True)
        self.assertEqual(second['content']['supersedes'], first['id'])
        self.assertEqual(self.service.get(first['id'])['superseded_by'], second['id'])
        self.assertEqual(self.service.get(second['id'])['protocol']['methods'], self.service.get(self.reservation)['protocol']['methods'])
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_changed_scenarios_cannot_be_laundered_into_successor(self):
        self.stale()
        with patch('dlt.fixed_evaluation.backtest_scenarios', return_value={'changed': True}):
            with self.assertRaisesRegex(EvaluationError, '奖金情景已变化'):
                self.supersede()
        self.assertEqual(len(list_reservations(self.path)), 1)

    def test_backwards_clock_cannot_publish_a_successor(self):
        self.stale()
        with patch('dlt.fixed_evaluation._now', return_value='2000-01-01T00:00:00+00:00'):
            with self.assertRaisesRegex(EvaluationError, '本机时间顺序异常'):
                self.supersede()
        self.assertEqual(len(list_reservations(self.path)), 1)

    def test_tampered_successor_method_is_detected(self):
        self.stale(); child = self.supersede()
        protocol = deepcopy(self.service.get(child['id'])['protocol'])
        protocol['methods'][0]['protocol']['candidate']['method'] = 'cold'
        raw = _canonical(protocol)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_evaluations_fixed')
            db.execute('UPDATE fixed_evaluations SET protocol_json=?,protocol_sha256=? WHERE reservation_id=?',
                       (raw, hashlib.sha256(raw).hexdigest(), child['id']))
        with closing(sqlite3.connect(self.path)) as db:
            with self.assertRaisesRegex(ReservationError, '未完整保留'):
                reserved_issues(db)
        self.assertEqual(check_database(self.path)['status'], 'failed')

    def test_start_and_supersede_have_one_atomic_winner(self):
        self.stale()
        barrier = threading.Barrier(2); local = threading.local()
        def environment():
            return {'old': True} if getattr(local, 'role', '') == 'start' else {'new': True}
        def start():
            local.role = 'start'; barrier.wait()
            try:
                self.service.start(self.reservation, request_key='racing-run')
                return 'started'
            except (EvaluationError, ReservationError):
                return 'blocked'
        def supersede():
            local.role = 'supersede'; barrier.wait()
            try:
                self.supersede()
                return 'superseded'
            except (EvaluationError, ReservationError):
                return 'blocked'
        with patch('dlt.fixed_evaluation._environment', side_effect=environment), ThreadPoolExecutor(max_workers=2) as pool:
            a, b = pool.submit(start), pool.submit(supersede)
            outcomes = [a.result(timeout=15), b.result(timeout=15)]
        self.assertIn(outcomes, [['started', 'blocked'], ['blocked', 'superseded']])
        reservations = list_reservations(self.path)
        self.assertEqual(len(reservations), 2 if outcomes[1] == 'superseded' else 1)


if __name__ == '__main__':
    unittest.main()
