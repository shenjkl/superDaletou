from contextlib import closing
from copy import deepcopy
from decimal import Decimal
import hashlib
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dlt.backtest import _run_from_snapshot
from dlt.candidates import _canonical
from dlt.fixed_evaluation import EvaluationError, FixedEvaluations
from dlt.holdout_reservation import reserve_range
from dlt.integrity import check_database
from dlt.recovery import RecoveryError, prepare_isolated_copy
from dlt.snapshots import SnapshotStore
from test_backtest import protocol


class FixedEvaluationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'research.sqlite3'
        raw = (Path(__file__).resolve().parents[1]/'research/data/official_page1.json').read_bytes()
        self.snapshot = SnapshotStore(self.path).import_official(raw, 'local fixture', '2026-10-29T12:00:00+08:00')['snapshot_id']
        self.reservation = reserve_range(self.path, self.snapshot, '26122', '26123', confirmed=True)['id']
        self.service = FixedEvaluations(self.path); self.addCleanup(self.service.stop)
        base = protocol(self.snapshot)
        base.update(targets=['26122', '26123'], replicas=['a', 'b'], history_periods=12)
        base['candidate'].update(budget_fen=400, max_tickets=10)
        other = deepcopy(base); other['candidate']['method'] = 'cold'
        self.methods = [{'id': 'hot', 'protocol': base}, {'id': 'cold', 'protocol': other}]

    def freeze(self, methods=None, hypothesis='固定比较热号与冷号'):
        return self.service.freeze(self.reservation, methods or self.methods, hypothesis=hypothesis, timeout=30, confirmed=True)

    def wait(self):
        until = time.monotonic()+35
        while time.monotonic() < until:
            result = self.service.get(self.reservation)
            if result['status'] != 'running':
                return result
            time.sleep(.05)
        self.fail('evaluation did not finish')

    def test_multiple_methods_are_fixed_once_and_run_as_descriptive_reconstruction(self):
        fixed = self.freeze()
        self.assertEqual(fixed['status'], 'pending')
        self.assertEqual(fixed['protocol']['independence'], 'not_verified')
        self.assertEqual(self.freeze()['protocol_sha256'], fixed['protocol_sha256'])
        with self.assertRaisesRegex(EvaluationError, '不能替换'):
            self.freeze(hypothesis='after result changed')
        self.service.start(self.reservation, request_key='initial')
        result = self.wait()
        self.assertEqual(result['status'], 'complete', result['error'])
        self.assertEqual([x['id'] for x in result['report']['methods']], ['hot', 'cold'])
        for method in result['report']['methods']:
            report = method['result']
            self.assertEqual((report['planned_periods'], report['complete_periods']), (2, 2))
            self.assertEqual([x['target']['issue'] for x in report['periods']], ['26122', '26123'])
            self.assertEqual(report['periods'][1]['replicas'][0]['pair']['history_last'], '26122')
            self.assertIn('not independently verified', result['report']['evidence'])
        self.assertEqual(self.service.start(self.reservation, request_key='initial')['report_sha256'], result['report_sha256'])
        with self.assertRaisesRegex(EvaluationError, '已运行或完成'):
            self.service.start(self.reservation, request_key='unrelated')
        self.assertEqual(result['validity'], 'local_descriptive')
        self.assertEqual(set(result['paired_descriptions']), {'hot', 'cold'})
        self.assertTrue(all(item['included'] == 2 for item in result['paired_descriptions'].values()))
        self.assertTrue(all('paired_description' not in item['result'] for item in result['report']['methods']))
        check = check_database(self.path)
        self.assertEqual(check['status'], 'passed', check)
        with closing(sqlite3.connect(self.path)) as db, self.assertRaises(sqlite3.IntegrityError):
            db.execute("UPDATE fixed_evaluations SET status='pending'")

    def test_legacy_fixed_report_with_wrong_prize_is_withdrawn_from_descriptive_results(self):
        self.freeze()
        self.service.start(self.reservation, request_key='legacy-precision')
        result = self.wait()
        self.assertEqual(result['status'], 'complete', result['error'])
        report = deepcopy(result['report'])
        method = report['methods'][0]['result']
        method.pop('calculation_version'); method.pop('settlement_code_sha256')
        method['code_sha256'] = '0' * 64
        row = method['periods'][0]['replicas'][0]['candidate_settlement']
        row['gross'] = str(Decimal(row['gross']) + 1)
        row['tax'] = str(Decimal(row['tax']) + 1)
        raw = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_evaluations_terminal')
            db.execute('UPDATE fixed_evaluations SET report_json=?,report_sha256=? WHERE reservation_id=?',
                       (raw, hashlib.sha256(raw).hexdigest(), self.reservation))
        state = self.service.get(self.reservation)
        self.assertEqual(state['validity'], 'review_required')
        self.assertIn('精确计算不一致', state['validity_reason'])
        self.assertNotIn('paired_descriptions', state)
        self.assertEqual(state['report'], report)

    def test_fixed_amount_correction_is_append_only_reproducible_and_idempotent(self):
        self.freeze()
        self.service.start(self.reservation, request_key='before-correction')
        finished = self.wait()
        self.assertEqual(finished['status'], 'complete', finished['error'])
        legacy = deepcopy(finished['report'])
        result = legacy['methods'][0]['result']
        result.pop('calculation_version'); result.pop('settlement_code_sha256')
        result['code_sha256'] = '0' * 64
        row = result['periods'][0]['replicas'][0]['candidate_settlement']
        row['gross'] = str(Decimal(row['gross']) + 1)
        row['tax'] = str(Decimal(row['tax']) + 1)
        raw = _canonical(legacy); old_sha = hashlib.sha256(raw).hexdigest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_evaluations_terminal')
            db.execute('DROP TRIGGER fixed_evaluation_attempts_terminal')
            db.execute('UPDATE fixed_evaluations SET report_json=?,report_sha256=? WHERE reservation_id=?',
                       (raw, old_sha, self.reservation))
            db.execute('UPDATE fixed_evaluation_attempts SET report_json=?,report_sha256=? WHERE request_key=?',
                       (raw, old_sha, 'before-correction'))
        old_state = self.service.get(self.reservation)
        self.assertEqual(old_state['validity'], 'review_required')
        self.assertTrue(old_state['can_recalculate_amounts'])
        with patch.object(self.service, '_get', return_value={'validity': 'review_required'}):
            with self.assertRaisesRegex(EvaluationError, '未发布新结果'):
                self.service.recalculate_amounts(self.reservation)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM fixed_evaluation_corrections').fetchone()[0], 0)
        with patch('dlt.backtest._generate', side_effect=AssertionError('must not retrain')):
            corrected = self.service.recalculate_amounts(self.reservation)
        self.assertEqual(corrected['validity'], 'local_descriptive', corrected.get('validity_reason'))
        self.assertEqual(corrected['original_report'], legacy)
        self.assertEqual(corrected['report'], finished['report'] | {'methods': corrected['report']['methods']})
        self.assertEqual(corrected['report']['methods'][0]['result']['periods'][0]['replicas'][0]['candidate_settlement'],
                         finished['report']['methods'][0]['result']['periods'][0]['replicas'][0]['candidate_settlement'])
        self.assertEqual(corrected['correction']['base_report_sha256'], old_sha)
        self.assertNotEqual(corrected['correction']['report_sha256'], old_sha)
        self.assertEqual(self.service.recalculate_amounts(self.reservation)['correction']['id'], corrected['correction']['id'])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT report_json FROM fixed_evaluations WHERE reservation_id=?', (self.reservation,)).fetchone()[0], raw)
            self.assertEqual(db.execute('SELECT count(*) FROM fixed_evaluation_corrections').fetchone()[0], 1)
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM fixed_evaluation_corrections')
        self.assertEqual(check_database(self.path)['status'], 'passed')
        workspace = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir=workspace) as directory:
            root = Path(directory).resolve()
            self.assertTrue(root.is_relative_to(workspace))
            isolated = root / 'with-correction-copy.sqlite3'
            with patch('dlt.recovery._require_private_acl'):
                self.assertEqual(prepare_isolated_copy(self.path, isolated)['status'], 'prepared')
            isolated_service = FixedEvaluations(isolated)
            try:
                self.assertEqual(isolated_service.get(self.reservation)['correction']['id'], corrected['correction']['id'])
            finally:
                isolated_service.stop()
        restarted = FixedEvaluations(self.path); self.addCleanup(restarted.stop)
        self.assertEqual(restarted.get(self.reservation)['correction']['id'], corrected['correction']['id'])
        changed = json.loads((Path(__file__).resolve().parents[1]/'research/data/official_page1.json').read_bytes())
        draw = next(item for item in changed['value']['list'] if item['lotteryDrawNum'] == '26122')
        original_numbers = draw['lotteryDrawResult']
        draw['lotteryDrawResult'] = ('01 02 03 04 05 01 02' if original_numbers != '01 02 03 04 05 01 02'
                                     else '06 07 08 09 10 03 04')
        SnapshotStore(self.path).import_official(json.dumps(changed).encode(), 'conflicting fixture',
                                                 '2026-10-30T12:00:00+08:00')
        self.assertEqual(restarted.get(self.reservation)['validity'], 'review_required')
        with self.assertRaisesRegex(ValueError, 'conflict|奖金情景'):
            restarted.recalculate_amounts(self.reservation)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_evaluation_corrections_no_update')
            db.execute('UPDATE fixed_evaluation_corrections SET record_json=?', (b'{}',))
        damaged = restarted.get(self.reservation)
        self.assertEqual(damaged['validity'], 'review_required')
        self.assertIn('摘要', damaged['validity_reason'])
        self.assertNotIn('paired_descriptions', damaged)
        self.assertEqual(check_database(self.path)['status'], 'failed')

    def test_correction_refuses_disagreement_with_original_attempt(self):
        self.freeze(); self.service.start(self.reservation, request_key='attempt-mismatch')
        finished = self.wait()
        self.assertEqual(finished['status'], 'complete', finished['error'])
        report = deepcopy(finished['report'])
        result = report['methods'][0]['result']
        result.pop('calculation_version'); result.pop('settlement_code_sha256')
        result['code_sha256'] = '0' * 64
        result['periods'][0]['replicas'][0]['candidate_settlement']['gross'] = '999'
        raw = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_evaluations_terminal')
            db.execute('UPDATE fixed_evaluations SET report_json=?,report_sha256=? WHERE reservation_id=?',
                       (raw, hashlib.sha256(raw).hexdigest(), self.reservation))
        with self.assertRaisesRegex(EvaluationError, '运行记录不一致'):
            self.service.recalculate_amounts(self.reservation)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM fixed_evaluation_corrections').fetchone()[0], 0)

    def test_correct_report_does_not_create_a_correction(self):
        self.freeze(); self.service.start(self.reservation, request_key='no-correction-needed')
        finished = self.wait()
        self.assertEqual(finished['status'], 'complete', finished['error'])
        with self.assertRaisesRegex(EvaluationError, '无需更正'):
            self.service.recalculate_amounts(self.reservation)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM fixed_evaluation_corrections').fetchone()[0], 0)

    def test_amount_correction_route_enforces_session_and_origin_without_socket(self):
        from dlt.app import Handler, _fixed_evaluation_write_digest
        import uuid
        calls = []
        fake = SimpleNamespace(recalculate_amounts=lambda reservation, **kwargs: calls.append(reservation) or {'validity': 'local_descriptive'},
                               write_status=lambda *args: {'status': 'accepted'})
        def call(*, csrf='secret', origin='http://127.0.0.1:8766'):
            request = {'reservation_id': 'a'*64}
            body = {**request, 'request_key': str(uuid.uuid4()),
                    'request_sha256': _fixed_evaluation_write_digest('recalculate_amounts', request)}
            raw = json.dumps(body).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', fixed_evaluations=fake)
            handler.path = '/api/fixed-evaluations/recalculate-amounts'
            handler.headers = {'Origin': origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': csrf,
                               'Content-Type': 'application/json', 'Content-Length': str(len(raw))}
            handler.rfile = BytesIO(raw)
            handler._validate_host = lambda: True
            handler._session = lambda: True
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        self.assertEqual(call(csrf='wrong')[0], 403)
        self.assertEqual(call(origin='https://invalid.example')[0], 403)
        self.assertEqual(call()[0], 200)
        self.assertEqual(calls, ['a'*64])

    def test_requires_same_budget_replica_manifest_and_exact_range(self):
        for mutate in ('cost', 'replica', 'range', 'structure'):
            methods = deepcopy(self.methods)
            if mutate == 'cost': methods[1]['protocol']['candidate']['budget_fen'] = 600
            if mutate == 'replica': methods[1]['protocol']['replicas'] = ['x', 'y']
            if mutate == 'range': methods[1]['protocol']['targets'] = ['26123']
            if mutate == 'structure': methods[1]['protocol']['candidate']['front_count'] = 8
            with self.subTest(mutate=mutate), self.assertRaisesRegex(EvaluationError, '固定范围'):
                self.freeze(methods)

    def test_invalid_structure_or_unaffordable_combination_cannot_be_fixed(self):
        for structure in ('unsupported', 'dantuo'):
            methods = deepcopy(self.methods)
            for method in methods:
                method['protocol']['candidate']['structure'] = structure
            with self.subTest(structure=structure), self.assertRaises(EvaluationError):
                self.freeze(methods)
        self.assertEqual(self.service.list(), [])

    def test_unfrozen_kernel_cannot_bypass_exploration_exclusion(self):
        fixed = self.freeze()
        spec = fixed['protocol']
        with self.assertRaisesRegex(EvaluationError, '不匹配'):
            _run_from_snapshot(self.path, self.methods[0]['protocol'], spec['scenarios'],
                               fixed_run=self.reservation, method_id='hot')
        with self.assertRaisesRegex(ValueError, '排除范围'):
            _run_from_snapshot(self.path, self.methods[0]['protocol'], spec['scenarios'])

    def test_changed_environment_blocks_start_before_consuming_run(self):
        original = self.freeze()
        with patch('dlt.fixed_evaluation._environment', return_value={'changed': True}):
            self.assertEqual(self.freeze()['protocol_sha256'], original['protocol_sha256'])
            with self.assertRaisesRegex(EvaluationError, '执行环境'):
                self.service.start(self.reservation)
        self.assertEqual(self.service.get(self.reservation)['status'], 'pending')

    def test_other_reserved_history_cannot_be_silently_used(self):
        reserve_range(self.path, self.snapshot, '26115', '26115', confirmed=True)
        with self.assertRaisesRegex(EvaluationError, '其他固定排除范围'):
            self.freeze()

    def test_missing_payout_period_is_preserved_as_incomplete(self):
        from test_backtest import source_data
        other = Path(self.tmp.name)/'synthetic.sqlite3'
        sid = SnapshotStore(other).import_official(json.dumps(source_data()).encode(), 'synthetic', '2026-02-01T12:00:00+08:00')['snapshot_id']
        reservation = reserve_range(other, sid, '26018', '26019', confirmed=True)['id']
        service = FixedEvaluations(other); self.addCleanup(service.stop)
        p = protocol(sid); p.update(targets=['26018', '26019'], replicas=['a'], history_periods=12)
        p['candidate']['max_tickets'] = 10
        service.freeze(reservation, [{'id': 'hot', 'protocol': p}], hypothesis='synthetic', timeout=20, confirmed=True)
        service.start(reservation)
        until = time.monotonic()+25
        while time.monotonic() < until:
            result = service.get(reservation)
            if result['status'] != 'running': break
            time.sleep(.05)
        self.assertEqual(result['status'], 'complete', result['error'])
        report = result['report']['methods'][0]['result']
        self.assertEqual(report['complete_periods'], 0)
        self.assertEqual(report['incomplete_issues'], ['26018', '26019'])
        self.assertIsNone(report['descriptive_mean_fraction'])

    def test_interrupted_execution_fails_without_replacing_protocol(self):
        self.freeze()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE fixed_evaluations SET status='running',started_at='2026-01-01'")
        self.service.recover_interrupted()
        result = self.service.get(self.reservation)
        self.assertEqual(result['status'], 'failed')
        with self.assertRaisesRegex(EvaluationError, '显式重试'):
            self.service.start(self.reservation)
        with self.assertRaisesRegex(EvaluationError, '不能替换'):
            self.freeze(hypothesis='replacement')

    def test_timeout_is_terminal_and_never_publishes_a_partial_report(self):
        self.service.freeze(self.reservation, self.methods, hypothesis='timeout test', timeout=1, confirmed=True)
        original = subprocess.Popen
        def slow_worker(*args, **kwargs):
            return original([sys.executable, '-c', 'import time; time.sleep(10)'], **kwargs)
        with patch('dlt.fixed_evaluation.subprocess.Popen', side_effect=slow_worker):
            self.service.start(self.reservation)
            result = self.wait()
        self.assertEqual(result['status'], 'failed')
        self.assertIn('时间上限', result['error'])
        self.assertIsNone(result['report'])
        with self.assertRaisesRegex(EvaluationError, '显式重试'):
            self.service.start(self.reservation)

    def test_service_stop_terminates_active_worker_and_preserves_failure(self):
        self.freeze()
        original = subprocess.Popen
        def slow_worker(*args, **kwargs):
            return original([sys.executable, '-c', 'import time; time.sleep(10)'], **kwargs)
        with patch('dlt.fixed_evaluation.subprocess.Popen', side_effect=slow_worker):
            self.service.start(self.reservation)
            self.service.stop()
        result = self.service.get(self.reservation)
        self.assertEqual(result['status'], 'failed')
        self.assertIsNone(result['report'])
        with self.service.lock:
            self.assertFalse(self.service.threads)
            self.assertFalse(self.service.processes)

    def test_explicit_retry_keeps_failed_attempt_and_is_idempotent(self):
        self.freeze()
        original = subprocess.Popen
        def fail_worker(*args, **kwargs):
            return original([sys.executable, '-c', 'raise SystemExit(1)'], **kwargs)
        with patch('dlt.fixed_evaluation.subprocess.Popen', side_effect=fail_worker):
            self.service.start(self.reservation, request_key='first-run')
            failed = self.wait()
        self.assertEqual(failed['status'], 'failed')
        repeated = self.service.start(self.reservation, request_key='first-run')
        self.assertEqual(repeated['requested_attempt']['status'], 'failed')
        with self.assertRaisesRegex(EvaluationError, '不同'):
            self.service.start(self.reservation, request_key='first-run', retry=True)
        for _ in range(50):
            with self.service.lock:
                if self.reservation not in self.service.threads: break
            time.sleep(.02)
        self.service.start(self.reservation, request_key='second-run', retry=True)
        result = self.wait()
        self.assertEqual(result['status'], 'complete', result['error'])
        self.assertEqual([a['status'] for a in result['attempts']], ['failed', 'complete'])
        self.assertEqual(result['attempts'][1]['retry_of'], 'first-run')
        self.assertEqual(self.service.start(self.reservation, request_key='first-run')['requested_attempt']['status'], 'failed')
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_evaluation_attempts_fixed')
            db.execute('DROP TRIGGER fixed_evaluation_attempts_terminal')
            db.execute("UPDATE fixed_evaluation_attempts SET retry_of=NULL WHERE request_key='second-run'")
            db.execute('CREATE TRIGGER fixed_evaluation_attempts_fixed BEFORE UPDATE OF request_key,reservation_id,attempt_no,retry_of,retry_requested,started_at ON fixed_evaluation_attempts '
                       "BEGIN SELECT RAISE(ABORT, 'immutable evaluation attempt'); END")
            db.execute("CREATE TRIGGER fixed_evaluation_attempts_terminal BEFORE UPDATE ON fixed_evaluation_attempts WHEN OLD.status IN ('complete','failed') "
                       "BEGIN SELECT RAISE(ABORT, 'immutable evaluation attempt result'); END")
        self.assertEqual(check_database(self.path)['status'], 'failed')
        with self.assertRaises(RecoveryError):
            prepare_isolated_copy(self.path, self.path.with_name('broken-retry.sqlite3'))

    def test_legacy_failed_record_is_migrated_without_losing_error(self):
        self.freeze()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE fixed_evaluations SET status='running',started_at='2026-01-01'")
            db.execute("UPDATE fixed_evaluations SET status='failed',finished_at='2026-01-02',error='legacy failure'")
            db.execute('DROP TABLE fixed_evaluation_attempts')
            db.execute('DROP TRIGGER fixed_evaluations_terminal')
            db.execute("CREATE TRIGGER fixed_evaluations_terminal BEFORE UPDATE ON fixed_evaluations WHEN OLD.status IN ('complete','failed') "
                       "BEGIN SELECT RAISE(ABORT, 'immutable evaluation result'); END")
        self.service.initialize()
        result = self.service.get(self.reservation)
        self.assertEqual(result['attempts'][0]['error'], 'legacy failure')
        self.assertTrue(result['attempts'][0]['request_key'].startswith('legacy-'))
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)

    def test_source_conflict_withdraws_current_validity_but_preserves_report(self):
        self.freeze(); self.service.start(self.reservation)
        original = self.wait()
        self.assertEqual(original['status'], 'complete', original['error'])
        raw = (Path(__file__).resolve().parents[1]/'research/data/official_page1.json').read_bytes()
        source = json.loads(raw)
        row = next(item for item in source['value']['list'] if item['lotteryDrawNum'] == '26123')
        row['lotteryDrawResult'] = '01 02 03 04 35 01 02'
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'conflicting fixture', '2026-10-30T12:00:00+08:00')
        result = self.service.get(self.reservation)
        self.assertEqual(result['validity'], 'review_required')
        self.assertEqual(result['report_sha256'], original['report_sha256'])

    def test_report_corruption_blocks_recovery_copy(self):
        self.freeze(); self.service.start(self.reservation)
        self.assertEqual(self.wait()['status'], 'complete')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fixed_evaluations_terminal')
            db.execute("UPDATE fixed_evaluations SET report_json=x'7b7d'")
            db.execute("CREATE TRIGGER fixed_evaluations_terminal BEFORE UPDATE ON fixed_evaluations WHEN OLD.status='complete' "
                       "BEGIN SELECT RAISE(ABORT, 'immutable evaluation result'); END")
        check = check_database(self.path)
        self.assertEqual(check['status'], 'failed', check)
        self.assertTrue(any(item['area'] == 'fixed_evaluations' for item in check['errors']))
        with self.assertRaises(RecoveryError):
            prepare_isolated_copy(self.path, self.path.with_name('recovery.sqlite3'))


if __name__ == '__main__':
    unittest.main()
