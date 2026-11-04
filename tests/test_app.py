import http.client
import base64
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid
from unittest.mock import patch

from dlt.app import LocalServer, SingleInstance
from dlt.snapshots import SnapshotStore
from dlt.pair_repository import PairRepository
from test_candidates import config


class AppTests(unittest.TestCase):
    def setUp(self):
        # These route tests run under the managed Codex sandbox, which does not
        # permit PowerShell Get-Acl to inspect temporary directories. ACL checks
        # have separate recovery tests; keep this class focused on HTTP behavior.
        self.acl_patch = patch('dlt.recovery._require_private_acl')
        self.acl_patch.start(); self.addCleanup(self.acl_patch.stop)
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'research.sqlite3'
        self.server = LocalServer(0, self.path)
        self.server.jobs.recover_interrupted()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
        self.addCleanup(self.close_server)
        self.port = self.server.server_port
        status, headers, payload = self.request('GET', '/api/session')
        self.assertEqual(status, 200)
        self.cookie = headers['set-cookie'].split(';')[0]
        self.csrf = json.loads(payload)['csrf']

    def close_server(self):
        self.server.shutdown(); self.server.server_close(); self.server.jobs.stop(); self.server.backtests.stop(); self.thread.join(timeout=3)

    def request(self, method, path, *, body=None, headers=None):
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=10)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            response = conn.getresponse()
            return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
        finally:
            conn.close()

    def write(self, path, value, headers=None):
        value = dict(value)
        if path == '/api/snapshots/import-bundled' and 'request_key' not in value:
            value['request_key'] = str(uuid.uuid4())
        if path == '/api/holdout-reservations' and value.get('confirmed') is True:
            value.setdefault('request_key', str(uuid.uuid4()))
        if path == '/api/source-reviews':
            from dlt.app import _source_write_digest
            document = base64.b64decode(value['document_base64'], validate=True)
            request = {'kind': value['kind'], 'source_url': value['source_url'],
                       'document_sha256': hashlib.sha256(document).hexdigest(),
                       'document_size': len(document), 'document_name': value.get('document_name'),
                       'extracted': value['extracted'], 'reviewer_note': value['reviewer_note'],
                       'confirmed': value['confirmed']}
            value.update(request)
            value.update(request_key=str(uuid.uuid4()), request_sha256=_source_write_digest(request))
        base = {'Cookie': self.cookie, 'Origin': self.server.origin, 'X-DLT-CSRF': self.csrf,
                'Content-Type': 'application/json'}
        base.update(headers or {})
        return self.request('POST', path, body=json.dumps(value).encode(), headers=base)

    def test_s01_candidate_evaluation_comparison_over_local_http(self):
        raw = (Path(__file__).resolve().parents[1] / 'research/data/official_page1.json').read_bytes()
        snapshot = SnapshotStore(self.path).import_official(
            raw, 'S01 local HTTP fixture', '2026-10-29T12:00:00+08:00')['snapshot_id']
        draws = SnapshotStore(self.path).get_draws(snapshot)
        pairs = PairRepository(self.path)
        for key, budget in (('pair-one', 400), ('pair-two', 600)):
            pairs.save_draft(key, {**config(), 'snapshot_id': snapshot, 'target_issue': '26124',
                                   'target_date': '2026-10-31', 'budget_fen': budget},
                             history_first_issue=draws[0]['issue'], history_cutoff_issue=draws[-1]['issue'])

        for request_key, pair_key in (('evaluation-one', 'pair-one'), ('evaluation-two', 'pair-two')):
            status, _, payload = self.write('/api/evaluations', {
                'request_key': request_key, 'pair_key': pair_key, 'scenario_snapshot_id': snapshot,
                'scenario_issue': '26123', 'samples': 1000, 'seed': 7})
            self.assertEqual(status, 200, payload)
            self.assertEqual(json.loads(payload)['status'], 'research_draft')

        status, _, payload = self.request('GET', '/api/evaluations/comparison-options',
                                          headers={'Cookie': self.cookie})
        self.assertEqual(status, 200, payload)
        options = json.loads(payload)['options']
        self.assertEqual({item['request_key'] for item in options}, {'evaluation-one', 'evaluation-two'})

        status, _, payload = self.request('GET',
            '/api/evaluations/compare?key=evaluation-one&key=evaluation-two',
            headers={'Cookie': self.cookie})
        self.assertEqual(status, 200, payload)
        comparison = json.loads(payload)
        self.assertEqual([item['pair_key'] for item in comparison['entries']], ['pair-one', 'pair-two'])
        self.assertEqual([item['pair']['candidate']['cost_fen'] for item in comparison['entries']], [400, 600])
        self.assertEqual([item['result']['scenario']['source_issue'] for item in comparison['entries']],
                         ['26123', '26123'])
        self.assertIn('not an adoption recommendation', comparison['assurance'])
        self.assertEqual(self.request('GET', '/api/evaluations/comparison-options')[0], 401)

    def test_origin_csrf_cookie_host_and_static_whitelist(self):
        self.assertEqual(self.write('/api/snapshots/import-bundled', {}, {'Origin': 'https://attacker.invalid'})[0], 403)
        self.assertEqual(self.write('/api/snapshots/import-bundled', {}, {'X-DLT-CSRF': 'wrong'})[0], 403)
        self.assertEqual(self.write('/api/snapshots/import-bundled', {}, {'Cookie': 'dlt_session=wrong'})[0], 403)
        self.assertEqual(self.request('GET', '/', headers={'Host': 'localhost:'+str(self.port)})[0], 403)
        self.assertEqual(self.request('GET', '/../../PRD.md')[0], 404)
        self.assertEqual(self.request('GET', '/api/jobs')[0], 401)
        status, headers, page = self.request('GET', '/')
        self.assertEqual(status, 200); self.assertIn('大乐透研究台', page.decode())
        self.assertEqual(headers['x-frame-options'], 'DENY')

    def test_integrity_read_requires_session_and_limits_concurrency(self):
        self.assertEqual(self.request('GET', '/api/integrity')[0], 401)
        status, _, payload = self.request('GET', '/api/integrity', headers={'Cookie': self.cookie})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload)['check']['status'], 'passed')
        self.assertTrue(self.server.integrity_lock.acquire(blocking=False))
        try:
            self.assertEqual(self.request('GET', '/api/integrity', headers={'Cookie': self.cookie})[0], 409)
        finally:
            self.server.integrity_lock.release()

    def test_holdout_audit_http_requires_write_session_and_reports_limits(self):
        status, _, payload = self.write('/api/snapshots/import-bundled', {})
        self.assertEqual(status, 200)
        snapshot = json.loads(payload)['snapshot_id']
        body = {'snapshot_id': snapshot, 'start_issue': '26025', 'end_issue': '26029'}
        self.assertEqual(self.write('/api/holdout-audit', body, {'X-DLT-CSRF': 'bad'})[0], 403)
        status, _, payload = self.write('/api/holdout-audit', body)
        self.assertEqual(status, 200, payload)
        audit = json.loads(payload)['audit']
        self.assertEqual(audit['issues'], ['26025', '26026', '26027', '26028', '26029'])
        self.assertEqual(audit['independence'], 'not_verified')
        self.assertTrue(self.server.holdout_audit_lock.acquire(blocking=False))
        try:
            self.assertEqual(self.write('/api/holdout-audit', body)[0], 409)
        finally:
            self.server.holdout_audit_lock.release()

    def test_reservation_http_requires_session_csrf_and_explicit_confirmation(self):
        status, _, payload = self.write('/api/snapshots/import-bundled', {})
        self.assertEqual(status, 200)
        body = {'snapshot_id': json.loads(payload)['snapshot_id'], 'start_issue': '26025', 'end_issue': '26029'}
        self.assertEqual(self.request('GET', '/api/holdout-reservations')[0], 401)
        self.assertEqual(self.write('/api/holdout-reservations', {**body, 'confirmed': True}, {'Origin': 'https://attacker.invalid'})[0], 403)
        self.assertEqual(self.write('/api/holdout-reservations', body)[0], 400)
        status, _, payload = self.write('/api/holdout-reservations', {**body, 'confirmed': True})
        self.assertEqual(status, 200, payload)
        ident = json.loads(payload)['reservation']['id']
        status, _, payload = self.request('GET', '/api/holdout-reservations', headers={'Cookie': self.cookie})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload)['reservations'][0]['id'], ident)

    def test_generation_replay_export_requires_session_origin_and_csrf(self):
        from test_candidates import config
        from test_model_repository import payload as historical_payload
        from dlt.generation_replay import verify_generation_replay
        snapshot = SnapshotStore(self.path).import_official(historical_payload(), 'synthetic SECRET_SOURCE',
                       '2026-01-20T12:00:00+08:00')['snapshot_id']
        key = 'SECRET_REQUEST'
        PairRepository(self.path).save_draft(key, {**config(), 'snapshot_id': snapshot,
            'rule_version': 'SECRET_RULE', 'calendar_version': 'SECRET_CALENDAR'},
            history_first_issue='26001', history_cutoff_issue='26016')
        body = {'pair_key': key}
        self.assertEqual(self.write('/api/generation-replay', body, {'Origin': 'https://attacker.invalid'})[0], 403)
        self.assertEqual(self.write('/api/generation-replay', body, {'X-DLT-CSRF': 'bad'})[0], 403)
        self.assertEqual(self.write('/api/generation-replay', body, {'Cookie': 'dlt_session=wrong'})[0], 403)
        status, _, payload = self.write('/api/generation-replay', body)
        self.assertEqual(status, 200, payload)
        packed = base64.b64decode(json.loads(payload)['package_base64'], validate=True)
        self.assertNotIn(b'SECRET_', packed)
        self.assertEqual(verify_generation_replay(json.loads(packed))['status'], 'matched')
        self.assertTrue(self.server.replay_export_lock.acquire(blocking=False))
        try:
            self.assertEqual(self.write('/api/generation-replay', body)[0], 409)
        finally:
            self.server.replay_export_lock.release()

    def test_training_excluded_period_rejected_and_recorded_over_http(self):
        from test_backtest import source_data
        from test_model_repository import config
        from dlt.models import ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION
        snapshot = SnapshotStore(self.path).import_official(json.dumps(source_data()).encode(), 'fixture',
                    '2026-02-01T12:00:00+08:00')['snapshot_id']
        cfg = {**config(snapshot), 'train_end': '26014', 'cutoff': '26019',
               'holdout_start': '26018', 'holdout_end': '26020',
               'algorithm_version': ALGORITHM_VERSION['tree'],
               'feature_schema_version': FEATURE_SCHEMA_VERSION,
               'preprocessing_version': PREPROCESSING_VERSION['tree']}
        body = {'request_key': 'boundary-http', 'name': '训练', 'config': cfg,
                'first_issue': '26001', 'timeout': 20}
        status, _, payload = self.write('/api/jobs', body)
        self.assertEqual(status, 400, payload)
        self.assertIn('排除', json.loads(payload)['error'])
        body['config']['cutoff'] = '26017'
        status, _, payload = self.write('/api/jobs', body)
        self.assertEqual(status, 200, payload)
        deadline = time.monotonic()+20
        while time.monotonic() < deadline:
            _, _, payload = self.request('GET', '/api/jobs', headers={'Cookie': self.cookie})
            job = next(item for item in json.loads(payload)['jobs'] if item['request_key'] == 'boundary-http')
            if job['status'] != 'running':
                break
            time.sleep(.05)
        self.assertEqual(job['status'], 'complete', job.get('error'))
        self.assertEqual(job['request']['config']['holdout_start'], '26018')
        self.assertEqual(job['request']['config']['holdout_end'], '26020')

    def test_real_historical_backtest_over_local_http(self):
        from test_backtest import protocol
        status, _, payload = self.write('/api/snapshots/import-bundled', {})
        self.assertEqual(status, 200)
        snapshot = json.loads(payload)['snapshot_id']
        cfg = protocol(snapshot)
        cfg.update(targets=['26121', '26122', '26123'], replicas=['one'], history_periods=12)
        cfg['candidate'].update(budget_fen=600, max_tickets=10, additional=True)
        body = {'request_key': 'rolling-http', 'protocol': cfg, 'timeout': 30}
        self.assertEqual(self.write('/api/backtests', body, {'X-DLT-CSRF': 'bad'})[0], 403)
        self.assertEqual(self.request('GET', '/api/backtests')[0], 401)
        status, _, response = self.write('/api/backtests', body)
        self.assertEqual(status, 200, response)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            status, _, response = self.request('GET', '/api/backtests?key=rolling-http', headers={'Cookie': self.cookie})
            job = json.loads(response)['job']
            if job['status'] != 'running': break
            time.sleep(.1)
        self.assertEqual(job['status'], 'complete', job.get('error'))
        self.assertEqual(job['result']['status'], 'research_only')
        report = job['result']['report']
        self.assertEqual((report['planned_periods'], report['complete_periods']), (3, 2))
        self.assertIn('奖金缺失', report['periods'][0]['replicas'][0]['reason'])
        seed = report['periods'][1]['replicas'][0]['pair']['candidate']['seed']
        from dlt.backtest_repository import BacktestRepository
        original = BacktestRepository(self.path).get('rolling-http')['periods'][1]['replicas'][0]['pair']['candidate']['seed']
        self.assertEqual(int(seed), original)
        self.assertTrue(isinstance(seed, str) or abs(seed) <= 9007199254740991)

    def test_update_plans_are_paused_until_explicitly_enabled(self):
        config = {'start_at': '2026-11-01T20:00:00+08:00', 'interval_seconds': 3600,
                  'timezone_offset_minutes': 480, 'page_size': 20, 'timeout_seconds': 20,
                  'max_bytes': 1048576, 'retry_count': 2, 'retry_wait_seconds': 30}
        self.assertEqual(self.write('/api/update-plans', {'config': config}, {'X-DLT-CSRF': 'bad'})[0], 403)
        status, _, payload = self.write('/api/update-plans', {'request_key': str(uuid.uuid4()), 'config': config})
        self.assertEqual(status, 200)
        plan = json.loads(payload); self.assertFalse(plan['enabled'])
        status, _, payload = self.request('GET', '/api/update-plans', headers={'Cookie': self.cookie})
        self.assertEqual(status, 200)
        self.assertFalse(json.loads(payload)['plans'][0]['enabled'])
        self.assertEqual(self.write('/api/update-plans/set-enabled', {'request_key': str(uuid.uuid4()), 'plan_id': plan['id'], 'enabled': 'yes'})[0], 400)
        self.assertEqual(self.write('/api/update-plans/set-enabled', {'request_key': str(uuid.uuid4()), 'plan_id': plan['id'], 'enabled': True})[0], 200)
        _, _, payload = self.request('GET', '/api/update-plans', headers={'Cookie': self.cookie})
        self.assertTrue(json.loads(payload)['plans'][0]['enabled'])
        self.assertEqual(json.loads(payload)['plans'][0]['runs'], [])
        self.assertEqual(self.write('/api/update-plans/set-enabled', {'request_key': str(uuid.uuid4()), 'plan_id': plan['id'], 'enabled': False})[0], 200)
        self.assertEqual(self.write('/api/update-plans/run-now', {'request_key': str(uuid.uuid4()), 'plan_id': plan['id']}, {'X-DLT-CSRF': 'bad'})[0], 403)
        status, _, payload = self.write('/api/update-plans/run-now', {'request_key': str(uuid.uuid4()), 'plan_id': plan['id']})
        self.assertEqual(status, 200)
        one_off = json.loads(payload)
        self.assertEqual(one_off['status'], 'queued')
        _, _, payload = self.request('GET', '/api/update-plans', headers={'Cookie': self.cookie})
        plans = {p['id']: p for p in json.loads(payload)['plans']}
        self.assertFalse(plans[plan['id']]['enabled'])
        self.assertTrue(plans[one_off['id']]['enabled'])
        self.assertTrue(plans[one_off['id']]['config']['run_once'])

    def test_saved_snapshot_detail_distinguishes_conflict_and_settlement_eligibility(self):
        status, _, payload = self.write('/api/snapshots/import-bundled', {})
        self.assertEqual(status, 200)
        snapshot = json.loads(payload)['snapshot_id']
        status, _, payload = self.request('GET', '/api/draws?snapshot='+snapshot, headers={'Cookie': self.cookie})
        result = json.loads(payload)
        self.assertEqual(status, 200)
        self.assertEqual(len(result['draws']), 12)
        self.assertEqual(result['draws'][0]['issue'], '26123')
        self.assertTrue(result['draws'][0]['numbers_usable'])
        self.assertFalse(result['draws'][0]['settlement_ready'])
        self.assertFalse(result['draws'][0]['conflict'])
        source = json.loads((Path(__file__).resolve().parents[1]/'research/data/official_page1.json').read_text(encoding='utf-8'))
        source['value']['list'][0]['prizeLevelList'][0]['stakeAmount'] = '1'
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'conflicting test',
                                                datetime.now(timezone.utc).isoformat())
        _, _, payload = self.request('GET', '/api/draws?snapshot='+snapshot, headers={'Cookie': self.cookie})
        self.assertTrue(json.loads(payload)['draws'][0]['conflict'])
        self.assertEqual(self.request('GET', '/api/draws?snapshot='+('f'*64), headers={'Cookie': self.cookie})[0], 404)

    def test_source_document_rejects_paths_and_corruption(self):
        body = {'kind': 'calendar', 'source_url': 'https://www.sporttery.cn/synthetic-test',
                'document_base64': base64.b64encode(b'original').decode(), 'document_name': '../secret.txt',
                'extracted': {}, 'reviewer_note': 'synthetic only', 'confirmed': True}
        self.assertEqual(self.write('/api/source-reviews', body)[0], 400)
        body['document_name'] = 'review.txt'
        status, _, payload = self.write('/api/source-reviews', body)
        self.assertEqual(status, 200)
        ident = json.loads(payload)['review_id']
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER reviewed_sources_no_update')
            db.execute('UPDATE reviewed_sources SET document=? WHERE id=?', (b'altered', ident))
        status, _, _ = self.request('GET', '/api/source-review-document?id='+ident, headers={'Cookie': self.cookie})
        self.assertEqual(status, 409)

    def test_personally_reviewed_experiment_registration_over_http(self):
        from test_candidates import config, input_rows
        from test_settlement import fixture
        from dlt.prize_rules import (DLT_2026_HIT_GROUPS, DLT_2026_OFFICIAL_SORT_CODES,
                                     DLT_2026_ADDITIONAL_GROUPS)
        china = timezone(timedelta(hours=8))
        now = datetime.now(timezone.utc)
        day = (now.astimezone(china) + timedelta(days=2)).date()
        deadline = datetime.combine(day, datetime.min.time(), tzinfo=china) + timedelta(hours=20)
        history = input_rows()
        for i, row in enumerate(history):
            row['date'] = (day-timedelta(days=18-i)).isoformat()
        source = {'success': True, 'errorCode': '0', 'value': {'list': [
            {'lotteryGameNum': '85', 'lotteryDrawNum': row['issue'], 'lotteryDrawTime': row['date'],
             'lotteryDrawResult': ' '.join(map(str, row['front']+row['back'])), 'prizeLevelList': []}
            for row in history]}}
        sid = SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'test source',
                                                      (now-timedelta(seconds=2)).isoformat())['snapshot_id']
        cfg = {**config(), 'snapshot_id': sid, 'target_date': day.isoformat(), 'budget_fen': 400}
        PairRepository(self.path).save_draft('one', cfg, history_first_issue='26001', history_cutoff_issue='26016')
        url = 'https://www.sporttery.cn/synthetic-test'
        rule = fixture()['rule']; rule.update(version=cfg['rule_version'], applicable_issues=['26017'], source=url)
        rule['payout_sort_map'] = {code: code for code in sorted(DLT_2026_OFFICIAL_SORT_CODES)}
        rule['hit_groups'] = {outcome: (str(rank) if rank is not None else None)
                              for outcome, rank in DLT_2026_HIT_GROUPS.items()}
        rule['additional_groups'] = {str(base): str(extra)
                                     for base, extra in DLT_2026_ADDITIONAL_GROUPS.items()}
        extracts = {'calendar': {'issue': '26017', 'draw_date': day.isoformat(), 'cutoff_at': deadline.isoformat(),
                                 'version': cfg['calendar_version']}, 'rule': rule,
                    'tax': {'tax': rule['tax'], 'applicable_issues': ['26017']}}
        ids = {}
        for kind, extracted in extracts.items():
            body = {'kind': kind, 'source_url': url, 'document_base64': base64.b64encode(b'synthetic').decode(),
                    'document_name': '来源资料.txt', 'extracted': extracted,
                    'reviewer_note': 'synthetic test only', 'confirmed': True}
            if kind == 'calendar':
                self.assertEqual(self.write('/api/source-reviews', {**body, 'confirmed': False})[0], 400)
                self.assertEqual(self.write('/api/source-reviews', body, {'X-DLT-CSRF': 'bad'})[0], 403)
            status, _, result = self.write('/api/source-reviews', body)
            self.assertEqual(status, 200, result)
            ids[kind] = json.loads(result)['review_id']
        status, headers, document = self.request('GET', '/api/source-review-document?id='+ids['calendar'],
                                                 headers={'Cookie': self.cookie})
        self.assertEqual(status, 403)
        self.assertNotIn(b'synthetic', document)
        self.assertNotIn('content-disposition', headers)
        self.assertEqual(self.request('GET', '/api/source-review-document?id='+ids['calendar'])[0], 401)
        protocol = {'hypothesis': 'synthetic hypothesis', 'primary_metric': 'paired_net_difference',
                    'target_issue': '26017', 'observation_issues': ['26017'],
                    'replicas': [{'replica': 'a', 'pair_key': 'one'}], 'aggregation': 'arithmetic_mean_v1',
                    'exclusion_rule': 'record failures', 'retraining_policy': 'none', 'selection_rule': 'fixed',
                    'time_assurance': 'local_clock', 'internal_deadline': (deadline-timedelta(hours=1)).isoformat(),
                    'calendar_review_id': ids['calendar'], 'rule_review_id': ids['rule'], 'tax_review_id': ids['tax'],
                    'other_winnings_assumption': 'no_other_same_issue_winnings'}
        body = {'request_key': 'http-exp', 'protocol': protocol}
        status, _, prepared = self.write('/api/experiments/prepare', body)
        self.assertEqual(status, 200, prepared)
        intent = json.loads(prepared)['intent']
        status, _, state = self.request('GET', '/api/experiments/write-status?request_key=http-exp&request_sha256='+intent['request_sha256'],
                                        headers={'Cookie': self.cookie})
        self.assertEqual((status, json.loads(state)['write']['status']), (200, 'unknown'))
        body['require_intent'] = True
        status, _, result = self.write('/api/experiments', body)
        self.assertEqual(status, 200, result)
        self.assertEqual(json.loads(result)['write']['status'], 'committed')
        status, _, state = self.request('GET', '/api/experiments/write-status?request_key=http-exp&request_sha256='+intent['request_sha256'],
                                        headers={'Cookie': self.cookie})
        self.assertEqual((status, json.loads(state)['write']['status']), (200, 'committed'))
        self.assertEqual(json.loads(result)['experiment']['status'], 'locally_attested')
        status, _, result = self.request('GET', '/api/experiments?key=http-exp', headers={'Cookie': self.cookie})
        entry = json.loads(result)['experiment']
        self.assertEqual(entry['paper_result']['status'], 'waiting')
        self.assertIn('not independently', entry['content']['time_evidence'])
        from dlt.prospective_closure_writes import digest as closure_digest
        close_request = {'object_key': 'http-exp', 'reason': 'test'}
        self.assertEqual(self.write('/api/experiments/close', {
            'request_key': str(uuid.uuid4()), **close_request,
            'request_sha256': closure_digest('experiment_close', close_request)})[0], 200)
        _, _, result = self.request('GET', '/api/experiments', headers={'Cookie': self.cookie})
        self.assertEqual(json.loads(result)['experiments'][0]['status'], 'closed')

        pending = {'request_key': 'http-pending', 'protocol': {**protocol, 'hypothesis': 'pending synthetic test'}}
        status, _, result = self.write('/api/experiments/prepare', pending)
        self.assertEqual(status, 200, result)
        digest = json.loads(result)['intent']['request_sha256']
        status, _, result = self.write('/api/experiments/fence', {'request_key': 'http-pending', 'request_sha256': digest})
        self.assertEqual((status, json.loads(result)['write']['status']), (200, 'fenced'))
        self.assertEqual(self.write('/api/experiments', {**pending, 'require_intent': True})[0], 400)

    def test_real_train_over_local_http(self):
        from dlt.models import ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION
        status, _, payload = self.write('/api/snapshots/import-bundled', {})
        self.assertEqual(status, 200, payload)
        snapshot = json.loads(payload)['snapshot_id']
        _, _, payload = self.request('GET', '/api/snapshots', headers={'Cookie': self.cookie})
        rows = json.loads(payload)['snapshots'][0]['issues']
        body = {'request_key': 'job-one', 'name': '真实树模型', 'first_issue': rows[0], 'timeout': 30,
                'config': {'snapshot_id': snapshot, 'method': 'tree', 'window': 5, 'seed': 123,
                           'train_end': rows[-11], 'cutoff': rows[-1], 'availability_mode': 'historical_reconstruction',
                           'algorithm_version': ALGORITHM_VERSION['tree'],
                           'feature_schema_version': FEATURE_SCHEMA_VERSION,
                           'preprocessing_version': PREPROCESSING_VERSION['tree'],
                           'parameters': {'max_depth': 2, 'min_leaf': 2}}}
        status, _, payload = self.write('/api/jobs', body)
        self.assertEqual(status, 200, payload)
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            _, _, payload = self.request('GET', '/api/jobs', headers={'Cookie': self.cookie})
            job = json.loads(payload)['jobs'][0]
            if job['status'] != 'running': break
            time.sleep(.1)
        self.assertEqual(job['status'], 'complete', job.get('error'))
        self.assertTrue(job['artifact_id'])
        self.assertEqual(job['artifact_status'], 'ready')
        pair_request = {'request_key': 'model-pair', 'history_first_issue': rows[0], 'history_cutoff_issue': rows[-1],
                        'model_selection': {'version': 'model-selection-v1', 'considered_attempts': ['job-one'],
                                            'selected_attempt': 'job-one', 'reason': 'synthetic HTTP test selection',
                                            'trial_group_id': None},
                        'artifact_id': job['artifact_id'], 'config': {'method': 'tree', 'structure': 'singles',
                        'target_issue': '26124', 'target_date': '2026-10-31', 'snapshot_id': snapshot,
                        'rule_version': 'manual-price', 'calendar_version': 'manual-date', 'budget_fen': 3600,
                        'base_price_fen': 200, 'additional_price_fen': 300, 'additional': False,
                        'window': 60, 'seed': 42, 'max_tickets': 500, 'max_sampling_attempts': 20000,
                        'score_temperature': 1.0}}
        status, _, payload = self.write('/api/pairs', pair_request)
        self.assertEqual(status, 200, payload)
        self.assertEqual(json.loads(payload)['seed_encoding'], 'decimal_string_uint64')
        pair = json.loads(payload)['pair']
        self.assertIsInstance(pair['candidate']['seed'], str)
        self.assertEqual(pair['candidate']['seed'], str(PairRepository(self.path).get('model-pair')['candidate']['seed']))
        self.assertEqual(pair['model_artifact_id'], job['artifact_id'])
        self.assertEqual(pair['candidate']['cost_fen'], pair['control']['cost_fen'])
        _, _, listed = self.request('GET', '/api/pairs', headers={'Cookie': self.cookie})
        self.assertNotIn('candidate', json.loads(listed)['pairs'][0]['summary'])
        self.assertNotIn('control', json.loads(listed)['pairs'][0]['summary'])
        _, _, reopened = self.request('GET', '/api/pairs?key=model-pair', headers={'Cookie': self.cookie})
        self.assertEqual(json.loads(reopened)['pair']['pair_sha256'], pair['pair_sha256'])
        self.assertEqual(json.loads(reopened)['request']['history_first_issue'], rows[0])
        self.assertEqual(self.write('/api/pairs', pair_request)[0], 200)
        changed = {**pair_request, 'config': {**pair_request['config'], 'budget_fen': 4000}}
        self.assertEqual(self.write('/api/pairs', changed)[0], 400)
        evaluation = {'request_key': 'evaluation-one', 'pair_key': 'model-pair', 'scenario_snapshot_id': snapshot,
                      'samples': 1000, 'seed': 99}
        self.assertEqual(self.write('/api/evaluations', evaluation, {'Origin': 'https://attacker.invalid'})[0], 403)
        self.server.evaluation_lock.acquire()
        try:
            self.assertEqual(self.write('/api/evaluations', evaluation)[0], 409)
        finally:
            self.server.evaluation_lock.release()
        status, _, evaluation_body = self.write('/api/evaluations', evaluation)
        self.assertEqual(status, 200, evaluation_body)
        evaluated = json.loads(evaluation_body)['result']
        self.assertEqual(evaluated['pair_sha256'], pair['pair_sha256'])
        self.assertEqual(evaluated['sampling']['samples'], 1000)
        self.assertIsInstance(evaluated['portfolios']['candidate']['mean_gross_tax_before']['fraction'][0], str)
        self.assertEqual(self.write('/api/evaluations', evaluation)[0], 200)
        self.assertEqual(self.write('/api/evaluations', {**evaluation, 'seed': 100})[0], 400)
        _, _, latest = self.request('GET', '/api/evaluations?pair=model-pair', headers={'Cookie': self.cookie})
        self.assertEqual(json.loads(latest)['evaluation']['result']['pair_sha256'], pair['pair_sha256'])
        unsafe = {**pair_request, 'request_key': 'too-large', 'config': {**pair_request['config'], 'max_tickets': 501}}
        self.assertEqual(self.write('/api/pairs', unsafe)[0], 400)
        self.assertEqual(self.write('/api/pairs', pair_request, {'Origin': 'https://attacker.invalid'})[0], 403)
        path = Path(__file__).resolve().parents[1]/'research/data/official_page1.json'
        source = json.loads(path.read_text(encoding='utf-8'))
        source['value']['list'][0]['lotteryDrawResult'] = '01 02 03 04 05 01 02'
        SnapshotStore(self.path).import_official(json.dumps(source).encode(), 'synthetic changed source',
                                                 '2026-10-30T00:00:00+08:00')
        _, _, payload = self.request('GET', '/api/jobs', headers={'Cookie': self.cookie})
        job = json.loads(payload)['jobs'][0]
        self.assertEqual(job['status'], 'complete')
        self.assertEqual(job['artifact_status'], 'review_required')
        _, _, payload = self.request('GET', '/api/pairs', headers={'Cookie': self.cookie})
        self.assertEqual(json.loads(payload)['pairs'][0]['status'], 'review_required')
        _, _, payload = self.request('GET', '/api/evaluations?pair=model-pair', headers={'Cookie': self.cookie})
        self.assertEqual(json.loads(payload)['evaluation']['status'], 'review_required')
        self.assertEqual(self.write('/api/pairs', {**pair_request, 'request_key': 'conflicted-pair'})[0], 400)

    def test_database_single_instance_lock(self):
        with SingleInstance(self.path):
            with self.assertRaises(RuntimeError):
                with SingleInstance(self.path):
                    pass


if __name__ == '__main__':
    unittest.main()
