import http.client
import json
import threading
import time
import unittest
from datetime import timedelta
from unittest.mock import patch

from dlt.app import Handler, LocalServer
import test_rolling_policy as fixtures


class RollingTrainingHttpTests(unittest.TestCase):
    setUpBase = fixtures.RollingPolicyTests.setUp
    review = fixtures.RollingPolicyTests.review
    import_draw = fixtures.RollingPolicyTests.import_draw
    _db = fixtures.RollingPolicyTests._db
    prepare_second = fixtures.RollingPolicyTests.prepare_second
    train = fixtures.RollingPolicyTests.train

    def setUp(self):
        self.setUpBase()
        self.series.create(fixtures.ROLLING_SERIES_KEY, self.manifest)
        self.acl_patch = patch('dlt.recovery._require_private_acl')
        self.acl_patch.start(); self.addCleanup(self.acl_patch.stop)
        self.server = LocalServer(0, self.path)
        self.server.experiments = self.repo
        self.server.jobs.recover_interrupted()
        self.server.backtests.recover_interrupted()
        self.server.rolling_loop.start()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.close_server)
        _, headers, session = self.request('GET', '/api/session')
        self.read = {'Cookie': headers['set-cookie'].split(';')[0]}
        self.write = {**self.read, 'Origin': self.server.origin, 'X-DLT-CSRF': session['csrf'],
                      'Content-Type': 'application/json'}

    def close_server(self):
        self.server.rolling_loop.stop()
        self.server.shutdown(); self.server.server_close()
        self.server.jobs.stop(); self.server.backtests.stop(); self.thread.join(timeout=3)

    def restart_server(self):
        old = self.server
        old.rolling_loop.stop(); old.shutdown(); old.server_close()
        old.jobs.stop(); old.backtests.stop(); self.thread.join(timeout=5)
        self.assertFalse(self.thread.is_alive())
        self.server = LocalServer(old.server_port, self.path)
        self.server.experiments = self.repo
        self.server.rolling_loop.start()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        _, headers, session = self.request('GET', '/api/session')
        self.read = {'Cookie': headers['set-cookie'].split(';')[0]}
        self.write = {**self.read, 'Origin': self.server.origin, 'X-DLT-CSRF': session['csrf'],
                      'Content-Type': 'application/json'}

    def submit_losing_response(self, path, body):
        committed, release = threading.Event(), threading.Event()
        errors = []
        original_json = Handler._json

        def drop(handler, status, data, **kwargs):
            if handler.path.split('?', 1)[0] == path and status == 200 and data.get('write', {}).get('status') == 'accepted':
                committed.set()
                if not release.wait(15):
                    raise TimeoutError('test did not release dropped plan response')
                handler.connection.close()
                return
            return original_json(handler, status, data, **kwargs)

        def submit():
            try:
                self.request('POST', path, body, self.write)
            except Exception as exc:
                errors.append(exc)

        with patch.object(Handler, '_json', drop):
            worker = threading.Thread(target=submit)
            worker.start()
            try:
                self.assertTrue(committed.wait(15), f'{path} did not commit before response loss')
            finally:
                release.set()
                worker.join(15)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(errors), 1, errors)
        self.assertIsInstance(errors[0], (http.client.RemoteDisconnected, ConnectionError, OSError), errors)

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=10)
        try:
            conn.request(method, path, body=json.dumps(body).encode() if body is not None else None, headers=headers or {})
            response = conn.getresponse()
            return response.status, {k.lower(): v for k, v in response.getheaders()}, json.loads(response.read())
        finally:
            conn.close()

    def test_local_plan_runs_without_browser_polling_and_rejects_cross_site(self):
        body = {'series_key': fixtures.ROLLING_SERIES_KEY, 'issue': '26017', 'calendar_review_id': self.cal_id,
                'expected_cutoff_issue': '26016', 'start_at': (self.now-timedelta(minutes=1)).isoformat(),
                'timeout': 20, 'confirmed': True,
                'request_key': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'}
        self.assertEqual(self.request('GET', '/api/rolling-training-plans')[0], 401)
        self.assertEqual(self.request('POST', '/api/rolling-training-plans', body,
                                      {**self.write, 'Origin': 'https://attacker.invalid'})[0], 403)
        self.assertEqual(self.request('POST', '/api/rolling-training-plans', {**body, 'confirmed': False}, self.write)[0], 400)
        status, _, response = self.request('POST', '/api/rolling-training-plans', body, self.write)
        self.assertEqual(status, 200, response)
        ident = response['plan']['id']
        # No browser requests are needed while the background thread runs.
        deadline = time.monotonic()+20
        while time.monotonic() < deadline:
            plan = next(p for p in self.server.rolling_training.list() if p['id'] == ident)
            if plan['status'] not in ('waiting', 'dispatching', 'running'):
                break
            time.sleep(.1)
        self.assertEqual(plan['status'], 'complete', plan['message'])
        status, _, response = self.request('GET', '/api/rolling-training-plans', headers=self.read)
        self.assertEqual(status, 200)
        self.assertEqual(response['worker']['state'], 'running')
        self.assertEqual(response['plans'][0]['status'], 'complete')
        self.assertEqual(self.server.jobs.get(plan['job_key'])['request']['config']['rolling_issue'], '26017')

    def test_registration_plan_runs_without_browser_and_rejects_cross_site(self):
        training = {'series_key': fixtures.ROLLING_SERIES_KEY, 'issue': '26017', 'calendar_review_id': self.cal_id,
                    'expected_cutoff_issue': '26016', 'start_at': (self.now-timedelta(minutes=1)).isoformat(),
                    'timeout': 20, 'confirmed': True,
                    'request_key': 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'}
        status, _, response = self.request('POST', '/api/rolling-training-plans', training, self.write)
        self.assertEqual(status, 200, response)
        body = {'training_plan_id': response['plan']['id'], 'rule_review_id': self.rule_id,
                'tax_review_id': self.tax_id, 'internal_deadline': (self.deadline-timedelta(hours=1)).isoformat(),
                'confirmed': True, 'request_key': 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'}
        self.assertEqual(self.request('GET', '/api/rolling-registration-plans')[0], 401)
        self.assertEqual(self.request('POST', '/api/rolling-registration-plans', body,
                                      {**self.write, 'Origin': 'https://attacker.invalid'})[0], 403)
        self.assertEqual(self.request('POST', '/api/rolling-registration-plans', {**body, 'confirmed': False}, self.write)[0], 400)
        status, _, response = self.request('POST', '/api/rolling-registration-plans', body, self.write)
        self.assertEqual(status, 200, response)
        ident = response['plan']['id']
        deadline = time.monotonic()+25
        while time.monotonic() < deadline:
            plan = next(p for p in self.server.rolling_registration.list() if p['id'] == ident)
            if plan['status'] != 'waiting':
                break
            time.sleep(.1)
        self.assertEqual(plan['status'], 'complete', plan['message'])
        status, _, response = self.request('GET', '/api/rolling-registration-plans', headers=self.read)
        self.assertEqual(status, 200)
        self.assertEqual(response['plans'][0]['status'], 'complete')
        self.assertEqual(len(self.repo.get('auto-exp-'+ident)['pairs']), 2)

    def test_training_and_rolling_registration_recover_after_response_loss_and_service_restart(self):
        training = {'series_key': fixtures.ROLLING_SERIES_KEY, 'issue': '26017', 'calendar_review_id': self.cal_id,
                    'expected_cutoff_issue': '26016', 'start_at': (self.now-timedelta(minutes=1)).isoformat(),
                    'timeout': 20, 'confirmed': True,
                    'request_key': 'd1111111-1111-4111-8111-111111111111'}
        self.submit_losing_response('/api/rolling-training-plans', training)
        self.restart_server()
        status, _, result = self.request('GET', '/api/rolling-training-plan-writes?key='+training['request_key'], headers=self.read)
        self.assertEqual((status, result['status']), (200, 'accepted'))
        replay_status, _, replay = self.request('POST', '/api/rolling-training-plans', training, self.write)
        self.assertEqual((replay_status, replay['write']['status']), (200, 'accepted'))
        training_id = replay['plan']['id']
        deadline = time.monotonic()+20
        while time.monotonic()<deadline:
            plan = next(p for p in self.server.rolling_training.list() if p['id']==training_id)
            if plan['status'] not in ('waiting','dispatching','running'): break
            time.sleep(.1)
        self.assertEqual(plan['status'], 'complete', plan['message'])

        registration = {'training_plan_id': training_id, 'rule_review_id': self.rule_id,
                        'tax_review_id': self.tax_id, 'internal_deadline': (self.deadline-timedelta(hours=1)).isoformat(),
                        'confirmed': True, 'request_key': 'd2222222-2222-4222-8222-222222222222'}
        self.submit_losing_response('/api/rolling-registration-plans', registration)
        self.restart_server()
        status, _, result = self.request('GET', '/api/rolling-registration-plan-writes?key='+registration['request_key'], headers=self.read)
        self.assertEqual((status, result['status']), (200, 'accepted'))
        replay_status, _, replay = self.request('POST', '/api/rolling-registration-plans', registration, self.write)
        self.assertEqual((replay_status, replay['write']['status']), (200, 'accepted'))
        ident = replay['plan']['id']
        deadline = time.monotonic()+25
        while time.monotonic()<deadline:
            plan = next(p for p in self.server.rolling_registration.list() if p['id']==ident)
            if plan['status']!='waiting': break
            time.sleep(.1)
        self.assertEqual(plan['status'], 'complete', plan['message'])
        self.assertEqual(len(self.repo.get('auto-exp-'+ident)['pairs']), 2)
