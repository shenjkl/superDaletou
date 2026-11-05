from datetime import timedelta
import http.client
import json
import threading
import time
import unittest
from unittest.mock import patch

from dlt.app import Handler, LocalServer
import test_prospective_series as fixtures
import test_static_registration as registration_fixtures


class StaticRegistrationHttpTests(unittest.TestCase):
    setUpBase = fixtures.SeriesTests.setUp
    review = fixtures.SeriesTests.review
    import_draw = fixtures.SeriesTests.import_draw
    _db = fixtures.SeriesTests._db
    prepare_second = fixtures.SeriesTests.prepare_second

    def setUp(self):
        self.setUpBase(); self.series.create(registration_fixtures.STATIC_SERIES_KEY, self.manifest)
        self.acl_patch = patch('dlt.recovery._require_private_acl')
        self.acl_patch.start(); self.addCleanup(self.acl_patch.stop)
        self.server = LocalServer(0, self.path); self.server.experiments = self.repo
        self.server.rolling_loop.start()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
        self.addCleanup(self.close_server)
        _, headers, session = self.request('GET', '/api/session')
        self.read = {'Cookie': headers['set-cookie'].split(';')[0]}
        self.write = {**self.read, 'Origin': self.server.origin, 'X-DLT-CSRF': session['csrf'],
                      'Content-Type': 'application/json'}

    def close_server(self):
        self.server.rolling_loop.stop(); self.server.shutdown(); self.server.server_close()
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

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=10)
        try:
            conn.request(method, path, body=json.dumps(body).encode() if body is not None else None, headers=headers or {})
            response = conn.getresponse()
            return response.status, {k.lower(): v for k, v in response.getheaders()}, json.loads(response.read())
        finally:
            conn.close()

    def test_local_background_registers_without_browser_and_rejects_cross_site(self):
        body = {'request_key': '99999999-9999-4999-8999-999999999999',
                'series_key': registration_fixtures.STATIC_SERIES_KEY, 'issue': '26017', 'calendar_review_id': self.cal_id,
                'rule_review_id': self.rule_id, 'tax_review_id': self.tax_id, 'expected_cutoff_issue': '26016',
                'history_periods': 6, 'start_at': (self.now-timedelta(minutes=1)).isoformat(),
                'internal_deadline': (self.deadline-timedelta(hours=1)).isoformat(), 'confirmed': True}
        self.assertEqual(self.request('GET', '/api/static-registration-plans')[0], 401)
        self.assertEqual(self.request('POST', '/api/static-registration-plans', body,
                                      {**self.write, 'Origin': 'https://attacker.invalid'})[0], 403)
        self.assertEqual(self.request('POST', '/api/static-registration-plans', {**body, 'confirmed': False}, self.write)[0], 400)
        status, _, response = self.request('POST', '/api/static-registration-plans', body, self.write)
        self.assertEqual(status, 200, response)
        ident = response['plan']['id']; deadline = time.monotonic()+20
        while time.monotonic() < deadline:
            plan = next(p for p in self.server.static_registration.list() if p['id'] == ident)
            if plan['status'] != 'waiting':
                break
            time.sleep(.1)
        self.assertEqual(plan['status'], 'complete', plan['message'])
        self.assertEqual(len(self.repo.get('static-exp-'+ident)['pairs']), 2)
        status, _, response = self.request('GET', '/api/static-registration-plans', headers=self.read)
        self.assertEqual((status, response['plans'][0]['status']), (200, 'complete'))

    def test_static_registration_recovers_after_response_loss_and_service_restart(self):
        body = {'request_key': '99999999-9999-4999-8999-999999999998',
                'series_key': registration_fixtures.STATIC_SERIES_KEY, 'issue': '26017',
                'calendar_review_id': self.cal_id, 'rule_review_id': self.rule_id,
                'tax_review_id': self.tax_id, 'expected_cutoff_issue': '26016',
                'history_periods': 6, 'start_at': (self.now-timedelta(minutes=1)).isoformat(),
                'internal_deadline': (self.deadline-timedelta(hours=1)).isoformat(), 'confirmed': True}
        committed, release = threading.Event(), threading.Event()
        errors = []
        original_json = Handler._json

        def drop(handler, status, data, **kwargs):
            if handler.path.split('?', 1)[0] == '/api/static-registration-plans' and status == 200 and data.get('write', {}).get('status') == 'accepted':
                committed.set()
                if not release.wait(15):
                    raise TimeoutError('test did not release dropped static registration response')
                handler.connection.close()
                return
            return original_json(handler, status, data, **kwargs)

        def submit():
            try:
                self.request('POST', '/api/static-registration-plans', body, self.write)
            except Exception as exc:
                errors.append(exc)

        with patch.object(Handler, '_json', drop):
            worker = threading.Thread(target=submit)
            worker.start()
            try:
                self.assertTrue(committed.wait(15), 'static registration did not commit before response loss')
            finally:
                release.set(); worker.join(15)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(errors), 1, errors)
        self.assertIsInstance(errors[0], (http.client.RemoteDisconnected, ConnectionError, OSError), errors)

        self.restart_server()
        status, _, result = self.request('GET', '/api/static-registration-plan-writes?key='+body['request_key'], headers=self.read)
        self.assertEqual((status, result['status']), (200, 'accepted'))
        replay_status, _, replay = self.request('POST', '/api/static-registration-plans', body, self.write)
        self.assertEqual((replay_status, replay['write']['status']), (200, 'accepted'))
        ident = replay['plan']['id']; deadline = time.monotonic()+20
        while time.monotonic()<deadline:
            plan = next(p for p in self.server.static_registration.list() if p['id']==ident)
            if plan['status']!='waiting': break
            time.sleep(.1)
        self.assertEqual(plan['status'], 'complete', plan['message'])
        self.assertEqual(len(self.repo.get('static-exp-'+ident)['pairs']), 2)
        status, _, listing = self.request('GET', '/api/static-registration-plans', headers=self.read)
        self.assertEqual((status, next(p for p in listing['plans'] if p['id']==ident)['status']), (200, 'complete'))


if __name__ == '__main__':
    unittest.main()
