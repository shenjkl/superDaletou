from datetime import timedelta
import http.client
import json
import threading
import time
import unittest
from unittest.mock import patch

from dlt.app import LocalServer
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


if __name__ == '__main__':
    unittest.main()
