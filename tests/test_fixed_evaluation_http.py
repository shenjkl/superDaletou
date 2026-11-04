import http.client
import json
import threading
import time
import unittest
from unittest.mock import patch

from dlt.app import LocalServer, _fixed_evaluation_write_digest
import test_fixed_evaluation as fixtures


class FixedEvaluationHttpTests(unittest.TestCase):
    setUpBase = fixtures.FixedEvaluationTests.setUp

    def setUp(self):
        self.setUpBase()
        self.acl_patch = patch('dlt.recovery._require_private_acl')
        self.acl_patch.start(); self.addCleanup(self.acl_patch.stop)
        self.server = LocalServer(0, self.path)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
        self.addCleanup(self.close_server)
        _, headers, session = self.request('GET', '/api/session')
        self.read = {'Cookie': headers['set-cookie'].split(';')[0]}
        self.write = {**self.read, 'Origin': self.server.origin, 'X-DLT-CSRF': session['csrf'],
                      'Content-Type': 'application/json'}

    def close_server(self):
        self.server.shutdown(); self.server.server_close(); self.server.fixed_evaluations.stop()
        self.server.jobs.stop(); self.server.backtests.stop(); self.thread.join(timeout=3)

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=15)
        try:
            conn.request(method, path, body=json.dumps(body).encode() if body is not None else None, headers=headers or {})
            response = conn.getresponse()
            return response.status, {k.lower(): v for k, v in response.getheaders()}, json.loads(response.read())
        finally:
            conn.close()

    def test_confirmed_multimethod_protocol_and_result_are_session_protected(self):
        body = {'reservation_id': self.reservation, 'methods': self.methods, 'hypothesis': '预先固定比较',
                'timeout': 30, 'confirmed': True,
                'request_key': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'}
        body['request_sha256'] = _fixed_evaluation_write_digest('freeze', {
            key: body[key] for key in ('reservation_id', 'methods', 'hypothesis', 'timeout', 'confirmed')})
        self.assertEqual(self.request('GET', '/api/fixed-evaluations')[0], 401)
        self.assertEqual(self.request('POST', '/api/fixed-evaluations', body, {**self.write, 'Origin': 'https://evil.invalid'})[0], 403)
        self.assertEqual(self.request('POST', '/api/fixed-evaluations', {**body, 'confirmed': False}, self.write)[0], 400)
        status, _, result = self.request('POST', '/api/fixed-evaluations', body, self.write)
        self.assertEqual(status, 200, result)
        self.assertEqual(result['evaluation']['status'], 'pending')
        start_body = {'reservation_id': self.reservation, 'request_key': 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
                      'retry': False}
        start_body['request_sha256'] = _fixed_evaluation_write_digest('start', {
            'reservation_id': self.reservation, 'retry': False})
        status, _, result = self.request('POST', '/api/fixed-evaluations/start', start_body, self.write)
        self.assertEqual(status, 200, result)
        until = time.monotonic()+30
        while time.monotonic() < until:
            result = self.server.fixed_evaluations.get(self.reservation)
            if result['status'] != 'running': break
            time.sleep(.05)
        self.assertEqual(result['status'], 'complete', result['error'])
        status, _, result = self.request('GET', '/api/fixed-evaluations', headers=self.read)
        self.assertEqual(status, 200)
        self.assertEqual(result['evaluations'][0]['validity'], 'local_descriptive')
        self.assertEqual([x['id'] for x in result['evaluations'][0]['report']['methods']], ['hot', 'cold'])

    def test_start_before_freeze_does_not_create_partial_schema(self):
        body = {'reservation_id': self.reservation, 'request_key': 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
                'retry': False}
        body['request_sha256'] = _fixed_evaluation_write_digest('start', {
            'reservation_id': self.reservation, 'retry': False})
        self.assertEqual(self.request('POST', '/api/fixed-evaluations/start', body, self.write)[0], 400)
        status, _, result = self.request('GET', '/api/integrity', headers=self.read)
        self.assertEqual(status, 200)
        self.assertEqual(result['check']['status'], 'passed', result['check'])
