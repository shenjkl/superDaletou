import http.client
import json
import sqlite3
import threading
import time
import unittest
from contextlib import closing
from urllib.parse import urlencode
from unittest.mock import patch

from dlt.app import Handler, LocalServer, _fixed_evaluation_write_digest
from dlt.holdout_reservation import reserve_range
from dlt.fixed_evaluation import _environment as fixed_evaluation_environment
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

    def restart_server(self):
        old = self.server
        old.shutdown(); old.server_close(); old.fixed_evaluations.stop()
        old.jobs.stop(); old.backtests.stop(); self.thread.join(timeout=5)
        self.assertFalse(self.thread.is_alive())
        self.server = LocalServer(old.server_port, self.path)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        _, headers, session = self.request('GET', '/api/session')
        self.read = {'Cookie': headers['set-cookie'].split(';')[0]}
        self.write = {**self.read, 'Origin': self.server.origin, 'X-DLT-CSRF': session['csrf'],
                      'Content-Type': 'application/json'}

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


    def test_freeze_and_supersede_response_loss_recovery_and_fenced_late_write(self):
        def key(letter):
            return f'{letter * 8}-{letter * 4}-4{letter * 3}-8{letter * 3}-{letter * 12}'

        def submit_losing_response(path, body):
            ready = threading.Event()
            release = threading.Event()
            errors = []
            original_json = Handler._json

            def drop_after_accept(handler, response_status, data, **kwargs):
                route = handler.path.split('?', 1)[0]
                state = data.get('write', {}).get('status') if isinstance(data, dict) else None
                if route == path and state == 'accepted' and response_status == 200:
                    ready.set()
                    if not release.wait(timeout=15):
                        raise TimeoutError('test did not release dropped fixed evaluation response')
                    handler.connection.close()
                    return
                if route == path:
                    errors.append(('unexpected response', response_status, data))
                    ready.set()
                return original_json(handler, response_status, data, **kwargs)

            def submit():
                try:
                    self.request('POST', path, body, self.write)
                except Exception as exc:
                    errors.append(exc)

            with patch.object(Handler, '_json', drop_after_accept):
                thread = threading.Thread(target=submit)
                thread.start()
                try:
                    self.assertTrue(ready.wait(timeout=15), f'{path} did not commit before its response')
                finally:
                    release.set()
                    thread.join(timeout=15)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(errors), 1, errors)
            self.assertIsInstance(errors[0], (http.client.RemoteDisconnected, ConnectionError, OSError), errors)

        def status_for(request_key, operation, digest):
            query = urlencode({'request_key': request_key, 'operation': operation, 'request_sha256': digest})
            status, _, result = self.request('GET', '/api/fixed-evaluation-writes?' + query, headers=self.read)
            self.assertEqual(status, 200, result)
            return result['write']

        freeze = {'reservation_id': self.reservation, 'methods': self.methods,
                  'hypothesis': '预先固定比较', 'timeout': 30, 'confirmed': True,
                  'request_key': key('a')}
        freeze['request_sha256'] = _fixed_evaluation_write_digest('freeze', {
            k: freeze[k] for k in ('reservation_id', 'methods', 'hypothesis', 'timeout', 'confirmed')})
        submit_losing_response('/api/fixed-evaluations', freeze)
        self.restart_server()
        self.assertEqual(status_for(freeze['request_key'], 'freeze', freeze['request_sha256'])['status'], 'accepted')
        replay = self.request('POST', '/api/fixed-evaluations', freeze, self.write)
        self.assertEqual((replay[0], replay[2]['write']['status']), (200, 'accepted'))

        pending_reservation = self.reservation
        supersede = {'reservation_id': pending_reservation, 'reason': '修订预先固定的方法集合',
                     'confirmed': True, 'request_key': key('e')}
        supersede['request_sha256'] = _fixed_evaluation_write_digest('supersede', {
            k: supersede[k] for k in ('reservation_id', 'reason', 'confirmed')})
        replacement_environment = {**fixed_evaluation_environment(), 'python': '3.12.999'}
        with patch('dlt.fixed_evaluation._environment', return_value=replacement_environment):
            submit_losing_response('/api/fixed-evaluations/supersede', supersede)
        self.assertEqual(status_for(supersede['request_key'], 'supersede',
                                    supersede['request_sha256'])['status'], 'accepted')
        replay = self.request('POST', '/api/fixed-evaluations/supersede', supersede, self.write)
        self.assertEqual((replay[0], replay[2]['write']['status']), (200, 'accepted'))
        self.assertEqual(replay[2]['reservation']['content']['supersession_reason'], supersede['reason'])
        with closing(sqlite3.connect(self.path)) as db:
            lineage = [json.loads(row[0]) for row in db.execute('SELECT content_json FROM holdout_reservations')]
        supersessions = sum(item.get('supersedes') == pending_reservation for item in lineage)
        self.assertEqual(supersessions, 1)

        successor_id = replay[2]['reservation']['id']
        fenced_reservation = successor_id
        fenced_key = key('f')
        fenced_request = {'reservation_id': fenced_reservation, 'methods': self.methods,
                          'hypothesis': '将被封止的比较', 'timeout': 30, 'confirmed': True}
        fenced_digest = _fixed_evaluation_write_digest('freeze', fenced_request)
        fence_body = {'request_key': fenced_key, 'operation': 'freeze', 'request': fenced_request,
                      'request_sha256': fenced_digest}
        status, _, fence_result = self.request('POST', '/api/fixed-evaluation-writes/fence', fence_body, self.write)
        self.assertEqual((status, fence_result['write']['status']), (409, 'fenced'))
        fenced_freeze = {**fenced_request, 'request_key': fenced_key, 'request_sha256': fenced_digest}
        status, _, rejected = self.request('POST', '/api/fixed-evaluations', fenced_freeze, self.write)
        self.assertEqual((status, rejected['write']['status']), (409, 'fenced'))
        self.assertEqual(status_for(fenced_key, 'freeze', fenced_digest)['status'], 'fenced')
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT status FROM fixed_evaluations WHERE reservation_id=?',
                              (fenced_reservation,)).fetchall()
        self.assertEqual(rows, [('pending',)])
