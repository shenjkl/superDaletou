from copy import deepcopy
import http.client
import hashlib
import json
import threading
import unittest
import uuid
from unittest.mock import patch

from dlt.app import LocalServer
from dlt.scheduler import Scheduler
from dlt.candidates import _canonical
from dlt.source_write_protocol import digest as source_write_digest
import test_prospective_settlement as fixtures
from test_prospective import swap_first_second_prize_codes


class SourceCorrectionHttpTests(unittest.TestCase):
    setUp = fixtures.PaperResultsTests.setUp
    import_draw = fixtures.PaperResultsTests.import_draw
    review = fixtures.PaperResultsTests.review

    def start_server(self):
        # The sandbox cannot inspect temp-directory ACLs; this HTTP class
        # exercises route behavior while recovery tests cover ACL enforcement.
        with patch('dlt.recovery._require_private_acl'):
            server = LocalServer(0, self.path)
        self.http_server = server
        server.experiments = self.repo
        server.jobs.recover_interrupted(); server.backtests.recover_interrupted()
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        self.addCleanup(lambda: (server.shutdown(), server.server_close(), server.jobs.stop(),
                                 server.backtests.stop(), thread.join(timeout=3)))
        def request(method, path, body=None, headers=None):
            conn = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=10)
            try:
                conn.request(method, path, body=body, headers=headers or {})
                response = conn.getresponse()
                return response.status, {k.lower(): v for k, v in response.getheaders()}, json.loads(response.read())
            finally:
                conn.close()
        _, headers, session = request('GET', '/api/session')
        cookie = headers['set-cookie'].split(';')[0]
        write = {'Cookie': cookie, 'Origin': server.origin, 'X-DLT-CSRF': session['csrf'],
                 'Content-Type': 'application/json'}
        return request, {'Cookie': cookie}, write

    def correction_body(self):
        corrected = deepcopy(self.frozen['rule_review']['extracted'])
        corrected['version'] = 'corrected-http-v2'
        corrected = swap_first_second_prize_codes(corrected)
        new_id = self.review('rule', corrected)
        request = {'old_review_id': self.rule_id, 'new_review_id': new_id, 'issue': None,
                   'note': 'synthetic correction only', 'confirmed': True}
        return {**request, 'request_key': str(uuid.uuid4()),
                'request_sha256': source_write_digest(request)}

    def test_http_correction_invalidates_recalculates_and_retries_idempotently(self):
        sid = self.import_draw(self.draw)
        original = self.paper.process_one('exp', sid)
        body = self.correction_body()
        request, read, write = self.start_server()
        with patch('dlt.app.PaperResults', return_value=self.paper):
            self.assertEqual(request('GET', '/api/source-corrections')[0], 401)
            self.assertEqual(request('POST', '/api/source-corrections', json.dumps(body),
                                     {**write, 'X-DLT-CSRF': 'bad'})[0], 403)
            status, _, response = request('POST', '/api/source-corrections', json.dumps(body), write)
            self.assertEqual(status, 200, response)
            correction_id = response['correction']['correction_id']
            self.assertEqual(response['followup']['details']['exp']['status'], 'complete')
            replacement = response['followup']['details']['exp']['group_id']
            self.assertNotEqual(replacement, original['group_id'])
            _, _, reviews = request('GET', '/api/source-reviews', headers=read)
            old = next(item for item in reviews['reviews'] if item['review_id'] == self.rule_id)
            self.assertEqual(old['superseded_by'], body['new_review_id'])
            _, _, history = request('GET', '/api/experiment-history?key=exp', headers=read)
            self.assertEqual([p['group_id'] for p in history['history']['publications']], [original['group_id'], replacement])
            retry_request = {'correction_id': correction_id, 'scope': 'global'}
            status, _, retried = request('POST', '/api/source-corrections/retry', json.dumps({
                **retry_request, 'request_key': str(uuid.uuid4()),
                'request_sha256': hashlib.sha256(_canonical(retry_request)).hexdigest()}), write)
            self.assertEqual(status, 200)
            self.assertEqual(retried['followup']['details']['exp']['group_id'], replacement)
            _, _, history = request('GET', '/api/experiment-history?key=exp', headers=read)
            self.assertEqual(len(history['history']['publications']), 2)
            status, _, duplicate = request('POST', '/api/source-corrections', json.dumps(body), write)
            self.assertEqual(status, 200, duplicate)
            self.assertEqual(duplicate['correction']['correction_id'], correction_id)

    def test_committed_correction_recovers_if_immediate_dispatch_fails(self):
        sid = self.import_draw(self.draw)
        original = self.paper.process_one('exp', sid)
        body = self.correction_body()
        request, _, write = self.start_server()
        with patch('dlt.app.PaperResults', return_value=self.paper), \
             patch.object(self.paper, 'process_source_correction', side_effect=RuntimeError('injected dispatch failure')):
            status, _, response = request('POST', '/api/source-corrections', json.dumps(body), write)
        self.assertEqual(status, 200)
        self.assertEqual(response['followup']['status'], 'pending_or_unknown')
        self.assertEqual(self.repo.list_source_corrections()[0]['followup_status'], 'pending')
        self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
        with patch('dlt.app.PaperResults', return_value=self.paper):
            _, _, history = request('GET', '/api/experiment-history?key=exp', headers={'Cookie': write['Cookie']})
        self.assertEqual(history['history']['current']['status'], 'review_required')
        self.assertIsNone(history['history']['current']['group_id'])
        self.assertEqual(history['history']['publications'][0]['group_id'], original['group_id'])
        scheduler = Scheduler(self.path, clock=lambda: self.now)
        scheduler.initialize(); scheduler.run_due()
        self.assertEqual(self.repo.list_source_corrections()[0]['followup_status'], 'complete')
        current = self.paper.inspect_current('exp')
        self.assertEqual(current['status'], 'locally_attested')
        self.assertNotEqual(current['group_id'], original['group_id'])

    def test_correction_lost_response_status_replay_and_fence_over_http(self):
        from dlt.app import Handler

        self.import_draw(self.draw)
        body = self.correction_body()
        request, read, write = self.start_server()
        ready = threading.Event()
        release = threading.Event()
        errors = []
        original_json = Handler._json

        def drop_committed_response(handler, response_status, data, **kwargs):
            if (handler.path == '/api/source-corrections' and response_status == 200
                    and data.get('write', {}).get('status') == 'accepted'):
                ready.set()
                if not release.wait(timeout=10):
                    raise TimeoutError('test did not release the dropped correction response')
                handler.connection.close()
                return
            return original_json(handler, response_status, data, **kwargs)

        def submit():
            try:
                request('POST', '/api/source-corrections', json.dumps(body), write)
            except Exception as exc:
                errors.append(exc)

        with patch.object(Handler, '_json', drop_committed_response):
            worker = threading.Thread(target=submit)
            worker.start()
            try:
                self.assertTrue(ready.wait(timeout=10), 'correction did not reach its committed response')
                status, _, current = request(
                    'GET', '/api/source-write-status?request_key='+body['request_key']
                    +'&request_sha256='+body['request_sha256'], headers=read)
                self.assertEqual(status, 200)
                self.assertEqual(current['write']['status'], 'accepted')
                correction_id = current['write']['result']['correction_id']
                self.assertTrue(correction_id)
            finally:
                release.set()
                worker.join(timeout=10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], (http.client.RemoteDisconnected, ConnectionError, OSError))

        status, _, replay = request('POST', '/api/source-corrections', json.dumps(body), write)
        self.assertEqual(status, 200, replay)
        self.assertEqual(replay['write']['status'], 'accepted')
        self.assertEqual(replay['write']['result']['correction_id'], correction_id)
        self.assertEqual(len(self.repo.list_source_corrections()), 1)

        # A fence committed first must exclude the delayed correction request.
        fenced = self.correction_body()
        request_data = {key: value for key, value in fenced.items()
                        if key not in ('request_key', 'request_sha256')}
        fence_body = {'request_key': fenced['request_key'], 'operation': 'source_correction',
                      'request': request_data, 'request_sha256': fenced['request_sha256']}
        status, _, fence_result = request('POST', '/api/source-writes/fence',
                                          json.dumps(fence_body), write)
        self.assertEqual(status, 409, fence_result)
        self.assertEqual(fence_result['write']['status'], 'fenced')
        status, _, late = request('POST', '/api/source-corrections', json.dumps(fenced), write)
        self.assertEqual(status, 409, late)
        self.assertEqual(len(self.repo.list_source_corrections()), 1)
        status, _, current = request(
            'GET', '/api/source-write-status?request_key='+fenced['request_key']
            +'&request_sha256='+fenced['request_sha256'], headers=read)
        self.assertEqual((status, current['write']['status']), (200, 'fenced'))


if __name__ == '__main__':
    unittest.main()
