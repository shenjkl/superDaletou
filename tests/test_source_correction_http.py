from copy import deepcopy
import http.client
import json
import threading
import unittest
from unittest.mock import patch

from dlt.app import LocalServer
from dlt.scheduler import Scheduler
import test_prospective_settlement as fixtures


class SourceCorrectionHttpTests(unittest.TestCase):
    setUp = fixtures.PaperResultsTests.setUp
    import_draw = fixtures.PaperResultsTests.import_draw
    review = fixtures.PaperResultsTests.review

    def start_server(self):
        server = LocalServer(0, self.path)
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
        corrected['payout_sort_map'] = {'first': '301', 'first-extra': '401', 'second': '101', 'second-extra': '201'}
        new_id = self.review('rule', corrected)
        return {'old_review_id': self.rule_id, 'new_review_id': new_id,
                'note': 'synthetic correction only', 'confirmed': True}

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
            status, _, retried = request('POST', '/api/source-corrections/retry',
                                         json.dumps({'correction_id': correction_id}), write)
            self.assertEqual(status, 200)
            self.assertEqual(retried['followup']['details']['exp']['group_id'], replacement)
            _, _, history = request('GET', '/api/experiment-history?key=exp', headers=read)
            self.assertEqual(len(history['history']['publications']), 2)
            self.assertEqual(request('POST', '/api/source-corrections', json.dumps(body), write)[0], 400)

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


if __name__ == '__main__':
    unittest.main()
