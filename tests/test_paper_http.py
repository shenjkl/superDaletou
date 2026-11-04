from copy import deepcopy
import http.client
import json
import sqlite3
import threading
import unittest
from contextlib import closing
from unittest.mock import patch

from dlt.app import LocalServer
import test_prospective_settlement as paper_fixtures


class PaperHttpTests(unittest.TestCase):
    def setUp(self):
        # The sandbox cannot inspect temp-directory ACLs; ACL enforcement is
        # covered separately from these route-level HTTP contracts.
        self._acl_patch = patch('dlt.recovery._require_private_acl')
        self._acl_patch.start()
        self.addCleanup(self._acl_patch.stop)
        paper_fixtures.PaperResultsTests.setUp(self)
    import_draw = paper_fixtures.PaperResultsTests.import_draw
    review = paper_fixtures.PaperResultsTests.review

    def test_conflict_review_recalculates_and_preserves_historical_result(self):
        original_id = self.import_draw(self.draw)
        original = self.paper.process_one('exp', original_id)
        updated = deepcopy(self.draw); updated['prizeLevelList'][0]['stakeAmount'] = '7000'
        selected_id = self.import_draw(updated)
        server = LocalServer(0, self.path)
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

        with patch('dlt.app.PaperResults', return_value=self.paper):
            _, headers, session = request('GET', '/api/session')
            cookie = headers['set-cookie'].split(';')[0]
            write_headers = {'Cookie': cookie, 'Origin': server.origin, 'X-DLT-CSRF': session['csrf'],
                             'Content-Type': 'application/json'}
            read_headers = {'Cookie': cookie}
            self.assertEqual(request('GET', '/api/draw-conflicts?issue=26017')[0], 401)
            status, _, result = request('GET', '/api/draw-conflicts?issue=26017', headers=read_headers)
            self.assertEqual(status, 200)
            first_view = result['review']
            selected = next(v for v in first_view['variants']
                            if any(s['snapshot_id'] == selected_id for s in v['snapshots']))
            body = {'issue': '26017', 'snapshot_id': selected_id, 'variants_sha256': first_view['variants_sha256'],
                    'row_sha256': selected['row_sha256'], 'note': 'synthetic correction reviewed', 'confirmed': True}
            denied = {**write_headers, 'X-DLT-CSRF': 'bad'}
            self.assertEqual(request('POST', '/api/draw-conflicts/resolve', json.dumps(body), denied)[0], 403)
            third = deepcopy(self.draw); third['prizeLevelList'][0]['stakeAmount'] = '8000'
            self.import_draw(third)
            status, _, result = request('POST', '/api/draw-conflicts/resolve', json.dumps(body), write_headers)
            self.assertEqual(status, 400)
            self.assertIn('changed since review', result['error'])
            _, _, refreshed = request('GET', '/api/draw-conflicts?issue=26017', headers=read_headers)
            body['variants_sha256'] = refreshed['review']['variants_sha256']
            status, _, result = request('POST', '/api/draw-conflicts/resolve', json.dumps(body), write_headers)
            self.assertEqual(status, 200)
            self.assertEqual(result['recalculations']['exp']['status'], 'complete')
            replacement = result['recalculations']['exp']['group_id']
            self.assertNotEqual(replacement, original['group_id'])
            _, _, history = request('GET', '/api/experiment-history?key=exp', headers=read_headers)
            publications = history['history']['publications']
            self.assertEqual([item['group_id'] for item in publications], [original['group_id'], replacement])
            self.assertEqual(publications[1]['previous_group'], original['group_id'])
            self.assertEqual(publications[0]['result'], original['result'])
            self.assertEqual(self.paper.inspect_current('exp')['group_id'], replacement)
            retry = {'issue': '26017', 'snapshot_id': selected_id}
            status, _, repeated = request('POST', '/api/paper-results/recalculate-issue', json.dumps(retry), write_headers)
            self.assertEqual(status, 200)
            self.assertEqual(repeated['recalculations']['exp']['group_id'], replacement)
            _, _, history = request('GET', '/api/experiment-history?key=exp', headers=read_headers)
            self.assertEqual(len(history['history']['publications']), 2)
            fourth = deepcopy(self.draw); fourth['prizeLevelList'][0]['stakeAmount'] = '9000'
            self.import_draw(fourth)
            status, _, _ = request('POST', '/api/paper-results/recalculate-issue', json.dumps(retry), write_headers)
            self.assertEqual(status, 400)
            self.assertEqual(self.paper.inspect_current('exp')['status'], 'review_required')
            with closing(sqlite3.connect(self.path)) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM paper_resolutions').fetchone()[0], 1)


if __name__ == '__main__':
    unittest.main()
