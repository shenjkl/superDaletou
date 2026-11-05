from copy import deepcopy
import http.client
import json
import sqlite3
import sys
import threading
import traceback
import uuid
import unittest
from contextlib import closing
from unittest.mock import patch

from dlt.app import Handler, LocalServer
import test_prospective_settlement as paper_fixtures


class PaperHttpTests(unittest.TestCase):
    def setUp(self):
        # The sandbox cannot inspect temp-directory ACLs; ACL enforcement is
        # covered separately from these route-level HTTP contracts.
        self._acl_patch = patch('dlt.recovery._require_private_acl')
        self._acl_patch.start()
        self.addCleanup(self._acl_patch.stop)
        paper_fixtures.PaperResultsTests.setUp(self)
        original_json = Handler._json
        def report_internal_error(handler, status, payload, **kwargs):
            if status >= 500 and sys.exception() is not None:
                traceback.print_exception(sys.exception())
            return original_json(handler, status, payload, **kwargs)
        self._error_patch = patch.object(Handler, '_json', report_internal_error)
        self._error_patch.start()
        self.addCleanup(self._error_patch.stop)
    import_draw = paper_fixtures.PaperResultsTests.import_draw
    review = paper_fixtures.PaperResultsTests.review

    def test_conflict_review_recalculates_and_preserves_historical_result(self):
        original_id = self.import_draw(self.draw)
        original = self.paper.process_one('exp', original_id)
        updated = deepcopy(self.draw); updated['prizeLevelList'][0]['stakeAmount'] = '7000'
        selected_id = self.import_draw(updated)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER paper_write_requests_guard_update')
            db.execute("""CREATE TRIGGER paper_write_requests_guard_update BEFORE UPDATE ON paper_write_requests
                WHEN OLD.request_key!=NEW.request_key OR OLD.operation!=NEW.operation OR OLD.object_key!=NEW.object_key
                  OR OLD.request_sha256!=NEW.request_sha256 OR OLD.request_json!=NEW.request_json
                  OR OLD.created_at!=NEW.created_at OR OLD.result_json IS NOT NULL OR NEW.result_json IS NULL
                BEGIN SELECT RAISE(ABORT, 'immutable paper write receipt'); END""")
        server = LocalServer(0, self.path)
        with closing(sqlite3.connect(self.path)) as db:
            migrated_trigger = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' "
                                           "AND name='paper_write_requests_guard_update'").fetchone()[0]
            self.assertIn('json_extract', migrated_trigger.lower())
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
            request_key = str(uuid.uuid4())
            request_sha256, _ = self.paper._write_request('draw_resolve', body)
            accepted_body = {**body, 'request_key': request_key, 'request_sha256': request_sha256}
            status, _, result = request('POST', '/api/draw-conflicts/resolve', json.dumps(accepted_body), write_headers)
            self.assertEqual(status, 200)
            self.assertEqual(result['recalculations']['exp']['status'], 'complete')
            self.assertEqual(result['write']['status'], 'accepted')
            self.assertEqual(result['write']['result']['resolution'], result['resolution'])
            self.assertEqual(result['write']['result']['recalculations']['exp']['status'], 'complete')
            replacement = result['recalculations']['exp']['group_id']
            self.assertNotEqual(replacement, original['group_id'])
            status, _, replayed = request('POST', '/api/draw-conflicts/resolve', json.dumps(accepted_body), write_headers)
            self.assertEqual(status, 200)
            self.assertEqual(replayed['write']['result'], result['write']['result'])
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
