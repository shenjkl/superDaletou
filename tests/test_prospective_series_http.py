from copy import deepcopy
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import http.client
import hashlib
import json
import sqlite3
import threading
import unittest
import uuid
from unittest.mock import patch

from dlt.candidates import _canonical
from dlt.prospective_closure_writes import digest as closure_digest
import test_prospective_series as series_fixtures
import test_source_correction_http as http_fixtures


class SeriesHttpTests(unittest.TestCase):
    setUp = series_fixtures.SeriesTests.setUp
    _db = series_fixtures.SeriesTests._db
    review = series_fixtures.SeriesTests.review
    start_server = http_fixtures.SourceCorrectionHttpTests.start_server

    def test_create_register_close_and_exact_seed_over_http(self):
        manifest = deepcopy(self.manifest)
        manifest['seeds']['26018']['b'] = str(2**64-1)
        request, read, write = self.start_server()
        body = {'series_key': series_fixtures.SERIES_KEY, 'manifest': manifest}
        self.assertEqual(request('GET', '/api/experiment-series')[0], 401)
        self.assertEqual(request('POST', '/api/experiment-series', json.dumps(body),
                                 {**write, 'X-DLT-CSRF': '00000000-0000-4000-8000-000000000008'})[0], 403)
        unsafe = deepcopy(body); unsafe['manifest']['seeds']['26018']['b'] = 2**64-1
        self.assertEqual(request('POST', '/api/experiment-series', json.dumps(unsafe), write)[0], 400)
        self.assertEqual(request('POST', '/api/experiment-series', json.dumps(body), write)[0], 400)
        body['manifest']['seeds']['26018']['b'] = str(2**32-1)
        status, _, result = request('POST', '/api/experiment-series', json.dumps(body), write)
        self.assertEqual(status, 200, result)
        self.assertEqual(result['series']['manifest']['seeds']['26018']['b'], 2**32-1)
        period = {'series_key': None, 'request_key': 'bad', 'protocol': self.protocol}
        self.assertEqual(request('POST', '/api/experiment-series/periods', json.dumps(period), write)[0], 400)
        period.update(series_key=series_fixtures.SERIES_KEY, request_key='period-1')
        status, _, result = request('POST', '/api/experiments/prepare', json.dumps(period), write)
        self.assertEqual(status, 200, result)
        period['require_intent'] = True
        status, _, result = request('POST', '/api/experiment-series/periods', json.dumps(period), write)
        self.assertEqual(status, 200, result)
        self.assertEqual(result['write']['status'], 'committed')
        series_key = series_fixtures.SERIES_KEY
        _, _, listing = request('GET', f'/api/experiment-series?key={series_key}', headers=read)
        self.assertEqual(listing['series']['periods'], [{'issue': '26017', 'experiment_key': 'period-1'}])
        self.assertEqual(request('GET', f'/api/experiment-series/report?key={series_key}')[0], 401)
        status, _, payload = request('GET', f'/api/experiment-series/report?key={series_key}', headers=read)
        self.assertEqual(status, 200)
        self.assertEqual((payload['report']['planned'], payload['report']['valid']), (2, 0))
        self.assertIsNone(payload['report']['mean_difference_fraction'])
        self.assertEqual(request('GET', '/api/experiment-series/report?key=missing', headers=read)[0], 404)
        close_request = {'object_key': series_key, 'reason': 'done'}
        close_key = 'close-series-lost-response'
        close_sha = closure_digest('series_close', close_request)
        close_body = {**close_request, 'request_key': close_key, 'request_sha256': close_sha}
        from dlt.app import Handler
        response_ready = threading.Event()
        release_response = threading.Event()
        errors = []
        original_json = Handler._json

        def lose_committed_series_close(handler, response_status, data, **kwargs):
            if (handler.path == '/api/experiment-series/close' and response_status == 200
                    and data.get('write', {}).get('status') == 'accepted'):
                response_ready.set()
                if not release_response.wait(timeout=10):
                    raise TimeoutError('series close test did not release the response drop')
                handler.connection.close()
                return
            return original_json(handler, response_status, data, **kwargs)

        def close_and_lose_response():
            try:
                request('POST', '/api/experiment-series/close', json.dumps(close_body), write)
            except Exception as exc:
                errors.append(exc)

        with patch.object(Handler, '_json', lose_committed_series_close):
            closer = threading.Thread(target=close_and_lose_response)
            closer.start()
            try:
                self.assertTrue(response_ready.wait(timeout=10), 'series close did not reach its accepted response')
                status, _, state = request(
                    'GET', '/api/prospective-closure-writes?request_key='+close_key+
                    '&operation=series_close&request_sha256='+close_sha, headers=read)
                self.assertEqual(status, 200, state)
                self.assertEqual(state['write']['status'], 'accepted')
                self.assertEqual(state['write']['result']['reason'], 'done')
                with closing(sqlite3.connect(self.path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM series_closures WHERE series_key=?',
                                                 (series_key,)).fetchone()[0], 1)
            finally:
                release_response.set()
                closer.join(timeout=10)
            self.assertFalse(closer.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], (http.client.RemoteDisconnected, ConnectionError, OSError))
        status, _, replay = request('POST', '/api/experiment-series/close', json.dumps(close_body), write)
        self.assertEqual(status, 200, replay)
        self.assertEqual(replay['write']['status'], 'accepted')
        status, _, state = request(
            'GET', '/api/prospective-closure-writes?request_key='+close_key+
            '&operation=series_close&request_sha256='+close_sha, headers=read)
        self.assertEqual((status, state['write']['status']), (200, 'accepted'))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM series_closures WHERE series_key=?',
                                        (series_key,)).fetchone()[0], 1)
        self.assertEqual(request('POST', '/api/experiment-series/periods', json.dumps(period), write)[0], 200)

        # Race a second series close against its atomic fence.
        race_key = str(uuid.uuid4())
        self.series.create(race_key, deepcopy(self.manifest))
        race_close_request = {'object_key': race_key, 'reason': 'race'}
        race_request_key = 'close-series-fence-race'
        race_sha = closure_digest('series_close', race_close_request)
        race_body = {**race_close_request, 'request_key': race_request_key, 'request_sha256': race_sha}
        start = threading.Barrier(3)

        def submit_close():
            start.wait()
            return request('POST', '/api/experiment-series/close', json.dumps(race_body), write)

        def submit_fence():
            start.wait()
            return request('POST', '/api/prospective-closure-writes/fence', json.dumps({
                'request_key': race_request_key, 'operation': 'series_close',
                'request': race_close_request, 'request_sha256': race_sha}), write)

        with ThreadPoolExecutor(max_workers=2) as pool:
            close_future = pool.submit(submit_close)
            fence_future = pool.submit(submit_fence)
            start.wait()
            close_status, _, close_result = close_future.result(timeout=15)
            fence_status, _, fence_result = fence_future.result(timeout=15)
        status, _, final = request(
            'GET', '/api/prospective-closure-writes?request_key='+race_request_key+
            '&operation=series_close&request_sha256='+race_sha, headers=read)
        self.assertEqual(status, 200, final)
        final_status = final['write']['status']
        fence_write_status = fence_result['write']['status']
        with closing(sqlite3.connect(self.path)) as db:
            close_count = db.execute('SELECT count(*) FROM series_closures WHERE series_key=?',
                                     (race_key,)).fetchone()[0]
        if final_status == 'accepted':
            self.assertEqual(close_status, 200, close_result)
            self.assertEqual(close_result['write']['status'], 'accepted')
            self.assertEqual((fence_status, fence_write_status), (200, 'accepted'))
            self.assertEqual(close_count, 1)
        else:
            self.assertEqual(final_status, 'fenced')
            self.assertEqual(close_status, 409, close_result)
            self.assertEqual((fence_status, fence_write_status), (409, 'fenced'))
            self.assertEqual(close_count, 0)

    def test_legacy_oversized_seed_is_exactly_readable_over_http(self):
        series_key = series_fixtures.LEGACY_SERIES_KEY
        original_manifest = deepcopy(self.manifest)
        content = self.series.create(series_key, original_manifest)
        content['manifest']['seeds']['26018']['b'] = 2**64-1
        raw = _canonical(content)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER experiment_series_no_update')
            db.execute('UPDATE experiment_series SET content_json=?,content_sha256=? WHERE series_key=?',
                       (raw, hashlib.sha256(raw).hexdigest(), series_key))
            db.execute("CREATE TRIGGER experiment_series_no_update BEFORE UPDATE ON experiment_series BEGIN SELECT RAISE(ABORT, 'immutable experiment series'); END")
        request, read, write = self.start_server()
        status, _, response = request('GET', f'/api/experiment-series?key={series_key}', headers=read)
        self.assertEqual(status, 200)
        self.assertEqual(response['series']['unsupported_seed_issues'], ['26018'])
        self.assertEqual(response['series']['content']['manifest']['seeds']['26018']['b'], str(2**64-1))


if __name__ == '__main__':
    unittest.main()
