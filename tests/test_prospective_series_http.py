from copy import deepcopy
from contextlib import closing
import hashlib
import json
import sqlite3
import unittest

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
        close_body = {**close_request, 'request_key': 'close-series-1',
                      'request_sha256': closure_digest('series_close', close_request)}
        status, _, _ = request('POST', '/api/experiment-series/close', json.dumps(close_body), write)
        self.assertEqual(status, 200)
        self.assertEqual(request('POST', '/api/experiment-series/periods', json.dumps(period), write)[0], 200)

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
