import json
import unittest

import test_rolling_policy as fixtures
import test_source_correction_http as http_fixtures


class RollingPolicyHttpTests(unittest.TestCase):
    setUp = fixtures.RollingPolicyTests.setUp
    review = fixtures.RollingPolicyTests.review
    import_draw = fixtures.RollingPolicyTests.import_draw
    _db = fixtures.RollingPolicyTests._db
    prepare_second = fixtures.RollingPolicyTests.prepare_second
    train = fixtures.RollingPolicyTests.train
    start_server = http_fixtures.SourceCorrectionHttpTests.start_server

    def test_preview_then_fix_rolling_policy_over_local_session(self):
        request, read, write = self.start_server()
        body = {'pair_key': 'first-tree-1'}
        self.assertEqual(request('POST', '/api/experiment-series/rolling-preview', json.dumps(body),
                                 {**write, 'Origin': 'https://attacker.invalid'})[0], 403)
        self.assertEqual(request('POST', '/api/experiment-series/rolling-preview', json.dumps(body),
                                 {**write, 'X-DLT-CSRF': 'bad'})[0], 403)
        status, _, response = request('POST', '/api/experiment-series/rolling-preview', json.dumps(body), write)
        self.assertEqual(status, 200, response)
        spec = response['spec']
        self.assertEqual((spec['training_periods'], spec['validation_periods']), (16, 4))
        status, _, listing = request('GET', '/api/experiment-series', headers=read)
        self.assertEqual(status, 200)
        self.assertEqual(listing['series'], [])
        status, _, response = request('POST', '/api/experiment-series',
            json.dumps({'series_key': 'rolling-http', 'manifest': self.manifest}), write)
        self.assertEqual(status, 200, response)
        self.assertEqual(response['series']['rolling_spec'], spec)
        self.assertEqual(response['series']['manifest']['retraining_policy'], 'rolling_retrain_v1')
        self.assertEqual(request('POST', '/api/experiment-series/rolling-preview',
                                 json.dumps({'pair_key': 'pair-1'}), write)[0], 400)
