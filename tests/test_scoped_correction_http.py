from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

import test_source_correction_http as fixtures
from test_prospective import swap_first_second_prize_codes
from dlt.source_write_protocol import digest as source_write_digest


class ScopedCorrectionHttpTests(unittest.TestCase):
    setUp = fixtures.SourceCorrectionHttpTests.setUp
    review = fixtures.SourceCorrectionHttpTests.review
    import_draw = fixtures.SourceCorrectionHttpTests.import_draw
    start_server = fixtures.SourceCorrectionHttpTests.start_server

    def test_scoped_and_global_equal_ids_retry_the_correct_queue(self):
        sid = self.import_draw(self.draw)
        self.assertEqual(self.paper.process_one('exp', sid)['status'], 'complete')
        calendar = {**self.calendar, 'version': 'calendar-http',
                    'cutoff_at': (self.deadline+timedelta(minutes=30)).isoformat()}
        cal_id = self.review('calendar', calendar)
        rule = deepcopy(self.frozen['rule_review']['extracted']); rule['version'] = 'scoped-http'
        rule = swap_first_second_prize_codes(rule)
        rule_id = self.review('rule', rule)
        global_request = {'old_review_id': self.cal_id, 'new_review_id': cal_id, 'issue': None,
                          'note': 'fixture', 'confirmed': True}
        scoped_request = {'old_review_id': self.rule_id, 'new_review_id': rule_id, 'issue': '26017',
                          'note': 'fixture', 'confirmed': True}
        global_body = {**global_request, 'request_key': '00000000-0000-4000-8000-000000000011',
                       'request_sha256': source_write_digest(global_request)}
        scoped_body = {**scoped_request, 'request_key': '00000000-0000-4000-8000-000000000012',
                       'request_sha256': source_write_digest(scoped_request)}
        request, read, write = self.start_server()
        with patch('dlt.app.PaperResults', return_value=self.paper):
            self.assertEqual(request('POST', '/api/source-corrections', json.dumps(scoped_body),
                                     {**write, 'Origin': 'https://evil.invalid'})[0], 403)
            status, _, global_result = request('POST', '/api/source-corrections', json.dumps(global_body), write)
            self.assertEqual(status, 200, global_result)
            status, _, issue_result = request('POST', '/api/source-corrections', json.dumps(scoped_body), write)
            self.assertEqual(status, 200, issue_result)
            self.assertEqual(global_result['correction']['correction_id'], issue_result['correction']['correction_id'])
            self.assertEqual(issue_result['correction']['scope'], 'issue')
            self.assertEqual(issue_result['followup']['details']['exp']['status'], 'complete')
            ident = issue_result['correction']['correction_id']
            retry_request = {'correction_id': ident, 'scope': 'issue'}
            retry_sha256, _ = self.paper._write_request('source_correction_retry', retry_request)
            retry_body = {**retry_request, 'request_key': '00000000-0000-4000-8000-000000000013',
                          'request_sha256': retry_sha256}
            status, _, retry = request('POST', '/api/source-corrections/retry', json.dumps(retry_body), write)
            self.assertEqual(status, 200, retry)
            _, _, records = request('GET', '/api/source-corrections', headers=read)
            by_scope = {item['scope']: item for item in records['corrections']}
            self.assertEqual(by_scope['issue']['issue'], '26017')
            self.assertEqual(by_scope['issue']['attempts'], 2)
            self.assertEqual(by_scope['global']['attempts'], 1)
            self.assertEqual(self.paper.inspect_current('exp')['result']['rule_review_id'], rule_id)


if __name__ == '__main__':
    unittest.main()
