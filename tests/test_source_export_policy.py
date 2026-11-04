"""Read-only, no-socket checks for unsafe original-document export."""
from contextlib import closing
from io import BytesIO
import json
import sqlite3
from types import SimpleNamespace
import unittest

from dlt.app import Handler
import test_prospective as fixtures


class SourceExportPolicyTests(unittest.TestCase):
    setUp = fixtures.ProspectiveTests.setUp
    review = fixtures.ProspectiveTests.review

    def request(self, ident, *, session=True):
        handler = object.__new__(Handler)
        handler.path = '/api/source-review-document?id=' + ident
        handler.server = SimpleNamespace(db_path=self.path, experiments=self.repo)
        handler._validate_host = lambda: True
        handler._session = lambda: session
        handler._json = lambda status, payload, **kwargs: (status, payload)
        handler._headers = lambda *args, **kwargs: self.fail('raw download headers must not be emitted')
        handler.wfile = BytesIO()
        return handler.do_GET(), handler.wfile.getvalue()

    def test_original_is_retained_but_not_exported_with_credentials_or_paths(self):
        sensitive = b'PRIVATE_TOKEN=fixture_secret C:\\Users\\private\\file.txt'
        ident = self.repo.record_source_review(kind='calendar', source_url='https://www.sporttery.cn/synthetic-test',
            document=sensitive, extracted={}, reviewer_note='synthetic', confirmed=True, document_name='fixture.txt')
        (status, result), raw = self.request(ident)
        self.assertEqual(status, 403)
        self.assertEqual(raw, b'')
        response = json.dumps(result).encode()
        self.assertNotIn(b'fixture_secret', response)
        self.assertNotIn(b'Users', response)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT document FROM reviewed_sources WHERE id=?', (ident,)).fetchone()[0], sensitive)
        self.assertEqual(self.request(ident, session=False)[0][0], 401)

    def test_invalid_missing_and_corrupted_records_remain_distinct(self):
        self.assertEqual(self.request('x')[0][0], 400)
        self.assertEqual(self.request('f'*64)[0][0], 404)
        ident = self.repo.record_source_review(kind='calendar', source_url='https://www.sporttery.cn/synthetic-test',
            document=b'original', extracted={}, reviewer_note='synthetic', confirmed=True)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER reviewed_sources_no_update')
            db.execute('UPDATE reviewed_sources SET document=? WHERE id=?', (b'damaged', ident))
        self.assertEqual(self.request(ident)[0][0], 409)


if __name__ == '__main__':
    unittest.main()
