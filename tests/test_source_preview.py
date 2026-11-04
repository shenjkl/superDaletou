from contextlib import closing
from io import BytesIO
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import threading
import time
import unittest
from unittest.mock import patch

from dlt.app import Handler
from dlt.source_preview import PAGE_CHARS, SourcePreviewError, _pdf_page, preview_source
import test_prospective as fixtures


def synthetic_pdf(pages=2):
    """Generate a synthetic multi-page PDF without external fixture files."""
    font_id = 3 + 2*pages
    kids = ' '.join(f'{3+2*i} 0 R' for i in range(pages))
    objects = [b'<< /Type /Catalog /Pages 2 0 R >>',
               f'<< /Type /Pages /Kids [{kids}] /Count {pages} >>'.encode()]
    for i in range(pages):
        content = f'BT /F1 16 Tf 20 150 Td (Synthetic page {i+1}) Tj ET'.encode()
        objects.append(f'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {4+2*i} 0 R >>'.encode())
        objects.append(b'<< /Length %d >>\nstream\n'%len(content)+content+b'\nendstream')
    objects.append(b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>')
    output = bytearray(b'%PDF-1.4\n'); offsets = [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(output)); output.extend(f'{index} 0 obj\n'.encode()+obj+b'\nendobj\n')
    start = len(output)
    output.extend(f'xref\n0 {len(offsets)}\n'.encode()+b'0000000000 65535 f \n')
    for offset in offsets[1:]: output.extend(f'{offset:010} 00000 n \n'.encode())
    output.extend(f'trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{start}\n%%EOF\n'.encode())
    return bytes(output)


class SourcePreviewTests(unittest.TestCase):
    review = fixtures.ProspectiveTests.review

    def setUp(self):
        # Keep synthetic databases in the writable workspace; the bundled
        # runtime cannot create SQLite files in the restricted system temp.
        workspace = Path(__file__).resolve().parents[1]
        with patch('tempfile.tempdir', str(workspace)):
            fixtures.ProspectiveTests.setUp(self)
        self.assertTrue(self.path.resolve().is_relative_to(workspace))

    def record(self, document):
        return self.repo.record_source_review(kind='calendar', source_url='https://www.sporttery.cn/synthetic-test',
            document=document, extracted={}, reviewer_note='synthetic', confirmed=True)

    def test_read_only_pagination_preserves_unicode_and_exact_digest(self):
        text = '中'*(PAGE_CHARS-1)+'😀'+'第2页\n<script>alert(1)</script>'
        raw = text.encode('utf-8'); ident = self.record(raw)
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        first = preview_source(self.path, ident)
        second = preview_source(self.path, ident, first['next_offset'])
        self.assertEqual(first['text'] + second['text'], text)
        self.assertEqual((len(first['text']), first['next_offset']), (PAGE_CHARS, PAGE_CHARS))
        self.assertEqual((second['previous_offset'], second['next_offset']), (0, None))
        self.assertEqual(first['document_sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)

    def test_opaque_pdf_binary_invalid_utf8_and_controls_are_not_returned(self):
        for document in (b'%PDF-1.7\nsecret', b'PK\x03\x04secret', b'\xff\x00secret', b'ok\x00secret'):
            ident = self.record(document)
            with self.subTest(prefix=document[:4]), self.assertRaises(SourcePreviewError) as error:
                preview_source(self.path, ident)
            self.assertIn(error.exception.status, (415, 503))
            self.assertNotIn('secret', str(error.exception))

    def test_invalid_offset_missing_record_and_modified_document_are_rejected(self):
        ident = self.record(b'synthetic')
        for offset in (-1, True, 100, '0'):
            with self.subTest(offset=offset), self.assertRaises(SourcePreviewError):
                preview_source(self.path, ident, offset)
        with self.assertRaises(SourcePreviewError) as error:
            preview_source(self.path, 'f'*64)
        self.assertEqual(error.exception.status, 404)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER reviewed_sources_no_update')
            db.execute('UPDATE reviewed_sources SET document=? WHERE id=?', (b'modified', ident))
        with self.assertRaises(SourcePreviewError) as error:
            preview_source(self.path, ident)
        self.assertEqual(error.exception.status, 409)

    def test_preview_route_requires_same_origin_session_and_csrf_without_socket(self):
        ident = self.record(b'PRIVATE_TOKEN=local-only')
        def call(*, csrf='secret', origin='http://127.0.0.1:8766', session=True):
            body = json.dumps({'review_id': ident, 'offset': 0}).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.path,
                                             source_preview_lock=threading.BoundedSemaphore(1))
            handler.path = '/api/source-review-preview'
            handler.headers = {'Origin': origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': csrf,
                               'Content-Type': 'application/json', 'Content-Length': str(len(body))}
            handler.rfile = BytesIO(body)
            handler._validate_host = lambda: True
            handler._session = lambda: session
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        for kwargs in ({'csrf': 'wrong'}, {'origin': 'https://other.invalid'}, {'session': False}):
            status, response = call(**kwargs)
            self.assertEqual(status, 403)
            self.assertNotIn('local-only', json.dumps(response))
        status, result = call()
        self.assertEqual(status, 200)
        self.assertEqual(result['preview']['text'], 'PRIVATE_TOKEN=local-only')

    def test_preview_response_is_json_no_store_and_not_a_file_download(self):
        ident = self.record(b'<script>synthetic</script>')
        body = json.dumps({'review_id': ident}).encode()
        handler = object.__new__(Handler)
        handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.path,
                                         source_preview_lock=threading.BoundedSemaphore(1))
        handler.path = '/api/source-review-preview'
        handler.headers = {'Origin': handler.server.origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': 'secret',
                           'Content-Type': 'application/json', 'Content-Length': str(len(body))}
        handler.rfile = BytesIO(body); handler.wfile = BytesIO()
        handler._validate_host = lambda: True; handler._session = lambda: True
        headers = {}
        handler.send_response = lambda status: headers.update(status=status)
        handler.send_header = lambda name, value: headers.update({name.lower(): value})
        handler.end_headers = lambda: None
        handler.do_POST()
        self.assertEqual(headers['status'], 200)
        self.assertIn('application/json', headers['content-type'])
        self.assertEqual(headers['cache-control'], 'no-store')
        self.assertNotIn('content-disposition', headers)
        self.assertEqual(json.loads(handler.wfile.getvalue())['preview']['text'], '<script>synthetic</script>')

    def test_preview_busy_limit_refuses_second_request_and_releases_afterwards(self):
        ident = self.record(b'synthetic')
        lock = threading.BoundedSemaphore(1)
        def call():
            body = json.dumps({'review_id': ident}).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.path,
                                             source_preview_lock=lock)
            handler.path = '/api/source-review-preview'
            handler.headers = {'Origin': handler.server.origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': 'secret',
                               'Content-Type': 'application/json', 'Content-Length': str(len(body))}
            handler.rfile = BytesIO(body); handler._validate_host = lambda: True; handler._session = lambda: True
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        self.assertTrue(lock.acquire(blocking=False))
        self.assertEqual(call()[0], 409)
        lock.release()
        self.assertEqual(call()[0], 200)
        self.assertTrue(lock.acquire(blocking=False))
        lock.release()

    @unittest.skipUnless(importlib.util.find_spec('pypdfium2') and importlib.util.find_spec('PIL'),
                         'project PDF preview dependencies are unavailable')
    def test_pdf_pages_are_rasterized_without_returning_original_bytes(self):
        import base64
        from PIL import Image
        original = synthetic_pdf(); ident = self.record(original)
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        first = preview_source(self.path, ident)
        second = preview_source(self.path, ident, first['next_offset'])
        self.assertEqual((first['kind'], first['offset'], first['total_pages']), ('pdf_page', 0, 2))
        self.assertEqual((second['offset'], second['previous_offset'], second['next_offset']), (1, 0, None))
        self.assertEqual(first['document_sha256'], hashlib.sha256(original).hexdigest())
        self.assertNotIn('document_base64', first)
        self.assertNotIn('Synthetic page', json.dumps(first))
        png1 = base64.b64decode(first['image_base64'], validate=True)
        png2 = base64.b64decode(second['image_base64'], validate=True)
        self.assertTrue(png1.startswith(b'\x89PNG\r\n\x1a\n'))
        self.assertNotEqual(png1, png2)
        with Image.open(BytesIO(png1)) as image:
            self.assertEqual(image.size, (first['width'], first['height']))
            self.assertLess(image.convert('L').getextrema()[0], 100)
            self.assertGreater(image.convert('L').getpixel((0, 0)), 245)
            self.assertEqual(image.info, {})
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        with self.assertRaises(SourcePreviewError):
            preview_source(self.path, ident, 2)
        oversized = self.record(synthetic_pdf(201))
        with self.assertRaises(SourcePreviewError) as error:
            preview_source(self.path, oversized)
        self.assertEqual(error.exception.status, 415)

    @unittest.skipUnless(importlib.util.find_spec('pypdfium2') and importlib.util.find_spec('PIL'),
                         'project PDF preview dependencies are unavailable')
    def test_pdf_preview_route_returns_raster_not_original_file(self):
        original = synthetic_pdf(); ident = self.record(original)
        body = json.dumps({'review_id': ident, 'offset': 1}).encode()
        handler = object.__new__(Handler)
        handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.path,
                                         source_preview_lock=threading.BoundedSemaphore(1))
        handler.path = '/api/source-review-preview'
        handler.headers = {'Origin': handler.server.origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': 'secret',
                           'Content-Type': 'application/json', 'Content-Length': str(len(body))}
        handler.rfile = BytesIO(body); handler.wfile = BytesIO()
        handler._validate_host = lambda: True; handler._session = lambda: True
        headers = {}
        handler.send_response = lambda status: headers.update(status=status)
        handler.send_header = lambda name, value: headers.update({name.lower(): value})
        handler.end_headers = lambda: None
        handler.do_POST()
        data = handler.wfile.getvalue()
        self.assertEqual(headers['status'], 200)
        self.assertEqual(headers['cache-control'], 'no-store')
        self.assertNotIn('content-disposition', headers)
        self.assertNotIn(original, data)
        self.assertEqual(json.loads(data)['preview']['kind'], 'pdf_page')

    @unittest.skipUnless(importlib.util.find_spec('pypdfium2') and importlib.util.find_spec('PIL')
                         and importlib.util.find_spec('reportlab'), 'PDF fixture or renderer unavailable')
    def test_password_protected_pdf_is_rejected_without_leaking_data(self):
        from reportlab.lib.pdfencrypt import StandardEncryption
        from reportlab.pdfgen import canvas
        output = BytesIO()
        document = canvas.Canvas(output, encrypt=StandardEncryption('fixture-password'))
        document.drawString(20, 700, 'synthetic private content')
        document.save()
        ident = self.record(output.getvalue())
        with self.assertRaises(SourcePreviewError) as error:
            preview_source(self.path, ident)
        self.assertEqual(error.exception.status, 415)
        self.assertNotIn('fixture-password', str(error.exception))

    def test_pdf_timeout_missing_dependencies_and_invalid_output_fail_closed(self):
        import subprocess
        with patch('dlt.source_preview.subprocess.run', side_effect=subprocess.TimeoutExpired('worker', 10)):
            with self.assertRaises(SourcePreviewError) as error:
                _pdf_page(synthetic_pdf(), 0)
            self.assertEqual(error.exception.status, 408)
        for output, expected in ((b'{"error":"dependency_missing"}', 503),
                                 (b'{"error":"resource_limit"}', 503),
                                 (b'{"error":"invalid_or_unsupported"}', 415)):
            with patch('dlt.source_preview.subprocess.run', return_value=SimpleNamespace(returncode=1, stdout=output)):
                with self.assertRaises(SourcePreviewError) as error:
                    _pdf_page(synthetic_pdf(), 0)
                self.assertEqual(error.exception.status, expected)
        corrupt = json.dumps({'page': 0, 'pages': 1, 'width': 3, 'height': 3,
                              'png_base64': 'bm90IGFuIGltYWdl'}).encode()
        with patch('dlt.source_preview.subprocess.run', return_value=SimpleNamespace(returncode=0, stdout=corrupt)):
            with self.assertRaises(SourcePreviewError) as error:
                _pdf_page(synthetic_pdf(), 0)
            self.assertEqual(error.exception.status, 422)
        with patch('dlt.source_preview.subprocess.run', return_value=SimpleNamespace(returncode=1, stdout=b'SECRET_PATH')):
            with self.assertRaises(SourcePreviewError) as error:
                _pdf_page(synthetic_pdf(), 0)
            self.assertNotIn('SECRET_PATH', str(error.exception))

    def test_pdf_timeout_terminates_a_real_stalled_subprocess(self):
        import subprocess
        import sys
        run = subprocess.run
        def stall(_command, **kwargs):
            return run([sys.executable, '-c', 'import time; time.sleep(30)'], **kwargs)
        start = time.monotonic()
        with patch('dlt.source_preview.PDF_TIMEOUT_SECONDS', 0.5), patch('dlt.source_preview.subprocess.run', side_effect=stall):
            with self.assertRaises(SourcePreviewError) as error:
                _pdf_page(synthetic_pdf(), 0)
        self.assertEqual(error.exception.status, 408)
        self.assertLess(time.monotonic()-start, 6)


if __name__ == '__main__':
    unittest.main()
