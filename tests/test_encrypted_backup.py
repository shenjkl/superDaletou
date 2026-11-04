"""Real encrypted snapshot and isolated candidate tests; no network."""
from contextlib import closing
import hashlib
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dlt.encrypted_backup import (BackupError, HEADER, _crypto, _key,
                                  create_encrypted_snapshot, prepare_encrypted_snapshot)
from dlt.integrity import check_database
from dlt.app import Handler
from dlt.recovery_guard import RecoveryQuarantined, require_path_execution_allowed
from dlt.recovery import protect_private_directory
from dlt.scheduler import Scheduler
from dlt.snapshots import SnapshotStore
from test_scheduler import configuration
import test_prospective_settlement as fixtures


PASSWORD = 'synthetic-long-test-password'
SECRET = 'SYNTHETIC-PRIVATE-SOURCE-EXAMPLE'


@unittest.skipUnless(hasattr(sqlite3.Connection, 'serialize') and hasattr(sqlite3.Connection, 'deserialize'),
                     'requires Python 3.12 SQLite serialization')
class EncryptedBackupTests(unittest.TestCase):
    review = fixtures.PaperResultsTests.review
    import_draw = fixtures.PaperResultsTests.import_draw

    @classmethod
    def setUpClass(cls):
        workspace = Path(__file__).resolve().parents[1]
        cls.root_directory = tempfile.TemporaryDirectory(dir=workspace)
        protect_private_directory(cls.root_directory.name)
        cls.previous_tempdir = tempfile.tempdir
        tempfile.tempdir = cls.root_directory.name

    @classmethod
    def tearDownClass(cls):
        tempfile.tempdir = cls.previous_tempdir
        cls.root_directory.cleanup()

    def setUp(self):
        fixtures.PaperResultsTests.setUp(self)
        protect_private_directory(self.path.parent)
        workspace = Path(__file__).resolve().parents[1]
        self.directory = tempfile.TemporaryDirectory(dir=workspace)
        self.addCleanup(self.directory.cleanup)
        protect_private_directory(self.directory.name)
        root = Path(self.directory.name).resolve()
        self.assertTrue(root.is_relative_to(workspace))
        self.encrypted, self.candidate = root / 'snapshot.dltbak', root / 'candidate.sqlite3'
        payload = {'success': True, 'errorCode': '0', 'sensitive_note': SECRET, 'value': {'list': [self.draw]}}
        self.extra_snapshot = SnapshotStore(self.path).import_official(json.dumps(payload).encode(), 'fixture',
                                                    self.now.isoformat())['snapshot_id']
        self.scheduler = Scheduler(self.path)
        self.plan = self.scheduler.create_plan(configuration())
        self.scheduler.set_enabled(self.plan, True)

    def create(self):
        return create_encrypted_snapshot(self.path, self.encrypted, PASSWORD)

    def prepare(self, password=PASSWORD, **kwargs):
        return prepare_encrypted_snapshot(self.encrypted, self.candidate, password,
                                          acknowledge_plaintext=kwargs.get('acknowledge_plaintext', True))

    def test_roundtrip_contains_original_data_and_blocks_execution(self):
        with closing(sqlite3.connect(self.path)) as original:
            source_document = original.execute('SELECT document FROM reviewed_sources LIMIT 1').fetchone()[0]
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        self.assertEqual(self.create()['status'], 'encrypted_snapshot_created')
        archive = self.encrypted.read_bytes()
        self.assertNotIn(SECRET.encode(), archive)
        self.assertNotIn(b'SQLite format 3\x00', archive)
        result = self.prepare()
        self.assertEqual(result['status'], 'prepared')
        self.assertEqual(check_database(self.candidate)['status'], 'passed')
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        with closing(sqlite3.connect(self.candidate)) as db:
            self.assertEqual(db.execute('SELECT enabled FROM update_plans WHERE id=?', (self.plan,)).fetchone(), (1,))
            self.assertEqual(db.execute('SELECT count(*) FROM update_runs').fetchone(), (0,))
            self.assertEqual(db.execute('SELECT document FROM reviewed_sources LIMIT 1').fetchone()[0], source_document)
            raw = db.execute('SELECT raw_payload FROM observations WHERE snapshot_id=?', (self.extra_snapshot,)).fetchone()[0]
            self.assertIn(SECRET.encode(), raw)
        with self.assertRaises(RecoveryQuarantined):
            require_path_execution_allowed(self.candidate)
        with self.assertRaises(RecoveryQuarantined):
            Scheduler(self.candidate).run_due()

    def test_bad_password_corruption_and_acknowledgement_leave_no_candidate(self):
        self.create()
        with self.assertRaises(BackupError):
            self.prepare('incorrect-long-password')
        self.assertFalse(self.candidate.exists())
        with self.assertRaises(BackupError):
            self.prepare(acknowledge_plaintext=False)
        self.assertFalse(self.candidate.exists())
        original = self.encrypted.read_bytes()
        for index in (0, HEADER.size - 1, len(original) - 1):
            data = bytearray(original); data[index] ^= 1; self.encrypted.write_bytes(data)
            with self.subTest(index=index), self.assertRaises(BackupError):
                self.prepare()
            self.assertFalse(self.candidate.exists())
        self.encrypted.write_bytes(original)
        self.assertFalse(list(self.candidate.parent.glob('.dlt-recovery-*')))

    def test_existing_destinations_and_size_limits_never_overwrite(self):
        self.encrypted.write_bytes(b'previous')
        with self.assertRaises(BackupError):
            self.create()
        self.assertEqual(self.encrypted.read_bytes(), b'previous')
        self.encrypted.unlink()
        with patch('dlt.encrypted_backup.MAX_DATABASE_BYTES', 1):
            with self.assertRaises(BackupError):
                self.create()
        self.create()
        self.candidate.write_bytes(b'previous-candidate')
        with self.assertRaises(BackupError):
            self.prepare()
        self.assertEqual(self.candidate.read_bytes(), b'previous-candidate')
        self.candidate.unlink()
        Path(str(self.candidate) + '-wal').write_bytes(b'existing-wal')
        with self.assertRaises(BackupError):
            self.prepare()
        self.assertFalse(self.candidate.exists())

    def test_business_corruption_inside_validly_encrypted_snapshot_blocks_publish(self):
        self.create()
        encrypted = self.encrypted.read_bytes()
        header = encrypted[:HEADER.size]
        magic, version, salt, nonce, _size = HEADER.unpack(header)
        AESGCM, _, _ = _crypto()
        key = _key(PASSWORD, salt)
        plaintext = AESGCM(key).decrypt(nonce, encrypted[HEADER.size:], header)
        with closing(sqlite3.connect(':memory:')) as db:
            db.deserialize(plaintext)
            db.execute('DROP TRIGGER draws_no_update')
            db.execute("UPDATE draw_versions SET front_json='[1,2,3,4,35]' WHERE snapshot_id=?", (self.extra_snapshot,))
            db.execute("CREATE TRIGGER draws_no_update BEFORE UPDATE ON draw_versions BEGIN SELECT RAISE(ABORT, 'immutable draw'); END")
            db.commit()
            changed = db.serialize()
        header = HEADER.pack(magic, version, salt, nonce, len(changed))
        self.encrypted.write_bytes(header + AESGCM(key).encrypt(nonce, changed, header))
        with self.assertRaises(BackupError) as captured:
            self.prepare()
        self.assertEqual(captured.exception.check['status'], 'failed')
        self.assertFalse(self.candidate.exists())
        self.assertFalse(list(self.candidate.parent.glob('.dlt-recovery-*')))

    def test_uncheckpointed_wal_is_included(self):
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('UPDATE update_plans SET enabled=0 WHERE id=?', (self.plan,))
            writer.commit()
            self.assertGreater(Path(str(self.path) + '-wal').stat().st_size, 0)
            self.create()
            self.prepare()
            with closing(sqlite3.connect(self.candidate)) as restored:
                self.assertEqual(restored.execute('SELECT enabled FROM update_plans WHERE id=?', (self.plan,)).fetchone(), (0,))

    def test_quarantined_database_cannot_be_backed_up_as_a_live_source(self):
        self.create(); self.prepare()
        with self.assertRaises(BackupError):
            create_encrypted_snapshot(self.candidate, self.candidate.with_suffix('.bak'), PASSWORD)

    def test_publication_failure_removes_staging_and_preserves_source(self):
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        with patch('dlt.encrypted_backup._publish_staged', side_effect=PermissionError('denied')):
            with self.assertRaises(BackupError):
                self.create()
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        self.assertFalse(self.encrypted.exists())
        self.assertFalse(list(self.encrypted.parent.glob('.dlt-encrypted-*')))
        self.create()
        with patch('dlt.encrypted_backup._publish_staged', side_effect=PermissionError('denied')):
            with self.assertRaises(BackupError):
                self.prepare()
        self.assertFalse(self.candidate.exists())
        self.assertFalse(list(self.candidate.parent.glob('.dlt-recovery-*')))

    def test_download_route_protects_session_and_returns_decryptable_ciphertext(self):
        lock = threading.BoundedSemaphore(1)
        def call(body, *, origin='http://127.0.0.1:8766', csrf='secret', session=True):
            encoded = json.dumps(body).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.path,
                                             backup_lock=lock)
            handler.path = '/api/encrypted-backup'
            handler.headers = {'Origin': origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': csrf,
                               'Content-Type': 'application/json', 'Content-Length': str(len(encoded))}
            handler.rfile = BytesIO(encoded); handler.wfile = BytesIO()
            handler._validate_host = lambda: True
            handler._session = lambda: session
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            sent = []
            handler._headers = lambda status, kind, size, **kwargs: sent.append((status, kind, size, kwargs))
            result = handler.do_POST()
            return result, sent, handler.wfile.getvalue()
        body = {'password': PASSWORD, 'acknowledge': True}
        for override in ({'origin': 'https://example.invalid'}, {'csrf': 'bad'}, {'session': False}):
            result, headers, data = call(body, **override)
            self.assertEqual(result[0], 403)
            self.assertFalse(headers or data)
        result, _, _ = call({'password': PASSWORD, 'acknowledge': False})
        self.assertEqual(result[0], 400)
        result, _, _ = call({'password': 'short', 'acknowledge': True})
        self.assertEqual(result[0], 400)
        self.assertNotIn('short', str(result[1]))
        with lock:
            result, _, _ = call(body)
            self.assertEqual(result[0], 409)
        result, headers, data = call(body)
        self.assertIsNone(result)
        self.assertEqual(headers[0][:3], (200, 'application/octet-stream', len(data)))
        self.assertEqual(headers[0][3]['download_name'], 'research-snapshot.dltbak')
        self.assertNotIn(SECRET.encode(), data)
        self.encrypted.write_bytes(data)
        self.assertEqual(self.prepare()['status'], 'prepared')


if __name__ == '__main__':
    unittest.main()
