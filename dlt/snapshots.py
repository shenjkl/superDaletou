"""Immutable, provenance-preserving imports of official draw snapshots.

The importer qualifies number data separately from prize data. It never asserts
that a row is settlement-ready: that requires a verified, applicable rule.
"""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import closing
import hashlib
import json
import re
import sqlite3
from pathlib import Path


class InvalidSnapshot(ValueError):
    """Invalid source payload; import is rejected without partial writes."""


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise InvalidSnapshot(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def _prizes(row):
    levels = row.get('prizeLevelList')
    if not isinstance(levels, list) or not levels:
        return False, 'PRIZES_MISSING'
    seen = set()
    for level in levels:
        if not isinstance(level, dict):
            return False, 'INVALID_PRIZE_FIELDS'
        key = str(level.get('sort', ''))
        value = str(level.get('stakeAmount', ''))
        if (not re.fullmatch(r'\d+', key) or key in seen
                or not re.fullmatch(r'(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d{1,2})?', value)):
            return False, 'INVALID_PRIZE_FIELDS'
        seen.add(key)
    # Completeness against a specific rule, including additional prizes, is
    # checked later, not inferred from a non-empty prize list.
    return True, 'RULE_NOT_VERIFIED'


def parse_snapshot(raw: bytes):
    try:
        payload = json.loads(raw.decode('utf-8-sig'), object_pairs_hook=_pairs)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise InvalidSnapshot('invalid UTF-8 JSON') from exc
    if not isinstance(payload, dict) or payload.get('success') is not True or str(payload.get('errorCode')) != '0':
        raise InvalidSnapshot('official source returned an unsuccessful response')
    value = payload.get('value')
    rows = value.get('list') if isinstance(value, dict) else None
    if not isinstance(rows, list) or not rows:
        raise InvalidSnapshot('draw list is empty or invalid')
    parsed, issues = [], set()
    for row in rows:
        if not isinstance(row, dict):
            raise InvalidSnapshot('draw row is not an object')
        issue = str(row.get('lotteryDrawNum', ''))
        if not re.fullmatch(r'\d{5}', issue) or str(row.get('lotteryGameNum')) != '85':
            raise InvalidSnapshot('invalid game or issue')
        if issue in issues:
            raise InvalidSnapshot(f'duplicate issue: {issue}')
        issues.add(issue)
        try:
            date = datetime.fromisoformat(str(row.get('lotteryDrawTime', ''))).date().isoformat()
            draw_result = row.get('lotteryDrawResult')
            number_tokens = draw_result.split() if isinstance(draw_result, str) else []
            if len(number_tokens) != 7 or any(not re.fullmatch(r'[0-9]{1,2}', token)
                                              for token in number_tokens):
                raise ValueError('draw numbers must be seven ASCII decimal tokens')
            numbers = [int(token) for token in number_tokens]
        except (ValueError, TypeError, AttributeError) as exc:
            raise InvalidSnapshot(f'invalid date or numbers: {issue}') from exc
        front, back = numbers[:5], numbers[5:]
        if (len(numbers) != 7 or len(set(front)) != 5 or len(set(back)) != 2
                or not all(1 <= n <= 35 for n in front) or not all(1 <= n <= 12 for n in back)):
            raise InvalidSnapshot(f'invalid number combination: {issue}')
        prize_fields_present, reason = _prizes(row)
        parsed.append({'issue': issue, 'draw_date': date, 'front': sorted(front), 'back': sorted(back),
                       'numbers_usable': True, 'prize_fields_present': prize_fields_present,
                       'settlement_ready': False, 'settlement_reason': reason, 'raw_row': row})
    return payload, parsed


class SnapshotStore:
    """Explicit-path SQLite store. No module import or read creates files."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS snapshots (
                    id TEXT PRIMARY KEY, raw_sha256 TEXT NOT NULL,
                    canonical_json BLOB NOT NULL, row_count INTEGER NOT NULL CHECK(row_count > 0)
                );
                CREATE TABLE IF NOT EXISTS observations (
                    snapshot_id TEXT NOT NULL REFERENCES snapshots(id),
                    source TEXT NOT NULL, observed_at TEXT NOT NULL, raw_sha256 TEXT NOT NULL, raw_payload BLOB NOT NULL,
                    PRIMARY KEY(snapshot_id, source, observed_at)
                );
                CREATE TABLE IF NOT EXISTS source_quarantine (
                    id TEXT PRIMARY KEY, source TEXT NOT NULL, observed_at TEXT NOT NULL,
                    raw_sha256 TEXT NOT NULL, reason TEXT NOT NULL, raw_payload BLOB NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS draw_versions (
                    snapshot_id TEXT NOT NULL REFERENCES snapshots(id), issue TEXT NOT NULL,
                    draw_date TEXT NOT NULL, front_json TEXT NOT NULL, back_json TEXT NOT NULL,
                    numbers_usable INTEGER NOT NULL, prize_fields_present INTEGER NOT NULL,
                    settlement_ready INTEGER NOT NULL, settlement_reason TEXT NOT NULL, row_json BLOB NOT NULL,
                    PRIMARY KEY(snapshot_id, issue)
                );
                CREATE TABLE IF NOT EXISTS bundled_snapshot_imports (
                    request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL,
                    request_json BLOB NOT NULL, result_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bundled_snapshot_import_fences (
                    request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL,
                    request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bundled_snapshot_import_retries (
                    request_key TEXT PRIMARY KEY,
                    retry_of TEXT NOT NULL UNIQUE REFERENCES bundled_snapshot_import_fences(request_key),
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS snapshots_no_update BEFORE UPDATE ON snapshots BEGIN SELECT RAISE(ABORT, 'immutable snapshot'); END;
                CREATE TRIGGER IF NOT EXISTS snapshots_no_delete BEFORE DELETE ON snapshots BEGIN SELECT RAISE(ABORT, 'immutable snapshot'); END;
                CREATE TRIGGER IF NOT EXISTS draws_no_update BEFORE UPDATE ON draw_versions BEGIN SELECT RAISE(ABORT, 'immutable draw'); END;
                CREATE TRIGGER IF NOT EXISTS draws_no_delete BEFORE DELETE ON draw_versions BEGIN SELECT RAISE(ABORT, 'immutable draw'); END;
                CREATE TRIGGER IF NOT EXISTS observations_no_update BEFORE UPDATE ON observations BEGIN SELECT RAISE(ABORT, 'immutable observation'); END;
                CREATE TRIGGER IF NOT EXISTS observations_no_delete BEFORE DELETE ON observations BEGIN SELECT RAISE(ABORT, 'immutable observation'); END;
                CREATE TRIGGER IF NOT EXISTS source_quarantine_no_update BEFORE UPDATE ON source_quarantine BEGIN SELECT RAISE(ABORT, 'immutable source quarantine'); END;
                CREATE TRIGGER IF NOT EXISTS source_quarantine_no_delete BEFORE DELETE ON source_quarantine BEGIN SELECT RAISE(ABORT, 'immutable source quarantine'); END;
                CREATE TRIGGER IF NOT EXISTS bundled_snapshot_imports_no_update BEFORE UPDATE ON bundled_snapshot_imports BEGIN SELECT RAISE(ABORT, 'immutable bundled snapshot import'); END;
                CREATE TRIGGER IF NOT EXISTS bundled_snapshot_imports_no_delete BEFORE DELETE ON bundled_snapshot_imports BEGIN SELECT RAISE(ABORT, 'immutable bundled snapshot import'); END;
                CREATE TRIGGER IF NOT EXISTS bundled_snapshot_import_fences_no_update BEFORE UPDATE ON bundled_snapshot_import_fences BEGIN SELECT RAISE(ABORT, 'immutable bundled snapshot import fence'); END;
                CREATE TRIGGER IF NOT EXISTS bundled_snapshot_import_fences_no_delete BEFORE DELETE ON bundled_snapshot_import_fences BEGIN SELECT RAISE(ABORT, 'immutable bundled snapshot import fence'); END;
                CREATE TRIGGER IF NOT EXISTS bundled_snapshot_import_retries_no_update BEFORE UPDATE ON bundled_snapshot_import_retries BEGIN SELECT RAISE(ABORT, 'immutable bundled snapshot import retry'); END;
                CREATE TRIGGER IF NOT EXISTS bundled_snapshot_import_retries_no_delete BEFORE DELETE ON bundled_snapshot_import_retries BEGIN SELECT RAISE(ABORT, 'immutable bundled snapshot import retry'); END;
                CREATE TRIGGER IF NOT EXISTS bundled_snapshot_imports_no_fenced_insert BEFORE INSERT ON bundled_snapshot_imports
                    WHEN EXISTS (SELECT 1 FROM bundled_snapshot_import_fences WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'fenced bundled snapshot import'); END;
                CREATE TRIGGER IF NOT EXISTS bundled_snapshot_import_fences_no_accepted_insert BEFORE INSERT ON bundled_snapshot_import_fences
                    WHEN EXISTS (SELECT 1 FROM bundled_snapshot_imports WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'accepted bundled snapshot import'); END;
            ''')
            columns = {row[1] for row in db.execute('PRAGMA table_info(observations)')}
            if 'raw_payload' not in columns:
                # Earlier stores retained only the raw hash. Do not manufacture
                # missing original bytes during schema migration.
                db.execute('ALTER TABLE observations ADD COLUMN raw_payload BLOB')

    def record_quarantine(self, raw: bytes, source: str, observed_at: str, reason: str, *, transaction_guard=None):
        """Retain a bounded rejected source response without creating a usable snapshot."""
        if not isinstance(raw, bytes) or len(raw) > 5 * 1024 * 1024:
            raise InvalidSnapshot('quarantined source bytes are outside the supported size')
        if (not isinstance(source, str) or len(source) > 2048
                or not source.startswith('https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry?')):
            raise InvalidSnapshot('quarantined source address is invalid')
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise InvalidSnapshot('quarantined source reason is invalid')
        try:
            observed = datetime.fromisoformat(observed_at)
            if observed.tzinfo is None or observed.utcoffset() is None:
                raise ValueError('timezone required')
        except (ValueError, TypeError) as exc:
            raise InvalidSnapshot('quarantined source time must include timezone') from exc
        observed_iso = observed.astimezone(timezone.utc).isoformat()
        raw_hash = hashlib.sha256(raw).hexdigest()
        ident = hashlib.sha256(_canonical({'source': source, 'observed_at': observed_iso,
                                           'raw_sha256': raw_hash})).hexdigest()
        recorded_at = datetime.now(timezone.utc).isoformat()
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if transaction_guard is not None:
                transaction_guard(db)
            existing = db.execute('SELECT source,observed_at,raw_sha256,reason,raw_payload FROM source_quarantine WHERE id=?',
                                  (ident,)).fetchone()
            expected = (source, observed_iso, raw_hash, reason, raw)
            if existing and existing != expected:
                raise InvalidSnapshot('quarantined source identity conflicts with retained content')
            db.execute('INSERT OR IGNORE INTO source_quarantine VALUES (?,?,?,?,?,?,?)',
                       (ident, source, observed_iso, raw_hash, reason, raw, recorded_at))
        return {'id': ident, 'source': source, 'observed_at': observed_iso,
                'raw_sha256': raw_hash, 'reason': reason, 'size': len(raw)}

    def list_quarantines(self, limit=50):
        if type(limit) is not int or not 1 <= limit <= 200:
            raise InvalidSnapshot('quarantine list limit is invalid')
        if not self.path.is_file():
            return []
        uri = self.path.resolve().as_uri() + '?mode=ro'
        with closing(sqlite3.connect(uri, uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_quarantine'").fetchone()
            if not exists:
                return []
            rows = db.execute('SELECT id,source,observed_at,raw_sha256,reason,length(raw_payload),recorded_at '
                              'FROM source_quarantine ORDER BY recorded_at DESC,id DESC LIMIT ?', (limit,)).fetchall()
        return [{'id': row[0], 'source': row[1], 'observed_at': row[2], 'raw_sha256': row[3],
                 'reason': row[4], 'size': row[5], 'recorded_at': row[6]} for row in rows]

    def quarantine_document(self, ident):
        if not isinstance(ident, str) or not re.fullmatch(r'[0-9a-f]{64}', ident):
            raise InvalidSnapshot('quarantined source identifier is invalid')
        if not self.path.is_file():
            return None
        uri = self.path.resolve().as_uri() + '?mode=ro'
        with closing(sqlite3.connect(uri, uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_quarantine'").fetchone()
            row = (db.execute('SELECT source,observed_at,raw_sha256,raw_payload FROM source_quarantine WHERE id=?',
                              (ident,)).fetchone() if exists else None)
        if not row:
            return None
        source, observed_at, digest, raw = row
        if (not isinstance(raw, bytes) or len(raw) > 5 * 1024 * 1024
                or hashlib.sha256(raw).hexdigest() != digest):
            raise InvalidSnapshot('quarantined source bytes do not match their retained digest')
        return {'source': source, 'observed_at': observed_at, 'raw_sha256': digest, 'raw': raw}

    @staticmethod
    def _bundle_import_identity(raw, source):
        if not isinstance(raw, bytes) or not isinstance(source, str) or not source.strip():
            raise InvalidSnapshot('raw bytes and a source are required')
        payload, rows = parse_snapshot(raw)
        snapshot_id = hashlib.sha256(_canonical(payload)).hexdigest()
        raw_hash = hashlib.sha256(raw).hexdigest()
        request = {'source': source, 'snapshot_id': snapshot_id, 'raw_sha256': raw_hash}
        request_raw = _canonical(request)
        return payload, rows, snapshot_id, raw_hash, request, request_raw, hashlib.sha256(request_raw).hexdigest()

    @staticmethod
    def _import_retry(db, request_key, digest, request_raw, retry_of, now):
        prior = db.execute('SELECT retry_of,request_sha256,request_json FROM bundled_snapshot_import_retries WHERE request_key=?',
                           (request_key,)).fetchone()
        if retry_of is None:
            if prior:
                raise InvalidSnapshot('snapshot import request must retain its recorded retry association')
            return
        if (not isinstance(retry_of, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', retry_of, re.I)
                or retry_of == request_key):
            raise InvalidSnapshot('snapshot import retry key is invalid')
        original = db.execute('SELECT request_sha256,request_json FROM bundled_snapshot_import_fences WHERE request_key=?',
                              (retry_of,)).fetchone()
        if not original or original != (digest, request_raw):
            raise InvalidSnapshot('snapshot import retry must match an original fenced request')
        if (db.execute('SELECT 1 FROM bundled_snapshot_import_retries WHERE retry_of=?', (retry_of,)).fetchone()
                or db.execute('SELECT 1 FROM bundled_snapshot_import_retries WHERE request_key=?', (retry_of,)).fetchone()):
            raise InvalidSnapshot('snapshot import retry must be a direct retry of the original fence')
        wanted = (retry_of, digest, request_raw)
        if prior:
            if prior != wanted:
                raise InvalidSnapshot('snapshot import retry key is already associated with other content')
            return
        db.execute('INSERT INTO bundled_snapshot_import_retries VALUES (?,?,?,?,?)',
                   (request_key, retry_of, digest, request_raw, now))

    def import_official(self, raw: bytes, source: str, observed_at: str, *, transaction_guard=None,
                        request_key=None, retry_of=None):
        if not isinstance(raw, bytes) or not source.strip():
            raise InvalidSnapshot('raw bytes and a source are required')
        try:
            observed = datetime.fromisoformat(observed_at)
            if observed.tzinfo is None or observed.utcoffset() is None:
                raise ValueError('timezone required')
        except (ValueError, TypeError) as exc:
            raise InvalidSnapshot('observation time must include timezone') from exc
        payload, rows, snapshot_id, raw_hash, request, request_raw, request_digest = self._bundle_import_identity(raw, source)
        canonical = _canonical(payload)
        if request_key is not None:
            if not isinstance(request_key, str) or not re.fullmatch(
                    r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I):
                raise InvalidSnapshot('snapshot import request key is invalid')
            import_request = (request, request_raw, request_digest)
        else:
            import_request = None
        # Schema creation is deliberate; all content writes are one transaction.
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys = ON')
            db.execute('BEGIN IMMEDIATE')
            if import_request:
                request, request_raw, request_digest = import_request
                previous_request = db.execute('SELECT request_sha256,request_json,result_json FROM bundled_snapshot_imports '
                                              'WHERE request_key=?', (request_key,)).fetchone()
                if previous_request:
                    if previous_request[:2] != (request_digest, request_raw):
                        raise InvalidSnapshot('snapshot import request key used for different bundle content')
                    retry = db.execute('SELECT retry_of FROM bundled_snapshot_import_retries WHERE request_key=?',
                                       (request_key,)).fetchone()
                    if (retry[0] if retry else None) != retry_of:
                        raise InvalidSnapshot('snapshot import retry association differs from the accepted request')
                    return json.loads(previous_request[2])
                if db.execute('SELECT 1 FROM bundled_snapshot_import_fences WHERE request_key=?', (request_key,)).fetchone():
                    raise InvalidSnapshot('snapshot import request was fenced; late submissions are rejected')
                self._import_retry(db, request_key, request_digest, request_raw, retry_of,
                                   datetime.now(timezone.utc).isoformat())
                prior_observation = db.execute('SELECT observed_at,raw_payload FROM observations WHERE snapshot_id=? AND source=? AND raw_sha256=? '
                                               'ORDER BY observed_at LIMIT 1', (snapshot_id, source, raw_hash)).fetchone()
                if prior_observation and isinstance(prior_observation[1], bytes):
                    observed = datetime.fromisoformat(prior_observation[0])
                else:
                    observed = observed.astimezone(timezone.utc)
                observed_iso = observed.isoformat()
            # A worker may fence an expired or superseded execution before any
            # content is inserted. The guard executes under the same write lock.
            if transaction_guard is not None:
                transaction_guard(db)
            db.execute('INSERT OR IGNORE INTO snapshots VALUES (?, ?, ?, ?)',
                       (snapshot_id, raw_hash, canonical, len(rows)))
            previous = db.execute('SELECT raw_sha256, raw_payload FROM observations WHERE snapshot_id=? AND source=? AND observed_at=?',
                                  (snapshot_id, source, observed.isoformat())).fetchone()
            if previous and previous[0] != raw_hash:
                raise InvalidSnapshot('conflicting bytes for the same observation')
            if previous and previous[1] is None:
                raise InvalidSnapshot('legacy observation has no retained raw bytes; record a new observation')
            db.execute('INSERT OR IGNORE INTO observations VALUES (?, ?, ?, ?, ?)',
                       (snapshot_id, source, observed.isoformat(), raw_hash, raw))
            for row in rows:
                db.execute('INSERT OR IGNORE INTO draw_versions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                           (snapshot_id, row['issue'], row['draw_date'], json.dumps(row['front']),
                            json.dumps(row['back']), 1, int(row['prize_fields_present']), 0,
                            row['settlement_reason'], _canonical(row['raw_row'])))
            result = {'snapshot_id': snapshot_id, 'row_count': len(rows), 'source': source,
                      'observed_at': observed.isoformat()}
            if import_request:
                db.execute('INSERT INTO bundled_snapshot_imports VALUES (?,?,?,?,?)',
                           (request_key, request_digest, request_raw, _canonical(result), datetime.now(timezone.utc).isoformat()))
        return result

    def bundled_import_status(self, request_key):
        if not isinstance(request_key, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I):
            raise InvalidSnapshot('snapshot import request key is invalid')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT request_sha256,request_json,result_json FROM bundled_snapshot_imports WHERE request_key=?',
                             (request_key,)).fetchone()
            fence = db.execute('SELECT request_sha256,request_json FROM bundled_snapshot_import_fences WHERE request_key=?',
                               (request_key,)).fetchone()
            retry = db.execute('SELECT retry_of FROM bundled_snapshot_import_retries WHERE request_key=?',
                               (request_key,)).fetchone()
            retry_row = db.execute('SELECT retry_of,request_sha256,request_json FROM bundled_snapshot_import_retries WHERE request_key=?',
                                   (request_key,)).fetchone()
            has_retry_child = db.execute('SELECT 1 FROM bundled_snapshot_import_retries WHERE retry_of=?',
                                         (request_key,)).fetchone()
            if row and fence:
                raise InvalidSnapshot('snapshot import request is both accepted and fenced')
            if retry_row:
                if not row and not fence:
                    raise InvalidSnapshot('snapshot import retry record has no accepted or fenced request')
                retry_of, retry_digest, retry_raw = retry_row
                original = db.execute('SELECT request_sha256,request_json FROM bundled_snapshot_import_fences WHERE request_key=?',
                                      (retry_of,)).fetchone()
                try:
                    retry_valid = (retry_of != request_key and retry_digest == hashlib.sha256(retry_raw).hexdigest()
                                   and _canonical(json.loads(retry_raw)) == retry_raw)
                except (TypeError, ValueError, json.JSONDecodeError):
                    retry_valid = False
                if not retry_valid or not original or original != (retry_digest, retry_raw):
                    raise InvalidSnapshot('snapshot import retry record does not match its original fence')
                current = row or fence
                if current and current[:2] != (retry_digest, retry_raw):
                    raise InvalidSnapshot('snapshot import status differs from its retry record')
            if has_retry_child:
                child_key, child_of, child_digest, child_raw = db.execute(
                    'SELECT request_key,retry_of,request_sha256,request_json FROM bundled_snapshot_import_retries WHERE retry_of=?',
                    (request_key,)).fetchone()
                child_accepted = db.execute('SELECT request_sha256,request_json FROM bundled_snapshot_imports WHERE request_key=?',
                                            (child_key,)).fetchone()
                child_fenced = db.execute('SELECT request_sha256,request_json FROM bundled_snapshot_import_fences WHERE request_key=?',
                                          (child_key,)).fetchone()
                try:
                    child_valid = (isinstance(child_raw, bytes)
                                   and child_digest == hashlib.sha256(child_raw).hexdigest()
                                   and _canonical(json.loads(child_raw)) == child_raw)
                except (TypeError, ValueError, json.JSONDecodeError):
                    child_valid = False
                if (row or not fence or child_of != request_key or not child_valid
                        or (child_accepted is None) == (child_fenced is None)
                        or (child_accepted or child_fenced) != (child_digest, child_raw)
                        or fence != (child_digest, child_raw)):
                    raise InvalidSnapshot('snapshot import retry association is orphaned or inconsistent')
        if row:
            try:
                request = json.loads(row[1])
                result = json.loads(row[2])
                if (not isinstance(request, dict) or set(request) != {'source', 'snapshot_id', 'raw_sha256'}
                        or not isinstance(request['source'], str) or not request['source'].strip()
                        or not re.fullmatch(r'[0-9a-f]{64}', request['snapshot_id'])
                        or not re.fullmatch(r'[0-9a-f]{64}', request['raw_sha256'])
                        or _canonical(request) != row[1] or hashlib.sha256(row[1]).hexdigest() != row[0]
                        or not isinstance(result, dict) or set(result) != {'snapshot_id', 'row_count', 'source', 'observed_at'}
                        or result['snapshot_id'] != request['snapshot_id'] or result['source'] != request['source']
                        or type(result['row_count']) is not int or result['row_count'] < 1
                        or not isinstance(result['observed_at'], str) or _canonical(result) != row[2]):
                    raise ValueError
                observed_at = datetime.fromisoformat(result['observed_at'])
                if observed_at.tzinfo is None or observed_at.utcoffset() is None:
                    raise ValueError
                with closing(sqlite3.connect(self.path)) as db:
                    snapshot = db.execute('SELECT raw_sha256,canonical_json,row_count FROM snapshots WHERE id=?',
                                          (request['snapshot_id'],)).fetchone()
                    observation = db.execute('SELECT raw_payload FROM observations WHERE snapshot_id=? AND source=? AND observed_at=? AND raw_sha256=?',
                                             (request['snapshot_id'], request['source'], result['observed_at'], request['raw_sha256'])).fetchone()
                if (not snapshot or hashlib.sha256(snapshot[1]).hexdigest() != request['snapshot_id']
                        or snapshot[2] != result['row_count'] or not observation
                        or not isinstance(observation[0], bytes)
                        or hashlib.sha256(observation[0]).hexdigest() != request['raw_sha256']):
                    raise ValueError
                payload, parsed = parse_snapshot(observation[0])
                if (hashlib.sha256(_canonical(payload)).hexdigest() != request['snapshot_id']
                        or len(parsed) != result['row_count']):
                    raise ValueError
            except (TypeError, KeyError, ValueError, json.JSONDecodeError) as exc:
                raise InvalidSnapshot('accepted snapshot import receipt does not match its durable snapshot') from exc
            return {'status': 'accepted', 'request_key': request_key, 'request_sha256': row[0],
                    'request': request, 'result': result,
                    'retry_of': retry[0] if retry else None}
        if fence:
            try:
                request = json.loads(fence[1])
                if (not isinstance(request, dict) or set(request) != {'source', 'snapshot_id', 'raw_sha256'}
                        or not isinstance(request['source'], str) or not request['source'].strip()
                        or not isinstance(request['snapshot_id'], str)
                        or not re.fullmatch(r'[0-9a-f]{64}', request['snapshot_id'])
                        or not isinstance(request['raw_sha256'], str)
                        or not re.fullmatch(r'[0-9a-f]{64}', request['raw_sha256'])
                        or _canonical(request) != fence[1]
                        or hashlib.sha256(fence[1]).hexdigest() != fence[0]):
                    raise ValueError
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise InvalidSnapshot('snapshot import fence record is invalid') from exc
            return {'status': 'fenced', 'request_key': request_key, 'request_sha256': fence[0],
                    'request': request, 'retry_of': retry[0] if retry else None}
        return {'status': 'unknown', 'request_key': request_key}

    def fence_bundled_import(self, request_key, raw: bytes, source: str, *, retry_of=None):
        if not isinstance(request_key, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I):
            raise InvalidSnapshot('snapshot import request key is invalid')
        _, _, _, _, request, request_raw, digest = self._bundle_import_identity(raw, source)
        self.initialize()
        now = datetime.now(timezone.utc).isoformat()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys = ON')
            db.execute('BEGIN IMMEDIATE')
            accepted = db.execute('SELECT request_sha256,request_json FROM bundled_snapshot_imports WHERE request_key=?',
                                  (request_key,)).fetchone()
            fence = db.execute('SELECT request_sha256,request_json FROM bundled_snapshot_import_fences WHERE request_key=?',
                               (request_key,)).fetchone()
            if accepted and accepted != (digest, request_raw):
                raise InvalidSnapshot('snapshot import request key used for different bundle content')
            if fence and fence != (digest, request_raw):
                raise InvalidSnapshot('snapshot import fence key used for different bundle content')
            retry = db.execute('SELECT retry_of FROM bundled_snapshot_import_retries WHERE request_key=?',
                               (request_key,)).fetchone()
            if accepted or fence:
                if (retry[0] if retry else None) != retry_of:
                    raise InvalidSnapshot('snapshot import retry association differs from the recorded request')
            else:
                self._import_retry(db, request_key, digest, request_raw, retry_of, now)
                db.execute('INSERT INTO bundled_snapshot_import_fences VALUES (?,?,?,?)',
                           (request_key, digest, request_raw, now))
        return self.bundled_import_status(request_key)

    def get_draws(self, snapshot_id: str):
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute('SELECT * FROM draw_versions WHERE snapshot_id = ? ORDER BY draw_date, issue',
                              (snapshot_id,)).fetchall()
        return [{**dict(row), 'front': json.loads(row['front_json']), 'back': json.loads(row['back_json'])}
                for row in rows]

    def conflicts(self, issue: str):
        """Return all versions for an issue when source row content differs; choose none."""
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT snapshot_id, row_json FROM draw_versions WHERE issue = ? ORDER BY snapshot_id',
                              (issue,)).fetchall()
        if len({row[1] for row in rows}) <= 1:
            return []
        return [{'snapshot_id': sid, 'row': json.loads(raw)} for sid, raw in rows]
