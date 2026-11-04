"""Locally attested research protocol and paired portfolio registration.

The operator explicitly reviews supplied source documents. Neither a local
timestamp nor a locally supplied URL and document is independent authentication.
No ticket purchase, official publication verification or settlement occurs here.
"""
from contextlib import closing
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlsplit

from dlt.candidates import CandidateError, _canonical
from dlt.model_repository import ModelRepository
from dlt.pair_repository import PairRepository
from dlt.settlement import SettlementBlocked, exact_money_context, settle_portfolio
from dlt.prize_rules import validate_dlt_2026_rule_mapping


class RegistrationError(ValueError):
    pass


def _time(value):
    try:
        result = datetime.fromisoformat(value)
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError('timezone required')
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError) as exc:
        raise RegistrationError('timezone-aware timestamp required') from exc


def _source_url(value, kind):
    if not isinstance(value, str) or not 1 <= len(value) <= 2048:
        return False
    try:
        parts = urlsplit(value)
        allowed = {'www.sporttery.cn', 'm.sporttery.cn', 'static.sporttery.cn', 'webapi.sporttery.cn',
                   'lottery.gov.cn', 'www.lottery.gov.cn', 'm.lottery.gov.cn'}
        host = parts.hostname or ''
        tax_official = kind == 'tax' and (host == 'chinatax.gov.cn' or host.endswith('.chinatax.gov.cn')
                                           or host == 'mof.gov.cn' or host.endswith('.mof.gov.cn'))
        return (parts.scheme == 'https' and (host in allowed or tax_official)
                and parts.username is None and parts.password is None and parts.port in (None, 443))
    except (ValueError, TypeError):
        return False


class ProspectiveRepository:
    def __init__(self, path, *, clock=None):
        self.path = Path(path)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.pairs = PairRepository(path)

    def _now(self):
        current = self.clock()
        if not isinstance(current, datetime) or current.tzinfo is None or current.utcoffset() is None:
            raise RegistrationError('local clock must provide a timezone-aware instant')
        return current.astimezone(timezone.utc)

    def initialize(self):
        self.pairs._initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS reviewed_sources (
                    id TEXT PRIMARY KEY, record_json BLOB NOT NULL, document BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fixed_experiments (
                    request_key TEXT PRIMARY KEY, request_json BLOB NOT NULL, content_sha256 TEXT NOT NULL,
                    content_json BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fixed_experiment_write_intents (
                    request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL, series_key TEXT,
                    retry_of TEXT UNIQUE REFERENCES fixed_experiment_write_intents(request_key), created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fixed_experiment_write_fences (
                    request_key TEXT PRIMARY KEY REFERENCES fixed_experiment_write_intents(request_key),
                    request_sha256 TEXT NOT NULL, fenced_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS experiment_closures (
                    experiment_key TEXT PRIMARY KEY REFERENCES fixed_experiments(request_key),
                    closed_at TEXT NOT NULL, reason TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS prospective_closure_write_requests (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, object_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, result_json BLOB NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS prospective_closure_write_fences (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, object_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS prospective_closure_write_retries (
                    request_key TEXT PRIMARY KEY,
                    retry_of TEXT NOT NULL UNIQUE REFERENCES prospective_closure_write_fences(request_key),
                    operation TEXT NOT NULL, object_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS experiment_series (
                    series_key TEXT PRIMARY KEY, content_sha256 TEXT NOT NULL, content_json BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS experiment_series_write_requests (
                    request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL,
                    result_json BLOB NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(request_key) REFERENCES experiment_series(series_key)
                );
                CREATE TABLE IF NOT EXISTS experiment_series_write_fences (
                    request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL,
                    request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS experiment_series_write_retries (
                    request_key TEXT PRIMARY KEY,
                    retry_of TEXT NOT NULL UNIQUE REFERENCES experiment_series_write_fences(request_key),
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS series_periods (
                    series_key TEXT NOT NULL REFERENCES experiment_series(series_key),
                    issue TEXT NOT NULL, experiment_key TEXT NOT NULL UNIQUE REFERENCES fixed_experiments(request_key),
                    PRIMARY KEY(series_key,issue)
                );
                CREATE TABLE IF NOT EXISTS series_closures (
                    series_key TEXT PRIMARY KEY REFERENCES experiment_series(series_key),
                    closed_at TEXT NOT NULL, reason TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS experiment_series_no_update BEFORE UPDATE ON experiment_series
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment series'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_no_delete BEFORE DELETE ON experiment_series
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment series'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_write_requests_no_update BEFORE UPDATE ON experiment_series_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment series write'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_write_requests_no_delete BEFORE DELETE ON experiment_series_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment series write'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_write_fences_no_update BEFORE UPDATE ON experiment_series_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment series fence'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_write_fences_no_delete BEFORE DELETE ON experiment_series_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment series fence'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_write_retries_no_update BEFORE UPDATE ON experiment_series_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment series retry'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_write_retries_no_delete BEFORE DELETE ON experiment_series_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment series retry'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_write_requests_no_fenced_insert BEFORE INSERT ON experiment_series_write_requests
                    WHEN EXISTS (SELECT 1 FROM experiment_series_write_fences WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'fenced experiment series write'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_write_fences_no_accepted_insert BEFORE INSERT ON experiment_series_write_fences
                    WHEN EXISTS (SELECT 1 FROM experiment_series_write_requests WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'accepted experiment series write'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_series_no_fenced_insert BEFORE INSERT ON experiment_series
                    WHEN EXISTS (SELECT 1 FROM experiment_series_write_fences WHERE request_key=NEW.series_key)
                    BEGIN SELECT RAISE(ABORT, 'fenced experiment series write'); END;
                CREATE TRIGGER IF NOT EXISTS series_periods_no_update BEFORE UPDATE ON series_periods
                    BEGIN SELECT RAISE(ABORT, 'immutable series period'); END;
                CREATE TRIGGER IF NOT EXISTS series_periods_no_delete BEFORE DELETE ON series_periods
                    BEGIN SELECT RAISE(ABORT, 'immutable series period'); END;
                CREATE TRIGGER IF NOT EXISTS series_closures_no_update BEFORE UPDATE ON series_closures
                    BEGIN SELECT RAISE(ABORT, 'immutable series closure'); END;
                CREATE TRIGGER IF NOT EXISTS series_closures_no_delete BEFORE DELETE ON series_closures
                    BEGIN SELECT RAISE(ABORT, 'immutable series closure'); END;
                CREATE TABLE IF NOT EXISTS source_corrections (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    old_review_id TEXT NOT NULL UNIQUE REFERENCES reviewed_sources(id),
                    new_review_id TEXT NOT NULL UNIQUE REFERENCES reviewed_sources(id),
                    kind TEXT NOT NULL, note TEXT NOT NULL, declared_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_correction_followups (
                    correction_id INTEGER PRIMARY KEY REFERENCES source_corrections(seq),
                    status TEXT NOT NULL, token TEXT, lease_until TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0, details_json BLOB, updated_at TEXT
                );
                CREATE TABLE IF NOT EXISTS source_write_requests (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, object_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, result_json BLOB NOT NULL,
                    created_at TEXT NOT NULL, retry_of TEXT
                );
                CREATE TABLE IF NOT EXISTS source_write_fences (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, object_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_write_retries (
                    request_key TEXT PRIMARY KEY,
                    retry_of TEXT NOT NULL UNIQUE REFERENCES source_write_fences(request_key),
                    operation TEXT NOT NULL, object_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scoped_source_corrections (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, issue TEXT NOT NULL,
                    old_review_id TEXT NOT NULL REFERENCES reviewed_sources(id),
                    new_review_id TEXT NOT NULL REFERENCES reviewed_sources(id),
                    kind TEXT NOT NULL, note TEXT NOT NULL, declared_at TEXT NOT NULL,
                    UNIQUE(issue,old_review_id), UNIQUE(issue,new_review_id)
                );
                CREATE TABLE IF NOT EXISTS scoped_correction_followups (
                    correction_id INTEGER PRIMARY KEY REFERENCES scoped_source_corrections(seq),
                    status TEXT NOT NULL, token TEXT, lease_until TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0, details_json BLOB, updated_at TEXT
                );
                CREATE TRIGGER IF NOT EXISTS reviewed_sources_no_update BEFORE UPDATE ON reviewed_sources
                    BEGIN SELECT RAISE(ABORT, 'immutable source review'); END;
                CREATE TRIGGER IF NOT EXISTS reviewed_sources_no_delete BEFORE DELETE ON reviewed_sources
                    BEGIN SELECT RAISE(ABORT, 'immutable source review'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_experiments_no_update BEFORE UPDATE ON fixed_experiments
                    BEGIN SELECT RAISE(ABORT, 'immutable fixed experiment'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_experiments_no_delete BEFORE DELETE ON fixed_experiments
                    BEGIN SELECT RAISE(ABORT, 'immutable fixed experiment'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_experiment_write_intents_no_update BEFORE UPDATE ON fixed_experiment_write_intents
                    BEGIN SELECT RAISE(ABORT, 'immutable fixed experiment write intent'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_experiment_write_intents_no_delete BEFORE DELETE ON fixed_experiment_write_intents
                    BEGIN SELECT RAISE(ABORT, 'immutable fixed experiment write intent'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_experiment_write_fences_no_update BEFORE UPDATE ON fixed_experiment_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable fixed experiment write fence'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_experiment_write_fences_no_delete BEFORE DELETE ON fixed_experiment_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable fixed experiment write fence'); END;
                CREATE TRIGGER IF NOT EXISTS fixed_experiments_no_fenced_insert BEFORE INSERT ON fixed_experiments
                    WHEN EXISTS (SELECT 1 FROM fixed_experiment_write_fences WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'fenced fixed experiment request'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_closures_no_update BEFORE UPDATE ON experiment_closures
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment closure'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_closures_no_delete BEFORE DELETE ON experiment_closures
                    BEGIN SELECT RAISE(ABORT, 'immutable experiment closure'); END;
                CREATE TRIGGER IF NOT EXISTS experiment_closures_no_unresolved_fence BEFORE INSERT ON experiment_closures
                    WHEN EXISTS (SELECT 1 FROM prospective_closure_write_fences f
                        WHERE f.operation='experiment_close' AND f.object_key=NEW.experiment_key
                        AND NOT EXISTS (SELECT 1 FROM prospective_closure_write_retries r
                            WHERE r.retry_of=f.request_key AND (EXISTS (SELECT 1 FROM prospective_closure_write_requests q WHERE q.request_key=r.request_key)
                                OR EXISTS (SELECT 1 FROM prospective_closure_write_fences f2 WHERE f2.request_key=r.request_key))))
                    BEGIN SELECT RAISE(ABORT, 'unresolved fenced experiment closure'); END;
                CREATE TRIGGER IF NOT EXISTS series_closures_no_unresolved_fence BEFORE INSERT ON series_closures
                    WHEN EXISTS (SELECT 1 FROM prospective_closure_write_fences f
                        WHERE f.operation='series_close' AND f.object_key=NEW.series_key
                        AND NOT EXISTS (SELECT 1 FROM prospective_closure_write_retries r
                            WHERE r.retry_of=f.request_key AND (EXISTS (SELECT 1 FROM prospective_closure_write_requests q WHERE q.request_key=r.request_key)
                                OR EXISTS (SELECT 1 FROM prospective_closure_write_fences f2 WHERE f2.request_key=r.request_key))))
                    BEGIN SELECT RAISE(ABORT, 'unresolved fenced series closure'); END;
                CREATE TRIGGER IF NOT EXISTS prospective_closure_write_requests_no_update BEFORE UPDATE ON prospective_closure_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable prospective closure write request'); END;
                CREATE TRIGGER IF NOT EXISTS prospective_closure_write_requests_no_delete BEFORE DELETE ON prospective_closure_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable prospective closure write request'); END;
                CREATE TRIGGER IF NOT EXISTS prospective_closure_write_fences_no_update BEFORE UPDATE ON prospective_closure_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable prospective closure write fence'); END;
                CREATE TRIGGER IF NOT EXISTS prospective_closure_write_fences_no_delete BEFORE DELETE ON prospective_closure_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable prospective closure write fence'); END;
                CREATE TRIGGER IF NOT EXISTS prospective_closure_write_retries_no_update BEFORE UPDATE ON prospective_closure_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable prospective closure write retry'); END;
                CREATE TRIGGER IF NOT EXISTS prospective_closure_write_retries_no_delete BEFORE DELETE ON prospective_closure_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable prospective closure write retry'); END;
                CREATE TRIGGER IF NOT EXISTS prospective_closure_write_requests_no_fenced_insert BEFORE INSERT ON prospective_closure_write_requests
                    WHEN EXISTS (SELECT 1 FROM prospective_closure_write_fences WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'fenced prospective closure write'); END;
                CREATE TRIGGER IF NOT EXISTS prospective_closure_write_fences_no_committed_insert BEFORE INSERT ON prospective_closure_write_fences
                    WHEN EXISTS (SELECT 1 FROM prospective_closure_write_requests WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'committed prospective closure write'); END;
                CREATE TRIGGER IF NOT EXISTS source_write_requests_no_update BEFORE UPDATE ON source_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable source write request'); END;
                CREATE TRIGGER IF NOT EXISTS source_write_requests_no_delete BEFORE DELETE ON source_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable source write request'); END;
                CREATE TRIGGER IF NOT EXISTS source_write_fences_no_update BEFORE UPDATE ON source_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable source write fence'); END;
                CREATE TRIGGER IF NOT EXISTS source_write_fences_no_delete BEFORE DELETE ON source_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable source write fence'); END;
                CREATE TRIGGER IF NOT EXISTS source_write_retries_no_update BEFORE UPDATE ON source_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable source write retry'); END;
                CREATE TRIGGER IF NOT EXISTS source_write_retries_no_delete BEFORE DELETE ON source_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable source write retry'); END;
                CREATE TRIGGER IF NOT EXISTS source_write_requests_no_fenced_insert BEFORE INSERT ON source_write_requests
                    WHEN EXISTS (SELECT 1 FROM source_write_fences WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'fenced source write'); END;
                CREATE TRIGGER IF NOT EXISTS source_write_fences_no_committed_insert BEFORE INSERT ON source_write_fences
                    WHEN EXISTS (SELECT 1 FROM source_write_requests WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'committed source write'); END;
                CREATE TRIGGER IF NOT EXISTS source_corrections_no_update BEFORE UPDATE ON source_corrections
                    BEGIN SELECT RAISE(ABORT, 'immutable source correction'); END;
                CREATE TRIGGER IF NOT EXISTS source_corrections_no_delete BEFORE DELETE ON source_corrections
                    BEGIN SELECT RAISE(ABORT, 'immutable source correction'); END;
                CREATE TRIGGER IF NOT EXISTS source_corrections_queue AFTER INSERT ON source_corrections
                    BEGIN INSERT INTO source_correction_followups(correction_id,status) VALUES (new.seq,'pending'); END;
                CREATE TRIGGER IF NOT EXISTS scoped_source_corrections_no_update BEFORE UPDATE ON scoped_source_corrections
                    BEGIN SELECT RAISE(ABORT, 'immutable scoped source correction'); END;
                CREATE TRIGGER IF NOT EXISTS scoped_source_corrections_no_delete BEFORE DELETE ON scoped_source_corrections
                    BEGIN SELECT RAISE(ABORT, 'immutable scoped source correction'); END;
                CREATE TRIGGER IF NOT EXISTS scoped_source_corrections_queue AFTER INSERT ON scoped_source_corrections
                    BEGIN INSERT INTO scoped_correction_followups(correction_id,status) VALUES (new.seq,'pending'); END;
            ''')
            # Migrate corrections created before the durable follow-up queue.
            db.execute("INSERT OR IGNORE INTO source_correction_followups(correction_id,status) "
                       "SELECT seq,'pending' FROM source_corrections")

    def record_source_review(self, *, kind, source_url, document, extracted, reviewer_note, confirmed,
                             document_name=None, write_key=None, request_sha256=None, retry_of=None):
        """Record an explicit personal review, retaining its exact source bytes."""
        if kind not in ('calendar', 'rule', 'tax') or not _source_url(source_url, kind):
            raise RegistrationError('supported official HTTPS source and review kind required')
        if (not isinstance(document, bytes) or not 1 <= len(document) <= 5*1024*1024
                or not isinstance(extracted, dict) or not isinstance(reviewer_note, str)
                or not reviewer_note.strip() or confirmed is not True):
            raise RegistrationError('source bytes, extracted data, review note and explicit confirmation required')
        if document_name is not None and (not isinstance(document_name, str) or not 1 <= len(document_name) <= 128
                or document_name in ('.', '..') or any(ch in document_name for ch in '/\\\r\n\x00')
                or any(ord(ch) < 32 for ch in document_name)):
            raise RegistrationError('source document name must be a plain filename')
        document_sha256 = hashlib.sha256(document).hexdigest()
        reviewer_note = reviewer_note.strip()
        request = {'kind': kind, 'source_url': source_url, 'document_sha256': document_sha256,
                   'document_size': len(document), 'extracted': extracted, 'reviewer_note': reviewer_note,
                   'confirmed': True, 'document_name': document_name}
        record = {'kind': kind, 'source_url': source_url, 'document_sha256': document_sha256,
                  'extracted': extracted, 'reviewer_note': reviewer_note, 'reviewed_at': self._now().isoformat(),
                  'assurance': 'personal review of supplied bytes; source origin not independently authenticated'}
        if document_name is not None:
            record['document_name'] = document_name
        try:
            payload = _canonical(record)
        except (ValueError, TypeError) as exc:
            raise RegistrationError('review must be finite JSON') from exc
        ident = hashlib.sha256(payload).hexdigest()
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            if write_key is not None:
                db.execute('BEGIN IMMEDIATE')
                from dlt.source_write_protocol import accept

                def insert_review():
                    db.execute('INSERT OR IGNORE INTO reviewed_sources VALUES (?, ?, ?)', (ident, payload, document))
                    row = db.execute('SELECT record_json, document FROM reviewed_sources WHERE id=?', (ident,)).fetchone()
                    if row != (payload, document):
                        raise RegistrationError('source review identifier conflict')
                    return {'review_id': ident,
                            'assurance': 'personal review; origin not independently authenticated'}

                return accept(db, request_key=write_key, operation='source_review', request=request,
                              request_sha256=request_sha256, retry_of=retry_of, insert=insert_review)
            db.execute('INSERT OR IGNORE INTO reviewed_sources VALUES (?, ?, ?)', (ident, payload, document))
            row = db.execute('SELECT record_json, document FROM reviewed_sources WHERE id=?', (ident,)).fetchone()
            if row != (payload, document):
                raise RegistrationError('source review identifier conflict')
        return ident

    @staticmethod
    def _get_review(db, ident, kind):
        row = db.execute('SELECT record_json, document FROM reviewed_sources WHERE id=?', (ident,)).fetchone()
        if not row:
            raise RegistrationError(f'{kind} review not found')
        record = json.loads(row[0])
        if (hashlib.sha256(row[0]).hexdigest() != ident or record['kind'] != kind
                or hashlib.sha256(row[1]).hexdigest() != record['document_sha256']):
            raise RegistrationError(f'{kind} review integrity failure')
        return record

    @classmethod
    def _effective_review(cls, db, ident, kind, *, issue=None):
        """Follow a declared correction chain without changing the original."""
        seen, chain = set(), []
        while True:
            if ident in seen:
                raise RegistrationError('source correction cycle detected')
            seen.add(ident)
            record = cls._get_review(db, ident, kind)
            chain.append(ident)
            link = db.execute('SELECT new_review_id, kind FROM source_corrections WHERE old_review_id=?',
                              (ident,)).fetchone()
            scoped = None
            if issue is not None and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='scoped_source_corrections'").fetchone():
                scoped = db.execute('SELECT new_review_id,kind FROM scoped_source_corrections WHERE old_review_id=? AND issue=?',
                                    (ident, issue)).fetchone()
            if link and scoped:
                raise RegistrationError('global and issue-scoped source corrections conflict')
            link = link or scoped
            if not link:
                return ident, record, chain
            if link[1] != kind:
                raise RegistrationError('source correction kind mismatch')
            ident = link[0]

    @classmethod
    def _calendar_for_fixed(cls, db, content):
        """Recheck original registration against the current reviewed calendar."""
        protocol = content['protocol']; issue = protocol['target_issue']
        ident, record, chain = cls._effective_review(db, protocol['calendar_review_id'], 'calendar')
        calendar = record['extracted']; cutoff = _time(calendar.get('cutoff_at'))
        if (calendar.get('issue') != issue or not isinstance(calendar.get('version'), str)
                or not calendar['version'].strip()
                or cutoff.astimezone(timezone(timedelta(hours=8))).date().isoformat() != calendar.get('draw_date')):
            raise RegistrationError('corrected calendar issue, date or cutoff is invalid')
        if any(item['pair']['config']['target_date'] != calendar['draw_date'] for item in content['pairs']):
            raise RegistrationError('corrected calendar draw date differs from fixed pair target date')
        if _time(content['registered_at']) >= cutoff:
            raise RegistrationError('registration at or after corrected calendar cutoff is not eligible')
        return ident, record, chain

    def _declare_source_correction_db(self, db, old_id, new_id, note):
        raw = db.execute('SELECT record_json FROM reviewed_sources WHERE id=?', (old_id,)).fetchone()
        if not raw:
            raise RegistrationError('original source review not found')
        try:
            kind = json.loads(raw[0]).get('kind')
        except (ValueError, TypeError, AttributeError) as exc:
            raise RegistrationError('original source review is invalid') from exc
        if kind not in ('rule', 'tax', 'calendar'):
            raise RegistrationError('only calendar, rule and tax corrections are supported')
        old = self._get_review(db, old_id, kind)
        new = self._get_review(db, new_id, kind)
        if kind == 'calendar':
            before, after = old['extracted'], new['extracted']
            cutoff = _time(after.get('cutoff_at'))
            if (not re.fullmatch(r'[0-9]{5}', str(after.get('issue', '')))
                    or before.get('issue') != after.get('issue')
                    or not isinstance(after.get('version'), str) or not after['version'].strip()
                    or cutoff.astimezone(timezone(timedelta(hours=8))).date().isoformat() != after.get('draw_date')):
                raise RegistrationError('calendar correction must identify the same issue with valid date and cutoff')
        if db.execute('SELECT 1 FROM source_corrections WHERE old_review_id=?', (old_id,)).fetchone():
            raise RegistrationError('original review already superseded; reload and review the latest version')
        if db.execute('SELECT 1 FROM scoped_source_corrections WHERE old_review_id=?', (old_id,)).fetchone():
            raise RegistrationError('original review already has issue-scoped corrections; global correction would conflict')
        if db.execute('SELECT 1 FROM source_corrections WHERE old_review_id=? OR new_review_id=?',
                      (new_id, new_id)).fetchone():
            raise RegistrationError('replacement review is already in a correction chain')
        if db.execute('SELECT 1 FROM scoped_source_corrections WHERE old_review_id=? OR new_review_id=?', (new_id, new_id)).fetchone():
            raise RegistrationError('replacement review is already in an issue-scoped correction chain')
        if _time(new['reviewed_at']) < _time(old['reviewed_at']):
            raise RegistrationError('replacement review predates the original')
        now = self._now()
        if _time(new['reviewed_at']) > now:
            raise RegistrationError('replacement review time is later than local clock')
        cursor = db.execute('INSERT INTO source_corrections(old_review_id,new_review_id,kind,note,declared_at) '
                            'VALUES (?, ?, ?, ?, ?)', (old_id, new_id, kind, note, now.isoformat()))
        return {'correction_id': cursor.lastrowid, 'kind': kind, 'old_review_id': old_id,
                'new_review_id': new_id, 'note': note, 'declared_at': now.isoformat(),
                'scope': 'global', 'assurance': 'personal correction; source origin and time not independently authenticated'}

    def _declare_scoped_source_correction_db(self, db, old_id, new_id, issue, note):
        row = db.execute('SELECT record_json FROM reviewed_sources WHERE id=?', (old_id,)).fetchone()
        if not row:
            raise RegistrationError('original source review not found')
        kind = json.loads(row[0]).get('kind')
        if kind not in ('rule', 'tax'):
            raise RegistrationError('issue-scoped corrections support only rule and tax reviews')
        old = self._get_review(db, old_id, kind); new = self._get_review(db, new_id, kind)
        old_issues = old['extracted'].get('applicable_issues'); new_issues = new['extracted'].get('applicable_issues')
        if (not isinstance(old_issues, list) or not isinstance(new_issues, list)
                or issue not in old_issues or issue not in new_issues):
            raise RegistrationError('original and replacement reviews must cover the selected issue')
        if self._effective_review(db, old_id, kind, issue=issue)[0] != old_id:
            raise RegistrationError('original review already superseded for this issue; choose current review')
        if (db.execute('SELECT 1 FROM source_corrections WHERE old_review_id=? OR new_review_id=?', (new_id, new_id)).fetchone()
                or db.execute('SELECT 1 FROM scoped_source_corrections WHERE issue=? AND (old_review_id=? OR new_review_id=?)',
                              (issue, new_id, new_id)).fetchone()):
            raise RegistrationError('replacement review is already in this issue correction chain')
        now = self._now()
        if _time(new['reviewed_at']) < _time(old['reviewed_at']) or _time(new['reviewed_at']) > now:
            raise RegistrationError('replacement review time is invalid')
        cursor = db.execute('INSERT INTO scoped_source_corrections(issue,old_review_id,new_review_id,kind,note,declared_at) '
                            'VALUES (?,?,?,?,?,?)', (issue, old_id, new_id, kind, note, now.isoformat()))
        return {'correction_id': cursor.lastrowid, 'scope': 'issue', 'issue': issue, 'kind': kind,
                'old_review_id': old_id, 'new_review_id': new_id, 'note': note, 'declared_at': now.isoformat(),
                'assurance': 'personal correction; source origin and time not independently authenticated'}

    def declare_source_correction(self, old_id, new_id, *, note, confirmed,
                                  write_key=None, request_sha256=None, retry_of=None):
        """Append a personally reviewed rule, tax or calendar correction."""
        if (not isinstance(old_id, str) or not re.fullmatch(r'[0-9a-f]{64}', old_id)
                or not isinstance(new_id, str) or not re.fullmatch(r'[0-9a-f]{64}', new_id)
                or old_id == new_id or not isinstance(note, str) or not note.strip()
                or len(note) > 1000 or confirmed is not True):
            raise RegistrationError('distinct review identifiers, reason and explicit confirmation required')
        note = note.strip()
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            if write_key is None:
                return self._declare_source_correction_db(db, old_id, new_id, note)
            from dlt.source_write_protocol import accept
            request = {'old_review_id': old_id, 'new_review_id': new_id, 'issue': None,
                       'note': note, 'confirmed': True}
            return accept(db, request_key=write_key, operation='source_correction', request=request,
                          request_sha256=request_sha256, retry_of=retry_of,
                          insert=lambda: self._declare_source_correction_db(db, old_id, new_id, note))

    def declare_scoped_source_correction(self, old_id, new_id, *, issue, note, confirmed,
                                         write_key=None, request_sha256=None, retry_of=None):
        """Append a rule or tax replacement for one issue only."""
        if (not isinstance(old_id, str) or not re.fullmatch(r'[0-9a-f]{64}', old_id)
                or not isinstance(new_id, str) or not re.fullmatch(r'[0-9a-f]{64}', new_id)
                or old_id == new_id or not isinstance(issue, str) or not re.fullmatch(r'[0-9]{5}', issue)
                or not isinstance(note, str) or not note.strip() or len(note) > 1000 or confirmed is not True):
            raise RegistrationError('distinct review identifiers, issue, reason and explicit confirmation required')
        note = note.strip()
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON'); db.execute('BEGIN IMMEDIATE')
            if write_key is None:
                return self._declare_scoped_source_correction_db(db, old_id, new_id, issue, note)
            from dlt.source_write_protocol import accept
            request = {'old_review_id': old_id, 'new_review_id': new_id, 'issue': issue,
                       'note': note, 'confirmed': True}
            return accept(db, request_key=write_key, operation='source_correction', request=request,
                          request_sha256=request_sha256, retry_of=retry_of,
                          insert=lambda: self._declare_scoped_source_correction_db(db, old_id, new_id, issue, note))

    def list_source_corrections(self):
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT c.*, 'global' AS scope, NULL AS issue, f.status AS followup_status, f.attempts, f.details_json, f.updated_at "
                              'FROM source_corrections c LEFT JOIN source_correction_followups f ON f.correction_id=c.seq').fetchall()
            rows += db.execute("SELECT c.seq,c.old_review_id,c.new_review_id,c.kind,c.note,c.declared_at, 'issue' AS scope, c.issue, "
                               'f.status AS followup_status,f.attempts,f.details_json,f.updated_at FROM scoped_source_corrections c '
                               'LEFT JOIN scoped_correction_followups f ON f.correction_id=c.seq').fetchall()
            output = []
            for row in rows:
                item = dict(row)
                encoded = item.pop('details_json')
                item['details'] = json.loads(encoded) if encoded else None
                output.append(item)
            return sorted(output, key=lambda item: (item['declared_at'], item['scope'], item['seq']))

    @staticmethod
    def _protocol(protocol):
        if not isinstance(protocol, dict):
            raise RegistrationError('protocol required')
        required = ('hypothesis', 'primary_metric', 'target_issue', 'observation_issues', 'replicas', 'aggregation',
                    'exclusion_rule', 'retraining_policy', 'selection_rule', 'time_assurance', 'internal_deadline',
                    'calendar_review_id', 'rule_review_id', 'tax_review_id', 'other_winnings_assumption')
        if any(key not in protocol for key in required):
            raise RegistrationError('incomplete protocol')
        for key in ('hypothesis', 'exclusion_rule', 'retraining_policy', 'selection_rule'):
            if not isinstance(protocol[key], str) or not protocol[key].strip():
                raise RegistrationError(f'{key} is required')
        if protocol['primary_metric'] != 'paired_net_difference' or protocol['aggregation'] != 'arithmetic_mean_v1':
            raise RegistrationError('explicit paired metric and arithmetic replica aggregation required')
        if protocol['time_assurance'] != 'local_clock':
            raise RegistrationError('only local time evidence is supported')
        if protocol['other_winnings_assumption'] != 'no_other_same_issue_winnings':
            raise RegistrationError('explicit no-other-winnings tax assumption required for each alternative')
        if not isinstance(protocol['target_issue'], str) or not re.fullmatch(r'\d{5}', protocol['target_issue']):
            raise RegistrationError('target issue required')
        issues = protocol['observation_issues']
        if (not isinstance(issues, list) or not issues
                or any(not isinstance(i, str) or not re.fullmatch(r'\d{5}', i) for i in issues)
                or len(set(issues)) != len(issues)
                or protocol['target_issue'] not in issues):
            raise RegistrationError('distinct observation issue manifest including target required')
        replicas = protocol['replicas']
        if (not isinstance(replicas, list) or not replicas or any(not isinstance(r, dict)
                or not isinstance(r.get('replica'), str) or not r['replica'].strip()
                or not isinstance(r.get('pair_key'), str) or not r['pair_key'].strip() for r in replicas)
                or len({r['replica'] for r in replicas}) != len(replicas)
                or len({r['pair_key'] for r in replicas}) != len(replicas)):
            raise RegistrationError('distinct replica-to-pair manifest required')
        _time(protocol['internal_deadline'])
        try:
            return json.loads(_canonical(protocol))
        except (ValueError, TypeError) as exc:
            raise RegistrationError('protocol must be finite JSON') from exc

    @staticmethod
    def _write_digest(protocol, series_key):
        return hashlib.sha256(_canonical({'protocol': protocol, 'series_key': series_key})).hexdigest()

    def prepare_write(self, request_key, protocol, *, series_key=None, retry_of=None):
        """Persist a queryable intent before a fixed experiment is submitted."""
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
            raise RegistrationError('request key must be 1 to 128 characters')
        if series_key is not None and (not isinstance(series_key, str) or not series_key.strip() or len(series_key) > 128):
            raise RegistrationError('series identifier must be 1 to 128 characters')
        if retry_of is not None and (not isinstance(retry_of, str) or not retry_of.strip()
                                     or len(retry_of) > 128 or retry_of == request_key):
            raise RegistrationError('retry request key is invalid')
        protocol = self._protocol(protocol)
        request = _canonical(protocol)
        digest = self._write_digest(protocol, series_key)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT request_json,content_sha256,content_json FROM fixed_experiments WHERE request_key=?',
                               (request_key,)).fetchone()
            if prior:
                linked = db.execute('SELECT series_key FROM series_periods WHERE experiment_key=?', (request_key,)).fetchone()
                if prior[0] != request or (linked[0] if linked else None) != series_key:
                    raise RegistrationError('request key already used for a different fixed experiment')
                if hashlib.sha256(prior[2]).hexdigest() != prior[1]:
                    raise RegistrationError('fixed experiment integrity failure')
                return {'request_key': request_key, 'request_sha256': digest, 'status': 'committed'}
            intent = db.execute('SELECT request_sha256,series_key,retry_of FROM fixed_experiment_write_intents WHERE request_key=?',
                                (request_key,)).fetchone()
            if intent:
                if intent != (digest, series_key, retry_of):
                    raise RegistrationError('request key already prepared for different input')
                fence = db.execute('SELECT request_sha256 FROM fixed_experiment_write_fences WHERE request_key=?',
                                   (request_key,)).fetchone()
                if fence and fence[0] != digest:
                    raise RegistrationError('request fence digest mismatch')
                return {'request_key': request_key, 'request_sha256': digest,
                        'status': 'fenced' if fence else 'unknown'}
            if retry_of is not None:
                original = db.execute('SELECT request_sha256,series_key FROM fixed_experiment_write_intents WHERE request_key=?',
                                      (retry_of,)).fetchone()
                original_fence = db.execute('SELECT request_sha256 FROM fixed_experiment_write_fences WHERE request_key=?',
                                            (retry_of,)).fetchone()
                if (not original or not original_fence or original[0] != original_fence[0]
                        or original[1] != series_key):
                    raise RegistrationError('retry requires a fenced request for the same experiment scope')
            db.execute('INSERT INTO fixed_experiment_write_intents VALUES (?, ?, ?, ?, ?)',
                       (request_key, digest, series_key, retry_of, self._now().isoformat()))
        return {'request_key': request_key, 'request_sha256': digest, 'status': 'unknown'}

    def write_status(self, request_key, request_sha256):
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
            raise RegistrationError('request key must be 1 to 128 characters')
        if not isinstance(request_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', request_sha256):
            raise RegistrationError('request digest is invalid')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            intent = db.execute('SELECT request_sha256 FROM fixed_experiment_write_intents WHERE request_key=?',
                                (request_key,)).fetchone()
            fence = db.execute('SELECT request_sha256 FROM fixed_experiment_write_fences WHERE request_key=?',
                               (request_key,)).fetchone()
            prior = db.execute('SELECT request_json,content_sha256,content_json FROM fixed_experiments WHERE request_key=?',
                               (request_key,)).fetchone()
            if (intent and intent[0] != request_sha256) or (fence and fence[0] != request_sha256):
                raise RegistrationError('request digest does not match stored intent')
            if prior:
                if hashlib.sha256(prior[2]).hexdigest() != prior[1]:
                    raise RegistrationError('fixed experiment integrity failure')
                protocol = json.loads(prior[0])
                linked = db.execute('SELECT series_key FROM series_periods WHERE experiment_key=?', (request_key,)).fetchone()
                if self._write_digest(protocol, linked[0] if linked else None) != request_sha256:
                    raise RegistrationError('request digest does not match committed experiment')
                return {'request_key': request_key, 'request_sha256': request_sha256, 'status': 'committed'}
            if fence:
                return {'request_key': request_key, 'request_sha256': request_sha256, 'status': 'fenced'}
            return {'request_key': request_key, 'request_sha256': request_sha256, 'status': 'unknown'}

    def fence_write(self, request_key, request_sha256):
        """Atomically fence a prepared request against fixed experiment commit."""
        state = self.write_status(request_key, request_sha256)
        if state['status'] == 'committed':
            return state
        if state['status'] == 'unprepared':
            raise RegistrationError('cannot fence a request without a durable preparation')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT request_json,content_sha256,content_json FROM fixed_experiments WHERE request_key=?',
                               (request_key,)).fetchone()
            if prior:
                if hashlib.sha256(prior[2]).hexdigest() != prior[1]:
                    raise RegistrationError('fixed experiment integrity failure')
                protocol = json.loads(prior[0])
                linked = db.execute('SELECT series_key FROM series_periods WHERE experiment_key=?', (request_key,)).fetchone()
                if self._write_digest(protocol, linked[0] if linked else None) != request_sha256:
                    raise RegistrationError('request digest does not match committed experiment')
                return {'request_key': request_key, 'request_sha256': request_sha256, 'status': 'committed'}
            intent = db.execute('SELECT request_sha256 FROM fixed_experiment_write_intents WHERE request_key=?',
                                (request_key,)).fetchone()
            if not intent or intent[0] != request_sha256:
                raise RegistrationError('request preparation changed; cannot fence')
            fence = db.execute('SELECT request_sha256 FROM fixed_experiment_write_fences WHERE request_key=?',
                               (request_key,)).fetchone()
            if fence:
                if fence[0] != request_sha256:
                    raise RegistrationError('request fence digest mismatch')
                return {'request_key': request_key, 'request_sha256': request_sha256, 'status': 'fenced'}
            db.execute('INSERT INTO fixed_experiment_write_fences VALUES (?, ?, ?)',
                       (request_key, request_sha256, self._now().isoformat()))
        return {'request_key': request_key, 'request_sha256': request_sha256, 'status': 'fenced'}

    def get(self, request_key):
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT content_sha256, content_json FROM fixed_experiments WHERE request_key=?',
                             (request_key,)).fetchone()
        if row is None:
            raise KeyError(request_key)
        if hashlib.sha256(row[1]).hexdigest() != row[0]:
            raise RegistrationError('fixed experiment integrity failure')
        return json.loads(row[1])

    @exact_money_context
    def freeze(self, request_key, protocol, *, series_key=None, transaction_guard=None, transaction_after_insert=None,
               require_intent=False):
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
            raise RegistrationError('request key must be 1 to 128 characters')
        if series_key is not None and (not isinstance(series_key, str) or not series_key.strip() or len(series_key) > 128):
            raise RegistrationError('series identifier must be 1 to 128 characters')
        protocol = self._protocol(protocol)
        request = _canonical(protocol)
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT request_json, content_sha256, content_json FROM fixed_experiments WHERE request_key=?',
                               (request_key,)).fetchone()
            if prior:
                if prior[0] != request:
                    raise RegistrationError('request key already used for different protocol')
                if hashlib.sha256(prior[2]).hexdigest() != prior[1]:
                    raise RegistrationError('fixed experiment integrity failure')
                linked = db.execute('SELECT series_key,issue FROM series_periods WHERE experiment_key=?',
                                    (request_key,)).fetchone()
                if series_key is None and linked is not None:
                    raise RegistrationError('fixed experiment is linked to a series')
                if series_key is not None and linked != (series_key, protocol['target_issue']):
                    raise RegistrationError('fixed experiment is not linked to this series')
                return json.loads(prior[2])
            if require_intent:
                digest = self._write_digest(protocol, series_key)
                intent = db.execute('SELECT request_sha256,series_key FROM fixed_experiment_write_intents WHERE request_key=?',
                                    (request_key,)).fetchone()
                if not intent or intent != (digest, series_key):
                    raise RegistrationError('fixed experiment must use its matching prepared request')
                if db.execute('SELECT 1 FROM fixed_experiment_write_fences WHERE request_key=?',
                              (request_key,)).fetchone():
                    raise RegistrationError('fixed experiment request has been fenced')
            calendar = self._get_review(db, protocol['calendar_review_id'], 'calendar')
            rule_review = self._get_review(db, protocol['rule_review_id'], 'rule')
            tax_review = self._get_review(db, protocol['tax_review_id'], 'tax')
            for kind in ('calendar', 'rule', 'tax'):
                ident = protocol[kind+'_review_id']
                if self._effective_review(db, ident, kind, issue=protocol['target_issue'])[0] != ident:
                    raise RegistrationError(f'{kind} review has been superseded; choose current review')
            cal, rule = calendar['extracted'], rule_review['extracted']
            target = protocol['target_issue']
            if cal.get('issue') != target or not cal.get('version') or not rule.get('version'):
                raise RegistrationError('reviewed calendar or rule does not identify target and version')
            if rule.get('source') != rule_review['source_url']:
                raise RegistrationError('reviewed rule source does not match retained document source')
            if (tax_review['extracted'].get('tax') != rule.get('tax')
                    or target not in tax_review['extracted'].get('applicable_issues', [])):
                raise RegistrationError('tax review does not cover rule and target')
            deadline, internal = _time(cal.get('cutoff_at')), _time(protocol['internal_deadline'])
            now = self._now()
            if now >= deadline or now >= internal:
                raise RegistrationError('registration at or after deadline is not eligible')
            if any(_time(item['reviewed_at']) > now for item in (calendar, rule_review, tax_review)):
                raise RegistrationError('source review time is later than local clock')
            if deadline.astimezone(timezone(timedelta(hours=8))).date().isoformat() != cal.get('draw_date'):
                raise RegistrationError('reviewed deadline does not match draw date')
            pairs, reference, model_reference, used_hashes, used_seeds = [], None, None, set(), set()
            for manifest in protocol['replicas']:
                try:
                    pair = self.pairs._checked_pair(db, manifest['pair_key'], require_replay=True)
                except (CandidateError, KeyError) as exc:
                    raise RegistrationError(f'pair unavailable or needs review: {exc}') from exc
                cfg = pair['config']
                if (cfg['target_issue'] != target or cfg['target_date'] != cal['draw_date']
                        or cfg['calendar_version'] != cal['version'] or cfg['rule_version'] != rule['version']):
                    raise RegistrationError('pair target, calendar or rule version mismatch')
                shared = {k: v for k, v in cfg.items() if k != 'seed'}
                if reference is not None and reference != shared:
                    raise RegistrationError('replicas must share method, data, budget and structure')
                reference = shared
                if pair['pair_sha256'] in used_hashes or cfg['seed'] in used_seeds:
                    raise RegistrationError('replicas must use distinct pairs and seeds')
                used_hashes.add(pair['pair_sha256']); used_seeds.add(cfg['seed'])
                if pair['candidate']['cost_fen'] != pair['control']['cost_fen']:
                    raise RegistrationError('candidate/control costs do not match')
                created = db.execute('SELECT created_at FROM draft_pairs WHERE request_key=?', (manifest['pair_key'],)).fetchone()
                if not created or _time(created[0]) > now:
                    raise RegistrationError('pair creation time is later than local clock')
                source_has_target = db.execute('SELECT 1 FROM draw_versions WHERE issue=?', (target,)).fetchone()
                observations = db.execute('SELECT observed_at FROM observations WHERE snapshot_id=?', (cfg['snapshot_id'],)).fetchall()
                if source_has_target or not observations or not any(_time(x[0]) <= _time(created[0]) for x in observations):
                    raise RegistrationError('snapshot timing or target outcome is incompatible with registration')
                if pair['model_artifact_id']:
                    model_row = db.execute('SELECT first_recorded_at FROM model_artifacts WHERE artifact_id=?',
                                           (pair['model_artifact_id'],)).fetchone()
                    if not model_row or _time(model_row[0]) > _time(created[0]):
                        raise RegistrationError('model artifact was recorded after the pair')
                    model = ModelRepository(self.path).get(pair['model_artifact_id'])
                    model_spec = {'config': {k: v for k, v in model['config'].items() if k != 'seed'},
                                  'algorithm_version': model['algorithm_version'],
                                  'training_rows_sha256': model['training_rows_sha256'],
                                  'code_sha256': model['code_sha256']}
                    if model_reference is not None and model_reference != model_spec:
                        raise RegistrationError('replicas must share model configuration and training history')
                    model_reference = model_spec
                if target not in rule.get('applicable_issues', []):
                    raise RegistrationError('reviewed rule does not apply to target')
                try:
                    validate_dlt_2026_rule_mapping(target, rule)
                except ValueError as exc:
                    raise RegistrationError(str(exc)) from exc
                # Validate the entire payout/tax mapping with synthetic zeros;
                # actual winnings are never inferred at registration time.
                groups = rule.get('hit_groups', {})
                extras = rule.get('additional_groups', {})
                if not isinstance(groups, dict) or not isinstance(extras, dict):
                    raise RegistrationError('incomplete reviewed prize mapping')
                zeros = {g: '0' for g in list(groups.values()) + list(extras.values()) if isinstance(g, str)}
                try:
                    checks = [settle_portfolio(issue=target, draw={'issue': target, 'front': [1, 2, 3, 4, 5],
                                              'back': [1, 2], 'promotion': False}, tickets=pair[label]['tickets'],
                                              payouts=zeros, rule=rule, duplicate_policy='reject')
                              for label in ('candidate', 'control')]
                    if (any(Decimal(check['cost'])*100 != pair[label]['cost_fen']
                            for check, label in zip(checks, ('candidate', 'control')))
                            or Decimal(str(rule['prices']['base']))*100 != cfg['base_price_fen']
                            or Decimal(str(rule['prices']['additional']))*100 != cfg['additional_price_fen']):
                        raise RegistrationError('reviewed prices do not match pair cost')
                except (SettlementBlocked, InvalidOperation, KeyError, TypeError) as exc:
                    raise RegistrationError(f'incomplete reviewed rule: {exc}') from exc
                pairs.append({'replica': manifest['replica'], 'pair_key': manifest['pair_key'], 'pair': pair})
            if series_key is not None:
                from dlt.prospective_series import validate_period
                validate_period(db, series_key, protocol, pairs)
            # Recheck both deadlines immediately before publication under the
            # same SQLite write lock; local time is not a trusted timestamp.
            registered = self._now()
            if registered < now or registered >= deadline or registered >= internal:
                raise RegistrationError('clock changed or deadline reached before commit')
            # Optional internal callers can tie an automatic registration to a
            # live, explicitly authorised plan under the same write lock.
            if transaction_guard is not None:
                transaction_guard(db, registered)
                final_time = self._now()
                if final_time < registered or final_time >= deadline or final_time >= internal:
                    raise RegistrationError('clock changed or deadline reached before commit')
                registered = final_time
            content = {'protocol': protocol, 'pairs': pairs, 'calendar_review': calendar, 'rule_review': rule_review,
                       'tax_review': tax_review,
                       'registered_at': registered.isoformat(), 'status': 'locally_attested',
                       'time_evidence': 'local clock only; not independently certified',
                       'source_evidence': 'personal review; document origin not independently authenticated'}
            payload = _canonical(content)
            db.execute('INSERT INTO fixed_experiments VALUES (?, ?, ?, ?)',
                       (request_key, request, hashlib.sha256(payload).hexdigest(), payload))
            if series_key is not None:
                db.execute('INSERT INTO series_periods VALUES (?, ?, ?)', (series_key, target, request_key))
            if transaction_after_insert is not None:
                transaction_after_insert(db, content)
        return content

    def inspect(self, request_key):
        content = self.get(request_key)
        try:
            with closing(sqlite3.connect(self.path)) as db:
                from dlt.fixed_contract import validate_fixed_record
                db.execute('BEGIN')
                fixed = db.execute('SELECT request_json,content_sha256,content_json FROM fixed_experiments WHERE request_key=?',
                                   (request_key,)).fetchone()
                if not fixed or validate_fixed_record(db, *fixed) != content:
                    raise RegistrationError('固定实验记录不一致，需要复核')
                calendar_id, calendar, _ = self._calendar_for_fixed(db, content)
                self._get_review(db, content['protocol']['rule_review_id'], 'rule')
                self._get_review(db, content['protocol']['tax_review_id'], 'tax')
                for replica in content['pairs']:
                    current = self.pairs._checked_pair(db, replica['pair_key'])
                    if current['pair_sha256'] != replica['pair']['pair_sha256']:
                        raise RegistrationError('fixed pair reference does not match historical record')
                closure = db.execute('SELECT closed_at, reason FROM experiment_closures WHERE experiment_key=?',
                                     (request_key,)).fetchone()
        except (CandidateError, KeyError, RegistrationError, ValueError) as exc:
            return {'content': content, 'status': 'review_required', 'reason': str(exc)}
        return {'content': content, 'status': 'closed' if closure else 'locally_attested',
                'closure': {'closed_at': closure[0], 'reason': closure[1]} if closure else None,
                'effective_calendar': {'review_id': calendar_id, 'draw_date': calendar['extracted']['draw_date']},
                'reason': 'source and timestamp are not independently authenticated'}

    def close(self, request_key, reason, *, write_key=None, retry_of=None):
        if not isinstance(reason, str) or not reason.strip() or len(reason.strip()) > 1000:
            raise RegistrationError('closure reason required')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM fixed_experiments WHERE request_key=?', (request_key,)).fetchone():
                raise KeyError(request_key)
            if write_key is None:
                existing = db.execute('SELECT closed_at, reason FROM experiment_closures WHERE experiment_key=?',
                                      (request_key,)).fetchone()
                if existing:
                    return {'closed_at': existing[0], 'reason': existing[1]}
                now = self._now().isoformat()
                db.execute('INSERT INTO experiment_closures VALUES (?, ?, ?)', (request_key, now, reason.strip()))
                return {'closed_at': now, 'reason': reason.strip()}
            from dlt.prospective_closure_writes import accept
            return accept(db, write_key, 'experiment_close', {'object_key': request_key, 'reason': reason}, retry_of,
                          closed_at=lambda: self._now().isoformat(),
                          existing=lambda: db.execute('SELECT closed_at,reason FROM experiment_closures WHERE experiment_key=?',
                                                      (request_key,)).fetchone(),
                          insert=lambda result: db.execute('INSERT INTO experiment_closures VALUES (?,?,?)',
                                                          (request_key, result['closed_at'], result['reason'])))
