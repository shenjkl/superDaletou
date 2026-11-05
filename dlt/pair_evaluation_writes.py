"""Durable request, fence, and direct-retry records for pair/evaluation writes."""
from datetime import datetime, timezone
import hashlib
import json
import re

from dlt.candidates import _canonical


SPECS = {
    'pair_create': ('pair_write_requests', 'pair_write_fences', 'pair_write_retries', 'draft_pairs'),
    'evaluation': ('evaluation_write_requests', 'evaluation_write_fences', 'evaluation_write_retries', 'draft_evaluations'),
    'selection_decision': ('selection_write_requests', 'selection_write_fences', 'selection_write_retries', 'model_selection_decisions'),
}
UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', re.I)


def initialize(db, *, pair=False, evaluation=False):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS pair_write_requests (
            request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL,
            result_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pair_write_fences (
            request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL,
            request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pair_write_retries (
            request_key TEXT PRIMARY KEY,
            retry_of TEXT NOT NULL UNIQUE REFERENCES pair_write_fences(request_key),
            request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS evaluation_write_requests (
            request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL,
            result_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS evaluation_write_fences (
            request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL,
            request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS evaluation_write_retries (
            request_key TEXT PRIMARY KEY,
            retry_of TEXT NOT NULL UNIQUE REFERENCES evaluation_write_fences(request_key),
            request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS selection_write_requests (
            request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL,
            result_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS selection_write_fences (
            request_key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL,
            request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS selection_write_retries (
            request_key TEXT PRIMARY KEY,
            retry_of TEXT NOT NULL UNIQUE REFERENCES selection_write_fences(request_key),
            request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS pair_write_requests_no_update BEFORE UPDATE ON pair_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable pair write'); END;
        CREATE TRIGGER IF NOT EXISTS pair_write_requests_no_delete BEFORE DELETE ON pair_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable pair write'); END;
        CREATE TRIGGER IF NOT EXISTS pair_write_fences_no_update BEFORE UPDATE ON pair_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable pair fence'); END;
        CREATE TRIGGER IF NOT EXISTS pair_write_fences_no_delete BEFORE DELETE ON pair_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable pair fence'); END;
        CREATE TRIGGER IF NOT EXISTS pair_write_retries_no_update BEFORE UPDATE ON pair_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable pair retry'); END;
        CREATE TRIGGER IF NOT EXISTS pair_write_retries_no_delete BEFORE DELETE ON pair_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable pair retry'); END;
        CREATE TRIGGER IF NOT EXISTS pair_write_requests_no_fenced_insert BEFORE INSERT ON pair_write_requests
            WHEN EXISTS (SELECT 1 FROM pair_write_fences WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'fenced pair write'); END;
        CREATE TRIGGER IF NOT EXISTS pair_write_fences_no_accepted_insert BEFORE INSERT ON pair_write_fences
            WHEN EXISTS (SELECT 1 FROM pair_write_requests WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'accepted pair write'); END;
        CREATE TRIGGER IF NOT EXISTS evaluation_write_requests_no_update BEFORE UPDATE ON evaluation_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable evaluation write'); END;
        CREATE TRIGGER IF NOT EXISTS evaluation_write_requests_no_delete BEFORE DELETE ON evaluation_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable evaluation write'); END;
        CREATE TRIGGER IF NOT EXISTS evaluation_write_fences_no_update BEFORE UPDATE ON evaluation_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable evaluation fence'); END;
        CREATE TRIGGER IF NOT EXISTS evaluation_write_fences_no_delete BEFORE DELETE ON evaluation_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable evaluation fence'); END;
        CREATE TRIGGER IF NOT EXISTS evaluation_write_retries_no_update BEFORE UPDATE ON evaluation_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable evaluation retry'); END;
        CREATE TRIGGER IF NOT EXISTS evaluation_write_retries_no_delete BEFORE DELETE ON evaluation_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable evaluation retry'); END;
        CREATE TRIGGER IF NOT EXISTS evaluation_write_requests_no_fenced_insert BEFORE INSERT ON evaluation_write_requests
            WHEN EXISTS (SELECT 1 FROM evaluation_write_fences WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'fenced evaluation write'); END;
        CREATE TRIGGER IF NOT EXISTS evaluation_write_fences_no_accepted_insert BEFORE INSERT ON evaluation_write_fences
            WHEN EXISTS (SELECT 1 FROM evaluation_write_requests WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'accepted evaluation write'); END;
        CREATE TRIGGER IF NOT EXISTS selection_write_requests_no_update BEFORE UPDATE ON selection_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable selection write'); END;
        CREATE TRIGGER IF NOT EXISTS selection_write_requests_no_delete BEFORE DELETE ON selection_write_requests
            BEGIN SELECT RAISE(ABORT, 'immutable selection write'); END;
        CREATE TRIGGER IF NOT EXISTS selection_write_fences_no_update BEFORE UPDATE ON selection_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable selection fence'); END;
        CREATE TRIGGER IF NOT EXISTS selection_write_fences_no_delete BEFORE DELETE ON selection_write_fences
            BEGIN SELECT RAISE(ABORT, 'immutable selection fence'); END;
        CREATE TRIGGER IF NOT EXISTS selection_write_retries_no_update BEFORE UPDATE ON selection_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable selection retry'); END;
        CREATE TRIGGER IF NOT EXISTS selection_write_retries_no_delete BEFORE DELETE ON selection_write_retries
            BEGIN SELECT RAISE(ABORT, 'immutable selection retry'); END;
        CREATE TRIGGER IF NOT EXISTS selection_write_requests_no_fenced_insert BEFORE INSERT ON selection_write_requests
            WHEN EXISTS (SELECT 1 FROM selection_write_fences WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'fenced selection write'); END;
        CREATE TRIGGER IF NOT EXISTS selection_write_fences_no_accepted_insert BEFORE INSERT ON selection_write_fences
            WHEN EXISTS (SELECT 1 FROM selection_write_requests WHERE request_key=NEW.request_key)
            BEGIN SELECT RAISE(ABORT, 'accepted selection write'); END;
    ''')
    if pair:
        db.executescript('''
            CREATE TRIGGER IF NOT EXISTS draft_pairs_no_fenced_insert BEFORE INSERT ON draft_pairs
                WHEN EXISTS (SELECT 1 FROM pair_write_fences WHERE request_key=NEW.request_key)
                BEGIN SELECT RAISE(ABORT, 'fenced pair write'); END;
        ''')
    if evaluation:
        db.executescript('''
            CREATE TRIGGER IF NOT EXISTS draft_evaluations_no_fenced_insert BEFORE INSERT ON draft_evaluations
                WHEN EXISTS (SELECT 1 FROM evaluation_write_fences WHERE request_key=NEW.request_key)
                BEGIN SELECT RAISE(ABORT, 'fenced evaluation write'); END;
        ''')


def identity(request):
    if not isinstance(request, dict):
        raise ValueError('write request must be an object')
    raw = _canonical(request)
    return raw, hashlib.sha256(raw).hexdigest()


def _register_retry(db, operation, key, raw, digest, retry_of, now):
    requests, fences, retries, _ = SPECS[operation]
    prior = db.execute(f'SELECT retry_of,request_sha256,request_json FROM {retries} WHERE request_key=?', (key,)).fetchone()
    if retry_of is None:
        if prior:
            raise ValueError('write retry must retain its recorded association')
        return
    if not isinstance(retry_of, str) or not UUID.fullmatch(retry_of) or retry_of == key:
        raise ValueError('write retry association is invalid')
    original = db.execute(f'SELECT request_sha256,request_json FROM {fences} WHERE request_key=?', (retry_of,)).fetchone()
    if original != (digest, raw):
        raise ValueError('write retry must match an original fenced request')
    if (db.execute(f'SELECT 1 FROM {retries} WHERE retry_of=?', (retry_of,)).fetchone()
            or db.execute(f'SELECT 1 FROM {retries} WHERE request_key=?', (retry_of,)).fetchone()):
        raise ValueError('only one direct retry of the original fence is allowed')
    wanted = (retry_of, digest, raw)
    if prior:
        if prior != wanted:
            raise ValueError('write retry key is already associated with different input')
        return
    db.execute(f'INSERT INTO {retries} VALUES (?,?,?,?,?)', (key, retry_of, digest, raw, now))


def record_acceptance(db, operation, key, request, result, *, retry_of=None):
    requests, fences, retries, business = SPECS[operation]
    if not isinstance(key, str) or not key.strip() or len(key) > 128:
        raise ValueError('write request key must be 1 to 128 characters')
    if not isinstance(key, str) or not key.strip() or len(key) > 128:
        raise ValueError('write request key must be 1 to 128 characters')
    if not isinstance(key, str) or not key.strip() or len(key) > 128:
        raise ValueError('write request key must be 1 to 128 characters')
    raw, digest = identity(request)
    prior = db.execute(f'SELECT request_sha256,request_json,result_json FROM {requests} WHERE request_key=?', (key,)).fetchone()
    fence = db.execute(f'SELECT request_sha256,request_json FROM {fences} WHERE request_key=?', (key,)).fetchone()
    if prior:
        if prior[:2] != (digest, raw):
            raise ValueError('request key already used for different input')
        retry = db.execute(f'SELECT retry_of FROM {retries} WHERE request_key=?', (key,)).fetchone()
        if (retry[0] if retry else None) != retry_of:
            raise ValueError('write retry association differs from accepted request')
        return json.loads(prior[2])
    if fence:
        raise ValueError('write request was fenced; late submission rejected')
    now = datetime.now(timezone.utc).isoformat()
    _register_retry(db, operation, key, raw, digest, retry_of, now)
    result_raw = _canonical(result)
    db.execute(f'INSERT INTO {requests} VALUES (?,?,?,?,?)', (key, digest, raw, result_raw, now))
    return result


def fence(db, operation, key, request, *, retry_of=None):
    if not isinstance(key, str) or not key.strip() or len(key) > 128:
        raise ValueError('write request key must be 1 to 128 characters')
    requests, fences, retries, _ = SPECS[operation]
    raw, digest = identity(request)
    accepted = db.execute(f'SELECT request_sha256,request_json,result_json FROM {requests} WHERE request_key=?', (key,)).fetchone()
    prior_fence = db.execute(f'SELECT request_sha256,request_json FROM {fences} WHERE request_key=?', (key,)).fetchone()
    if accepted and accepted[:2] != (digest, raw):
        raise ValueError('request key already accepted for different input')
    if prior_fence and prior_fence != (digest, raw):
        raise ValueError('request key fenced for different input')
    retry = db.execute(f'SELECT retry_of FROM {retries} WHERE request_key=?', (key,)).fetchone()
    if accepted or prior_fence:
        if (retry[0] if retry else None) != retry_of:
            raise ValueError('write retry association differs from recorded request')
    else:
        now = datetime.now(timezone.utc).isoformat()
        _register_retry(db, operation, key, raw, digest, retry_of, now)
        db.execute(f'INSERT INTO {fences} VALUES (?,?,?,?)', (key, digest, raw, now))
    return status_in_transaction(db, operation, key)


def status_in_transaction(db, operation, key):
    if operation not in SPECS:
        raise ValueError('unknown write operation')
    if not isinstance(key, str) or not key.strip() or len(key) > 128:
        raise ValueError('write request key is invalid')
    requests, fences, retries, _ = SPECS[operation]
    accepted = db.execute(f'SELECT request_sha256,request_json,result_json FROM {requests} WHERE request_key=?', (key,)).fetchone()
    fenced = db.execute(f'SELECT request_sha256,request_json FROM {fences} WHERE request_key=?', (key,)).fetchone()
    retry = db.execute(f'SELECT retry_of FROM {retries} WHERE request_key=?', (key,)).fetchone()
    if operation == 'selection_decision':
        business = db.execute('SELECT 1 FROM model_selection_decisions WHERE request_key=?', (key,)).fetchone()
        if business and not accepted:
            raise ValueError('model selection decision has no accepted write receipt')
    if accepted and fenced:
        raise ValueError('write request is both accepted and fenced')
    if retry:
        if not accepted and not fenced:
            raise ValueError('write retry record has no accepted or fenced request')
        retry_row = db.execute(f'SELECT retry_of,request_sha256,request_json FROM {retries} WHERE request_key=?',
                               (key,)).fetchone()
        retry_of, retry_digest, retry_raw = retry_row
        original = db.execute(f'SELECT request_sha256,request_json FROM {fences} WHERE request_key=?',
                              (retry_of,)).fetchone()
        try:
            retry_valid = (isinstance(retry_raw, bytes)
                           and retry_digest == hashlib.sha256(retry_raw).hexdigest()
                           and _canonical(json.loads(retry_raw)) == retry_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            retry_valid = False
        if (retry_of == key or not retry_valid
                or not original or original != (retry_digest, retry_raw)):
            raise ValueError('write retry record does not match its original fence')
        if accepted and accepted[:2] != (retry_digest, retry_raw):
            raise ValueError('accepted write differs from its retry record')
        if fenced and fenced != (retry_digest, retry_raw):
            raise ValueError('fenced write differs from its retry record')
    child = db.execute(f'SELECT request_key,retry_of,request_sha256,request_json FROM {retries} WHERE retry_of=?',
                       (key,)).fetchone()
    if child:
        child_key, child_of, child_digest, child_raw = child
        child_accepted = db.execute(f'SELECT request_sha256,request_json FROM {requests} WHERE request_key=?',
                                    (child_key,)).fetchone()
        child_fenced = db.execute(f'SELECT request_sha256,request_json FROM {fences} WHERE request_key=?',
                                  (child_key,)).fetchone()
        try:
            child_valid = (isinstance(child_raw, bytes) and child_digest == hashlib.sha256(child_raw).hexdigest()
                           and _canonical(json.loads(child_raw)) == child_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            child_valid = False
        if (accepted or not fenced or child_of != key or not child_valid
                or (child_accepted is None) == (child_fenced is None)
                or (child_accepted or child_fenced) != (child_digest, child_raw)
                or fenced != (child_digest, child_raw)):
            raise ValueError('write retry association is orphaned or inconsistent')
    if accepted:
        digest, request_raw, result_raw = accepted
        try:
            request_value, result_value = json.loads(request_raw), json.loads(result_raw)
            if (_canonical(request_value) != request_raw or hashlib.sha256(request_raw).hexdigest() != digest
                    or not isinstance(request_value, dict) or not isinstance(result_value, dict)
                    or _canonical(result_value) != result_raw):
                raise ValueError
            if operation == 'pair_create':
                row = db.execute('SELECT request_json,pair_sha256,pair_json FROM draft_pairs WHERE request_key=?',
                                 (key,)).fetchone()
                if not row:
                    raise ValueError
                business_request = json.loads(row[0])
                pair = json.loads(row[2])
                pair_content = dict(pair)
                pair_digest = pair_content.pop('pair_sha256', None)
                if not isinstance(business_request, dict):
                    raise ValueError
                expected_request = {'config': business_request['config'],
                                    'history_first_issue': business_request['history_first_issue'],
                                    'history_cutoff_issue': business_request['history_cutoff_issue'],
                                    'artifact_id': business_request.get('artifact_id'),
                                    'prediction_at': business_request.get('prediction_at'),
                                    'target_deadline': business_request.get('target_deadline')}
                if business_request.get('model_selection') is not None:
                    expected_request['model_selection'] = business_request['model_selection']
                if (_canonical(business_request) != row[0] or request_value != expected_request
                        or pair_digest != row[1] or hashlib.sha256(_canonical(pair_content)).hexdigest() != pair_digest
                        or result_value.get('request_key') != key or result_value.get('request') != request_value
                        or result_value.get('pair') != pair):
                    raise ValueError
            elif operation == 'evaluation':
                row = db.execute('SELECT pair_key,request_json,result_sha256,result_json FROM draft_evaluations WHERE request_key=?',
                                 (key,)).fetchone()
                if not row:
                    raise ValueError
                business_request = json.loads(row[1])
                business_result = json.loads(row[3])
                scenario = business_request.get('scenario', {})
                dependency = scenario.get('source_dependency', {}) if isinstance(scenario, dict) else {}
                if scenario.get('kind') == 'custom_payout_v1':
                    expected_request = {'pair_key': business_request['pair_key'], 'scenario_kind': 'custom_payout_v1',
                                        'custom_input': scenario['custom_input'], 'samples': business_request['samples'],
                                        'seed': business_request['seed']}
                else:
                    expected_request = {'pair_key': business_request['pair_key'],
                                        'scenario_snapshot_id': dependency.get('snapshot_id'),
                                        'scenario_issue': dependency.get('issue'),
                                        'samples': business_request['samples'], 'seed': business_request['seed']}
                if (hashlib.sha256(row[3]).hexdigest() != row[2] or _canonical(business_request) != row[1]
                        or business_request.get('pair_key') != row[0]
                        or result_value.get('request_key') != key or result_value.get('request') != business_request
                        or result_value.get('result') != business_result or request_value != expected_request):
                    raise ValueError
            else:
                row = db.execute('SELECT pair_key,seq,previous_seq,selection_sha256,selection_json '
                                 'FROM model_selection_decisions WHERE request_key=?', (key,)).fetchone()
                if not row:
                    raise ValueError
                selection = json.loads(row[4])
                if (not isinstance(selection, dict) or hashlib.sha256(row[4]).hexdigest() != row[3]
                        or _canonical(selection) != row[4]
                        or request_value != {'pair_key': row[0], 'selection': selection,
                                             'expected_previous_seq': row[2]}
                        or result_value.get('request_key') != key
                        or result_value.get('request') != request_value
                        or result_value.get('decision_seq') != row[1]
                        or result_value.get('selection_sha256') != row[3]):
                    raise ValueError
        except (TypeError, KeyError, ValueError, AttributeError, json.JSONDecodeError) as exc:
            raise ValueError('accepted write receipt does not match its durable business record') from exc
        return {'status': 'accepted', 'request_key': key, 'request_sha256': accepted[0],
                'request': json.loads(accepted[1]), 'result': json.loads(accepted[2]),
                'retry_of': retry[0] if retry else None}
    if fenced:
        try:
            request_value = json.loads(fenced[1])
            if (_canonical(request_value) != fenced[1] or hashlib.sha256(fenced[1]).hexdigest() != fenced[0]
                    or not isinstance(request_value, dict)):
                raise ValueError
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError('fenced write record is invalid') from exc
        return {'status': 'fenced', 'request_key': key, 'request_sha256': fenced[0],
                'request': json.loads(fenced[1]), 'retry_of': retry[0] if retry else None}
    return {'status': 'unknown', 'request_key': key}
