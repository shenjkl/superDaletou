"""Append-only, paired paper results for locally attested experiments.

Inputs remain personal/local evidence. No claim of official source authentication,
actual purchase, or complete tax situation is made by this module.
"""
from contextlib import closing
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import sqlite3
import platform
import re
import uuid
from urllib.parse import parse_qs, urlsplit

from dlt.candidates import CandidateError, _canonical
from dlt.official import BASE
from dlt.prize_rules import validate_dlt_2026_rule_mapping
from dlt.scenarios import ScenarioError
from dlt.prospective import ProspectiveRepository, RegistrationError, _time
from dlt.settlement import SettlementBlocked, exact_money_context, settle_portfolio, require_tax_unit
from dlt.snapshots import InvalidSnapshot, parse_snapshot
from dlt import settlement as settlement_module


class PaperResultError(ValueError):
    pass


class CorrectionLeaseExpired(RuntimeError):
    """A correction worker no longer owns the right to publish results."""


def _sha(value):
    return hashlib.sha256(value).hexdigest()


@exact_money_context
def _calculate_results(content, issue, raw, rule):
    """Reconstruct every fixed pair from the retained draw and reviewed rule."""
    try:
        validate_dlt_2026_rule_mapping(issue, rule)
    except ValueError as exc:
        raise PaperResultError(str(exc)) from exc
    payouts = PaperResults._payouts(rule, raw)
    from dlt.historical_scenarios import promotion_data
    try:
        promotion, promotion_payouts = promotion_data(raw, issue)
    except ScenarioError as exc:
        if str(exc) == '派奖标记与已核对的 2026 活动期号范围不一致':
            raise PaperResultError('promotion eligibility does not match the verified activity issue range') from exc
        raise PaperResultError(str(exc)) from exc
    if promotion and rule.get('promotion_policy') != 'official_draw_fields_and_ticket_group_threshold':
        raise PaperResultError('reviewed rule does not declare the active promotion ticket-group policy')
    if not promotion and rule.get('promotion_policy') not in (None, 'excluded'):
        raise PaperResultError('reviewed rule promotion policy does not match the draw')
    numbers = [int(n) for n in raw['lotteryDrawResult'].split()]
    draw = {'issue': issue, 'front': numbers[:5], 'back': numbers[5:], 'promotion': promotion}
    results = []
    for item in content['pairs']:
        pair = item['pair']
        settled = [settle_portfolio(issue=issue, draw=draw, tickets=pair[label]['tickets'], payouts=payouts,
                                    rule=rule, duplicate_policy='reject',
                                    ticket_groups=pair[label].get('ticket_groups'),
                                    promotion_payouts=promotion_payouts if promotion else None)
                   for label in ('candidate', 'control')]
        if any(Decimal(result['cost'])*100 != pair[label]['cost_fen']
               for result, label in zip(settled, ('candidate', 'control'))):
            raise PaperResultError('result cost differs from fixed pair')
        results.append({'replica': item['replica'], 'pair_sha256': pair['pair_sha256'],
                        'candidate': settled[0], 'control': settled[1],
                        'difference_yuan': str(Decimal(settled[0]['net'])-Decimal(settled[1]['net']))})
    if not results:
        raise PaperResultError('fixed experiment contains no replicas')
    average = sum((Fraction(Decimal(x['difference_yuan'])) for x in results), Fraction(0))/len(results)
    return payouts, results, [average.numerator, average.denominator]


def _money_fraction(value):
    if not isinstance(value, (str, int)) or isinstance(value, bool) or len(str(value)) > 512:
        raise ValueError('invalid stored amount')
    number = Decimal(str(value).replace(',', ''))
    if not number.is_finite() or abs(number.as_tuple().exponent) > 256:
        raise ValueError('invalid stored amount')
    return Fraction(number)


def _matches_calculation(saved, payouts, results, mean, promotion_payouts=None):
    """Compare mathematical amounts, not incidental decimal formatting."""
    try:
        old_payouts = saved['payouts']
        old_results = saved['replicas']
        if (not isinstance(old_payouts, dict) or set(old_payouts) != set(payouts)
                or any(_money_fraction(old_payouts[k]) != _money_fraction(v) for k, v in payouts.items())
                or not isinstance(old_results, list) or len(old_results) != len(results)
                or saved['mean_difference_fraction'] != mean):
            return False
        old_promotion_payouts = saved.get('promotion_payouts')
        if promotion_payouts:
            if (not isinstance(old_promotion_payouts, dict)
                    or set(old_promotion_payouts) != set(promotion_payouts)
                    or any(_money_fraction(old_promotion_payouts[k]) != _money_fraction(v)
                           for k, v in promotion_payouts.items())):
                return False
        elif old_promotion_payouts not in (None, {}):
            return False
        for old, new in zip(old_results, results):
            if any(old[k] != new[k] for k in ('replica', 'pair_sha256')):
                return False
            if _money_fraction(old['difference_yuan']) != _money_fraction(new['difference_yuan']):
                return False
            for label in ('candidate', 'control'):
                a, b = old[label], new[label]
                if (any(a[k] != b[k] for k in ('issue', 'rule_version', 'winning_tickets'))
                        or any(_money_fraction(a[k]) != _money_fraction(b[k]) for k in ('gross', 'tax', 'cost', 'net'))):
                    return False
                for key in ('promotion_bonus',):
                    if (key in a) != (key in b) or (key in a and _money_fraction(a[key]) != _money_fraction(b[key])):
                        return False
                key = 'promotion_qualified_ticket_groups'
                if (key in a) != (key in b) or (key in a and a[key] != b[key]):
                    return False
        return True
    except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError):
        return False


def _official_url(value):
    try:
        parsed, base = urlsplit(value), urlsplit(BASE)
        query = parse_qs(parsed.query, keep_blank_values=True)
        return (parsed.scheme == base.scheme and parsed.netloc == base.netloc and parsed.path == base.path
                and query.get('gameNo') == ['85'] and query.get('provinceId') == ['0']
                and query.get('isVerify') == ['1'] and parsed.fragment == '')
    except (TypeError, ValueError):
        return False


class PaperResults:
    def __init__(self, path, *, clock=None):
        self.path = Path(path)
        self.experiments = ProspectiveRepository(path, clock=clock)

    def initialize(self):
        self.experiments.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS paper_resolutions (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, issue TEXT NOT NULL, selected_snapshot TEXT NOT NULL,
                    selected_row_sha256 TEXT NOT NULL, variants_sha256 TEXT NOT NULL, note TEXT NOT NULL, reviewed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_groups (
                    id TEXT PRIMARY KEY, experiment_key TEXT NOT NULL REFERENCES fixed_experiments(request_key),
                    issue TEXT NOT NULL, row_sha256 TEXT NOT NULL, content_json BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_current (
                    experiment_key TEXT PRIMARY KEY REFERENCES fixed_experiments(request_key),
                    group_id TEXT NOT NULL REFERENCES paper_groups(id)
                );
                CREATE TABLE IF NOT EXISTS paper_events (
                    id TEXT PRIMARY KEY, experiment_key TEXT NOT NULL, snapshot_id TEXT NOT NULL,
                    status TEXT NOT NULL, reason TEXT NOT NULL, recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_publications (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, experiment_key TEXT NOT NULL,
                    previous_group TEXT, group_id TEXT NOT NULL, snapshot_id TEXT NOT NULL, published_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_write_requests (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, object_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, result_json BLOB,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_write_fences (
                    request_key TEXT PRIMARY KEY, operation TEXT NOT NULL, object_key TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL, request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_write_retries (
                    request_key TEXT PRIMARY KEY, retry_of TEXT NOT NULL UNIQUE REFERENCES paper_write_fences(request_key),
                    operation TEXT NOT NULL, object_key TEXT NOT NULL, request_sha256 TEXT NOT NULL,
                    request_json BLOB NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS paper_write_requests_no_delete BEFORE DELETE ON paper_write_requests
                    BEGIN SELECT RAISE(ABORT, 'immutable paper write receipt'); END;
                CREATE TRIGGER IF NOT EXISTS paper_write_requests_guard_update BEFORE UPDATE ON paper_write_requests
                    WHEN OLD.request_key!=NEW.request_key OR OLD.operation!=NEW.operation OR OLD.object_key!=NEW.object_key
                      OR OLD.request_sha256!=NEW.request_sha256 OR OLD.request_json!=NEW.request_json
                      OR OLD.created_at!=NEW.created_at OR OLD.result_json IS NOT NULL OR NEW.result_json IS NULL
                    BEGIN SELECT RAISE(ABORT, 'immutable paper write receipt'); END;
                CREATE TRIGGER IF NOT EXISTS paper_write_requests_no_fenced_insert BEFORE INSERT ON paper_write_requests
                    WHEN EXISTS (SELECT 1 FROM paper_write_fences WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'fenced paper write'); END;
                CREATE TRIGGER IF NOT EXISTS paper_write_fences_no_update BEFORE UPDATE ON paper_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable paper write fence'); END;
                CREATE TRIGGER IF NOT EXISTS paper_write_fences_no_delete BEFORE DELETE ON paper_write_fences
                    BEGIN SELECT RAISE(ABORT, 'immutable paper write fence'); END;
                CREATE TRIGGER IF NOT EXISTS paper_write_fences_no_committed_insert BEFORE INSERT ON paper_write_fences
                    WHEN EXISTS (SELECT 1 FROM paper_write_requests WHERE request_key=NEW.request_key)
                    BEGIN SELECT RAISE(ABORT, 'committed paper write'); END;
                CREATE TRIGGER IF NOT EXISTS paper_write_retries_no_update BEFORE UPDATE ON paper_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable paper write retry'); END;
                CREATE TRIGGER IF NOT EXISTS paper_write_retries_no_delete BEFORE DELETE ON paper_write_retries
                    BEGIN SELECT RAISE(ABORT, 'immutable paper write retry'); END;
                CREATE TRIGGER IF NOT EXISTS paper_groups_no_update BEFORE UPDATE ON paper_groups
                    BEGIN SELECT RAISE(ABORT, 'immutable paper group'); END;
                CREATE TRIGGER IF NOT EXISTS paper_groups_no_delete BEFORE DELETE ON paper_groups
                    BEGIN SELECT RAISE(ABORT, 'immutable paper group'); END;
                CREATE TRIGGER IF NOT EXISTS paper_resolutions_no_update BEFORE UPDATE ON paper_resolutions
                    BEGIN SELECT RAISE(ABORT, 'immutable resolution'); END;
                CREATE TRIGGER IF NOT EXISTS paper_resolutions_no_delete BEFORE DELETE ON paper_resolutions
                    BEGIN SELECT RAISE(ABORT, 'immutable resolution'); END;
                CREATE TRIGGER IF NOT EXISTS paper_events_no_update BEFORE UPDATE ON paper_events
                    BEGIN SELECT RAISE(ABORT, 'immutable paper event'); END;
                CREATE TRIGGER IF NOT EXISTS paper_events_no_delete BEFORE DELETE ON paper_events
                    BEGIN SELECT RAISE(ABORT, 'immutable paper event'); END;
                CREATE TRIGGER IF NOT EXISTS paper_publications_no_update BEFORE UPDATE ON paper_publications
                    BEGIN SELECT RAISE(ABORT, 'immutable publication'); END;
                CREATE TRIGGER IF NOT EXISTS paper_publications_no_delete BEFORE DELETE ON paper_publications
                    BEGIN SELECT RAISE(ABORT, 'immutable publication'); END;
            ''')

    @staticmethod
    def _variants(db, issue):
        rows = db.execute('SELECT snapshot_id, row_json FROM draw_versions WHERE issue=?', (issue,)).fetchall()
        if any(not isinstance(raw, bytes) for _, raw in rows):
            raise PaperResultError('stored draw record is invalid')
        variants = sorted({_sha(raw) for _, raw in rows})
        return rows, _sha(_canonical(variants)), variants

    @classmethod
    def _selection(cls, db, issue):
        rows, fingerprint, variants = cls._variants(db, issue)
        if not variants:
            raise PaperResultError('target draw is missing')
        if len(variants) == 1:
            return variants[0]
        decision = db.execute('SELECT selected_row_sha256 FROM paper_resolutions WHERE issue=? AND variants_sha256=? '
                              'ORDER BY seq DESC LIMIT 1', (issue, fingerprint)).fetchone()
        if not decision or decision[0] not in variants:
            raise PaperResultError('conflicting draw or prize versions require review')
        return decision[0]

    def inspect_draw_versions(self, issue):
        """Expose the exact variant set and observations for personal review.

        The fingerprint binds a subsequent decision to this exact set, even if
        another source arrives between displaying the page and submitting it.
        Source URL and retained-byte checks do not independently authenticate
        publication time or prove that a document came from the claimed URL.
        """
        if not isinstance(issue, str) or not re.fullmatch(r'\d{5}', issue):
            raise PaperResultError('valid five-digit issue required')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            rows, fingerprint, variants = self._variants(db, issue)
            grouped = {}
            for snapshot_id, raw in rows:
                digest = _sha(raw)
                try:
                    row_data = json.loads(raw)
                except (ValueError, TypeError) as exc:
                    raise PaperResultError('stored draw record is invalid') from exc
                if not isinstance(row_data, dict):
                    raise PaperResultError('stored draw record is invalid')
                entry = grouped.setdefault(digest, {'row_sha256': digest, 'row': row_data, 'snapshots': []})
                observations = []
                for source, observed_at, raw_sha, payload in db.execute(
                        'SELECT source, observed_at, raw_sha256, raw_payload FROM observations '
                        'WHERE snapshot_id=? ORDER BY observed_at DESC, source', (snapshot_id,)):
                    intact = payload is not None and _sha(payload) == raw_sha
                    belongs = False
                    if intact:
                        try:
                            decoded, parsed = parse_snapshot(payload)
                            match = [item for item in parsed if item['issue'] == issue]
                            belongs = (_sha(_canonical(decoded)) == snapshot_id and len(match) == 1
                                       and _canonical(match[0]['raw_row']) == raw)
                        except (InvalidSnapshot, ValueError, TypeError):
                            pass
                    observations.append({'source': source, 'observed_at': observed_at,
                                         'official_url_format': _official_url(source),
                                         'bytes_and_row_match': belongs})
                entry['snapshots'].append({'snapshot_id': snapshot_id, 'observations': observations})
            for item in grouped.values():
                item['snapshots'].sort(key=lambda snapshot: snapshot['snapshot_id'])
            decision = db.execute('SELECT selected_snapshot, selected_row_sha256, note, reviewed_at '
                                  'FROM paper_resolutions WHERE issue=? AND variants_sha256=? '
                                  'ORDER BY seq DESC LIMIT 1', (issue, fingerprint)).fetchone()
        return {'issue': issue, 'variants_sha256': fingerprint,
                'variants': [grouped[digest] for digest in variants],
                'resolution': ({'selected_snapshot': decision[0], 'selected_row_sha256': decision[1],
                                'note': decision[2], 'reviewed_at': decision[3]} if decision else None),
                'assurance': 'personal review only; source origin and time not independently authenticated'}

    @staticmethod
    def _write_key(value):
        if not isinstance(value, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', value, re.I):
            raise PaperResultError('纸面结果写入请求标识无效')
        return value

    @staticmethod
    def _write_request(operation, request):
        raw = _canonical(request)
        return hashlib.sha256(raw).hexdigest(), raw

    def write_status(self, request_key, operation, request_sha256):
        self._write_key(request_key)
        if operation not in ('draw_resolve', 'issue_recalculate', 'source_correction_retry') or not isinstance(request_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', request_sha256):
            raise PaperResultError('纸面结果写入请求摘要无效')
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            saved = db.execute('SELECT operation,object_key,request_sha256,request_json,result_json '
                               'FROM paper_write_requests WHERE request_key=?', (request_key,)).fetchone()
            fence = db.execute('SELECT operation,object_key,request_sha256,request_json '
                               'FROM paper_write_fences WHERE request_key=?', (request_key,)).fetchone()
            retry = db.execute('SELECT retry_of,operation,object_key,request_sha256,request_json '
                               'FROM paper_write_retries WHERE request_key=?', (request_key,)).fetchone()
            if saved and fence:
                raise PaperResultError('纸面结果请求同时存在接受与封止记录')
            if retry and not (saved or fence):
                raise PaperResultError('纸面结果关联重试缺少接受或封止记录')
            if not saved and not fence:
                return {'status': 'unknown', 'request_key': request_key, 'request_sha256': request_sha256}
            row = saved or fence
            if row[0] != operation or row[2] != request_sha256:
                raise PaperResultError('请求标识已用于不同的纸面结果操作或摘要')
            raw = row[3]
            try:
                request = json.loads(raw)
            except (TypeError, ValueError, UnicodeDecodeError) as exc:
                raise PaperResultError('纸面结果写入请求无法解析') from exc
            if not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != request_sha256:
                raise PaperResultError('纸面结果写入请求摘要不匹配')
            if not isinstance(request, dict) or _canonical(request) != raw:
                raise PaperResultError('纸面结果写入请求无法规范化核验')
            if operation == 'source_correction_retry':
                if (set(request) != {'correction_id', 'scope'} or type(request.get('correction_id')) is not int
                        or request['correction_id'] < 1 or request.get('scope') not in ('global', 'issue')):
                    raise PaperResultError('来源更正重试请求内容无效')
                expected_object = f"{request['scope']}:{request['correction_id']}"
                followup_table = ('source_correction_followups' if request['scope'] == 'global'
                                  else 'scoped_correction_followups')
                if not db.execute(f'SELECT 1 FROM {followup_table} WHERE correction_id=?',
                                  (request['correction_id'],)).fetchone():
                    raise PaperResultError('来源更正重试对应的后续任务不存在')
            elif operation == 'draw_resolve':
                if (set(request) != {'issue', 'snapshot_id', 'variants_sha256', 'row_sha256', 'note', 'confirmed'}
                        or not isinstance(request.get('issue'), str) or not re.fullmatch(r'\d{5}', request['issue'])
                        or any(not isinstance(request.get(field), str)
                               or not re.fullmatch(r'[0-9a-f]{64}', request[field])
                               for field in ('snapshot_id', 'variants_sha256', 'row_sha256'))
                        or not isinstance(request.get('note'), str) or not request['note'].strip()
                        or len(request['note']) > 1000 or request.get('confirmed') is not True):
                    raise PaperResultError('开奖复核请求内容无效')
                expected_object = request['issue']
            else:
                if (set(request) != {'issue', 'snapshot_id'} or not isinstance(request.get('issue'), str)
                        or not re.fullmatch(r'\d{5}', request['issue'])
                        or not isinstance(request.get('snapshot_id'), str)
                        or not re.fullmatch(r'[0-9a-f]{64}', request['snapshot_id'])):
                    raise PaperResultError('纸面重算请求内容无效')
                expected_object = request['issue']
            if row[1] != expected_object:
                raise PaperResultError('纸面结果写入对象键与请求不一致')
            retry_of = retry[0] if retry else None
            if retry:
                if retry[1:] != (operation, row[1], request_sha256, raw):
                    raise PaperResultError('纸面结果关联重试与当前请求不一致')
                original = db.execute('SELECT operation,object_key,request_sha256,request_json '
                                      'FROM paper_write_fences WHERE request_key=?', (retry_of,)).fetchone()
                if original != (operation, row[1], request_sha256, raw):
                    raise PaperResultError('纸面结果关联重试来源无法核验')
            if fence:
                return {'status': 'fenced', 'request_key': request_key, 'operation': operation,
                        'object_key': row[1], 'request_sha256': request_sha256,
                        'request': request, 'retry_of': retry_of}
            result_raw = saved[4]
            if operation != 'issue_recalculate' and result_raw is None:
                raise PaperResultError('已接受纸面结果请求缺少接受回执')
            if result_raw is not None:
                try:
                    stored_result = json.loads(result_raw)
                except (TypeError, ValueError, UnicodeDecodeError) as exc:
                    raise PaperResultError('纸面结果接受回执无法解析') from exc
                if not isinstance(result_raw, bytes) or _canonical(stored_result) != result_raw:
                    raise PaperResultError('纸面结果接受回执摘要或格式不一致')
                if operation == 'source_correction_retry':
                    expected_result = {'status': 'accepted', 'correction_id': request['correction_id'],
                                       'scope': request['scope']}
                    if stored_result != expected_result:
                        raise PaperResultError('来源更正重试接受回执与请求不一致')
                elif operation == 'draw_resolve':
                    resolution = stored_result.get('resolution') if isinstance(stored_result, dict) and 'resolution' in stored_result else stored_result
                    if (not isinstance(resolution, dict) or type(resolution.get('resolution_id')) is not int
                            or resolution.get('variants_sha256') != request['variants_sha256']
                            or resolution.get('selected_row_sha256') != request['row_sha256']):
                        raise PaperResultError('开奖复核接受结果与请求不一致')
                    persisted = db.execute('SELECT issue,selected_snapshot,selected_row_sha256,variants_sha256,note '
                                           'FROM paper_resolutions WHERE id=?',
                                           (resolution['resolution_id'],)).fetchone()
                    if persisted != (request['issue'], request['snapshot_id'], request['row_sha256'],
                                     request['variants_sha256'], request['note']):
                        raise PaperResultError('开奖复核接受结果与业务记录不一致')
                    if 'resolution' in stored_result and (set(stored_result) != {'resolution', 'recalculations'}
                                                          or not isinstance(stored_result['recalculations'], dict)):
                        raise PaperResultError('开奖复核后续重算结果无法核验')
                elif not isinstance(stored_result, dict):
                    raise PaperResultError('纸面重算结果格式无效')
            output = {'status': 'accepted', 'request_key': request_key, 'operation': operation,
                      'object_key': row[1], 'request_sha256': request_sha256, 'request': request,
                      'retry_of': retry_of}
            if result_raw is not None:
                output['result'] = stored_result
            if operation == 'source_correction_retry':
                followup = db.execute(f'SELECT status,attempts,details_json,updated_at FROM {followup_table} '
                                      'WHERE correction_id=?', (request['correction_id'],)).fetchone()
                output['followup'] = {'status': followup[0], 'attempts': followup[1],
                                      'details': json.loads(followup[2]) if followup[2] else {},
                                      'updated_at': followup[3]}
            return output

    def record_draw_followup(self, request_key, request_sha256, resolution, recalculations):
        self._write_key(request_key)
        payload = {'resolution': resolution, 'recalculations': recalculations}
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT operation,request_sha256,result_json FROM paper_write_requests WHERE request_key=?",
                             (request_key,)).fetchone()
            if not row or row[:2] != ('draw_resolve', request_sha256):
                raise PaperResultError('开奖复核请求回执无法关联后续计算')
            if row[2] is not None:
                current = json.loads(row[2])
                if current == payload:
                    return current
                if not (isinstance(current, dict) and set(current) == {'resolution_id', 'variants_sha256', 'selected_row_sha256'}
                        and current == resolution):
                    raise PaperResultError('开奖复核后续结果已记录且与当前计算不一致')
            db.execute('UPDATE paper_write_requests SET result_json=? WHERE request_key=?',
                       (_canonical(payload), request_key))
        return payload

    def retry_source_correction(self, correction_id, scope, *, request_key, request_sha256, retry_of=None):
        """Durably enqueue one explicitly requested correction follow-up retry."""
        if (type(correction_id) is not int or correction_id < 1 or scope not in ('global', 'issue')):
            raise PaperResultError('来源更正重试目标无效')
        request = {'correction_id': correction_id, 'scope': scope}
        operation = 'source_correction_retry'
        digest, raw = self._write_request(operation, request)
        if digest != request_sha256:
            raise PaperResultError('来源更正重试请求摘要不匹配')
        request_key = self._write_key(request_key)
        object_key = f'{scope}:{correction_id}'
        corrections, followups = (('source_corrections', 'source_correction_followups') if scope == 'global'
                                  else ('scoped_source_corrections', 'scoped_correction_followups'))
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT operation,object_key,request_sha256,request_json,result_json '
                               'FROM paper_write_requests WHERE request_key=?', (request_key,)).fetchone()
            if prior:
                if prior[:4] != (operation, object_key, digest, raw) or prior[4] is None:
                    raise PaperResultError('请求标识已用于不同的来源更正重试')
                self._record_retry(db, request_key, operation, object_key, digest, raw, retry_of,
                                   self.experiments._now().isoformat())
            else:
                fence = db.execute('SELECT operation,object_key,request_sha256,request_json FROM paper_write_fences WHERE request_key=?',
                                   (request_key,)).fetchone()
                if fence:
                    raise PaperResultError('来源更正重试请求已封止，迟到提交将被拒绝')
                row = db.execute(f'SELECT f.status,f.lease_until FROM {corrections} c '
                                 f'JOIN {followups} f ON f.correction_id=c.seq WHERE c.seq=?',
                                 (correction_id,)).fetchone()
                if not row:
                    raise KeyError(correction_id)
                status, lease_until = row
                now = self.experiments._now()
                running = status == 'running' and lease_until and _time(lease_until) > now
                if status not in ('pending', 'running', 'complete', 'failed'):
                    raise PaperResultError('来源更正后续任务状态不能重试')
                self._record_retry(db, request_key, operation, object_key, digest, raw, retry_of, now.isoformat())
                if not running:
                    db.execute(f"UPDATE {followups} SET status='pending',token=NULL,lease_until=NULL,updated_at=? WHERE correction_id=?",
                               (now.isoformat(), correction_id))
                db.execute('INSERT INTO paper_write_requests VALUES (?,?,?,?,?,?,?)',
                           (request_key, operation, object_key, digest, raw,
                            _canonical({'status': 'accepted', 'correction_id': correction_id, 'scope': scope}), now.isoformat()))
        # The follow-up row is already pending in the same transaction as the
        # acceptance receipt. Startup recovery can claim it if this process exits.
        followup = self.process_source_correction(correction_id, scope=scope)
        return {'write': self.write_status(request_key, operation, digest), 'followup': followup}

    def source_correction_followup(self, correction_id, *, scope='global'):
        if type(correction_id) is not int or correction_id < 1 or scope not in ('global', 'issue'):
            raise PaperResultError('来源更正后续目标无效')
        table = 'source_correction_followups' if scope == 'global' else 'scoped_correction_followups'
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute(f'SELECT status,attempts,details_json,updated_at FROM {table} WHERE correction_id=?',
                             (correction_id,)).fetchone()
        if not row:
            raise KeyError(correction_id)
        return {'status': row[0], 'attempts': row[1], 'details': json.loads(row[2]) if row[2] else {},
                'updated_at': row[3]}

    @staticmethod
    def _record_retry(db, request_key, operation, object_key, digest, raw, retry_of, now):
        prior = db.execute('SELECT retry_of,operation,object_key,request_sha256,request_json FROM paper_write_retries WHERE request_key=?',
                           (request_key,)).fetchone()
        if retry_of is None:
            if prior:
                raise PaperResultError('该请求必须沿用已记录的关联重试')
            return
        PaperResults._write_key(retry_of)
        original = db.execute('SELECT operation,object_key,request_sha256,request_json FROM paper_write_fences WHERE request_key=?',
                              (retry_of,)).fetchone()
        wanted = (retry_of, operation, object_key, digest, raw)
        if retry_of == request_key or original != wanted[1:]:
            raise PaperResultError('关联重试必须与已封止的纸面结果请求内容完全一致')
        if prior:
            if prior != wanted:
                raise PaperResultError('请求标识已用于其他纸面结果关联重试')
            return
        db.execute('INSERT INTO paper_write_retries VALUES (?,?,?,?,?,?,?)',
                   (request_key, retry_of, operation, object_key, digest, raw, now))

    def fence_write(self, request_key, operation, request, request_sha256, *, retry_of=None):
        self._write_key(request_key)
        if operation == 'source_correction_retry':
            if (not isinstance(request, dict) or set(request) != {'correction_id', 'scope'}
                    or type(request.get('correction_id')) is not int or request['correction_id'] < 1
                    or request.get('scope') not in ('global', 'issue')):
                raise PaperResultError('来源更正重试封止请求内容无效')
            object_key = f"{request['scope']}:{request['correction_id']}"
        elif operation in ('draw_resolve', 'issue_recalculate'):
            if (not isinstance(request, dict) or not isinstance(request.get('issue'), str)
                    or not re.fullmatch(r'\d{5}', request['issue'])
                    or not isinstance(request.get('snapshot_id'), str)
                    or not re.fullmatch(r'[0-9a-f]{64}', request['snapshot_id'])):
                raise PaperResultError('纸面结果写入操作无效')
            object_key = request['issue']
        else:
            raise PaperResultError('纸面结果写入操作无效')
        if operation == 'draw_resolve' and (set(request) != {'issue', 'snapshot_id', 'variants_sha256', 'row_sha256', 'note', 'confirmed'}
                or not isinstance(request.get('variants_sha256'), str) or not re.fullmatch(r'[0-9a-f]{64}', request['variants_sha256'])
                or not isinstance(request.get('row_sha256'), str) or not re.fullmatch(r'[0-9a-f]{64}', request['row_sha256'])
                or not isinstance(request.get('note'), str) or not request['note'].strip() or len(request['note']) > 1000
                or request.get('confirmed') is not True):
            raise PaperResultError('开奖复核封止请求内容无效')
        if operation == 'issue_recalculate' and set(request) != {'issue', 'snapshot_id'}:
            raise PaperResultError('纸面重算封止请求内容无效')
        digest, raw = self._write_request(operation, request)
        if digest != request_sha256:
            raise PaperResultError('纸面结果写入请求摘要不匹配')
        object_key = (f"{request['scope']}:{request['correction_id']}" if operation == 'source_correction_retry'
                      else request['issue'])
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            state = self.write_status(request_key, operation, digest)
            if state['status'] != 'unknown':
                if state.get('request') != request or (state.get('retry_of') or None) != retry_of:
                    raise PaperResultError('原纸面结果请求内容与当前封止请求不符')
                return state
            now = self.experiments._now().isoformat()
            self._record_retry(db, request_key, operation, object_key, digest, raw, retry_of, now)
            db.execute('INSERT INTO paper_write_fences VALUES (?,?,?,?,?,?)',
                       (request_key, operation, object_key, digest, raw, now))
            return {'status': 'fenced', 'request_key': request_key, 'operation': operation,
                    'object_key': object_key, 'request_sha256': digest, 'request': request, 'retry_of': retry_of}

    def resolve_draw(self, issue, snapshot_id, *, expected_variants_sha256, expected_row_sha256, note, confirmed,
                     request_key=None, request_sha256=None, retry_of=None):
        """Append explicit personal selection for the exact set of seen variants."""
        if (not isinstance(issue, str) or not re.fullmatch(r'\d{5}', issue)
                or not isinstance(snapshot_id, str) or not re.fullmatch(r'[0-9a-f]{64}', snapshot_id)
                or not isinstance(expected_variants_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', expected_variants_sha256)
                or not isinstance(expected_row_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', expected_row_sha256)
                or not isinstance(note, str) or not note.strip() or len(note) > 1000 or confirmed is not True):
            raise PaperResultError('explicit review and a reason are required')
        self.initialize()
        request = {'issue': issue, 'snapshot_id': snapshot_id, 'variants_sha256': expected_variants_sha256,
                   'row_sha256': expected_row_sha256, 'note': note, 'confirmed': confirmed}
        request_digest, raw = self._write_request('draw_resolve', request)
        if request_key is not None and (request_digest != request_sha256):
            raise PaperResultError('纸面结果写入请求摘要不匹配')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if request_key is not None:
                request_key = self._write_key(request_key)
                old = db.execute('SELECT operation,object_key,request_sha256,request_json,result_json FROM paper_write_requests WHERE request_key=?', (request_key,)).fetchone()
                if old:
                    if old[:4] != ('draw_resolve', issue, request_digest, raw):
                        raise PaperResultError('请求标识已用于不同的开奖复核')
                    self._record_retry(db, request_key, 'draw_resolve', issue, request_digest, raw, retry_of,
                                       self.experiments._now().isoformat())
                    return json.loads(old[4])
                fence = db.execute('SELECT operation,object_key,request_sha256,request_json FROM paper_write_fences WHERE request_key=?', (request_key,)).fetchone()
                if fence:
                    raise PaperResultError('开奖复核请求已封止，迟到提交将被拒绝')
                self._record_retry(db, request_key, 'draw_resolve', issue, request_digest, raw, retry_of,
                                   self.experiments._now().isoformat())
            rows, fingerprint, variants = self._variants(db, issue)
            if fingerprint != expected_variants_sha256:
                raise PaperResultError('draw versions changed since review; reload and review again')
            selected = [raw for sid, raw in rows if sid == snapshot_id]
            if not selected or len(variants) < 2:
                raise PaperResultError('selected conflicting version is unavailable')
            digest = _sha(selected[0])
            if digest != expected_row_sha256:
                raise PaperResultError('selected row differs from reviewed version')
            cursor = db.execute('INSERT INTO paper_resolutions(issue, selected_snapshot, selected_row_sha256, variants_sha256, note, reviewed_at) '
                                'VALUES (?, ?, ?, ?, ?, ?)',
                                (issue, snapshot_id, digest, fingerprint, note, self.experiments._now().isoformat()))
            result = {'resolution_id': cursor.lastrowid, 'variants_sha256': fingerprint, 'selected_row_sha256': digest}
            if request_key is not None:
                db.execute('INSERT INTO paper_write_requests VALUES (?,?,?,?,?,?,?)',
                           (request_key, 'draw_resolve', issue, request_digest, raw, _canonical(result), self.experiments._now().isoformat()))
            return result

    @staticmethod
    def _observed_draw(db, snapshot_id, issue, registered, cutoff, now):
        row = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?', (snapshot_id, issue)).fetchone()
        if not row:
            raise PaperResultError('snapshot does not include target draw')
        raw_row = row[0]
        observations = db.execute('SELECT source, observed_at, raw_sha256, raw_payload FROM observations WHERE snapshot_id=?',
                                  (snapshot_id,)).fetchall()
        for source, observed_at, raw_sha, payload in observations:
            if payload is None or not _official_url(source) or _sha(payload) != raw_sha:
                continue
            instant = _time(observed_at)
            if instant < registered or instant < cutoff or instant > now:
                continue
            try:
                decoded, draws = parse_snapshot(payload)
                if _sha(_canonical(decoded)) != snapshot_id:
                    continue
                matching = [d for d in draws if d['issue'] == issue]
                if len(matching) == 1 and _canonical(matching[0]['raw_row']) == raw_row:
                    return json.loads(raw_row), observed_at, source, raw_row
            except InvalidSnapshot:
                continue
        raise PaperResultError('no consistent official-source observation after registration and cutoff')

    @staticmethod
    def _payouts(rule, draw):
        mapping, levels = rule.get('payout_sort_map'), draw.get('prizeLevelList')
        if type(draw.get('verify')) is not int or draw['verify'] != 1:
            raise PaperResultError('source draw verification flag is missing or not confirmed')
        if not isinstance(mapping, dict) or not isinstance(levels, list):
            raise PaperResultError('reviewed payout mapping or published prize list is missing')
        if type(draw.get('lotteryPromotionFlag')) is not int or draw['lotteryPromotionFlag'] not in (0, 1):
            raise PaperResultError('promotion status is unknown')
        groups, extras = rule.get('hit_groups'), rule.get('additional_groups')
        if not isinstance(groups, dict) or not isinstance(extras, dict):
            raise PaperResultError('reviewed prize mapping is incomplete')
        needed = {x for x in groups.values() if x is not None} | set(extras.values())
        if not needed.issubset(mapping) or any(not isinstance(mapping[k], (str, int)) for k in needed):
            raise PaperResultError('reviewed payout sort mapping is incomplete')
        if len({str(mapping[k]) for k in needed}) != len(needed):
            raise PaperResultError('payout sort mapping contains duplicates')
        by_sort = {}
        for item in levels:
            if not isinstance(item, dict) or 'sort' not in item or 'stakeAmount' not in item:
                raise PaperResultError('published prize row is incomplete')
            key = str(item['sort'])
            if key in by_sort:
                raise PaperResultError('published prize sort is duplicated')
            by_sort[key] = item['stakeAmount']
        if any(str(mapping[k]) not in by_sort for k in needed):
            raise PaperResultError('published payout is missing')
        return {key: by_sort[str(mapping[key])] for key in needed}

    def _event(self, db, experiment_key, snapshot_id, status, reason):
        ident = _sha(_canonical({'experiment': experiment_key, 'snapshot': snapshot_id, 'status': status, 'reason': reason}))
        db.execute('INSERT OR IGNORE INTO paper_events VALUES (?, ?, ?, ?, ?, ?)',
                   (ident, experiment_key, snapshot_id, status, reason, self.experiments._now().isoformat()))

    @exact_money_context
    def process_one(self, experiment_key, snapshot_id, *, transaction_guard=None):
        self.initialize()
        content = self.experiments.get(experiment_key)
        issue = content['protocol']['target_issue']
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            try:
                if transaction_guard is not None:
                    transaction_guard(db)
                current_row = db.execute('SELECT request_json,content_sha256,content_json FROM fixed_experiments WHERE request_key=?',
                                         (experiment_key,)).fetchone()
                from dlt.fixed_contract import validate_fixed_record
                if not current_row or validate_fixed_record(db, *current_row) != content:
                    raise PaperResultError('fixed experiment integrity failure')
                calendar_id, calendar_review, calendar_chain = self.experiments._calendar_for_fixed(db, content)
                rule_id, rule_review, rule_chain = self.experiments._effective_review(
                    db, content['protocol']['rule_review_id'], 'rule', issue=issue)
                tax_id, tax_review, tax_chain = self.experiments._effective_review(
                    db, content['protocol']['tax_review_id'], 'tax', issue=issue)
                rule = rule_review['extracted']
                if rule.get('source') != rule_review['source_url']:
                    raise PaperResultError('corrected rule source does not match reviewed source')
                if (tax_review['extracted'].get('tax') != rule.get('tax')
                        or issue not in tax_review['extracted'].get('applicable_issues', [])):
                    raise PaperResultError('corrected tax review does not cover rule and target')
                for item in content['pairs']:
                    pair = self.experiments.pairs._checked_pair(db, item['pair_key'])
                    if pair['pair_sha256'] != item['pair']['pair_sha256']:
                        raise PaperResultError('fixed pair integrity failure')
                selected = self._selection(db, issue)
                raw, observed, source, raw_row = self._observed_draw(
                    db, snapshot_id, issue, _time(content['registered_at']),
                    _time(calendar_review['extracted']['cutoff_at']), self.experiments._now())
                row_hash = _sha(raw_row)
                if row_hash != selected:
                    raise PaperResultError('snapshot is not the reviewed current draw version')
                if datetime.fromisoformat(raw['lotteryDrawTime']).date().isoformat() != calendar_review['extracted']['draw_date']:
                    raise PaperResultError('draw date differs from fixed calendar')
                payouts, results, average = _calculate_results(content, issue, raw, rule)
                payload = {'experiment_key': experiment_key, 'experiment_sha256': current_row[1], 'issue': issue,
                           'row_sha256': row_hash, 'source_snapshot_id': snapshot_id, 'source_url': source,
                           'observed_at': observed, 'payouts': payouts,
                           'promotion_payouts': promotion_payouts if promotion else None,
                           'rule_review_id': rule_id,
                           'tax_review_id': tax_id, 'original_rule_review_id': content['protocol']['rule_review_id'],
                           'original_tax_review_id': content['protocol']['tax_review_id'],
                           'calendar_review_id': calendar_id, 'original_calendar_review_id': content['protocol']['calendar_review_id'],
                           'calendar_correction_chain': calendar_chain,
                           'rule_correction_chain': rule_chain, 'tax_correction_chain': tax_chain, 'replicas': results,
                           'mean_difference_fraction': average,
                           'code_sha256': _sha(Path(__file__).read_bytes()), 'runtime': platform.python_version(),
                           'settlement_code_sha256': _sha(Path(settlement_module.__file__).read_bytes()),
                           'calculation_version': 'exact-money-v1',
                           'tax_assumption': 'each alternative is separate and has no other same-issue winnings',
                           'payout_assumption': ('published base and promotion payouts; ticket groups and single-ticket thresholds are modeled; '
                                                 'hypothetical added winners and limits not recomputed' if promotion else
                                                 'published payouts unchanged; hypothetical added winners and limits not recomputed'),
                           'assurance': 'paper result; local source and time attestations are not independently certified'}
                if transaction_guard is not None:
                    transaction_guard(db)
                existing = None
                for candidate in db.execute('SELECT id, content_json FROM paper_groups WHERE experiment_key=? AND row_sha256=? '
                                            'ORDER BY (id = (SELECT group_id FROM paper_current WHERE experiment_key=?)) DESC, rowid DESC',
                                            (experiment_key, row_hash, experiment_key)):
                    if _sha(candidate[1]) != candidate[0]:
                        raise PaperResultError('historical result integrity failure')
                    prior = json.loads(candidate[1])
                    if (prior.get('rule_review_id') == rule_id and prior.get('tax_review_id') == tax_id
                            and prior.get('calendar_review_id', content['protocol']['calendar_review_id']) == calendar_id
                            and _matches_calculation(prior, payouts, results, average,
                                                     promotion_payouts if promotion else None)):
                        existing = candidate
                        break
                if existing:
                    ident, encoded = existing
                    if _sha(encoded) != ident:
                        raise PaperResultError('historical result integrity failure')
                    payload = json.loads(encoded)
                else:
                    encoded = _canonical(payload); ident = _sha(encoded)
                    db.execute('INSERT INTO paper_groups VALUES (?, ?, ?, ?, ?)',
                               (ident, experiment_key, issue, row_hash, encoded))
                old = db.execute('SELECT group_id FROM paper_current WHERE experiment_key=?', (experiment_key,)).fetchone()
                db.execute('INSERT INTO paper_current VALUES (?, ?) ON CONFLICT(experiment_key) DO UPDATE SET group_id=excluded.group_id',
                           (experiment_key, ident))
                if not old or old[0] != ident:
                    db.execute('INSERT INTO paper_publications(experiment_key, previous_group, group_id, snapshot_id, published_at) '
                               'VALUES (?, ?, ?, ?, ?)',
                               (experiment_key, old[0] if old else None, ident, snapshot_id, self.experiments._now().isoformat()))
                self._event(db, experiment_key, snapshot_id, 'complete', ident)
                return {'status': 'complete', 'group_id': ident, 'result': payload, 'observation': observed, 'source': source}
            except (PaperResultError, RegistrationError, CandidateError, SettlementBlocked, ValueError, KeyError) as exc:
                if transaction_guard is not None:
                    transaction_guard(db)
                self._event(db, experiment_key, snapshot_id, 'blocked', str(exc))
                return {'status': 'blocked', 'reason': str(exc)}

    def process_snapshot(self, snapshot_id):
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT request_key, content_json FROM fixed_experiments').fetchall()
            targets = {row[0] for row in db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=?', (snapshot_id,))}
        return {key: self.process_one(key, snapshot_id) for key, raw in rows
                if json.loads(raw)['protocol']['target_issue'] in targets}

    def process_update_and_catchup(self, snapshot_id):
        """Check this update and unresolved registrations against retained snapshots.

        A later latest-page response may no longer include an earlier target.
        Retained observations are rechecked by process_one, not treated as proof
        of an official result merely because a matching issue is present.
        """
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT request_key, content_sha256, content_json FROM fixed_experiments').fetchall()
            targets = {r[0] for r in db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=?', (snapshot_id,))}
            if not db.execute('SELECT 1 FROM snapshots WHERE id=?', (snapshot_id,)).fetchone():
                raise PaperResultError('update snapshot is missing')
        outcomes = {}
        for key, digest, raw in rows:
            try:
                if not isinstance(raw, bytes) or _sha(raw) != digest:
                    raise PaperResultError('fixed experiment integrity failure; target cannot be verified')
                content = json.loads(raw)
                issue = content['protocol']['target_issue']
                if not isinstance(issue, str) or not re.fullmatch(r'\d{5}', issue):
                    raise PaperResultError('fixed experiment target is invalid')
                with closing(sqlite3.connect(self.path)) as db:
                    db.execute('BEGIN')
                    current_status = self.inspect_current_in_transaction(db, key)['status']
                    if current_status == 'locally_attested':
                        continue
                    _, calendar, _ = self.experiments._calendar_for_fixed(db, content)
                    if self.experiments._now() < _time(calendar['extracted']['cutoff_at']):
                        continue
                    candidates = [r[0] for r in db.execute(
                        'SELECT d.snapshot_id FROM draw_versions d JOIN observations o ON o.snapshot_id=d.snapshot_id '
                        'WHERE d.issue=? GROUP BY d.snapshot_id '
                        'ORDER BY (d.snapshot_id=?) DESC, MAX(o.observed_at) DESC, d.snapshot_id',
                        (issue, snapshot_id))]
                if issue in targets and snapshot_id not in candidates:
                    candidates.insert(0, snapshot_id)
                if not candidates:
                    outcomes[key] = {'status': 'waiting_source', 'issue': issue,
                                     'reason': 'target not found in retained snapshots; wait for a result or check older pages'}
                    continue
                for candidate in candidates:
                    try:
                        result = self.process_one(key, candidate)
                    except Exception as exc:
                        result = {'status': 'error', 'reason': str(exc)[:1000], 'issue': issue}
                    outcomes[key] = result
                    if result['status'] == 'complete':
                        break
            except Exception as exc:
                # A damaged record must not prevent unrelated experiments from
                # being checked, and the follow-up must not report full success.
                outcomes[key] = {'status': 'error', 'reason': str(exc)[:1000]}
        return outcomes

    def process_issue(self, issue, snapshot_id):
        """Retry only experiments for one reviewed issue, preserving failures."""
        if not isinstance(issue, str) or not re.fullmatch(r'\d{5}', issue):
            raise PaperResultError('valid five-digit issue required')
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT request_key, content_sha256, content_json FROM fixed_experiments').fetchall()
        results = {}
        for key, digest, raw in rows:
            try:
                if _sha(raw) != digest:
                    raise PaperResultError('fixed experiment integrity failure; target cannot be verified')
                if json.loads(raw)['protocol']['target_issue'] != issue:
                    continue
                result = self.process_one(key, snapshot_id)
                results[key] = {k: result[k] for k in ('status', 'reason', 'group_id') if k in result}
            except Exception as exc:
                # The resolution has already been committed; never claim it
                # was rolled back if one later recalculation fails unexpectedly.
                reason = ('unexpected recalculation error: ' + str(exc))[:1000]
                recorded = False
                try:
                    with closing(sqlite3.connect(self.path)) as db, db:
                        db.execute('BEGIN IMMEDIATE')
                        self._event(db, key, snapshot_id, 'error', reason)
                    recorded = True
                except (sqlite3.Error, OSError):
                    pass
                results[key] = {'status': 'error', 'reason': reason, 'recorded': recorded}
        return results

    def recalculate_issue(self, issue, snapshot_id, *, request_key=None, request_sha256=None, retry_of=None):
        if not isinstance(issue, str) or not re.fullmatch(r'\d{5}', issue):
            raise PaperResultError('valid five-digit issue required')
        request = {'issue': issue, 'snapshot_id': snapshot_id}
        request_digest, raw = self._write_request('issue_recalculate', request)
        if request_key is not None and request_digest != request_sha256:
            raise PaperResultError('纸面结果写入请求摘要不匹配')
        self.initialize()
        if request_key is not None:
            request_key = self._write_key(request_key)
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('BEGIN IMMEDIATE')
                prior = db.execute('SELECT operation,object_key,request_sha256,request_json,result_json '
                                   'FROM paper_write_requests WHERE request_key=?', (request_key,)).fetchone()
                if prior:
                    if prior[:4] != ('issue_recalculate', issue, request_digest, raw):
                        raise PaperResultError('请求标识已用于不同的纸面重算')
                    self._record_retry(db, request_key, 'issue_recalculate', issue, request_digest, raw, retry_of,
                                       self.experiments._now().isoformat())
                    if prior[4] is not None:
                        return json.loads(prior[4])
                else:
                    if db.execute('SELECT 1 FROM paper_write_fences WHERE request_key=?', (request_key,)).fetchone():
                        raise PaperResultError('纸面重算请求已封止，迟到提交将被拒绝')
                    now = self.experiments._now().isoformat()
                    self._record_retry(db, request_key, 'issue_recalculate', issue, request_digest, raw, retry_of, now)
                    db.execute('INSERT INTO paper_write_requests VALUES (?,?,?,?,?,?,?)',
                               (request_key, 'issue_recalculate', issue, request_digest, raw, None,
                                now))
        result = self.process_issue(issue, snapshot_id)
        if request_key is not None:
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute('BEGIN IMMEDIATE')
                db.execute('UPDATE paper_write_requests SET result_json=? WHERE request_key=? AND request_sha256=?',
                           (_canonical(result), request_key, request_digest))
        return result

    def process_source_correction(self, correction_id, *, retry=False, scope='global'):
        """Claim durable correction work and safely retry affected results.

        A crashed owner is retried after its lease expires. Result publication
        rechecks the current correction chain in its own write transaction, so
        repeated or late workers cannot publish an obsolete rule version.
        """
        if type(correction_id) is not int or correction_id < 1 or type(retry) is not bool or scope not in ('global', 'issue'):
            raise PaperResultError('invalid correction task')
        corrections, followups = (('source_corrections', 'source_correction_followups') if scope == 'global'
                                  else ('scoped_source_corrections', 'scoped_correction_followups'))
        issue_column = 'NULL' if scope == 'global' else 'c.issue'
        self.initialize()
        now = self.experiments._now()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute(f'SELECT c.kind,c.old_review_id,c.new_review_id,f.status,f.lease_until,f.details_json,{issue_column} '
                             f'FROM {corrections} c JOIN {followups} f ON f.correction_id=c.seq '
                             'WHERE c.seq=?', (correction_id,)).fetchone()
            if not row:
                raise KeyError(correction_id)
            kind, old_id, new_id, status, lease_until, encoded, scoped_issue = row
            if status == 'running' and lease_until and _time(lease_until) > now:
                return {'status': 'running'}
            if status in ('complete', 'failed') and not retry:
                return {'status': status, 'details': json.loads(encoded) if encoded else {}}
            token = uuid.uuid4().hex
            db.execute(f"UPDATE {followups} SET status='running',token=?,lease_until=?,"
                       "attempts=attempts+1,updated_at=? WHERE correction_id=?",
                       (token, (now+timedelta(seconds=180)).isoformat(), now.isoformat(), correction_id))
        details = {}
        def guard(db):
            ownership = db.execute(f'SELECT status,token,lease_until FROM {followups} '
                                   'WHERE correction_id=?', (correction_id,)).fetchone()
            if (not ownership or ownership[0] != 'running' or ownership[1] != token
                    or not ownership[2] or _time(ownership[2]) <= self.experiments._now()):
                raise CorrectionLeaseExpired('source correction execution ownership expired')
        try:
            with closing(sqlite3.connect(self.path)) as db:
                rows = db.execute('SELECT request_key,content_sha256,content_json FROM fixed_experiments').fetchall()
            for key, digest, raw in rows:
                snapshot_id = None
                try:
                    if _sha(raw) != digest:
                        raise PaperResultError('fixed experiment integrity failure; dependencies cannot be verified')
                    content = json.loads(raw)
                    target_issue = content['protocol']['target_issue']
                    if scoped_issue is not None and target_issue != scoped_issue:
                        continue
                    with closing(sqlite3.connect(self.path)) as db:
                        _, _, chain = self.experiments._effective_review(db, content['protocol'][kind+'_review_id'], kind, issue=target_issue)
                        if old_id not in chain or new_id not in chain:
                            continue
                        prior = db.execute('SELECT g.id,g.content_json FROM paper_current c '
                                           'JOIN paper_groups g ON g.id=c.group_id WHERE c.experiment_key=?',
                                           (key,)).fetchone()
                        if prior:
                            if _sha(prior[1]) != prior[0]:
                                raise PaperResultError('current result integrity failure')
                            snapshot_id = json.loads(prior[1])['source_snapshot_id']
                        else:
                            event = db.execute('SELECT snapshot_id FROM paper_events WHERE experiment_key=? '
                                               'ORDER BY recorded_at DESC,rowid DESC LIMIT 1', (key,)).fetchone()
                            snapshot_id = event[0] if event else None
                    if not snapshot_id:
                        details[key] = {'status': 'waiting', 'reason': 'no prior draw observation'}
                        continue
                    result = self.process_one(key, snapshot_id, transaction_guard=guard)
                    details[key] = {k: result[k] for k in ('status', 'reason', 'group_id') if k in result}
                except CorrectionLeaseExpired:
                    return {'status': 'superseded', 'details': details}
                except Exception as exc:
                    reason = ('unexpected recalculation error: ' + str(exc))[:1000]
                    recorded = False
                    if snapshot_id:
                        try:
                            with closing(sqlite3.connect(self.path)) as db, db:
                                db.execute('BEGIN IMMEDIATE')
                                guard(db)
                                self._event(db, key, snapshot_id, 'error', reason)
                            recorded = True
                        except CorrectionLeaseExpired:
                            return {'status': 'superseded', 'details': details}
                        except (sqlite3.Error, OSError):
                            pass
                    details[key] = {'status': 'error', 'reason': reason, 'recorded': recorded}
            final = 'failed' if any(item['status'] == 'error' for item in details.values()) else 'complete'
        except Exception as exc:
            final, details = 'failed', {'dispatch': {'status': 'error', 'reason': str(exc)[:1000]}}
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            finished = self.experiments._now()
            updated = db.execute(f'UPDATE {followups} SET status=?,details_json=?,updated_at=? '
                                 'WHERE correction_id=? AND token=? AND status=\'running\' AND lease_until>?',
                                 (final, _canonical(details), finished.isoformat(), correction_id, token, finished.isoformat()))
            if updated.rowcount != 1:
                return {'status': 'superseded', 'details': details}
        return {'status': final, 'details': details}

    def recover_pending_source_corrections(self, *, limit=20):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise PaperResultError('invalid correction recovery limit')
        with closing(sqlite3.connect(self.path)) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_correction_followups'").fetchone():
                return []
            rows = [('global', *row) for row in db.execute("SELECT correction_id,status,lease_until FROM source_correction_followups "
                                                           "WHERE status IN ('pending','running') ORDER BY correction_id")]
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='scoped_correction_followups'").fetchone():
                rows += [('issue', *row) for row in db.execute("SELECT correction_id,status,lease_until FROM scoped_correction_followups "
                                                              "WHERE status IN ('pending','running') ORDER BY correction_id")]
        now = self.experiments._now()
        ready = [(scope, ident) for scope, ident, status, lease in rows
                 if status == 'pending' or not lease or _time(lease) <= now][:limit]
        return [{'correction_id': ident, 'scope': scope, **self.process_source_correction(ident, scope=scope)} for scope, ident in ready]

    def inspect_current(self, experiment_key):
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            return self.inspect_current_in_transaction(db, experiment_key)

    def inspect_current_in_transaction(self, db, experiment_key):
        """Read status and dependencies using the caller's single snapshot."""
        fixed = db.execute('SELECT content_sha256,content_json,request_json FROM fixed_experiments WHERE request_key=?',
                           (experiment_key,)).fetchone()
        if not fixed:
            raise KeyError(experiment_key)
        if _sha(fixed[1]) != fixed[0]:
            return {'status': 'review_required', 'result': None, 'reason': 'fixed experiment integrity failure'}
        try:
            from dlt.fixed_contract import validate_fixed_record
            content = validate_fixed_record(db, fixed[2], fixed[0], fixed[1])
            calendar_id, _, _ = self.experiments._calendar_for_fixed(db, content)
            issue = content['protocol']['target_issue']
            rule_id, rule_review, _ = self.experiments._effective_review(db, content['protocol']['rule_review_id'], 'rule', issue=issue)
            tax_id, tax_review, _ = self.experiments._effective_review(db, content['protocol']['tax_review_id'], 'tax', issue=issue)
            if (tax_review['extracted'].get('tax') != rule_review['extracted'].get('tax')
                    or issue not in tax_review['extracted'].get('applicable_issues', [])):
                raise PaperResultError('规则与税费资料不一致，需要复核')
            require_tax_unit(rule_review['extracted'].get('tax'))
            row = db.execute('SELECT g.id, g.row_sha256, g.content_json FROM paper_current c '
                             'JOIN paper_groups g ON g.id=c.group_id AND g.experiment_key=c.experiment_key '
                             'WHERE c.experiment_key=?', (experiment_key,)).fetchone()
            if not row:
                if db.execute('SELECT 1 FROM paper_current WHERE experiment_key=?', (experiment_key,)).fetchone():
                    raise PaperResultError('current result reference is invalid')
                event = db.execute('SELECT status, reason FROM paper_events WHERE experiment_key=? '
                                   'ORDER BY recorded_at DESC, rowid DESC LIMIT 1', (experiment_key,)).fetchone()
                if event and event[0] in ('blocked', 'error'):
                    return {'status': 'blocked', 'result': None, 'reason': event[1]}
                return {'status': 'waiting', 'result': None}
            if _sha(row[2]) != row[0]:
                raise PaperResultError('result integrity failure')
            publication = db.execute('SELECT group_id FROM paper_publications WHERE experiment_key=? '
                                     'ORDER BY seq DESC LIMIT 1', (experiment_key,)).fetchone()
            if not publication or publication[0] != row[0]:
                raise PaperResultError('current result does not match publication history')
            saved = json.loads(row[2])
            if saved.get('experiment_key') != experiment_key or saved.get('experiment_sha256') != fixed[0]:
                raise PaperResultError('result experiment reference mismatch')
            draw = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?',
                              (saved.get('source_snapshot_id'), issue)).fetchone()
            if (saved.get('issue') != issue or saved.get('row_sha256') != row[1]
                    or not draw or _sha(draw[0]) != row[1]):
                raise PaperResultError('result draw reference mismatch')
            if saved.get('rule_review_id') != rule_id or saved.get('tax_review_id') != tax_id:
                raise PaperResultError('reviewed rule or tax correction awaits recalculation')
            if saved.get('calendar_review_id', content['protocol']['calendar_review_id']) != calendar_id:
                raise PaperResultError('reviewed calendar correction awaits recalculation')
            if self._selection(db, issue) != row[1]:
                raise PaperResultError('reviewed correction awaits recalculation')
            for item in content['pairs']:
                pair = self.experiments.pairs._checked_pair(db, item['pair_key'])
                if pair['pair_sha256'] != item['pair']['pair_sha256']:
                    raise PaperResultError('fixed pair reference mismatch')
            for kind in ('calendar', 'rule', 'tax'):
                self.experiments._get_review(db, content['protocol'][kind+'_review_id'], kind)
            # Older results did not record the settlement engine digest and
            # may have been rounded by the caller's Decimal context. Rebuild
            # them from their retained inputs before showing them as current.
            if (saved.get('calculation_version') != 'exact-money-v1'
                    or saved.get('code_sha256') != _sha(Path(__file__).read_bytes())
                    or saved.get('settlement_code_sha256') != _sha(Path(settlement_module.__file__).read_bytes())):
                raw = json.loads(draw[0])
                from dlt.historical_scenarios import promotion_data
                promotion, promotion_payouts = promotion_data(raw, issue)
                payouts, results, mean = _calculate_results(content, issue, raw, rule_review['extracted'])
                if not _matches_calculation(saved, payouts, results, mean,
                                            promotion_payouts if promotion else None):
                    raise PaperResultError('历史结算与当前精确计算不一致，请重新计算；原记录仍保留')
        except (PaperResultError, CandidateError, RegistrationError, SettlementBlocked, KeyError, ValueError, TypeError, AttributeError) as exc:
            return {'status': 'review_required', 'result': None, 'reason': str(exc)}
        return {'status': 'locally_attested', 'group_id': row[0], 'result': saved}

    def get_group(self, group_id):
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT content_json FROM paper_groups WHERE id=?', (group_id,)).fetchone()
        if not row:
            raise KeyError(group_id)
        if _sha(row[0]) != group_id:
            raise PaperResultError('historical result integrity failure')
        return json.loads(row[0])

    def list_history(self, experiment_key):
        self.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            db.row_factory = sqlite3.Row
            publications = [dict(row) for row in db.execute('SELECT * FROM paper_publications WHERE experiment_key=? ORDER BY seq',
                                                             (experiment_key,))]
            events = [dict(row) for row in db.execute('SELECT * FROM paper_events WHERE experiment_key=? ORDER BY recorded_at, rowid',
                                                      (experiment_key,))]
        return {'publications': publications, 'events': events}
