"""Immutable multi-period research protocol over existing per-issue records.

Locally attested descriptive experiments may use a fixed artifact, an untrained
method, or a fixed rolling-model contract. No source or time is authenticated.
"""
from contextlib import closing, nullcontext
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import hashlib
import json
import re
import sqlite3

from dlt.candidates import _canonical
from dlt.prospective import ProspectiveRepository, RegistrationError, _time
from dlt.paired_description import describe_pairs


DYNAMIC_FIELDS = {'target_issue', 'target_date', 'snapshot_id', 'rule_version', 'calendar_version', 'seed'}
SHARED_FIELDS = ('hypothesis', 'primary_metric', 'observation_issues', 'aggregation', 'exclusion_rule',
                 'retraining_policy', 'selection_rule', 'time_assurance', 'other_winnings_assumption')


def _profile(config):
    return {key: value for key, value in config.items() if key not in DYNAMIC_FIELDS}


def _generator_identity(pair):
    return {key: pair[key] for key in ('version', 'code_sha256', 'runtime')}


def _load(db, key):
    row = db.execute('SELECT content_sha256,content_json FROM experiment_series WHERE series_key=?', (key,)).fetchone()
    if not row:
        raise KeyError(key)
    if hashlib.sha256(row[1]).hexdigest() != row[0]:
        raise RegistrationError('series integrity failure')
    return json.loads(row[1])


def validate_period(db, key, protocol, pairs):
    """Validate under the same write lock used to insert the fixed period."""
    series = _load(db, key)
    manifest = series['manifest']
    if db.execute('SELECT 1 FROM series_closures WHERE series_key=?', (key,)).fetchone():
        raise RegistrationError('series is closed to new periods')
    issue = protocol['target_issue']
    if issue not in manifest['observation_issues']:
        raise RegistrationError('target issue is outside the fixed observation manifest')
    if db.execute('SELECT 1 FROM series_periods WHERE series_key=? AND issue=?', (key, issue)).fetchone():
        raise RegistrationError('series already has a fixed period for this issue')
    _validate_contract(series, protocol, pairs)
    if manifest['retraining_policy'] == 'rolling_retrain_v1':
        from dlt.rolling_policy import RollingPolicyError, validate_rolling_pair
        try:
            for item in pairs:
                validate_rolling_pair(db, series['rolling_spec'], item['pair'], item['pair_key'])
        except RollingPolicyError as exc:
            raise RegistrationError(str(exc)) from exc
        except (ValueError, KeyError, sqlite3.Error) as exc:
            raise RegistrationError('滚动训练配置或历史记录无法核验') from exc


def _validate_contract(series, protocol, pairs):
    manifest = series['manifest']
    issue = protocol['target_issue']
    if any(protocol.get(field) != manifest.get(field) for field in SHARED_FIELDS):
        raise RegistrationError('period protocol differs from fixed series protocol')
    if [item['replica'] for item in pairs] != manifest['replica_ids']:
        raise RegistrationError('period replica manifest differs from fixed series protocol')
    rolling = manifest['retraining_policy'] == 'rolling_retrain_v1'
    period_artifact = pairs[0]['pair']['model_artifact_id'] if pairs else None
    for item in pairs:
        pair, replica = item['pair'], item['replica']
        if (_profile(pair['config']) != series['config_profile']
                or (pair['model_artifact_id'] != period_artifact if rolling else pair['model_artifact_id'] != series['artifact_id'])
                or (rolling and not period_artifact)
                or _generator_identity(pair) != series['generator_identity']
                or pair['config']['seed'] != manifest['seeds'][issue][replica]):
            raise RegistrationError('period method, model, configuration or seed differs from fixed series protocol')


def _money(value):
    if not isinstance(value, str) or len(value) > 100:
        raise ValueError('invalid monetary amount')
    number = Decimal(value)
    if (not number.is_finite() or abs(number) > Decimal('1e30')
            or abs(number.as_tuple().exponent) > 100):
        raise ValueError('invalid monetary amount')
    return Fraction(number)


def _period_difference(series, fixed, result):
    """Require every preregistered pair and recompute using exact fractions."""
    protocol, pairs, manifest = fixed['protocol'], fixed['pairs'], series['manifest']
    _validate_contract(series, protocol, pairs)
    if result['issue'] != protocol['target_issue']:
        raise ValueError('result issue mismatch')
    rows = result['replicas']
    if not isinstance(rows, list) or len(rows) != len(manifest['replica_ids']):
        raise ValueError('incomplete replica results')
    differences = []
    sides = {name: {'gross': [], 'tax': [], 'cost': [], 'net': [], 'won': [], 'profit': []}
             for name in ('candidate', 'control')}
    for item, row, replica in zip(pairs, rows, manifest['replica_ids']):
        if row['replica'] != replica or row['pair_sha256'] != item['pair']['pair_sha256']:
            raise ValueError('replica mapping mismatch')
        nets = []
        for label in ('candidate', 'control'):
            value = row[label]
            gross, tax, cost, net = (_money(value[name]) for name in ('gross', 'tax', 'cost', 'net'))
            if (gross < 0 or tax < 0 or cost < 0 or tax > gross or gross-tax-cost != net
                    or cost != Fraction(item['pair'][label]['cost_fen'], 100)):
                raise ValueError('invalid replica amounts')
            winning = value['winning_tickets']
            maximum = sum(ticket['multiplier'] for ticket in item['pair'][label]['tickets'])
            if type(winning) is not int or not 0 <= winning <= maximum or (winning > 0) != (gross > 0):
                raise ValueError('invalid winning ticket count')
            for name, amount in (('gross', gross), ('tax', tax), ('cost', cost), ('net', net)):
                sides[label][name].append(amount)
            sides[label]['won'].append(Fraction(winning > 0))
            sides[label]['profit'].append(Fraction(net > 0))
            nets.append(net)
        difference = nets[0]-nets[1]
        if _money(row['difference_yuan']) != difference:
            raise ValueError('replica difference mismatch')
        differences.append(difference)
    mean = sum(differences, Fraction(0))/len(differences)
    if result['mean_difference_fraction'] != [mean.numerator, mean.denominator]:
        raise ValueError('period mean mismatch')
    return {'difference': mean, 'sides': {label: {name: sum(values, Fraction(0))/len(values)
                                                  for name, values in measures.items()}
                                          for label, measures in sides.items()}}


def _fraction(value):
    return [value.numerator, value.denominator] if value is not None else None


def _side_summary(observations, label):
    if not observations:
        return None
    rows = [(issue, result['sides'][label]) for issue, result in observations]
    total = lambda field: sum((entry[field] for _, entry in rows), Fraction(0))
    cost, net, gross = total('cost'), total('net'), total('gross')
    ordered = sorted(entry['net'] for _, entry in rows)
    # Nearest-rank empirical quantile of one replica-averaged value per issue.
    quantile = lambda num, den: ordered[(len(ordered)*num + den - 1)//den - 1]
    largest_issue, largest = max(((issue, entry['gross']) for issue, entry in rows), key=lambda item: item[1])
    return {'gross_total_fraction': _fraction(gross), 'tax_total_fraction': _fraction(total('tax')),
            'cost_total_fraction': _fraction(cost), 'net_total_fraction': _fraction(net),
            'roi_fraction': _fraction(net/cost) if cost else None,
            'win_fraction': _fraction(total('won')/len(rows)),
            'profit_fraction': _fraction(total('profit')/len(rows)),
            'net_p25_fraction': _fraction(quantile(1, 4)), 'net_p50_fraction': _fraction(quantile(1, 2)),
            'net_p75_fraction': _fraction(quantile(3, 4)),
            'largest_gross_issue': largest_issue if gross else None,
            'largest_gross_share_fraction': _fraction(largest/gross) if gross else None}


class SeriesRepository:
    def __init__(self, path, *, clock=None):
        self.repo = ProspectiveRepository(path, clock=clock)
        self.path = self.repo.path

    def preview_rolling(self, pair_key):
        """Inspect the template without publishing a series or starting work."""
        if not isinstance(pair_key, str) or not pair_key.strip() or len(pair_key) > 128:
            raise RegistrationError('需要有效的首期配对模板')
        from dlt.rolling_policy import RollingPolicyError, derive_rolling_spec, validate_rolling_pair
        try:
            with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
                db.execute('PRAGMA query_only=ON')
                db.execute('BEGIN')
                pair = self.repo.pairs._checked_pair(db, pair_key)
                spec = derive_rolling_spec(db, pair)
                validate_rolling_pair(db, spec, pair, pair_key)
                return spec
        except RollingPolicyError as exc:
            raise RegistrationError(str(exc)) from exc
        except (ValueError, KeyError, sqlite3.Error) as exc:
            raise RegistrationError('滚动训练模板或依赖无法核验') from exc

    @staticmethod
    def _write_retry(db, request_key, request_raw, digest, retry_of, now):
        prior = db.execute('SELECT retry_of,request_sha256,request_json FROM experiment_series_write_retries '
                           'WHERE request_key=?', (request_key,)).fetchone()
        if retry_of is None:
            if prior:
                raise RegistrationError('系列创建请求必须保留已记录的关联重试')
            return
        if (not isinstance(retry_of, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', retry_of, re.I)
                or retry_of == request_key):
            raise RegistrationError('系列创建关联请求标识无效')
        original = db.execute('SELECT request_sha256,request_json FROM experiment_series_write_fences WHERE request_key=?',
                              (retry_of,)).fetchone()
        if not original or original != (digest, request_raw):
            raise RegistrationError('系列创建关联重试必须匹配已封止原请求')
        if (db.execute('SELECT 1 FROM experiment_series_write_retries WHERE retry_of=?', (retry_of,)).fetchone()
                or db.execute('SELECT 1 FROM experiment_series_write_retries WHERE request_key=?', (retry_of,)).fetchone()):
            raise RegistrationError('系列创建只允许对原封止请求进行一次直接重试')
        wanted = (retry_of, digest, request_raw)
        if prior:
            if prior != wanted:
                raise RegistrationError('系列创建重试标识已关联其他请求')
            return
        db.execute('INSERT INTO experiment_series_write_retries VALUES (?,?,?,?,?)',
                   (request_key, retry_of, digest, request_raw, now))

    def create_status(self, request_key):
        if not isinstance(request_key, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I):
            raise RegistrationError('系列创建请求标识无效')
        self.repo.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            accepted = db.execute('SELECT request_sha256,request_json,result_json FROM experiment_series_write_requests '
                                  'WHERE request_key=?', (request_key,)).fetchone()
            fence = db.execute('SELECT request_sha256,request_json FROM experiment_series_write_fences WHERE request_key=?',
                               (request_key,)).fetchone()
            retry = db.execute('SELECT retry_of,request_sha256,request_json FROM experiment_series_write_retries WHERE request_key=?',
                               (request_key,)).fetchone()
            legacy = db.execute('SELECT content_sha256,content_json FROM experiment_series WHERE series_key=?',
                                (request_key,)).fetchone()
            if accepted and fence:
                raise RegistrationError('系列创建请求同时存在接受与封止记录')
            if retry and not (accepted or fence):
                raise RegistrationError('系列创建关联重试缺少接受或封止记录')
            if accepted:
                digest, request_raw, result_raw = accepted
                try:
                    request = json.loads(request_raw)
                    result = json.loads(result_raw)
                except (TypeError, ValueError, UnicodeDecodeError) as exc:
                    raise RegistrationError('系列创建接受结果无法解析') from exc
                if (not isinstance(request_raw, bytes) or not isinstance(result_raw, bytes)
                        or not isinstance(request, dict) or not isinstance(result, dict)
                        or hashlib.sha256(request_raw).hexdigest() != digest
                        or _canonical(request) != request_raw
                        or _canonical(result) != result_raw
                        or result.get('manifest') != request):
                    raise RegistrationError('系列创建接受结果摘要或协议不一致')
                if legacy != (hashlib.sha256(result_raw).hexdigest(), result_raw):
                    raise RegistrationError('系列创建接受结果与已保存版本不一致')
                if retry:
                    original = db.execute('SELECT request_sha256,request_json FROM experiment_series_write_fences '
                                          'WHERE request_key=?', (retry[0],)).fetchone()
                    if retry[1:] != (digest, request_raw) or original != (digest, request_raw):
                        raise RegistrationError('系列创建关联重试来源无法核验')
                return {'status': 'committed', 'request_key': request_key, 'request_sha256': digest,
                        'request': request, 'result': result, 'retry_of': retry[0] if retry else None}
            if fence:
                digest, request_raw = fence
                try:
                    request = json.loads(request_raw)
                except (TypeError, ValueError, UnicodeDecodeError) as exc:
                    raise RegistrationError('系列创建封止请求无法解析') from exc
                if (not isinstance(request_raw, bytes) or not isinstance(request, dict)
                        or hashlib.sha256(request_raw).hexdigest() != digest
                        or _canonical(request) != request_raw
                        or legacy is not None):
                    raise RegistrationError('系列创建封止记录与请求内容不一致')
                if retry:
                    original = db.execute('SELECT request_sha256,request_json FROM experiment_series_write_fences '
                                          'WHERE request_key=?', (retry[0],)).fetchone()
                    if retry[1:] != (digest, request_raw) or original != (digest, request_raw):
                        raise RegistrationError('系列创建关联重试来源无法核验')
                return {'status': 'fenced', 'request_key': request_key, 'request_sha256': digest,
                        'request': request, 'retry_of': retry[0] if retry else None}
            if legacy:
                content = _load(db, request_key)
                request_raw = _canonical(content['manifest'])
                if (not isinstance(legacy[1], bytes) or legacy[1] != _canonical(content)
                        or hashlib.sha256(legacy[1]).hexdigest() != legacy[0]):
                    raise RegistrationError('旧版系列记录完整性失败')
                return {'status': 'committed', 'request_key': request_key,
                        'request_sha256': hashlib.sha256(request_raw).hexdigest(),
                        'request': json.loads(request_raw), 'result': content, 'retry_of': None}
        return {'status': 'unknown', 'request_key': request_key}

    def fence_create(self, request_key, manifest, *, retry_of=None):
        if (not isinstance(request_key, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I)
                or not isinstance(manifest, dict)):
            raise RegistrationError('系列创建封止请求无效')
        request_raw = _canonical(manifest)
        digest = hashlib.sha256(request_raw).hexdigest()
        self.repo.initialize()
        now = self.repo._now().isoformat()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            accepted = db.execute('SELECT request_sha256,request_json FROM experiment_series_write_requests '
                                  'WHERE request_key=?', (request_key,)).fetchone()
            fence = db.execute('SELECT request_sha256,request_json FROM experiment_series_write_fences WHERE request_key=?',
                               (request_key,)).fetchone()
            legacy = db.execute('SELECT 1 FROM experiment_series WHERE series_key=?', (request_key,)).fetchone()
            if accepted and accepted != (digest, request_raw):
                raise RegistrationError('系列创建请求标识对应不同协议')
            if fence and fence != (digest, request_raw):
                raise RegistrationError('系列创建封止标识对应不同协议')
            if legacy and not accepted and not fence:
                if retry_of is not None:
                    raise RegistrationError('既有实验版本已提交，不能再作为关联重试封止')
                content = _load(db, request_key)
                if _canonical(content['manifest']) != request_raw:
                    raise RegistrationError('既有实验版本请求标识对应不同协议')
                result_raw = _canonical(content)
                db.execute('INSERT INTO experiment_series_write_requests VALUES (?,?,?,?,?)',
                           (request_key, digest, request_raw, result_raw, now))
                return {'status': 'committed', 'request_key': request_key, 'request_sha256': digest,
                        'request': json.loads(request_raw), 'result': json.loads(result_raw), 'retry_of': None}
            retry = db.execute('SELECT retry_of FROM experiment_series_write_retries WHERE request_key=?',
                               (request_key,)).fetchone()
            if accepted or fence:
                if (retry[0] if retry else None) != retry_of:
                    raise RegistrationError('系列创建关联重试与已记录请求不一致')
            else:
                self._write_retry(db, request_key, request_raw, digest, retry_of, now)
                db.execute('INSERT INTO experiment_series_write_fences VALUES (?,?,?,?)',
                           (request_key, digest, request_raw, now))
        return self.create_status(request_key)

    def create(self, key, manifest, *, retry_of=None):
        if not isinstance(key, str) or not key.strip() or len(key) > 128 or not isinstance(manifest, dict):
            raise RegistrationError('series identifier and manifest required')
        if not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', key, re.I):
            raise RegistrationError('series request identifier must be a UUID')
        required = set(SHARED_FIELDS) | {'replica_ids', 'seeds', 'template_pair_key', 'first_calendar_review_id'}
        if set(manifest) not in (required, required | {'hypothesis_draft_key'}):
            raise RegistrationError('series manifest fields are incomplete or unsupported')
        issues, replicas, seeds = manifest['observation_issues'], manifest['replica_ids'], manifest['seeds']
        if (not isinstance(issues, list) or not 1 <= len(issues) <= 200
                or any(not isinstance(x, str) or not re.fullmatch(r'\d{5}', x) for x in issues)
                or len(set(issues)) != len(issues)):
            raise RegistrationError('distinct explicit observation issues required')
        if (not isinstance(replicas, list) or not 1 <= len(replicas) <= 100
                or any(not isinstance(x, str) or not x.strip() or len(x) > 80 for x in replicas)
                or len(set(replicas)) != len(replicas)):
            raise RegistrationError('distinct fixed replica identifiers required')
        if not isinstance(seeds, dict) or set(seeds) != set(issues):
            raise RegistrationError('seed schedule must cover all observation issues')
        unsupported_seeds = False
        for issue in issues:
            schedule = seeds[issue]
            if (not isinstance(schedule, dict) or set(schedule) != set(replicas)
                    or any(type(x) is not int or not 0 <= x < 2**64 for x in schedule.values())
                    or len(set(schedule.values())) != len(replicas)):
                raise RegistrationError('each issue requires distinct unsigned 64-bit replica seeds')
            unsupported_seeds = unsupported_seeds or any(x > 4294967295 for x in schedule.values())
        if (manifest['primary_metric'] != 'paired_net_difference' or manifest['aggregation'] != 'arithmetic_mean_v1'
                or manifest['time_assurance'] != 'local_clock'
                or manifest['other_winnings_assumption'] != 'no_other_same_issue_winnings'
                or manifest['retraining_policy'] not in ('fixed_artifact_or_untrained_v1', 'rolling_retrain_v1')):
            raise RegistrationError('unsupported metric, aggregation, time, tax or retraining policy')
        for field in ('hypothesis', 'exclusion_rule', 'selection_rule', 'template_pair_key', 'first_calendar_review_id'):
            if not isinstance(manifest[field], str) or not manifest[field].strip() or len(manifest[field]) > 1000:
                raise RegistrationError(f'{field} is required')
        request = _canonical(manifest)
        request_digest = hashlib.sha256(request).hexdigest()
        self.repo.initialize()
        from dlt.prospective_settlement import PaperResults
        PaperResults(self.path, clock=self.repo.clock).initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            prior_write = db.execute('SELECT request_sha256,request_json,result_json FROM experiment_series_write_requests '
                                     'WHERE request_key=?', (key,)).fetchone()
            if prior_write:
                if prior_write[:2] != (request_digest, request):
                    raise RegistrationError('series key already used for different protocol')
                retry = db.execute('SELECT retry_of FROM experiment_series_write_retries WHERE request_key=?', (key,)).fetchone()
                if (retry[0] if retry else None) != retry_of:
                    raise RegistrationError('series retry association differs from accepted request')
                return json.loads(prior_write[2])
            if db.execute('SELECT 1 FROM experiment_series_write_fences WHERE request_key=?', (key,)).fetchone():
                raise RegistrationError('series creation request was fenced; late submission rejected')
            self._write_retry(db, key, request, request_digest, retry_of, self.repo._now().isoformat())
            prior = db.execute('SELECT content_sha256,content_json FROM experiment_series WHERE series_key=?', (key,)).fetchone()
            if prior:
                content = _load(db, key)
                if _canonical(content['manifest']) != request:
                    raise RegistrationError('series key already used for different protocol')
                payload = _canonical(content)
                db.execute('INSERT INTO experiment_series_write_requests VALUES (?,?,?,?,?)',
                           (key, request_digest, request, payload, self.repo._now().isoformat()))
                return content
            if 'hypothesis_draft_key' in manifest:
                from dlt.research_decisions import load_draft
                try:
                    draft = load_draft(db, manifest['hypothesis_draft_key'])
                except (KeyError, ValueError, sqlite3.Error) as exc:
                    raise RegistrationError('关联的新假设草稿不存在或无法核验') from exc
                if draft['hypothesis'] != manifest['hypothesis']:
                    raise RegistrationError('实验假设与关联草稿不一致，请重新选择草稿')
            if unsupported_seeds:
                raise RegistrationError('预定种子必须在 0 至 4294967295 之间，才能生成所有奖期的方案')
            now = self.repo._now()
            first = issues[0]
            if any(db.execute('SELECT 1 FROM draw_versions WHERE issue=?', (issue,)).fetchone() for issue in issues):
                raise RegistrationError('observation manifest contains an already observed issue')
            calendar = self.repo._get_review(db, manifest['first_calendar_review_id'], 'calendar')
            if self.repo._effective_review(db, manifest['first_calendar_review_id'], 'calendar')[0] != manifest['first_calendar_review_id']:
                raise RegistrationError('calendar review has been superseded; choose current review')
            pair = self.repo.pairs._checked_pair(db, manifest['template_pair_key'], require_replay=True)
            cal, cfg = calendar['extracted'], pair['config']
            created = db.execute('SELECT created_at FROM draft_pairs WHERE request_key=?',
                                 (manifest['template_pair_key'],)).fetchone()
            if (cal.get('issue') != first or cfg['target_issue'] != first or cal.get('draw_date') != cfg['target_date']
                    or cal.get('version') != cfg['calendar_version'] or now >= _time(cal.get('cutoff_at'))
                    or _time(calendar['reviewed_at']) > now or not created or _time(created[0]) > now):
                raise RegistrationError('first issue calendar, target, timing or template is invalid')
            if cfg['seed'] not in seeds[first].values():
                raise RegistrationError('template seed must be in first issue seed schedule')
            rolling_spec = None
            if manifest['retraining_policy'] == 'rolling_retrain_v1':
                from dlt.rolling_policy import RollingPolicyError, derive_rolling_spec, validate_rolling_pair
                try:
                    rolling_spec = derive_rolling_spec(db, pair)
                    validate_rolling_pair(db, rolling_spec, pair, manifest['template_pair_key'])
                except RollingPolicyError as exc:
                    raise RegistrationError(str(exc)) from exc
                except (ValueError, KeyError, sqlite3.Error) as exc:
                    raise RegistrationError('滚动训练模板或历史记录无法核验') from exc
            committed = self.repo._now()
            if committed < now or committed >= _time(cal.get('cutoff_at')):
                raise RegistrationError('clock changed or first issue deadline reached before series commit')
            content = {'manifest': json.loads(request), 'config_profile': _profile(cfg),
                       'artifact_id': pair['model_artifact_id'], 'template_pair_sha256': pair['pair_sha256'],
                       'generator_identity': _generator_identity(pair),
                       'created_at': committed.isoformat(),
                       'assurance': 'personal local record; source and time not independently authenticated'}
            if rolling_spec is not None:
                content['rolling_spec'] = rolling_spec
            payload = _canonical(content)
            db.execute('INSERT INTO experiment_series VALUES (?, ?, ?)', (key, hashlib.sha256(payload).hexdigest(), payload))
            db.execute('INSERT INTO experiment_series_write_requests VALUES (?,?,?,?,?)',
                       (key, request_digest, request, payload, self.repo._now().isoformat()))
            return content

    def get(self, key):
        with closing(sqlite3.connect(self.path)) as db:
            content = _load(db, key)
            periods = [{'issue': issue, 'experiment_key': experiment_key} for issue, experiment_key in
                       db.execute('SELECT issue,experiment_key FROM series_periods WHERE series_key=?', (key,))]
            closure = db.execute('SELECT closed_at,reason FROM series_closures WHERE series_key=?', (key,)).fetchone()
        manifest = content['manifest']
        unsupported = [issue for issue in manifest['observation_issues']
                       if any(seed > 4294967295 for seed in manifest['seeds'][issue].values())]
        return {'content': content, 'periods': periods, 'unsupported_seed_issues': unsupported,
                'closure': {'closed_at': closure[0], 'reason': closure[1]} if closure else None}

    def describe(self, key, *, db=None):
        """Return a descriptive per-issue report from one consistent read snapshot."""
        from dlt.prospective_settlement import PaperResults
        paper = PaperResults(self.path, clock=self.repo.clock)
        started_at = datetime.now(timezone.utc).isoformat()
        with (closing(sqlite3.connect(self.path)) if db is None else nullcontext(db)) as db:
            if not db.in_transaction:
                db.execute('BEGIN')
            series = _load(db, key)
            issues = series['manifest']['observation_issues']
            links = dict(db.execute('SELECT issue,experiment_key FROM series_periods WHERE series_key=?', (key,)))
            if set(links) - set(issues):
                raise RegistrationError('series period outside observation manifest')
            periods, values, outcome_observations = [], [], []
            for issue in issues:
                experiment_key = links.get(issue)
                if not experiment_key:
                    observed = db.execute('SELECT 1 FROM draw_versions WHERE issue=? LIMIT 1', (issue,)).fetchone()
                    periods.append({'issue': issue, 'status': 'missed_registration' if observed else 'unregistered'})
                    continue
                try:
                    current = paper.inspect_current_in_transaction(db, experiment_key)
                    status = current['status']
                    if status == 'locally_attested':
                        record = db.execute('SELECT content_json FROM fixed_experiments WHERE request_key=?',
                                            (experiment_key,)).fetchone()
                        fixed = json.loads(record[0])
                        if fixed['protocol']['target_issue'] != issue:
                            raise ValueError('period reference mismatch')
                        if series['manifest']['retraining_policy'] == 'rolling_retrain_v1':
                            from dlt.rolling_policy import validate_rolling_pair
                            for item in fixed['pairs']:
                                validate_rolling_pair(db, series['rolling_spec'], item['pair'], item['pair_key'])
                        period_result = _period_difference(series, fixed, current['result'])
                        value = period_result['difference']
                        values.append(value)
                        outcome_observations.append((issue, period_result))
                        periods.append({'issue': issue, 'status': status, 'experiment_key': experiment_key,
                                        'group_id': current['group_id'],
                                        'difference_fraction': [value.numerator, value.denominator]})
                    else:
                        periods.append({'issue': issue, 'status': status, 'experiment_key': experiment_key,
                                        'reason': current.get('reason')})
                except (KeyError, ValueError, TypeError, AttributeError, InvalidOperation, RegistrationError, ZeroDivisionError):
                    periods.append({'issue': issue, 'status': 'review_required', 'experiment_key': experiment_key,
                                    'reason': 'period result, replica mapping or amounts need review'})
            counts = {state: sum(p['status'] == state for p in periods)
                      for state in ('locally_attested', 'unregistered', 'missed_registration', 'waiting', 'blocked', 'review_required')}
            mean = sum(values, Fraction(0))/len(values) if values else None
            return {'series_key': key, 'as_of_utc': started_at, 'planned': len(issues), 'registered': len(links),
                    'valid': len(values), 'missing': len(issues)-len(values), 'counts': counts,
                    'completeness': 'complete' if len(values) == len(issues) else 'partial' if values else 'no_results',
                    'mean_difference_fraction': [mean.numerator, mean.denominator] if mean is not None else None,
                    'paired_description': describe_pairs([(issue, item['difference']) for issue, item in outcome_observations],
                                                         planned=len(issues)),
                    'outcomes': {label: _side_summary(outcome_observations, label)
                                 for label in ('candidate', 'control')},
                    'periods': periods, 'replicas_per_issue': len(series['manifest']['replica_ids']),
                    'aggregation': series['manifest']['aggregation'],
                    'evidence': 'descriptive only; local sources and time not independently certified; no inference'}

    def close(self, key, reason, *, write_key=None, retry_of=None):
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise RegistrationError('series closure reason required')
        self.repo.initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            _load(db, key)
            if write_key is None:
                prior = db.execute('SELECT closed_at,reason FROM series_closures WHERE series_key=?', (key,)).fetchone()
                if prior:
                    return {'closed_at': prior[0], 'reason': prior[1]}
                now = self.repo._now().isoformat()
                db.execute('INSERT INTO series_closures VALUES (?, ?, ?)', (key, now, reason.strip()))
                return {'closed_at': now, 'reason': reason.strip()}
            from dlt.prospective_closure_writes import accept
            return accept(db, write_key, 'series_close', {'object_key': key, 'reason': reason}, retry_of,
                          closed_at=lambda: self.repo._now().isoformat(),
                          existing=lambda: db.execute('SELECT closed_at,reason FROM series_closures WHERE series_key=?',
                                                      (key,)).fetchone(),
                          insert=lambda result: db.execute('INSERT INTO series_closures VALUES (?,?,?)',
                                                          (key, result['closed_at'], result['reason'])))
