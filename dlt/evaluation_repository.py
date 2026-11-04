"""Persist fixed-scenario evaluations linked to immutable research drafts."""
from contextlib import closing
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import re
from pathlib import Path
import sqlite3

from dlt.candidates import CandidateError, _canonical
from dlt.evaluation import EvaluationError, evaluate_pair
from dlt.pair_repository import PairRepository
from dlt.snapshots import InvalidSnapshot, parse_snapshot
from dlt.models import TrainingError
from dlt.settlement import exact_money_context, require_tax_unit
from dlt import evaluation as evaluation_module, settlement as settlement_module
from dlt.holdout_reservation import exploration_activity
from dlt.custom_scenarios import check_custom_scenario


def _legacy_default_precision_safe(pair, scenario):
    """Conservatively bound intermediate digits under the old 28-digit context.

    A true result means the bounded nonnegative additions, products, tax
    quantization and subtraction fit 28 digits, assuming the old caller used
    Python's default context. This does not independently replay samples.
    """
    try:
        rule = scenario['rule']
        values = list(scenario['payouts'].values()) + [rule['prices']['base'], rule['prices']['additional']]
        rate_value = rule['tax']['rate']
        def amount(value):
            if isinstance(value, (bool, float)) or not isinstance(value, (str, int)) or len(str(value)) > 512:
                raise ValueError('invalid amount')
            number = Decimal(str(value).replace(',', ''))
            if (not number.is_finite() or number < 0 or len(number.as_tuple().digits) > 128
                    or abs(number.as_tuple().exponent) > 128 or number.adjusted() > 128):
                raise ValueError('amount out of bounds')
            return number
        parsed, rate = [amount(x) for x in values], amount(rate_value)
        if not parsed or rate > 1:
            return False
        totals = []
        for label in ('candidate', 'control'):
            tickets = pair[label]['tickets']
            if not isinstance(tickets, list) or not tickets or len(tickets) > 10000:
                return False
            multipliers = [ticket['multiplier'] for ticket in tickets]
            if any(type(m) is not int or not 1 <= m <= 1000000 for m in multipliers):
                return False
            totals.append(sum(multipliers))
        whole = max(1, max(n.adjusted()+1 for n in parsed)) + len(str(2*max(totals))) + 1
        scale = max(0, max(-n.as_tuple().exponent for n in parsed))
        rate_scale = max(0, -rate.as_tuple().exponent)
        return whole + max(2, scale, scale + rate_scale) <= 28
    except (KeyError, TypeError, ValueError, InvalidOperation, AttributeError):
        return False


class EvaluationRepository:
    def __init__(self, path):
        self.path = Path(path)
        self.pairs = PairRepository(path)

    def _initialize(self):
        self.pairs._initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS draft_evaluations (
                    request_key TEXT PRIMARY KEY, pair_key TEXT NOT NULL REFERENCES draft_pairs(request_key),
                    request_json BLOB NOT NULL, result_sha256 TEXT NOT NULL, result_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS draft_evaluations_no_update BEFORE UPDATE ON draft_evaluations
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation'); END;
                CREATE TRIGGER IF NOT EXISTS draft_evaluations_no_delete BEFORE DELETE ON draft_evaluations
                    BEGIN SELECT RAISE(ABORT, 'immutable evaluation'); END;
            ''')
            from dlt.pair_evaluation_writes import initialize as initialize_write_protocol
            initialize_write_protocol(db, evaluation=True)

    @staticmethod
    def _decode(row):
        data = row[1]
        if hashlib.sha256(data).hexdigest() != row[0]:
            raise EvaluationError('stored evaluation integrity check failed')
        return json.loads(data)

    @staticmethod
    def _check_result_shape(result):
        """Reject damaged values before exposing a result to the interface."""
        try:
            samples = result['sampling']['samples']
            if type(samples) is not int or samples < 1:
                raise ValueError('invalid sample count')
            for label in ('candidate', 'control'):
                item = result['portfolios'][label]
                numbers = [item['cost_yuan'], item['mean_gross_tax_before']['yuan'],
                           item['jackpot_coverage']['probability']]
                numbers.extend(item['net_quantiles_yuan'][key] for key in ('p05', 'p50', 'p95'))
                if any(not Decimal(str(value)).is_finite() for value in numbers):
                    raise ValueError('invalid monetary or probability value')
                coverage = Decimal(str(item['jackpot_coverage']['probability']))
                quantiles = [Decimal(str(item['net_quantiles_yuan'][key])) for key in ('p05', 'p50', 'p95')]
                if (not Decimal(0) <= coverage <= Decimal(1) or Decimal(str(item['cost_yuan'])) < 0
                        or not quantiles[0] <= quantiles[1] <= quantiles[2]):
                    raise ValueError('invalid coverage')
                fraction = item['mean_gross_tax_before']['fraction']
                if (not isinstance(fraction, list) or len(fraction) != 2
                        or any(type(value) is not int for value in fraction) or fraction[0] < 0 or fraction[1] <= 0):
                    raise ValueError('invalid exact mean')
                with localcontext() as context:
                    context.prec = 28
                    if Decimal(str(item['mean_gross_tax_before']['yuan'])) != Decimal(fraction[0]) / Decimal(fraction[1]):
                        raise ValueError('exact mean disagrees with its decimal value')
                for metric in ('any_prize', 'break_even', 'profit'):
                    value = item[metric]
                    if (type(value['successes']) is not int or value['samples'] != samples
                            or not 0 <= value['successes'] <= samples):
                        raise ValueError('invalid observed count')
                    bounds = value['interval_95']
                    if not isinstance(bounds, list) or len(bounds) != 2:
                        raise ValueError('invalid simulation interval')
                    probabilities = [Decimal(str(x)) for x in [value['estimate'], *bounds]]
                    if any(not p.is_finite() or not Decimal(0) <= p <= Decimal(1) for p in probabilities):
                        raise ValueError('invalid simulation probability')
                    if (abs(float(probabilities[0]) - value['successes']/samples) > 1e-12
                            or probabilities[1] > probabilities[2]
                            or probabilities[0] + Decimal('1e-12') < probabilities[1]
                            or probabilities[0] - Decimal('1e-12') > probabilities[2]):
                        raise ValueError('simulation probability disagrees with count or interval')
        except (KeyError, TypeError, ValueError, InvalidOperation, AttributeError) as exc:
            raise EvaluationError('评估结果字段损坏') from exc

    @staticmethod
    def _check_scenario(db, scenario):
        dependency = scenario.get('source_dependency') if isinstance(scenario, dict) else None
        if dependency is None:
            return  # legacy synthetic scenarios have no stored source dependency
        if not isinstance(dependency, dict) or not all(isinstance(dependency.get(k), str)
                                                        for k in ('snapshot_id', 'issue', 'row_sha256')):
            raise EvaluationError('奖金情景来源信息无效')
        if scenario.get('source_issue') != dependency['issue']:
            raise EvaluationError('奖金情景期号与来源不一致')
        source = db.execute('SELECT canonical_json FROM snapshots WHERE id=?', (dependency['snapshot_id'],)).fetchone()
        row = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?',
                         (dependency['snapshot_id'], dependency['issue'])).fetchone()
        if (not source or hashlib.sha256(source[0]).hexdigest() != dependency['snapshot_id'] or not row
                or hashlib.sha256(row[0]).hexdigest() != dependency['row_sha256']):
            raise EvaluationError('奖金情景来源缺失或摘要不符')
        try:
            _, parsed = parse_snapshot(source[0])
            matching = [item for item in parsed if item['issue'] == dependency['issue']]
            if len(matching) != 1 or _canonical(matching[0]['raw_row']) != row[0]:
                raise EvaluationError('奖金情景与原快照不一致')
        except InvalidSnapshot as exc:
            raise EvaluationError('奖金情景快照无法解析') from exc
        versions = db.execute('SELECT DISTINCT row_json FROM draw_versions WHERE issue=?', (dependency['issue'],)).fetchall()
        if len(versions) != 1:
            raise EvaluationError('奖金情景来源有冲突，需复核')

    def get(self, request_key):
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT result_sha256, result_json FROM draft_evaluations WHERE request_key=?',
                             (request_key,)).fetchone()
        if row is None:
            raise KeyError(request_key)
        return self._decode(row)

    def get_request(self, request_key):
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        self._initialize()
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT request_json FROM draft_evaluations WHERE request_key=?', (request_key,)).fetchone()
        if not row:
            raise KeyError(request_key)
        return json.loads(row[0])

    def inspect(self, request_key):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            return self.inspect_in_transaction(db, request_key)

    @exact_money_context
    def inspect_in_transaction(self, db, request_key):
        row = db.execute('SELECT pair_key, request_json, result_sha256, result_json FROM draft_evaluations WHERE request_key=?', (request_key,)).fetchone()
        if not row:
            raise KeyError(request_key)
        result = None
        try:
            result = self._decode(row[2:])
            self._check_result_shape(result)
            request = json.loads(row[1])
            scenario = request['scenario']
            require_tax_unit(scenario['rule']['tax'])
            if (request['pair_key'] != row[0]
                    or result['scenario_sha256'] != hashlib.sha256(_canonical(scenario)).hexdigest()
                    or result['scenario']['source_issue'] != scenario['source_issue']
                    or result['sampling']['samples'] != request['samples']
                    or result['sampling']['seed'] != request['seed']):
                raise EvaluationError('评估结果与固定请求不一致')
            self._check_scenario(db, scenario)
            pair = self.pairs._checked_pair(db, row[0])
            check_custom_scenario(pair, scenario)
            if (pair['pair_sha256'] != result['pair_sha256']
                    or result['scenario']['target_issue'] != pair['config']['target_issue']):
                raise EvaluationError('评估结果与组合摘要或目标期不一致')
            for label in ('candidate', 'control'):
                if Decimal(result['portfolios'][label]['cost_yuan']) * 100 != pair[label]['cost_fen']:
                    raise EvaluationError('评估成本与组合不一致')
            sampling = result['sampling']
            current_engine = (sampling.get('calculation_version') == 'exact-money-v1'
                              and sampling.get('code_sha256') == hashlib.sha256(Path(evaluation_module.__file__).read_bytes()).hexdigest()
                              and sampling.get('settlement_code_sha256') == hashlib.sha256(Path(settlement_module.__file__).read_bytes()).hexdigest())
            if not current_engine and not _legacy_default_precision_safe(pair, scenario):
                raise EvaluationError('旧版金额超出默认精度的保障范围，请按原条件重新评估；原记录仍保留')
        except (EvaluationError, CandidateError, TrainingError, ValueError, TypeError, KeyError, AttributeError, InvalidOperation) as exc:
            return {'result': result, 'status': 'review_required', 'reason': str(exc)}
        return {'result': result, 'status': 'research_draft', 'reason': 'rule and calendar references not verified'}

    def latest_for_pair(self, pair_key):
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        self._initialize()
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT request_key, request_json FROM draft_evaluations WHERE pair_key=? ORDER BY rowid DESC LIMIT 1',
                             (pair_key,)).fetchone()
        if not row:
            return None
        return {'request_key': row[0], 'request': json.loads(row[1]), **self.inspect(row[0])}

    def list_for_pair(self, pair_key, *, limit=30):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise EvaluationError('invalid evaluation history limit')
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        self._initialize()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            rows = db.execute('SELECT request_key,request_json FROM draft_evaluations WHERE pair_key=? '
                              'ORDER BY rowid DESC LIMIT ?', (pair_key, limit)).fetchall()
            entries = []
            for key, raw in rows:
                try:
                    request = json.loads(raw)
                except (ValueError, TypeError):
                    request = {}
                entries.append({'request_key': key, 'request': request, **self.inspect_in_transaction(db, key)})
            return entries

    def comparison_options(self, *, limit=100):
        """Return bounded summaries of saved evaluations in one read snapshot."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise EvaluationError('invalid comparison list limit')
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        self._initialize()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            rows = db.execute('SELECT request_key,pair_key,request_json FROM draft_evaluations '
                              'ORDER BY rowid DESC LIMIT ?', (limit,)).fetchall()
            output = []
            for key, pair_key, raw in rows:
                checked = self.inspect_in_transaction(db, key)
                item = {'request_key': key, 'pair_key': pair_key, 'status': checked['status'],
                        'reason': checked['reason']}
                try:
                    request = json.loads(raw)
                    scenario = request['scenario']
                    dep = scenario.get('source_dependency', {})
                    pair = self.pairs._checked_pair(db, pair_key)
                    if scenario.get('kind') == 'custom_payout_v1':
                        check_custom_scenario(pair, scenario)
                        source = {'source_issue': 'custom', 'snapshot_id': None,
                                  'scenario_kind': 'custom_payout_v1', 'scenario_label': scenario['custom_input']['label']}
                    else:
                        source = {'source_issue': dep['issue'], 'snapshot_id': dep['snapshot_id'], 'scenario_kind': 'historical'}
                    item.update({**source,
                                 'target_issue': pair['config']['target_issue'],
                                 'method': pair['config']['method'], 'structure': pair['config']['structure'],
                                 'budget_fen': pair['config']['budget_fen'],
                                 'cost_fen': pair['candidate']['cost_fen'],
                                 'samples': request['samples'], 'seed': request['seed']})
                except (KeyError, ValueError, TypeError, AttributeError, CandidateError, TrainingError):
                    item['status'] = 'review_required'
                    item['reason'] = '组合或历史情景信息不可用'
                output.append(item)
            return output

    def compare_pairs(self, keys):
        """Compare distinct pairs under equivalent fixed payout assumptions."""
        if (not isinstance(keys, list) or not 2 <= len(keys) <= 3
                or any(not isinstance(key, str) or not key.strip() or len(key) > 128 for key in keys)
                or len(set(keys)) != len(keys)):
            raise EvaluationError('请选择 2—3 条不同的评估记录')
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        self._initialize()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            entries, seen_pairs, reference = [], set(), None
            for key in keys:
                row = db.execute('SELECT pair_key,request_json FROM draft_evaluations WHERE request_key=?', (key,)).fetchone()
                if not row:
                    raise EvaluationError('所选评估记录不存在')
                pair_key, raw = row
                checked = self.inspect_in_transaction(db, key)
                if checked['status'] != 'research_draft':
                    raise EvaluationError('所选评估需要复核：' + checked['reason'])
                if pair_key in seen_pairs:
                    raise EvaluationError('请选择不同的组合草稿')
                seen_pairs.add(pair_key)
                request = json.loads(raw)
                scenario = request['scenario']
                dependency = scenario.get('source_dependency')
                if not isinstance(dependency, dict) and scenario.get('kind') != 'custom_payout_v1':
                    raise EvaluationError('所选情景缺少可核对的历史来源')
                pair = self.pairs._checked_pair(db, pair_key)
                rule = scenario['rule']
                comparable_rule = {k: v for k, v in rule.items() if k not in ('prices', 'applicable_issues')}
                comparable_scenario = {**scenario, 'rule': comparable_rule,
                                       'target_issue': pair['config']['target_issue']}
                if scenario.get('kind') == 'custom_payout_v1':
                    # The descriptive label is not a payout assumption.
                    comparable_scenario.pop('reference', None)
                    comparable_scenario['custom_input'] = {k: v for k, v in scenario['custom_input'].items() if k != 'label'}
                signature = hashlib.sha256(_canonical(comparable_scenario)).hexdigest()
                if reference is not None and reference != signature:
                    raise EvaluationError('所选记录的目标期、情景来源或奖金税费假设不一致')
                reference = signature
                entries.append({'request_key': key, 'pair_key': pair_key, 'request': request,
                                'result': checked['result'], 'pair': pair})
            return {'entries': entries, 'scenario_sha256': reference,
                    'assurance': 'fixed payout research assumptions; not an adoption recommendation'}

    def run(self, request_key, pair_key, *, scenario, samples, seed, max_samples, max_ticket_draws,
            write_request=None, retry_of=None):
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
            raise EvaluationError('request key must be 1 to 128 characters')
        request = _canonical({'pair_key': pair_key, 'scenario': scenario, 'samples': samples, 'seed': seed,
                              'max_samples': max_samples, 'max_ticket_draws': max_ticket_draws})
        self._initialize()
        if write_request is not None:
            with closing(sqlite3.connect(self.path)) as db:
                if db.execute('SELECT 1 FROM evaluation_write_fences WHERE request_key=?', (request_key,)).fetchone():
                    raise EvaluationError('evaluation request was fenced; late simulation rejected')
        with closing(sqlite3.connect(self.path)) as db:
            prior = db.execute('SELECT request_json,result_sha256,result_json FROM draft_evaluations WHERE request_key=?',
                               (request_key,)).fetchone()
            if prior:
                if prior[0] != request:
                    raise EvaluationError('request key already used for different input')
                result = self._decode(prior[1:])
                if write_request is not None:
                    try:
                        with closing(sqlite3.connect(self.path)) as write_db, write_db:
                            write_db.execute('PRAGMA foreign_keys=ON')
                            write_db.execute('BEGIN IMMEDIATE')
                            from dlt.pair_evaluation_writes import record_acceptance
                            record_acceptance(write_db, 'evaluation', request_key, write_request,
                                              {'request_key': request_key, 'request': json.loads(request),
                                               'result': result}, retry_of=retry_of)
                    except ValueError as exc:
                        raise EvaluationError(str(exc)) from exc
                return result
        dependency = scenario.get('source_dependency') if isinstance(scenario, dict) else None
        args = {'scenario': scenario, 'samples': samples, 'seed': seed,
                'max_samples': max_samples, 'max_ticket_draws': max_ticket_draws}
        if isinstance(scenario, dict) and scenario.get('kind') == 'custom_payout_v1':
            provenance = scenario.get('custom_input', {}).get('provenance')
            if not isinstance(provenance, dict) or provenance.get('kind') not in ('synthetic', 'historical'):
                raise EvaluationError('自定义奖金需声明来源；既有记录可查看，请补充来源后新建评估')
            if provenance['kind'] == 'historical':
                issue = provenance.get('issue')
                if not isinstance(issue, str) or not re.fullmatch(r'[0-9]{5}', issue):
                    raise EvaluationError('历史奖金来源期号无效')
                with exploration_activity(self.path, kind='portfolio_scenario_evaluation', inputs=(), targets=(issue,)):
                    return self._run_new(request_key, pair_key, write_request=write_request,
                                         retry_of=retry_of, **args)
            return self._run_new(request_key, pair_key, write_request=write_request, retry_of=retry_of, **args)
        if dependency is None:
            issue = scenario.get('source_issue') if isinstance(scenario, dict) else None
            # Without a provenance reference, a five-digit label may still
            # designate actual historical winnings; count it conservatively.
            if not isinstance(issue, str) or not re.fullmatch(r'[0-9]{5}', issue):
                return self._run_new(request_key, pair_key, write_request=write_request,
                                     retry_of=retry_of, **args)
        else:
            issue = dependency.get('issue') if isinstance(dependency, dict) else None
            if not isinstance(issue, str) or not re.fullmatch(r'[0-9]{5}', issue):
                raise EvaluationError('奖金情景来源期号无效')
            if scenario.get('source_issue') != issue:
                raise EvaluationError('奖金情景期号与来源不一致')
        with exploration_activity(self.path, kind='portfolio_scenario_evaluation', inputs=(), targets=(issue,)):
            return self._run_new(request_key, pair_key, write_request=write_request, retry_of=retry_of, **args)

    def _run_new(self, request_key, pair_key, *, scenario, samples, seed, max_samples, max_ticket_draws,
                 write_request=None, retry_of=None):
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
            raise EvaluationError('request key must be 1 to 128 characters')
        request = _canonical({'pair_key': pair_key, 'scenario': scenario, 'samples': samples, 'seed': seed,
                              'max_samples': max_samples, 'max_ticket_draws': max_ticket_draws})
        self._initialize()
        with closing(sqlite3.connect(self.path)) as db:
            prior = db.execute('SELECT request_json, result_sha256, result_json FROM draft_evaluations WHERE request_key=?',
                               (request_key,)).fetchone()
            if prior:
                if prior[0] != request:
                    raise EvaluationError('request key already used for different input')
                result = self._decode(prior[1:])
                if write_request is not None:
                    try:
                        with closing(sqlite3.connect(self.path)) as write_db, write_db:
                            write_db.execute('PRAGMA foreign_keys=ON')
                            write_db.execute('BEGIN IMMEDIATE')
                            from dlt.pair_evaluation_writes import record_acceptance
                            record_acceptance(write_db, 'evaluation', request_key, write_request,
                                              {'request_key': request_key, 'request': json.loads(request),
                                               'result': result}, retry_of=retry_of)
                    except ValueError as exc:
                        raise EvaluationError(str(exc)) from exc
                return result
            try:
                pair = self.pairs._checked_pair(db, pair_key)
                self._check_scenario(db, scenario)
                check_custom_scenario(pair, scenario)
            except CandidateError as exc:
                raise EvaluationError(str(exc)) from exc
        result = evaluate_pair(pair, scenario=scenario, samples=samples, seed=seed,
                               max_samples=max_samples, max_ticket_draws=max_ticket_draws)
        payload = _canonical(result)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            if write_request is not None and db.execute(
                    'SELECT 1 FROM evaluation_write_fences WHERE request_key=?', (request_key,)).fetchone():
                raise EvaluationError('evaluation request was fenced; late simulation rejected')
            prior = db.execute('SELECT request_json, result_sha256, result_json FROM draft_evaluations WHERE request_key=?',
                               (request_key,)).fetchone()
            if prior:
                if prior[0] != request:
                    raise EvaluationError('request key already used for different input')
                return self._decode(prior[1:])
            try:
                current = self.pairs._checked_pair(db, pair_key)
                self._check_scenario(db, scenario)
                check_custom_scenario(current, scenario)
            except CandidateError as exc:
                raise EvaluationError(str(exc)) from exc
            if current['pair_sha256'] != result['pair_sha256']:
                raise EvaluationError('pair changed during evaluation')
            db.execute('INSERT INTO draft_evaluations VALUES (?, ?, ?, ?, ?)',
                       (request_key, pair_key, request, hashlib.sha256(payload).hexdigest(), payload))
            if write_request is not None:
                from dlt.pair_evaluation_writes import record_acceptance
                record_acceptance(db, 'evaluation', request_key, write_request,
                                  {'request_key': request_key, 'request': json.loads(request), 'result': result},
                                  retry_of=retry_of)
        return result
