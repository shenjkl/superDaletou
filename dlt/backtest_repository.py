"""Append-only storage and dependency review for reconstruction reports."""
from contextlib import closing
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import sqlite3

from dlt.backtest import BacktestError, run_from_snapshot, _validate
from dlt.candidates import _canonical
from dlt.model_repository import _assert_consistent_history
from dlt.models import TrainingError
from dlt.snapshots import SnapshotStore
from dlt.snapshots import InvalidSnapshot, parse_snapshot
from dlt.holdout_reservation import check_backtest_allowed
from dlt.paired_description import describe_backtest, PairedDescriptionError
from dlt.settlement import SettlementBlocked, exact_money_context, settle_portfolio
from dlt.historical_scenarios import amounts_match, promotion_data, published_payouts
from dlt import backtest as backtest_module, settlement as settlement_module


def _fraction(value):
    if not isinstance(value, (str, int)) or isinstance(value, bool) or len(str(value)) > 512:
        raise BacktestError('invalid stored monetary value')
    amount = Decimal(str(value).replace(',', ''))
    if not amount.is_finite() or abs(amount.as_tuple().exponent) > 256:
        raise BacktestError('invalid stored monetary value')
    return Fraction(amount)


@exact_money_context
def _recheck_settlements(db, report, *, rebuild=False):
    """Check or rebuild only saved settlement amounts, without model training."""
    try:
        if rebuild:
            report = deepcopy(report)
        protocol, scenarios = report['protocol'], report['scenarios']
        if (report['protocol_sha256'] != hashlib.sha256(_canonical(protocol)).hexdigest()
                or report['scenarios_sha256'] != hashlib.sha256(_canonical(scenarios)).hexdigest()):
            raise BacktestError('historical protocol or scenario digest mismatch')
        snapshot_id = protocol['snapshot_id']
        snapshot = db.execute('SELECT canonical_json FROM snapshots WHERE id=?', (snapshot_id,)).fetchone()
        if not snapshot or hashlib.sha256(snapshot[0]).hexdigest() != snapshot_id:
            raise BacktestError('historical snapshot missing or damaged')
        _, parsed = parse_snapshot(snapshot[0])
        source = {item['issue']: item for item in parsed}
        for period in report['periods']:
            issue = period['target']['issue']
            if issue not in source:
                raise BacktestError('historical draw missing')
            source_row = db.execute('SELECT row_json,front_json,back_json FROM draw_versions '
                                    'WHERE snapshot_id=? AND issue=?', (snapshot_id, issue)).fetchone()
            original = source[issue]
            if (not source_row or source_row[0] != _canonical(original['raw_row'])
                    or json.loads(source_row[1]) != original['front']
                    or json.loads(source_row[2]) != original['back']
                    or period['target']['date'] != original['draw_date']):
                raise BacktestError('historical draw differs from retained snapshot')
            scenario = scenarios.get(issue)
            if scenario is None:
                # Preserve periods that were explicitly retained as incomplete
                # because their historical payout protocol was unavailable.
                continue
            promotion, published_promotion_payouts = promotion_data(original['raw_row'], issue)
            if scenario.get('promotion') is not promotion:
                raise BacktestError('派奖场景与留存开奖标记不一致')
            expected_payouts = published_payouts(original['raw_row'], scenario.get('rule'),
                                                 protocol['candidate']['additional'])
            if not amounts_match(scenario.get('payouts'), expected_payouts):
                raise BacktestError('基本/追加奖金映射与留存开奖字段不一致')
            if promotion:
                supplied = scenario.get('promotion_payouts')
                if not amounts_match(supplied, published_promotion_payouts):
                    raise BacktestError('派奖奖金金额与留存开奖字段不一致')
            completed = [replica for replica in period['replicas'] if replica['status'] == 'complete']
            if not completed:
                continue
            draw = {'issue': issue, 'front': original['front'], 'back': original['back'],
                    'promotion': scenario['promotion']}
            for replica in completed:
                pair = replica['pair']
                content = dict(pair); digest = content.pop('pair_sha256')
                if (digest != hashlib.sha256(_canonical(content)).hexdigest()
                        or pair['config']['target_issue'] != issue):
                    raise BacktestError('historical ticket pair digest mismatch')
                computed = []
                for label in ('candidate', 'control'):
                    expected = settle_portfolio(issue=issue, draw=draw, tickets=pair[label]['tickets'],
                                                payouts=scenario['payouts'], rule=scenario['rule'], duplicate_policy='reject',
                                                ticket_groups=pair[label].get('ticket_groups'),
                                                promotion_payouts=scenario.get('promotion_payouts'))
                    saved = replica[label+'_settlement']
                    expected_keys = ('issue', 'rule_version', 'winning_tickets', 'gross', 'tax', 'cost', 'net')
                    if promotion:
                        expected_keys += ('promotion_bonus', 'promotion_qualified_ticket_groups')
                    if (not isinstance(saved, dict) or any(k not in saved for k in expected_keys)
                            or any(expected[k] != saved[k] for k in ('issue', 'rule_version'))):
                        raise BacktestError('原始结算记录不完整或引用不一致')
                    if _fraction(expected['cost'])*100 != pair[label]['cost_fen']:
                        raise BacktestError('固定票包成本与核算成本不一致')
                    if not rebuild and (any(expected[k] != saved[k] for k in expected_keys[:3])
                            or (promotion and expected['promotion_qualified_ticket_groups'] != saved['promotion_qualified_ticket_groups'])
                            or any(_fraction(expected[k]) != _fraction(saved[k]) for k in ('gross', 'tax', 'cost', 'net'))):
                        raise BacktestError('历史回测金额与当前精确计算不一致，需要重新核算；原报告仍保留')
                    if not rebuild and promotion and _fraction(expected['promotion_bonus']) != _fraction(saved['promotion_bonus']):
                        raise BacktestError('历史回测派奖金额与当前精确计算不一致，需要重新核算；原报告仍保留')
                    if rebuild:
                        replica[label+'_settlement'] = expected
                    computed.append(expected)
                if _fraction(computed[0]['cost']) != _fraction(computed[1]['cost']):
                    raise BacktestError('固定候选与对照成本不一致')
                if rebuild:
                    replica['difference_yuan'] = str(Decimal(computed[0]['net'])-Decimal(computed[1]['net']))
                elif _fraction(replica['difference_yuan']) != _fraction(computed[0]['net'])-_fraction(computed[1]['net']):
                    raise BacktestError('历史回测配对差额与当前精确计算不一致，需要重新核算')
            if rebuild and period['status'] == 'complete':
                differences = [_fraction(replica['difference_yuan']) for replica in period['replicas']]
                mean = sum(differences, Fraction(0))/len(differences)
                period['mean_difference_fraction'] = [mean.numerator, mean.denominator]
        if rebuild:
            completed = [period for period in report['periods'] if period['status'] == 'complete']
            mean = (sum((Fraction(*period['mean_difference_fraction']) for period in completed), Fraction(0))
                    / len(completed) if completed else None)
            report['descriptive_mean_fraction'] = [mean.numerator, mean.denominator] if mean is not None else None
            describe_backtest(report)
        return report
    except (InvalidOperation, InvalidSnapshot, SettlementBlocked, KeyError, ValueError, TypeError, AttributeError, ZeroDivisionError) as exc:
        if isinstance(exc, BacktestError):
            raise
        raise BacktestError('历史回测金额或重算依据无法核验') from exc


class BacktestRepository:
    def __init__(self, path):
        self.path = Path(path)

    def _initialize(self):
        if not self.path.is_file():
            raise BacktestError('stored snapshot not found')
        SnapshotStore(self.path).initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS backtest_reports (
                    request_key TEXT PRIMARY KEY, request_json BLOB NOT NULL,
                    report_sha256 TEXT NOT NULL, report_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS backtest_reports_no_update BEFORE UPDATE ON backtest_reports
                    BEGIN SELECT RAISE(ABORT, 'immutable backtest report'); END;
                CREATE TRIGGER IF NOT EXISTS backtest_reports_no_delete BEFORE DELETE ON backtest_reports
                    BEGIN SELECT RAISE(ABORT, 'immutable backtest report'); END;
            ''')

    @staticmethod
    def _decode(row):
        if hashlib.sha256(row[1]).hexdigest() != row[0]:
            raise BacktestError('report integrity check failed')
        return json.loads(row[1])

    def get(self, request_key):
        """Historical audit read; use inspect for current source status."""
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT report_sha256, report_json FROM backtest_reports WHERE request_key=?',
                             (request_key,)).fetchone()
        if row is None:
            raise KeyError(request_key)
        return self._decode(row)

    @staticmethod
    def _check_dependencies(db, report, *, check_calculation=True):
        p = report['protocol']
        from dlt.settlement import require_tax_unit
        from dlt.pair_validation import validate_pair_structure, validate_pair_history, validate_random_control
        from dlt.model_pair_validation import validate_model_candidate
        rows = db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date, issue',
                          (p['snapshot_id'],)).fetchall()
        source = [row[0] for row in rows]
        for period in report['periods']:
            if any(replica.get('status') == 'complete' for replica in period['replicas']):
                try:
                    require_tax_unit(report['scenarios'][period['target']['issue']]['rule']['tax'])
                    target = period['target']
                    index = source.index(target['issue'])
                    if index < p['history_periods']:
                        raise ValueError('historical window unavailable')
                    for replica in period['replicas']:
                        if replica['status'] != 'complete':
                            continue
                        pair = replica['pair']
                        config = {**p['candidate'], 'snapshot_id': p['snapshot_id'],
                                  'seed': backtest_module._seed(p['seed'], target['issue'], replica['replica']),
                                  'target_issue': target['issue'], 'target_date': target['date']}
                        validate_pair_structure(pair, config)
                        request = {'config': config, 'artifact_id': pair['model_artifact_id'],
                            'history_first_issue': source[index-p['history_periods']],
                            'history_cutoff_issue': source[index-1]}
                        history = validate_pair_history(db, pair, request)
                        if not validate_random_control(pair, history):
                            raise BacktestError('原生成代码或运行时不同，随机对照无法重放')
                        if config['method'] in ('tree', 'lstm'):
                            if not validate_model_candidate(db, pair, request, replica['artifact']):
                                raise BacktestError('原模型代码或运行时不同，模型候选无法重放')
                except (KeyError, ValueError, TypeError) as exc:
                    raise BacktestError(f'历史回测的计税单位、配对结构或历史输入需复核；原记录仍保留：{exc}') from exc
        relevant = set()
        for issue in p['targets']:
            if issue not in source:
                raise BacktestError('target source unavailable')
            index = source.index(issue)
            relevant.update(source[max(0, index-p['history_periods']):index])
            variants = db.execute('SELECT DISTINCT row_json FROM draw_versions WHERE issue=?', (issue,)).fetchall()
            if len(variants) > 1:
                raise BacktestError('target draw or prize source has unresolved conflict')
        try:
            _assert_consistent_history(db, relevant)
        except TrainingError as exc:
            raise BacktestError(str(exc)) from exc
        if check_calculation and (report.get('calculation_version') != 'exact-money-v1'
                or report.get('code_sha256') != hashlib.sha256(Path(backtest_module.__file__).read_bytes()).hexdigest()
                or report.get('settlement_code_sha256') != hashlib.sha256(Path(settlement_module.__file__).read_bytes()).hexdigest()):
            _recheck_settlements(db, report)

    def inspect(self, request_key):
        report = self.get(request_key)
        protocol = report.get('protocol')
        if not isinstance(protocol, dict) or protocol.get('selection_role') != 'exploration':
            return {'report': report, 'status': 'review_required',
                    'reason': 'holdout role was not independently verified; historical record is audit-only'}
        try:
            with closing(sqlite3.connect(self.path)) as db:
                db.execute('BEGIN')
                self._check_dependencies(db, report)
            description = describe_backtest(report)
        except (BacktestError, PairedDescriptionError, KeyError, TypeError, sqlite3.Error) as exc:
            return {'report': report, 'status': 'review_required', 'reason': str(exc)}
        return {'report': report, 'status': 'research_only',
                'reason': 'scenario sources and holdout independence are not verified', 'paired_description': description}

    def run(self, request_key, protocol, scenarios):
        """Persist all expected outcomes and failures in one immutable report.

        A source conflict arriving during computation is retained as audit data
        but the returned status is review_required. Same-key retries return the
        original report; a changed request needs a new key.
        """
        if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 128:
            raise BacktestError('request key must be 1 to 128 characters')
        protocol = _validate(protocol)
        try:
            request = _canonical({'protocol': protocol, 'scenarios': scenarios})
        except (ValueError, TypeError) as exc:
            raise BacktestError('request must be finite JSON') from exc
        self._initialize()
        with closing(sqlite3.connect(self.path)) as db:
            prior = db.execute('SELECT request_json FROM backtest_reports WHERE request_key=?', (request_key,)).fetchone()
        if prior:
            if prior[0] != request:
                raise BacktestError('request key already used for different input')
            return self.inspect(request_key)
        report = run_from_snapshot(self.path, protocol, scenarios)
        payload = _canonical(report)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT request_json FROM backtest_reports WHERE request_key=?', (request_key,)).fetchone()
            if prior:
                if prior[0] != request:
                    raise BacktestError('request key already used for different input')
            else:
                check_backtest_allowed(db, protocol)
                db.execute('INSERT INTO backtest_reports VALUES (?, ?, ?, ?)',
                           (request_key, request, hashlib.sha256(payload).hexdigest(), payload))
        return self.inspect(request_key)
