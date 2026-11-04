"""Chronological rolling reconstruction of candidate/control portfolios.

This offline research engine deliberately does not claim historical source
availability, independently verified payout provenance, holdout independence,
or predictive advantage. It records potential data use but does not persist
jobs or publish formal evidence.
"""
from contextlib import closing
from decimal import Decimal
from fractions import Fraction
import hashlib
import json
import math
from pathlib import Path
import platform
import sqlite3

from dlt.candidates import CandidateError, _canonical, generate_pair
from dlt.backtest_limits import DEFAULT_BACKTEST_LIMITS
from dlt.model_repository import _assert_consistent_history
from dlt.models import (ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION,
                        PREPROCESSING_VERSION, TrainingError, predict, train)
from dlt.training_limits import DEFAULT_TRAINING_LIMITS
from dlt.settlement import SettlementBlocked, exact_money_context, settle_portfolio
from dlt import settlement as settlement_module
from dlt.snapshots import SnapshotStore
from dlt.holdout_reservation import check_backtest_allowed, exploration_activity
from dlt.historical_scenarios import amounts_match, promotion_data, published_payouts
from dlt.scenarios import ScenarioError


class BacktestError(ValueError):
    pass


def _seed(base, issue, replica):
    raw = _canonical({'version': 'backtest-seed-v1', 'base': base, 'issue': issue, 'replica': replica})
    return int.from_bytes(hashlib.sha256(raw).digest()[:4], 'big')


def _generate(history, target, protocol, replica):
    """Receive only prior outcomes plus target ID/date; no target result/payout."""
    seed = _seed(protocol['seed'], target['issue'], replica)
    cfg = {**protocol['candidate'], 'snapshot_id': protocol['snapshot_id'], 'seed': seed,
           'target_issue': target['issue'], 'target_date': target['date']}
    method = cfg['method']
    artifact = scores = None
    if method in ('tree', 'lstm'):
        spec = protocol['model']
        validation = spec['validation_periods']
        if len(history) <= spec['window'] + validation:
            raise TrainingError('insufficient history for model training and validation')
        training_cfg = {'method': method, 'window': spec['window'], 'train_end': history[-1-validation]['issue'],
                        'cutoff': history[-1]['issue'], 'availability_mode': 'historical_reconstruction',
                        'snapshot_id': protocol['snapshot_id'], 'seed': seed,
                        'algorithm_version': spec['algorithm_version'],
                        'feature_schema_version': spec['feature_schema_version'],
                        'preprocessing_version': spec['preprocessing_version'],
                        'parameters': spec['parameters']}
        artifact = train(history, training_cfg)
        scores = predict(artifact, history[-spec['window']:], target_issue=target['issue'], target_date=target['date'])
    return generate_pair(history=history, config=cfg, model_scores=scores), artifact


def _validate(protocol):
    if not isinstance(protocol, dict):
        raise BacktestError('protocol object required')
    if protocol.get('mode') != 'historical_reconstruction' or protocol.get('analysis') != 'descriptive':
        raise BacktestError('only descriptive historical reconstruction is supported')
    if protocol.get('aggregation') != 'arithmetic_mean_v1':
        raise BacktestError('explicit arithmetic_mean_v1 replica aggregation required')
    targets, replicas = protocol.get('targets'), protocol.get('replicas')
    if (not isinstance(targets, list) or not targets or not all(isinstance(x, str) and x for x in targets)
            or len(set(targets)) != len(targets)):
        raise BacktestError('distinct target manifest required')
    if (not isinstance(replicas, list) or not replicas or not all(isinstance(x, str) and x for x in replicas)
            or len(set(replicas)) != len(replicas)):
        raise BacktestError('distinct replica manifest required')
    limits = protocol.get('limits')
    if (not isinstance(limits, dict) or any(type(limits.get(k)) is not int or limits[k] < 1
                                             for k in ('max_targets', 'max_replicas', 'max_history'))
            or limits['max_targets'] > DEFAULT_BACKTEST_LIMITS.max_targets
            or limits['max_replicas'] > DEFAULT_BACKTEST_LIMITS.max_replicas
            or limits['max_history'] > DEFAULT_BACKTEST_LIMITS.max_history_periods
            or len(targets) > limits['max_targets'] or len(replicas) > limits['max_replicas']
            or len(targets)*len(replicas) > DEFAULT_BACKTEST_LIMITS.max_target_replica_pairs):
        raise BacktestError('explicit resource limits required')
    if (type(protocol.get('seed')) is not int or not 0 <= protocol['seed'] <= 4294967295
            or type(protocol.get('history_periods')) is not int
            or not DEFAULT_BACKTEST_LIMITS.min_history_periods <= protocol['history_periods'] <= limits['max_history']):
        raise BacktestError('invalid seed or history length')
    if protocol.get('selection_role') != 'exploration':
        raise BacktestError('only exploration is supported; holdout independence is not verified')
    for issue in targets:
        seeds = [_seed(protocol['seed'], issue, replica) for replica in replicas]
        if len(set(seeds)) != len(seeds):
            raise BacktestError('replica seed collision; change the manifest or seed')
    candidate = protocol.get('candidate')
    if not isinstance(candidate, dict) or candidate.get('method') not in ('random', 'hot', 'cold', 'tree', 'lstm'):
        raise BacktestError('valid candidate method required')
    if any(not isinstance(candidate.get(key), str) or not candidate[key].strip() or len(candidate[key]) > 128
           for key in ('rule_version', 'calendar_version')):
        raise BacktestError('rule and calendar versions required')
    if (type(candidate.get('max_tickets')) is not int
            or not 1 <= candidate['max_tickets'] <= DEFAULT_BACKTEST_LIMITS.max_tickets
            or type(candidate.get('max_sampling_attempts')) is not int
            or not 1 <= candidate['max_sampling_attempts'] <= DEFAULT_BACKTEST_LIMITS.max_sampling_attempts
            or type(candidate.get('budget_fen')) is not int
            or not DEFAULT_BACKTEST_LIMITS.budget_fen[0] <= candidate['budget_fen'] <= DEFAULT_BACKTEST_LIMITS.budget_fen[1]
            or type(candidate.get('window')) is not int or not 1 <= candidate['window'] <= protocol['history_periods']
            or candidate.get('base_price_fen') != 200 or candidate.get('additional_price_fen') != 300
            or type(candidate.get('additional')) is not bool):
        raise BacktestError('candidate resource limits or cost configuration invalid')
    structure = candidate.get('structure')
    if structure not in ('singles', 'compound', 'dantuo'):
        raise BacktestError('supported candidate structure required')
    front_count, back_count = candidate.get('front_count', 5), candidate.get('back_count', 2)
    if (structure == 'compound'
            and (type(front_count) is not int or not 5 <= front_count <= 35
                 or type(back_count) is not int or not 2 <= back_count <= 12)):
        raise BacktestError('compound combination counts invalid')
    ticket_price = candidate['additional_price_fen'] if candidate['additional'] else candidate['base_price_fen']
    ticket_count = (candidate['budget_fen']//ticket_price if structure == 'singles'
                    else math.comb(front_count, 5)*math.comb(back_count, 2) if structure == 'compound' else 18)
    if (ticket_count < 1 or ticket_count > candidate['max_tickets']
            or ticket_count*ticket_price > candidate['budget_fen']):
        raise BacktestError('budget cannot contain the requested candidate structure')
    if candidate['method'] in ('tree', 'lstm'):
        model = protocol.get('model')
        if (not isinstance(model, dict) or type(model.get('window')) is not int or model['window'] < 1
                or model['window'] > DEFAULT_TRAINING_LIMITS.window_periods[1]
                or type(model.get('validation_periods')) is not int or model['validation_periods'] < 1
                or model['validation_periods'] > DEFAULT_BACKTEST_LIMITS.max_model_validation_periods
                or not isinstance(model.get('parameters'), dict)
                or model.get('algorithm_version') != ALGORITHM_VERSION[candidate['method']]
                or model.get('feature_schema_version') != FEATURE_SCHEMA_VERSION
                or model.get('preprocessing_version') != PREPROCESSING_VERSION[candidate['method']]
                or protocol['history_periods'] <= model['window'] + model['validation_periods']):
            raise BacktestError('explicit model window, validation length and parameters required')
        temperature = candidate.get('score_temperature')
        if (type(temperature) not in (int, float) or not math.isfinite(temperature)
                or temperature <= 0):
            raise BacktestError('positive finite model score temperature required')
        params = model['parameters']
        if candidate['method'] == 'tree':
            if (set(params) != {'max_depth', 'min_leaf'}
                    or type(params.get('max_depth')) is not int
                    or not DEFAULT_TRAINING_LIMITS.tree_max_depth[0] <= params['max_depth'] <= DEFAULT_TRAINING_LIMITS.tree_max_depth[1]
                    or type(params.get('min_leaf')) is not int
                    or not DEFAULT_TRAINING_LIMITS.tree_min_leaf[0] <= params['min_leaf'] <= DEFAULT_TRAINING_LIMITS.tree_min_leaf[1]):
                raise BacktestError('tree model parameters exceed configured limits')
        else:
            if (set(params) != {'hidden_size', 'epochs', 'learning_rate'}
                    or type(params.get('hidden_size')) is not int
                    or not DEFAULT_TRAINING_LIMITS.lstm_hidden_size[0] <= params['hidden_size'] <= DEFAULT_TRAINING_LIMITS.lstm_hidden_size[1]
                    or type(params.get('epochs')) is not int
                    or not DEFAULT_TRAINING_LIMITS.lstm_epochs[0] <= params['epochs'] <= DEFAULT_TRAINING_LIMITS.lstm_epochs[1]
                    or type(params.get('learning_rate')) not in (int, float)
                    or not DEFAULT_TRAINING_LIMITS.lstm_learning_rate[0] <= params['learning_rate'] <= DEFAULT_TRAINING_LIMITS.lstm_learning_rate[1]):
                raise BacktestError('LSTM parameters exceed configured limits')
    # Reject non-JSON, NaN, and infinite configuration values before any work.
    try:
        return json.loads(_canonical(protocol))
    except (ValueError, TypeError) as exc:
        raise BacktestError('protocol must be finite JSON') from exc


def run_from_snapshot(path, protocol, scenarios):
    protocol = _validate(protocol)
    rows = SnapshotStore(path).get_draws(protocol['snapshot_id'])
    issues = [row['issue'] for row in rows]
    inputs = set()
    for issue in protocol['targets']:
        if issue not in issues:
            raise BacktestError('target manifest references issue outside snapshot')
        index = issues.index(issue)
        inputs.update(issues[max(0, index-protocol['history_periods']):index])
    with exploration_activity(path, kind='backtest', inputs=inputs, targets=protocol['targets']):
        return _run_from_snapshot(path, protocol, scenarios)


@exact_money_context
def _run_from_snapshot(path, protocol, scenarios, *, fixed_run=None, method_id=None, fixed_attempt=None):
    """Run all fixed targets/replicas; missing input is recorded, never filled zero.

    scenarios maps each target issue to a rule, payout map, promotion boolean,
    and explicit source reference. The caller must independently verify them.
    """
    protocol = _validate(protocol)
    if not isinstance(scenarios, dict):
        raise BacktestError('per-issue payout scenarios required')
    try:
        scenario_copy = json.loads(_canonical(scenarios))
    except (ValueError, TypeError) as exc:
        raise BacktestError('scenarios must be finite JSON') from exc
    with closing(sqlite3.connect(path)) as db:
        if fixed_run is None:
            check_backtest_allowed(db, protocol)
        else:
            from dlt.fixed_evaluation import validate_execution
            validate_execution(db, fixed_run, fixed_attempt, method_id, protocol, scenario_copy)
    snapshot_id = protocol.get('snapshot_id')
    stored = SnapshotStore(path).get_draws(snapshot_id)
    issues = [r['issue'] for r in stored]
    if any(issue not in issues for issue in protocol['targets']):
        raise BacktestError('target manifest references issue outside snapshot')
    indices = [issues.index(issue) for issue in protocol['targets']]
    if indices != sorted(indices):
        raise BacktestError('target manifest must follow snapshot chronology')
    records = []
    for target_index in indices:
        row = stored[target_index]
        target = {'issue': row['issue'], 'date': row['draw_date']}
        past = stored[max(0, target_index-protocol['history_periods']):target_index]
        replicas = []
        for replica in protocol['replicas']:
            record = {'replica': replica, 'status': 'failed', 'reason': None, 'pair': None,
                      'artifact': None, 'candidate_settlement': None, 'control_settlement': None, 'difference_yuan': None}
            try:
                if len(past) != protocol['history_periods'] or any(not item['numbers_usable'] for item in past):
                    raise BacktestError('required historical window unavailable')
                with closing(sqlite3.connect(path)) as db:
                    _assert_consistent_history(db, {item['issue'] for item in past})
                history = [{'issue': item['issue'], 'date': item['draw_date'], 'front': item['front'], 'back': item['back']}
                           for item in past]
                pair, artifact = _generate(history, target, protocol, replica)
                record['pair'], record['artifact'] = pair, artifact
                # Target outcome and payout checks happen only after the pair
                # exists, so they cannot influence candidate construction.
                if SnapshotStore(path).conflicts(target['issue']):
                    raise BacktestError('target draw or prize source has unresolved conflict')
                sc = scenario_copy.get(target['issue'])
                if isinstance(sc, dict) and isinstance(sc.get('blocked_reason'), str):
                    raise BacktestError(sc['blocked_reason'])
                if not isinstance(sc, dict) or not isinstance(sc.get('source_ref'), str) or not sc['source_ref'].strip():
                    raise BacktestError('missing payout scenario or source reference')
                raw_row = json.loads(row['row_json'])
                promotion, published_promotion_payouts = promotion_data(raw_row, target['issue'])
                if sc.get('promotion') is not promotion:
                    raise BacktestError('payout scenario promotion flag differs from retained draw')
                expected_payouts = published_payouts(raw_row, sc.get('rule'), protocol['candidate']['additional'])
                if not amounts_match(sc.get('payouts'), expected_payouts):
                    raise BacktestError('payout scenario differs from retained draw fields')
                if promotion:
                    supplied = sc.get('promotion_payouts')
                    if not amounts_match(supplied, published_promotion_payouts):
                        raise BacktestError('promotion payout scenario differs from retained draw fields')
                draw = {'issue': target['issue'], 'front': row['front'], 'back': row['back'],
                        'promotion': promotion}
                settlements = [settle_portfolio(issue=target['issue'], draw=draw, tickets=pair[label]['tickets'],
                                                payouts=sc.get('payouts'), rule=sc.get('rule'), duplicate_policy='reject',
                                                ticket_groups=pair[label].get('ticket_groups'),
                                                promotion_payouts=sc.get('promotion_payouts'))
                               for label in ('candidate', 'control')]
                if (Decimal(settlements[0]['cost'])*100 != pair['candidate']['cost_fen']
                        or Decimal(settlements[1]['cost'])*100 != pair['control']['cost_fen']
                        or Decimal(settlements[0]['cost']) != Decimal(settlements[1]['cost'])):
                    raise BacktestError('settlement costs conflict with matched portfolio costs')
                difference = Decimal(settlements[0]['net']) - Decimal(settlements[1]['net'])
                record.update({'status': 'complete', 'candidate_settlement': settlements[0],
                               'control_settlement': settlements[1], 'difference_yuan': str(difference)})
            except (BacktestError, TrainingError, CandidateError, SettlementBlocked, ScenarioError) as exc:
                record['reason'] = str(exc)
            replicas.append(record)
        all_complete = all(item['status'] == 'complete' for item in replicas)
        fraction = (sum((Fraction(Decimal(item['difference_yuan'])) for item in replicas), Fraction(0)) / len(replicas)
                    if all_complete else None)
        records.append({'target': target, 'status': 'complete' if all_complete else 'incomplete', 'replicas': replicas,
                        'mean_difference_fraction': [fraction.numerator, fraction.denominator] if fraction is not None else None})
    completed = [item for item in records if item['status'] == 'complete']
    mean = (sum((Fraction(*item['mean_difference_fraction']) for item in completed), Fraction(0)) / len(completed)
            if completed else None)
    return {'version': 'rolling-reconstruction-v1', 'protocol': protocol,
            'protocol_sha256': hashlib.sha256(_canonical(protocol)).hexdigest(),
            'scenarios': scenario_copy, 'scenarios_sha256': hashlib.sha256(_canonical(scenario_copy)).hexdigest(),
            'code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'python': platform.python_version(),
            'settlement_code_sha256': hashlib.sha256(Path(settlement_module.__file__).read_bytes()).hexdigest(),
            'calculation_version': 'exact-money-v1',
            'periods': records, 'planned_periods': len(records), 'complete_periods': len(completed),
            'incomplete_issues': [item['target']['issue'] for item in records if item['status'] != 'complete'],
            'descriptive_mean_fraction': [mean.numerator, mean.denominator] if mean is not None else None,
            'completeness': 'complete' if len(completed) == len(records) else 'partial',
            'evidence': 'descriptive reconstruction only; source and holdout independence not verified'}
