"""Reproducible, budget-matched portfolio construction.

The caller resolves the calendar, immutable snapshot and verified price rule.
Scores are uncalibrated method signals; no purchase recommendation is implied.
"""
from __future__ import annotations

from datetime import date
import hashlib
from itertools import combinations
import json
import math
from pathlib import Path
import platform
import random
import re


class CandidateError(ValueError):
    pass


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def _numbers(values, size, count, label):
    if (not isinstance(values, list) or len(values) != count
            or any(type(v) is not int or not 1 <= v <= size for v in values)
            or len(set(values)) != count):
        raise CandidateError(f'invalid {label}')
    return sorted(values)


def generate_pair(*, history, config, model_scores=None):
    """Generate two independent alternatives with identical cost and structure.

    config must include a verified calendar/price reference, target and source
    snapshot identifier. Prices and budget are integer fen, not floats.
    The caller verifies the references and any model scores before calling.
    """
    if not isinstance(config, dict):
        raise CandidateError('configuration required')
    required = ('method', 'target_issue', 'target_date', 'snapshot_id', 'rule_version', 'calendar_version',
                'budget_fen', 'base_price_fen', 'additional_price_fen', 'structure', 'additional', 'window', 'seed')
    if any(key not in config for key in required):
        raise CandidateError('incomplete generation configuration')
    method = config['method']; structure = config['structure']
    if method not in ('random', 'hot', 'cold', 'tree', 'lstm') or structure not in ('singles', 'compound', 'dantuo'):
        raise CandidateError('unsupported method or structure')
    if not isinstance(config['target_issue'], str) or not re.fullmatch(r'[0-9]{5}', config['target_issue']):
        raise CandidateError('target issue required')
    if not isinstance(config['snapshot_id'], str) or not re.fullmatch(r'[0-9a-f]{64}', config['snapshot_id']):
        raise CandidateError('snapshot identifier required')
    if any(not isinstance(config[key], str) or not config[key].strip() or len(config[key]) > 128
           for key in ('rule_version', 'calendar_version')):
        raise CandidateError('rule and calendar versions required')
    try:
        target_day = date.fromisoformat(config['target_date'])
    except (TypeError, ValueError) as exc:
        raise CandidateError('calendar-resolved target date required') from exc
    if (type(config['additional']) is not bool or type(config['budget_fen']) is not int or config['budget_fen'] <= 0
            or type(config['base_price_fen']) is not int or config['base_price_fen'] <= 0
            or type(config['additional_price_fen']) is not int or config['additional_price_fen'] <= 0
            or type(config['window']) is not int or config['window'] < 1
            or type(config['seed']) is not int or not 0 <= config['seed'] <= 4294967295):
        raise CandidateError('invalid budget, price, window, addition or seed')
    if not isinstance(history, list) or len(history) < config['window']:
        raise CandidateError('insufficient history')
    days, issues = [], []
    for row in history:
        if not isinstance(row, dict) or not isinstance(row.get('issue'), str) or not re.fullmatch(r'[0-9]{5}', row['issue']):
            raise CandidateError('invalid historical issue')
        try:
            day = date.fromisoformat(row['date'])
        except (KeyError, TypeError, ValueError) as exc:
            raise CandidateError('invalid historical date') from exc
        _numbers(row.get('front'), 35, 5, 'historical front')
        _numbers(row.get('back'), 12, 2, 'historical back')
        issues.append(row['issue']); days.append(day)
    if (len(set(issues)) != len(issues) or config['target_issue'] in issues
            or any(a >= b for a, b in zip(days, days[1:])) or days[-1] >= target_day):
        raise CandidateError('history violates chronological target boundary')
    window = history[-config['window']:]
    count_front = [1]*35; count_back = [1]*12
    for row in window:
        for n in row['front']: count_front[n-1] += 1
        for n in row['back']: count_back[n-1] += 1
    if method in ('tree', 'lstm'):
        if (not isinstance(model_scores, dict) or not isinstance(model_scores.get('artifact_sha256'), str)
                or not re.fullmatch(r'[0-9a-f]{64}', model_scores['artifact_sha256'])
                or any(not isinstance(model_scores.get(k), list) or len(model_scores[k]) != n
                       or any(type(v) not in (int, float) or not math.isfinite(v) for v in model_scores[k])
                       for k, n in [('front_scores', 35), ('back_scores', 12)])):
            raise CandidateError('verified model scores and artifact identifier required')
        temp = config.get('score_temperature')
        if type(temp) not in (int, float) or not math.isfinite(temp) or temp <= 0:
            raise CandidateError('model score temperature must be explicitly positive')
        def score_weights(values):
            highest = max(values)
            return [math.exp(max(-700, (v-highest)/temp)) for v in values]
        front_weights, back_weights = score_weights(model_scores['front_scores']), score_weights(model_scores['back_scores'])
    elif model_scores is not None:
        raise CandidateError('model scores must not be supplied for a non-model method')
    elif method == 'hot':
        front_weights, back_weights = count_front, count_back
    elif method == 'cold':
        front_weights, back_weights = [1/x for x in count_front], [1/x for x in count_back]
    else:
        front_weights, back_weights = None, None
    front_count, back_count = config.get('front_count', 5), config.get('back_count', 2)
    if structure == 'compound' and (type(front_count) is not int or not 5 <= front_count <= 35
                                    or type(back_count) is not int or not 2 <= back_count <= 12):
        raise CandidateError('invalid compound structure')
    price = config['additional_price_fen'] if config['additional'] else config['base_price_fen']
    if structure == 'singles':
        n = config['budget_fen']//price
    elif structure == 'compound':
        n = math.comb(front_count, 5)*math.comb(back_count, 2)
    else:
        n = 18  # fixed 4-dan/6-drag, back 3
    limit = config.get('max_tickets')
    if type(limit) is not int or limit < 1:
        raise CandidateError('explicit positive ticket limit required')
    if n < 1 or n > limit or n*price > config['budget_fen']:
        raise CandidateError('budget or ticket limit cannot accommodate structure')
    if n > math.comb(35, 5)*math.comb(12, 2):
        raise CandidateError('requested unique tickets exceed the outcome space')

    def seed_for(label):
        return int.from_bytes(hashlib.sha256(f"dlt-pair-v1:{config['seed']}:{label}".encode()).digest()[:8], 'big')

    def build(label, weights_front, weights_back):
        seed = seed_for(label); rng = random.Random(seed)
        def pick(size, count, weights):
            pool = list(range(1, size+1)); result = []
            for _ in range(count):
                if weights is None:
                    index = rng.randrange(len(pool))
                else:
                    total = sum(weights[n-1] for n in pool)
                    mark = rng.random()*total; index = len(pool)-1
                    for i, number in enumerate(pool):
                        mark -= weights[number-1]
                        if mark < 0:
                            index = i; break
                result.append(pool.pop(index))
            return result
        tickets, pool = [], None
        if structure == 'singles':
            seen = set(); attempts = 0
            # Reject pathological score collapse rather than loop indefinitely.
            attempts_limit = config.get('max_sampling_attempts')
            if type(attempts_limit) is not int or attempts_limit < n:
                raise CandidateError('explicit sampling-attempt limit must be at least ticket count')
            while len(tickets) < n and attempts < attempts_limit:
                f, b = tuple(sorted(pick(35, 5, weights_front))), tuple(sorted(pick(12, 2, weights_back)))
                attempts += 1
                if (f, b) not in seen:
                    seen.add((f, b)); tickets.append((f, b))
            if len(tickets) != n:
                raise CandidateError('unique ticket generation exhausted sampling-attempt limit')
        else:
            f = pick(35, front_count if structure == 'compound' else 10, weights_front)
            b = sorted(pick(12, back_count if structure == 'compound' else 3, weights_back))
            if structure == 'compound':
                f = sorted(f); fronts = combinations(f, 5); pool = {'front': f, 'back': b}
            else:
                dan, drag = sorted(f[:4]), sorted(f[4:]); pool = {'dan': dan, 'drag': drag, 'back': b}
                fronts = (tuple(sorted(dan+[x])) for x in drag)
            for one in fronts:
                for rear in combinations(b, 2): tickets.append((tuple(one), rear))
        result = [{'front': list(f), 'back': list(b), 'additional': config['additional'], 'multiplier': 1} for f, b in tickets]
        if len(result) != n or len({(tuple(t['front']), tuple(t['back'])) for t in result}) != n:
            raise CandidateError('combination validation failed')
        # A candidate/control portfolio is represented as one hypothetical
        # ticket per alternative for ticket-level rule simulation. This is an
        # explicit paper assumption, not evidence of an actual printed ticket.
        ticket_groups = [list(range(len(result)))]
        return {'seed': seed, 'tickets': result, 'pool': pool, 'count': n,
                'cost_fen': n*price, 'remaining_fen': config['budget_fen']-n*price,
                'ticket_groups': ticket_groups}

    candidate = build('candidate', front_weights, back_weights)
    control = build('control', None, None)
    source_rows = [{k: r[k] for k in ('issue', 'date', 'front', 'back')} for r in window]
    result = {'version': 'python-candidate-v1', 'runtime': platform.python_version(),
              'code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'config': json.loads(_canonical(config)), 'history_first': window[0]['issue'], 'history_last': window[-1]['issue'],
              'history_sha256': hashlib.sha256(_canonical(source_rows)).hexdigest(),
              'model_artifact_id': model_scores['artifact_sha256'] if model_scores else None,
              'candidate': candidate, 'control': control, 'evidence': 'pending',
              'recommendation': 'no evidence supporting preferential adoption'}
    result['pair_sha256'] = hashlib.sha256(_canonical(result)).hexdigest()
    return result
