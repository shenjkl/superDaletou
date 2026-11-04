"""Evaluate actual paired tickets under an explicitly supplied fixed payout scenario.

Source authentication, rule applicability verification, job persistence and
currentness checks belong to the caller. A rule's `verified` flag is an input,
not independent evidence of verification.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
import math
from pathlib import Path
import platform
import random

from dlt.candidates import _canonical
from dlt.settlement import ROUNDING, SettlementBlocked, exact_money_context, settle_portfolio
from dlt import settlement as settlement_module


class EvaluationError(ValueError):
    pass


def _wilson(successes, n):
    z = 1.959963984540054
    p = successes / n
    den = 1 + z*z/n
    center = (p + z*z/(2*n))/den
    half = z*math.sqrt(p*(1-p)/n + z*z/(4*n*n))/den
    return {'successes': successes, 'samples': n, 'estimate': p,
            'interval_95': [max(0.0, center-half), min(1.0, center+half)], 'method': 'wilson'}


def _mask(numbers):
    return sum(1 << (n-1) for n in numbers)


def _prepare(tickets, rule, payouts):
    groups = rule['hit_groups']
    extras = rule['additional_groups']
    needs_extra = any(t['additional'] for t in tickets)
    table = {}
    for f in range(6):
        for b in range(3):
            group = groups[f'{f},{b}']
            base = Decimal(str(payouts[group]).replace(',', '')) if group is not None else Decimal(0)
            extra = Decimal(str(payouts[extras[group]]).replace(',', '')) if needs_extra and group in extras else Decimal(0)
            table[f, b, False] = base
            table[f, b, True] = base + extra
    return [(_mask(t['front']), _mask(t['back']), t['additional'], t['multiplier']) for t in tickets], table


def _calc(compiled, table, front, back, cost, tax):
    f, b = _mask(front), _mask(back)
    gross = sum((table[(fm & f).bit_count(), (bm & b).bit_count(), extra] * multiple
                 for fm, bm, extra, multiple in compiled), Decimal(0))
    threshold, rate = Decimal(str(tax['threshold']).replace(',', '')), Decimal(str(tax['rate']))
    taxable = gross > threshold if tax['comparison'] == 'gt' else gross >= threshold
    withheld = (gross * rate).quantize(Decimal('0.01'), rounding=ROUNDING[tax['rounding']]) if taxable else Decimal(0)
    return gross, gross - withheld - cost


def _mean_gross(compiled, table):
    total = math.comb(35, 5) * math.comb(12, 2)
    expectation = Fraction(0)
    for front_hits in range(6):
        front_count = math.comb(5, front_hits) * math.comb(30, 5-front_hits)
        for back_hits in range(3):
            count = front_count * math.comb(2, back_hits) * math.comb(10, 2-back_hits)
            for _, _, additional, multiple in compiled:
                expectation += Fraction(table[front_hits, back_hits, additional]) * multiple * count / total
    with localcontext() as context:
        context.prec = 28
        approximate = Decimal(expectation.numerator) / Decimal(expectation.denominator)
    return {'yuan': str(approximate), 'fraction': [expectation.numerator, expectation.denominator],
            'method': 'exact combinatorial mean; decimal rounded to 28 significant digits'}


@exact_money_context
def evaluate_pair(pair, *, scenario, samples, seed, max_samples, max_ticket_draws):
    """Evaluate on identical independent uniform draws, with explicit resource limits.

    Scenario must identify its source issue and explain whether payouts have
    been adjusted for hypothetical added winners/limits. Fixed payout values
    are assumptions and do not forecast the next draw's prizes.
    """
    if not isinstance(pair, dict) or not isinstance(pair.get('pair_sha256'), str):
        raise EvaluationError('pair and content digest required')
    content = dict(pair); digest = content.pop('pair_sha256')
    if hashlib.sha256(_canonical(content)).hexdigest() != digest:
        raise EvaluationError('pair integrity check failed')
    if not isinstance(scenario, dict) or not all(isinstance(scenario.get(k), str) and scenario[k].strip()
                                                  for k in ('source_issue', 'reference', 'adjustment_assumption')):
        raise EvaluationError('scenario source issue, reference and adjustment assumption required')
    rule, payouts = scenario.get('rule'), scenario.get('payouts')
    if not isinstance(rule, dict) or not isinstance(payouts, dict):
        raise EvaluationError('scenario rule and payouts required')
    target = pair.get('config', {}).get('target_issue')
    if (type(samples) is not int or type(max_samples) is not int or samples < 1 or samples > max_samples
            or type(max_ticket_draws) is not int or max_ticket_draws < 1
            or type(seed) is not int or not 0 <= seed <= 4294967295):
        raise EvaluationError('invalid samples, seed or limits')
    portfolios = []
    for label in ('candidate', 'control'):
        portfolio = pair.get(label)
        if not isinstance(portfolio, dict) or not isinstance(portfolio.get('tickets'), list):
            raise EvaluationError('both portfolios require complete tickets')
        portfolios.append(portfolio['tickets'])
    if sum(map(len, portfolios))*samples > max_ticket_draws:
        raise EvaluationError('ticket-draw resource limit exceeded')
    draw = {'issue': target, 'front': [1, 2, 3, 4, 5], 'back': [1, 2], 'promotion': scenario.get('promotion')}
    try:
        # Validate every ticket, mapping, payout and tax contract once using
        # the independent settlement implementation before the faster loop.
        settled = [settle_portfolio(issue=target, draw=draw, tickets=tickets, payouts=payouts,
                                    rule=rule, duplicate_policy='allow') for tickets in portfolios]
        costs = [Decimal(row['cost']) for row in settled]
        for index, label in enumerate(('candidate', 'control')):
            if (type(pair[label].get('cost_fen')) is not int
                    or Decimal(pair[label]['cost_fen']) != costs[index] * 100):
                raise EvaluationError('scenario price conflicts with recorded portfolio cost')
        prepared = [_prepare(tickets, rule, payouts) for tickets in portfolios]
    except (SettlementBlocked, KeyError, ValueError) as exc:
        raise EvaluationError(str(exc)) from exc
    rng = random.Random(seed)
    observations = [[], []]
    for _ in range(samples):
        front, back = rng.sample(range(1, 36), 5), rng.sample(range(1, 13), 2)
        for index, (compiled, table) in enumerate(prepared):
            observations[index].append(_calc(compiled, table, front, back, costs[index], rule['tax']))
    result = {}
    universe = math.comb(35, 5)*math.comb(12, 2)
    for index, label in enumerate(('candidate', 'control')):
        compiled, table = prepared[index]
        outcomes = observations[index]
        ordered = sorted(net for _, net in outcomes)
        quantile = lambda q: str(ordered[max(0, math.ceil(q*samples)-1)])
        unique = len({(fm, bm) for fm, bm, _, _ in compiled})
        result[label] = {'cost_yuan': str(costs[index]), 'ticket_rows': len(compiled),
                         'any_prize': _wilson(sum(gross > 0 for gross, _ in outcomes), samples),
                         'break_even': _wilson(sum(net >= 0 for _, net in outcomes), samples),
                         'profit': _wilson(sum(net > 0 for _, net in outcomes), samples),
                         'net_quantiles_yuan': {'p05': quantile(.05), 'p50': quantile(.5), 'p95': quantile(.95)},
                         'mean_gross_tax_before': _mean_gross(compiled, table),
                         'jackpot_coverage': {'unique_combinations': unique, 'outcomes': universe,
                                              'probability': unique/universe, 'method': 'exact uniform full-number-match coverage'}}
    return {'version': 'paired-monte-carlo-v1', 'pair_sha256': digest,
            'scenario_sha256': hashlib.sha256(_canonical(scenario)).hexdigest(),
            'scenario': {'source_issue': scenario['source_issue'], 'reference': scenario['reference'],
                         'adjustment_assumption': scenario['adjustment_assumption'], 'rule_version': rule['version'],
                         'target_issue': target, 'promotion': scenario['promotion']},
            'sampling': {'samples': samples, 'seed': seed, 'shared_draws': True, 'python': platform.python_version(),
                         'code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                         'settlement_code_sha256': hashlib.sha256(Path(settlement_module.__file__).read_bytes()).hexdigest(),
                         'calculation_version': 'exact-money-v1',
                         'created_at': datetime.now(timezone.utc).isoformat()},
            'portfolios': result, 'nonparticipation': {'cost_yuan': '0', 'net_yuan': '0', 'roi': None},
            'interpretation': 'Fixed payout assumptions are not a forecast; intervals describe Monte Carlo error only.'}
