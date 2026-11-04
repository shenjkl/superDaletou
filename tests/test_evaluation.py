from copy import deepcopy
from decimal import Decimal
from fractions import Fraction
import hashlib
import math
import random
import unittest
from unittest.mock import patch

from dlt.candidates import _canonical, generate_pair
from dlt.evaluation import EvaluationError, _calc, _prepare, evaluate_pair
from dlt.settlement import settle_portfolio
from test_candidates import config, input_rows
from test_settlement import fixture


def scenario():
    values = fixture()
    values['rule']['applicable_issues'] = ['26017']
    return {'source_issue': '26124', 'reference': 'synthetic scenario',
            'adjustment_assumption': 'fixed amounts; no added-winner adjustment',
            'promotion': False, 'rule': values['rule'], 'payouts': values['payouts']}


def rehash(pair):
    pair = deepcopy(pair)
    pair.pop('pair_sha256', None)
    pair['pair_sha256'] = hashlib.sha256(_canonical(pair)).hexdigest()
    return pair


class EvaluationTests(unittest.TestCase):
    def setUp(self):
        self.pair = generate_pair(history=input_rows(), config={**config(), 'budget_fen': 400})
        self.args = {'scenario': scenario(), 'samples': 100, 'seed': 7,
                     'max_samples': 1000, 'max_ticket_draws': 10000}

    def test_paired_reproducible_and_exact_mean(self):
        a = evaluate_pair(self.pair, **self.args)
        b = evaluate_pair(self.pair, **self.args)
        a['sampling'].pop('created_at'); b['sampling'].pop('created_at')
        self.assertEqual(a, b)
        self.assertTrue(a['sampling']['shared_draws'])
        p = a['portfolios']['candidate']
        # Each of two distinct tickets has first and second tier expectations,
        # regardless of overlap; joint profit probability need not be the same.
        universe = math.comb(35, 5)*math.comb(12, 2)
        expected = Fraction(2*6000*(1+5*30), universe)
        self.assertEqual(Fraction(*p['mean_gross_tax_before']['fraction']), expected)
        self.assertEqual(p['jackpot_coverage']['unique_combinations'], 2)
        self.assertEqual(p['jackpot_coverage']['outcomes'], universe)
        self.assertGreater(p['profit']['interval_95'][1], 0)  # zero observations is not exact zero
        self.assertIsNone(a['nonparticipation']['roi'])

    def test_same_draws_same_tickets_same_observations(self):
        pair = deepcopy(self.pair)
        pair['control']['tickets'] = deepcopy(pair['candidate']['tickets'])
        pair = rehash(pair)
        results = evaluate_pair(pair, **self.args)['portfolios']
        self.assertEqual(results['candidate'], results['control'])

    def test_compiled_simulation_matches_settlement_including_tax(self):
        source = fixture(); tickets = source['tickets']; rule = source['rule']; payouts = source['payouts']
        compiled, table = _prepare(tickets, rule, payouts)
        rng = random.Random(9)
        draws = [([1, 2, 3, 4, 5], [1, 2])] + [
            (rng.sample(range(1, 36), 5), rng.sample(range(1, 13), 2)) for _ in range(30)]
        for front, back in draws:
            expected = settle_portfolio(**{**source, 'draw': {'issue': '26124', 'front': front,
                                                               'back': back, 'promotion': False}})
            gross, net = _calc(compiled, table, front, back, Decimal(expected['cost']), rule['tax'])
            self.assertEqual(gross, Decimal(expected['gross']))
            self.assertEqual(net, Decimal(expected['net']))

    def test_additional_prizes_and_missing_unused_additional_payouts(self):
        source = fixture()
        source['tickets'][0]['additional'] = True
        compiled, table = _prepare(source['tickets'], source['rule'], source['payouts'])
        expected = settle_portfolio(**source)
        gross, net = _calc(compiled, table, source['draw']['front'], source['draw']['back'],
                           Decimal(expected['cost']), source['rule']['tax'])
        self.assertEqual(gross, Decimal(expected['gross']))
        self.assertEqual(net, Decimal(expected['net']))
        sc = scenario()
        del sc['payouts']['first-extra']
        del sc['payouts']['second-extra']
        evaluate_pair(self.pair, **{**self.args, 'scenario': sc})

    def test_missing_assumptions_invalid_pair_and_limits_block(self):
        broken = deepcopy(self.pair); broken['candidate']['tickets'][0]['front'][0] = 35
        with self.assertRaisesRegex(EvaluationError, 'integrity'):
            evaluate_pair(broken, **self.args)
        with self.assertRaisesRegex(EvaluationError, 'resource limit'):
            evaluate_pair(self.pair, **{**self.args, 'max_ticket_draws': 1})
        sc = scenario(); del sc['adjustment_assumption']
        with self.assertRaisesRegex(EvaluationError, 'assumption'):
            evaluate_pair(self.pair, **{**self.args, 'scenario': sc})
        sc = scenario(); del sc['payouts']['first']
        with self.assertRaisesRegex(EvaluationError, 'first'):
            evaluate_pair(self.pair, **{**self.args, 'scenario': sc})
        sc = scenario(); sc['rule']['prices']['base'] = '3'
        with self.assertRaisesRegex(EvaluationError, 'recorded portfolio cost'):
            evaluate_pair(self.pair, **{**self.args, 'scenario': sc})

    def test_distinct_layouts_produce_distinct_results(self):
        pair = deepcopy(self.pair)
        # Duplicate one candidate combination; expected gross stays fixed but
        # jackpot coverage decreases while the control remains two unique rows.
        pair['candidate']['tickets'][1] = deepcopy(pair['candidate']['tickets'][0])
        result = evaluate_pair(rehash(pair), **self.args)['portfolios']
        self.assertEqual(result['candidate']['jackpot_coverage']['unique_combinations'], 1)
        self.assertEqual(result['control']['jackpot_coverage']['unique_combinations'], 2)
        self.assertEqual(result['candidate']['mean_gross_tax_before']['fraction'],
                         result['control']['mean_gross_tax_before']['fraction'])

    def test_large_prizes_use_exact_arithmetic_in_simulation(self):
        source = fixture()
        source['tickets'][1]['multiplier'] = 2
        pair = deepcopy(self.pair)
        for label in ('candidate', 'control'):
            pair[label]['tickets'] = deepcopy(source['tickets'])
            pair[label]['cost_fen'] = 600
        sc = scenario()
        sc['payouts']['first'] = sc['payouts']['second'] = '12345678901234567890123456.78'
        gross_cents = 1234567890123456789012345678 * 3
        net_cents = gross_cents - gross_cents // 5 - 600
        class FixedDraw:
            def __init__(self, seed): pass
            def sample(self, population, count):
                return [1, 2, 3, 4, 5] if count == 5 else [1, 2]
        with patch('dlt.evaluation.random.Random', FixedDraw):
            result = evaluate_pair(rehash(pair), scenario=sc, samples=1, seed=1,
                                   max_samples=1, max_ticket_draws=4)
        net = result['portfolios']['candidate']['net_quantiles_yuan']['p50']
        self.assertEqual(Fraction(Decimal(net)), Fraction(net_cents, 100))


if __name__ == '__main__':
    unittest.main()
