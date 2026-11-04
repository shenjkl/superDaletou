from copy import deepcopy
from decimal import Decimal, localcontext
from fractions import Fraction
import random
import unittest

from dlt.paired_description import PairedDescriptionError, describe_backtest, describe_pairs


def complete_replica(replica, candidate, control, cost='4'):
    with localcontext() as context:
        context.prec = 200
        candidate_gross = str(Decimal(candidate)+Decimal(cost))
        control_gross = str(Decimal(control)+Decimal(cost))
    return {'replica': replica, 'status': 'complete',
            'candidate_settlement': {'gross': candidate_gross, 'tax': '0', 'net': candidate, 'cost': cost},
            'control_settlement': {'gross': control_gross, 'tax': '0', 'net': control, 'cost': cost},
            'difference_yuan': str(Fraction(candidate)-Fraction(control))}


class PairedDescriptionTests(unittest.TestCase):
    def test_known_exact_distribution_sensitivity_and_concentration(self):
        entries = [('1', Fraction(-4)), ('2', Fraction(0)), ('3', Fraction(2)), ('4', Fraction(10))]
        result = describe_pairs(entries, planned=5)
        self.assertEqual((result['included'], result['missing'], result['completeness']), (4, 1, 'partial'))
        self.assertEqual((result['positive'], result['zero'], result['negative']), (2, 1, 1))
        self.assertEqual(result['mean_fraction'], [2, 1])
        self.assertEqual([result[k] for k in ('min_fraction', 'p25_fraction', 'median_fraction', 'p75_fraction', 'max_fraction')],
                         [[-4, 1], [-4, 1], [0, 1], [2, 1], [10, 1]])
        self.assertEqual((result['leave_one_out_min_fraction'], result['leave_one_out_max_fraction']), ([-2, 3], [4, 1]))
        self.assertEqual((result['largest_absolute_issue'], result['largest_absolute_share_fraction']), ('4', [5, 8]))

    def test_empty_single_zero_and_tied_extremes(self):
        self.assertEqual(describe_pairs([], planned=3)['completeness'], 'no_results')
        single = describe_pairs([('a', Fraction(3, 2))], planned=1)
        self.assertIsNone(single['leave_one_out_min_fraction'])
        self.assertEqual(single['largest_absolute_share_fraction'], [1, 1])
        zero = describe_pairs([('a', Fraction(0)), ('b', Fraction(0))], planned=2)
        self.assertIsNone(zero['largest_absolute_share_fraction'])
        self.assertEqual(zero['leave_one_out_min_fraction'], [0, 1])
        tied = describe_pairs([('a', Fraction(-2)), ('b', Fraction(2))], planned=2)
        self.assertEqual((tied['largest_absolute_issue'], tied['largest_absolute_share_fraction']), ('a', [1, 2]))
        with self.assertRaises(PairedDescriptionError):
            describe_pairs([('a', Fraction(1)), ('a', Fraction(2))], planned=2)

    def test_leave_one_out_extrema_match_independent_enumeration(self):
        generator = random.Random(42)
        for count in range(2, 31):
            entries = [(str(i), Fraction(generator.randint(-100, 100), generator.randint(1, 9)))
                       for i in range(count)]
            result = describe_pairs(entries, planned=count)
            brute = [sum((value for j, (_, value) in enumerate(entries) if j != i), Fraction(0))/(count-1)
                     for i in range(count)]
            self.assertEqual(Fraction(*result['leave_one_out_min_fraction']), min(brute))
            self.assertEqual(Fraction(*result['leave_one_out_max_fraction']), max(brute))

    def test_backtest_recomputes_replicas_and_keeps_missing_period(self):
        report = {'protocol': {'targets': ['26001', '26002'], 'replicas': ['a', 'b']},
                  'planned_periods': 2, 'complete_periods': 1, 'incomplete_issues': ['26002'],
                  'completeness': 'partial', 'descriptive_mean_fraction': [1, 2],
                  'periods': [{'target': {'issue': '26001'}, 'status': 'complete',
                               'replicas': [complete_replica('a', '3', '1'), complete_replica('b', '1', '2')],
                               'mean_difference_fraction': [1, 2]},
                              {'target': {'issue': '26002'}, 'status': 'incomplete',
                               'replicas': [complete_replica('a', '100', '0'), {'replica': 'b', 'status': 'failed'}],
                               'mean_difference_fraction': None}]}
        result = describe_backtest(report)
        self.assertEqual((result['included'], result['missing'], result['mean_fraction']), (1, 1, [1, 2]))
        corruptions = []
        x = deepcopy(report); x['periods'][0]['replicas'][0]['difference_yuan'] = '1'; corruptions.append(x)
        x = deepcopy(report); x['periods'][0]['mean_difference_fraction'] = [1, 1]; corruptions.append(x)
        x = deepcopy(report); x['periods'][1]['mean_difference_fraction'] = [100, 1]; corruptions.append(x)
        x = deepcopy(report); x['periods'][0]['replicas'][0]['candidate_settlement']['cost'] = '2'; corruptions.append(x)
        x = deepcopy(report); x['periods'][0]['replicas'][0]['candidate_settlement']['gross'] = '200'; corruptions.append(x)
        x = deepcopy(report); x['incomplete_issues'] = []; corruptions.append(x)
        x = deepcopy(report); x['periods'][0]['replicas'].reverse(); corruptions.append(x)
        for item in corruptions:
            with self.subTest(item=item), self.assertRaises(PairedDescriptionError):
                describe_backtest(item)

    def test_large_exact_money_and_nonfinite_values(self):
        amount = '123456789012345678901234567890'
        report = {'protocol': {'targets': ['26001'], 'replicas': ['a']}, 'planned_periods': 1,
                  'complete_periods': 1, 'incomplete_issues': [], 'completeness': 'complete',
                  'descriptive_mean_fraction': [int(amount), 1],
                  'periods': [{'target': {'issue': '26001'}, 'status': 'complete',
                               'replicas': [complete_replica('a', amount, '0')],
                               'mean_difference_fraction': [int(amount), 1]}]}
        self.assertEqual(describe_backtest(report)['mean_fraction'], [int(amount), 1])
        for value in ('NaN', 'Infinity', '1e999999999'):
            broken = deepcopy(report); broken['periods'][0]['replicas'][0]['candidate_settlement']['net'] = value
            with self.assertRaises(PairedDescriptionError):
                describe_backtest(broken)

    def test_rounded_tax_can_exceed_a_fractional_prize(self):
        candidate = {'gross': '0.006', 'tax': '0.01', 'cost': '4', 'net': '-4.004'}
        control = {'gross': '0', 'tax': '0', 'cost': '4', 'net': '-4'}
        replica = {'replica': 'a', 'status': 'complete', 'candidate_settlement': candidate,
                   'control_settlement': control, 'difference_yuan': '-0.004'}
        report = {'protocol': {'targets': ['26001'], 'replicas': ['a']}, 'planned_periods': 1,
                  'complete_periods': 1, 'incomplete_issues': [], 'completeness': 'complete',
                  'descriptive_mean_fraction': [-1, 250],
                  'periods': [{'target': {'issue': '26001'}, 'status': 'complete',
                               'replicas': [replica], 'mean_difference_fraction': [-1, 250]}]}
        self.assertEqual(describe_backtest(report)['mean_fraction'], [-1, 250])


if __name__ == '__main__':
    unittest.main()
