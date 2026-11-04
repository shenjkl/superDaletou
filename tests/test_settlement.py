import unittest
from decimal import Decimal, getcontext, localcontext
from fractions import Fraction

from dlt.settlement import SettlementBlocked, settle_portfolio


def fixture():
    groups = {f'{f},{b}': None for f in range(6) for b in range(3)}
    groups['5,2'] = 'first'
    groups['4,2'] = 'second'
    rule = {'version': 'synthetic-v1', 'source': 'synthetic test fixture', 'verified': True,
            'applicable_issues': ['26124'], 'hit_groups': groups,
            'additional_groups': {'first': 'first-extra', 'second': 'second-extra'},
            'promotion_policy': 'excluded', 'prices': {'base': '2', 'additional': '3'},
            'tax': {'unit': 'same_person_same_issue_same_game', 'threshold': '10000', 'comparison': 'gt', 'rate': '0.2',
                    'rounding': 'down', 'quantum': '0.01'}}
    draw = {'issue': '26124', 'front': [1, 2, 3, 4, 5], 'back': [1, 2], 'promotion': False}
    tickets = [{'front': [1, 2, 3, 4, 5], 'back': [1, 2], 'additional': False, 'multiplier': 1},
               {'front': [1, 2, 3, 4, 6], 'back': [1, 2], 'additional': False, 'multiplier': 1}]
    return {'issue': '26124', 'draw': draw, 'tickets': tickets,
            'payouts': {'first': '6000', 'second': '6000', 'first-extra': '100', 'second-extra': '200'},
            'rule': rule, 'duplicate_policy': 'reject'}


class SettlementTests(unittest.TestCase):
    def test_tax_applies_to_combined_portfolio_not_each_ticket(self):
        args = fixture()
        result = settle_portfolio(**args)
        self.assertEqual(result['gross'], '12000')
        self.assertEqual(result['tax'], '2400.00')
        self.assertEqual(result['cost'], '4')
        self.assertEqual(result['net'], '9596.00')
        self.assertEqual(result['winning_tickets'], 2)
        # Alternative portfolios must not be aggregated into one tax base.
        for ticket in args['tickets']:
            single = settle_portfolio(**{**args, 'tickets': [ticket]})
            self.assertEqual(single['tax'], '0')

    def test_additional_requires_all_published_extra_payouts(self):
        args = fixture()
        args['tickets'][0]['additional'] = True
        del args['payouts']['second-extra']
        with self.assertRaisesRegex(SettlementBlocked, 'second-extra'):
            settle_portfolio(**args)

    def test_unknown_tax_rules_or_missing_hit_mapping_are_blocked(self):
        args = fixture()
        args['rule']['verified'] = False
        with self.assertRaisesRegex(SettlementBlocked, 'verified'):
            settle_portfolio(**args)
        args = fixture()
        del args['rule']['hit_groups']['0,0']
        with self.assertRaisesRegex(SettlementBlocked, '18 hit mappings'):
            settle_portfolio(**args)

    def test_missing_or_unsupported_tax_unit_is_blocked_even_without_winnings(self):
        for unit in (None, 'per_ticket', 'unknown'):
            args = fixture()
            args['draw']['front'] = [10, 11, 12, 13, 14]
            if unit is None:
                del args['rule']['tax']['unit']
            else:
                args['rule']['tax']['unit'] = unit
            with self.subTest(unit=unit), self.assertRaisesRegex(SettlementBlocked, 'tax unit'):
                settle_portfolio(**args)

    def test_promotional_draw_and_wrong_issue_do_not_settle(self):
        args = fixture()
        args['draw']['promotion'] = True
        with self.assertRaisesRegex(SettlementBlocked, 'promotion'):
            settle_portfolio(**args)
        args['rule']['promotion_policy'] = 'included'
        with self.assertRaisesRegex(SettlementBlocked, 'verified 2026 activity issue range'):
            settle_portfolio(**args)
        args = fixture()
        args['draw']['issue'] = '26125'
        with self.assertRaisesRegex(SettlementBlocked, 'issue'):
            settle_portfolio(**args)

    def test_duplicate_policy_and_multiplier_are_explicit(self):
        args = fixture()
        args['tickets'] = [args['tickets'][0], args['tickets'][0].copy()]
        with self.assertRaisesRegex(SettlementBlocked, 'duplicate'):
            settle_portfolio(**args)
        args['duplicate_policy'] = 'allow'
        self.assertEqual(settle_portfolio(**args)['gross'], '12000')
        args['tickets'][0]['multiplier'] = True
        with self.assertRaisesRegex(SettlementBlocked, 'multiplier'):
            settle_portfolio(**args)

    def test_float_money_rejected_to_avoid_precision_loss(self):
        args = fixture()
        args['payouts']['first'] = 6000.0
        with self.assertRaisesRegex(SettlementBlocked, 'exact decimal'):
            settle_portfolio(**args)

    def test_large_amounts_are_exact_even_under_low_caller_precision(self):
        args = fixture()
        args['payouts']['first'] = args['payouts']['second'] = '12345678901234567890123456.78'
        args['tickets'][1]['multiplier'] = 2
        gross_cents = 1234567890123456789012345678 * 3
        tax_cents = gross_cents // 5
        with localcontext() as caller:
            caller.prec = 6
            result = settle_portfolio(**args)
            self.assertEqual(getcontext().prec, 6)
        self.assertEqual(Fraction(Decimal(result['gross'])), Fraction(gross_cents, 100))
        self.assertEqual(Fraction(Decimal(result['tax'])), Fraction(tax_cents, 100))
        self.assertEqual(Fraction(Decimal(result['net'])), Fraction(gross_cents - tax_cents - 600, 100))

    def test_unbounded_amounts_and_multipliers_are_rejected(self):
        for value in ('1e129', '1e-129', '9' * 129, '0' * 513):
            args = fixture(); args['payouts']['first'] = value
            with self.subTest(value=value), self.assertRaisesRegex(SettlementBlocked, 'resource limit'):
                settle_portfolio(**args)
        args = fixture(); args['tickets'][0]['multiplier'] = 1000001
        with self.assertRaisesRegex(SettlementBlocked, 'multiplier'):
            settle_portfolio(**args)
        args = fixture(); args['tickets'] = args['tickets'] * 5001
        with self.assertRaisesRegex(SettlementBlocked, 'ticket resource limit'):
            settle_portfolio(**args)


if __name__ == '__main__':
    unittest.main()
