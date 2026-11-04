from copy import deepcopy
from datetime import timedelta
from unittest.mock import patch
import unittest

from dlt.pair_repository import PairRepository
from dlt.prospective import RegistrationError
from dlt.prospective_settlement import PaperResults
import test_prospective as prospective_fixtures
import test_prospective_settlement as paper_fixtures


class TaxUnitGateTests(unittest.TestCase):
    review = prospective_fixtures.ProspectiveTests.review
    import_draw = paper_fixtures.PaperResultsTests.import_draw

    def setUp(self):
        prospective_fixtures.ProspectiveTests.setUp(self)
        rule = deepcopy(self.repo._get_review(self._connection(), self.rule_id, 'rule')['extracted'])
        self.explicit_rule = deepcopy(rule)
        del rule['tax']['unit']
        self.legacy_rule_id = self.review('rule', rule)
        self.legacy_tax_id = self.review('tax', {'tax': rule['tax'], 'applicable_issues': ['26017']})
        self.legacy_protocol = {**self.protocol, 'rule_review_id': self.legacy_rule_id,
                                'tax_review_id': self.legacy_tax_id}
        self.paper = PaperResults(self.path, clock=lambda: self.now)

    def _connection(self):
        import sqlite3
        db = sqlite3.connect(self.path)
        self.addCleanup(db.close)
        return db

    def test_new_registration_rejects_unstated_tax_unit(self):
        with self.assertRaisesRegex(RegistrationError, 'tax unit'):
            self.repo.freeze('unverified', self.legacy_protocol)

    def test_existing_result_waits_for_review_and_recalculation(self):
        # Reproduce records written by the previous implementation without
        # silently changing their immutable rule or settlement history.
        with patch('dlt.settlement.require_tax_unit'):
            self.repo.freeze('legacy', self.legacy_protocol)
        before_draw = self.paper.inspect_current('legacy')
        self.assertEqual(before_draw['status'], 'review_required')
        self.assertIn('tax unit', before_draw['reason'])
        self.now = self.deadline + timedelta(hours=2)
        ticket = PairRepository(self.path).get('pair-1')['candidate']['tickets'][0]
        draw = {'lotteryGameNum': '85', 'lotteryDrawNum': '26017', 'lotteryDrawTime': self.calendar['draw_date'],
                'lotteryDrawResult': ' '.join(map(str, ticket['front']+ticket['back'])),
                'lotteryPromotionFlag': 0, 'verify': 1,
                'prizeLevelList': [{'sort': 101, 'stakeAmount': '6000'}, {'sort': 201, 'stakeAmount': '100'},
                                   {'sort': 301, 'stakeAmount': '5000'}, {'sort': 401, 'stakeAmount': '200'}]}
        snapshot = self.import_draw(draw)
        with patch('dlt.settlement.require_tax_unit'):
            original = self.paper.process_one('legacy', snapshot)
        self.assertEqual(original['status'], 'complete')
        current = self.paper.inspect_current('legacy')
        self.assertEqual(current['status'], 'review_required')
        self.assertIn('tax unit', current['reason'])
        self.assertIsNone(current['result'])

        next_rule = self.review('rule', self.explicit_rule)
        next_tax = self.review('tax', {'tax': self.explicit_rule['tax'], 'applicable_issues': ['26017']})
        self.repo.declare_source_correction(self.legacy_rule_id, next_rule, note='单位经本人核对', confirmed=True)
        self.repo.declare_source_correction(self.legacy_tax_id, next_tax, note='单位经本人核对', confirmed=True)
        recomputed = self.paper.process_one('legacy', snapshot)
        self.assertEqual(recomputed['status'], 'complete')
        self.assertNotEqual(original['group_id'], recomputed['group_id'])
        self.assertEqual(self.paper.inspect_current('legacy')['status'], 'locally_attested')
        self.assertEqual(self.paper.get_group(original['group_id']), original['result'])
