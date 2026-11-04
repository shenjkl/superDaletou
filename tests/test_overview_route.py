"""Exercise the overview's experiment listing without starting an HTTP server."""
from datetime import timedelta
from types import SimpleNamespace
import unittest

from dlt.app import Handler
import test_prospective_settlement as fixtures


class OverviewRouteTests(unittest.TestCase):
    setUp = fixtures.PaperResultsTests.setUp
    review = fixtures.PaperResultsTests.review
    import_draw = fixtures.PaperResultsTests.import_draw

    def get_experiments(self):
        handler = object.__new__(Handler)
        handler.server = SimpleNamespace(db_path=self.path, experiments=self.repo)
        handler.path = '/api/experiments'
        handler._validate_host = lambda: True
        handler._session = lambda: True
        handler._json = lambda status, payload, **kwargs: (status, payload)
        return handler.do_GET()

    def test_list_uses_effective_calendar_and_withholds_invalid_date(self):
        status, payload = self.get_experiments()
        self.assertEqual(status, 200)
        self.assertEqual(payload['experiments'][0]['target_date'], self.calendar['draw_date'])
        replacement = {**self.calendar, 'version': 'corrected',
                       'cutoff_at': (self.deadline+timedelta(minutes=20)).isoformat()}
        ident = self.review('calendar', replacement)
        self.repo.declare_source_correction(self.cal_id, ident, note='fixture', confirmed=True)
        status, payload = self.get_experiments()
        self.assertEqual(status, 200)
        self.assertEqual(payload['experiments'][0]['target_date'], self.calendar['draw_date'])
        shifted = self.deadline + timedelta(days=1)
        replacement.update(version='shifted', cutoff_at=shifted.isoformat(), draw_date=shifted.date().isoformat())
        next_id = self.review('calendar', replacement)
        self.repo.declare_source_correction(ident, next_id, note='fixture', confirmed=True)
        status, payload = self.get_experiments()
        self.assertEqual(status, 200)
        self.assertEqual(payload['experiments'][0]['status'], 'review_required')
        self.assertIsNone(payload['experiments'][0]['target_date'])
