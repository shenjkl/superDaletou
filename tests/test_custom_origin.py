from contextlib import closing
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest

from dlt.app import Handler
from dlt.custom_origin import OriginError, list_legacy_custom, record_declaration
from dlt.custom_scenarios import custom_scenario, PAYOUT_KEYS
from dlt.evaluation_repository import EvaluationRepository
from dlt.holdout_audit import audit_holdout
from dlt.holdout_reservation import ReservationError, list_reservations
from dlt.integrity import check_database
from dlt.recovery import prepare_isolated_copy
from dlt.pair_repository import PairRepository
from dlt.snapshots import SnapshotStore
from test_candidates import config
import test_holdout_scenarios as holdout
import test_fixed_evaluation as fixed


class CustomOriginTests(unittest.TestCase):
    reserve = holdout.HistoricalScenarioExclusionTests.reserve

    def setUp(self):
        holdout.HistoricalScenarioExclusionTests.setUp(self)
        scenario = custom_scenario(self.pair, {'label': '旧情景', 'payouts': {key: '100' for key in PAYOUT_KEYS}})
        self.args = {'scenario': scenario, 'samples': 100, 'seed': 7, 'max_samples': 1000, 'max_ticket_draws': 10000}
        self.evaluation._run_new('legacy', 'pair', **self.args)

    def declare(self, event='event-1', kind='synthetic', issue=None, reason='本人回忆人工设定'):
        return record_declaration(self.path, event_id=event, evaluation_key='legacy', kind=kind,
                                  issue=issue, reason=reason, confirmed=True)

    def test_synthetic_declaration_is_append_only_and_clears_unknown_status(self):
        self.assertEqual(audit_holdout(self.path, self.snapshot, '26122', '26122')['status'], 'unresolved')
        original = self.evaluation.get('legacy')
        saved = self.declare()
        self.assertEqual(saved, self.declare())
        with self.assertRaises(OriginError):
            self.declare(reason='changed')
        self.assertEqual(audit_holdout(self.path, self.snapshot, '26122', '26122')['status'], 'no_recorded_use')
        self.assertEqual(self.evaluation.get('legacy'), original)
        self.assertEqual(len(list_legacy_custom(self.path)[0]['declarations']), 1)
        with closing(sqlite3.connect(self.path)) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("DELETE FROM custom_origin_declarations")
        report = check_database(self.path)
        self.assertEqual(report['status'], 'passed', report)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as directory:
            target = Path(directory) / 'candidate.sqlite3'
            self.assertEqual(prepare_isolated_copy(self.path, target)['status'], 'prepared')
            self.assertEqual(len(list_legacy_custom(target)[0]['declarations']), 1)

    def test_all_historical_declarations_remain_possible_exposure_after_correction(self):
        self.declare(kind='historical', issue='26122', reason='当时参考了该期')
        self.declare(event='event-2', kind='synthetic', reason='后来认为是人工假设')
        audit = audit_holdout(self.path, self.snapshot, '26122', '26122')
        self.assertEqual(audit['status'], 'known_use')
        self.assertTrue(any(x['target_issues'] == ['26122'] for x in audit['findings']))
        with self.assertRaises(ReservationError):
            self.reserve()

    def test_corrupted_declaration_does_not_clear_unknown(self):
        self.declare()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER custom_origin_declarations_no_update')
            db.execute("UPDATE custom_origin_declarations SET content_json=x'7b7d'")
            db.execute("CREATE TRIGGER custom_origin_declarations_no_update BEFORE UPDATE ON custom_origin_declarations BEGIN SELECT RAISE(ABORT, 'immutable custom origin declaration'); END")
        audit = audit_holdout(self.path, self.snapshot, '26122', '26122')
        self.assertEqual(audit['status'], 'unresolved')
        self.assertEqual(check_database(self.path)['status'], 'failed')

    def test_route_requires_session_and_confirmation(self):
        def call(body, csrf='secret', session=True):
            raw = json.dumps(body).encode()
            handler = object.__new__(Handler)
            handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', db_path=self.path)
            handler.path = '/api/custom-origin-declarations'
            handler.headers = {'Origin':'http://127.0.0.1:8766', 'Sec-Fetch-Site':'same-origin', 'X-DLT-CSRF':csrf,
                               'Content-Type':'application/json', 'Content-Length':str(len(raw))}
            handler.rfile = BytesIO(raw)
            handler._validate_host = lambda: True
            handler._session = lambda: session
            handler._discard_bounded_body = lambda: None
            handler._json = lambda status, payload, **kwargs: (status, payload)
            return handler.do_POST()
        body = {'event_id':'event', 'evaluation_key':'legacy', 'kind':'synthetic', 'issue':None,
                'reason':'本人声明', 'confirmed':True}
        self.assertEqual(call(body, csrf='bad')[0], 403)
        self.assertEqual(call(body, session=False)[0], 403)
        self.assertEqual(call({**body, 'confirmed':False})[0], 400)
        self.assertEqual(call(body)[0], 200)


class ExistingReservationTests(unittest.TestCase):
    def setUp(self):
        fixed.FixedEvaluationTests.setUp(self)
        self.fixed = self.service.freeze(self.reservation, self.methods, hypothesis='固定对照', timeout=30, confirmed=True)
        rows = SnapshotStore(self.path).get_draws(self.snapshot)
        cfg = {**config(), 'snapshot_id':self.snapshot, 'target_issue':'26124', 'target_date':'2026-10-31', 'budget_fen':400}
        pair = PairRepository(self.path).save_draft('legacy-pair', cfg, history_first_issue=rows[0]['issue'], history_cutoff_issue='26119')
        sc = custom_scenario(pair, {'label':'旧奖金', 'payouts':{key:'100' for key in PAYOUT_KEYS}})
        EvaluationRepository(self.path)._run_new('legacy-eval', 'legacy-pair', scenario=sc, samples=100, seed=7,
                                                 max_samples=1000, max_ticket_draws=10000)

    def test_unknown_and_declared_historical_exposure_block_previously_fixed_protocol(self):
        state = self.service.get(self.reservation)
        self.assertEqual(state['validity'], 'review_required')
        self.assertTrue(list_reservations(self.path)[0]['custom_origin_exposure'])
        with self.assertRaisesRegex(ValueError, '来源'):
            self.service.start(self.reservation, request_key='blocked')
        record_declaration(self.path, event_id='historic', evaluation_key='legacy-eval', kind='historical',
                           issue='26122', reason='回忆参考历史期', confirmed=True)
        state = self.service.get(self.reservation)
        self.assertEqual(state['validity'], 'review_required')
        with self.assertRaisesRegex(ValueError, '来源'):
            self.service.start(self.reservation, request_key='blocked-again')

    def test_synthetic_declaration_removes_only_unknown_exposure(self):
        record_declaration(self.path, event_id='synthetic', evaluation_key='legacy-eval', kind='synthetic',
                           issue=None, reason='本人声明假设值', confirmed=True)
        self.assertEqual(self.service.get(self.reservation)['custom_origin_exposure'], [])
        self.assertEqual(list_reservations(self.path)[0]['custom_origin_exposure'], [])

    def test_completed_report_is_withdrawn_when_legacy_exposure_is_discovered(self):
        fixture = fixed.FixedEvaluationTests('test_multiple_methods_are_fixed_once_and_run_as_descriptive_reconstruction')
        fixture.setUp(); self.addCleanup(fixture.doCleanups)
        fixture.freeze()
        fixture.service.start(fixture.reservation, request_key='finished-before-discovery')
        finished = fixture.wait()
        self.assertEqual(finished['validity'], 'local_descriptive', finished)
        rows = SnapshotStore(fixture.path).get_draws(fixture.snapshot)
        cfg = {**config(), 'snapshot_id':fixture.snapshot, 'target_issue':'26124', 'target_date':'2026-10-31', 'budget_fen':400}
        pair = PairRepository(fixture.path).save_draft('late-pair', cfg, history_first_issue=rows[0]['issue'], history_cutoff_issue='26119')
        sc = custom_scenario(pair, {'label':'来源未知', 'payouts':{key:'100' for key in PAYOUT_KEYS}})
        EvaluationRepository(fixture.path)._run_new('late-legacy', 'late-pair', scenario=sc, samples=100, seed=7,
                                                    max_samples=1000, max_ticket_draws=10000)
        result = fixture.service.get(fixture.reservation)
        self.assertEqual(result['validity'], 'review_required')
        self.assertNotIn('paired_descriptions', result)
        self.assertEqual(result['report_sha256'], finished['report_sha256'])
        record_declaration(fixture.path, event_id='retrospective', evaluation_key='late-legacy', kind='historical',
                           issue='26122', reason='本人回忆参考该期', confirmed=True)
        self.assertEqual(fixture.service.get(fixture.reservation)['validity'], 'review_required')


if __name__ == '__main__':
    unittest.main()
