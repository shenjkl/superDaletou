from contextlib import closing
from copy import deepcopy
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from urllib.parse import urlencode

from dlt.app import Handler
from dlt.integrity import check_database
from dlt.prospective import RegistrationError
from dlt.recovery import prepare_isolated_copy
from dlt.recovery_guard import RecoveryQuarantined
from dlt.research_decisions import DecisionError, ResearchDecisions, StaleEvidence, _sha
import test_prospective_series as series_fixtures
import test_fixed_evaluation as fixed_fixtures
import test_series_report as report_fixtures
from dlt.prospective_settlement import PaperResults


class ResearchDecisionTests(unittest.TestCase):
    review = series_fixtures.SeriesTests.review
    import_draw = series_fixtures.SeriesTests.import_draw
    _db = series_fixtures.SeriesTests._db

    def setUp(self):
        series_fixtures.SeriesTests.setUp(self)
        self.series.create('original', self.manifest)
        self.decisions = ResearchDecisions(self.path, clock=lambda: self.now)
        self.decisions.initialize()

    def save(self, key='judgment', context=None):
        context = context or self.decisions.context('series', 'original')
        return self.decisions.save(key, kind='series', key='original', choice='continue',
                                   reason='继续观察', limitations='样本不足',
                                   evidence_sha256=context['evidence_sha256'])

    def test_preserves_snapshot_and_detects_changed_evidence(self):
        old = self.decisions.context('series', 'original')
        record = self.save(context=old)
        self.assertEqual(record, self.save(context=old))
        self.assertEqual(self.decisions.list('series', 'original')[0]['evidence_status'], 'current')
        self.series.close('original', '暂时结束')
        with self.assertRaises(StaleEvidence):
            self.save('stale', old)
        entry = self.decisions.list('series', 'original')[0]
        self.assertEqual(entry['evidence_status'], 'changed')
        self.assertEqual(entry['record']['evidence'], old['evidence'])
        self.assertEqual(entry['record']['request']['reason'], '继续观察')
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM research_decisions').fetchone()[0], 1)
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM research_decisions')

    def test_draft_is_linked_and_future_series_must_match(self):
        self.save()
        draft = self.decisions.draft('draft', decision_key='judgment', hypothesis='新的研究假设')
        self.assertEqual(draft, self.decisions.draft('draft', decision_key='judgment', hypothesis='新的研究假设'))
        with self.assertRaises(DecisionError):
            self.decisions.draft('draft', decision_key='judgment', hypothesis='不同假设')
        manifest = deepcopy(self.manifest)
        manifest.update(hypothesis='新的研究假设', hypothesis_draft_key='draft')
        linked = self.series.create('new-series', manifest)
        self.assertEqual(linked['manifest']['hypothesis_draft_key'], 'draft')
        self.assertEqual(self.decisions.list('series', 'original')[0]['drafts'][0], draft)
        self.assertEqual(self.decisions.drafts()[0], {**draft, 'parent_evidence_status': 'current'})
        with self.assertRaisesRegex(RegistrationError, '不一致'):
            self.series.create('wrong', {**manifest, 'hypothesis': '其他假设'})
        with self.assertRaisesRegex(RegistrationError, '不存在'):
            self.series.create('missing', {**manifest, 'hypothesis_draft_key': 'missing'})
        check = check_database(self.path)
        self.assertEqual(check['status'], 'passed', check)
        self.assertEqual(check['counts']['research_decisions'], 1)
        self.assertEqual(check['counts']['research_hypothesis_drafts'], 1)

    def test_idempotency_and_tamper_fail_closed(self):
        context = self.decisions.context('series', 'original')
        self.save(context=context)
        with self.assertRaisesRegex(DecisionError, '请求标识'):
            self.decisions.save('judgment', kind='series', key='original', choice='finish',
                                reason='不同', limitations='样本不足', evidence_sha256=context['evidence_sha256'])
        self.decisions.draft('draft', decision_key='judgment', hypothesis='新假设')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER research_decisions_no_update')
            db.execute("UPDATE research_decisions SET record_json=X'7B7D'")
        with self.assertRaises(DecisionError):
            self.decisions.list('series', 'original')
        self.assertEqual(check_database(self.path)['status'], 'failed')

    def test_source_correction_invalidates_the_observed_result(self):
        self.repo.freeze('period-1', self.protocol, series_key='original')
        self.paper = PaperResults(self.path, clock=lambda: self.now)
        self.now = self.deadline + timedelta(hours=2)
        report_fixtures.SeriesReportTests.settle(self, 'period-1', '26017', self.calendar['draw_date'], 'pair-1')
        context = self.decisions.context('series', 'original')
        self.assertEqual(context['evidence']['report']['valid'], 1)
        self.save(context=context)
        tax = deepcopy(self.repo.get('period-1')['tax_review']['extracted'])
        tax['tax']['threshold'] = '1'
        next_id = self.review('tax', tax)
        self.repo.declare_source_correction(self.tax_id, next_id, note='合成更正', confirmed=True)
        with self.assertRaises(StaleEvidence):
            self.save('late', context)
        current = self.decisions.context('series', 'original')['evidence']['report']
        self.assertEqual((current['valid'], current['counts']['review_required']), (0, 1))
        previous = self.decisions.list('series', 'original')[0]
        self.assertEqual(previous['evidence_status'], 'changed')
        self.assertEqual(previous['record']['evidence']['report']['valid'], 1)

    def decision_digest(self):
        context = self.decisions.context('series', 'original')
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue',
                   'reason': '继续观察', 'limitations': '样本不足',
                   'evidence_sha256': context['evidence_sha256']}
        return _sha(request), context

    def test_unknown_fence_blocks_late_save_and_survives_reopen(self):
        digest, context = self.decision_digest()
        self.assertEqual(self.decisions.write_status('decision', 'late', digest)['status'], 'unknown')
        fenced = self.decisions.fence_write('decision', 'late', digest)
        self.assertEqual(fenced['status'], 'fenced')
        self.assertEqual(fenced, ResearchDecisions(self.path).write_status('decision', 'late', digest))
        self.assertEqual(fenced, self.decisions.fence_write('decision', 'late', digest))
        with self.assertRaisesRegex(DecisionError, '已封止'):
            self.save('late', context)
        with closing(sqlite3.connect(self.path)) as db, db:
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'fenced research decision'):
                db.execute('INSERT INTO research_decisions VALUES (?,?,?,?)', ('late', b'{}', '0'*64, b'{}'))
        with self.assertRaisesRegex(DecisionError, '摘要不一致'):
            self.decisions.write_status('decision', 'late', '0'*64)
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_committed_record_is_returned_and_mismatched_input_rejected(self):
        digest, context = self.decision_digest()
        record = self.save('done', context)
        state = self.decisions.fence_write('decision', 'done', digest)
        self.assertEqual(state['status'], 'committed'); self.assertEqual(state['record'], record)
        self.assertEqual(self.decisions.write_status('decision', 'done', digest), state)
        with self.assertRaisesRegex(DecisionError, '摘要不一致'):
            self.decisions.fence_write('decision', 'done', '1'*64)

    def test_draft_fence_and_committed_lookup_are_separate(self):
        self.save()
        digest = _sha({'decision_key': 'judgment', 'hypothesis': '新假设'})
        self.assertEqual(self.decisions.fence_write('draft', 'late-draft', digest)['status'], 'fenced')
        with self.assertRaisesRegex(DecisionError, '已封止'):
            self.decisions.draft('late-draft', decision_key='judgment', hypothesis='新假设')
        with closing(sqlite3.connect(self.path)) as db, db:
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'fenced research draft'):
                db.execute('INSERT INTO research_hypothesis_drafts VALUES (?,?,?,?,?)',
                           ('late-draft', 'judgment', b'{}', '0'*64, b'{}'))
        draft = self.decisions.draft('done-draft', decision_key='judgment', hypothesis='新假设')
        self.assertEqual(self.decisions.write_status('draft', 'done-draft', digest)['record'], draft)

    def test_save_and_fence_share_the_same_sqlite_write_lock(self):
        digest, context = self.decision_digest()
        entered, release = threading.Event(), threading.Event()
        original_context = self.decisions._context
        def delayed_context(db, kind, key):
            entered.set()
            if not release.wait(5):
                raise RuntimeError('test timeout')
            return original_context(db, kind, key)
        self.decisions._context = delayed_context
        with ThreadPoolExecutor(max_workers=2) as pool:
            save_future = pool.submit(self.save, 'racing', context)
            self.assertTrue(entered.wait(5))
            fence_future = pool.submit(self.decisions.fence_write, 'decision', 'racing', digest)
            release.set()
            record, state = save_future.result(10), fence_future.result(10)
        self.assertEqual(state['status'], 'committed'); self.assertEqual(state['record'], record)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM research_write_fences WHERE request_key='racing'").fetchone()[0], 0)

    def test_fence_schema_and_tampering_are_checked(self):
        digest, _ = self.decision_digest()
        self.decisions.fence_write('decision', 'protected', digest)
        with closing(sqlite3.connect(self.path)) as db, db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("DELETE FROM research_write_fences WHERE request_key='protected'")
            db.execute('DROP TRIGGER research_write_fences_no_update')
            db.execute("UPDATE research_write_fences SET request_sha256='broken'")
        check = check_database(self.path)
        self.assertEqual(check['status'], 'failed')
        with self.assertRaises(DecisionError):
            ResearchDecisions._write_state(self._db(), 'decision', 'protected', digest)

    def test_failed_fence_insert_and_quarantined_database_fail_closed(self):
        digest, context = self.decision_digest()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("CREATE TRIGGER fail_fence BEFORE INSERT ON research_write_fences "
                       "BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.decisions.fence_write('decision', 'rollback', digest)
        self.assertEqual(self.decisions.write_status('decision', 'rollback', digest)['status'], 'unknown')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER fail_fence')
            db.execute('CREATE TABLE recovery_quarantine (id INTEGER PRIMARY KEY, created_at TEXT, reason TEXT)')
        with self.assertRaises(RecoveryQuarantined):
            self.decisions.fence_write('decision', 'quarantined', digest)
        with self.assertRaises(RecoveryQuarantined):
            self.save('quarantined', context)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM research_write_fences').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT count(*) FROM research_decisions').fetchone()[0], 0)

    def test_integrity_rejects_missing_fenced_insert_guard(self):
        digest, _ = self.decision_digest()
        self.decisions.fence_write('decision', 'guarded', digest)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TRIGGER research_decisions_no_fenced_insert')
        self.assertEqual(check_database(self.path)['status'], 'failed')

    def test_prepare_uses_server_normalization_and_strictly_matches_the_write(self):
        _, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue',
                   'reason': '\u001c继续观察\u001c', 'limitations': ' 样本不足 ',
                   'evidence_sha256': context['evidence_sha256']}
        intent = self.decisions.prepare_write('decision', 'prepared', request)
        normalized = {**request, 'reason': '继续观察', 'limitations': '样本不足'}
        self.assertEqual(intent['request_sha256'], _sha(normalized))
        self.assertEqual(intent, self.decisions.prepare_write('decision', 'prepared', request))
        with self.assertRaisesRegex(DecisionError, '摘要不一致'):
            self.decisions.save('prepared', **{**request, 'reason': '修改后的理由'}, require_intent=True)
        with self.assertRaisesRegex(DecisionError, '摘要不一致'):
            self.decisions.save('prepared', **{**request, 'reason': '修改后的理由'})
        with self.assertRaisesRegex(DecisionError, '摘要不一致'):
            self.decisions.fence_write('decision', 'prepared', '0'*64)
        with self.assertRaisesRegex(DecisionError, '摘要不一致'):
            self.decisions.write_status('decision', 'prepared', '0'*64)
        saved = self.decisions.save('prepared', **request, require_intent=True)
        self.assertEqual(saved['request'], normalized)
        status = self.decisions.write_status('decision', 'prepared', intent['request_sha256'], require_intent=True)
        self.assertEqual(status['status'], 'committed'); self.assertEqual(status['record'], saved)
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_strict_lookup_and_write_refuse_another_database(self):
        _, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue', 'reason': '继续观察',
                   'limitations': '样本不足', 'evidence_sha256': context['evidence_sha256']}
        intent = self.decisions.prepare_write('decision', 'db-bound', request)
        other = ResearchDecisions(self.path.with_name('different-research.sqlite3'))
        with self.assertRaisesRegex(DecisionError, '未找到对应的写入准备'):
            other.write_status('decision', 'db-bound', intent['request_sha256'], require_intent=True)
        with self.assertRaisesRegex(DecisionError, '未找到对应的写入准备'):
            other.fence_write('decision', 'db-bound', intent['request_sha256'], require_intent=True)
        with self.assertRaisesRegex(DecisionError, '未找到对应的写入准备'):
            other.save('db-bound', **request, require_intent=True)
        self.assertEqual(self.decisions.write_status('decision', 'db-bound', intent['request_sha256'], require_intent=True)['status'], 'unknown')

    def test_retry_is_linked_only_to_one_fenced_same_object(self):
        _, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue', 'reason': '继续观察',
                   'limitations': '样本不足', 'evidence_sha256': context['evidence_sha256']}
        original = self.decisions.prepare_write('decision', 'first', request)
        with self.assertRaisesRegex(DecisionError, '已封止'):
            self.decisions.prepare_write('decision', 'premature', request, retry_of='first')
        self.decisions.fence_write('decision', 'first', original['request_sha256'], require_intent=True)
        retry = self.decisions.prepare_write('decision', 'second', {**request, 'reason': '更新理由'}, retry_of='first')
        self.assertEqual(retry['retry_of'], 'first')
        with self.assertRaisesRegex(DecisionError, '已有重试'):
            self.decisions.prepare_write('decision', 'third', request, retry_of='first')
        with self.assertRaisesRegex(DecisionError, '同一对象'):
            self.decisions.prepare_write('decision', 'other-object', {**request, 'key': 'other'}, retry_of='first')
        with self.assertRaisesRegex(DecisionError, '已封止'):
            self.decisions.save('first', **request, require_intent=True)
        self.assertEqual(self.decisions.save('second', **{**request, 'reason': '更新理由'}, require_intent=True)['request_key'], 'second')
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_object_slot_blocks_different_client_request_keys(self):
        _, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue', 'reason': '继续观察',
                   'limitations': '样本不足', 'evidence_sha256': context['evidence_sha256']}
        first = self.decisions.prepare_write('decision', 'client-one', request)
        with self.assertRaisesRegex(DecisionError, '已有待确认写入'):
            self.decisions.prepare_write('decision', 'client-two', request)
        current = self.decisions.current_write('decision', 'series', 'original')
        self.assertEqual((current['request_key'], current['slot_version']), ('client-one', first['slot_version']))
        self.assertEqual(self.decisions.write_status('decision', 'client-one', first['request_sha256'],
                                                     require_intent=True)['status'], 'unknown')
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_concurrent_new_request_keys_cannot_both_claim_one_object(self):
        _, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue', 'reason': '继续观察',
                   'limitations': '样本不足', 'evidence_sha256': context['evidence_sha256']}
        barrier = threading.Barrier(2)
        def prepare(key):
            barrier.wait()
            try:
                return ('prepared', ResearchDecisions(self.path).prepare_write('decision', key, request))
            except DecisionError as exc:
                return ('blocked', str(exc))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [pool.submit(prepare, 'race-a'), pool.submit(prepare, 'race-b')]
            outcomes = [item.result() for item in results]
        self.assertEqual(sorted(item[0] for item in outcomes), ['blocked', 'prepared'])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM research_write_slots').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM research_write_intents').fetchone()[0], 1)

    def test_retry_atomically_transfers_slot_and_late_ack_cannot_release_successor(self):
        _, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue', 'reason': '继续观察',
                   'limitations': '样本不足', 'evidence_sha256': context['evidence_sha256']}
        first = self.decisions.prepare_write('decision', 'slot-first', request)
        self.decisions.fence_write('decision', 'slot-first', first['request_sha256'], require_intent=True)
        second = self.decisions.prepare_write('decision', 'slot-second',
                                              {**request, 'reason': '重新核对后继续'}, retry_of='slot-first')
        self.assertEqual(second['slot_version'], first['slot_version'] + 1)
        with self.assertRaisesRegex(DecisionError, '占用已变化'):
            self.decisions.acknowledge_write('decision', 'slot-first', first['request_sha256'], first['slot_version'])
        current = self.decisions.current_write('decision', 'series', 'original')
        self.assertEqual((current['request_key'], current['slot_version']),
                         ('slot-second', second['slot_version']))
        self.decisions.save('slot-second', **{**request, 'reason': '重新核对后继续'}, require_intent=True)
        ack = self.decisions.acknowledge_write('decision', 'slot-second', second['request_sha256'],
                                               second['slot_version'])
        self.assertEqual(ack['result_status'], 'committed')
        self.assertEqual(self.decisions.current_write('decision', 'series', 'original')['status'], 'none')
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_acknowledgment_is_terminal_idempotent_and_required_before_new_write(self):
        _, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue', 'reason': '继续观察',
                   'limitations': '样本不足', 'evidence_sha256': context['evidence_sha256']}
        first = self.decisions.prepare_write('decision', 'ack-first', request)
        with self.assertRaisesRegex(DecisionError, '只有已提交或已封止'):
            self.decisions.acknowledge_write('decision', 'ack-first', first['request_sha256'], first['slot_version'])
        self.decisions.save('ack-first', **request, require_intent=True)
        ack = self.decisions.acknowledge_write('decision', 'ack-first', first['request_sha256'], first['slot_version'])
        self.assertEqual(self.decisions.acknowledge_write('decision', 'ack-first', first['request_sha256'],
                                                          first['slot_version']), ack)
        second = self.decisions.prepare_write('decision', 'ack-second', request)
        self.assertEqual(second['slot_version'], 1)
        # A repeated acknowledgment for the old request must not delete a newer slot.
        self.decisions.acknowledge_write('decision', 'ack-first', first['request_sha256'], first['slot_version'])
        self.assertEqual(self.decisions.current_write('decision', 'series', 'original')['request_key'], 'ack-second')
        self.assertEqual(check_database(self.path)['status'], 'passed')

    def test_draft_slots_are_scoped_to_parent_decision(self):
        self.save('parent-one')
        self.save('parent-two')
        first = self.decisions.prepare_write('draft', 'draft-one',
                                             {'decision_key': 'parent-one', 'hypothesis': '假设一'})
        with self.assertRaisesRegex(DecisionError, '已有待确认写入'):
            self.decisions.prepare_write('draft', 'draft-two',
                                         {'decision_key': 'parent-one', 'hypothesis': '假设二'})
        second = self.decisions.prepare_write('draft', 'draft-other-parent',
                                              {'decision_key': 'parent-two', 'hypothesis': '假设三'})
        self.assertNotEqual(first['request_key'], second['request_key'])
        self.assertEqual(self.decisions.current_write('draft', 'decision', 'parent-one')['request_key'], 'draft-one')
        self.assertEqual(self.decisions.current_write('draft', 'decision', 'parent-two')['request_key'],
                         'draft-other-parent')

    def test_integrity_rejects_modified_slot_and_acknowledgment(self):
        _, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue', 'reason': '继续观察',
                   'limitations': '样本不足', 'evidence_sha256': context['evidence_sha256']}
        intent = self.decisions.prepare_write('decision', 'integrity-slot', request)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE research_write_slots SET object_key='other' WHERE request_key='integrity-slot'")
        self.assertEqual(check_database(self.path)['status'], 'failed')

    def test_isolated_recovery_copy_preserves_fence_and_cannot_execute(self):
        digest, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue', 'reason': '继续观察',
                   'limitations': '样本不足', 'evidence_sha256': context['evidence_sha256']}
        self.decisions.prepare_write('decision', 'recovery-fence', request)
        self.decisions.fence_write('decision', 'recovery-fence', digest, require_intent=True)
        self.decisions.prepare_write('decision', 'recovery-retry', request, retry_of='recovery-fence')
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as tmp:
            target = Path(tmp) / 'candidate.sqlite3'
            report = prepare_isolated_copy(self.path, target)
            self.assertEqual(report['status'], 'prepared')
            self.assertEqual(report['check']['counts']['research_write_fences'], 1)
            self.assertEqual(report['check']['counts']['research_write_intents'], 2)
            with closing(sqlite3.connect(target)) as db:
                row = db.execute('SELECT operation,request_key,request_sha256 FROM research_write_fences').fetchone()
                self.assertEqual(row, ('decision', 'recovery-fence', digest))
                self.assertEqual(db.execute("SELECT retry_of FROM research_write_intents WHERE request_key='recovery-retry'").fetchone(), ('recovery-fence',))
            with self.assertRaises(RecoveryQuarantined):
                ResearchDecisions(target).fence_write('decision', 'new', digest)

    def test_status_and_fence_routes_require_session_and_csrf(self):
        digest, _ = self.decision_digest()
        handler = object.__new__(Handler)
        handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', research_decisions=self.decisions)
        handler._validate_host = lambda: True; handler._session = lambda: True
        handler._discard_bounded_body = lambda: None; handler._json = lambda status, data, **kwargs: (status, data)
        data = {'operation': 'decision', 'request_key': 'route', 'request_sha256': digest}
        handler.path = '/api/research-writes/status?' + urlencode(data)
        status, response = handler.do_GET()
        self.assertEqual(status, 200); self.assertEqual(response['write']['status'], 'unknown')
        handler._session = lambda: False
        self.assertEqual(handler.do_GET()[0], 401)
        handler._session = lambda: True; handler.path = '/api/research-writes/fence'
        encoded = json.dumps(data).encode('utf-8'); handler.rfile = BytesIO(encoded)
        handler.headers = {'Origin': handler.server.origin, 'X-DLT-CSRF': 'wrong',
                           'Content-Type': 'application/json', 'Content-Length': str(len(encoded))}
        self.assertEqual(handler.do_POST()[0], 403)
        self.assertEqual(self.decisions.write_status('decision', 'route', digest)['status'], 'unknown')
        handler.headers['X-DLT-CSRF'] = 'secret'; handler.rfile = BytesIO(encoded)
        status, response = handler.do_POST()
        self.assertEqual(status, 200); self.assertEqual(response['write']['status'], 'fenced')

    def test_prepare_and_strict_status_routes_use_the_server_normalized_digest(self):
        _, context = self.decision_digest()
        request = {'kind': 'series', 'key': 'original', 'choice': 'continue', 'reason': ' 继续观察 ',
                   'limitations': '样本不足', 'evidence_sha256': context['evidence_sha256']}
        body = {'operation': 'decision', 'request_key': 'prepared-route', 'request': request}
        encoded = json.dumps(body, ensure_ascii=False).encode('utf-8')
        handler = object.__new__(Handler)
        handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', research_decisions=self.decisions)
        handler._validate_host = lambda: True; handler._session = lambda: True
        handler._discard_bounded_body = lambda: None; handler._json = lambda status, data, **kwargs: (status, data)
        handler.path = '/api/research-writes/prepare'; handler.rfile = BytesIO(encoded)
        handler.headers = {'Origin': handler.server.origin, 'X-DLT-CSRF': 'secret',
                           'Content-Type': 'application/json', 'Content-Length': str(len(encoded))}
        status, response = handler.do_POST()
        self.assertEqual(status, 200); intent = response['intent']
        self.assertEqual(intent['request_sha256'], _sha({**request, 'reason': '继续观察'}))
        self.assertEqual(intent['slot_version'], 1)
        query_current = {'operation': 'decision', 'object_kind': 'series', 'object_key': 'original'}
        handler.path = '/api/research-writes/current?' + urlencode(query_current)
        current = handler.do_GET()[1]['write']
        self.assertEqual((current['request_key'], current['status'], current['slot_version']),
                         ('prepared-route', 'unknown', 1))
        collision = {'operation': 'decision', 'request_key': 'other-browser', 'request': request}
        encoded_collision = json.dumps(collision, ensure_ascii=False).encode('utf-8')
        handler.path = '/api/research-writes/prepare'; handler.rfile = BytesIO(encoded_collision)
        handler.headers['Content-Length'] = str(len(encoded_collision))
        blocked_status, blocked = handler.do_POST()
        self.assertEqual(blocked_status, 409)
        self.assertEqual(blocked['code'], 'object_occupied')
        self.assertEqual(blocked['write']['request_key'], 'prepared-route')
        query = {'operation': 'decision', 'request_key': 'prepared-route',
                 'request_sha256': intent['request_sha256'], 'require_intent': '1'}
        handler.path = '/api/research-writes/status?' + urlencode(query)
        self.assertEqual(handler.do_GET()[1]['write']['status'], 'unknown')
        write_body = {'request_key': 'prepared-route', 'require_intent': True, **request}
        encoded = json.dumps(write_body, ensure_ascii=False).encode('utf-8')
        handler.path = '/api/research-decisions'; handler.rfile = BytesIO(encoded)
        handler.headers['Content-Length'] = str(len(encoded))
        saved_status, saved = handler.do_POST()
        self.assertEqual(saved_status, 200); self.assertEqual(saved['decision']['request']['reason'], '继续观察')
        handler.path = '/api/research-writes/status?' + urlencode(query)
        committed = handler.do_GET()[1]['write']
        self.assertEqual((committed['status'], committed['slot_version']), ('committed', 1))
        ack_body = {'operation': 'decision', 'request_key': 'prepared-route',
                    'request_sha256': intent['request_sha256'], 'slot_version': intent['slot_version']}
        encoded_ack = json.dumps(ack_body).encode('utf-8')
        handler.path = '/api/research-writes/acknowledge'; handler.rfile = BytesIO(encoded_ack)
        handler.headers['Content-Length'] = str(len(encoded_ack))
        ack_status, ack_response = handler.do_POST()
        self.assertEqual(ack_status, 200)
        self.assertEqual(ack_response['acknowledgment']['result_status'], 'committed')
        handler.path = '/api/research-writes/current?' + urlencode(query_current)
        self.assertEqual(handler.do_GET()[1]['write']['status'], 'none')
        other = ResearchDecisions(self.path.with_name('route-other.sqlite3'))
        handler.server.research_decisions = other
        self.assertEqual(handler.do_GET()[0], 409)
        query['require_intent'] = 'invalid'; handler.path = '/api/research-writes/status?' + urlencode(query)
        self.assertEqual(handler.do_GET()[0], 400)

    def test_http_decision_write_rejects_legacy_submission_without_preparation(self):
        _, context = self.decision_digest()
        body = {'request_key': 'legacy-http', 'kind': 'series', 'key': 'original',
                'choice': 'continue', 'reason': '继续观察', 'limitations': '样本不足',
                'evidence_sha256': context['evidence_sha256']}
        encoded = json.dumps(body, ensure_ascii=False).encode('utf-8')
        handler = object.__new__(Handler)
        handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='secret', research_decisions=self.decisions)
        handler._validate_host = lambda: True; handler._session = lambda: True
        handler._discard_bounded_body = lambda: None; handler._json = lambda status, data, **kwargs: (status, data)
        handler.path = '/api/research-decisions'; handler.rfile = BytesIO(encoded)
        handler.headers = {'Origin': handler.server.origin, 'X-DLT-CSRF': 'secret',
                           'Content-Type': 'application/json', 'Content-Length': str(len(encoded))}
        status, response = handler.do_POST()
        self.assertEqual(status, 400)
        self.assertIn('必须先准备', response['error'])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM research_decisions WHERE request_key='legacy-http'").fetchone()[0], 0)


class FixedDecisionTests(unittest.TestCase):
    freeze = fixed_fixtures.FixedEvaluationTests.freeze
    wait = fixed_fixtures.FixedEvaluationTests.wait

    def setUp(self):
        fixed_fixtures.FixedEvaluationTests.setUp(self)
        self.freeze()
        self.decisions = ResearchDecisions(self.path)
        self.decisions.initialize()

    def test_fixed_evaluation_status_change_does_not_rewrite_saved_judgment(self):
        context = self.decisions.context('fixed_evaluation', self.reservation)
        self.assertEqual(context['evidence']['status'], 'pending')
        saved = self.decisions.save('fixed-judgment', kind='fixed_evaluation', key=self.reservation,
                                    choice='not_participate', reason='等待评估', limitations='尚无结果',
                                    evidence_sha256=context['evidence_sha256'])
        self.service.start(self.reservation, request_key='assessment')
        finished = self.wait()
        self.assertEqual(finished['status'], 'complete', finished['error'])
        updated = self.decisions.context('fixed_evaluation', self.reservation)
        self.assertEqual(updated['evidence']['validity'], 'local_descriptive')
        self.assertEqual(len(updated['evidence']['methods']), 2)
        self.assertNotEqual(context['evidence_sha256'], updated['evidence_sha256'])
        with self.assertRaises(StaleEvidence):
            self.decisions.save('stale-fixed', kind='fixed_evaluation', key=self.reservation,
                                choice='finish', reason='结束', limitations='无推断',
                                evidence_sha256=context['evidence_sha256'])
        entry = self.decisions.list('fixed_evaluation', self.reservation)[0]
        self.assertEqual(entry['evidence_status'], 'changed')
        self.assertEqual(entry['record'], saved)
