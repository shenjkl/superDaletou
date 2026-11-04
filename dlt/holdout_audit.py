"""Inspect recorded local uses of a candidate holdout range.

Absence of recorded use does not establish that the researcher or an external
tool has never seen those draws. This is not a holdout reservation or test.
"""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from dlt.candidates import _canonical
from dlt.snapshots import parse_snapshot


class HoldoutAuditError(ValueError):
    pass


def audit_holdout(path, snapshot_id, start_issue, end_issue):
    if (not isinstance(snapshot_id, str) or not re.fullmatch(r'[0-9a-f]{64}', snapshot_id)
            or any(not isinstance(x, str) or not re.fullmatch(r'[0-9]{5}', x) for x in (start_issue, end_issue))):
        raise HoldoutAuditError('快照标识或奖期格式无效')
    found, unresolved = [], []
    started_at = datetime.now(timezone.utc).isoformat()
    db_path = Path(path).resolve()
    if not db_path.is_file():
        raise HoldoutAuditError('研究数据库不存在')
    with closing(sqlite3.connect(db_path.as_uri()+'?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        cache = {}
        def source_issues(ident):
            if ident in cache:
                return cache[ident]
            row = db.execute('SELECT canonical_json,row_count FROM snapshots WHERE id=?', (ident,)).fetchone()
            if not row or hashlib.sha256(row[0]).hexdigest() != ident:
                raise HoldoutAuditError('快照不存在或摘要不符')
            _, parsed = parse_snapshot(row[0])
            expected = sorted((x['draw_date'], x['issue']) for x in parsed)
            stored = db.execute('SELECT draw_date,issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue',
                                (ident,)).fetchall()
            if len(expected) != row[1] or stored != expected:
                raise HoldoutAuditError('快照奖期与留存记录不一致')
            cache[ident] = [x[1] for x in expected]
            return cache[ident]
        selected = source_issues(snapshot_id)
        if start_issue not in selected or end_issue not in selected:
            raise HoldoutAuditError('起止奖期必须存在于所选快照')
        first, last = selected.index(start_issue), selected.index(end_issue)
        if first > last or last-first+1 > 200:
            raise HoldoutAuditError('起始奖期不得晚于结束奖期，范围最多 200 期')
        requested = selected[first:last+1]

        def interval(ident, first_issue, last_issue):
            issues = source_issues(ident)
            if first_issue not in issues or last_issue not in issues:
                raise HoldoutAuditError('recorded history endpoints unavailable')
            a, b = issues.index(first_issue), issues.index(last_issue)
            if a > b:
                raise HoldoutAuditError('recorded history endpoints inverted')
            return set(issues[a:b+1])

        def add(kind, key, status, input_issues=(), target_issues=()):
            input_set, outcome_set = set(input_issues), set(target_issues)
            inputs = [issue for issue in requested if issue in input_set]
            outcomes = [issue for issue in requested if issue in outcome_set]
            if inputs or outcomes:
                found.append({'kind': kind, 'record_key': str(key), 'status': status,
                              'input_issues': inputs, 'target_issues': outcomes})

        def problem(kind, key):
            unresolved.append({'kind': kind, 'record_key': str(key), 'reason': 'record or referenced history could not be checked'})

        if 'exploration_activities' in tables:
            for key, digest, raw, status in db.execute('SELECT id,content_sha256,content_json,status FROM exploration_activities'):
                try:
                    if hashlib.sha256(raw).hexdigest() != digest:
                        raise HoldoutAuditError('activity digest mismatch')
                    activity = json.loads(raw)
                    if (activity['version'] != 'exploration-activity-v1' or not isinstance(activity['inputs'], list)
                            or not isinstance(activity['targets'], list) or status not in ('running', 'finished')):
                        raise HoldoutAuditError('activity content invalid')
                    add('exploration_activity', key, status, activity['inputs'], activity['targets'])
                except (ValueError, TypeError, KeyError, IndexError, sqlite3.Error):
                    problem('exploration_activity', key)

        if 'training_jobs' in tables:
            for key, raw, status in db.execute('SELECT request_key,request_json,status FROM training_jobs'):
                try:
                    request = json.loads(raw); cfg = request['config']
                    add('training_task', key, status, interval(cfg['snapshot_id'], request['first_issue'], cfg['cutoff']))
                except (ValueError, TypeError, KeyError, IndexError, sqlite3.Error):
                    problem('training_task', key)
        if 'model_artifacts' in tables:
            for key, raw in db.execute('SELECT artifact_id,artifact_json FROM model_artifacts'):
                try:
                    artifact = json.loads(raw); content = dict(artifact); digest = content.pop('artifact_sha256')
                    if digest != key or hashlib.sha256(_canonical(content)).hexdigest() != key:
                        raise HoldoutAuditError('artifact digest mismatch')
                    add('model_artifact', key, 'saved', interval(artifact['config']['snapshot_id'],
                        artifact['history_first_issue'], artifact['history_last_issue']))
                except (ValueError, TypeError, KeyError, IndexError, sqlite3.Error):
                    problem('model_artifact', key)
        if 'draft_pairs' in tables:
            for key, request_raw, digest, raw in db.execute('SELECT request_key,request_json,pair_sha256,pair_json FROM draft_pairs'):
                try:
                    request, pair = json.loads(request_raw), json.loads(raw)
                    content = dict(pair); stored_digest = content.pop('pair_sha256')
                    if (digest != stored_digest or hashlib.sha256(_canonical(content)).hexdigest() != digest
                            or pair['config'] != request['config']):
                        raise HoldoutAuditError('pair digest mismatch')
                    add('candidate_generation', key, 'saved', interval(request['config']['snapshot_id'],
                        request['history_first_issue'], request['history_cutoff_issue']))
                except (ValueError, TypeError, KeyError, IndexError, sqlite3.Error):
                    problem('candidate_generation', key)

        if 'draft_evaluations' in tables:
            for key, pair_key, request_raw, digest, raw in db.execute(
                    'SELECT request_key,pair_key,request_json,result_sha256,result_json FROM draft_evaluations'):
                try:
                    request, result = json.loads(request_raw), json.loads(raw)
                    scenario = request['scenario']
                    if (hashlib.sha256(raw).hexdigest() != digest or request['pair_key'] != pair_key
                            or result['scenario_sha256'] != hashlib.sha256(_canonical(scenario)).hexdigest()):
                        raise HoldoutAuditError('evaluation or scenario digest mismatch')
                    dependency = scenario.get('source_dependency')
                    if dependency is None:
                        if scenario.get('kind') == 'custom_payout_v1':
                            from dlt.custom_scenarios import check_custom_scenario
                            from dlt.pair_repository import PairRepository
                            pair_row = db.execute('SELECT pair_sha256,pair_json FROM draft_pairs WHERE request_key=?', (pair_key,)).fetchone()
                            if not pair_row:
                                raise HoldoutAuditError('custom evaluation pair missing')
                            check_custom_scenario(PairRepository._decode(pair_row), scenario)
                            provenance = scenario.get('custom_input', {}).get('provenance')
                            if provenance is None:
                                from dlt.custom_origin import declarations
                                declarations_list = declarations(db, key, result['scenario_sha256'])
                                if not declarations_list:
                                    unresolved.append({'kind': 'custom_scenario_origin', 'record_key': str(key),
                                                       'reason': 'custom payout origin not declared'})
                                for declaration in declarations_list:
                                    if declaration['kind'] == 'historical':
                                        add('portfolio_scenario_unverified', key, 'user_declared',
                                            target_issues=[declaration['issue']])
                            elif provenance['kind'] == 'historical':
                                add('portfolio_scenario_unverified', key, 'user_declared', target_issues=[provenance['issue']])
                            continue
                        # A legacy/free-form scenario may name an actual issue
                        # without a source; conservatively treat the label as
                        # possible use, even if its inputs were synthetic.
                        label = scenario.get('source_issue')
                        if isinstance(label, str) and re.fullmatch(r'[0-9]{5}', label):
                            add('portfolio_scenario_unverified', key, 'saved', target_issues=[label])
                        continue
                    issue = dependency['issue']
                    snapshot = dependency['snapshot_id']
                    if (not isinstance(issue, str) or not re.fullmatch(r'[0-9]{5}', issue)
                            or scenario.get('source_issue') != issue or issue not in source_issues(snapshot)):
                        raise HoldoutAuditError('evaluation source issue is invalid')
                    row = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?',
                                     (snapshot, issue)).fetchone()
                    if not row or hashlib.sha256(row[0]).hexdigest() != dependency['row_sha256']:
                        raise HoldoutAuditError('evaluation source row mismatch')
                    add('portfolio_scenario_evaluation', key, 'saved', target_issues=[issue])
                except (ValueError, TypeError, KeyError, IndexError, AttributeError, sqlite3.Error):
                    problem('portfolio_scenario_evaluation', key)

        def check_backtest(kind, key, status, protocol):
            issues = source_issues(protocol['snapshot_id']); targets = protocol['targets']; window = protocol['history_periods']
            if (not isinstance(targets, list) or not targets or type(window) is not int or window < 1
                    or any(issue not in issues for issue in targets)):
                raise HoldoutAuditError('backtest input provenance unavailable')
            history = set()
            for issue in targets:
                index = issues.index(issue)
                history.update(issues[max(0, index-window):index])
            add(kind, key, status, history, targets)

        jobs = {}
        if 'backtest_jobs' in tables:
            for key, raw, status in db.execute('SELECT request_key,request_json,status FROM backtest_jobs'):
                jobs[key] = None
                try:
                    protocol = json.loads(raw)['protocol']
                    check_backtest('backtest_task', key, status, protocol)
                    jobs[key] = protocol
                except (ValueError, TypeError, KeyError, IndexError, sqlite3.Error):
                    problem('backtest_task', key)
        if 'backtest_reports' in tables:
            for key, raw_request, digest, raw in db.execute('SELECT request_key,request_json,report_sha256,report_json FROM backtest_reports'):
                try:
                    if hashlib.sha256(raw).hexdigest() != digest:
                        raise HoldoutAuditError('report digest mismatch')
                    report, request = json.loads(raw), json.loads(raw_request)
                    if (report['protocol'] != request['protocol'] or report['scenarios'] != request['scenarios']
                            or report['protocol_sha256'] != hashlib.sha256(_canonical(report['protocol'])).hexdigest()
                            or report['scenarios_sha256'] != hashlib.sha256(_canonical(report['scenarios'])).hexdigest()
                            or (key in jobs and jobs[key] is not None and jobs[key] != report['protocol'])):
                        raise HoldoutAuditError('report protocol mismatch')
                    if key not in jobs:
                        check_backtest('backtest_report', key, 'saved', report['protocol'])
                except (ValueError, TypeError, KeyError, IndexError, sqlite3.Error):
                    problem('backtest_report', key)
        if 'fixed_evaluations' in tables:
            for key, digest, raw, status, started, finished, error, report_sha, report_raw in db.execute(
                    'SELECT reservation_id,protocol_sha256,protocol_json,status,started_at,finished_at,error,report_sha256,report_json FROM fixed_evaluations'):
                try:
                    if hashlib.sha256(raw).hexdigest() != digest:
                        raise HoldoutAuditError('fixed evaluation digest mismatch')
                    spec = json.loads(raw)
                    if (not isinstance(spec.get('methods'), list) or not spec['methods']
                            or any(not isinstance(item, dict) or not isinstance(item.get('protocol'), dict)
                                   or not isinstance(item['protocol'].get('targets'), list) for item in spec['methods'])):
                        raise HoldoutAuditError('fixed evaluation protocol invalid')
                    if status == 'pending':
                        if (any(value is not None for value in (started, finished, error, report_sha, report_raw))
                                or ('fixed_evaluation_attempts' in tables and db.execute(
                                'SELECT 1 FROM fixed_evaluation_attempts WHERE reservation_id=? LIMIT 1', (key,)).fetchone())):
                            raise HoldoutAuditError('pending evaluation has an execution attempt')
                        if ('holdout_reservations' not in tables or spec.get('reservation_id') != key
                                or spec.get('version') != 'fixed-descriptive-evaluation-v1'
                                or spec.get('independence') != 'not_verified'):
                            raise HoldoutAuditError('pending evaluation has invalid reservation')
                        from dlt.fixed_evaluation import _load
                        _load(db, key)
                        reserved = db.execute('SELECT content_json FROM holdout_reservations WHERE id=?', (key,)).fetchone()
                        if not reserved or hashlib.sha256(reserved[0]).hexdigest() != key:
                            raise HoldoutAuditError('pending evaluation reservation digest mismatch')
                        reservation = json.loads(reserved[0])
                        from dlt.backtest import _validate
                        methods = spec['methods']
                        if len(methods) > 5 or len({item['id'] for item in methods}) != len(methods):
                            raise HoldoutAuditError('pending evaluation methods invalid')
                        reference_protocol = _validate(methods[0]['protocol']); baseline = reference_protocol['candidate']
                        for item in methods:
                            protocol = _validate(item['protocol']); candidate = protocol['candidate']
                            if (protocol['snapshot_id'] != reservation['snapshot_id']
                                    or protocol['targets'] != reservation['issues']
                                    or protocol['replicas'] != reference_protocol['replicas']
                                    or any(candidate.get(field) != baseline.get(field) for field in
                                           ('budget_fen', 'base_price_fen', 'additional_price_fen', 'additional',
                                            'structure', 'front_count', 'back_count', 'max_tickets'))):
                                raise HoldoutAuditError('pending evaluation scope mismatch')
                        continue
                    for item in spec['methods']:
                        check_backtest('fixed_evaluation', key+'/'+item['id'], status, item['protocol'])
                except (ValueError, TypeError, KeyError, IndexError, AttributeError, sqlite3.Error):
                    problem('fixed_evaluation', key)
        if 'paper_groups' in tables:
            for key, issue, digest, raw in db.execute('SELECT experiment_key,issue,id,content_json FROM paper_groups'):
                try:
                    if hashlib.sha256(raw).hexdigest() != digest or json.loads(raw)['issue'] != issue:
                        raise HoldoutAuditError('paper result digest mismatch')
                    add('paper_result', f'{key}/{digest[:12]}', 'saved', target_issues=[issue])
                except (ValueError, TypeError, KeyError, IndexError, sqlite3.Error):
                    problem('paper_result', key)
    return {'as_of_utc': started_at, 'snapshot_id': snapshot_id, 'issues': selected[first:last+1],
            'status': 'unresolved' if unresolved else 'known_use' if found else 'no_recorded_use',
            'findings': found, 'unresolved': unresolved, 'independence': 'not_verified',
            'limitations': ['only recorded local tasks, artifacts, drafts, scenario evaluations, reports and paper results were checked',
                            'historical outcomes or external use may already be known to the researcher',
                            'this check does not reserve data or establish an independent holdout']}
