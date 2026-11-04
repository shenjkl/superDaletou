"""Connect persisted model artifacts and snapshot data to candidate generation.

This is a synchronous research service; the caller is responsible for
calendar/price verification, task resource limits and atomic publication.
"""
from __future__ import annotations

from contextlib import closing
import sqlite3

from dlt.candidates import CandidateError, generate_pair
from dlt.model_repository import ModelRepository, _assert_consistent_history
from dlt.models import TrainingError, predict
from dlt.snapshots import SnapshotStore
from dlt.holdout_reservation import check_exploration_allowed, exploration_activity


def generate_from_snapshot(path, config, *, history_first_issue, history_cutoff_issue,
                           artifact_id=None, prediction_at=None, target_deadline=None):
    snapshot_id = config.get('snapshot_id') if isinstance(config, dict) else None
    if not snapshot_id:
        raise CandidateError('snapshot identifier required')
    issues = [row['issue'] for row in SnapshotStore(path).get_draws(snapshot_id)]
    if history_first_issue not in issues or history_cutoff_issue not in issues:
        raise CandidateError('history endpoints must exist in the selected snapshot')
    first, last = issues.index(history_first_issue), issues.index(history_cutoff_issue)
    if first > last:
        raise CandidateError('history start must precede cutoff')
    targets = [config['target_issue']] if config.get('target_issue') else []
    with exploration_activity(path, kind='generation', inputs=issues[first:last+1], targets=targets):
        return _generate_from_snapshot(path, config, history_first_issue=history_first_issue,
                                       history_cutoff_issue=history_cutoff_issue, artifact_id=artifact_id,
                                       prediction_at=prediction_at, target_deadline=target_deadline)


def _generate_from_snapshot(path, config, *, history_first_issue, history_cutoff_issue,
                            artifact_id=None, prediction_at=None, target_deadline=None):
    snapshot_id = config.get('snapshot_id') if isinstance(config, dict) else None
    if not snapshot_id:
        raise CandidateError('snapshot identifier required')
    rows = SnapshotStore(path).get_draws(snapshot_id)
    issues = [r['issue'] for r in rows]
    if history_first_issue not in issues or history_cutoff_issue not in issues:
        raise CandidateError('history endpoints must exist in the selected snapshot')
    first, last = issues.index(history_first_issue), issues.index(history_cutoff_issue)
    if first > last:
        raise CandidateError('history start must precede cutoff')
    selected = rows[first:last+1]
    if any(not r['numbers_usable'] for r in selected):
        raise CandidateError('history has unqualified number records')
    try:
        with closing(sqlite3.connect(path)) as db:
            _assert_consistent_history(db, {r['issue'] for r in selected})
            check_exploration_allowed(db, {r['issue'] for r in selected} | {config.get('target_issue')})
    except TrainingError as exc:
        raise CandidateError(str(exc)) from exc
    history = [{'issue': r['issue'], 'date': r['draw_date'], 'front': r['front'], 'back': r['back']} for r in selected]
    method = config.get('method')
    if method in ('tree', 'lstm'):
        if not artifact_id:
            raise CandidateError('model artifact required')
        artifact = ModelRepository(path).get_for_prediction(artifact_id)
        if artifact['config']['method'] != method:
            raise CandidateError('model method does not match artifact')
        if artifact['config']['availability_mode'] == 'strict_replay':
            raise CandidateError('strict inference requires independently verified availability evidence')
        window = artifact['config']['window']
        if len(history) < window:
            raise CandidateError('insufficient model history')
        scores = predict(artifact, history[-window:], target_issue=config['target_issue'], target_date=config['target_date'],
                         prediction_at=prediction_at, target_deadline=target_deadline)
    else:
        if artifact_id is not None:
            raise CandidateError('non-model method must not reference a model artifact')
        scores = None
    # Recheck conflict status and source versions immediately before persistence
    # in the future transaction layer; this function only returns an in-memory pair.
    return generate_pair(history=history, config=config, model_scores=scores)
