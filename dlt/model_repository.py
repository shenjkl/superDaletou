"""Persist actual local training artifacts against immutable input snapshots.

This synchronous core is not a background job API. Historical reconstruction
is supported; strict replay must wait for an independently sourced availability
record rather than inventing publication times from a later snapshot.
"""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from dlt.models import TrainingError, _canonical, train
from dlt.snapshots import SnapshotStore
from dlt.training_boundary import check_holdout_boundary, check_rolling_target
from dlt.holdout_reservation import check_exploration_allowed, exploration_activity


def _assert_consistent_history(db, relevant):
    versions = db.execute('SELECT issue, draw_date, front_json, back_json FROM draw_versions').fetchall()
    variants = {}
    for issue, day, front, back in versions:
        if issue in relevant:
            variants.setdefault(issue, set()).add((day, front, back))
    if any(len(values) > 1 for values in variants.values()):
        raise TrainingError('unresolved number or date conflicts in selected history')


class ModelRepository:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def _initialize(self):
        SnapshotStore(self.path).initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys = ON')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS model_artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    snapshot_id TEXT NOT NULL REFERENCES snapshots(id),
                    method TEXT NOT NULL, cutoff TEXT NOT NULL,
                    availability_mode TEXT NOT NULL, artifact_json BLOB NOT NULL,
                    first_recorded_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS model_artifacts_no_update BEFORE UPDATE ON model_artifacts
                    BEGIN SELECT RAISE(ABORT, 'immutable model artifact'); END;
                CREATE TRIGGER IF NOT EXISTS model_artifacts_no_delete BEFORE DELETE ON model_artifacts
                    BEGIN SELECT RAISE(ABORT, 'immutable model artifact'); END;
            ''')

    def train_from_snapshot(self, snapshot_id, config, *, first_issue):
        from dlt.recovery_guard import require_path_execution_allowed
        require_path_execution_allowed(self.path)
        if not isinstance(config, dict) or config.get('snapshot_id') != snapshot_id:
            raise TrainingError('configuration must reference the selected snapshot')
        source = SnapshotStore(self.path).get_draws(snapshot_id)
        issues = [r['issue'] for r in source]
        if first_issue not in issues or config.get('cutoff') not in issues:
            raise TrainingError('history endpoints must exist in the selected snapshot')
        first, last = issues.index(first_issue), issues.index(config['cutoff'])
        if first > last:
            raise TrainingError('history start must precede cutoff')
        targets = [config['rolling_issue']] if config.get('rolling_issue') else []
        with exploration_activity(self.path, kind='training', inputs=issues[first:last+1], targets=targets):
            return self._train_from_snapshot(snapshot_id, config, first_issue=first_issue)

    def _train_from_snapshot(self, snapshot_id, config, *, first_issue):
        """Train on a contiguous source-order slice ending at the supplied cutoff.

        Input provenance is fetched from the stored snapshot, never supplied as
        arbitrary training rows by the caller. No artifact is published if any
        training or validation step fails. This is a synchronous operation;
        cancellation/resource limits and resumable tasks require the service.
        """
        if not isinstance(config, dict) or config.get('snapshot_id') != snapshot_id:
            raise TrainingError('configuration must reference the selected snapshot')
        if config.get('availability_mode') != 'historical_reconstruction':
            raise TrainingError('strict replay requires verified availability evidence, not snapshot observation time')
        if not self.path.is_file():
            raise TrainingError('stored snapshot not found')
        source = SnapshotStore(self.path).get_draws(snapshot_id)
        issues = [r['issue'] for r in source]
        check_holdout_boundary(issues, first_issue, config)
        if first_issue not in issues or config.get('cutoff') not in issues:
            raise TrainingError('history endpoints must exist in the selected snapshot')
        first, last = issues.index(first_issue), issues.index(config['cutoff'])
        if first > last:
            raise TrainingError('history start must precede cutoff')
        # A changed historical number/date in any recorded source is unresolved
        # until a separate resolution workflow exists. Prize-only differences do
        # not block a model whose input uses numbers alone.
        relevant = set(issues[first:last+1])
        with closing(sqlite3.connect(self.path)) as db:
            check_rolling_target(db, config)
            check_exploration_allowed(db, relevant | {config.get('rolling_issue')})
            _assert_consistent_history(db, relevant)
        history = [{'issue': r['issue'], 'date': r['draw_date'], 'front': r['front'], 'back': r['back']}
                   for r in source[first:last+1] if r['numbers_usable']]
        if len(history) != last-first+1:
            raise TrainingError('history includes a draw not eligible for number research')
        artifact = train(history, config)
        payload = _canonical(artifact)
        self._initialize()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('PRAGMA foreign_keys = ON')
            db.execute('BEGIN IMMEDIATE')
            from dlt.recovery_guard import require_execution_allowed
            require_execution_allowed(db)
            current_issues = [r[0] for r in db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue', (snapshot_id,))]
            check_holdout_boundary(current_issues, first_issue, config)
            check_rolling_target(db, config)
            check_exploration_allowed(db, relevant | {config.get('rolling_issue')})
            # Recheck under the write lock: a conflicting source may have been
            # imported while the model was training.
            _assert_consistent_history(db, relevant)
            existing = db.execute('SELECT artifact_json FROM model_artifacts WHERE artifact_id=?',
                                  (artifact['artifact_sha256'],)).fetchone()
            if existing is not None and existing[0] != payload:
                raise TrainingError('artifact identifier conflict')
            db.execute('INSERT OR IGNORE INTO model_artifacts VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (artifact['artifact_sha256'], snapshot_id, config['method'], config['cutoff'],
                        config['availability_mode'], payload, datetime.now(timezone.utc).isoformat()))
        return artifact['artifact_sha256']

    def get(self, artifact_id):
        """Return the immutable historical artifact, including if under review.

        User-facing inference paths must call get_for_prediction to check the
        current source status. A digest is integrity evidence, not a signature.
        """
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            return self.get_in_transaction(db, artifact_id)

    @staticmethod
    def get_in_transaction(db, artifact_id):
        """Validate an artifact within the caller's consistent read snapshot."""
        row = db.execute('SELECT artifact_json FROM model_artifacts WHERE artifact_id=?', (artifact_id,)).fetchone()
        if row is None:
            raise KeyError(artifact_id)
        artifact = json.loads(row[0])
        # Validate both the stored identifier and its content before returning.
        import hashlib
        content = dict(artifact)
        digest = content.pop('artifact_sha256', None)
        if artifact_id != digest or digest != hashlib.sha256(_canonical(content)).hexdigest():
            raise TrainingError('stored artifact integrity check failed')
        return artifact

    def inspect(self, artifact_id):
        artifact = self.get(artifact_id)
        try:
            with closing(sqlite3.connect(self.path)) as db:
                db.execute('BEGIN')
                artifact = self.get_in_transaction(db, artifact_id)
                from dlt.model_pair_validation import validate_model_artifact
                if not validate_model_artifact(db, artifact):
                    return {'artifact': artifact, 'status': 'review_required',
                            'reason': '模型代码或运行时与训练记录不同，暂不能核验产物'}
        except (ValueError, KeyError, TypeError, sqlite3.Error) as exc:
            return {'artifact': artifact, 'status': 'review_required', 'reason': str(exc)}
        return {'artifact': artifact, 'status': 'ready', 'reason': None}

    def get_for_prediction(self, artifact_id):
        result = self.inspect(artifact_id)
        if result['status'] != 'ready':
            raise TrainingError('model artifact requires review: ' + result['reason'])
        return result['artifact']
