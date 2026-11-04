"""Train one artifact without publishing it; parent owns cancellation/commit."""
from contextlib import closing
import json
import sqlite3
import sys

from dlt.model_repository import _assert_consistent_history
from dlt.models import train
from dlt.snapshots import SnapshotStore
from dlt.training_boundary import check_holdout_boundary, check_rolling_target
from dlt.holdout_reservation import check_exploration_allowed


def main():
    try:
        request = json.load(sys.stdin)
        from dlt.recovery_guard import require_path_execution_allowed
        require_path_execution_allowed(request['path'])
        config = request['config']
        rows = SnapshotStore(request['path']).get_draws(config['snapshot_id'])
        issues = [row['issue'] for row in rows]
        check_holdout_boundary(issues, request['first_issue'], config)
        first, last = issues.index(request['first_issue']), issues.index(config['cutoff'])
        if first > last:
            raise ValueError('history start follows cutoff')
        selected = rows[first:last+1]
        if any(not row['numbers_usable'] for row in selected):
            raise ValueError('history contains unqualified numbers')
        with closing(sqlite3.connect(request['path'])) as db:
            check_rolling_target(db, config)
            check_exploration_allowed(db, {row['issue'] for row in selected} | {config.get('rolling_issue')})
            _assert_consistent_history(db, {row['issue'] for row in selected})
        history = [{'issue': row['issue'], 'date': row['draw_date'], 'front': row['front'], 'back': row['back']}
                   for row in selected]
        artifact = train(history, config)
        print(json.dumps(artifact, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False))
        return 0
    except Exception as exc:
        print(f'{type(exc).__name__}: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
