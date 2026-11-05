"""Prepare a private database with a completed synthetic tree-model training job."""
import json
from pathlib import Path
import sqlite3
from contextlib import closing
import sys
import time

from dlt.models import ALGORITHM_VERSION, FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION
from dlt.recovery import protect_private_directory
from dlt.snapshots import SnapshotStore
from dlt.training_jobs import TrainingJobs


def main():
    if len(sys.argv) > 2 and sys.argv[2] == '--verify':
        database = Path(sys.argv[1]).resolve() / 'browser-test.sqlite3'
        pair_key, decision_key, evaluation_key = sys.argv[3:6]
        with closing(sqlite3.connect(database)) as db:
            result = {
                'pairs': db.execute('SELECT count(*) FROM draft_pairs WHERE request_key=?', (pair_key,)).fetchone()[0],
                'pair_receipts': db.execute('SELECT count(*) FROM pair_write_requests WHERE request_key=?', (pair_key,)).fetchone()[0],
                'decisions': db.execute('SELECT count(*) FROM model_selection_decisions WHERE request_key=?', (decision_key,)).fetchone()[0],
                'decision_receipts': db.execute('SELECT count(*) FROM selection_write_requests WHERE request_key=?', (decision_key,)).fetchone()[0],
                'evaluations': db.execute('SELECT count(*) FROM draft_evaluations WHERE request_key=?', (evaluation_key,)).fetchone()[0],
                'evaluation_receipts': db.execute('SELECT count(*) FROM evaluation_write_requests WHERE request_key=?', (evaluation_key,)).fetchone()[0],
            }
        print(json.dumps(result))
        return
    directory = Path(sys.argv[1]).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    protect_private_directory(directory)
    database = directory / 'browser-test.sqlite3'
    source = Path(__file__).resolve().parents[1] / 'research' / 'data' / 'official_page1.json'
    snapshot = SnapshotStore(database).import_official(
        source.read_bytes(), 'synthetic browser acceptance fixture', '2026-10-29T12:00:00+08:00')['snapshot_id']
    rows = SnapshotStore(database).get_draws(snapshot)
    first_issue, cutoff = rows[0]['issue'], rows[-1]['issue']
    train_end = rows[-11]['issue']
    job_key = 'browser-tree-training'
    config = {'snapshot_id': snapshot, 'method': 'tree', 'window': 5, 'seed': 123,
              'train_end': train_end, 'cutoff': cutoff, 'availability_mode': 'historical_reconstruction',
              'algorithm_version': ALGORITHM_VERSION['tree'],
              'feature_schema_version': FEATURE_SCHEMA_VERSION,
              'preprocessing_version': PREPROCESSING_VERSION['tree'],
              'parameters': {'max_depth': 2, 'min_leaf': 2}}
    jobs = TrainingJobs(database)
    jobs.submit(job_key, name='浏览器组合恢复验收模型', config=config,
                first_issue=first_issue, timeout=30)
    deadline = time.monotonic() + 45
    try:
        while time.monotonic() < deadline:
            job = jobs.get(job_key)
            if job['status'] != 'running':
                break
            time.sleep(.1)
        if job['status'] != 'complete' or not job.get('artifact_id'):
            raise RuntimeError(f'training fixture failed: {job.get("error")}')
    finally:
        jobs.stop()
    print(json.dumps({'snapshot_id': snapshot, 'job_key': job_key, 'artifact_id': job['artifact_id'],
                      'first_issue': first_issue, 'cutoff': cutoff, 'target': f'{int(cutoff)+1:05d}',
                      'target_date': '2026-11-07'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
