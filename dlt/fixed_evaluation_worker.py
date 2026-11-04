"""Compute only the locally fixed set of methods; parent publishes the report."""
from contextlib import closing
import json
import sqlite3
import sys

from dlt.backtest import _run_from_snapshot
from dlt.candidates import _canonical
from dlt.fixed_evaluation import _hash, _load


def main():
    request = json.load(sys.stdin)
    from dlt.recovery_guard import require_path_execution_allowed
    require_path_execution_allowed(request['path'])
    with closing(sqlite3.connect(request['path'])) as db:
        spec = _load(db, request['reservation_id'])
    reports = []
    for item in spec['methods']:
        result = _run_from_snapshot(request['path'], item['protocol'], spec['scenarios'],
                                    fixed_run=request['reservation_id'], fixed_attempt=request['attempt_id'], method_id=item['id'])
        reports.append({'id': item['id'], 'result': result})
    output = {'version': 'fixed-descriptive-report-v1', 'reservation_id': request['reservation_id'],
              'protocol_sha256': _hash(spec), 'methods': reports,
              'evidence': 'locally fixed historical reconstruction; independence, availability and source not independently verified'}
    sys.stdout.buffer.write(_canonical(output))


if __name__ == '__main__':
    main()
