"""Isolated worker: compute one historical reconstruction without publishing it."""
import json
import sys

from dlt.backtest import run_from_snapshot
from dlt.candidates import _canonical


def main():
    request = json.load(sys.stdin)
    from dlt.recovery_guard import require_path_execution_allowed
    require_path_execution_allowed(request['path'])
    report = run_from_snapshot(request['path'], request['protocol'], request['scenarios'])
    sys.stdout.buffer.write(_canonical(report))


if __name__ == '__main__':
    main()
