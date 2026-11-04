"""Claim a queued update run for the LocalServer browser cancellation test."""
import sys
from pathlib import Path

from dlt.scheduler import Scheduler


database = Path(sys.argv[1]).resolve()
plan_id = sys.argv[2]
claim = Scheduler(database).claim(plan_id)
if claim is None:
    raise SystemExit('the synthetic one-off update run was not claimable')
print(claim['run_id'])
