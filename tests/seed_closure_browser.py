"""Seed a private browser-test database with one fixed experiment and one series."""
from pathlib import Path
import sqlite3
import sys

from dlt.recovery import protect_private_directory
from test_prospective_series import SERIES_KEY, SeriesTests


target = Path(sys.argv[1]).resolve()
if len(sys.argv) > 2 and sys.argv[2] == '--verify':
    with sqlite3.connect(target) as db:
        single = db.execute('SELECT count(*),min(reason) FROM experiment_closures WHERE experiment_key=?',
                            ('closure-browser-single',)).fetchone()
        series = db.execute('SELECT count(*),min(reason) FROM series_closures WHERE series_key=?',
                            (SERIES_KEY,)).fetchone()
    print(__import__('json').dumps({'single': single, 'series': series}))
    raise SystemExit(0)
protect_private_directory(target.parent)
fixture = SeriesTests('test_two_periods_preserve_protocol_and_close_without_cancelling_existing')
fixture.setUp()
try:
    fixture.repo.freeze('closure-browser-single', fixture.protocol)
    fixture.series.create(SERIES_KEY, fixture.manifest)
    fixture.repo.freeze('closure-browser-period', fixture.protocol, series_key=SERIES_KEY)
    with sqlite3.connect(fixture.path) as source, sqlite3.connect(target) as destination:
        source.backup(destination)
finally:
    fixture.doCleanups()

print(SERIES_KEY)
