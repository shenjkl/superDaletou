"""Start the real application server for the Chrome source-write integration test."""
import sys
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dlt.app import LocalServer
from dlt.recovery import protect_private_directory

if os.environ.get('DLT_BROWSER_TEST_PAPER_CLOCK_OFFSET_DAYS'):
    import dlt.app as app_module
    base_paper_results = app_module.PaperResults
    offset_days = int(os.environ['DLT_BROWSER_TEST_PAPER_CLOCK_OFFSET_DAYS'])

    class BrowserTestPaperResults(base_paper_results):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault('clock', lambda: datetime.now(timezone.utc) + timedelta(days=offset_days))
            super().__init__(*args, **kwargs)

    app_module.PaperResults = BrowserTestPaperResults


data_directory = Path(sys.argv[1]).resolve()
data_directory.mkdir(parents=True, exist_ok=True)
protect_private_directory(data_directory)
port = int(sys.argv[2]) if len(sys.argv) > 2 else 0
server = LocalServer(port, data_directory / 'browser-test.sqlite3')
print(server.server_port, flush=True)
try:
    server.serve_forever()
finally:
    server.shutdown()
    server.server_close()
    server.jobs.stop()
    server.backtests.stop()
