"""Start the real application server for the Chrome source-write integration test."""
import sys
from pathlib import Path

from dlt.app import LocalServer
from dlt.recovery import protect_private_directory


data_directory = Path(sys.argv[1]).resolve()
data_directory.mkdir(parents=True, exist_ok=True)
protect_private_directory(data_directory)
server = LocalServer(0, data_directory / 'browser-test.sqlite3')
print(server.server_port, flush=True)
try:
    server.serve_forever()
finally:
    server.shutdown()
    server.server_close()
    server.jobs.stop()
    server.backtests.stop()
