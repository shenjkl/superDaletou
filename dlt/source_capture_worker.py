"""Killable worker for bounded official source document retrieval."""
import json
import sys

from dlt.source_capture import SourceCaptureError, fetch_document


def main():
    try:
        request = json.loads(sys.stdin.buffer.read(8192))
        result = fetch_document(request['source_url'], request['kind'],
                                max_bytes=request['max_bytes'], timeout_seconds=request['timeout_seconds'])
        sys.stdout.write(json.dumps(result, ensure_ascii=True, separators=(',', ':')))
        return 0
    except (SourceCaptureError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        sys.stderr.write('source capture rejected')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
