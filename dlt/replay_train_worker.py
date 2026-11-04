"""Subprocess-only model training for an explicitly requested replay."""
import json
import sys

from dlt.models import _canonical, train


def main():
    try:
        raw = sys.stdin.buffer.read(10*1024*1024+1)
        if len(raw) > 10*1024*1024:
            return 2
        request = json.loads(raw)
        artifact = train(request['history'], request['config'])
        sys.stdout.buffer.write(_canonical({'model': artifact['model']}))
        return 0
    except Exception:
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
