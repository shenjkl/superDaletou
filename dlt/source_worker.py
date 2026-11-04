"""Isolated network fetch; parent enforces an overall process deadline."""
import base64
import json
import sys

from dlt.official import InvalidOfficialPayload, SourceError, fetch_page


def main():
    try:
        request = json.load(sys.stdin)
        try:
            result = fetch_page(**request)
        except InvalidOfficialPayload as invalid:
            result = {'status': 'invalid_payload', 'source': invalid.source,
                      'observed_at': invalid.observed_at, 'reason': str(invalid), 'raw': invalid.raw}
        else:
            result = {'status': 'valid', 'source': result['source'], 'observed_at': result['observed_at'],
                      'row_count': result['row_count'], 'raw': result['raw']}
        print(json.dumps({'status': result['status'], 'source': result['source'],
                          'observed_at': result['observed_at'], 'row_count': result.get('row_count'),
                          'reason': result.get('reason'),
                          'raw_base64': base64.b64encode(result['raw']).decode('ascii')}))
    except (ValueError, TypeError, SourceError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
