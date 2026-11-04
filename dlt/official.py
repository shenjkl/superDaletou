"""Bounded read-only adapter for the official historical draw endpoint.

Obtaining a response does not certify its publication time, historical rule,
calendar deadline, tax treatment or promotion. Settlement remains blocked
until those dependencies are separately verified.
"""
from datetime import datetime, timezone
import base64
import json
import sqlite3
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from dlt.snapshots import InvalidSnapshot, parse_snapshot

BASE = 'https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry'
HARD_MAX_BYTES = 5 * 1024 * 1024


class SourceError(ValueError):
    pass


class InvalidOfficialPayload(SourceError):
    """A bounded official response was received but failed draw validation."""

    def __init__(self, raw, source, observed_at, reason):
        super().__init__(reason)
        self.raw = raw
        self.source = source
        self.observed_at = observed_at


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise SourceError('source redirect rejected')


def fetch_raw_page(*, page_size, timeout_seconds, max_bytes, page_no=1, opener=None):
    """Fetch bounded response bytes without parsing untrusted draw fields."""
    if type(page_size) is not int or not 1 <= page_size <= 100:
        raise SourceError('page_size must be an integer from 1 to 100')
    if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 120:
        raise SourceError('timeout must be greater than 0 and at most 120 seconds')
    if type(max_bytes) is not int or not 1 <= max_bytes <= HARD_MAX_BYTES:
        raise SourceError('max_bytes exceeds supported bounds')
    if type(page_no) is not int or not 1 <= page_no <= 1000:
        raise SourceError('page_no must be an integer from 1 to 1000')
    query = urlencode({'gameNo': 85, 'provinceId': 0, 'pageSize': page_size, 'isVerify': 1, 'pageNo': page_no})
    url = BASE + '?' + query
    request = Request(url, headers={'Accept': 'application/json', 'Accept-Encoding': 'identity',
                                    'User-Agent': 'DaletouResearch/1.0',
                                    'Referer': 'https://www.sporttery.cn/'}, method='GET')
    try:
        client = opener if opener is not None else build_opener(_NoRedirect())
        with client.open(request, timeout=timeout_seconds) as response:
            if response.status != 200 or response.geturl() != url:
                raise SourceError('unexpected status or redirected source')
            encoding = response.headers.get('Content-Encoding', 'identity').lower()
            if encoding not in ('', 'identity'):
                raise SourceError('unsupported response content encoding')
            raw = response.read(max_bytes + 1)
        if len(raw) > max_bytes:
            raise SourceError('source response exceeds configured byte limit')
    except SourceError:
        raise
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise SourceError(f'official source request or validation failed: {exc}') from exc
    return {'source': url, 'observed_at': datetime.now(timezone.utc).isoformat(), 'raw': raw}


def fetch_page(*, page_size, timeout_seconds, max_bytes, page_no=1, opener=None):
    """Fetch and validate one page without asserting completeness."""
    fetched = fetch_raw_page(page_size=page_size, timeout_seconds=timeout_seconds,
                             max_bytes=max_bytes, page_no=page_no, opener=opener)
    try:
        _, rows = parse_snapshot(fetched['raw'])
        if len(rows) > page_size:
            raise InvalidSnapshot('source returned more rows than requested')
    except InvalidSnapshot as exc:
        raise InvalidOfficialPayload(fetched['raw'], fetched['source'], fetched['observed_at'], str(exc)) from exc
    return {**fetched, 'row_count': len(rows), 'scope': 'single page; no completeness or rule verification implied'}


def fetch_latest(*, page_size, timeout_seconds, max_bytes, opener=None):
    return fetch_page(page_size=page_size, timeout_seconds=timeout_seconds, max_bytes=max_bytes, opener=opener)


def _retain_invalid_payload(store, invalid, transaction_guard):
    try:
        return store.record_quarantine(invalid.raw, invalid.source, invalid.observed_at,
                                       str(invalid)[:1000], transaction_guard=transaction_guard)
    except (InvalidSnapshot, OSError, sqlite3.Error) as exc:
        raise SourceError('官方开奖响应格式未通过核验，且隔离留存失败；未导入该响应') from exc


def update_snapshot(store, *, page_size, timeout_seconds, max_bytes, opener=None, transaction_guard=None):
    # The production path isolates the network request in a killable process:
    # a socket timeout alone does not bound a slow streaming response.
    try:
        fetched = (fetch_latest(page_size=page_size, timeout_seconds=timeout_seconds, max_bytes=max_bytes, opener=opener)
                   if opener is not None else fetch_latest_bounded(page_size=page_size, timeout_seconds=timeout_seconds,
                                                                   max_bytes=max_bytes))
    except InvalidOfficialPayload as exc:
        _retain_invalid_payload(store, exc, transaction_guard)
        raise SourceError('官方开奖响应格式未通过核验，原文已隔离留存') from exc
    return store.import_official(fetched['raw'], fetched['source'], fetched['observed_at'],
                                 transaction_guard=transaction_guard)


def fetch_latest_bounded(*, page_size, timeout_seconds, max_bytes):
    return fetch_page_bounded(page_size=page_size, timeout_seconds=timeout_seconds, max_bytes=max_bytes, page_no=1)


def fetch_page_bounded(*, page_size, timeout_seconds, max_bytes, page_no):
    if type(page_no) is not int or not 1 <= page_no <= 1000:
        raise SourceError('page_no must be an integer from 1 to 1000')
    request = {'page_size': page_size, 'timeout_seconds': timeout_seconds, 'max_bytes': max_bytes, 'page_no': page_no}
    try:
        process = subprocess.run([sys.executable, '-m', 'dlt.source_worker'],
                                 input=json.dumps(request).encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 timeout=timeout_seconds, check=False)
    except (subprocess.TimeoutExpired, OSError, TypeError, ValueError) as exc:
        raise SourceError(f'official source process timed out or could not start: {exc}') from exc
    if process.returncode:
        raise SourceError('official source process failed: ' + process.stderr.decode('utf-8', 'replace')[:1000])
    try:
        payload = json.loads(process.stdout)
        raw = base64.b64decode(payload['raw_base64'], validate=True)
        if len(raw) > max_bytes:
            raise ValueError('response exceeds configured byte limit')
        expected = BASE + '?' + urlencode({'gameNo': 85, 'provinceId': 0, 'pageSize': page_size, 'isVerify': 1, 'pageNo': page_no})
        observed_at = payload['observed_at']
        observed = datetime.fromisoformat(observed_at)
        if payload['source'] != expected or observed.tzinfo is None or observed.utcoffset() is None:
            raise ValueError('invalid source or observation time')
    except (KeyError, TypeError, ValueError, InvalidSnapshot) as exc:
        raise SourceError(f'invalid official source process result: {exc}') from exc
    if payload.get('status') == 'invalid_payload':
        reason = payload.get('reason')
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise SourceError('official source process returned an invalid quarantine reason')
        raise InvalidOfficialPayload(raw, expected, observed_at, reason)
    row_count = payload.get('row_count')
    if (payload.get('status') != 'valid' or type(row_count) is not int
            or not 1 <= row_count <= page_size):
        raise SourceError('official source process returned an invalid page result')
    return {'source': expected, 'observed_at': observed_at, 'raw': raw, 'row_count': row_count,
            'scope': 'single page; no completeness or rule verification implied'}


def update_page(store, *, page_no, page_size, timeout_seconds, max_bytes, transaction_guard=None):
    try:
        fetched = fetch_page_bounded(page_no=page_no, page_size=page_size, timeout_seconds=timeout_seconds, max_bytes=max_bytes)
    except InvalidOfficialPayload as exc:
        _retain_invalid_payload(store, exc, transaction_guard)
        raise SourceError('官方开奖响应格式未通过核验，原文已隔离留存') from exc
    return store.import_official(fetched['raw'], fetched['source'], fetched['observed_at'],
                                 transaction_guard=transaction_guard)
