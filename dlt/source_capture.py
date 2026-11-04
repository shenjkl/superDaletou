"""Fetch a bounded source document from an allowlisted official HTTPS host.

The capture retains transport bytes for a later personal review; it does not
authenticate the publisher, applicability period, publication time, or content.
"""
from __future__ import annotations

import base64
import hashlib
import json
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from dlt.prospective import _source_url

MAX_SOURCE_BYTES = 5 * 1024 * 1024
MAX_CAPTURE_SECONDS = 30


class SourceCaptureError(ValueError):
    pass


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise SourceCaptureError('官方来源重定向已拒绝')


def fetch_document(url, kind, *, max_bytes=MAX_SOURCE_BYTES, timeout_seconds=20, opener=None):
    if not _source_url(url, kind):
        raise SourceCaptureError('请使用允许的官方 HTTPS 来源网址')
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_SOURCE_BYTES:
        raise SourceCaptureError('来源文件大小限制无效')
    if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= MAX_CAPTURE_SECONDS:
        raise SourceCaptureError('来源读取时限无效')
    request = Request(url, headers={'Accept': 'application/pdf,image/*,text/plain,application/octet-stream',
                                    'Accept-Encoding': 'identity',
                                    'User-Agent': 'DaletouResearch/1.0'}, method='GET')
    try:
        client = opener if opener is not None else build_opener(_NoRedirect())
        with client.open(request, timeout=timeout_seconds) as response:
            if response.status != 200 or response.geturl() != url:
                raise SourceCaptureError('官方来源返回状态异常或地址发生变化')
            encoding = response.headers.get('Content-Encoding', 'identity').lower()
            if encoding not in ('', 'identity'):
                raise SourceCaptureError('官方来源返回了不支持的压缩内容')
            length = response.headers.get('Content-Length')
            if length is not None:
                try:
                    declared_length = int(length)
                except ValueError as exc:
                    raise SourceCaptureError('官方来源长度无效') from exc
                if declared_length < 0:
                    raise SourceCaptureError('官方来源长度无效')
                if declared_length > max_bytes:
                    raise SourceCaptureError('官方来源文件超过 5 MiB 限制')
            content_type = response.headers.get_content_type()
            raw = response.read(max_bytes + 1)
        if not raw:
            raise SourceCaptureError('官方来源文件为空')
        if len(raw) > max_bytes:
            raise SourceCaptureError('官方来源文件超过 5 MiB 限制')
        return {'source_url': url, 'document_base64': base64.b64encode(raw).decode('ascii'),
                'document_size': len(raw), 'document_sha256': hashlib.sha256(raw).hexdigest(),
                'content_type': content_type if isinstance(content_type, str) else 'application/octet-stream'}
    except SourceCaptureError:
        raise
    except HTTPError as exc:
        raise SourceCaptureError('官方来源请求返回 HTTP 错误') from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise SourceCaptureError('官方来源连接失败或读取超时') from exc


def capture_document_bounded(url, kind, *, timeout_seconds=20, max_bytes=MAX_SOURCE_BYTES):
    if not _source_url(url, kind):
        raise SourceCaptureError('请使用允许的官方 HTTPS 来源网址')
    if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= MAX_CAPTURE_SECONDS:
        raise SourceCaptureError('来源读取时限无效')
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_SOURCE_BYTES:
        raise SourceCaptureError('来源文件大小限制无效')
    request = {'source_url': url, 'kind': kind, 'timeout_seconds': timeout_seconds, 'max_bytes': max_bytes}
    try:
        process = subprocess.run([sys.executable, '-m', 'dlt.source_capture_worker'],
                                 input=json.dumps(request).encode('utf-8'), stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, timeout=MAX_CAPTURE_SECONDS + 3, check=False)
    except (subprocess.TimeoutExpired, OSError, TypeError, ValueError) as exc:
        raise SourceCaptureError('官方来源抓取进程无法启动或超时') from exc
    if process.returncode:
        raise SourceCaptureError('官方来源抓取进程失败')
    if len(process.stdout) > ((MAX_SOURCE_BYTES * 4 // 3) + 4096):
        raise SourceCaptureError('官方来源抓取结果超过大小限制')
    try:
        result = json.loads(process.stdout)
        raw = base64.b64decode(result['document_base64'], validate=True)
        if (result.get('source_url') != url or len(raw) != result.get('document_size')
                or len(raw) > max_bytes
                or hashlib.sha256(raw).hexdigest() != result.get('document_sha256')):
            raise ValueError('captured bytes differ')
        return result
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SourceCaptureError('官方来源抓取结果无法核验') from exc
