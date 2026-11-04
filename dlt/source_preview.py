"""Bounded local viewing of an authenticated stored source.

This is not a sanitized export: displayed text may contain personal data.
PDF pages are rasterized in an isolated worker; original PDF bytes are not returned.
"""
import base64
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import struct
import subprocess
import sys


MAX_DOCUMENT_BYTES = 5 * 1024 * 1024
PAGE_CHARS = 20000
IDENTIFIER = re.compile(r'[0-9a-f]{64}')
PDF_TIMEOUT_SECONDS = 10
MAX_PDF_IMAGE_BYTES = 2 * 1024 * 1024
MAX_WORKER_OUTPUT_BYTES = 3 * 1024 * 1024
MAX_RASTER_PIXELS = 20_000_000


class SourcePreviewError(ValueError):
    def __init__(self, message, *, status=400):
        super().__init__(message)
        self.status = status


def _load_verified_document(path, review_id):
    source = Path(path).resolve()
    if not source.is_file():
        raise SourcePreviewError('研究数据库不存在', status=404)
    try:
        with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('PRAGMA trusted_schema=OFF')
            db.execute('BEGIN')
            row = db.execute('SELECT record_json,document FROM reviewed_sources WHERE id=?', (review_id,)).fetchone()
            if not row:
                raise SourcePreviewError('资料不存在', status=404)
            raw, document = row
            if (not isinstance(raw, bytes) or len(raw) > MAX_DOCUMENT_BYTES or hashlib.sha256(raw).hexdigest() != review_id
                    or not isinstance(document, bytes) or not 1 <= len(document) <= MAX_DOCUMENT_BYTES):
                raise SourcePreviewError('资料完整性核验失败', status=409)
            record = json.loads(raw)
            digest = record.get('document_sha256')
            if (not isinstance(digest, str) or not IDENTIFIER.fullmatch(digest)
                    or hashlib.sha256(document).hexdigest() != digest):
                raise SourcePreviewError('资料完整性核验失败', status=409)
            return digest, document
    except SourcePreviewError:
        raise
    except (sqlite3.Error, OSError, ValueError, TypeError, AttributeError, KeyError, RecursionError) as exc:
        raise SourcePreviewError('资料读取失败或内容无效', status=409) from exc


def _pdf_page(document, page):
    if page >= 200:
        raise SourcePreviewError('PDF 页码超出支持范围')
    request = json.dumps({'document_base64': base64.b64encode(document).decode('ascii'), 'page': page},
                         separators=(',', ':')).encode('utf-8')
    try:
        result = subprocess.run([sys.executable, '-m', 'dlt.source_pdf_worker'], input=request,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=PDF_TIMEOUT_SECONDS,
                                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        if len(result.stdout) > MAX_WORKER_OUTPUT_BYTES:
            raise SourcePreviewError('PDF 预览超出大小上限', status=422)
        payload = json.loads(result.stdout)
        if result.returncode:
            if payload.get('error') == 'dependency_missing':
                raise SourcePreviewError('缺少 PDF 预览依赖，请安装项目依赖', status=503)
            if payload.get('error') == 'resource_limit':
                raise SourcePreviewError('无法启用 PDF 预览资源限制', status=503)
            raise SourcePreviewError('PDF 无法解析、已加密或超出预览限制', status=415)
        if (not isinstance(payload, dict) or set(payload) != {'page', 'pages', 'width', 'height', 'png_base64'}
                or any(type(payload[k]) is not int for k in ('page', 'pages', 'width', 'height'))
                or payload['page'] != page or not 1 <= payload['pages'] <= 200 or page >= payload['pages']
                or not 1 <= payload['width'] <= 1400 or not 1 <= payload['height'] <= 1400
                or not isinstance(payload['png_base64'], str) or len(payload['png_base64']) > MAX_WORKER_OUTPUT_BYTES):
            raise SourcePreviewError('PDF 预览结果无效', status=422)
        png = base64.b64decode(payload['png_base64'], validate=True)
        if (not 24 <= len(png) <= MAX_PDF_IMAGE_BYTES or png[:8] != b'\x89PNG\r\n\x1a\n'
                or png[12:16] != b'IHDR'
                or struct.unpack('>II', png[16:24]) != (payload['width'], payload['height'])):
            raise SourcePreviewError('PDF 预览图像无效', status=422)
        return payload
    except SourcePreviewError:
        raise
    except subprocess.TimeoutExpired as exc:
        raise SourcePreviewError('PDF 预览超时，原文件仍保留在本机', status=408) from exc
    except (OSError, ValueError, TypeError, AttributeError, struct.error) as exc:
        raise SourcePreviewError('PDF 预览进程不可用或输出无效', status=503) from exc


def _raster_image(document):
    if len(document) > MAX_DOCUMENT_BYTES:
        raise SourcePreviewError('图像超过支持的大小', status=415)
    request = json.dumps({'document_base64': base64.b64encode(document).decode('ascii')},
                         separators=(',', ':')).encode('utf-8')
    try:
        result = subprocess.run([sys.executable, '-m', 'dlt.source_image_worker'], input=request,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=PDF_TIMEOUT_SECONDS,
                                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        if len(result.stdout) > MAX_WORKER_OUTPUT_BYTES:
            raise SourcePreviewError('图像预览超出大小上限', status=422)
        payload = json.loads(result.stdout)
        if result.returncode:
            if payload.get('error') == 'dependency_missing':
                raise SourcePreviewError('缺少图像预览依赖，请安装项目依赖', status=503)
            if payload.get('error') == 'resource_limit':
                raise SourcePreviewError('无法启用图像预览资源限制', status=503)
            raise SourcePreviewError('图像无法解析或超出预览限制', status=415)
        if (not isinstance(payload, dict) or set(payload) != {'width', 'height', 'png_base64'}
                or any(type(payload[k]) is not int or not 1 <= payload[k] <= 1400 for k in ('width', 'height'))
                or payload['width'] * payload['height'] > MAX_RASTER_PIXELS
                or not isinstance(payload['png_base64'], str) or len(payload['png_base64']) > MAX_WORKER_OUTPUT_BYTES):
            raise SourcePreviewError('图像预览结果无效', status=422)
        png = base64.b64decode(payload['png_base64'], validate=True)
        if (not 24 <= len(png) <= MAX_PDF_IMAGE_BYTES or png[:8] != b'\x89PNG\r\n\x1a\n'
                or png[12:16] != b'IHDR'
                or struct.unpack('>II', png[16:24]) != (payload['width'], payload['height'])):
            raise SourcePreviewError('图像预览无效', status=422)
        return payload
    except SourcePreviewError:
        raise
    except subprocess.TimeoutExpired as exc:
        raise SourcePreviewError('图像预览超时，原文件仍保留在本机', status=408) from exc
    except (OSError, ValueError, TypeError, AttributeError, struct.error) as exc:
        raise SourcePreviewError('图像预览进程不可用或输出无效', status=503) from exc


def preview_source(path, review_id, offset=0):
    """Read verified text or render one PDF page, without creating files."""
    if (not isinstance(review_id, str) or not IDENTIFIER.fullmatch(review_id)
            or type(offset) is not int or offset < 0):
        raise SourcePreviewError('资料标识或查看位置无效')
    digest, document = _load_verified_document(path, review_id)
    if b'%PDF-' in document[:1024]:
        page = _pdf_page(document, offset)
        return {'kind': 'pdf_page', 'review_id': review_id, 'document_sha256': digest, 'offset': offset,
                'total_pages': page['pages'], 'width': page['width'], 'height': page['height'],
                'image_base64': page['png_base64'], 'previous_offset': offset-1 if offset else None,
                'next_offset': offset+1 if offset+1 < page['pages'] else None,
                'assurance': 'raster view only; source origin is not independently authenticated'}
    if document.startswith((b'\x89PNG\r\n\x1a\n', b'\xff\xd8\xff')):
        if offset != 0:
            raise SourcePreviewError('图像只有一个预览页')
        page = _raster_image(document)
        return {'kind': 'raster_image', 'review_id': review_id, 'document_sha256': digest, 'offset': 0,
                'width': page['width'], 'height': page['height'], 'image_base64': page['png_base64'],
                'previous_offset': None, 'next_offset': None,
                'assurance': 'metadata-free raster view only; source origin is not independently authenticated'}
    if document.startswith((b'PK\x03\x04', b'\x1f\x8b')):
        raise SourcePreviewError('该格式暂不支持查看，原文件仍保留在本机', status=415)
    try:
        text = document.decode('utf-8-sig')
    except UnicodeDecodeError as exc:
        raise SourcePreviewError('仅支持 UTF-8 文本、PDF 和 PNG/JPEG 图像查看；原文件仍保留在本机', status=415) from exc
    if any((ord(ch) < 32 and ch not in '\n\r\t') or 0x7f <= ord(ch) < 0xa0 for ch in text):
        raise SourcePreviewError('文件包含不支持的控制字符，原文件仍保留在本机', status=415)
    if offset > len(text):
        raise SourcePreviewError('查看位置超出文本范围')
    end = min(len(text), offset + PAGE_CHARS)
    return {'kind': 'text', 'review_id': review_id, 'document_sha256': digest, 'text': text[offset:end],
            'offset': offset, 'total_chars': len(text), 'next_offset': end if end < len(text) else None,
            'previous_offset': max(0, offset-PAGE_CHARS) if offset else None,
            'assurance': 'text view only; source origin is not independently authenticated'}
