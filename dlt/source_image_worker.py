"""Create a bounded, metadata-free PNG preview from a stored raster image."""
import base64
import io
import json
import sys


MAX_REQUEST_BYTES = 8 * 1024 * 1024
MAX_DOCUMENT_BYTES = 5 * 1024 * 1024
MAX_SIDE = 1400
MAX_PIXELS = 20_000_000
MAX_PNG_BYTES = 2 * 1024 * 1024
MAX_WORKER_OUTPUT_BYTES = 3 * 1024 * 1024


def _render(request):
    if not isinstance(request, dict) or set(request) != {'document_base64'}:
        raise ValueError('invalid request')
    document = base64.b64decode(request['document_base64'], validate=True)
    if not 1 <= len(document) <= MAX_DOCUMENT_BYTES:
        raise ValueError('invalid document size')

    from PIL import Image, ImageOps

    Image.MAX_IMAGE_PIXELS = MAX_PIXELS
    with Image.open(io.BytesIO(document)) as probe:
        if probe.format not in ('PNG', 'JPEG') or getattr(probe, 'n_frames', 1) != 1:
            raise ValueError('unsupported image format')
        width, height = probe.size
        if (not isinstance(width, int) or not isinstance(height, int) or width < 1 or height < 1
                or width * height > MAX_PIXELS):
            raise ValueError('image dimensions outside supported range')
        probe.verify()

    with Image.open(io.BytesIO(document)) as source:
        oriented = ImageOps.exif_transpose(source)
        oriented.load()
        width, height = oriented.size
        if width * height > MAX_PIXELS:
            raise ValueError('image dimensions outside supported range')
        oriented.thumbnail((MAX_SIDE, MAX_SIDE), Image.Resampling.LANCZOS)
        mode = 'RGBA' if 'A' in oriented.getbands() else 'RGB'
        clean = oriented.convert(mode)
        out = io.BytesIO()
        clean.save(out, format='PNG', optimize=True)
        png = out.getvalue()
        if not 24 <= len(png) <= MAX_PNG_BYTES:
            raise ValueError('preview outside supported size')
        return {'width': clean.width, 'height': clean.height,
                'png_base64': base64.b64encode(png).decode('ascii')}


def main():
    try:
        from dlt.source_pdf_worker import _limit_process
        _limit_process()
    except Exception:
        sys.stdout.write('{"error":"resource_limit"}')
        return 1
    try:
        raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            raise ValueError('request too large')
        result = _render(json.loads(raw))
        sys.stdout.write(json.dumps(result, separators=(',', ':')))
        return 0
    except ImportError:
        sys.stdout.write('{"error":"dependency_missing"}')
        return 1
    except Exception:
        # Do not expose source bytes, metadata, paths, or decoder diagnostics.
        sys.stdout.write('{"error":"invalid_or_unsupported"}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
