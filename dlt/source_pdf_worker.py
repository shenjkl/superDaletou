"""Render one bounded PDF page for local viewing; never return the PDF bytes."""
import base64
import io
import json
import math
import os
import sys


MAX_REQUEST_BYTES = 8 * 1024 * 1024
MAX_DOCUMENT_BYTES = 5 * 1024 * 1024
MAX_PAGES = 200
MAX_SIDE = 1400
MAX_PNG_BYTES = 2 * 1024 * 1024
MAX_MEMORY_BYTES = 768 * 1024 * 1024
_job_handle = None


def _limit_process():
    """Enforce a per-process memory limit before importing the parser."""
    global _job_handle
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        class IoCounters(ctypes.Structure):
            _fields_ = [(name, ctypes.c_uint64) for name in
                        ('read_ops', 'write_ops', 'other_ops', 'read_bytes', 'write_bytes', 'other_bytes')]
        class BasicLimits(ctypes.Structure):
            _fields_ = [('process_time', ctypes.c_int64), ('job_time', ctypes.c_int64),
                        ('flags', wintypes.DWORD), ('min_working_set', ctypes.c_size_t),
                        ('max_working_set', ctypes.c_size_t), ('active_processes', wintypes.DWORD),
                        ('affinity', ctypes.c_size_t), ('priority', wintypes.DWORD), ('scheduling', wintypes.DWORD)]
        class ExtendedLimits(ctypes.Structure):
            _fields_ = [('basic', BasicLimits), ('io', IoCounters), ('process_memory', ctypes.c_size_t),
                        ('job_memory', ctypes.c_size_t), ('peak_process_memory', ctypes.c_size_t),
                        ('peak_job_memory', ctypes.c_size_t)]
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel.CreateJobObjectW.restype = wintypes.HANDLE
        kernel.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        kernel.SetInformationJobObject.restype = wintypes.BOOL
        kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        job = kernel.CreateJobObjectW(None, None)
        limits = ExtendedLimits()
        limits.basic.flags = 0x100 | 0x2000  # process memory, terminate children when job closes
        limits.process_memory = MAX_MEMORY_BYTES
        if (not job or not kernel.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits))
                or not kernel.AssignProcessToJobObject(job, kernel.GetCurrentProcess())):
            raise RuntimeError('resource limit unavailable')
        _job_handle = job  # Keep the job open until this process exits.
    else:
        import resource
        resource.setrlimit(resource.RLIMIT_AS, (MAX_MEMORY_BYTES, MAX_MEMORY_BYTES))
        resource.setrlimit(resource.RLIMIT_CPU, (8, 8))


def _render(request):
    if not isinstance(request, dict) or set(request) != {'document_base64', 'page'}:
        raise ValueError('invalid request')
    page_number = request['page']
    if type(page_number) is not int or not 0 <= page_number < MAX_PAGES:
        raise ValueError('invalid page')
    document = base64.b64decode(request['document_base64'], validate=True)
    if not 1 <= len(document) <= MAX_DOCUMENT_BYTES or b'%PDF-' not in document[:1024]:
        raise ValueError('invalid document')
    import pypdfium2 as pdfium
    from PIL import Image
    pdf = pdfium.PdfDocument(document)
    try:
        count = len(pdf)
        if not 1 <= count <= MAX_PAGES or page_number >= count:
            raise ValueError('page count outside supported range')
        page = pdf[page_number]
        try:
            width, height = page.get_size()
            if not all(isinstance(x, (int, float)) and math.isfinite(x) and 0 < x <= 20000 for x in (width, height)):
                raise ValueError('page dimensions outside supported range')
            scale = min(1.5, (MAX_SIDE-2)/max(width, height))
            bitmap = page.render(scale=scale)
            try:
                image = bitmap.to_pil()
                if image.width < 1 or image.height < 1 or max(image.width, image.height) > MAX_SIDE:
                    raise ValueError('bitmap dimensions outside supported range')
                clean = Image.new('RGB', image.size, 'white')
                foreground = image.convert('RGBA')
                clean.paste(foreground, mask=foreground.getchannel('A'))
                out = io.BytesIO(); clean.save(out, format='PNG')
                data = out.getvalue()
                if len(data) > MAX_PNG_BYTES:
                    raise ValueError('image exceeds supported size')
                return {'page': page_number, 'pages': count, 'width': clean.width, 'height': clean.height,
                        'png_base64': base64.b64encode(data).decode('ascii')}
            finally:
                bitmap.close()
        finally:
            page.close()
    finally:
        pdf.close()


def main():
    try:
        _limit_process()
    except Exception:
        sys.stdout.write('{"error":"resource_limit"}'); return 1
    try:
        raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES+1)
        if len(raw) > MAX_REQUEST_BYTES:
            raise ValueError('request too large')
        result = _render(json.loads(raw))
        sys.stdout.write(json.dumps(result, separators=(',', ':')))
        return 0
    except ImportError:
        sys.stdout.write('{"error":"dependency_missing"}'); return 1
    except Exception:
        # Never expose source bytes, paths, parser messages or file metadata.
        sys.stdout.write('{"error":"invalid_or_unsupported"}'); return 1


if __name__ == '__main__':
    raise SystemExit(main())
