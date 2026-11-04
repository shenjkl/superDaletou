"""Prepare a checked SQLite copy at a new path without touching the source.

This is an isolation step, not certification that all business contracts,
source provenance, secret handling, or a full restore have been validated.
"""
from contextlib import closing
from datetime import datetime, timezone
import argparse
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import ctypes
import tempfile

from dlt.integrity import check_database
from dlt.recovery_guard import (RecoveryQuarantined, require_execution_allowed,
                               require_path_execution_allowed)


MAX_DATABASE_BYTES = 512 * 1024 * 1024
RECOVERY_WRITE_OPERATIONS = {'recovery_prepare', 'recovery_activate'}


def _validate_write_receipt(receipt):
    if receipt is None:
        return None
    if not isinstance(receipt, dict) or set(receipt) != {'request_key', 'operation', 'request_sha256'}:
        raise RecoveryError('恢复写入回执参数无效')
    from dlt.source_write_protocol import _request_key
    _request_key(receipt['request_key'])
    if (receipt['operation'] not in RECOVERY_WRITE_OPERATIONS
            or not isinstance(receipt['request_sha256'], str)
            or not re.fullmatch(r'[0-9a-f]{64}', receipt['request_sha256'])):
        raise RecoveryError('恢复写入回执参数无效')
    return receipt


def _record_write_receipt(database, receipt):
    receipt = _validate_write_receipt(receipt)
    if receipt is None:
        return
    with closing(sqlite3.connect(database)) as db, db:
        db.executescript('''
            CREATE TABLE IF NOT EXISTS recovery_write_receipts (
                request_key TEXT PRIMARY KEY,
                operation TEXT NOT NULL CHECK(operation IN ('recovery_prepare','recovery_activate')),
                request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
                created_at TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS recovery_write_receipts_no_update BEFORE UPDATE ON recovery_write_receipts
                BEGIN SELECT RAISE(ABORT, 'immutable recovery write receipt'); END;
            CREATE TRIGGER IF NOT EXISTS recovery_write_receipts_no_delete BEFORE DELETE ON recovery_write_receipts
                BEGIN SELECT RAISE(ABORT, 'immutable recovery write receipt'); END;
        ''')
        row = (receipt['request_key'], receipt['operation'], receipt['request_sha256'],
               datetime.now(timezone.utc).isoformat())
        prior = db.execute('SELECT request_key,operation,request_sha256,created_at '
                           'FROM recovery_write_receipts WHERE request_key=?', (row[0],)).fetchone()
        if prior:
            if prior[:3] != row[:3]:
                raise RecoveryError('恢复副本中的请求回执与当前操作不符')
            return
        db.execute('INSERT INTO recovery_write_receipts VALUES (?,?,?,?)', row)


def _has_write_receipt(database, receipt):
    receipt = _validate_write_receipt(receipt)
    if receipt is None:
        return False
    try:
        uri = Path(database).resolve().as_uri() + '?mode=ro'
        with closing(sqlite3.connect(uri, uri=True, timeout=3)) as db:
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_write_receipts'").fetchone()
            if not exists:
                return False
            row = db.execute('SELECT operation,request_sha256 FROM recovery_write_receipts WHERE request_key=?',
                             (receipt['request_key'],)).fetchone()
            return row == (receipt['operation'], receipt['request_sha256'])
    except (OSError, sqlite3.Error, ValueError):
        return False


def _windows_security_apis():
    advapi = ctypes.WinDLL('advapi32', use_last_error=True)
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    handle = ctypes.c_void_p
    dword = ctypes.c_ulong
    boolean = ctypes.c_int
    advapi.OpenProcessToken.argtypes = [handle, dword, ctypes.POINTER(handle)]
    advapi.OpenProcessToken.restype = boolean
    advapi.GetTokenInformation.argtypes = [handle, dword, ctypes.c_void_p, dword,
                                           ctypes.POINTER(dword)]
    advapi.GetTokenInformation.restype = boolean
    advapi.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_wchar_p)]
    advapi.ConvertSidToStringSidW.restype = boolean
    advapi.GetNamedSecurityInfoW.argtypes = [ctypes.c_wchar_p, dword, dword,
                                             ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_void_p),
                                             ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_void_p),
                                             ctypes.POINTER(ctypes.c_void_p)]
    advapi.GetNamedSecurityInfoW.restype = dword
    advapi.GetAclInformation.argtypes = [ctypes.c_void_p, ctypes.c_void_p, dword, dword]
    advapi.GetAclInformation.restype = boolean
    advapi.GetAce.argtypes = [ctypes.c_void_p, dword, ctypes.POINTER(ctypes.c_void_p)]
    advapi.GetAce.restype = boolean
    advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [
        ctypes.c_wchar_p, dword, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(dword)]
    advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW.restype = boolean
    advapi.GetSecurityDescriptorDacl.argtypes = [ctypes.c_void_p, ctypes.POINTER(boolean),
                                                  ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(boolean)]
    advapi.GetSecurityDescriptorDacl.restype = boolean
    advapi.SetNamedSecurityInfoW.argtypes = [ctypes.c_wchar_p, dword, dword, ctypes.c_void_p,
                                             ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
    advapi.SetNamedSecurityInfoW.restype = dword
    kernel.GetCurrentProcess.restype = handle
    kernel.CloseHandle.argtypes = [handle]
    kernel.CloseHandle.restype = boolean
    kernel.LocalFree.argtypes = [handle]
    kernel.LocalFree.restype = handle
    return advapi, kernel


def _require_private_acl(path):
    """Inspect Windows allow ACEs without printing the path or ACL contents."""
    advapi, kernel = _windows_security_apis()

    class TokenUser(ctypes.Structure):
        _fields_ = [('sid', ctypes.c_void_p), ('attributes', ctypes.c_ulong)]

    class AclSizeInformation(ctypes.Structure):
        _fields_ = [('ace_count', ctypes.c_ulong), ('bytes_used', ctypes.c_ulong),
                    ('bytes_free', ctypes.c_ulong)]

    token = ctypes.c_void_p()
    if not advapi.OpenProcessToken(kernel.GetCurrentProcess(), 0x0008, ctypes.byref(token)):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        required = ctypes.c_ulong()
        advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(required))
        if not required.value:
            raise ctypes.WinError(ctypes.get_last_error())
        token_data = ctypes.create_string_buffer(required.value)
        if not advapi.GetTokenInformation(token, 1, token_data, required.value, ctypes.byref(required)):
            raise ctypes.WinError(ctypes.get_last_error())
        current_sid_ptr = ctypes.cast(token_data, ctypes.POINTER(TokenUser)).contents.sid
        current_sid_buf = ctypes.c_wchar_p()
        if not advapi.ConvertSidToStringSidW(current_sid_ptr, ctypes.byref(current_sid_buf)):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            allowed_sids = {current_sid_buf.value, 'S-1-5-18', 'S-1-5-32-544'}
        finally:
            kernel.LocalFree(ctypes.cast(current_sid_buf, ctypes.c_void_p))
    finally:
        kernel.CloseHandle(token)

    owner = ctypes.c_void_p()
    group = ctypes.c_void_p()
    dacl = ctypes.c_void_p()
    sacl = ctypes.c_void_p()
    security_descriptor = ctypes.c_void_p()
    result = advapi.GetNamedSecurityInfoW(str(path), 1, 0x00000004,
                                          ctypes.byref(owner), ctypes.byref(group),
                                          ctypes.byref(dacl), ctypes.byref(sacl),
                                          ctypes.byref(security_descriptor))
    if result != 0:
        raise OSError(result, 'GetNamedSecurityInfoW failed')
    try:
        if not dacl.value:
            raise RecoveryError('明文资料路径 ACL 不安全或无法核验')
        info = AclSizeInformation()
        if not advapi.GetAclInformation(dacl, ctypes.byref(info), ctypes.sizeof(info), 2):
            raise ctypes.WinError(ctypes.get_last_error())
        sensitive = 0x000F01FF  # FILE_ALL_ACCESS without SYNCHRONIZE.
        allowed_ace_types = {0, 5, 9, 11}  # Allow, object, callback and callback-object ACEs.
        for index in range(info.ace_count):
            ace = ctypes.c_void_p()
            if not advapi.GetAce(dacl, index, ctypes.byref(ace)):
                raise ctypes.WinError(ctypes.get_last_error())
            raw = ctypes.cast(ace, ctypes.POINTER(ctypes.c_ubyte))
            ace_type = raw[0]
            if ace_type not in allowed_ace_types:
                continue
            mask = int.from_bytes(ctypes.string_at(ace.value + 4, 4), 'little')
            if mask & 0x10000000:  # GENERIC_ALL
                mask |= 0x001F01FF
            if mask & 0x80000000:  # GENERIC_READ
                mask |= 0x00120089
            if mask & 0x40000000:  # GENERIC_WRITE
                mask |= 0x00120116
            if mask & 0x20000000:  # GENERIC_EXECUTE
                mask |= 0x001200A0
            if not mask & sensitive:
                continue
            sid_offset = 8
            if ace_type in (5, 11):
                object_flags = int.from_bytes(ctypes.string_at(ace.value + 8, 4), 'little')
                sid_offset += 4 + (16 if object_flags & 1 else 0) + (16 if object_flags & 2 else 0)
            sid_ptr = ctypes.c_void_p(ace.value + sid_offset)
            sid_text = ctypes.c_wchar_p()
            if not advapi.ConvertSidToStringSidW(sid_ptr, ctypes.byref(sid_text)):
                raise ctypes.WinError(ctypes.get_last_error())
            try:
                if sid_text.value not in allowed_sids:
                    raise RecoveryError('明文资料路径 ACL 不安全或无法核验')
            finally:
                kernel.LocalFree(ctypes.cast(sid_text, ctypes.c_void_p))
    finally:
        kernel.LocalFree(security_descriptor)


def _is_windows_reparse_point(path):
    if os.name != 'nt':
        return False
    info = Path(path).lstat()
    reparse_attribute = getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0x400)
    return bool(getattr(info, 'st_file_attributes', 0) & reparse_attribute)


def _path_entry_exists(path):
    try:
        Path(path).lstat()
        return True
    except FileNotFoundError:
        return False


def require_private_directory(directory):
    """Fail closed unless only the current user and OS administrators can access data files."""
    try:
        path = Path(directory)
        if path.is_symlink() or _is_windows_reparse_point(path) or not path.is_dir():
            raise RecoveryError('明文资料目录必须是实际存在的受保护目录')
        if os.name == 'nt':
            _require_private_acl(path)
        else:
            mode = stat.S_IMODE(path.stat().st_mode)
            if mode & 0o077:
                raise RecoveryError('明文资料目录须仅允许当前用户访问 (POSIX mode 0700)')
    except RecoveryError:
        raise
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        raise RecoveryError('明文资料目录访问权限无法核验；未继续恢复') from exc


def protect_private_directory(directory):
    """Restrict a newly created data directory to its owner and local administrators."""
    try:
        path = Path(directory)
        if path.is_symlink() or _is_windows_reparse_point(path) or not path.is_dir():
            raise RecoveryError('新建明文资料目录无法保护')
        if os.name == 'nt':
            advapi, kernel = _windows_security_apis()
            token = ctypes.c_void_p()
            if not advapi.OpenProcessToken(kernel.GetCurrentProcess(), 0x0008, ctypes.byref(token)):
                raise ctypes.WinError(ctypes.get_last_error())
            try:
                required = ctypes.c_ulong()
                advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(required))
                token_data = ctypes.create_string_buffer(required.value)
                if not required.value or not advapi.GetTokenInformation(
                        token, 1, token_data, required.value, ctypes.byref(required)):
                    raise ctypes.WinError(ctypes.get_last_error())
                current_sid = ctypes.cast(token_data, ctypes.POINTER(ctypes.c_void_p)).contents.value
                sid_text = ctypes.c_wchar_p()
                if not advapi.ConvertSidToStringSidW(current_sid, ctypes.byref(sid_text)):
                    raise ctypes.WinError(ctypes.get_last_error())
                try:
                    descriptor = ctypes.c_void_p()
                    descriptor_size = ctypes.c_ulong()
                    sddl = f'D:P(A;OICI;FA;;;{sid_text.value})(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)'
                    if not advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(
                            sddl, 1, ctypes.byref(descriptor), ctypes.byref(descriptor_size)):
                        raise ctypes.WinError(ctypes.get_last_error())
                    try:
                        present = ctypes.c_int()
                        defaulted = ctypes.c_int()
                        dacl = ctypes.c_void_p()
                        if not advapi.GetSecurityDescriptorDacl(descriptor, ctypes.byref(present),
                                                                ctypes.byref(dacl), ctypes.byref(defaulted)) or not present.value:
                            raise ctypes.WinError(ctypes.get_last_error())
                        result = advapi.SetNamedSecurityInfoW(str(path), 1, 0x80000004,
                                                              None, None, dacl, None)
                        if result != 0:
                            raise OSError(result, 'SetNamedSecurityInfoW failed')
                    finally:
                        kernel.LocalFree(descriptor)
                finally:
                    kernel.LocalFree(ctypes.cast(sid_text, ctypes.c_void_p))
            finally:
                kernel.CloseHandle(token)
        else:
            os.chmod(path, 0o700)
        require_private_directory(path)
    except RecoveryError:
        raise
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        raise RecoveryError('新建明文资料目录 ACL 设置失败') from exc


def require_private_file(file_path):
    """Require a plaintext candidate file with no group/world access."""
    try:
        path = Path(file_path)
        if path.is_symlink() or _is_windows_reparse_point(path) or not path.is_file():
            raise RecoveryError('明文候选必须是实际文件')
        if os.name == 'nt':
            _require_private_acl(path)
        elif stat.S_IMODE(path.stat().st_mode) & 0o077:
            raise RecoveryError('明文候选须仅允许当前用户访问 (POSIX mode 0600)')
    except RecoveryError:
        raise
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        raise RecoveryError('明文候选文件访问权限无法核验；未继续恢复') from exc


def reject_database_reparse_path(database):
    """Reject symlinks and Windows reparse points anywhere in the DB path."""
    try:
        current = Path(os.path.abspath(os.fspath(database)))
        reparse_attribute = getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0x400)
        while True:
            try:
                info = current.lstat()
            except FileNotFoundError:
                info = None
            if info is not None:
                if stat.S_ISLNK(info.st_mode):
                    raise RecoveryError('运行数据库路径不能包含符号链接')
                attributes = getattr(info, 'st_file_attributes', 0)
                if os.name == 'nt' and attributes & reparse_attribute:
                    raise RecoveryError('运行数据库路径不能包含 Windows reparse point')
            if current.parent == current:
                break
            current = current.parent
    except RecoveryError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise RecoveryError('运行数据库路径无法安全核验') from exc


def require_private_database_files(database):
    """Check the directory, SQLite database, and any existing sidecars."""
    path = Path(database)
    require_private_directory(path.parent)
    if path.exists():
        require_private_file(path)
    for suffix in ('-wal', '-shm', '-journal'):
        sidecar = Path(str(path) + suffix)
        if _path_entry_exists(sidecar):
            require_private_file(sidecar)


def require_runtime_database_execution_allowed(database):
    """Translate recovery quarantine failures into the runtime path error contract."""
    try:
        require_path_execution_allowed(database)
    except RecoveryQuarantined as exc:
        raise RecoveryError('该数据库处于恢复隔离状态，禁止启动运行服务') from exc
    except (OSError, sqlite3.Error, ValueError, TypeError) as exc:
        raise RecoveryError('运行数据库隔离状态无法核验；未启动服务或 worker') from exc


def prepare_runtime_database_path(database):
    """Protect the database directory and reject exposed existing database files."""
    supplied = Path(database)
    reject_database_reparse_path(supplied)
    path = supplied.resolve()
    try:
        parent_created = False
        try:
            path.parent.mkdir(parents=True, exist_ok=False)
            parent_created = True
        except FileExistsError:
            pass
        if parent_created:
            protect_private_directory(path.parent)
        require_private_database_files(path)
        # The quarantine probe opens SQLite read-only; do not parse an existing
        # database until its directory, file, and sidecars have passed ACL checks.
        require_runtime_database_execution_allowed(path)
    except RecoveryError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise RecoveryError('运行数据库路径无法保护或核验') from exc
    return path, path.exists()


def verify_runtime_database_permissions(database, *, existed_before_start):
    """Recheck runtime database access after schema initialization, before use."""
    path = Path(database)
    try:
        reject_database_reparse_path(path)
        if not existed_before_start and os.name != 'nt':
            os.chmod(path, 0o600, follow_symlinks=False)
        require_private_database_files(path)
    except RecoveryError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise RecoveryError('运行数据库初始化后的访问权限复核失败') from exc


class RecoveryError(ValueError):
    def __init__(self, message, *, check=None):
        super().__init__(message)
        self.check = check


def _publish_staged(staged, destination):
    """Atomically publish within one directory without replacing a target.

    On Windows os.rename fails if destination exists. On POSIX it can replace
    the destination, so use a hard link there instead. Both paths fail closed
    when the platform or filesystem does not support the operation.
    """
    if os.name == 'nt':
        os.rename(staged, destination)
    else:
        os.link(staged, destination)


def _flush_staged_file(staged):
    """Flush a completed SQLite staging file before publishing its name."""
    with open(staged, 'r+b') as stream:
        stream.flush()
        os.fsync(stream.fileno())


def verify_isolated_candidate(candidate, *, write_receipt=None):
    """Recheck a published candidate without changing its quarantine state."""
    try:
        supplied = Path(candidate)
        reject_database_reparse_path(supplied)
        path = supplied.resolve()
        if not path.is_file():
            raise RecoveryError('隔离候选数据库不存在')
        require_private_database_files(path)
        sidecars = [Path(str(path) + suffix) for suffix in ('-wal', '-shm', '-journal')]
        if any(_path_entry_exists(item) for item in sidecars):
            raise RecoveryError('隔离候选存在 SQLite 日志文件；先停止访问并检查目录')
        report = check_database(path)
        if report['status'] != 'passed' or report['counts'].get('recovery_quarantine') != 1:
            raise RecoveryError('隔离候选复核未通过；候选仍未改变', check=report)
        if write_receipt is not None and not _has_write_receipt(path, write_receipt):
            raise RecoveryError('隔离候选中没有匹配的恢复请求回执；候选未改变', check=report)
        if any(_path_entry_exists(item) for item in sidecars):
            raise RecoveryError('检查期间候选出现 SQLite 日志文件；候选仍未改变', check=report)
        return {'status': 'quarantined_candidate_verified', 'check': report,
                'limitations': ['candidate remains quarantined and cannot execute plans',
                                'selected integrity checks do not certify complete restore or runtime readiness',
                                'the candidate is plaintext and may contain sensitive material']}
    except RecoveryError:
        raise
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise RecoveryError('隔离候选复核失败；候选未改变') from exc


def verify_restored_copy(candidate, *, write_receipt=None):
    """Read-only check that a protected new copy is no longer quarantined."""
    try:
        supplied = Path(candidate)
        reject_database_reparse_path(supplied)
        path = supplied.resolve()
        if not path.is_file():
            raise RecoveryError('恢复副本数据库不存在')
        require_private_database_files(path)
        sidecars = [Path(str(path) + suffix) for suffix in ('-wal', '-shm', '-journal')]
        if any(_path_entry_exists(item) for item in sidecars):
            raise RecoveryError('恢复副本存在 SQLite 日志文件；先停止访问并检查目录')
        report = check_database(path)
        if report['status'] != 'passed' or report['counts'].get('recovery_quarantine') is not None:
            raise RecoveryError('恢复副本复核未通过；未更改该文件', check=report)
        if write_receipt is not None and not _has_write_receipt(path, write_receipt):
            raise RecoveryError('恢复副本中没有匹配的恢复请求回执；未更改该文件', check=report)
        if any(_path_entry_exists(item) for item in sidecars):
            raise RecoveryError('检查期间恢复副本出现 SQLite 日志文件；未更改该文件', check=report)
        return {'status': 'restored_copy_verified', 'check': report,
                'limitations': ['selected integrity checks only; not full business or runtime certification',
                                'verification does not prove which operation created the copy']}
    except RecoveryError:
        raise
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise RecoveryError('恢复副本复核失败；文件未改变') from exc


def activate_isolated_candidate(candidate, destination, *, acknowledge_plaintext=False,
                                enable_restored_plans=False, write_receipt=None):
    """Publish a runnable copy to a new path, leaving candidate and old DB intact.

    The destination must not exist. Clearing the quarantine marker is allowed
    only with explicit acknowledgement that restored plans may run.
    """
    if acknowledge_plaintext is not True or enable_restored_plans is not True:
        raise RecoveryError('恢复副本含明文资料且可能执行原计划；必须分别明确确认')
    write_receipt = _validate_write_receipt(write_receipt)
    try:
        source = Path(candidate)
        target = Path(destination).absolute()
        reject_database_reparse_path(source)
        reject_database_reparse_path(target)
        source = source.resolve()
        if not source.is_file() or not target.parent.is_dir():
            raise RecoveryError('隔离候选或目标目录不存在')
        if source == target.resolve() or _target_occupied(target):
            raise RecoveryError('恢复目标必须是新的数据库路径；原候选与已有目标均未修改')
        source_stat = source.stat()
        if source_stat.st_size > MAX_DATABASE_BYTES:
            raise RecoveryError('隔离候选超过恢复副本大小上限')
        require_private_directory(target.parent)
        verify_isolated_candidate(source)
    except RecoveryError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise RecoveryError('恢复候选或目标路径检查失败；原文件未修改') from exc
    staged = None
    published = False
    result = None
    try:
        for suffix in ('-wal', '-shm', '-journal'):
            sidecar = Path(str(source) + suffix)
            if _path_entry_exists(sidecar):
                raise RecoveryError('隔离候选出现 SQLite 日志文件；未生成恢复副本')
        fd, name = tempfile.mkstemp(prefix='.dlt-activate-', suffix='.sqlite3', dir=target.parent)
        os.close(fd)
        staged = Path(name)
        with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=5)) as src:
            src.execute('PRAGMA query_only=ON')
            src.execute('PRAGMA trusted_schema=OFF')
            src.execute('BEGIN')
            if not src.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_quarantine'").fetchone():
                raise RecoveryError('隔离标记缺失；未生成恢复副本')
            with closing(sqlite3.connect(staged)) as dst:
                src.backup(dst)
        _record_write_receipt(staged, write_receipt)
        current_stat = source.stat()
        if (current_stat.st_size, current_stat.st_mtime_ns) != (source_stat.st_size, source_stat.st_mtime_ns):
            raise RecoveryError('隔离候选在复制期间发生变化；未发布恢复副本')
        verify_isolated_candidate(source)
        current_stat = source.stat()
        if (current_stat.st_size, current_stat.st_mtime_ns) != (source_stat.st_size, source_stat.st_mtime_ns):
            raise RecoveryError('隔离候选复核期间发生变化；未发布恢复副本')
        report = check_database(staged)
        if report['status'] != 'passed' or report['counts'].get('recovery_quarantine') != 1:
            raise RecoveryError('待恢复副本的隔离完整性检查未通过；原候选和原库未改变', check=report)
        require_private_directory(target.parent)
        require_private_file(staged)
        _flush_staged_file(staged)
        if _target_occupied(target):
            raise RecoveryError('恢复目标已被其他文件占用；未覆盖目标')
        _publish_staged(staged, target)
        published = True
        try:
            require_private_file(target)
        except RecoveryError as exc:
            raise RecoveryError('已发布的恢复副本 ACL 未通过；副本仍处于隔离状态，禁止启动') from exc
        # Remove the extra POSIX hard link before activation. If this fails,
        # both names still point to the quarantined copy and neither can run.
        staged.unlink(missing_ok=True)
        staged = None
        require_private_file(target)
        with closing(sqlite3.connect(target)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA trusted_schema=OFF')
            marker = db.execute('SELECT id,reason FROM recovery_quarantine').fetchall()
            if marker != [(1, 'isolated recovery candidate')]:
                raise RecoveryError('已发布副本的隔离标记无效；副本仍不可执行')
            db.execute('DROP TRIGGER recovery_quarantine_no_update')
            db.execute('DROP TRIGGER recovery_quarantine_no_delete')
            db.execute('DROP TABLE recovery_quarantine')
        report = check_database(target)
        if report['status'] != 'passed' or report['counts'].get('recovery_quarantine') is not None:
            try:
                _add_quarantine_marker(target)
            except (OSError, sqlite3.Error, ValueError) as exc:
                raise RecoveryError('恢复副本检查失败，且无法确认隔离标记已恢复；禁止启动该目标，请保留原库和候选并人工处置',
                                    check=report) from exc
            raise RecoveryError('解除隔离后的恢复副本检查未通过；副本已重新隔离', check=report)
        try:
            require_private_file(target)
        except RecoveryError as exc:
            try:
                _add_quarantine_marker(target)
            except (OSError, sqlite3.Error, ValueError) as restore_exc:
                raise RecoveryError('恢复副本 ACL 检查失败，且无法确认隔离标记已恢复；禁止启动该目标，请保留原库和候选并人工处置',
                                    check=report) from restore_exc
            raise RecoveryError('恢复副本 ACL 检查失败；副本已重新隔离，禁止启动', check=report) from exc
        result = {'status': 'restored_copy_ready', 'check': report, 'plans_enabled': True,
                  'staging_cleanup': 'complete',
                  'limitations': ['the original database and isolated candidate were not modified',
                                  'restored plans retain their saved enabled state and may run when this copy is started',
                                  'runtime startup, access controls, and platform-specific file permissions still require verification']}
        return result
    except RecoveryError:
        raise
    except (OSError, sqlite3.Error, ValueError) as exc:
        if published:
            raise RecoveryError('恢复目标已发布但未完成启用；请保留该隔离副本并核验，原数据库和候选未修改') from exc
        raise RecoveryError('恢复副本准备失败；原数据库和隔离候选未修改') from exc
    finally:
        if staged is not None:
            try:
                staged.unlink(missing_ok=True)
            except OSError as exc:
                if published:
                    if result is None:
                        raise RecoveryError('恢复目标已发布，但临时明文副本未能清除；恢复目标仍须人工核验，请检查受保护目录') from exc
                    result['staging_cleanup'] = 'failed'
                else:
                    raise RecoveryError('恢复副本未发布，但明文临时文件未能清除，请检查目标目录') from exc


def _target_occupied(target):
    return any(_path_entry_exists(path) for path in
               (target, *(Path(str(target) + suffix) for suffix in ('-wal', '-shm', '-journal'))))


def _add_quarantine_marker(path):
    """Fail closed by restoring the execution marker after a failed final check."""
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript('''
            CREATE TABLE recovery_quarantine (
                id INTEGER PRIMARY KEY CHECK(id=1), created_at TEXT NOT NULL,
                reason TEXT NOT NULL CHECK(reason='isolated recovery candidate')
            );
            CREATE TRIGGER recovery_quarantine_no_update BEFORE UPDATE ON recovery_quarantine
                BEGIN SELECT RAISE(ABORT, 'immutable recovery quarantine'); END;
            CREATE TRIGGER recovery_quarantine_no_delete BEFORE DELETE ON recovery_quarantine
                BEGIN SELECT RAISE(ABORT, 'immutable recovery quarantine'); END;
        ''')
        db.execute('INSERT INTO recovery_quarantine VALUES (1,?,?)',
                   (datetime.now(timezone.utc).isoformat(), 'isolated recovery candidate'))


def prepare_isolated_copy(source, destination):
    """Copy a consistent source snapshot to a new path after partial checks.

    Publication uses an atomic, no-replace operation in the destination
    directory. A failed check or occupied target leaves existing files intact.
    The copy retains SQLite research data and adds a quarantine marker; it can
    include sensitive user content and is not a sanitized export.
    """
    reject_database_reparse_path(source)
    destination = Path(destination).absolute()
    reject_database_reparse_path(destination)
    source = Path(source).resolve()
    if not source.is_file() or not destination.parent.is_dir():
        raise RecoveryError('源数据库或目标目录不存在')
    def target_occupied():
        return any(_path_entry_exists(p) for p in
                   (destination, *(Path(str(destination) + suffix) for suffix in ('-wal', '-shm', '-journal'))))

    if target_occupied():
        raise RecoveryError('目标或其数据库日志已存在，未覆盖任何文件')
    if destination.resolve() == source:
        raise RecoveryError('不能覆盖源数据库')
    require_private_directory(destination.parent)
    require_private_database_files(source)

    staged = None
    result = None
    try:
        fd, name = tempfile.mkstemp(prefix='.dlt-recovery-', suffix='.sqlite3', dir=destination.parent)
        os.close(fd)
        staged = Path(name)
        with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=5)) as src:
            src.execute('PRAGMA query_only=ON')
            src.execute('PRAGMA trusted_schema=OFF')
            src.execute('BEGIN')
            require_execution_allowed(src)
            # Establish the same source snapshot before the size and copy.
            src.execute('SELECT count(*) FROM sqlite_master').fetchone()
            page_count = src.execute('PRAGMA page_count').fetchone()[0]
            page_size = src.execute('PRAGMA page_size').fetchone()[0]
            if page_count * page_size > MAX_DATABASE_BYTES:
                raise RecoveryError('源数据库超过隔离检查大小上限')
            with closing(sqlite3.connect(staged)) as dst:
                src.backup(dst)
        report = check_database(staged)
        if report['status'] != 'passed':
            raise RecoveryError('数据库检查失败或不完整，未创建隔离副本', check=report)
        # Preserve original plan states for audit, but prevent this copy from
        # being started by the application or automatic workers. The source is
        # never modified; activation is a separate explicit operation that
        # publishes a new runnable copy after rechecking this candidate.
        with closing(sqlite3.connect(staged)) as db, db:
            db.executescript('''
                CREATE TABLE recovery_quarantine (
                    id INTEGER PRIMARY KEY CHECK(id=1), created_at TEXT NOT NULL,
                    reason TEXT NOT NULL CHECK(reason='isolated recovery candidate')
                );
                CREATE TRIGGER recovery_quarantine_no_update BEFORE UPDATE ON recovery_quarantine
                    BEGIN SELECT RAISE(ABORT, 'immutable recovery quarantine'); END;
                CREATE TRIGGER recovery_quarantine_no_delete BEFORE DELETE ON recovery_quarantine
                    BEGIN SELECT RAISE(ABORT, 'immutable recovery quarantine'); END;
            ''')
            db.execute('INSERT INTO recovery_quarantine VALUES (1,?,?)',
                       (datetime.now(timezone.utc).isoformat(), 'isolated recovery candidate'))
        report = check_database(staged)
        if report['status'] != 'passed':
            raise RecoveryError('隔离标记或数据库检查失败，未创建隔离副本', check=report)
        require_private_directory(destination.parent)
        require_private_file(staged)
        _flush_staged_file(staged)
        if target_occupied():
            raise RecoveryError('目标或其数据库日志已存在，未覆盖任何文件')
        try:
            _publish_staged(staged, destination)
        except FileExistsError as exc:
            raise RecoveryError('目标已存在，未覆盖任何文件') from exc
        try:
            require_private_file(destination)
        except RecoveryError as exc:
            raise RecoveryError('隔离候选已发布但 ACL 未通过；候选仍处于隔离状态，禁止启动') from exc
        result = {'status': 'prepared', 'check': report, 'staging_cleanup': 'complete',
                  'limitations': ['only selected structural and reference checks have passed',
                                  'not a sanitized export or full restore certification',
                                  'no schedules or tasks were run by this operation',
                                  'candidate is quarantined and cannot start application or automatic plans']}
        return result
    except RecoveryError:
        raise
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise RecoveryError('隔离副本创建失败，源数据库与已有目标未被覆盖') from exc
    finally:
        if staged is not None:
            try:
                if staged.exists() or staged.is_symlink():
                    staged.unlink()
            except OSError as exc:
                if result is not None:
                    result['staging_cleanup'] = 'failed'
                else:
                    raise RecoveryError('隔离副本未发布，但临时文件未能清除，请检查目标目录') from exc


def main():
    parser = argparse.ArgumentParser(description='Prepare an isolated research database copy; never overwrite a target.')
    parser.add_argument('source')
    parser.add_argument('destination')
    args = parser.parse_args()
    try:
        result = prepare_isolated_copy(args.source, args.destination)
    except RecoveryError as exc:
        error = {'status': 'failed', 'error': str(exc)}
        if exc.check is not None:
            error['check'] = exc.check
        print(json.dumps(error, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result['staging_cleanup'] == 'complete' else 3


if __name__ == '__main__':
    raise SystemExit(main())
