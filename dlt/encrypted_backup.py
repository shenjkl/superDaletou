"""Password-protected SQLite snapshots and isolated, checked candidates.

The encrypted snapshot is not a sanitized export or a complete restore. The
only plaintext file created here is an explicitly requested isolated candidate.
"""
from contextlib import closing
from datetime import datetime, timezone
import argparse
import getpass
import json
import os
from pathlib import Path
import sqlite3
import struct
import tempfile

from dlt.integrity import check_database
from dlt.recovery import (RecoveryError, _publish_staged, verify_isolated_candidate,
                          activate_isolated_candidate, require_private_directory,
                          require_private_file, reject_database_reparse_path,
                          require_private_database_files,
                          _record_write_receipt, _flush_staged_file)
from dlt.recovery_guard import require_execution_allowed


MAX_DATABASE_BYTES = 64 * 1024 * 1024
MAGIC = b'DLTBAK01'
HEADER = struct.Struct('>8sB16s12sQ')
VERSION = 1
TAG_BYTES = 16


class BackupError(ValueError):
    def __init__(self, message, *, check=None):
        super().__init__(message)
        self.check = check


def _crypto():
    try:
        from cryptography.exceptions import InvalidTag
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
    except ImportError as exc:
        raise BackupError('缺少加密依赖，请按 requirements.txt 安装') from exc
    return AESGCM, Scrypt, InvalidTag


def _key(password, salt):
    if not isinstance(password, str) or not 12 <= len(password) <= 1024:
        raise BackupError('备份口令须为 12—1024 个字符')
    try:
        encoded = password.encode('utf-8')
    except UnicodeError as exc:
        raise BackupError('备份口令无法编码') from exc
    _, Scrypt, _ = _crypto()
    return Scrypt(salt=salt, length=32, n=2**15, r=8, p=1).derive(encoded)


def _occupied(destination, *, sqlite_target=False):
    paths = [destination]
    if sqlite_target:
        paths.extend(Path(str(destination) + suffix) for suffix in ('-wal', '-shm', '-journal'))
    return any(path.exists() or path.is_symlink() for path in paths)


def _serialize(connection):
    if not hasattr(connection, 'serialize') or not hasattr(connection, 'deserialize'):
        raise BackupError('加密备份需要支持 SQLite serialize 的 Python 3.12 环境')
    return connection.serialize()


def _require_deserialize(connection):
    if not hasattr(connection, 'serialize') or not hasattr(connection, 'deserialize'):
        raise BackupError('加密备份需要支持 SQLite serialize 的 Python 3.12 环境')


def _deserializable_image(image):
    # sqlite3_backup has already applied the committed WAL to this image.
    # SQLite documents changing header bytes 18 and 19 to rollback mode before
    # deserialize, which cannot open a WAL-mode memory image.
    if len(image) < 100 or not image.startswith(b'SQLite format 3\x00'):
        raise BackupError('数据库内容无效')
    if image[18:20] == b'\x02\x02':
        return image[:18] + b'\x01\x01' + image[20:]
    if image[18:20] != b'\x01\x01':
        raise BackupError('数据库日志格式不受支持')
    return image


def _validate_memory(db):
    if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
        raise BackupError('数据库完整性检查失败')
    if db.execute('PRAGMA foreign_key_check').fetchone():
        raise BackupError('数据库外键检查失败')


def _new_temp(destination, prefix):
    fd, name = tempfile.mkstemp(prefix=prefix, suffix='.tmp', dir=destination.parent)
    os.close(fd)
    return Path(name)


def encrypted_snapshot_bytes(source, password):
    """Return an authenticated encrypted snapshot; no plaintext file is written."""
    try:
        reject_database_reparse_path(source)
        require_private_database_files(source)
    except RecoveryError as exc:
        raise BackupError(str(exc)) from exc
    source = Path(source).resolve()
    if not source.is_file():
        raise BackupError('源数据库不存在')
    salt, nonce = os.urandom(16), os.urandom(12)
    key = _key(password, salt)
    AESGCM, _, _ = _crypto()
    try:
        with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=5)) as src:
            src.execute('PRAGMA query_only=ON')
            src.execute('PRAGMA trusted_schema=OFF')
            src.execute('BEGIN')
            require_execution_allowed(src)
            src.execute('SELECT count(*) FROM sqlite_master').fetchone()
            if src.execute('PRAGMA page_count').fetchone()[0] * src.execute('PRAGMA page_size').fetchone()[0] > MAX_DATABASE_BYTES:
                raise BackupError('数据库超过加密快照大小上限')
            with closing(sqlite3.connect(':memory:')) as memory:
                memory.execute('PRAGMA temp_store=MEMORY')
                src.backup(memory)
                _validate_memory(memory)
                plaintext = _deserializable_image(_serialize(memory))
        if not plaintext or len(plaintext) > MAX_DATABASE_BYTES:
            raise BackupError('数据库快照为空或超出大小上限')
        header = HEADER.pack(MAGIC, VERSION, salt, nonce, len(plaintext))
        return header + AESGCM(key).encrypt(nonce, plaintext, header)
    except BackupError:
        raise
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise BackupError('加密快照创建失败，源数据库未被修改') from exc


def create_encrypted_snapshot(source, destination, password):
    """Save one SQLite-consistent snapshot; never create a plaintext backup file."""
    source, destination = Path(source).resolve(), Path(destination).absolute()
    if not source.is_file() or not destination.parent.is_dir():
        raise BackupError('源数据库或目标目录不存在')
    if _occupied(destination) or destination.resolve() == source:
        raise BackupError('目标已存在，未覆盖任何文件')
    staged = None
    published = False
    result = None
    try:
        payload = encrypted_snapshot_bytes(source, password)
        fd, name = tempfile.mkstemp(prefix='.dlt-encrypted-', suffix='.tmp', dir=destination.parent)
        staged = Path(name)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if _occupied(destination):
            raise BackupError('目标已存在，未覆盖任何文件')
        _publish_staged(staged, destination)
        published = True
        result = {'status': 'encrypted_snapshot_created', 'format': 'dlt-encrypted-sqlite-v1',
                  'database_bytes': len(payload) - HEADER.size - TAG_BYTES, 'staging_cleanup': 'complete',
                  'limitations': ['contains original user data and possibly sensitive material',
                                  'not a sanitized export or complete restore certification',
                                  'password cannot be recovered by the application']}
        return result
    except BackupError:
        raise
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise BackupError('加密快照创建失败，源数据库和已有目标未被覆盖') from exc
    finally:
        if staged is not None:
            try:
                staged.unlink(missing_ok=True)
            except OSError as exc:
                if published:
                    if result is None:
                        raise BackupError('加密快照已发布，但临时文件别名未能清除；请检查目标目录') from exc
                    result['staging_cleanup'] = 'failed'
                else:
                    raise BackupError('未发布备份，但加密临时文件未能清除') from exc


def inspect_encrypted_snapshot(source, password):
    """Authenticate and run selected integrity checks entirely in memory."""
    source = Path(source).resolve()
    if not source.is_file():
        raise BackupError('加密快照不存在')
    AESGCM, _, InvalidTag = _crypto()
    try:
        size = source.stat().st_size
        if not HEADER.size + TAG_BYTES <= size <= HEADER.size + MAX_DATABASE_BYTES + TAG_BYTES:
            raise BackupError('加密快照大小无效')
        with source.open('rb') as stream:
            header = stream.read(HEADER.size)
            magic, version, salt, nonce, plain_size = HEADER.unpack(header)
            if (magic != MAGIC or version != VERSION or not 1 <= plain_size <= MAX_DATABASE_BYTES
                    or size != HEADER.size + plain_size + TAG_BYTES):
                raise BackupError('加密快照格式无效')
            ciphertext = stream.read(plain_size + TAG_BYTES + 1)
        try:
            plaintext = AESGCM(_key(password, salt)).decrypt(nonce, ciphertext, header)
        except InvalidTag as exc:
            raise BackupError('口令错误或加密快照已损坏') from exc
        if len(plaintext) != plain_size:
            raise BackupError('数据库内容无效')
        plaintext = _deserializable_image(plaintext)
        with closing(sqlite3.connect(':memory:')) as memory:
            _require_deserialize(memory)
            memory.deserialize(plaintext)
            memory.execute('PRAGMA trusted_schema=OFF')
            report = check_database(memory)
        return {'status': 'inspected', 'check': report, 'database_bytes': plain_size,
                'plaintext_files_created': False,
                'limitations': ['selected structural and reference checks only; not full restore certification',
                                'no candidate was published and no application database was changed']}
    except BackupError:
        raise
    except (OSError, sqlite3.Error, ValueError, struct.error) as exc:
        raise BackupError('加密快照检查失败；未创建明文文件或更改数据库') from exc


def prepare_encrypted_snapshot(source, destination, password, *, acknowledge_plaintext=False,
                               write_receipt=None):
    """Decrypt to a new checked, quarantined SQLite candidate, never activating it."""
    try:
        reject_database_reparse_path(source)
        reject_database_reparse_path(destination)
    except RecoveryError as exc:
        raise BackupError(str(exc)) from exc
    source, destination = Path(source).resolve(), Path(destination).absolute()
    if acknowledge_plaintext is not True:
        raise BackupError('解密候选包含原始敏感内容，须明确确认并选择受保护目录')
    if not source.is_file() or not destination.parent.is_dir():
        raise BackupError('加密快照或目标目录不存在')
    if _occupied(destination, sqlite_target=True) or destination.resolve() == source:
        raise BackupError('目标或其数据库日志已存在，未覆盖任何文件')
    try:
        require_private_directory(destination.parent)
    except RecoveryError as exc:
        raise BackupError(str(exc)) from exc
    AESGCM, _, InvalidTag = _crypto()
    staged = None
    published = False
    result = None
    try:
        size = source.stat().st_size
        if not HEADER.size + TAG_BYTES <= size <= HEADER.size + MAX_DATABASE_BYTES + TAG_BYTES:
            raise BackupError('加密快照大小无效')
        with source.open('rb') as stream:
            header = stream.read(HEADER.size)
            magic, version, salt, nonce, plain_size = HEADER.unpack(header)
            if (magic != MAGIC or version != VERSION or not 1 <= plain_size <= MAX_DATABASE_BYTES
                    or size != HEADER.size + plain_size + TAG_BYTES):
                raise BackupError('加密快照格式无效')
            ciphertext = stream.read(plain_size + TAG_BYTES + 1)
        try:
            plaintext = AESGCM(_key(password, salt)).decrypt(nonce, ciphertext, header)
        except InvalidTag as exc:
            raise BackupError('口令错误或加密快照已损坏') from exc
        if len(plaintext) != plain_size:
            raise BackupError('数据库内容无效')
        plaintext = _deserializable_image(plaintext)
        with closing(sqlite3.connect(':memory:')) as memory:
            _require_deserialize(memory)
            memory.deserialize(plaintext)
            memory.execute('PRAGMA trusted_schema=OFF')
            require_execution_allowed(memory)
            _validate_memory(memory)
            memory.executescript('''
                CREATE TABLE recovery_quarantine (
                    id INTEGER PRIMARY KEY CHECK(id=1), created_at TEXT NOT NULL,
                    reason TEXT NOT NULL CHECK(reason='isolated recovery candidate')
                );
                CREATE TRIGGER recovery_quarantine_no_update BEFORE UPDATE ON recovery_quarantine
                    BEGIN SELECT RAISE(ABORT, 'immutable recovery quarantine'); END;
                CREATE TRIGGER recovery_quarantine_no_delete BEFORE DELETE ON recovery_quarantine
                    BEGIN SELECT RAISE(ABORT, 'immutable recovery quarantine'); END;
            ''')
            memory.execute('INSERT INTO recovery_quarantine VALUES (1,?,?)',
                           (datetime.now(timezone.utc).isoformat(), 'isolated recovery candidate'))
            memory.commit()
            memory_report = check_database(memory)
            if memory_report['status'] != 'passed':
                raise BackupError('内存候选数据库检查失败或不完整，未创建明文候选文件', check=memory_report)
            memory.commit()
            staged = _new_temp(destination, '.dlt-recovery-')
            with closing(sqlite3.connect(staged)) as disk:
                memory.backup(disk)
        _record_write_receipt(staged, write_receipt)
        report = check_database(staged)
        if report['status'] != 'passed':
            raise BackupError('候选数据库检查失败或不完整，未发布', check=report)
        try:
            require_private_directory(destination.parent)
            require_private_file(staged)
        except RecoveryError as exc:
            raise BackupError(str(exc), check=report) from exc
        _flush_staged_file(staged)
        if _occupied(destination, sqlite_target=True):
            raise BackupError('目标或其数据库日志已存在，未覆盖任何文件')
        _publish_staged(staged, destination)
        published = True
        try:
            require_private_file(destination)
        except RecoveryError as exc:
            raise BackupError('隔离候选已发布但 ACL 未通过；候选仍处于隔离状态，禁止启动', check=report) from exc
        result = {'status': 'prepared', 'check': report, 'staging_cleanup': 'complete',
                  'limitations': ['candidate is plaintext and can contain sensitive original material',
                                  'only selected checks passed; not full restore certification',
                                  'candidate remains quarantined; no tasks or plans may execute']}
        return result
    except BackupError:
        raise
    except (OSError, sqlite3.Error, ValueError, struct.error) as exc:
        raise BackupError('解密候选准备失败，原快照与已有目标未被覆盖') from exc
    finally:
        if staged is not None:
            try:
                staged.unlink(missing_ok=True)
            except OSError as exc:
                if published:
                    if result is None:
                        raise BackupError('隔离候选已发布，但明文临时文件别名未能清除；候选仍处于隔离状态，请检查受保护目录') from exc
                    result['staging_cleanup'] = 'failed'
                else:
                    raise BackupError('未发布候选，但明文临时文件未能清除，请检查受保护目录') from exc


def main():
    parser = argparse.ArgumentParser(description='Password-protected local research database snapshot')
    parser.add_argument('action', choices=('create', 'inspect', 'prepare', 'verify-candidate', 'activate-candidate'))
    parser.add_argument('source')
    parser.add_argument('destination', nargs='?')
    parser.add_argument('--acknowledge-plaintext', action='store_true',
                        help='confirm that the isolated candidate contains plaintext sensitive material')
    parser.add_argument('--enable-restored-plans', action='store_true',
                        help='explicitly allow restored plans to run when the new database copy is started')
    args = parser.parse_args()
    try:
        if args.action in ('create', 'prepare', 'activate-candidate') and not args.destination:
            raise BackupError('缺少目标路径')
        if args.action in ('inspect', 'verify-candidate') and (
                args.destination or args.acknowledge_plaintext or args.enable_restored_plans):
            raise BackupError(f'{args.action} 不接受目标路径或确认参数')
        if args.action == 'prepare' and not args.acknowledge_plaintext:
            raise BackupError('解密候选包含原始敏感内容，须明确确认并选择受保护目录')
        if args.action == 'activate-candidate' and (
                not args.acknowledge_plaintext or not args.enable_restored_plans):
            raise BackupError('恢复副本含明文资料且可能执行原计划，须分别明确确认')
        if args.action != 'activate-candidate' and args.enable_restored_plans:
            raise BackupError('--enable-restored-plans 只适用于 activate-candidate')
        if args.action == 'verify-candidate':
            result = verify_isolated_candidate(args.source)
        elif args.action == 'activate-candidate':
            result = activate_isolated_candidate(args.source, args.destination,
                acknowledge_plaintext=args.acknowledge_plaintext,
                enable_restored_plans=args.enable_restored_plans)
        else:
            print('备份包含完整原始研究资料，可能含敏感内容；口令遗失后无法恢复。')
            password = getpass.getpass('备份口令：')
            if args.action == 'create':
                if password != getpass.getpass('再次输入口令：'):
                    raise BackupError('两次口令不一致')
                result = create_encrypted_snapshot(args.source, args.destination, password)
            elif args.action == 'prepare':
                result = prepare_encrypted_snapshot(args.source, args.destination, password,
                                                    acknowledge_plaintext=args.acknowledge_plaintext)
            else:
                result = inspect_encrypted_snapshot(args.source, password)
    except (BackupError, RecoveryError) as exc:
        error = {'status': 'failed', 'error': str(exc)}
        if exc.check is not None:
            error['check'] = exc.check
        print(json.dumps(error, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result.get('check', {}).get('status') not in (None, 'passed'):
        return 2
    return 0 if result.get('staging_cleanup', 'complete') == 'complete' else 3


if __name__ == '__main__':
    raise SystemExit(main())
