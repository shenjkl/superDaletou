"""Opt-in per-user Windows logon entry for this local service instance."""
from pathlib import Path
import os
import subprocess
import sys

from dlt.recovery_guard import require_path_execution_allowed


KEY_PATH = r'Software\Microsoft\Windows\CurrentVersion\Run'
VALUE_NAME = 'DaletouResearchLocal'
LAUNCHER = Path(__file__).resolve().parents[1] / 'start-local.pyw'


class AutostartError(ValueError):
    pass


def _registry():
    if os.name != 'nt':
        raise AutostartError('仅 Windows 支持登录后启动设置')
    import winreg
    return winreg


def _command(db_path, port):
    if type(port) is not int or not 1 <= port <= 65535:
        raise AutostartError('服务端口无效')
    executable = Path(sys.executable).resolve().with_name('pythonw.exe')
    return subprocess.list2cmdline([str(executable), str(LAUNCHER), '--db', str(Path(db_path).resolve()), '--port', str(port)])


def _available():
    return Path(sys.executable).resolve().with_name('pythonw.exe').is_file() and LAUNCHER.is_file()


def _unavailable_reason(command):
    if not _available():
        return '后台启动程序缺失，登录时无法启动'
    # Windows documents a 260-character limit for Run command lines. Count
    # UTF-16 units conservatively, so supplementary characters do not overflow.
    if len(command.encode('utf-16-le')) // 2 > 260:
        return '启动路径过长，超过 Windows 登录启动项的 260 字符限制'
    return None


def _read(registry, key):
    try:
        value, kind = registry.QueryValueEx(key, VALUE_NAME)
    except FileNotFoundError:
        return None
    return value if kind == registry.REG_SZ and isinstance(value, str) else False


def status(db_path, port):
    try:
        registry = _registry()
        expected = _command(db_path, port)
        unavailable = _unavailable_reason(expected)
        try:
            with registry.OpenKey(registry.HKEY_CURRENT_USER, KEY_PATH, 0, registry.KEY_QUERY_VALUE) as key:
                value = _read(registry, key)
        except FileNotFoundError:
            value = None
        if value is None:
            return {'state': 'disabled', 'reason': unavailable, 'can_enable': unavailable is None}
        if value == expected:
            return {'state': 'enabled', 'reason': unavailable, 'can_enable': unavailable is None}
        return {'state': 'conflict', 'reason': '已有其他启动配置；为保护原配置，此处不能覆盖或删除'}
    except (AutostartError, OSError):
        return {'state': 'unavailable', 'reason': '无法读取登录启动设置或找不到后台启动程序'}


def set_enabled(db_path, port, enabled):
    if type(enabled) is not bool:
        raise AutostartError('启动设置必须为开启或关闭')
    require_path_execution_allowed(db_path)
    registry = _registry()
    expected = _command(db_path, port)
    unavailable = _unavailable_reason(expected)
    if enabled and unavailable:
        raise AutostartError(unavailable)
    access = registry.KEY_QUERY_VALUE | registry.KEY_SET_VALUE
    try:
        if enabled:
            key = registry.CreateKeyEx(registry.HKEY_CURRENT_USER, KEY_PATH, 0, access)
        else:
            try:
                key = registry.OpenKey(registry.HKEY_CURRENT_USER, KEY_PATH, 0, access)
            except FileNotFoundError:
                return {'state': 'disabled', 'reason': unavailable, 'can_enable': unavailable is None}
        with key:
            value = _read(registry, key)
            if value is not None and value != expected:
                raise AutostartError('已有其他启动配置，未覆盖或删除')
            if enabled and value is None:
                registry.SetValueEx(key, VALUE_NAME, 0, registry.REG_SZ, expected)
            elif not enabled and value is not None:
                registry.DeleteValue(key, VALUE_NAME)
    except OSError as exc:
        raise AutostartError('登录启动设置未保存，请检查当前用户权限') from exc
    return {'state': 'enabled' if enabled else 'disabled', 'reason': unavailable, 'can_enable': unavailable is None}
