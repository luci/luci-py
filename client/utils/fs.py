# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Wraps os, os.path and shutil functions to work around MAX_PATH on Windows."""

import builtins
import inspect
import logging
import os
import re
import shutil
import subprocess
import sys

if sys.platform == 'win32':
  import ctypes
  from ctypes import wintypes
  from ctypes import windll


  CreateSymbolicLinkW = windll.kernel32.CreateSymbolicLinkW
  CreateSymbolicLinkW.argtypes = (
      wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
  CreateSymbolicLinkW.restype = wintypes.BOOL
  DeleteFile = windll.kernel32.DeleteFileW
  DeleteFile.argtypes = (wintypes.LPCWSTR,)
  DeleteFile.restype = wintypes.BOOL
  GetFileAttributesW = windll.kernel32.GetFileAttributesW
  GetFileAttributesW.argtypes = (wintypes.LPCWSTR,)
  GetFileAttributesW.restype = wintypes.DWORD
  RemoveDirectory = windll.kernel32.RemoveDirectoryW
  RemoveDirectory.argtypes = (wintypes.LPCWSTR,)
  RemoveDirectory.restype = wintypes.BOOL
  DeviceIoControl = windll.kernel32.DeviceIoControl
  DeviceIoControl.argtypes = (
      wintypes.HANDLE, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD,
      wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD),
      wintypes.LPVOID)
  DeviceIoControl.restype = wintypes.BOOL

  # It's *much* faster to temporarily waste 32Kb of memory than call
  # DeviceIoControl() one additional time to figure out the buffer size to
  # allocate.
  _MAX_WIN_PATH_LENGTH = 32768

  class _SYMBOLIC_LINK_REPARSE_BUFFER(ctypes.Structure):
    _fields_ = (
      ('SubstituteNameOffset', wintypes.USHORT),
      ('SubstituteNameLength', wintypes.USHORT),
      ('PrintNameOffset', wintypes.USHORT),
      ('PrintNameLength', wintypes.USHORT),
      ('Flags', wintypes.ULONG),
      ('PathBuffer', wintypes.WCHAR * _MAX_WIN_PATH_LENGTH),
    )

  class _MOUNT_POINT_REPARSE_BUFFER(ctypes.Structure):
    _fields_ = (
      ('SubstituteNameOffset', wintypes.USHORT),
      ('SubstituteNameLength', wintypes.USHORT),
      ('PrintNameOffset', wintypes.USHORT),
      ('PrintNameLength', wintypes.USHORT),
      ('PathBuffer', wintypes.WCHAR * _MAX_WIN_PATH_LENGTH),
    )

  class _REPARSE_BUFFER(ctypes.Union):
    _fields_ = (
      ('SymbolicLinkReparseBuffer', _SYMBOLIC_LINK_REPARSE_BUFFER),
      ('MountPointReparseBuffer', _MOUNT_POINT_REPARSE_BUFFER),
    )

  # https://msdn.microsoft.com/en-us/data/ff552012(v=vs.96)
  class REPARSE_DATA_BUFFER(ctypes.Structure):
    _fields_ = (
      # A bitpacked value.
      ('ReparseTag', wintypes.DWORD),
      ('ReparseDataLength', wintypes.WORD),
      ('Reserved', wintypes.WORD),
      ('ReparseBuffer', _REPARSE_BUFFER),
    )

  # Flags for GetFileAttributesW().
  FILE_ATTRIBUTE_REPARSE_POINT = 1024
  INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF

  # Flags for CreateFileW().
  FILE_FLAG_BACKUP_SEMANTICS = 0x2000000
  FILE_FLAG_OPEN_REPARSE_POINT = 0x200000
  FILE_SHARE_READ = 1
  FILE_SHARE_WRITE = 2
  FILE_SHARE_DELETE = 4
  FILE_READ_EA = 8
  OPEN_ALWAYS = 4
  INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

  # Flags for CreateSymbolicLinkW().
  SYMBOLIC_LINK_FLAG_DIRECTORY = 1
  SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE = 2

  # Flag for DeviceIoControl().
  FSCTL_GET_REPARSE_POINT = 0x900a8

  # https://msdn.microsoft.com/en-us/library/dd541667.aspx
  IO_REPARSE_TAG_MOUNT_POINT = 0xa0000003
  IO_REPARSE_TAG_SYMLINK = 0xa000000c


  # Set to true or false once symlink support is initialized.
  _SUPPORTS_SYMLINKS = None


  def _supports_unprivileged_symlinks():
    """Returns True if the OS supports sane symlinks without any user privilege
    modification.

    Actively work around AppCompat version lie shim. This is kinda insane to
    shell out to figure out the real Windows version but this is ironically the
    the most reliable way. This code was inspired by _get_os_numbers() in
    //appengine/swarming/swarming_bot/api/platforms/win.py.
    """
    global _SUPPORTS_SYMLINKS
    if _SUPPORTS_SYMLINKS != None:
      return _SUPPORTS_SYMLINKS

    _SUPPORTS_SYMLINKS = False
    # Windows is lying to us until python adds to its manifest:
    #   <supportedOS Id="{8e0f7a12-bfb3-4fe8-b9a5-48fd50a15a9a}"/>
    # and it doesn't.
    # So ask nicely to cmd.exe instead, which will always happily report the
    # right version. Here's some sample output:
    # - XP: Microsoft Windows XP [Version 5.1.2600]
    # - Win10: Microsoft Windows [Version 10.0.10240]
    # - Win7 or Win2K8R2: Microsoft Windows [Version 6.1.7601]
    try:
      out = subprocess.check_output(['cmd.exe', '/c', 'ver'], text=True).strip()
      match = re.search(r'\[Version (\d+\.\d+)\.(\d+)\]', out, re.IGNORECASE)
      if match:
        # That's a bit gross but good enough.
        major = float(match.group(1))
        if major > 10:
          _SUPPORTS_SYMLINKS = True
        elif major == 10:
          # https://blogs.windows.com/buildingapps/2016/12/02/symlinks-windows-10/
          _SUPPORTS_SYMLINKS = int(match.group(2)) >= 14971
    except (subprocess.CalledProcessError, OSError):
      # Catastrophic issue.
      pass
    return _SUPPORTS_SYMLINKS


  def extend(path):
    """Adds '\\\\?\\' when given an absolute path so the MAX_PATH (260) limit is
    not enforced.
    """
    assert os.path.isabs(path), path
    assert isinstance(path, str), "%s, type: %s" % (path, type(path))
    prefix = '\\\\?\\'
    return path if path.startswith(prefix) else prefix + path


  def trim(path):
    """Removes '\\\\?\\' when receiving a path."""
    assert isinstance(path, str), "%s, type: %s" % (path, type(path))
    prefix = '\\\\?\\'
    if path.startswith(prefix):
      path = path[len(prefix):]
    assert os.path.isabs(path), path
    return path


  def islink(path):
    """Proper implementation of islink() for Windows.

    The stdlib is broken.
    https://msdn.microsoft.com/library/windows/desktop/aa365682.aspx
    os.path.islink() always returns false on WindowsNT/95 and OS/2 in python2,
    https://github.com/python/cpython/blob/2.7/Lib/ntpath.py#L220
    and also for Windows prior to 6.0 in python3.
    https://github.com/python/cpython/blob/3.9/Lib/ntpath.py#L228
    """
    res = GetFileAttributesW(extend(path))
    if res == INVALID_FILE_ATTRIBUTES:
      return False
    return bool(res & FILE_ATTRIBUTE_REPARSE_POINT)


  def symlink(source, link_name):
    """Creates a symlink on Windows 7 and later.

    This function will only work once SeCreateSymbolicLinkPrivilege has been
    enabled. See file_path.enable_symlink().

    Useful material:
    CreateSymbolicLinkW:
      https://msdn.microsoft.com/library/windows/desktop/aa363866.aspx
    UAC and privilege stripping:
      https://msdn.microsoft.com/library/bb530410.aspx
    Privilege constants:
      https://msdn.microsoft.com/library/windows/desktop/bb530716.aspx
    Windows 10 and developer mode:
      https://blogs.windows.com/buildingapps/2016/12/02/symlinks-windows-10/
    """
    f = extend(link_name)
    # We need to convert to absolute path, so we can test if it points to a
    # directory or a file.
    real_source = source
    if not os.path.isabs(source):
      real_source = extend(
          os.path.normpath(os.path.join(os.path.dirname(link_name), source)))
    # pylint: disable=undefined-variable
    # isdir is generated dynamically.
    flags = SYMBOLIC_LINK_FLAG_DIRECTORY if isdir(real_source) else 0
    if _supports_unprivileged_symlinks():
      # This enables support for this specific case:
      # - Windows 10 with build 14971 or later
      # - Admin account
      # - UAC enabled
      # - Developer mode enabled (not the default)
      #
      # In this specific case, file_path.enable_symlink() is unnecessary and the
      # following flag make it magically work.
      flags |= SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE
    if not CreateSymbolicLinkW(f, source, flags):
      # pylint: disable=undefined-variable
      err = ctypes.GetLastError()
      if err == 1314:
        raise WindowsError(
            ('symlink(%r, %r) failed: ERROR_PRIVILEGE_NOT_HELD(1314). Try '
             'calling file_path.enable_symlink() first') % (source, link_name))
      raise WindowsError('symlink(%r, %r) failed: %s' %
                         (source, link_name, err))


  def remove(path):
    """Removes a symlink on Windows 7 and later.

    Does not delete the link source.

    If path is not a link, but a non-empty directory, will fail with a
    WindowsError.

    Useful material:
    CreateSymbolicLinkW:
      https://msdn.microsoft.com/library/windows/desktop/aa363866.aspx
    DeleteFileW:
      https://msdn.microsoft.com/en-us/library/windows/desktop/aa363915(v=vs.85).aspx
    RemoveDirectoryW:
      https://msdn.microsoft.com/en-us/library/windows/desktop/aa365488(v=vs.85).aspx
    """
    path = extend(path)
    # pylint: disable=undefined-variable
    # isdir is generated dynamically.
    if isdir(path):
      if not RemoveDirectory(path):
        # pylint: disable=undefined-variable
        raise WindowsError('unlink(%r): could not remove directory: %s' %
                           (path, ctypes.GetLastError()))
    else:
      if DeleteFile(path):
        return
      logging.warning('failed to call DeleteFile(%r): win error: %s', path,
                      ctypes.GetLastError())

      # fallback to unlink if it fails to call DeleteFile.
      os.unlink(path)


  def readlink(path):
    """Reads a symlink on Windows."""
    # Interestingly, when using FILE_FLAG_OPEN_REPARSE_POINT and the destination
    # is not a reparse point, the actual file will be opened. It's the
    # DeviceIoControl() below that will fail with 4390.
    handle = windll.kernel32.CreateFileW(
        extend(path),
        FILE_READ_EA,
        FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
        None,
        OPEN_ALWAYS,
        FILE_FLAG_BACKUP_SEMANTICS|FILE_FLAG_OPEN_REPARSE_POINT,
        None)
    if handle == INVALID_HANDLE_VALUE:
      # pylint: disable=undefined-variable
      raise WindowsError('readlink(%r): failed to open: %s' %
                         (path, ctypes.GetLastError()))
    try:
      buf = REPARSE_DATA_BUFFER()
      returned = wintypes.DWORD()
      ret = DeviceIoControl(
          handle, FSCTL_GET_REPARSE_POINT, None, 0, ctypes.byref(buf),
          ctypes.sizeof(buf), ctypes.byref(returned), None)
      if not ret:
        err = ctypes.GetLastError()
        # ERROR_MORE_DATA(234) should not happen because we preallocate the
        # maximum size.
        if err == 4390:
          # pylint: disable=undefined-variable
          raise WindowsError(
              'readlink(%r): failed to read: ERROR_NOT_A_REPARSE_POINT(4390)' %
              path)
        # pylint: disable=undefined-variable
        raise WindowsError('readlink(%r): failed to read: %s' % (path, err))
      if buf.ReparseTag == IO_REPARSE_TAG_SYMLINK:
        actual = buf.ReparseBuffer.SymbolicLinkReparseBuffer
      elif buf.ReparseTag == IO_REPARSE_TAG_MOUNT_POINT:
        actual = buf.ReparseBuffer.MountPointReparseBuffer
      else:
        raise WindowsError(  # pylint: disable=undefined-variable
            'readlink(%r): succeeded but doesn\'t know how to parse result!' %
            path)
      off = actual.PrintNameOffset // 2
      end = off + actual.PrintNameLength // 2
      return actual.PathBuffer[off:end]
    finally:
      windll.kernel32.CloseHandle(handle)


  def walk(top, topdown=True, onerror=None, followlinks=False):
    # We need to reimplement walk() here because the standard library ignores
    # the flag followlinks=False. This is because ntpath.islink() is hardcoded
    # to return false. On Windows, os.path is an alias to ntpath.
    utop = extend(top)
    try:
      names = listdir(utop)  # pylint: disable=undefined-variable
    except os.error as err:
      if onerror is not None:
        onerror(err)
      return

    dirs, nondirs = [], []
    for name in names:
      if isdir(os.path.join(top, name)):   # pylint: disable=undefined-variable
        dirs.append(name)
      else:
        nondirs.append(name)

    if topdown:
      yield top, dirs, nondirs
    for name in dirs:
      new_path = os.path.join(top, name)
      if followlinks or not islink(new_path):
        for x in walk(new_path, topdown, onerror, followlinks):
          yield x
    if not topdown:
      yield top, dirs, nondirs


else:


  def extend(path):
    """Path extending is not needed on POSIX."""
    assert os.path.isabs(path), path
    assert isinstance(path, str), "%s, type: %s" % (path, type(path))
    return path


  def trim(path):
    """Path mangling is not needed on POSIX."""
    assert os.path.isabs(path), path
    assert isinstance(path, str), "%s, type: %s" % (path, type(path))
    return path


  def islink(path):
    return os.path.islink(extend(path))


  def symlink(source, link_name):
    return os.symlink(source, extend(link_name))


  def readlink(path):
    return os.readlink(extend(path))


  def walk(top, *args, **kwargs):
    for root, dirs, files in os.walk(extend(top), *args, **kwargs):
      yield trim(root), dirs, files


  def remove(path):
    return os.remove(extend(path))


## builtin


def open(path, *args, **kwargs):  # pylint: disable=redefined-builtin
  return builtins.open(extend(path), *args, **kwargs)


## os


def link(source, link_name):
  return os.link(extend(source), extend(link_name))


def rename(old, new):
  return os.rename(extend(old), extend(new))


def renames(old, new):
  return os.renames(extend(old), extend(new))


def unlink(path):
  return remove(path)


## shutil


def copy2(src, dst):
  return shutil.copy2(extend(src), extend(dst))


def rmtree(path, *args, **kwargs):
  return shutil.rmtree(extend(path), *args, **kwargs)


## The rest


def _get_lambda(func):
  return lambda path, *args, **kwargs: func(extend(path), *args, **kwargs)


def _is_path_fn(func):
  return (inspect.getargspec(func)[0] or [None]) == 'path'


_os_fns = (
  'access', 'chdir', 'chflags', 'chroot', 'chmod', 'chown', 'lchflags',
  'lchmod', 'lchown', 'listdir', 'lstat', 'mknod', 'mkdir', 'makedirs',
  'removedirs', 'rmdir', 'stat', 'statvfs', 'utime')

_os_path_fns = (
  'exists', 'lexists', 'getatime', 'getmtime', 'getctime', 'getsize', 'isfile',
  'isdir', 'ismount')


for _fn in _os_fns:
  if hasattr(os, _fn):
    sys.modules[__name__].__dict__.setdefault(
        _fn, _get_lambda(getattr(os, _fn)))


for _fn in _os_path_fns:
  if hasattr(os.path, _fn):
    sys.modules[__name__].__dict__.setdefault(
        _fn, _get_lambda(getattr(os.path, _fn)))
