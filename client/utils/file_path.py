# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Provides functions: get_native_path_case(), isabs() and safe_join().

This module assumes that filesystem is not changing while current process
is running and thus it caches results of functions that depend on FS state.
"""

from collections import deque
import ctypes
import errno
import getpass
import logging
import os
import posixpath
import re
import shlex
import stat
import sys
import tempfile
import time

from utils import fs
from utils import subprocess42
from utils import tools

# Types of action accepted by link_file().
HARDLINK, HARDLINK_WITH_FALLBACK, SYMLINK, SYMLINK_WITH_FALLBACK, COPY = range(
    1, 6)


## OS-specific imports


if sys.platform == 'win32':
  import locale
  from ctypes import wintypes  # pylint: disable=ungrouped-imports
  from ctypes import windll  # pylint: disable=ungrouped-imports

if sys.platform == 'win32':
  class LUID(ctypes.Structure):
    _fields_ = [
      ('low_part', wintypes.DWORD), ('high_part', wintypes.LONG),
    ]


  class LUID_AND_ATTRIBUTES(ctypes.Structure):
    _fields_ = [('LUID', LUID), ('attributes', wintypes.DWORD)]


  class TOKEN_PRIVILEGES(ctypes.Structure):
    _fields_ = [
      ('count', wintypes.DWORD), ('privileges', LUID_AND_ATTRIBUTES*0),
    ]

    def get_array(self):
      array_type = LUID_AND_ATTRIBUTES * self.count
      return ctypes.cast(self.privileges, ctypes.POINTER(array_type)).contents


  GetCurrentProcess = windll.kernel32.GetCurrentProcess
  GetCurrentProcess.restype = wintypes.HANDLE
  OpenProcessToken = windll.advapi32.OpenProcessToken
  OpenProcessToken.argtypes = (
      wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE))
  OpenProcessToken.restype = wintypes.BOOL
  LookupPrivilegeValue = windll.advapi32.LookupPrivilegeValueW
  LookupPrivilegeValue.argtypes = (
      wintypes.LPCWSTR, wintypes.LPCWSTR, ctypes.POINTER(LUID))
  LookupPrivilegeValue.restype = wintypes.BOOL
  LookupPrivilegeName = windll.advapi32.LookupPrivilegeNameW
  LookupPrivilegeName.argtypes = (
      wintypes.LPCWSTR, ctypes.POINTER(LUID), wintypes.LPWSTR,
      ctypes.POINTER(wintypes.DWORD))
  LookupPrivilegeName.restype = wintypes.BOOL
  PTOKEN_PRIVILEGES = ctypes.POINTER(TOKEN_PRIVILEGES)
  AdjustTokenPrivileges = windll.advapi32.AdjustTokenPrivileges
  AdjustTokenPrivileges.restype = wintypes.BOOL
  AdjustTokenPrivileges.argtypes = (
      wintypes.HANDLE, wintypes.BOOL, PTOKEN_PRIVILEGES,
      wintypes.DWORD, PTOKEN_PRIVILEGES,
      ctypes.POINTER(wintypes.DWORD))


  def FormatError(err):
    """Returns a formatted error on Windows."""
    # We need to take in account the current code page.
    return ctypes.FormatError(err)


  def QueryDosDevice(drive_letter):
    """Returns the Windows 'native' path for a DOS drive letter."""
    assert re.match(r'^[a-zA-Z]:$', drive_letter), drive_letter
    assert isinstance(drive_letter, str)
    # Guesswork. QueryDosDeviceW never returns the required number of bytes.
    chars = 1024
    p = wintypes.create_unicode_buffer(chars)
    if not windll.kernel32.QueryDosDeviceW(drive_letter, p, chars):
      err = ctypes.GetLastError()
      if err:
        # pylint: disable=undefined-variable
        msg = 'QueryDosDevice(%s): %s (%d)' % (drive_letter, FormatError(err),
                                               err)
        raise WindowsError(err, msg.encode('utf-8'))
    return p.value


  def GetShortPathName(long_path):
    """Returns the Windows short path equivalent for a 'long' path."""
    path = fs.extend(long_path)
    chars = windll.kernel32.GetShortPathNameW(path, None, 0)
    if chars:
      p = ctypes.create_unicode_buffer(chars)
      if windll.kernel32.GetShortPathNameW(path, p, chars):
        return fs.trim(p.value)

    err = ctypes.GetLastError()
    if err:
      # pylint: disable=undefined-variable
      msg = 'GetShortPathName(%s): %s (%d)' % (long_path, FormatError(err), err)
      raise WindowsError(err, msg.encode('utf-8'))
    return None


  def GetLongPathName(short_path):
    """Returns the Windows long path equivalent for a 'short' path."""
    path = fs.extend(short_path)
    chars = windll.kernel32.GetLongPathNameW(path, None, 0)
    if chars:
      p = ctypes.create_unicode_buffer(chars)
      if windll.kernel32.GetLongPathNameW(path, p, chars):
        return fs.trim(p.value)

    err = ctypes.GetLastError()
    if err:
      # pylint: disable=undefined-variable
      msg = 'GetLongPathName(%s): %s (%d)' % (short_path, FormatError(err), err)
      raise WindowsError(err, msg.encode('utf-8'))
    return None


  def MoveFileEx(oldpath, newpath, flags):
    """Calls MoveFileEx, converting errors to WindowsError exceptions."""
    old_p = fs.extend(oldpath)
    new_p = fs.extend(newpath)
    if not windll.kernel32.MoveFileExW(old_p, new_p, int(flags)):
      # pylint: disable=undefined-variable
      err = ctypes.GetLastError()
      msg = 'MoveFileEx(%s, %s, %d): %s (%d)' % (oldpath, newpath, flags,
                                                 FormatError(err), err)
      raise WindowsError(err, msg.encode('utf-8'))


  class DosDriveMap:
    """Maps \\Device\\HarddiskVolumeN to N: on Windows."""
    # Keep one global cache.
    _MAPPING = {}

    def __init__(self):
      """Lazy loads the cache."""
      if not self._MAPPING:
        # This is related to UNC resolver on windows. Ignore that.
        self._MAPPING['\\Device\\Mup'] = None
        self._MAPPING['\\SystemRoot'] = os.environ['SystemRoot']

        for letter in (chr(l) for l in range(ord('C'), ord('Z') + 1)):
          try:
            letter = '%s:' % letter
            mapped = QueryDosDevice(letter)
            if mapped in self._MAPPING:
              logging.warning(
                  ('Two drives: \'%s\' and \'%s\', are mapped to the same disk'
                   '. Drive letters are a user-mode concept and the kernel '
                   'traces only have NT path, so all accesses will be '
                   'associated with the first drive letter, independent of the '
                   'actual letter used by the code') % (self._MAPPING[mapped],
                                                        letter))
            else:
              self._MAPPING[mapped] = letter
          except WindowsError:  # pylint: disable=undefined-variable
            pass

    def to_win32(self, path):
      """Converts a native NT path to Win32/DOS compatible path."""
      match = re.match(r'(^\\Device\\[a-zA-Z0-9]+)(\\.*)?$', path)
      if not match:
        raise ValueError(
            'Can\'t convert %s into a Win32 compatible path' % path,
            path)
      if not match.group(1) in self._MAPPING:
        # Unmapped partitions may be accessed by windows for the
        # fun of it while the test is running. Discard these.
        return None
      drive = self._MAPPING[match.group(1)]
      if not drive or not match.group(2):
        return drive
      return drive + match.group(2)


  def change_acl_for_delete(path):
    """Zaps the SECURITY_DESCRIPTOR's DACL on a directory entry that is tedious
    to delete.

    This function is a heavy hammer. It discards the SECURITY_DESCRIPTOR and
    creates a new one with only one DACL set to user:FILE_ALL_ACCESS.

    Used as last resort.
    """
    STANDARD_RIGHTS_REQUIRED = 0xf0000
    SYNCHRONIZE = 0x100000
    FILE_ALL_ACCESS = STANDARD_RIGHTS_REQUIRED | SYNCHRONIZE | 0x3ff

    import win32security
    user, _domain, _type = win32security.LookupAccountName(
        '', getpass.getuser())
    sd = win32security.SECURITY_DESCRIPTOR()
    sd.Initialize()
    sd.SetSecurityDescriptorOwner(user, False)
    dacl = win32security.ACL()
    dacl.Initialize()
    dacl.AddAccessAllowedAce(
        win32security.ACL_REVISION_DS, FILE_ALL_ACCESS, user)
    sd.SetSecurityDescriptorDacl(1, dacl, 0)
    # Note that this assumes the object is either owned by the current user or
    # its group or that the current ACL permits this. Otherwise it will silently
    # fail.
    win32security.SetFileSecurity(
        fs.extend(path), win32security.DACL_SECURITY_INFORMATION, sd)
    # It's important to also look for the read only bit after, as it's possible
    # the set_read_only() call to remove the read only bit had silently failed
    # because there was no DACL for the user.
    if not (os.stat(path).st_mode & stat.S_IWUSR):
      os.chmod(path, 0o777)


  def isabs(path):
    """Accepts X: as an absolute path, unlike python's os.path.isabs()."""
    return os.path.isabs(path) or len(path) == 2 and path[1] == ':'


  def get_process_token():
    """Get the current process token."""
    TOKEN_ALL_ACCESS = 0xF01FF
    token = ctypes.wintypes.HANDLE()
    if not OpenProcessToken(
        GetCurrentProcess(), TOKEN_ALL_ACCESS, ctypes.byref(token)):
      # pylint: disable=undefined-variable
      raise WindowsError('Couldn\'t get process token')
    return token


  def get_luid(name):
    """Returns the LUID for a privilege."""
    luid = LUID()
    if not LookupPrivilegeValue(None, str(name), ctypes.byref(luid)):
      # pylint: disable=undefined-variable
      raise WindowsError('Couldn\'t lookup privilege value')
    return luid


  def enable_privilege(name):
    """Enables the privilege for the current process token.

    Returns:
    - True if the assignment is successful.
    """
    SE_PRIVILEGE_ENABLED = 2
    ERROR_NOT_ALL_ASSIGNED = 1300

    size = ctypes.sizeof(TOKEN_PRIVILEGES) + ctypes.sizeof(LUID_AND_ATTRIBUTES)
    buf = ctypes.create_string_buffer(size)
    tp = ctypes.cast(buf, ctypes.POINTER(TOKEN_PRIVILEGES)).contents
    tp.count = 1
    tp.get_array()[0].LUID = get_luid(name)
    tp.get_array()[0].Attributes = SE_PRIVILEGE_ENABLED
    token = get_process_token()
    try:
      if not AdjustTokenPrivileges(token, False, tp, 0, None, None):
        # pylint: disable=undefined-variable
        raise WindowsError('AdjustTokenPrivileges(%r): failed: %s' %
                           (name, ctypes.GetLastError()))
    finally:
      windll.kernel32.CloseHandle(token)
    return ctypes.GetLastError() != ERROR_NOT_ALL_ASSIGNED


  def enable_symlink():
    """Enable SeCreateSymbolicLinkPrivilege for the current token.

    This function is only helpful in ONE of the following case:
    - UAC is disabled, account is admin OR SeCreateSymbolicLinkPrivilege was
      manually granted.
    - UAC is enabled, account is NOT admin AND SeCreateSymbolicLinkPrivilege was
      manually granted.

    If running Windows 10 and the following is true, then enable_symlink() is
    unnecessary.
    - Windows 10 with build 14971 or later
    - Admin account
    - UAC enabled
    - Developer mode enabled (not the default)

    Returns:
    - True if symlink support is enabled.
    """
    return enable_privilege('SeCreateSymbolicLinkPrivilege')


  def kill_children_processes(root):
    """Try to kill all children processes indistriminately and prints updates to
    stderr.

    Returns:
      True if at least one child process was found.
    """
    processes = _get_children_processes_win(root)
    if not processes:
      return False
    logging.debug('Enumerating processes:\n')
    for _, proc in sorted(processes.items()):
      logging.debug('- pid %d; Handles: %d; Exe: %s; Cmd: %s\n', proc.ProcessId,
                    proc.HandleCount, proc.ExecutablePath, proc.CommandLine)
    logging.debug('Terminating %d processes:\n', len(processes))
    for pid in sorted(processes):
      try:
        # Killing is asynchronous.
        os.kill(pid, 9)
        logging.debug('- %d killed\n', pid)
      except OSError as e:
        logging.error('- failed to kill %s, error %s\n', pid, e)
    return True


  ## Windows private code.


  def _enum_processes_win():
    """Returns all processes on the system that are accessible to this process.

    Returns:
      Win32_Process COM objects. See
      http://msdn.microsoft.com/library/aa394372.aspx for more details.
    """
    import win32com.client  # pylint: disable=F0401
    wmi_service = win32com.client.Dispatch('WbemScripting.SWbemLocator')
    wbem = wmi_service.ConnectServer('.', 'root\\cimv2')
    return list(wbem.ExecQuery('SELECT * FROM Win32_Process'))


  def _filter_processes_dir_win(processes, root_dir):
    """Returns all processes which has their main executable located inside
    root_dir.
    """
    def normalize_path(filename):
      try:
        return GetLongPathName(str(filename)).lower()
      except:  # pylint: disable=W0702
        return str(filename).lower()

    root_dir = normalize_path(root_dir)

    def process_name(proc):
      if proc.ExecutablePath:
        return normalize_path(proc.ExecutablePath)
      # proc.ExecutablePath may be empty if the process hasn't finished
      # initializing, but the command line may be valid.
      if proc.CommandLine is None:
        return None
      parsed_line = shlex.split(proc.CommandLine)
      if len(parsed_line) >= 1 and os.path.isabs(parsed_line[0]):
        return normalize_path(parsed_line[0])
      return None

    long_names = ((process_name(proc), proc) for proc in processes)

    return [
      proc for name, proc in long_names
      if name is not None and name.startswith(root_dir)
    ]


  def _filter_processes_tree_win(processes):
    """Returns all the processes under the current process."""
    # Convert to dict.
    processes = dict((p.ProcessId, p) for p in processes)
    root_pid = os.getpid()
    out = {root_pid: processes[root_pid]}
    while True:
      found = set()
      for pid in out:
        found.update(
            p.ProcessId for p in processes.values()
            if p.ParentProcessId == pid)
      found -= set(out)
      if not found:
        break
      out.update((p, processes[p]) for p in found)
    return out.values()


  def _get_children_processes_win(root):
    """Returns a list of processes.

    Enumerates both:
    - all child processes from this process.
    - processes where the main executable in inside 'root'. The reason is that
      the ancestry may be broken so stray grand-children processes could be
      undetected by the first technique.

    This technique is not fool-proof but gets mostly there.
    """
    processes = _enum_processes_win()
    tree_processes = _filter_processes_tree_win(processes)
    dir_processes = _filter_processes_dir_win(processes, root)
    # Convert to dict to remove duplicates.
    processes = dict((p.ProcessId, p) for p in tree_processes)
    processes.update((p.ProcessId, p) for p in dir_processes)
    processes.pop(os.getpid())
    return processes


elif sys.platform == 'darwin':


  # On non-windows, keep the stdlib behavior.
  isabs = os.path.isabs

  def enable_symlink():
    return True

else:  # OSes other than Windows and OSX.


  # On non-windows, keep the stdlib behavior.
  isabs = os.path.isabs

  def enable_symlink():
    return True


if sys.platform != 'win32':  # All non-Windows OSes.


  def safe_join(*args):
    """Joins path elements like os.path.join() but doesn't abort on absolute
    path.

    os.path.join('foo', '/bar') == '/bar'
    but safe_join('foo', '/bar') == 'foo/bar'.
    """
    out = ''
    for element in args:
      if element.startswith(os.path.sep):
        if out.endswith(os.path.sep):
          out += element[1:]
        else:
          out += element
      else:
        if out.endswith(os.path.sep):
          out += element
        else:
          out += os.path.sep + element
    return out


  @tools.profile
  def split_at_symlink(base_dir, relfile):
    """Scans each component of relfile and cut the string at the symlink if
    there is any.

    Returns a tuple (base_path, symlink, rest), with symlink == rest == None if
    not symlink was found.
    """
    if base_dir:
      assert relfile
      assert os.path.isabs(base_dir)
      index = 0
    else:
      assert os.path.isabs(relfile)
      index = 1

    def at_root(rest):
      if base_dir:
        return safe_join(base_dir, rest)
      return rest

    while True:
      try:
        index = relfile.index(os.path.sep, index)
      except ValueError:
        index = len(relfile)
      full = at_root(relfile[:index])
      if fs.islink(full):
        # A symlink!
        base = os.path.dirname(relfile[:index])
        symlink = os.path.basename(relfile[:index])
        rest = relfile[index:]
        logging.debug(
            'split_at_symlink(%s, %s) -> (%s, %s, %s)' %
            (base_dir, relfile, base, symlink, rest))
        return base, symlink, rest
      if index == len(relfile):
        break
      index += 1
    return relfile, None, None


  def kill_children_processes(root):
    """Not yet implemented on posix."""
    # pylint: disable=unused-argument
    return False


def relpath(path, root):
  """os.path.relpath() that keeps trailing os.path.sep."""
  out = os.path.relpath(path, root)
  if path.endswith(os.path.sep):
    out += os.path.sep
  return out


def safe_relpath(filepath, basepath):
  """Do not throw on Windows when filepath and basepath are on different drives.

  Different than relpath() above since this one doesn't keep the trailing
  os.path.sep and it swallows exceptions on Windows and return the original
  absolute path in the case of different drives.
  """
  try:
    return os.path.relpath(filepath, basepath)
  except ValueError:
    assert sys.platform == 'win32'
    return filepath


def normpath(path):
  """os.path.normpath() that keeps trailing os.path.sep."""
  out = os.path.normpath(path)
  if path.endswith(os.path.sep):
    out += os.path.sep
  return out


def posix_relpath(path, root):
  """posix.relpath() that keeps trailing slash.

  It is different from relpath() since it can be used on Windows.
  """
  out = posixpath.relpath(path, root)
  if path.endswith('/'):
    out += '/'
  return out


def is_url(path):
  """Returns True if it looks like an HTTP url instead of a file path."""
  return bool(re.match(r'^https?://.+$', path))


def path_starts_with(prefix, path):
  """Returns true if the components of the path |prefix| are the same as the
  initial components of |path| (or all of the components of |path|). The paths
  must be absolute.
  """
  #assert os.path.isabs(prefix) and os.path.isabs(path)
  prefix = os.path.normpath(prefix)
  path = os.path.normpath(path)
  #assert prefix == get_native_path_case(prefix), prefix
  #assert path == get_native_path_case(path), path
  prefix = prefix.rstrip(os.path.sep) + os.path.sep
  path = path.rstrip(os.path.sep) + os.path.sep
  return path.startswith(prefix)

def ensure_command_has_abs_path(command, cwd):
  """Ensures that an isolate command uses absolute path.

  This is needed since isolate can specify a command relative to 'cwd' and
  subprocess.call doesn't consider 'cwd' when searching for executable.
  """
  if not os.path.isabs(command[0]):
    command[0] = os.path.abspath(os.path.join(cwd, command[0]))


def is_same_filesystem(path1, path2):
  """Returns True if both paths are on the same filesystem.

  This is required to enable the use of hardlinks.
  """
  assert os.path.isabs(path1), path1
  assert os.path.isabs(path2), path2
  if sys.platform == 'win32':
    # If the drive letter mismatches, assume it's a separate partition.
    # TODO(maruel): It should look at the underlying drive, a drive letter could
    # be a mount point to a directory on another drive.
    assert re.match(r'^[a-zA-Z]\:\\.*', path1), path1
    assert re.match(r'^[a-zA-Z]\:\\.*', path2), path2
    if path1[0].lower() != path2[0].lower():
      return False
  return fs.stat(path1).st_dev == fs.stat(path2).st_dev


def get_free_space(path):
  """Returns the number of free bytes.

  On POSIX platforms, this returns the free space as visible by the current
  user. On some systems, there's a percentage of the free space on the partition
  that is only accessible as the root user.
  """
  if sys.platform == 'win32':
    free_bytes = ctypes.c_ulonglong(0)
    windll.kernel32.GetDiskFreeSpaceExW(
        ctypes.c_wchar_p(path), None, None, ctypes.pointer(free_bytes))
    return free_bytes.value
  # For OSes other than Windows.
  f = fs.statvfs(path)  # pylint: disable=E1101
  return f.f_bavail * f.f_frsize


### Write file functions.


def hardlink(source, link_name):
  """Hardlinks a file.

  Add support for os.link() on Windows.
  """
  assert isinstance(source, str), source
  assert isinstance(link_name, str), link_name
  if sys.platform == 'win32':
    if not windll.kernel32.CreateHardLinkW(
        fs.extend(link_name), fs.extend(source), 0):
      raise OSError()
  else:
    fs.link(source, link_name)


def readable_copy(outfile, infile):
  """Makes a copy of the file that is readable by everyone."""
  fs.copy2(infile, outfile)
  fs.chmod(
      outfile,
      fs.stat(outfile).st_mode | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


def set_read_only(path, read_only, orig_mode=None):
  """Sets or resets the write bit on a file or directory.

  Zaps out access to 'group' and 'others'.
  """
  if orig_mode is None:
    orig_mode = fs.lstat(path).st_mode
  mode = orig_mode
  # TODO(maruel): Stop removing GO bits.
  if read_only:
    mode &= stat.S_IRUSR | stat.S_IXUSR  # 0500
  else:
    mode |= stat.S_IRUSR | stat.S_IWUSR  # 0600
    if sys.platform != 'win32' and stat.S_ISDIR(mode):
      mode |= stat.S_IXUSR  # 0100
  if hasattr(os, 'lchmod'):
    fs.lchmod(path, mode)  # pylint: disable=E1101
  else:
    if stat.S_ISLNK(orig_mode):
      # Skip symlink without lchmod() support.
      return

    # TODO(maruel): Implement proper DACL modification on Windows.
    fs.chmod(path, mode)


def set_read_only_swallow(path, read_only, orig_mode=None):
  """Returns if an OSError exception occurred."""
  try:
    set_read_only(path, read_only, orig_mode)
  except OSError as e:
    return e
  return None


def remove(filepath):
  """Removes a file, even if it is read-only."""
  # TODO(maruel): Not do it unless necessary since it slows this function
  # down.
  if sys.platform == 'win32':
    # Deleting a read-only file will fail if it is read-only.
    set_read_only(filepath, False)
  else:
    # Deleting a read-only file will fail if the directory is read-only.
    set_read_only(os.path.dirname(filepath), False)
  fs.remove(filepath)


def try_remove(filepath):
  """Removes a file without crashing even if it doesn't exist.

  Returns:
    True if the removal succeeded.
  """
  try:
    remove(filepath)
    return True
  except OSError:
    return False


def link_file(outfile, infile, action):
  """Links a file. The type of link depends on |action|.

  Returns:
    True if the action was carried on, False if fallback was used.
  """
  if action < 1 or action > COPY:
    raise ValueError('Unknown mapping action %s' % action)
  # TODO(maruel): Skip these checks.
  if not fs.isfile(infile):
    raise OSError('%s is missing' % infile)
  if fs.isfile(outfile):
    raise OSError(
        '%s already exist; insize:%d; outsize:%d' %
        (outfile, fs.stat(infile).st_size, fs.stat(outfile).st_size))

  if action == COPY:
    readable_copy(outfile, infile)
    return True

  if action in (SYMLINK, SYMLINK_WITH_FALLBACK):
    try:
      fs.symlink(infile, outfile)  # pylint: disable=E1101
      return True
    except OSError:
      if action == SYMLINK:
        raise
      logging.warning(
          'Failed to symlink, falling back to copy %s to %s' % (
            infile, outfile))
      # Signal caller that fallback copy was used.
      readable_copy(outfile, infile)
      return False

  # HARDLINK or HARDLINK_WITH_FALLBACK.
  try:
    hardlink(infile, outfile)
    return True
  except OSError as e:
    if action == HARDLINK:
      raise OSError('Failed to hardlink %s to %s: %s' % (infile, outfile, e))

  # Probably a different file system.
  logging.warning(
      'Failed to hardlink, falling back to copy %s to %s' % (
        infile, outfile))
  readable_copy(outfile, infile)
  # Signal caller that fallback copy was used.
  return False


def atomic_replace(path, body):
  """Atomically replaces content of the file at given path.

  'body' will be written to the file as is (as in open(..., 'wb') mode).

  Does not preserve file attributes.

  Raises OSError or IOError on errors (e.g. in case the file is locked on
  Windows). The caller may retry a bunch of times in such cases before giving
  up.
  """
  assert path and path[-1] != os.sep, path
  path = os.path.abspath(path)
  dir_name, base_name = os.path.split(path)

  fd, tmp_name = tempfile.mkstemp(dir=dir_name, prefix=base_name+'_')
  try:
    with os.fdopen(fd, 'wb') as f:
      f.write(body)
      f.flush()
      os.fsync(fd)
    if sys.platform != 'win32':
      os.rename(tmp_name, path)
    else:
      # Flags are MOVEFILE_REPLACE_EXISTING|MOVEFILE_WRITE_THROUGH.
      MoveFileEx(tmp_name, path, 0x1 | 0x8)
    tmp_name = None # no need to remove it in 'finally' block anymore
  finally:
    if tmp_name:
      try:
        os.remove(tmp_name)
      except OSError as e:
        logging.warning(
            'Failed to delete temp file %s in replace_file_content: %s',
            tmp_name, e)


### Write directory functions.


def ensure_tree(path, perm=0o777):
  """Ensures a directory exists."""
  if not fs.isdir(path):
    try:
      fs.makedirs(path, perm)
    except OSError as e:
      # Do not raise if directory exists.
      if e.errno != errno.EEXIST or not fs.isdir(path):
        raise


def create_directories(base_directory, files):
  """Creates the directory structure needed by the given list of files."""
  logging.debug('create_directories(%s, %d)', base_directory, len(files))
  # Creates the tree of directories to create.
  directories = set(os.path.dirname(f) for f in files)
  for item in list(directories):
    while item:
      directories.add(item)
      item = os.path.dirname(item)
  for d in sorted(directories):
    if d:
      abs_d = os.path.join(base_directory, d)
      if not fs.isdir(abs_d):
        fs.mkdir(abs_d)


def make_tree_files_read_only(root):
  """Makes all the files in the directories read only but not the directories
  themselves.

  This means files can be created or deleted.
  """
  logging.debug('make_tree_files_read_only(%s)', root)
  if sys.platform != 'win32':
    set_read_only(root, False)
  for dirpath, dirnames, filenames in fs.walk(root, topdown=True):
    for filename in filenames:
      set_read_only(os.path.join(dirpath, filename), True)
    if sys.platform != 'win32':
      # It must not be done on Windows.
      for dirname in dirnames:
        set_read_only(os.path.join(dirpath, dirname), False)


def make_tree_deleteable(root):
  """Changes the appropriate permissions so the files in the directories can be
  deleted.

  On Windows, the files are modified. On other platforms, modify the directory.
  It only does the minimum so the files can be deleted safely.

  Warning on Windows: since file permission is modified, the file node is
  modified. This means that for hard-linked files, every directory entry for the
  file node has its file permission modified.
  """
  if sys.platform == 'win32':
    make_tree_deleteable_win(root)
  else:
    make_tree_deleteable_posix(root)


def make_tree_deleteable_win(root):
  logging.debug('Using file_path.make_tree_deleteable_win')
  err = None

  dirs = deque([root])
  while dirs:
    for entry in os.scandir(dirs.popleft()):
      if entry.is_file():
        e = set_read_only_swallow(entry.path, False, entry.stat().st_mode)
        if not err:
          err = e
      if not entry.is_dir() or _is_symlink_entry(entry):
        continue
      dirs.append(entry.path)

  if err:
    # pylint: disable=raising-bad-type
    raise err


def make_tree_deleteable_posix(root):
  logging.debug('Using file_path.make_tree_deleteable_posix')
  err = None
  sudo_failed = False

  def try_sudo(p):
    if sudo_failed:
      return True
    # Try passwordless sudo, just in case. In practice, it is preferable
    # to use linux capabilities.
    with open(os.devnull, 'rb') as f:
      if not subprocess42.call(['sudo', '-n', 'chmod', 'a+rwX,-t', p], stdin=f):
        return False
    logging.debug('sudo chmod %s failed', p)
    return True

  e = set_read_only_swallow(root, False)
  if e:
    sudo_failed = try_sudo(root)
  if not err:
    err = e

  dirs = deque([root])
  while dirs:
    for entry in os.scandir(dirs.popleft()):
      if not entry.is_dir() or _is_symlink_entry(entry):
        continue
      dirs.append(entry.path)
      e = set_read_only_swallow(entry.path, False, entry.stat().st_mode)
      if e:
        sudo_failed = try_sudo(entry.path)
      if not err:
        err = e
  if err:
    # pylint: disable=raising-bad-type
    raise err


def rmtree(root):
  """Wrapper around shutil.rmtree() to retry automatically on Windows.

  On Windows, forcibly kills processes that are found to interfere with the
  deletion.

  Raises an exception if it failed.
  """
  logging.info('file_path.rmtree(%s)', root)
  assert isinstance(root, str), repr(root)

  def change_tree_permission():
    logging.debug('file_path.make_tree_deleteable(%s) starting', root)
    start = time.time()
    try:
      make_tree_deleteable(root)
    except OSError as e:
      logging.warning('Swallowing make_tree_deleteable() error: %s', e)
    logging.debug('file_path.make_tree_deleteable(%s) took %s seconds', root,
                  time.time() - start)

  # First try the soft way: tries 3 times to delete and sleep a bit in between.
  # On Windows, try 4 times with a total 12 seconds of sleep.
  # Retries help if test subprocesses outlive main process and try to actively
  # use or write to the directory while it is being deleted.
  max_tries = 4 if sys.platform == 'win32' else 3
  has_called_change_tree_permission = False
  has_called_change_acl_for_delete = False
  for i in range(max_tries):
    # pylint: disable=cell-var-from-loop
    # errors is a list of tuple(function, path, excinfo).
    errors = []
    logging.debug('file_path.rmtree(%s) try=%d', root, i)
    start = time.time()
    fs.rmtree(root, onerror=lambda *args: errors.append(args))
    logging.debug('file_path.rmtree(%s) try=%d took %s seconds', root, i,
                  time.time() - start)
    if not errors or not fs.exists(root):
      if i:
        logging.debug('Succeeded.\n')
      return

    # Try to change tree permission.
    if not has_called_change_tree_permission:
      logging.warning(
          'Failed to delete %s (%d files remaining).\n'
          '  Maybe tree permission needs to be changed.\n', root, len(errors))
      change_tree_permission()
      has_called_change_tree_permission = True
      # do not sleep here.
      continue

    # Try change_acl_for_delete on Windows.
    if not has_called_change_acl_for_delete and sys.platform == 'win32':
      for path in sorted(set(path for _, path, _ in errors)):
        try:
          change_acl_for_delete(path)
        except Exception as e:
          logging.error('- %s (failed to update ACL: %s)\n', path, e)
      has_called_change_acl_for_delete = True

    if i < max_tries - 1:
      delay = (i+1)*2
      logging.warning(
          'Failed to delete %s (%d files remaining).\n'
          '  Maybe the test has a subprocess outliving it.\n'
          '  Sleeping %d seconds.\n', root, len(errors), delay)
      time.sleep(delay)

  logging.error('Failed to delete %s. The following files remain:\n', root)
  # The same path may be listed multiple times.
  for path in sorted(set(path for _, path, _ in errors)):
    logging.error('- %s\n', path)

  # If soft retries fail on Linux, there's nothing better we can do.
  if sys.platform != 'win32':
    raise errors[0][2][1]

  # The soft way was not good enough. Try the hard way.
  start = time.time()
  logging.debug('file_path.rmtree(%s) killing children processes', root)
  for i in range(max_tries):
    if not kill_children_processes(root):
      break
    if i != max_tries - 1:
      time.sleep((i+1)*2)
  else:
    processes = _get_children_processes_win(root)
    if processes:
      logging.error('Failed to terminate processes.\n')
      raise errors[0][2][1]
  logging.debug(
      'file_path.rmtree(%s) killing children processes took %d seconds', root,
      time.time() - start)

  # Now that annoying processes in root are evicted, try again.
  start = time.time()
  errors = []
  logging.debug('file_path.rmtree(%s) final try', root)
  fs.rmtree(root, onerror=lambda *args: errors.append(args))
  logging.debug('file_path.rmtree(%s) final try took %d seconds', root,
                time.time() - start)
  if not errors or not fs.exists(root):
    logging.debug('Succeeded at final try.\n')
    return

  # There's no hope: the directory was tried to be removed 4 times. Give up
  # and raise an exception.
  logging.error('Failed to delete %s. The following files remain:\n', root)
  # The same path may be listed multiple times.
  for path in sorted(set(path for _, path, _ in errors)):
    logging.error('- %s\n', path)
  raise errors[0][2][1]


def get_recursive_size(path):
  # type: (str) -> int
  """Returns the total data size for the specified path.

  This function can be surprisingly slow on OSX, so its output should be cached.
  """
  start = time.time()
  try:
    total, n_dirs, n_files, n_links, n_others = _get_recur_size_with_scandir(
        path)
    elapsed = time.time() - start
    logging.debug(
        '_get_recursive_size: traversed %s took %s seconds. '
        'files: %d, links: %d, dirs: %d, others: %d', path, elapsed, n_files,
        n_links, n_dirs, n_others)
    return total
  except (IOError, OSError, UnicodeEncodeError):
    logging.exception('Exception while getting the size of %s', path)
    return None


## Private code.


def _is_symlink_entry(entry):
  if entry.is_symlink():
    return True
  if sys.platform != 'win32':
    return False
  # both st_file_attributes and FILE_ATTRIBUTE_REPARSE_POINT are
  # windows-only symbols.
  return bool(entry.stat().st_file_attributes
              & stat.FILE_ATTRIBUTE_REPARSE_POINT)


def _get_recur_size_with_scandir(path):
  total = 0
  n_dirs = 0
  n_files = 0
  n_links = 0
  n_others = 0
  stack = [path]
  while stack:
    try:
      for entry in os.scandir(stack.pop()):
        if _is_symlink_entry(entry):
          n_links += 1
          continue
        if entry.is_file():
          n_files += 1
          total += entry.stat().st_size
        elif entry.is_dir():
          n_dirs += 1
          stack.append(entry.path)
        else:
          n_others += 1
          logging.warning('non directory/file entry: %s', entry)
    except PermissionError:
      logging.warning('Failed to scan directory', exc_info=True)
  return total, n_dirs, n_files, n_links, n_others
