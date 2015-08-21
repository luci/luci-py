# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Windows specific utility functions."""

import ctypes
import logging
import os
import platform
import re
import string
import sys

from utils import tools


@tools.cached
def _get_mount_points():
  """Returns the list of 'fixed' drives in format 'X:\\'."""
  ctypes.windll.kernel32.GetDriveTypeW.argtypes = (ctypes.c_wchar_p,)
  ctypes.windll.kernel32.GetDriveTypeW.restype = ctypes.c_ulong
  DRIVE_FIXED = 3
  # https://msdn.microsoft.com/library/windows/desktop/aa364939.aspx
  return [
    letter + ':\\'
    for letter in string.lowercase
    if ctypes.windll.kernel32.GetDriveTypeW(letter + ':\\') == DRIVE_FIXED
  ]


def _get_disk_info(mount_point):
  """Returns total and free space on a mount point in Mb."""
  total_bytes = ctypes.c_ulonglong(0)
  free_bytes = ctypes.c_ulonglong(0)
  ctypes.windll.kernel32.GetDiskFreeSpaceExW(
      ctypes.c_wchar_p(mount_point), None, ctypes.pointer(total_bytes),
      ctypes.pointer(free_bytes))
  return {
    'free_mb': round(free_bytes.value / 1024. / 1024., 1),
    'size_mb': round(total_bytes.value / 1024. / 1024., 1),
  }


### Public API.


def from_cygwin_path(path):
  """Converts an absolute cygwin path to a standard Windows path."""
  if not path.startswith('/cygdrive/'):
    logging.error('%s is not a cygwin path', path)
    return None

  # Remove the cygwin path identifier.
  path = path[len('/cygdrive/'):]

  # Add : after the drive letter.
  path = path[:1] + ':' + path[1:]
  return path.replace('/', '\\')


def to_cygwin_path(path):
  """Converts an absolute standard Windows path to a cygwin path."""
  if len(path) < 2 or path[1] != ':':
    # TODO(maruel): Accept \\?\ and \??\ if necessary.
    logging.error('%s is not a win32 path', path)
    return None
  return '/cygdrive/%s/%s' % (path[0].lower(), path[3:].replace('\\', '/'))


@tools.cached
def get_os_version_number():
  """Returns the normalized OS version number as a string.

  Returns:
    - 5.1, 6.1, etc. There is no way to distinguish between Windows 7
      and Windows Server 2008R2 since they both report 6.1.
  """
  if sys.platform == 'win32':
    version_raw = platform.version()
    version_parts = version_raw.split('.')
  else:
    # This handles 'CYGWIN_NT-5.1' and 'CYGWIN_NT-6.1-WOW64'.
    version_raw = platform.system()
    version_parts = version_raw.split('-')[1].split('.')
  assert len(version_parts) >= 2,  (
      'Unable to determine Windows version: %s' % version_raw)
  if version_parts[0] < 5 or (version_parts[0] == 5 and version_parts[1] < 1):
    assert False, 'Version before XP are unsupported: %s' % version_parts
  return u'.'.join(version_parts[:2])


@tools.cached
def get_os_version_name():
  """Returns the marketing name of the OS including the service pack."""
  marketing_name = platform.uname()[2]
  service_pack = platform.win32_ver()[2] or 'SP0'
  return '%s-%s' % (marketing_name, service_pack)


def get_startup_dir():
  # Do not use environment variables since it wouldn't work reliably on cygwin.
  # TODO(maruel): Stop hardcoding the values and use the proper function
  # described below. Postponed to a later CL since I'll have to spend quality
  # time on Windows to ensure it works well.
  # https://msdn.microsoft.com/library/windows/desktop/bb762494.aspx
  # CSIDL_STARTUP = 7
  # https://msdn.microsoft.com/library/windows/desktop/bb762180.aspx
  # shell.SHGetFolderLocation(NULL, CSIDL_STARTUP, NULL, NULL, string)
  if get_os_version_number() == u'5.1':
    startup = 'Start Menu\\Programs\\Startup'
  else:
    # Vista+
    startup = (
        'AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\Startup')

  # On cygwin 1.5, which is still used on some bots, '~' points inside
  # c:\\cygwin\\home so use USERPROFILE.
  return '%s\\%s\\' % (
    os.environ.get('USERPROFILE', 'DUMMY, ONLY USED IN TESTS'), startup)


def get_disks_info():
  """Returns disk infos on all mount point in Mb."""
  return dict((p, _get_disk_info(p)) for p in _get_mount_points())


@tools.cached
def get_gpu():
  """Returns video device as listed by WMI. See get_gpu()."""
  try:
    import win32com.client  # pylint: disable=F0401
  except ImportError:
    # win32com is included in pywin32, which is an optional package that is
    # installed by Swarming devs. If you find yourself needing it to run without
    # pywin32, for example in cygwin, please send us a CL with the
    # implementation that doesn't use pywin32.
    return None, None

  wmi_service = win32com.client.Dispatch('WbemScripting.SWbemLocator')
  wbem = wmi_service.ConnectServer('.', 'root\\cimv2')
  dimensions = set()
  state = set()
  # https://msdn.microsoft.com/library/aa394512.aspx
  for device in wbem.ExecQuery('SELECT * FROM Win32_VideoController'):
    vp = device.VideoProcessor
    if vp:
      state.add(vp)

    # The string looks like:
    #  PCI\VEN_15AD&DEV_0405&SUBSYS_040515AD&REV_00\3&2B8E0B4B&0&78
    pnp_string = device.PNPDeviceID
    ven_id = u'UNKNOWN'
    dev_id = u'UNKNOWN'
    match = re.search(r'VEN_([0-9A-F]{4})', pnp_string)
    if match:
      ven_id = match.group(1).lower()
    match = re.search(r'DEV_([0-9A-F]{4})', pnp_string)
    if match:
      dev_id = match.group(1).lower()
    dimensions.add(unicode(ven_id))
    dimensions.add(u'%s:%s' % (ven_id, dev_id))
  return sorted(dimensions), sorted(state)
