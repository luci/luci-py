#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""OS specific utility functions.

Includes code:
- to declare the current system this code is running under.
- to run a command on user login.
- to restart the host.
"""

import cgi
import ctypes
import logging
import multiprocessing
import os
import platform
import pprint
import re
import shlex
import socket
import subprocess
import sys
import time

try:
  import zipped_archive

  THIS_FILE = os.path.abspath(zipped_archive.get_main_script_path() or __file__)
except ImportError:
  THIS_FILE = os.path.abspath(__file__)

ROOT_DIR = os.path.dirname(THIS_FILE)


# Properties from an android device that should be kept as dimension.
ANDROID_DETAILS = frozenset(
    [
      # Hardware details.
      'ro.board.platform',
      'ro.product.board',  # or ro.product.device?
      'ro.product.cpu.abi',
      'ro.product.cpu.abi2',

      # OS details.
      'ro.build.id',
      'ro.build.tags',
      'ro.build.type',
      'ro.build.version.sdk',
    ])


### Private stuff.


def _write(filepath, content):
  """Writes out a file and returns True on success."""
  logging.info('Writing in %s:\n%s', filepath, content)
  try:
    with open(filepath, mode='wb') as f:
      f.write(content)
    return True
  except IOError as e:
    logging.error('Failed to write %s: %s', filepath, e)
    return False


def _from_cygwin_path(path):
  """Converts an absolute cygwin path to a standard Windows path."""
  if not path.startswith('/cygdrive/'):
    logging.error('%s is not a cygwin path', path)
    return None

  # Remove the cygwin path identifier.
  path = path[len('/cygdrive/'):]

  # Add : after the drive letter.
  path = path[:1] + ':' + path[1:]
  return path.replace('/', '\\')


def _to_cygwin_path(path):
  """Converts an absolute standard Windows path to a cygwin path."""
  if len(path) < 2 or path[1] != ':':
    # TODO(maruel): Accept \\?\ and \??\ if necessary.
    logging.error('%s is not a win32 path', path)
    return None
  return '/cygdrive/%s/%s' % (path[0].lower(), path[3:].replace('\\', '/'))


def _get_startup_dir_win():
  # Do not use environment variables since it wouldn't work reliably on cygwin.
  # TODO(maruel): Stop hardcoding the values and use the proper function
  # described below. Postponed to a later CL since I'll have to spend quality
  # time on Windows to ensure it works well.
  # https://msdn.microsoft.com/library/windows/desktop/bb762494.aspx
  # CSIDL_STARTUP = 7
  # https://msdn.microsoft.com/library/windows/desktop/bb762180.aspx
  # shell.SHGetFolderLocation(NULL, CSIDL_STARTUP, NULL, NULL, string)
  if get_os_version() == '5.1':
    startup = 'Start Menu\\Programs\\Startup'
  else:
    # Vista+
    startup = (
        'AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\Startup')

  # On cygwin 1.5, which is still used on some slaves, '~' points inside
  # c:\\cygwin\\home so use USERPROFILE.
  return '%s\\%s\\' % (
    os.environ.get('USERPROFILE', 'DUMMY, ONLY USED IN TESTS'), startup)


def _generate_launchd_plist(command, cwd, plistname):
  """Generates a plist content with the corresponding command for launchd."""
  # The documentation is available at:
  # https://developer.apple.com/library/mac/documentation/Darwin/Reference/ \
  #    ManPages/man5/launchd.plist.5.html
  escaped_plist = cgi.escape(plistname)
  entries = [
    '<key>Label</key><string>%s</string>' % escaped_plist,
    '<key>StandardOutPath</key><string>%s.log</string>' % escaped_plist,
    '<key>StandardErrorPath</key><string>%s-err.log</string>' % escaped_plist,
    '<key>LimitLoadToSessionType</key><array><string>Aqua</string></array>',
    '<key>RunAtLoad</key><true/>',
    '<key>Umask</key><integer>18</integer>',

    '<key>EnvironmentVariables</key>',
    '<dict>',
    '  <key>PATH</key>',
    # TODO(maruel): Makes it configurable if necessary.
    '  <string>/opt/local/bin:/opt/local/sbin:/usr/local/sbin:/usr/local/bin'
      ':/usr/sbin:/usr/bin:/sbin:/bin</string>',
    '</dict>',

    '<key>SoftResourceLimits</key>',
    '<dict>',
    '  <key>NumberOfFiles</key>',
    '  <integer>8000</integer>',
    '</dict>',
  ]
  entries.append(
      '<key>Program</key><string>%s</string>' % cgi.escape(command[0]))
  entries.append('<key>ProgramArguments</key>')
  entries.append('<array>')
  # Command[0] must be passed as an argument.
  entries.extend('  <string>%s</string>' % cgi.escape(i) for i in command)
  entries.append('</array>')
  entries.append(
      '<key>WorkingDirectory</key><string>%s</string>' % cgi.escape(cwd))
  header = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
    '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
    '<plist version="1.0">\n'
    '  <dict>\n'
    + ''.join('    %s\n' % l for l in entries) +
    '  </dict>\n'
    '</plist>\n')
  return header


def _get_gpu_linux():
  """Returns video device as listed by 'lspci'. See get_gpu()."""
  try:
    pci_devices = subprocess.check_output(['lspci', '-mm', '-nn']).splitlines()
  except subprocess.CalledProcessError as e:
    logging.error('Failed to run lspci: %s', e)
    return ['ERROR']

  out = set()
  re_id = re.compile(r'^(.+?) \[([0-9a-f]{4})\]$')
  for pci_device in pci_devices:
    # Bus, Type, Vendor [ID], Device [ID], extra...
    line = shlex.split(pci_device)
    # Look for display class as noted at http://wiki.osdev.org/PCI
    dev_type = re_id.match(line[1]).group(2)
    if dev_type.startswith('03'):
      vendor = re_id.match(line[2])
      device = re_id.match(line[3])
      ven_id = vendor.group(2)
      out.add(ven_id)
      out.add('%s:%s' % (ven_id, device.group(2)))
      out.add('%s %s' % (vendor.group(1), device.group(1)))
  return sorted(out)


def _get_gpu_osx():
  """Returns video device as listed by 'system_profiler'. See get_gpu()."""
  import plistlib
  sp = subprocess.check_output(
      ['system_profiler', 'SPDisplaysDataType', '-xml'])
  pl = plistlib.readPlistFromString(sp)
  try:
    out = set()
    for card in pl[0]['_items']:
      # Warning: the value provided depends on the driver manufacturer.
      # Other interesting values: spdisplays_vram, spdisplays_revision-id
      ven_id = 'UNKNOWN'
      if 'spdisplays_vendor-id' in card:
        # NVidia
        ven_id = card['spdisplays_vendor-id'][2:]
      elif 'spdisplays_vendor' in card:
        # Intel and ATI
        match = re.search(r'\(0x([0-9a-f]{4})\)', card['spdisplays_vendor'])
        if match:
          ven_id = match.group(1)
      dev_id = card['spdisplays_device-id'][2:]
      out.add(ven_id)
      out.add('%s:%s' % (ven_id, dev_id))

      # VMWare doesn't set it.
      if 'sppci_model' in card:
        out.add(card['sppci_model'])
    return sorted(out)
  except KeyError:
    return ['ERROR']


def _get_gpu_win():
  """Returns video device as listed by WMI. See get_gpu()."""
  try:
    import win32com.client  # pylint: disable=F0401
  except ImportError:
    # win32com is included in pywin32, which is an optional package that is
    # installed by Swarming devs. If you find yourself needing it to run without
    # pywin32, for example in cygwin, please send us a CL with the
    # implementation that doesn't use pywin32.
    return ['ERROR']

  wmi_service = win32com.client.Dispatch('WbemScripting.SWbemLocator')
  wbem = wmi_service.ConnectServer('.', 'root\\cimv2')
  out = set()
  # https://msdn.microsoft.com/library/aa394512.aspx
  for device in wbem.ExecQuery('SELECT * FROM Win32_VideoController'):
    vp = device.VideoProcessor
    if vp:
      out.add(vp)

    # The string looks like:
    #  PCI\VEN_15AD&DEV_0405&SUBSYS_040515AD&REV_00\3&2B8E0B4B&0&78
    pnp_string = device.PNPDeviceID
    ven_id = 'UNKNOWN'
    dev_id = 'UNKNOWN'
    match = re.search(r'VEN_([0-9A-F]{4})', pnp_string)
    if match:
      ven_id = match.group(1).lower()
    match = re.search(r'DEV_([0-9A-F]{4})', pnp_string)
    if match:
      dev_id = match.group(1).lower()
    out.add(ven_id)
    out.add('%s:%s' % (ven_id, dev_id))
  return sorted(out)


### Public API.


def get_os_version():
  """Returns the normalized OS version as a string.

  Returns:
    The format depends on the OS:
    - Windows: 5.1, 6.1, etc.
    - OSX: 10.7, 10.8, etc.
    - Ubuntu: 12.04, 10.04, etc.
    Others will return None.
  """
  if sys.platform in ('cygwin', 'win32'):
    version = (
        platform.system() if sys.platform == 'cygwin' else platform.version())
    if '-' in version:
      version = version.split('-')[1]
    version_parts = version.split('.')
    assert len(version_parts) >= 2,  'Unable to determine Windows version'
    if version_parts[0] < 5 or (version_parts[0] == 5 and version_parts[1] < 1):
      assert False, 'Version before XP are unsupported: %s' % version_parts
    return '.'.join(version_parts[:2])

  if sys.platform == 'darwin':
    version_parts = platform.mac_ver()[0].split('.')
    assert len(version_parts) >= 2, 'Unable to determine Mac version'
    return '.'.join(version_parts[:2])

  if sys.platform == 'linux2':
    # Assumes Ubuntu here. Improve if needed. No need to convert the Ubuntu
    # value since it already returns what we want like '12.04' or '10.04'.
    distro_details = platform.linux_distribution()
    assert distro_details[0] == 'Ubuntu', distro_details
    return distro_details[1]

  logging.error('Unable to determine platform version')
  return None


def get_os_name():
  """Returns standardized OS name.

  Defaults to sys.platform for OS not normalized.

  TODO(maruel): Differentiate between linux distros like Ubuntu or debian.
  """
  return {
    'cygwin': 'Windows',
    'darwin': 'Mac',
    'linux2': 'Linux',
    'win32': 'Windows',
  }.get(sys.platform, sys.platform)


def get_cpu_type():
  """Returns the type of processor: arm or x86."""
  machine = platform.machine().lower()
  if machine in ('amd64', 'x86_64', 'i386'):
    return 'x86'
  return machine


def get_cpu_bitness():
  """Returns the number of bits in the CPU architecture as a str: 32 or 64.

  Unless someone ported python to PDP-10 or 286.

  Note: this function may return 32 bits on 64 bits OS in case of a 32 bits
  python process.
  """
  if platform.machine().endswith('64'):
    return '64'
  # TODO(maruel): Work harder to figure out if OS is 64 bits.
  return '64' if sys.maxsize > 2**32 else '32'


def get_ip():
  """Returns the IP that is the most likely to be used for TCP connections."""
  # Tries for ~0.5s then give up.
  max_tries = 10
  for i in xrange(10):
    # It's guesswork and could return the wrong IP. In particular a host can
    # have multiple IPs.
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # This doesn't actually connect to the Google DNS server but this forces the
    # network system to figure out an IP interface to use.
    try:
      s.connect(('8.8.8.8', 80))
      return s.getsockname()[0]
    except socket.error:
      # Can raise "error: [Errno 10051] A socket operation was attempted to an
      # unreachable network" if the network is still booting up. We don't want
      # this function to crash.
      if i == max_tries - 1:
        # Can't determine the IP.
        return '0.0.0.0'
      time.sleep(0.05)
    finally:
      s.close()


def get_hostname():
  """Returns the machine's hostname."""
  # Windows enjoys putting random case in there. Enforces lower case for sanity.
  hostname = socket.getfqdn().lower()
  if hostname.endswith('.in-addr.arpa'):
    # The base name will be the IPv4 address reversed, which is not useful. This
    # happens on OSX.
    hostname = socket.gethostname()
  return hostname


def get_hostname_short():
  """Returns the base host name."""
  return get_hostname().split('.', 1)[0]


def get_num_processors():
  """Returns the number of processors.

  Python on OSX 10.6 raises a NotImplementedError exception.
  """
  try:
    # Multiprocessing
    return multiprocessing.cpu_count()
  except:  # pylint: disable=W0702
    try:
      # Mac OS 10.6
      return int(os.sysconf('SC_NPROCESSORS_ONLN'))  # pylint: disable=E1101
    except:
      logging.error('get_num_processors() failed to query number of cores')
      return 0


def get_physical_ram():
  """Returns the amount of installed RAM, rounded to the nearest number."""
  if sys.platform == 'win32':
    # https://msdn.microsoft.com/library/windows/desktop/aa366589.aspx
    class MemoryStatusEx(ctypes.Structure):
      _fields_ = [
        ('dwLength', ctypes.c_ulong),
        ('dwMemoryLoad', ctypes.c_ulong),
        ('dwTotalPhys', ctypes.c_ulonglong),
        ('dwAvailPhys', ctypes.c_ulonglong),
        ('dwTotalPageFile', ctypes.c_ulonglong),
        ('dwAvailPageFile', ctypes.c_ulonglong),
        ('dwTotalVirtual', ctypes.c_ulonglong),
        ('dwAvailVirtual', ctypes.c_ulonglong),
        ('dwAvailExtendedVirtual', ctypes.c_ulonglong),
      ]
    stat = MemoryStatusEx()
    stat.dwLength = ctypes.sizeof(MemoryStatusEx)  # pylint: disable=W0201
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
    return int(round(stat.dwTotalPhys / 1024. / 1024. / 1024.))

  if sys.platform == 'darwin':
    CTL_HW = 6
    HW_MEMSIZE = 24
    result = ctypes.c_uint64(0)
    arr = (ctypes.c_int * 2)()
    arr[0] = CTL_HW
    arr[1] = HW_MEMSIZE
    size = ctypes.c_size_t(ctypes.sizeof(result))
    ctypes.cdll.LoadLibrary("libc.dylib")
    libc = ctypes.CDLL("libc.dylib")
    libc.sysctl(
        arr, 2, ctypes.byref(result), ctypes.byref(size), None,
        ctypes.c_size_t(0))
    return int(round(result.value / 1024. / 1024. / 1024.))

  if os.path.isfile('/proc/meminfo'):
    # linux.
    with open('/proc/meminfo') as f:
      meminfo = f.read()
    matched = re.search(r'^MemTotal:\s+(\d+) kB', meminfo)
    if matched:
      return int(round(int(matched.groups()[0]) / 1024. / 1024.))

  logging.error('get_physical_ram() failed to query amount of physical RAM')
  return 0


def get_free_disk():
  """Gets free disk space in the partition containing the current file in GB."""
  if sys.platform == 'win32':
    free_bytes = ctypes.c_ulonglong(0)
    ctypes.windll.kernel32.GetDiskFreeSpaceExW(
        ctypes.c_wchar_p(ROOT_DIR), None, None, ctypes.pointer(free_bytes))
    return int(round(free_bytes.value / 1024. / 1024. / 1024.))

  # For OSes other than Windows.
  f = os.statvfs(ROOT_DIR)  # pylint: disable=E1101
  return int(round(f.f_bfree * f.f_frsize / 1024. / 1024. / 1024.))


def get_gpu():
  """Returns the installed video card(s) name.

  Returns:
    All the video cards detected.

  TODO(maruel): Add custom processing to normalize the string as much as
  possible but differences will occur between OSes.
  """
  if sys.platform == 'darwin':
    return _get_gpu_osx()
  if sys.platform == 'linux2':
    return _get_gpu_linux()
  if sys.platform == 'win32':
    return _get_gpu_win()
  return ['IMPLEMENT_ME']


### Windows.


def get_integrity_level_win():
  """Returns the integrity level of the current process as a string.

  TODO(maruel): It'd be nice to make it work on cygwin. The problem is that
  ctypes.windll is unaccessible and it is not known to the author how to use
  stdcall convention through ctypes.cdll.
  """
  if sys.platform != 'win32':
    return None

  mapping = {
    0x0000: 'untrusted',
    0x1000: 'low',
    0x2000: 'medium',
    0x2100: 'medium high',
    0x3000: 'high',
    0x4000: 'system',
    0x5000: 'protected process',
  }

  # This was specifically written this way to work on cygwin except for the
  # windll part. If someone can come up with a way to do stdcall on cygwin, that
  # would be appreciated.
  BOOL = ctypes.c_long
  DWORD = ctypes.c_ulong
  HANDLE = ctypes.c_void_p
  class SID_AND_ATTRIBUTES(ctypes.Structure):
    _fields_ = [
      ('Sid', ctypes.c_void_p),
      ('Attributes', DWORD),
    ]

  class TOKEN_MANDATORY_LABEL(ctypes.Structure):
    _fields_ = [
      ('Label', SID_AND_ATTRIBUTES),
    ]

  TOKEN_READ = DWORD(0x20008)
  # Use the same casing as in the C declaration:
  # https://msdn.microsoft.com/library/windows/desktop/aa379626.aspx
  TokenIntegrityLevel = ctypes.c_int(25)
  ERROR_INSUFFICIENT_BUFFER = 122

  # All the functions used locally. First open the process' token, then query
  # the SID to know its integrity level.
  ctypes.windll.kernel32.GetLastError.argtypes = ()
  ctypes.windll.kernel32.GetLastError.restype = DWORD
  ctypes.windll.kernel32.GetCurrentProcess.argtypes = ()
  ctypes.windll.kernel32.GetCurrentProcess.restype = ctypes.c_void_p
  ctypes.windll.advapi32.OpenProcessToken.argtypes = (
      HANDLE, DWORD, ctypes.POINTER(HANDLE))
  ctypes.windll.advapi32.OpenProcessToken.restype = BOOL
  ctypes.windll.advapi32.GetTokenInformation.argtypes = (
      HANDLE, ctypes.c_long, ctypes.c_void_p, DWORD, ctypes.POINTER(DWORD))
  ctypes.windll.advapi32.GetTokenInformation.restype = BOOL
  ctypes.windll.advapi32.GetSidSubAuthorityCount.argtypes = [ctypes.c_void_p]
  ctypes.windll.advapi32.GetSidSubAuthorityCount.restype = ctypes.POINTER(
      ctypes.c_ubyte)
  ctypes.windll.advapi32.GetSidSubAuthority.argtypes = (ctypes.c_void_p, DWORD)
  ctypes.windll.advapi32.GetSidSubAuthority.restype = ctypes.POINTER(DWORD)

  # First open the current process token, query it, then close everything.
  token = ctypes.c_void_p()
  proc_handle = ctypes.windll.kernel32.GetCurrentProcess()
  if not ctypes.windll.advapi32.OpenProcessToken(
      proc_handle,
      TOKEN_READ,
      ctypes.byref(token)):
    logging.error('Failed to get process\' token')
    return None
  if token.value == 0:
    logging.error('Got a NULL token')
    return None
  try:
    # The size of the structure is dynamic because the TOKEN_MANDATORY_LABEL
    # used will have the SID appened right after the TOKEN_MANDATORY_LABEL in
    # the heap allocated memory block, with .Label.Sid pointing to it.
    info_size = DWORD()
    if ctypes.windll.advapi32.GetTokenInformation(
        token,
        TokenIntegrityLevel,
        ctypes.c_void_p(),
        info_size,
        ctypes.byref(info_size)):
      logging.error('GetTokenInformation() failed expectation')
      return None
    if info_size.value == 0:
      logging.error('GetTokenInformation() returned size 0')
      return None
    if ctypes.windll.kernel32.GetLastError() != ERROR_INSUFFICIENT_BUFFER:
      logging.error(
          'GetTokenInformation(): Unknown error: %d',
          ctypes.windll.kernel32.GetLastError())
      return None
    token_info = TOKEN_MANDATORY_LABEL()
    ctypes.resize(token_info, info_size.value)
    if not ctypes.windll.advapi32.GetTokenInformation(
        token,
        TokenIntegrityLevel,
        ctypes.byref(token_info),
        info_size,
        ctypes.byref(info_size)):
      logging.error(
          'GetTokenInformation(): Unknown error with buffer size %d: %d',
          info_size.value,
          ctypes.windll.kernel32.GetLastError())
      return None
    p_sid_size = ctypes.windll.advapi32.GetSidSubAuthorityCount(
        token_info.Label.Sid)
    res = ctypes.windll.advapi32.GetSidSubAuthority(
        token_info.Label.Sid, p_sid_size.contents.value - 1)
    value = res.contents.value
    return mapping.get(value) or '0x%04x' % value
  finally:
    ctypes.windll.kernel32.CloseHandle(token)


### Android.


def get_adb_list_devices(adb_path='adb'):
  """Returns the list of devices available. This includes emulators."""
  output = subprocess.check_output([adb_path, 'devices'])
  devices = []
  for line in output.splitlines():
    if line.startswith(('*', 'List of')) or not line:
      continue
    # TODO(maruel): Handle 'offline', 'device' and 'no device'.
    devices.append(line.split()[0])
  return devices


def get_adb_device_properties_raw(device_id, adb_path='adb'):
  """Returns the system properties for a device."""
  output = subprocess.check_output(
      [adb_path, '-s', device_id, 'shell', 'cat', '/system/build.prop'])
  properties = {}
  for line in output.splitlines():
    if line.startswith('#') or not line:
      continue
    key, value = line.split('=', 1)
    properties[key] = value
  return properties


def get_dimensions_android(device_id, adb_path='adb'):
  """Returns the default dimensions for an android device.

  In this case, details are about the device, not about the host.
  """
  properties = get_adb_device_properties_raw(device_id, adb_path)
  return dict((k, v) for k, v in properties.iteritems() if k in ANDROID_DETAILS)


def get_attributes_android(device_id, adb_path='adb'):
  """Returns the default Swarming dictionary of attributes for this android
  device.
  """
  dimensions = get_dimensions_android(device_id, adb_path)
  # Also add the id as a dimension, so it's possible to trigger a task precisely
  # by device id, independent of things like hostname, especially in the case
  # where a single host runs multiple Swarming bots for multiple android
  # devices.
  dimensions['id'] = device_id
  return {
    'dimensions': dimensions,
    'id': device_id,
    # This is the IP of the host, not the device.
    'ip': get_ip(),
  }


###


def get_dimensions():
  """Returns the default dimensions."""
  os_name = get_os_name()
  cpu_type = get_cpu_type()
  dimensions = {
    'cores': str(get_num_processors()),
    'cpu': [
      cpu_type,
      cpu_type + '-' + get_cpu_bitness(),
    ],
    'disk': str(get_free_disk()),
    'gpu': get_gpu(),
    'hostname': get_hostname(),
    'os': [
      os_name,
      os_name + '-' + get_os_version(),
    ],
    'ram': str(get_physical_ram()),
  }
  if sys.platform in ('cygwin', 'win32'):
    dimensions['cygwin'] = str(int(sys.platform == 'cygwin'))
  if sys.platform == 'win32':
    dimensions['integrity'] = get_integrity_level_win()
  return dimensions


def get_attributes(id_tag):
  """Returns the default Swarming dictionary of attributes for this bot.

  'id' is used to uniquely identify the bot.
  'dimensions' is used for task selection.
  """
  dimensions = get_dimensions()
  id_tag = id_tag if id_tag else get_hostname_short()
  # Also add the id as a dimension, so it's possible to trigger a task precisely
  # by id, independent of things like hostname, especially in the case where a
  # single host runs multiple Swarming bot.
  dimensions['id'] = id_tag
  return {
    'dimensions': dimensions,
    'id': id_tag,
    'ip': get_ip(),
  }


def setup_auto_startup_win(command, cwd, batch_name):
  """Uses Startup folder in the Start Menu.

  This assumes the user is automatically logged in on OS startup.

  Works both inside cygwin's python or native python which makes this function a
  bit more tricky than necessary.

  Use the start up menu instead of registry for two reasons:
  - It's easy to remove in case of failure, for example in case of reboot loop.
  - It works well even with cygwin.

  TODO(maruel): This function assumes |command| is python script to be run.
  """
  logging.info('setup_auto_startup_win(%s, %s, %s)', command, cwd, batch_name)
  assert batch_name.endswith('.bat'), batch_name
  batch_path = _get_startup_dir_win() + batch_name

  # If we are running through cygwin, the path to write to must be changed to be
  # in the cywgin format, but we also need to change the commands to be in
  # non-cygwin format (since they will execute in a batch file).
  if sys.platform == 'cygwin':
    batch_path = _to_cygwin_path(batch_path)
    assert batch_path
    cwd = _from_cygwin_path(cwd)
    assert cwd

    # Convert all the cygwin paths in the command.
    for i in range(len(command)):
      if '/cygdrive/' in command[i]:
        command[i] = _from_cygwin_path(command[i])

  # TODO(maruel): Shell escape! Sadly shlex.quote() is only available starting
  # python 3.3 and it's tricky on Windows with '^'.
  # Don't forget the CRLF, otherwise cmd.exe won't process it.
  content = (
      '@echo off\r\n'
      ':: This file was generated automatically by os_utilities.py.\r\n'
      'cd /d %s\r\n'
      '%s 1>> swarming_bot_out.log 2>&1\r\n') % (cwd, ' '.join(command))
  return _write(batch_path, content)


def setup_auto_startup_osx(command, cwd, plistname):
  """Uses launchd to start the command when the user logs in.

  This assumes the user is automatically logged in on OS startup.

  In case of failure like reboot loop, simply remove the file in
  ~/Library/LaunchAgents/.
  """
  logging.info('setup_auto_startup_osx(%s, %s, %s)', command, cwd, plistname)
  assert plistname.endswith('.plist'), plistname
  launchd_dir = os.path.expanduser('~/Library/LaunchAgents')
  if not os.path.isdir(launchd_dir):
    # This directory doesn't exist by default.
    os.mkdir(launchd_dir)
  filepath = os.path.join(launchd_dir, plistname)
  return _write(filepath, _generate_launchd_plist(command, cwd, plistname))


def restart():
  """Restarts this machine.

  If it fails to reboot the host, it loops. This function never return.
  """
  while True:
    restart_and_return()
    # Sleep for 300 seconds to ensure we don't try to do anymore work while the
    # OS is preparing to shutdown.
    time.sleep(300)


def restart_and_return():
  """Tries to restart this host and immediately return to the caller.

  This is mostly useful when done via remote shell, like via ssh, where it is
  not worth waiting for the TCP connection to tear down.

  Returns:
    True if at least one command succeeded.
  """
  if sys.platform == 'win32':
    cmds = [
      ['shutdown', '-r', '-f', '-t', '1'],
    ]
  elif sys.platform == 'cygwin':
    # The one that will succeed depends if it is executed via a prompt or via
    # a ssh command. #itscomplicated.
    cmds = [
      ['shutdown', '-r', '-f', '-t', '1'],
      ['shutdown', '-r', '-f', '1'],
    ]
  elif sys.platform == 'linux2' or sys.platform == 'darwin':
    cmds = [['sudo', '/sbin/shutdown', '-r', 'now']]
  else:
    cmds = [['sudo', 'shutdown', '-r', 'now']]

  success = False
  for cmd in cmds:
    logging.info('Restarting machine with command %s', ' '.join(cmd))
    try:
      subprocess.check_call(cmd)
    except (OSError, subprocess.CalledProcessError) as e:
      logging.error('Failed to run %s: %s', ' '.join(cmd), e)
    else:
      success = True
  return success


def main():
  """Prints out the output of get_attributes()."""
  # Pass an empty tag, so pop it up since it has no significance.
  attribs = get_attributes(None)
  pprint.pprint(attribs)
  return 0


if __name__ == '__main__':
  sys.exit(main())
