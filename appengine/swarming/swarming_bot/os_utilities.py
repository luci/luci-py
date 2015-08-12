#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""OS specific utility functions.

Includes code:
- to declare the current system this code is running under.
- to run a command on user login.
- to restart the host.

This file serves as an API to bot_config.py. bot_config.py can be replaced on
the server to allow additional server-specific functionality.
"""

import cgi
import ctypes
import getpass
import glob
import json
import logging
import multiprocessing
import os
import pipes
import platform
import re
import shlex
import socket
import string
import subprocess
import sys
import tempfile
import threading
import time
import urllib
import urllib2

try:
  # The reason for this try/except is so someone can copy this single file on a
  # new machine and execute it as-is to get the dimensions that would be set.
  from utils import file_path
  from utils import tools
  from utils import zip_package

  THIS_FILE = os.path.abspath(zip_package.get_main_script_path() or __file__)
except ImportError:
  THIS_FILE = os.path.abspath(__file__)
  tools = None


# Properties from an android device that should be kept as dimension.
ANDROID_DETAILS = frozenset(
    [
      # Hardware details.
      u'ro.board.platform',
      u'ro.product.board',  # or ro.product.device?
      u'ro.product.cpu.abi',
      u'ro.product.cpu.abi2',

      # OS details.
      u'ro.build.id',
      u'ro.build.tags',
      u'ro.build.type',
      u'ro.build.version.sdk',
    ])


# https://cloud.google.com/compute/pricing#machinetype
GCE_MACHINE_COST_HOUR_US = {
  u'n1-standard-1': 0.063,
  u'n1-standard-2': 0.126,
  u'n1-standard-4': 0.252,
  u'n1-standard-8': 0.504,
  u'n1-standard-16': 1.008,
  u'f1-micro': 0.012,
  u'g1-small': 0.032,
  u'n1-highmem-2': 0.148,
  u'n1-highmem-4': 0.296,
  u'n1-highmem-8': 0.592,
  u'n1-highmem-16': 1.184,
  u'n1-highcpu-2': 0.080,
  u'n1-highcpu-4': 0.160,
  u'n1-highcpu-8': 0.320,
  u'n1-highcpu-16': 0.640,
}


# https://cloud.google.com/compute/pricing#machinetype
GCE_MACHINE_COST_HOUR_EUROPE_ASIA = {
  u'n1-standard-1': 0.069,
  u'n1-standard-2': 0.138,
  u'n1-standard-4': 0.276,
  u'n1-standard-8': 0.552,
  u'n1-standard-16': 1.104,
  u'f1-micro': 0.013,
  u'g1-small': 0.0347,
  u'n1-highmem-2': 0.162,
  u'n1-highmem-4': 0.324,
  u'n1-highmem-8': 0.648,
  u'n1-highmem-16': 1.296,
  u'n1-highcpu-2': 0.086,
  u'n1-highcpu-4': 0.172,
  u'n1-highcpu-8': 0.344,
  u'n1-highcpu-16': 0.688,
}


GCE_RAM_GB_PER_CORE_RATIOS = {
  0.9: u'n1-highcpu-',
  3.75: u'n1-standard-',
  6.5: u'n1-highmem-',
}


# https://cloud.google.com/compute/pricing#disk
GCE_HDD_GB_COST_MONTH = 0.04
GCE_SSD_GB_COST_MONTH = 0.17


# https://cloud.google.com/compute/pricing#premiumoperatingsystems
GCE_WINDOWS_COST_CORE_HOUR = 0.04


### Private stuff.


_STARTED_TS = time.time()

_CACHED_OAUTH2_TOKEN_GCE = {}
_CACHED_OAUTH2_TOKEN_GCE_LOCK = threading.Lock()

_MONITORING_SCOPE = 'https://www.googleapis.com/auth/monitoring'


# Make cached a no-op when client/utils/tools.py is unavailable.
if not tools:
  def cached(func):
    return func
else:
  cached = tools.cached


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


def _generate_initd(command, cwd, user):
  """Returns a valid init.d script for use for Swarming.

  Author is lazy so he copy-pasted.
  Source: https://github.com/fhd/init-script-template

  Copyright (C) 2012-2014 Felix H. Dahlke
  This is open source software, licensed under the MIT License. See the file
  LICENSE for details.

  LICENSE is at https://github.com/fhd/init-script-template/blob/master/LICENSE
  """
  return """#!/bin/sh
### BEGIN INIT INFO
# Provides:
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start daemon at boot time
# Description:       Enable service provided by daemon.
### END INIT INFO

dir='%(cwd)s'
user='%(user)s'
cmd='%(cmd)s'

name=`basename $0`
pid_file="/var/run/$name.pid"
stdout_log="/var/log/$name.log"
stderr_log="/var/log/$name.err"

get_pid() {
  cat "$pid_file"
}

is_running() {
  [ -f "$pid_file" ] && ps `get_pid` > /dev/null 2>&1
}

case "$1" in
  start)
  if is_running; then
    echo "Already started"
  else
    echo "Starting $name"
    cd "$dir"
    sudo -u "$user" $cmd >> "$stdout_log" 2>> "$stderr_log" &
    echo $! > "$pid_file"
    if ! is_running; then
      echo "Unable to start, see $stdout_log and $stderr_log"
      exit 1
    fi
  fi
  ;;
  stop)
  if is_running; then
    echo -n "Stopping $name.."
    kill `get_pid`
    for i in {1..10}
    do
      if ! is_running; then
        break
      fi

      echo -n "."
      sleep 1
    done
    echo

    if is_running; then
      echo "Not stopped; may still be shutting down or shutdown may have failed"
      exit 1
    else
      echo "Stopped"
      if [ -f "$pid_file" ]; then
        rm "$pid_file"
      fi
    fi
  else
    echo "Not running"
  fi
  ;;
  restart)
  $0 stop
  if is_running; then
    echo "Unable to stop, will not attempt to start"
    exit 1
  fi
  $0 start
  ;;
  status)
  if is_running; then
    echo "Running"
  else
    echo "Stopped"
    exit 1
  fi
  ;;
  *)
  echo "Usage: $0 {start|stop|restart|status}"
  exit 1
  ;;
esac

exit 0
""" % {
      'cmd': ' '.join(pipes.quote(c) for c in command),
      'cwd': pipes.quote(cwd),
      'user': pipes.quote(user),
    }

def _generate_autostart_destkop(command, name):
  """Returns a valid .desktop for use with Swarming bot.

  http://standards.freedesktop.org/desktop-entry-spec/desktop-entry-spec-latest.html
  """
  return (
    '[Desktop Entry]\n'
    'Type=Application\n'
    'Name=%(name)s\n'
    'Exec=%(cmd)s\n'
    'Hidden=false\n'
    'NoDisplay=false\n'
    'Comment=Created by os_utilties.py\n'
    'X-GNOME-Autostart-enabled=true\n') % {
      'cmd': ' '.join(pipes.quote(c) for c in command),
      'name': name,
    }


def _get_os_version_name_win():
  """Returns the marketing name of the OS including the service pack."""
  marketing_name = platform.uname()[2]
  service_pack = platform.win32_ver()[2] or 'SP0'
  return '%s-%s' % (marketing_name, service_pack)


def _get_gpu_linux():
  """Returns video device as listed by 'lspci'. See get_gpu().
  """
  try:
    pci_devices = subprocess.check_output(
        ['lspci', '-mm', '-nn'], stderr=subprocess.PIPE).splitlines()
  except (OSError, subprocess.CalledProcessError):
    # It normally happens on Google Compute Engine as lspci is not installed by
    # default and on ARM since they do not have a PCI bus. In either case, we
    # don't care about the GPU.
    return None, None

  dimensions = set()
  state = set()
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
      dimensions.add(ven_id)
      dimensions.add('%s:%s' % (ven_id, device.group(2)))
      state.add('%s %s' % (vendor.group(1), device.group(1)))
  return sorted(dimensions), sorted(state)


@cached
def _get_SPDisplaysDataType_osx():
  """Returns an XML about the system display properties."""
  import plistlib
  sp = subprocess.check_output(
      ['system_profiler', 'SPDisplaysDataType', '-xml'])
  return plistlib.readPlistFromString(sp)[0]['_items']


def _get_gpu_osx():
  """Returns video device as listed by 'system_profiler'. See get_gpu()."""
  dimensions = set()
  state = set()
  for card in _get_SPDisplaysDataType_osx():
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
    dimensions.add(ven_id)
    dimensions.add('%s:%s' % (ven_id, dev_id))

    # VMWare doesn't set it.
    if 'sppci_model' in card:
      state.add(card['sppci_model'])
  return sorted(dimensions), sorted(state)


def _get_monitor_hidpi_osx():
  """Returns True if the monitor is hidpi."""
  hidpi = any(
    any(m.get('spdisplays_retina') == 'spdisplays_yes'
        for m in card['spdisplays_ndrvs'])
    for card in _get_SPDisplaysDataType_osx()
    if 'spdisplays_ndrvs' in card)
  return str(int(hidpi))


def _get_gpu_win():
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
    ven_id = 'UNKNOWN'
    dev_id = 'UNKNOWN'
    match = re.search(r'VEN_([0-9A-F]{4})', pnp_string)
    if match:
      ven_id = match.group(1).lower()
    match = re.search(r'DEV_([0-9A-F]{4})', pnp_string)
    if match:
      dev_id = match.group(1).lower()
    dimensions.add(ven_id)
    dimensions.add('%s:%s' % (ven_id, dev_id))
  return sorted(dimensions), sorted(state)


@cached
def _get_mount_points_win():
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


def _get_disk_info_win(mount_point):
  """Returns total and free space on a mount point in Mb."""
  total_bytes = ctypes.c_ulonglong(0)
  free_bytes = ctypes.c_ulonglong(0)
  ctypes.windll.kernel32.GetDiskFreeSpaceExW(
      ctypes.c_wchar_p(mount_point), None, ctypes.pointer(total_bytes),
      ctypes.pointer(free_bytes))
  return {
    'free_mb': int(round(free_bytes.value / 1024. / 1024.)),
    'size_mb': int(round(total_bytes.value / 1024. / 1024.)),
  }


def _get_disks_info_win():
  """Returns disk infos on all mount point in Mb."""
  return dict((p, _get_disk_info_win(p)) for p in _get_mount_points_win())


def _run_df():
  """Runs df and returns the output."""
  proc = subprocess.Popen(
      ['/bin/df', '-k', '-P'], env={'LANG': 'C'},
      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for l in proc.communicate()[0].splitlines():
    if l.startswith('/dev/'):
      items = l.split()
      if (sys.platform == 'darwin' and
          items[5].startswith('/Volumes/firmwaresyncd.')):
        # There's an issue on OSX where sometimes a small volume is mounted
        # during boot time and may be caught here by accident. Just ignore it as
        # it could trigger the low free disk space check and cause an unexpected
        # bot self-quarantine.
        continue
      yield items


def _get_disks_info_posix():
  """Returns disks info on all mount point in Mb."""
  return dict(
      (
        items[5],
        {
          'free_mb': int(round(int(items[3]) / 1024.)),
          'size_mb': int(round(int(items[1]) / 1024.)),
        }
      ) for items in _run_df())


@cached
def _get_metadata_gce():
  """Returns the GCE metadata as a dict.

  Refs:
    https://cloud.google.com/compute/docs/metadata
    https://cloud.google.com/compute/docs/machine-types

  To get the at the command line from a GCE VM, use:
    curl --silent \
      http://metadata.google.internal/computeMetadata/v1/?recursive=true \
      -H "Metadata-Flavor: Google" | python -m json.tool | less
  """
  url = 'http://metadata.google.internal/computeMetadata/v1/?recursive=true'
  headers = {'Metadata-Flavor': 'Google'}
  try:
    return json.load(
        urllib2.urlopen(urllib2.Request(url, headers=headers), timeout=5))
  except IOError as e:
    logging.error('GCE metadata not available: %s', e)
    return None


def _get_cost_hour_gce():
  """Returns the $USD/hour of using this bot if applicable."""
  # Machine.
  machine_type = get_machine_type_gce()
  if get_zone_gce().startswith('us-'):
    machine_cost = GCE_MACHINE_COST_HOUR_US[machine_type]
  else:
    machine_cost = GCE_MACHINE_COST_HOUR_EUROPE_ASIA[machine_type]

  # OS.
  os_cost = 0.
  if sys.platform == 'win32':
    # Assume Windows Server.
    if machine_type in ('f1-micro', 'g1-small'):
      os_cost = 0.02
    else:
      os_cost = GCE_WINDOWS_COST_CORE_HOUR * get_num_processors()

  # Disk.
  # TODO(maruel): Figure out the disk type. The metadata is not useful AFAIK.
  disk_gb_cost = 0.
  for disk in get_disks_info().itervalues():
    disk_gb_cost += disk['free_mb'] / 1024. * (
        GCE_HDD_GB_COST_MONTH / 30. / 24.)

  # TODO(maruel): Network. It's not a constant cost, it's per task.
  # See https://cloud.google.com/monitoring/api/metrics
  # compute.googleapis.com/instance/network/sent_bytes_count
  return machine_cost + os_cost + disk_gb_cost


def _oauth2_access_token_gce(account='default'):
  """Returns a value of oauth2 access token."""
  # TODO(maruel): Move GCE VM authentication logic into client/utils/net.py.
  # As seen in google-api-python-client/oauth2client/gce.py
  with _CACHED_OAUTH2_TOKEN_GCE_LOCK:
    cached_tok = _CACHED_OAUTH2_TOKEN_GCE.get(account)
    # Cached and expires in more than 5 min from now.
    if cached_tok and cached_tok['expiresAt'] >= time.time() + 5*60:
      return cached_tok['accessToken']
    # Grab the token.
    url = (
        'http://metadata.google.internal/computeMetadata/v1/instance'
        '/service-accounts/%s/token' % account)
    headers = {'Metadata-Flavor': 'Google'}
    try:
      resp = json.load(
          urllib2.urlopen(urllib2.Request(url, headers=headers), timeout=20))
    except IOError as e:
      logging.error('Failed to grab GCE access token: %s', e)
      raise
    tok = {
      'accessToken': resp['access_token'],
      'expiresAt': time.time() + resp['expires_in'],
    }
    _CACHED_OAUTH2_TOKEN_GCE[account] = tok
    return tok['accessToken']


def _oauth2_available_scopes_gce(account='default'):
  """Returns a list of OAuth2 scopes granted to GCE service account."""
  metadata = _get_metadata_gce()
  if not metadata:
    return []
  accounts = metadata['instance']['serviceAccounts']
  return accounts.get(account, {}).get('scopes') or []


def _safe_read(filepath):
  """Returns the content of the file if possible, None otherwise."""
  try:
    with open(filepath, 'rb') as f:
      return f.read()
  except (IOError, OSError):
    return None


### Public API.


@cached
def get_os_version_number():
  """Returns the normalized OS version number as a string.

  Returns:
    The format depends on the OS:
    - Windows: 5.1, 6.1, etc. There is no way to distinguish between Windows 7
          and Windows Server 2008R2 since they both report 6.1.
    - OSX: 10.7, 10.8, etc.
    - Ubuntu: 12.04, 10.04, etc.
    Others will return None.
  """
  if sys.platform in ('cygwin', 'win32'):
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

  if sys.platform == 'darwin':
    version_parts = platform.mac_ver()[0].split('.')
    assert len(version_parts) >= 2, 'Unable to determine Mac version'
    return u'.'.join(version_parts[:2])

  if sys.platform == 'linux2':
    # On Ubuntu it will return a string like '12.04'. On Raspbian, it will look
    # like '7.6'.
    return unicode(platform.linux_distribution()[1])

  logging.error('Unable to determine platform version')
  return None


@cached
def get_os_version_name():
  """Returns the marketing name on Windows.

  Returns None on other OSes, since it's not problematic there. Having
  dimensions like Trusty or Snow Leopard is not useful.
  """
  if sys.platform == 'win32':
    return _get_os_version_name_win()
  return None


@cached
def get_os_name():
  """Returns standardized OS name.

  Defaults to sys.platform for OS not normalized.

  Returns:
    Windows, Mac, Ubuntu, Raspbian, etc.
  """
  value = {
    'cygwin': u'Windows',
    'darwin': u'Mac',
    'win32': u'Windows',
  }.get(sys.platform)
  if value:
    return value

  if sys.platform == 'linux2':
    # Try to figure out the distro. Supported distros are Debian, Ubuntu,
    # Raspbian.
    # Add support for other OSes as relevant.
    content = _safe_read('/etc/os-release')
    if content:
      os_release = dict(l.split('=', 1) for l in content.splitlines() if l)
      os_id = os_release.get('ID').strip('"')
      # Uppercase the first letter for consistency with the other platforms.
      return unicode(os_id[0].upper() + os_id[1:])

  return unicode(sys.platform)


@cached
def get_cpu_type():
  """Returns the type of processor: arm or x86."""
  machine = platform.machine().lower()
  if machine in ('amd64', 'x86_64', 'i386'):
    return u'x86'
  return unicode(machine)


@cached
def get_cpu_bitness():
  """Returns the number of bits in the CPU architecture as a str: 32 or 64.

  Unless someone ported python to PDP-10 or 286.

  Note: this function may return 32 bits on 64 bits OS in case of a 32 bits
  python process.
  """
  if platform.machine().endswith('64'):
    return u'64'
  # TODO(maruel): Work harder to figure out if OS is 64 bits.
  return u'64' if sys.maxsize > 2**32 else u'32'


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
  return unicode(hostname)


def get_hostname_short():
  """Returns the base host name."""
  return get_hostname().split(u'.', 1)[0]


@cached
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
      # Returns non-zero, otherwise it could generate a divide by zero later
      # when doing calculations, leading to a crash. Saw it happens on Win2K8R2
      # on python 2.7.5 on cygwin 1.7.28.
      logging.error('get_num_processors() failed to query number of cores')
      # Return an improbable number to make it easier to catch.
      return 5


@cached
def get_physical_ram():
  """Returns the amount of installed RAM in Mb, rounded to the nearest number.
  """
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
    return int(round(stat.dwTotalPhys / 1024. / 1024.))

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
    return int(round(result.value / 1024. / 1024.))

  if os.path.isfile('/proc/meminfo'):
    # linux.
    meminfo = _safe_read('/proc/meminfo') or ''
    matched = re.search(r'MemTotal:\s+(\d+) kB', meminfo)
    if matched:
      mb = int(matched.groups()[0]) / 1024.
      if 0. < mb < 1.:
        return 1
      return int(round(mb))

  logging.error('get_physical_ram() failed to query amount of physical RAM')
  return 0


def get_disks_info():
  """Returns a dict of dict of free and total disk space."""
  if sys.platform == 'win32':
    return _get_disks_info_win()
  return _get_disks_info_posix()


@cached
def get_gpu():
  """Returns the installed video card(s) name.

  Returns:
    All the video cards detected.
    tuple(list(dimensions), list(state)).
  """
  if sys.platform == 'darwin':
    dimensions, state = _get_gpu_osx()
  elif sys.platform == 'linux2':
    dimensions, state = _get_gpu_linux()
  elif sys.platform == 'win32':
    dimensions, state = _get_gpu_win()
  else:
    dimensions, state = None, None

  # 15ad is VMWare. It's akin not having a GPU card.
  dimensions = dimensions or [u'none']
  if '15ad' in dimensions:
    dimensions.append(u'none')
    dimensions.sort()
  return dimensions, state


@cached
def get_monitor_hidpi():
  """Returns True if there is an hidpi monitor detected."""
  if sys.platform == 'darwin':
    return [_get_monitor_hidpi_osx()]
  return None


@cached
def get_cost_hour():
  """Returns the cost in $USD/h as a floating point value if applicable."""
  if _get_metadata_gce():
    return _get_cost_hour_gce()

  # Get an approximate cost trying to emulate GCE equivalent cost.
  cores = get_num_processors()
  os_cost = 0.
  if sys.platform == 'darwin':
      # Apple tax. It's 50% better, right?
      os_cost = GCE_WINDOWS_COST_CORE_HOUR * 1.5 * cores
  elif sys.platform == 'win32':
      # MS tax.
      os_cost = GCE_WINDOWS_COST_CORE_HOUR * cores

  # Guess an equivalent machine_type.
  machine_cost = GCE_MACHINE_COST_HOUR_US.get(get_machine_type(), 0.)

  # Assume HDD for now, it's the cheapest. That's not true, we do have SSDs.
  disk_gb_cost = 0.
  for disk in get_disks_info().itervalues():
      disk_gb_cost += disk['free_mb'] / 1024. * (
          GCE_HDD_GB_COST_MONTH / 30. / 24.)
  return machine_cost + os_cost + disk_gb_cost


@cached
def get_machine_type():
  """Returns a GCE-equivalent machine type.

  If running on GCE, returns the right machine type. Otherwise tries to find the
  'closest' one.
  """
  if _get_metadata_gce():
    return get_machine_type_gce()

  ram_gb = get_physical_ram() / 1024.
  cores = get_num_processors()
  ram_gb_per_core = ram_gb / cores
  logging.info('RAM GB/core = %s', ram_gb_per_core)
  best_fit = None
  for ratio, prefix in GCE_RAM_GB_PER_CORE_RATIOS.iteritems():
    delta = (ram_gb_per_core-ratio)**2
    if best_fit is None or delta < best_fit[0]:
      best_fit = (delta, prefix)
  prefix = best_fit[1]
  machine_type = prefix + unicode(cores)
  if machine_type not in GCE_MACHINE_COST_HOUR_US:
    # Try a best fit.
    logging.info('Failed to find a good machine_type match: %s', machine_type)
    for i in (16, 8, 4, 2):
      if cores > i:
        machine_type = prefix + unicode(i)
        break
    else:
      if cores == 1:
        # There's no n1-highcpu-1 nor n1-highmem-1.
        if ram_gb < 1.7:
          machine_type = u'f1-micro'
        elif ram_gb < 3.75:
          machine_type = u'g1-small'
        else:
          machine_type = u'n1-standard-1'
      else:
        logging.info('Failed to find a fit: %s', machine_type)

  if machine_type not in GCE_MACHINE_COST_HOUR_US:
    return None
  return machine_type


@cached
def can_send_metric():
  """True if 'send_metric' really does something."""
  if _get_metadata_gce():
    return _MONITORING_SCOPE in _oauth2_available_scopes_gce()
  return False


def send_metric(name, value):
  if _get_metadata_gce():
    return send_metric_gce(name, value)
  # Ignore on other platforms for now.


### Google Cloud Compute Engine.


@cached
def get_zone_gce():
  """Returns the zone containing the GCE VM."""
  metadata = _get_metadata_gce()
  if not metadata:
    return None
  # Format is projects/<id>/zones/<zone>
  return unicode(metadata['instance']['zone'].rsplit('/', 1)[-1])


@cached
def get_machine_type_gce():
  """Returns the GCE machine type."""
  metadata = _get_metadata_gce()
  if not metadata:
    return None
  # Format is projects/<id>/machineTypes/<machine_type>
  return unicode(metadata['instance']['machineType'].rsplit('/', 1)[-1])


@cached
def get_tags_gce():
  """Returns a list of instance tags or empty list if not GCE VM."""
  metadata = _get_metadata_gce()
  if not metadata:
    return []
  return metadata['instance']['tags']


def send_metric_gce(name, value):
  """Sets a lightweight custom metric.

  In particular, the metric has no description and it is double. To make this
  work, use "--scopes https://www.googleapis.com/auth/monitoring" when running
  "gcloud compute instances create". You can verify if the scope is enabled from
  within a GCE VM with:
    curl "http://metadata.google.internal/computeMetadata/v1/instance/\
service-accounts/default/scopes" -H "Metadata-Flavor: Google"

  Ref: https://cloud.google.com/monitoring/custom-metrics/lightweight

  To create a metric, use:
  https://developers.google.com/apis-explorer/#p/cloudmonitoring/v2beta2/cloudmonitoring.metricDescriptors.create
  It is important to set the commonLabels.
  """
  logging.info('send_metric_gce(%s, %s)', name, value)
  assert isinstance(name, str), repr(name)
  assert isinstance(value, float), repr(value)

  metadata = _get_metadata_gce()
  project_id = metadata['project']['numericProjectId']

  url = (
    'https://www.googleapis.com/cloudmonitoring/v2beta2/projects/%s/'
    'timeseries:write') % project_id
  now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
  body = {
    'commonLabels': {
      'cloud.googleapis.com/service': 'compute.googleapis.com',
      'cloud.googleapis.com/location': get_zone_gce(),
      'compute.googleapis.com/resource_type': 'instance',
      'compute.googleapis.com/resource_id': metadata['instance']['id'],
    },
    'timeseries': [
      {
        'timeseriesDesc': {
          'metric': 'custom.cloudmonitoring.googleapis.com/' + name,
          'project': project_id,
        },
        'point': {
          'start': now,
          'end': now,
          'doubleValue': value,
        },
      },
    ],
  }
  headers = {
    'Authorization': 'Bearer ' + _oauth2_access_token_gce(),
    'Content-Type': 'application/json',
  }
  logging.info('%s', json.dumps(body, indent=2, sort_keys=True))
  try:
    resp = urllib2.urlopen(urllib2.Request(url, json.dumps(body), headers))
    # Result must be valid JSON. A sample response:
    #   {"kind": "cloudmonitoring#writeTimeseriesResponse"}
    logging.debug(json.load(resp))
  except urllib2.HTTPError as e:
    logging.error('send_metric failed: %s: %s' % (e, e.read()))
  except IOError as e:
    logging.error('send_metric failed: %s' % e)


### Windows.


@cached
def get_integrity_level_win():
  """Returns the integrity level of the current process as a string.

  TODO(maruel): It'd be nice to make it work on cygwin. The problem is that
  ctypes.windll is unaccessible and it is not known to the author how to use
  stdcall convention through ctypes.cdll.
  """
  if sys.platform != 'win32':
    return None
  if get_os_version_number() == u'5.1':
    # Integrity level is Vista+.
    return None

  mapping = {
    0x0000: u'untrusted',
    0x1000: u'low',
    0x2000: u'medium',
    0x2100: u'medium high',
    0x3000: u'high',
    0x4000: u'system',
    0x5000: u'protected process',
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
    return mapping.get(value) or u'0x%04x' % value
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
    # TODO(maruel): Handle 'offline', 'device', 'no device' and
    # 'unauthorized'.
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
  out = dict(
      (k, [v]) for k, v in properties.iteritems() if k in ANDROID_DETAILS)
  out[u'id'] = [device_id]
  return out


def get_state_android(device_id, adb_path='adb'):
  """Returns state information about the device.

  It's a big speculating TODO. Would be temperature, device uptime, partition
  space, etc.
  """
  # Unused argument - pylint: disable=W0613
  return {
    u'device': {
      # TODO(maruel): Fill me.
    },
    u'host': get_state(),
  }


###


def get_dimensions():
  """Returns the default dimensions."""
  os_name = get_os_name()
  cpu_type = get_cpu_type()
  cpu_bitness = get_cpu_bitness()
  dimensions = {
    u'cores': [unicode(get_num_processors())],
    u'cpu': [
      cpu_type,
      cpu_type + u'-' + cpu_bitness,
    ],
    u'gpu': get_gpu()[0],
    u'id': [get_hostname_short()],
    u'os': [os_name],
  }
  os_version_name = get_os_version_name()
  if os_version_name:
    # This only happens on Windows.
    dimensions[u'os'].append(u'%s-%s' % (os_name, os_version_name))
  else:
    dimensions[u'os'].append(u'%s-%s' % (os_name, get_os_version_number()))
  if u'none' not in dimensions[u'gpu']:
    hidpi = get_monitor_hidpi()
    if hidpi:
      dimensions[u'hidpi'] = hidpi

  machine_type = get_machine_type()
  if machine_type:
    dimensions[u'machine_type'] = [machine_type]
  zone = get_zone_gce()
  if zone:
    dimensions[u'zone'] = [zone]

  if cpu_type.startswith(u'arm') and cpu_type != u'arm':
    dimensions[u'cpu'].append(u'arm')
    dimensions[u'cpu'].append(u'arm-' + cpu_bitness)
    dimensions[u'cpu'].sort()

  if sys.platform == 'linux2':
    dimensions[u'os'].append(u'Linux')
    dimensions[u'os'].sort()

  return dimensions


def get_state(threshold_mb=2*1024, skip=None):
  """Returns dict with a state of the bot reported to the server with each poll.

  Supposed to be use only for dynamic state that changes while bot is running.

  The server can not use this state for immediate scheduling purposes (use
  'dimensions' for that), but it can use it for maintenance and bookkeeping
  tasks.

  Arguments:
  - threshold_mb: number of mb below which the bot will quarantine itself
        automatically. Set to 0 or None to disable.
  - skip: list of partitions to skip for automatic quarantining on low free
        space.
  """
  # TODO(vadimsh): Send 'uptime', number of open file descriptors, processes or
  # any other leaky resources. So that the server can decided to reboot the bot
  # to clean up.
  state = {
    u'cost_usd_hour': get_cost_hour(),
    u'cwd': os.getcwd(),
    u'disks': get_disks_info(),
    u'gpu': get_gpu()[1],
    u'ip': get_ip(),
    u'hostname': get_hostname(),
    u'ram': get_physical_ram(),
    u'running_time': int(round(time.time() - _STARTED_TS)),
    u'started_ts': int(round(_STARTED_TS)),
  }
  if sys.platform in ('cygwin', 'win32'):
    state[u'cygwin'] = [sys.platform == 'cygwin']
  if sys.platform == 'win32':
    # TODO(maruel): Have get_integrity_level_win() work in the first place.
    integrity = get_integrity_level_win()
    if integrity is not None:
      state[u'integrity'] = [integrity]
  auto_quarantine_on_low_space(state, threshold_mb, skip)
  return state


def auto_quarantine_on_low_space(state, threshold_mb=2*1024, skip=None):
  """Quarantines when less than threshold_mb on any partition.

  Modifies state in-place. Assumes state['free_disks'] is valid.
  """
  if not threshold_mb or state.get(u'quarantined'):
    return
  if skip is None:
    # Do not check these mount points for low disk space.
    skip = ['/boot', '/boot/efi']

  s = []
  for mount, infos in state[u'disks'].iteritems():
    space_mb = infos['free_mb']
    if mount not in skip and space_mb < threshold_mb:
      s.append('Not enough free disk space on %s.' % mount)
  if s:
    state[u'quarantined'] = '\n'.join(s)


def rmtree(path):
  """Removes a directory the bold way."""
  file_path.rmtree(path)


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
  if not os.path.isabs(cwd):
    raise ValueError('Refusing relative path')
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
  success = _write(batch_path, content)
  if success and sys.platform == 'cygwin':
    # For some reason, cygwin tends to create the file with 0644.
    os.chmod(batch_path, 0755)
  return success


def setup_auto_startup_osx(command, cwd, plistname):
  """Uses launchd to start the command when the user logs in.

  This assumes the user is automatically logged in on OS startup.

  In case of failure like reboot loop, simply remove the file in
  ~/Library/LaunchAgents/.
  """
  logging.info('setup_auto_startup_osx(%s, %s, %s)', command, cwd, plistname)
  if not os.path.isabs(cwd):
    raise ValueError('Refusing relative path')
  assert plistname.endswith('.plist'), plistname
  launchd_dir = os.path.expanduser('~/Library/LaunchAgents')
  if not os.path.isdir(launchd_dir):
    # This directory doesn't exist by default.
    # Sometimes ~/Library gets deleted.
    os.makedirs(launchd_dir)
  filepath = os.path.join(launchd_dir, plistname)
  return _write(filepath, _generate_launchd_plist(command, cwd, plistname))


def setup_auto_startup_initd_linux(command, cwd, user=None, name='swarming'):
  """Uses init.d to start the bot automatically."""
  if not user:
    user = getpass.getuser()
  logging.info(
      'setup_auto_startup_initd_linux(%s, %s, %s, %s)',
      command, cwd, user, name)
  if not os.path.isabs(cwd):
    raise ValueError('Refusing relative path')
  script = _generate_initd(command, cwd, user)
  filepath = pipes.quote(os.path.join('/etc/init.d', name))
  with tempfile.NamedTemporaryFile() as f:
    if not _write(f.name, script):
      return False

    # Need to do 3 things as sudo. Do it all at once to enable a single sudo
    # request.
    # TODO(maruel): Likely not the sanest thing, reevaluate.
    cmd = [
      'sudo', '/bin/sh', '-c',
      "cp %s %s && chmod 0755 %s && update-rc.d %s defaults" % (
        pipes.quote(f.name), filepath, filepath, name)
    ]
    subprocess.check_call(cmd)
    print('To remove, use:')
    print('  sudo update-rc.d -f %s remove' % name)
    print('  sudo rm %s' % filepath)
  return True


def setup_auto_startup_autostart_desktop_linux(command, name='swarming'):
  """Uses ~/.config/autostart to start automatically the bot on user login.

  http://standards.freedesktop.org/autostart-spec/autostart-spec-latest.html
  """
  basedir = os.path.expanduser('~/.config/autostart')
  if not os.path.isdir(basedir):
    os.makedirs(basedir)
  filepath = os.path.join(basedir, '%s.desktop' % name)
  return _write(filepath, _generate_autostart_destkop(command, name))


def restart(message=None, timeout=None):
  """Restarts this machine.

  If it fails to reboot the host, it loops until timeout. This function does
  not return on successful restart, or returns False if machine wasn't
  restarted within |timeout| seconds.
  """
  deadline = time.time() + timeout if timeout else None
  while True:
    restart_and_return(message)
    # Sleep for 300 seconds to ensure we don't try to do anymore work while the
    # OS is preparing to shutdown.
    duration = min(300, deadline - time.time()) if timeout else 300
    if duration > 0:
      logging.info('Sleeping for %s', duration)
      time.sleep(duration)
    if timeout and time.time() >= deadline:
      logging.warning(
          'Waited for host to restart for too long (%s); aborting', timeout)
      return False


def restart_and_return(message=None):
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
    logging.info(
        'Restarting machine with command %s (%s)', ' '.join(cmd), message)
    try:
      subprocess.check_call(cmd)
      logging.info('Restart command exited successfully')
    except (OSError, subprocess.CalledProcessError) as e:
      logging.error('Failed to run %s: %s', ' '.join(cmd), e)
    else:
      success = True
  return success


def roll_log(name):
  """Rolls a log in 5Mb chunks and keep the last 10 files."""
  try:
    if not os.path.isfile(name) or os.stat(name).st_size < 5*1024*1024:
      return
    if os.path.isfile('%s.9' % name):
      os.remove('%s.9' % name)
    for i in xrange(8, 0, -1):
      item = '%s.%d' % (name, i)
      if os.path.isfile(item):
        os.rename(item, '%s.%d' % (name, i+1))
    if os.path.isfile(name):
      os.rename(name, '%s.1' % name)
  except Exception as e:
    logging.exception('roll_log(%s) failed: %s', name, e)


def trim_rolled_log(name):
  try:
    for item in glob.iglob('%s.??' % name):
      os.remove(item)
    for item in glob.iglob('%s.???' % name):
      os.remove(item)
  except Exception as e:
    logging.exception('trim_rolled_log(%s) failed: %s', name, e)


def main():
  """Prints out the output of get_dimensions() and get_state()."""
  # Pass an empty tag, so pop it up since it has no significance.
  data = {
    u'dimensions': get_dimensions(),
    u'state': get_state(),
  }
  json.dump(data, sys.stdout, indent=2, sort_keys=True, separators=(',', ': '))
  print('')
  return 0


if __name__ == '__main__':
  sys.exit(main())
