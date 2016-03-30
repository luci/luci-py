# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""OSX specific utility functions."""

import cgi
import ctypes
import os
import platform
import re
import subprocess
import time

import plistlib

from utils import tools

import common


## Private stuff.


@tools.cached
def _get_system_profiler(data_type):
  """Returns an XML about the system display properties."""
  sp = subprocess.check_output(
      ['system_profiler', data_type, '-xml'])
  return plistlib.readPlistFromString(sp)[0]['_items']


@tools.cached
def _get_libc():
  ctypes.cdll.LoadLibrary('/usr/lib/libc.dylib')
  return ctypes.CDLL('/usr/lib/libc.dylib')


def _sysctl(ctl, item, result):
  """Calls sysctl. Ignores return value."""
  # https://developer.apple.com/library/mac/documentation/Darwin/Reference/ManPages/man3/sysctl.3.html
  # http://opensource.apple.com/source/xnu/xnu-1699.24.8/bsd/sys/sysctl.h
  arr = (ctypes.c_int * 2)(ctl, item)
  size = ctypes.c_size_t(ctypes.sizeof(result))
  _get_libc().sysctl(
      arr, len(arr), ctypes.byref(result), ctypes.byref(size),
      ctypes.c_void_p(), ctypes.c_size_t(0))


class _timeval(ctypes.Structure):
  _fields_ = [('tv_sec', ctypes.c_long), ('tv_usec', ctypes.c_int)]


@tools.cached
def _get_xcode_version(xcode_app):
  """Returns the version of Xcode installed at the given path.

  Args:
    xcode_app: Absolute path to the Xcode app directory, e.g
               /Applications/Xcode.app.

  Returns:
    A tuple of (Xcode version, Xcode build version), or None if
    this is not an Xcode installation.
  """
  xcodebuild = os.path.join(
      xcode_app, 'Contents', 'Developer', 'usr', 'bin', 'xcodebuild')
  if os.path.exists(xcodebuild):
    try:
      out = subprocess.check_output([xcodebuild, '-version']).splitlines()
    except subprocess.CalledProcessError:
      return None
    return out[0].split()[-1], out[1].split()[-1]


## Public API.


def get_xcode_state():
  """Returns the state of Xcode installations on this machine."""
  state = {}
  applications_dir = os.path.join('/Applications')
  for app in os.listdir(applications_dir):
    name, ext = os.path.splitext(app)
    if name.startswith('Xcode') and ext == '.app':
      xcode_app = os.path.join(applications_dir, app)
      version = _get_xcode_version(xcode_app)
      if version:
        state[xcode_app] = {
          'version': version[0],
          'build version': version[1],
        }
        device_support_dir = os.path.join(
            xcode_app, 'Contents', 'Developer', 'Platforms',
            'iPhoneOS.platform', 'DeviceSupport')
        if os.path.exists(device_support_dir):
          state[xcode_app]['device support'] = os.listdir(device_support_dir)
  return state


def get_xcode_versions():
  """Returns a list of Xcode versions installed on this machine."""
  return sorted(xcode['version'] for xcode in get_xcode_state().itervalues())


def get_current_xcode_version():
  """Returns the active version of Xcode."""
  try:
    out = subprocess.check_output(['xcodebuild', '-version']).splitlines()
  except (OSError, subprocess.CalledProcessError):
    return None
  return out[0].split()[-1], out[1].split()[-1]


def get_ios_device_ids():
  """Returns a list of UDIDs of attached iOS devices.

  Requires idevice_id in $PATH. idevice_id is part of libimobiledevice.
  See http://libimobiledevice.org.
  """
  try:
    return subprocess.check_output(['idevice_id', '--list']).splitlines()
  except (OSError, subprocess.CalledProcessError):
    return []


def get_ios_version(udid):
  """Returns the OS version of the specified iOS device.

  Requires ideviceinfo in $PATH. ideviceinfo is part of libimobiledevice.
  See http://libimobiledevice.org.

  Args:
    udid: UDID string as returned by get_ios_device_ids.
  """
  try:
    out = subprocess.check_output(
        ['ideviceinfo', '-k', 'ProductVersion', '-u', udid]).splitlines()
    if len(out) == 1:
      return out[0]
  except (OSError, subprocess.CalledProcessError):
    pass


@tools.cached
def get_ios_device_type(udid):
  """Returns the type of the specified iOS device.

  Requires ideviceinfo in $PATH. ideviceinfo is part of libimobiledevice.
  See http://libimobiledevice.org.

  Args:
    udid: UDID string as returned by get_ios_device_ids.
  """
  try:
    out = subprocess.check_output(
        ['ideviceinfo', '-k', 'ProductType', '-u', udid]).splitlines()
    if len(out) == 1:
      return out[0]
  except (OSError, subprocess.CalledProcessError):
    pass


@tools.cached
def get_hardware_model_string():
  """Returns the Mac model string.

  Returns:
    A string like Macmini5,3 or MacPro6,1.
  """
  try:
    return subprocess.check_output(['sysctl', '-n', 'hw.model']).rstrip()
  except (OSError, subprocess.CalledProcessError):
    return None


@tools.cached
def get_os_version_number():
  """Returns the normalized OS version number as a string.

  Returns:
    10.7, 10.8, etc.
  """
  version_parts = platform.mac_ver()[0].split('.')
  assert len(version_parts) >= 2, 'Unable to determine Mac version'
  return u'.'.join(version_parts[:2])


@tools.cached
def get_audio():
  """Returns the audio cards that are "connected"."""
  return [
    card['_name'] for card in _get_system_profiler('SPAudioDataType')
    if card.get('coreaudio_default_audio_output_device') == 'spaudio_yes'
  ]


@tools.cached
def get_gpu():
  """Returns video device as listed by 'system_profiler'. See get_gpu()."""
  dimensions = set()
  state = set()
  for card in _get_system_profiler('SPDisplaysDataType'):
    # Warning: the value provided depends on the driver manufacturer.
    # Other interesting values: spdisplays_vram, spdisplays_revision-id
    ven_id = u'UNKNOWN'
    if 'spdisplays_vendor-id' in card:
      # NVidia
      ven_id = card['spdisplays_vendor-id'][2:]
    elif 'spdisplays_vendor' in card:
      # Intel and ATI
      match = re.search(r'\(0x([0-9a-f]{4})\)', card['spdisplays_vendor'])
      if match:
        ven_id = match.group(1)
    dev_id = card['spdisplays_device-id'][2:]
    dimensions.add(unicode(ven_id))
    dimensions.add(u'%s:%s' % (ven_id, dev_id))

    # VMWare doesn't set it.
    if 'sppci_model' in card:
      state.add(unicode(card['sppci_model']))
  return sorted(dimensions), sorted(state)


@tools.cached
def get_cpuinfo():
  """Returns CPU information."""
  values = common._safe_parse(
      subprocess.check_output(['sysctl', 'machdep.cpu']))
  # http://unix.stackexchange.com/questions/43539/what-do-the-flags-in-proc-cpuinfo-mean
  return {
    u'flags': sorted(
        i.lower() for i in values[u'machdep.cpu.features'].split()),
    u'model': [
      int(values['machdep.cpu.family']), int(values['machdep.cpu.model']),
      int(values['machdep.cpu.stepping']),
      int(values['machdep.cpu.microcode_version']),
    ],
    u'name': values[u'machdep.cpu.brand_string'],
    u'vendor': values[u'machdep.cpu.vendor'],
  }


@tools.cached
def get_monitor_hidpi():
  """Returns True if the monitor is hidpi."""
  hidpi = any(
    any(m.get('spdisplays_retina') == 'spdisplays_yes'
        for m in card['spdisplays_ndrvs'])
    for card in _get_system_profiler('SPDisplaysDataType')
    if 'spdisplays_ndrvs' in card)
  return str(int(hidpi))


def get_xcode_versions():
  """Returns all Xcode versions installed."""
  return sorted(xcode['version'] for xcode in get_xcode_state().itervalues())


@tools.cached
def get_physical_ram():
  """Returns the amount of installed RAM in Mb, rounded to the nearest number.
  """
  CTL_HW = 6
  HW_MEMSIZE = 24
  result = ctypes.c_uint64(0)
  _sysctl(CTL_HW, HW_MEMSIZE, result)
  return int(round(result.value / 1024. / 1024.))


def get_uptime():
  """Returns uptime in seconds since system startup.

  Includes sleep time.
  """
  CTL_KERN = 1
  KERN_BOOTTIME = 21
  result = _timeval()
  _sysctl(CTL_KERN, KERN_BOOTTIME, result)
  start = float(result.tv_sec) + float(result.tv_usec) / 1000000.
  return time.time() - start


## Mutating code.


def generate_launchd_plist(command, cwd, plistname):
  """Generates a plist content with the corresponding command for launchd."""
  # The documentation is available at:
  # https://developer.apple.com/library/mac/documentation/Darwin/Reference/ \
  #    ManPages/man5/launchd.plist.5.html
  escaped_plist = cgi.escape(plistname)
  entries = [
    '<key>Label</key><string>%s</string>' % escaped_plist,
    '<key>StandardOutPath</key><string>logs/bot_stdout.log</string>',
    '<key>StandardErrorPath</key><string>logs/bot_stderr.log</string>',
    '<key>LimitLoadToSessionType</key><array><string>Aqua</string></array>',
    '<key>RunAtLoad</key><true/>',
    '<key>Umask</key><integer>18</integer>',

    # https://developer.apple.com/library/mac/documentation/Darwin/Reference/ManPages/man5/launchd.plist.5.html
    # launchd expects the process to call vproc_transaction_begin(), which we
    # don't. Otherwise it sends SIGKILL instead of SIGTERM, which isn't nice.
    '<key>EnableTransactions</key>',
    '<false/>',

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
