# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Android specific utility functions.

This file serves as an API to bot_config.py. bot_config.py can be replaced on
the server to allow additional server-specific functionality.
"""

import collections
import cStringIO
import inspect
import logging
import os
import pipes
import posixpath
import random
import re
import string
import subprocess
import sys
import threading
import time


from adb import adb_commands
from adb import common
from adb import sign_pythonrsa
from adb import usb_exceptions


from api import parallel


### Private stuff.


# Both following are set when ADB is initialized.
# _ADB_KEYS is set to a list of adb_protocol.AuthSigner instances. It contains
# one or multiple key used to authenticate to Android debug protocol (adb).
_ADB_KEYS = None
# _ADB_KEYS_RAW is set to a dict(pub: priv) when initialized, with the raw
# content of each key.
_ADB_KEYS_RAW = None


class _PerDeviceCache(object):
  """Caches data per device, thread-safe."""
  Device = collections.namedtuple(
      'Device',
      [
        # Cache of /system/build.prop on the Android device.
        'build_props',
        # Cache of $EXTERNAL_STORAGE_PATH.
        'external_storage_path',
        # /system/xbin/su exists.
        'has_su',
        # /root is readable.
        'is_root',
        # All the valid CPU scaling governors.
        'available_governors',
        # CPU frequency limits.
        'cpuinfo_max_freq',
        'cpuinfo_min_freq',
      ])

  def __init__(self):
    self._lock = threading.Lock()
    # Keys is usb path, value is a _Cache.Device.
    self._per_device = {}

  def get(self, device):
    assert isinstance(device, Device), device
    with self._lock:
      return self._per_device.get(device.port_path)

  def set(self, port_path, device):
    assert isinstance(device, self.Device), device
    with self._lock:
      self._per_device[port_path] = device

  def trim(self, devices):
    """Removes any stale cache for any device that is not found anymore.

    So if a device is disconnected, reflashed then reconnected, the cache isn't
    invalid.
    """
    device_keys = {d.port_path: d for d in devices}
    with self._lock:
      for port_path in self._per_device.keys():
        dev = device_keys.get(port_path)
        if not dev or not dev.is_valid:
          del self._per_device[port_path]


# Global cache of per device cache.
_per_device_cache = _PerDeviceCache()


def _dumpsys(device, arg):
  """dumpsys is a native android tool that returns semi structured data.

  It acts as a directory service but each service return their data without any
  real format, and will happily return failure.
  """
  out, exit_code = device.shell('dumpsys ' + arg)
  if exit_code != 0 or out.startswith('Can\'t find service: '):
    return None
  return out


def _parcel_to_list(lines):
  """Parses 'service call' output."""
  out = []
  for line in lines:
    match = re.match(
        '  0x[0-9a-f]{8}\\: ([0-9a-f ]{8}) ([0-9a-f ]{8}) ([0-9a-f ]{8}) '
        '([0-9a-f ]{8}) \'.{16}\'\\)?', line)
    if not match:
      break
    for i in xrange(1, 5):
      group = match.group(i)
      char = group[4:8]
      if char != '    ':
        out.append(char)
      char = group[0:4]
      if char != '    ':
        out.append(char)
  return out


def _load_device(bot, handle):
  """Return a Device wrapping an adb_commands.AdbCommands.
  """
  assert isinstance(handle, common.UsbHandle), handle
  port_path = '/'.join(map(str, handle.port_path))
  try:
    # If this succeeds, this initializes handle._handle, which is a
    # usb1.USBDeviceHandle.
    handle.Open()
  except common.usb1.USBErrorNoDevice as e:
    logging.warning('Got USBErrorNoDevice for %s: %s', port_path, e)
    # Do not kill adb, it just means the USB host is likely resetting and the
    # device is temporarily unavailable.We can't use handle.serial_number since
    # this communicates with the device.
    # TODO(maruel): In practice we'd like to retry for a few seconds.
    handle = None
  except common.usb1.USBErrorBusy as e:
    logging.warning('Got USBErrorBusy for %s. Killing adb: %s', port_path, e)
    kill_adb()
    try:
      # If it throws again, it probably means another process holds a handle to
      # the USB ports or group acl (plugdev) hasn't been setup properly.
      handle.Open()
    except common.usb1.USBErrorBusy as e:
      logging.warning(
          'USB port for %s is already open (and failed to kill ADB) '
          'Try rebooting the host: %s', port_path, e)
      handle = None
  except common.usb1.USBErrorAccess as e:
    # Do not try to use serial_number, since we can't even access the port.
    logging.warning('Try rebooting the host: %s: %s', port_path, e)
    handle = None

  cmd = None
  if handle:
    try:
      # Give 10s for the user to accept the dialog. The best design would be to
      # do a quick check with timeout=100ms and only if the first failed, try
      # again with a long timeout. The goal is not to hang the bots for several
      # minutes when all the devices are unauthenticated.
      # TODO(maruel): A better fix would be to change python-adb to continue the
      # authentication dance from where it stopped. This is left as a follow up.
      cmd = adb_commands.AdbCommands.Connect(
          handle, banner='swarming', rsa_keys=_ADB_KEYS,
          auth_timeout_ms=10000)
    except usb_exceptions.DeviceAuthError as e:
      logging.warning('AUTH FAILURE: %s: %s', port_path, e)
    except usb_exceptions.LibusbWrappingError as e:
      logging.warning('WRITE FAILURE: %s: %s', port_path, e)
    finally:
      # Do not leak the USB handle when we can't talk to the device.
      if not cmd:
        handle.Close()
  # Always create a Device, even if it points to nothing. It makes using the
  # client code much easier.
  return Device(bot, cmd, port_path)


def _init_cache(device):
  """Primes data known to be fetched soon right away that is static for the
  lifetime of the device.

  The data is cached in _per_device_cache() as long as the device is connected
  and responsive.
  """
  if not _per_device_cache.get(device):
    # TODO(maruel): This doesn't seem super useful since the following symlinks
    # already exist: /sdcard/, /mnt/sdcard, /storage/sdcard0.
    external_storage_path, exitcode = device.shell('echo -n $EXTERNAL_STORAGE')
    if exitcode:
      external_storage_path = None

    properties = {}
    out = device.pull_content('/system/build.prop')
    if not out:
      properties = None
    else:
      for line in out.splitlines():
        if line.startswith(u'#') or not line:
          continue
        key, value = line.split(u'=', 1)
        properties[key] = value

    mode, _, _ = device.stat('/system/xbin/su')
    has_su = bool(mode)

    is_root = device._is_root()

    if has_su and not is_root:
      # Opportinistically tries to switch adbd to run in root mode.
      if device._reset_adbd_as_root():
        is_root = device._is_root()

    available_governors = KNOWN_CPU_SCALING_GOVERNOR_VALUES
    out = device.pull_content(
        '/sys/devices/system/cpu/cpu0/cpufreq/scaling_available_governors')
    if out:
      available_governors = sorted(i for i in out.split())
      assert set(available_governors).issubset(
          KNOWN_CPU_SCALING_GOVERNOR_VALUES), available_governors

    cpuinfo_max_freq = device.pull_content(
        '/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq')
    if cpuinfo_max_freq:
      cpuinfo_max_freq = int(cpuinfo_max_freq)
    cpuinfo_min_freq = device.pull_content(
        '/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_min_freq')
    if cpuinfo_min_freq:
      cpuinfo_min_freq = int(cpuinfo_min_freq)

    c = _per_device_cache.Device(
        properties, external_storage_path, has_su, is_root, available_governors,
        cpuinfo_max_freq, cpuinfo_min_freq)
    _per_device_cache.set(device.port_path, c)


### Public API.


# List of known CPU scaling governor values.
KNOWN_CPU_SCALING_GOVERNOR_VALUES = (
  'conservative',  # Not available on Nexus 10
  'interactive',   # Default on Nexus 10.
  'ondemand',      # Not available on Nexus 10. Default on Nexus 4 and later.
  'performance',
  'powersave',     # Not available on Nexus 10.
  'userspace',
)


class Device(object):
  """Wraps an AdbCommands to make it exception safe.

  The fact that exceptions can be thrown any time makes the client code really
  hard to write safely. Convert USBError* to None return value.

  Only contains the low level commands. High level operations are built upon the
  low level functionality provided by this class.
  """
  # - CommonUsbError means that device I/O failed, e.g. a write or a read call
  #   returned an error.
  # - USBError means that a bus I/O failed, e.g. the device path is not present
  #   anymore.
  _ERRORS = (usb_exceptions.CommonUsbError, common.libusb1.USBError)

  def __init__(self, bot, adb_cmd, port_path):
    if adb_cmd:
      assert isinstance(adb_cmd, adb_commands.AdbCommands), adb_cmd
    self._bot = bot
    self._has_reset = False
    self._tries = 1
    self.adb_cmd = adb_cmd
    self.port_path = port_path
    self.serial = port_path
    if self.adb_cmd:
      self._set_serial_number()

  @property
  def is_valid(self):
    return bool(self.adb_cmd)

  def close(self):
    if self.adb_cmd:
      self.adb_cmd.Close()
      self.adb_cmd = None

  def listdir(self, destdir):
    """List a directory on the device."""
    assert destdir.startswith('/'), destdir
    if self.adb_cmd:
      for _ in xrange(self._tries):
        try:
          return self.adb_cmd.List(destdir)
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s): %s', destdir, e)
    return None

  def stat(self, dest):
    """Stats a file/dir on the device. It's likely faster than shell().

    Returns:
      tuple(mode, size, mtime)
    """
    assert dest.startswith('/'), dest
    if self.adb_cmd:
      for _ in xrange(self._tries):
        try:
          return self.adb_cmd.Stat(dest)
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s): %s', dest, e)
    return None, None, None

  def pull(self, remotefile, dest):
    """Retrieves a file from the device to dest on the host.

    Returns True on success.
    """
    assert remotefile.startswith('/'), remotefile
    if self.adb_cmd:
      for _ in xrange(self._tries):
        try:
          self.adb_cmd.Pull(remotefile, dest)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s, %s): %s', remotefile, dest, e)
    return False

  def pull_content(self, remotefile):
    """Reads a file from the device.

    Returns content on success as str, None on failure.
    """
    assert remotefile.startswith('/'), remotefile
    if self.adb_cmd:
      # TODO(maruel): Distinction between file is not present and I/O error.
      for _ in xrange(self._tries):
        try:
          return self.adb_cmd.Pull(remotefile, None)
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s): %s', remotefile, e)
    return None

  def push(self, localfile, dest, mtime='0'):
    """Pushes a local file to dest on the device.

    Returns True on success.
    """
    assert dest.startswith('/'), dest
    if self.adb_cmd:
      for _ in xrange(self._tries):
        try:
          self.adb_cmd.Push(localfile, dest, mtime)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s, %s): %s', localfile, dest, e)
    return False

  def push_content(self, dest, content, mtime='0'):
    """Writes content to dest on the device.

    Returns True on success.
    """
    assert dest.startswith('/'), dest
    if self.adb_cmd:
      for _ in xrange(self._tries):
        try:
          self.adb_cmd.Push(cStringIO.StringIO(content), dest, mtime)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s, %s): %s', dest, content, e)
    return False

  def reboot(self):
    """Reboot the device. Doesn't wait for it to be rebooted"""
    if self.adb_cmd:
      for _ in xrange(self._tries):
        try:
          # Use 'bootloader' to switch to fastboot.
          out = self.adb_cmd.Reboot()
          logging.info('reboot: %s', out)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(): %s', e)
    return False

  def remount(self):
    """Remount / as read-write."""
    if self.adb_cmd:
      for _ in xrange(self._tries):
        try:
          out = self.adb_cmd.Remount()
          logging.info('remount: %s', out)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(): %s', e)
    return False

  def shell(self, cmd):
    """Runs a command on an Android device while swallowing exceptions.

    Traps all kinds of USB errors so callers do not have to handle this.

    Returns:
      tuple(stdout, exit_code)
      - stdout is as unicode if it ran, None if an USB error occurred.
      - exit_code is set if ran.
    """
    for _ in xrange(self._tries):
      try:
        return self.shell_raw(cmd)
      except self._ERRORS as e:
        self._try_reset('(%s): %s', cmd, e)
    return None, None

  def shell_raw(self, cmd):
    """Runs a command on an Android device.

    It is expected that the user quote cmd properly.

    It may fail if cmd is >512 or output is >32768. Workaround for long cmd is
    to push a shell script first then run this. Workaround for large output is
    to is run_shell_wrapped().

    Returns:
      tuple(stdout, exit_code)
      - stdout is as unicode if it ran, None if an USB error occurred.
      - exit_code is set if ran.
    """
    if isinstance(cmd, unicode):
      cmd = cmd.encode('utf-8')
    assert isinstance(cmd, str), cmd
    if not self.adb_cmd:
      return None, None
    # The adb protocol doesn't return the exit code, so embed it inside the
    # command.
    complete_cmd = cmd + ' ;echo -e "\n$?"'
    assert len(complete_cmd) <= 512, 'Command is too long: %s' % complete_cmd
    out = self.adb_cmd.Shell(complete_cmd).decode('utf-8', 'replace')
    # Protect against & or other bash conditional execution that wouldn't make
    # the 'echo $?' command to run.
    if not out:
      return out, None
    # adb shell uses CRLF EOL. Only God Knows Why.
    out = out.replace('\r\n', '\n')
    # TODO(maruel): Remove and handle if this is ever trapped.
    assert out[-1] == '\n', out
    # Strip the last line to extract the exit code.
    parts = out[:-1].rsplit('\n', 1)
    return parts[0], int(parts[1])

  def _reset_adbd_as_root(self):
    """If adbd on the device is not root, ask it to restart as root.

    This causes the USB device to disapear momentarily, which causes a big mess,
    as we cannot communicate with it for a moment. So try to be clever and
    reenumerate the device until the device is back, then reinitialize the
    communication, all synchronously.
    """
    for _ in xrange(self._tries):
      try:
        out = self.adb_cmd.Root()
        logging.info('_reset_adbd_as_root() = %s', out)
        # Hardcoded strings in platform_system_core/adb/services.cpp
        if out == 'adbd is already running as root\n':
          return True
        if out == 'adbd cannot run as root in production builds\n':
          return False
        # 'restarting adbd as root\n'
        if not self._is_root():
          logging.error('Failed to id after _reset_adbd_as_root()')
          return False
        return True
      except self._ERRORS as e:
        self._try_reset('(): %s', e)
    return False

  def _reset_adbd_as_user(self):
    """If adbd on the device is root, ask it to restart as user."""
    for _ in xrange(self._tries):
      try:
        # It's defined in the adb code but doesn't work on 4.4.
        #out = self.adb_cmd.conn.Command(service='unroot')
        out = self.cmd.Shell(
            'setprop service.adb.root 0; setprop ctl.restart adbd')
        logging.info('_reset_adbd_as_user() = %s', out)
        # Hardcoded strings in platform_system_core/adb/services.cpp
        # 'adbd not running as root\n' or 'restarting adbd as non root\n'
        if self._is_root():
          logging.error('Failed to id after _reset_adbd_as_user()')
          return False
        return True
      except self._ERRORS as e:
        self._try_reset('(): %s', e)
    return False

  def _is_root(self):
    """Returns True if adbd is running as root."""
    out, exit_code = self.shell('id')
    if exit_code != 0 or not out:
      return False
    return out.startswith('out=0(root)')

  def _set_serial_number(self):
    """Tries to set self.serial to the serial number of the device.

    This means communicating to the USB device, so it may throw.
    """
    for _ in xrange(self._tries):
      try:
        # The serial number is attached to common.UsbHandle, no
        # adb_commands.AdbCommands.
        self.serial = self.adb_cmd.handle.serial_number
      except self._ERRORS as e:
        self._try_reset('(): %s', e)

  def _try_reset(self, fmt, *args):
    """When a self._ERRORS occurred, try to reset the device."""
    items = [self.port_path, inspect.stack()[1][3]]
    items.extend(args)
    msg = ('%s.%s' + fmt) % tuple(items)
    logging.error(msg)
    if self._bot:
      self._bot.post_error(msg)
    if not self._has_reset:
      self._has_reset = True
      # TODO(maruel): Actually do the reset via ADB protocol.

  def _reset(self):
    """Resets the USB connection and reconnect to the device at the low level
    ADB protocol layer.
    """
    # http://libusb.org/static/api-1.0/group__dev.html#ga7321bd8dc28e9a20b411bf18e6d0e9aa
    if self.adb_cmd:
      try:
        self.adb_cmd.handle._handle.resetDevice()
      except common.usb1.USBErrorNotFound:
        logging.info('resetDevice() failed')
      self.close()
      # Reopen itself.
      # TODO(maruel): This makes no sense to do a complete enumeration, we
      # know the exact port path.
      context = common.usb1.USBContext()
      for device in context.getDeviceList(skip_on_error=True):
        port_path = [device.getBusNumber()]
        try:
          port_path.extend(device.getPortNumberList())
        except AttributeError:
          # Temporary issue on Precise. Remove once we do not support 12.04
          # anymore.
          # https://crbug.com/532357
          port_path.append(device.getDeviceAddress())
        if port_path == self.port_path:
          device.Open()
          self.adb_cmd = adb_commands.AdbCommands.Connect(
              device, banner='swarming', rsa_keys=_ADB_KEYS,
              auth_timeout_ms=60000)
        break
      else:
        # TODO(maruel): Maybe a race condition.
        logging.info('failed to find device after reset')

  def __repr__(self):
    return '<android.Device %s %s>' % (
        self.port_path, self.serial if self.is_valid else '(broken)')


def initialize(pub_key, priv_key):
  """Initialize Android support through adb.

  You can steal pub_key, priv_key pair from ~/.android/adbkey and
  ~/.android/adbkey.pub.
  """
  global _ADB_KEYS
  global _ADB_KEYS_RAW
  if _ADB_KEYS is not None:
    assert False, 'initialize() was called repeatedly: ignoring keys'
  assert bool(pub_key) == bool(priv_key)
  pub_key = pub_key.strip() if pub_key else pub_key
  priv_key = priv_key.strip() if priv_key else priv_key

  _ADB_KEYS = []
  _ADB_KEYS_RAW = {}
  if pub_key:
    _ADB_KEYS.append(sign_pythonrsa.PythonRSASigner(pub_key, priv_key))
    _ADB_KEYS_RAW[pub_key] = priv_key

  # Try to add local adb keys if available.
  path = os.path.expanduser('~/.android/adbkey')
  if os.path.isfile(path) and os.path.isfile(path + '.pub'):
    with open(path + '.pub', 'rb') as f:
      pub = f.read().strip()
    with open(path, 'rb') as f:
      priv = f.read().strip()
    _ADB_KEYS.append(sign_pythonrsa.PythonRSASigner(pub, priv))
    _ADB_KEYS_RAW[pub] = priv

  return bool(_ADB_KEYS)


def kill_adb():
  """Stops the adb daemon.

  The Swarming bot doesn't use the Android's SDK adb. It uses python-adb to
  directly communicate with the devices. This works much better when a lot of
  devices are connected to the host.

  adb's stability is less than stellar. Kill it with fire.
  """
  if not _ADB_KEYS:
    return
  while True:
    try:
      subprocess.check_output(['pgrep', 'adb'])
    except subprocess.CalledProcessError:
      return
    try:
      subprocess.call(
          ['adb', 'kill-server'],
          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError:
      pass
    subprocess.call(
        ['killall', '--exact', 'adb'],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # Force thread scheduling to give a chance to the OS to clean out the
    # process.
    time.sleep(0.001)


def get_devices(bot=None):
  """Returns the list of devices available.

  Caller MUST call close_devices(devices) on the return value.

  Returns one of:
    - list of Device instances.
    - None if adb is unavailable.
  """
  if not _ADB_KEYS:
    return None

  handles = []
  generator = common.UsbHandle.FindDevices(
      adb_commands.DeviceIsAvailable, timeout_ms=60000)
  while True:
    try:
      # Use manual iterator handling instead of "for handle in generator" to
      # catch USB exception explicitly.
      handle = generator.next()
    except common.usb1.USBErrorOther as e:
      logging.warning(
          'Failed to open USB device, is user in group plugdev? %s', e)
      continue
    except StopIteration:
      break
    handles.append(handle)

  def fn(handle):
    # Open the device and do the initial adb-connect.
    device = _load_device(bot, handle)
    # Query the device, caching data upfront inside _per_device_cache.
    _init_cache(device)
    return device

  devices = parallel.pmap(fn, handles)
  _per_device_cache.trim(devices)
  return devices


def close_devices(devices):
  """Closes all devices opened by get_devices()."""
  for device in devices:
    device.close()


def set_cpu_scaling_governor(device, governor):
  """Sets the CPU scaling governor to the one specified.

  Returns:
    True on success.
  """
  assert isinstance(device, Device), device
  assert governor in KNOWN_CPU_SCALING_GOVERNOR_VALUES, governor
  cache = _per_device_cache.get(device)
  if cache:
    if governor not in cache.available_governors:
      if governor == 'powersave':
        return set_cpu_speed(device, cache.cpuinfo_min_freq)
      if governor == 'ondemand':
        governor = 'interactive'
      elif governor == 'interactive':
        governor = 'ondemand'
      else:
        logging.error('Can\'t switch to %s', governor)
        return False
  assert governor in KNOWN_CPU_SCALING_GOVERNOR_VALUES, governor

  path = '/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor'
  # First query the current state and only try to switch if it's different.
  prev = device.pull_content(path)
  if prev:
    prev = prev.strip()
    if prev == governor:
      return True
    if cache and prev not in cache.available_governors:
      logging.error(
          '%s: Read invalid scaling_governor: %s', device.port_path, prev)

  # This works on Nexus 10 but not on Nexus 5. Need to investigate more. In the
  # meantime, simply try one after the other.
  if not device.push_content(path, governor + '\n'):
    # Fallback via shell.
    _, exit_code = device.shell('echo "%s" > %s' % (governor, path))
    if exit_code != 0:
      logging.error(
          '%s: Writing scaling_governor %s failed; was %s',
          device.port_path, governor, prev)
      return False
  # Get it back to confirm.
  newval = device.pull_content(path)
  if not (newval or '').strip() == governor:
    logging.error(
        '%s: Wrote scaling_governor %s; was %s; got %s',
        device.port_path, governor, prev, newval)
    return False
  return True


def set_cpu_speed(device, speed):
  """Enforces strict CPU speed and disable the CPU scaling governor.

  Returns:
    True on success.
  """
  assert isinstance(device, Device), device
  assert isinstance(speed, int), speed
  assert 10000 <= speed <= 10000000, speed
  success = set_cpu_scaling_governor(device, 'userspace')
  if not device.push_content(
      '/sys/devices/system/cpu/cpu0/cpufreq/scaling_setspeed', '%d\n' % speed):
    return False
  # Get it back to confirm.
  val = device.pull_content(
      '/sys/devices/system/cpu/cpu0/cpufreq/scaling_setspeed')
  return success and (val or '').strip() == str(speed)


def get_build_prop(device):
  """Returns the system properties for a device.

  This isn't really changing through the lifetime of a bot. One corner case is
  when the device is flashed or disconnected.
  """
  assert isinstance(device, Device), device
  cache = _per_device_cache.get(device)
  if cache:
    return cache.build_props


def get_external_storage_path(device):
  """Returns the path to the SDCard (or emulated.

  May look like /storage/emulated/legacy or None.
  """
  assert isinstance(device, Device), device
  cache = _per_device_cache.get(device)
  if cache:
    return cache.external_storage_path


def get_temp(device):
  """Returns the device's 2 temperatures if available.

  Returns None otherwise.
  """
  assert isinstance(device, Device), device
  # Not all devices export this file. On other devices, the only real way to
  # read it is via Java
  # developer.android.com/guide/topics/sensors/sensors_environment.html
  temps = []
  try:
    for i in xrange(2):
      out = device.pull_content('/sys/class/thermal/thermal_zone%d/temp' % i)
      temps.append(int(out))
  except (TypeError, ValueError):
    return None
  return temps


def get_battery(device):
  """Returns details about the battery's state."""
  assert isinstance(device, Device), device
  props = {}
  out = _dumpsys(device, 'battery')
  if not out:
    return props
  for line in out.splitlines():
    if line.endswith(u':'):
      continue
    # On Android 4.1.2, it uses "voltage:123" instead of "voltage: 123".
    parts = line.split(u':', 2)
    if len(parts) == 2:
      key, value = parts
      props[key.lstrip()] = value.strip()
  out = {u'power': []}
  if props.get(u'AC powered') == u'true':
    out[u'power'].append(u'AC')
  if props.get(u'USB powered') == u'true':
    out[u'power'].append(u'USB')
  if props.get(u'Wireless powered') == u'true':
    out[u'power'].append(u'Wireless')
  for key in (u'health', u'level', u'status', u'temperature', u'voltage'):
    out[key] = int(props[key]) if key in props else None
  return out


def get_cpu_scale(device):
  """Returns the CPU scaling factor."""
  assert isinstance(device, Device), device
  mapping = {
    'cpuinfo_max_freq': u'max',
    'cpuinfo_min_freq': u'min',
    'scaling_cur_freq': u'cur',
    'scaling_governor': u'governor',
  }

  out = {}
  cache = _per_device_cache.get(device)
  if cache:
    out = {
      'max': cache.cpuinfo_max_freq,
      'min': cache.cpuinfo_min_freq,
    }
    del mapping['cpuinfo_max_freq']
    del mapping['cpuinfo_min_freq']

  out.update(
    (v, device.pull_content('/sys/devices/system/cpu/cpu0/cpufreq/' + k))
    for k, v in mapping.iteritems())
  return out


def get_uptime(device):
  """Returns the device's uptime in second."""
  assert isinstance(device, Device), device
  out = device.pull_content('/proc/uptime')
  if out:
    return float(out.split()[1])
  return None


def get_disk(device):
  """Returns details about the battery's state."""
  assert isinstance(device, Device), device
  props = {}
  out = _dumpsys(device, 'diskstats')
  if not out:
    return props
  for line in out.splitlines():
    if line.endswith(u':'):
      continue
    parts = line.split(u': ', 2)
    if len(parts) == 2:
      key, value = parts
      match = re.match(u'^(\d+)K / (\d+)K.*', value)
      if match:
        props[key.lstrip()] = {
          'free_mb': round(float(match.group(1)) / 1024., 1),
          'size_mb': round(float(match.group(2)) / 1024., 1),
        }
  return {
    u'cache': props[u'Cache-Free'],
    u'data': props[u'Data-Free'],
    u'system': props[u'System-Free'],
  }


def get_imei(device):
  """Returns the phone's IMEI."""
  assert isinstance(device, Device), device
  # Android <5.0.
  out = _dumpsys(device, 'iphonesubinfo')
  if out:
    match = re.search('  Device ID = (.+)$', out)
    if match:
      return match.group(1)

  # Android >= 5.0.
  out, _ = device.shell('service call iphonesubinfo 1')
  if out:
    lines = out.splitlines()
    if len(lines) >= 4 and lines[0] == 'Result: Parcel(':
      # Process the UTF-16 string.
      chars = _parcel_to_list(lines[1:])[4:-1]
      return u''.join(map(unichr, (int(i, 16) for i in chars)))
  return None


def get_ip(device):
  """Returns the IP address."""
  assert isinstance(device, Device), device
  # If ever needed, read wifi.interface from /system/build.prop if a device
  # doesn't use wlan0.
  ip, _ = device.shell('getprop dhcp.wlan0.ipaddress')
  if not ip:
    return None
  return ip.strip()


def get_last_uid(device):
  """Returns the highest UID on the device."""
  assert isinstance(device, Device), device
  # pylint: disable=line-too-long
  # Applications are set in the 10000-19999 UID space. The UID are not reused.
  # So after installing 10000 apps, including stock apps, ignoring uninstalls,
  # then the device becomes unusable. Oops!
  # https://android.googlesource.com/platform/frameworks/base/+/master/api/current.txt
  # or:
  # curl -sSLJ \
  #   https://android.googlesource.com/platform/frameworks/base/+/master/api/current.txt?format=TEXT \
  #   | base64 --decode | grep APPLICATION_UID
  out = device.pull_content('/data/system/packages.list')
  if not out:
    return None
  return max(int(l.split(' ', 2)[1]) for l in out.splitlines() if len(l) > 2)


def list_packages(device):
  """Returns the list of packages installed."""
  out, _ = device.shell('pm list packages')
  if not out:
    return None
  return [l.split(':', 1)[1] for l in out.strip().splitlines()]


def install_apk(device, destdir, apk):
  """Installs apk to destdir directory."""
  assert isinstance(device, Device), device
  # TODO(maruel): Test.
  # '/data/local/tmp/'
  dest = posixpath.join(destdir, os.path.basename(apk))
  if not device.push(apk, dest):
    return False
  return device.shell('pm install -r %s' % pipes.quote(dest))[1] is 0


def uninstall_apk(device, package):
  """Uninstalls the package."""
  assert isinstance(device, Device), device
  return device.shell('pm uninstall %s' % pipes.quote(package))[1] is 0


def get_application_path(device, package):
  assert isinstance(device, Device), device
  # TODO(maruel): Test.
  out, _ = device.shell('pm path %s' % pipes.quote(package))
  return out.strip() if out else out


def get_application_version(device, package):
  assert isinstance(device, Device), device
  # TODO(maruel): Test.
  out, _ = device.shell('dumpsys package %s' % pipes.quote(package))
  return out


def wait_for_device(device, timeout=180):
  """Waits for the device to be responsive."""
  assert isinstance(device, Device), device
  start = time.time()
  while (time.time() - start) < timeout:
    try:
      if device.shell_raw('echo \'hi\'')[1] == 0:
        return True
    except device._ERRORS:
      pass
  return False


def push_keys(device):
  """Pushes all the keys on the file system to the device.

  This is necessary when the device just got wiped but still has authorization,
  as soon as it reboots it'd loose the authorization.

  It never removes a previously trusted key, only adds new ones. Saves writing
  to the device if unnecessary.
  """
  if not _ADB_KEYS_RAW:
    # E.g. adb was never run locally and initialize(None, None) was used.
    return False
  keys = set(_ADB_KEYS_RAW)
  assert all('\n' not in k for k in keys), keys
  old_content = device.pull_content('/data/misc/adb/adb_keys')
  if old_content:
    old_keys = set(old_content.strip().splitlines())
    if keys.issubset(old_keys):
      return True
    keys = keys | old_keys
  assert all('\n' not in k for k in keys), keys
  if device.shell('mkdir -p /data/misc/adb')[1] != 0:
    return False
  if device.shell('restorecon /data/misc/adb')[1] != 0:
    return False
  if not device.push_content(
      '/data/misc/adb/adb_keys', ''.join(k + '\n' for k in sorted(keys))):
    return False
  if device.shell('restorecon /data/misc/adb/adb_keys')[1] != 0:
    return False
  return True


def mkstemp(device, content, prefix='swarming', suffix=''):
  """Make a new temporary files with content.

  The random part of the name is guaranteed to not require quote.

  Returns None in case of failure.
  """
  # There's a small race condition in there but it's assumed only this process
  # is doing something on the device at this point.
  choices = string.ascii_letters + string.digits
  for _ in xrange(5):
    name = '/data/local/tmp/' + prefix + ''.join(
        random.choice(choices) for _ in xrange(5)) + suffix
    mode, _, _ = device.stat(name)
    if mode:
      continue
    if device.push_content(name, content):
      return name


def run_shell_wrapped(device, commands):
  """Creates a temporary shell script, runs it then return the data.

  This is needed when:
  - the expected command is more than ~500 characters
  - the expected output is more than 32k characters

  Returns:
    tuple(stdout and stderr merged, exit_code).
  """
  content = ''.join(l + '\n' for l in commands)
  script = mkstemp(device, content, suffix='.sh')
  if not script:
    return False
  try:
    outfile = mkstemp(device, '', suffix='.txt')
    if not outfile:
      return False
    try:
      _, exit_code = device.shell('sh %s &> %s' % (script, outfile))
      out = device.pull_content(outfile)
      return out, exit_code
    finally:
      device.shell('rm %s' % outfile)
  finally:
    device.shell('rm %s' % script)


def get_prop(device, prop):
  out, exit_code = device.shell('getprop %s' % pipes.quote(prop))
  if exit_code != 0:
    return None
  return out.rstrip()
