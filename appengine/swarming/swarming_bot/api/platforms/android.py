# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Android specific utility functions.

This file serves as an API to bot_config.py. bot_config.py can be replaced on
the server to allow additional server-specific functionality.
"""

import collections
import logging
import os
import pipes
import posixpath
import random
import re
import string
import sys
import threading
import time


from adb import adb_commands_safe
from adb import common
from adb import sign_pythonrsa


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

  def __init__(self):
    self._lock = threading.Lock()
    # Keys is usb path, value is a _Cache.Device.
    self._per_device = {}

  def get(self, device):
    with self._lock:
      return self._per_device.get(device.port_path)

  def set(self, device, cache):
    with self._lock:
      self._per_device[device.port_path] = cache

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


def _init_cache(device):
  """Primes data known to be fetched soon right away that is static for the
  lifetime of the device.

  The data is cached in _per_device_cache() as long as the device is connected
  and responsive.
  """
  cache = _per_device_cache.get(device)
  if not cache:
    # TODO(maruel): This doesn't seem super useful since the following symlinks
    # already exist: /sdcard/, /mnt/sdcard, /storage/sdcard0.
    external_storage_path, exitcode = device.Shell('echo -n $EXTERNAL_STORAGE')
    if exitcode:
      external_storage_path = None

    properties = {}
    out = device.PullContent('/system/build.prop')
    if not out:
      properties = None
    else:
      for line in out.splitlines():
        if line.startswith(u'#') or not line:
          continue
        key, value = line.split(u'=', 1)
        properties[key] = value

    mode, _, _ = device.Stat('/system/xbin/su')
    has_su = bool(mode)

    available_governors = KNOWN_CPU_SCALING_GOVERNOR_VALUES
    out = device.PullContent(
        '/sys/devices/system/cpu/cpu0/cpufreq/scaling_available_governors')
    if out:
      available_governors = sorted(i for i in out.split())
      assert set(available_governors).issubset(
          KNOWN_CPU_SCALING_GOVERNOR_VALUES), available_governors

    cpuinfo_max_freq = device.PullContent(
        '/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq')
    if cpuinfo_max_freq:
      cpuinfo_max_freq = int(cpuinfo_max_freq)
    cpuinfo_min_freq = device.PullContent(
        '/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_min_freq')
    if cpuinfo_min_freq:
      cpuinfo_min_freq = int(cpuinfo_min_freq)

    cache = DeviceCache(
        properties, external_storage_path, has_su, available_governors,
        cpuinfo_max_freq, cpuinfo_min_freq)
    # Only save the cache if all the calls above worked.
    if all(i is not None for i in cache._asdict().itervalues()):
      _per_device_cache.set(device, cache)
  return cache


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


# DeviceCache is static information about a device that it preemptively
# initialized and that cannot change without formatting the device.
DeviceCache = collections.namedtuple(
    'DeviceCache',
    [
      # Cache of /system/build.prop on the Android device.
      'build_props',
      # Cache of $EXTERNAL_STORAGE_PATH.
      'external_storage_path',
      # /system/xbin/su exists.
      'has_su',
      # All the valid CPU scaling governors.
      'available_governors',
      # CPU frequency limits.
      'cpuinfo_max_freq',
      'cpuinfo_min_freq',
    ])


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


def open_device(handle, banner, rsa_keys, on_error):
  """Returns a HighDevice from an unopened common.UsbHandle.
  """
  device = adb_commands_safe.AdbCommandsSafe.ConnectHandle(
      handle, banner=banner, rsa_keys=rsa_keys, on_error=on_error)
  # Query the device, caching data upfront inside _per_device_cache so repeated
  # calls won't reload the cached items.
  return HighDevice(device, _init_cache(device))


def get_devices(bot):
  """Returns the list of devices available.

  Caller MUST call close_devices(devices) on the return value to close the USB
  handles.

  Returns one of:
    - list of HighDevice instances.
    - None if adb is unavailable.
  """
  if not _ADB_KEYS:
    return None

  # List of unopened common.UsbHandle.
  handles = []
  generator = common.UsbHandle.FindDevices(
      adb_commands_safe.DeviceIsAvailable, timeout_ms=60000)
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
    device = open_device(
        handle, 'swarming', _ADB_KEYS, bot.post_error if bot else None)
    # Opportinistically tries to switch adbd to run in root mode. This is
    # necessary for things like switching the CPU scaling governor to cool off
    # devices while idle.
    if device.cache.has_su and not device.is_root():
      device.reset_adbd_as_root()
    return device

  devices = parallel.pmap(fn, handles)
  _per_device_cache.trim(devices)
  return devices


def close_devices(devices):
  """Closes all devices opened by get_devices()."""
  for device in devices or []:
    device.Close()


class HighDevice(object):
  """High level device representation.

  This class contains all the methods that are effectively composite calls to
  the low level functionality provided by AdbCommandsSafe. As such there's no
  direct access by methods of this class to self.cmd.
  """
  def __init__(self, device, cache):
    self._device = device
    self._cache = cache

  # Proxy the embedded object methods.

  @property
  def cache(self):
    """Returns an instance of DeviceCache."""
    return self._cache

  @property
  def port_path(self):
    return self._device.port_path

  @property
  def serial(self):
    return self._device.serial

  @property
  def is_valid(self):
    return self._device.is_valid

  def Close(self):
    self._device.Close()

  def is_root(self):
    return self._device.IsRoot()

  def listdir(self, destdir):
    return self._device.List(destdir)

  def pull(self, *args, **kwargs):
    return self._device.Pull(*args, **kwargs)

  def pull_content(self, *args, **kwargs):
    return self._device.PullContent(*args, **kwargs)

  def push(self, *args, **kwargs):
    return self._device.Push(*args, **kwargs)

  def push_content(self, *args, **kwargs):
    return self._device.PushContent(*args, **kwargs)

  def reboot(self):
    return self._device.Reboot()

  def remount(self):
    return self._device.Remount()

  def reset_adbd_as_root(self):
    return self._device.Root()

  def reset_adbd_as_user(self):
    return self._device.Unroot()

  def shell(self, cmd):
    return self._device.Shell(cmd)

  def shell_raw(self, cmd):
    return self._device.ShellRaw(cmd)

  def stat(self, dest):
    return self._device.Stat(dest)

  def __repr__(self):
    return repr(self._device)

  # High level methods.

  def set_cpu_scaling_governor(self, governor):
    """Sets the CPU scaling governor to the one specified.

    Returns:
      True on success.
    """
    assert governor in KNOWN_CPU_SCALING_GOVERNOR_VALUES, governor
    if governor not in self.cache.available_governors:
      if governor == 'powersave':
        return self.set_cpu_speed(self.cache.cpuinfo_min_freq)
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
    prev = self.pull_content(path)
    if prev:
      prev = prev.strip()
      if prev == governor:
        return True
      if prev not in self.cache.available_governors:
        logging.error(
            '%s: Read invalid scaling_governor: %s', self.port_path, prev)

    # This works on Nexus 10 but not on Nexus 5. Need to investigate more. In
    # the meantime, simply try one after the other.
    if not self.push_content(path, governor + '\n'):
      # Fallback via shell.
      _, exit_code = self.shell('echo "%s" > %s' % (governor, path))
      if exit_code != 0:
        logging.error(
            '%s: Writing scaling_governor %s failed; was %s',
            self.port_path, governor, prev)
        return False
    # Get it back to confirm.
    newval = self.pull_content(path)
    if not (newval or '').strip() == governor:
      logging.error(
          '%s: Wrote scaling_governor %s; was %s; got %s',
          self.port_path, governor, prev, newval)
      return False
    return True

  def set_cpu_speed(self, speed):
    """Enforces strict CPU speed and disable the CPU scaling governor.

    Returns:
      True on success.
    """
    assert isinstance(speed, int), speed
    assert 10000 <= speed <= 10000000, speed
    success = self.set_cpu_scaling_governor('userspace')
    if not self.push_content(
        '/sys/devices/system/cpu/cpu0/cpufreq/scaling_setspeed', '%d\n' %
        speed):
      return False
    # Get it back to confirm.
    val = self.pull_content(
        '/sys/devices/system/cpu/cpu0/cpufreq/scaling_setspeed')
    return success and (val or '').strip() == str(speed)

  def get_temperatures(self):
    """Returns the device's 2 temperatures if available.

    Returns None otherwise.
    """
    # Not all devices export this file. On other devices, the only real way to
    # read it is via Java
    # developer.android.com/guide/topics/sensors/sensors_environment.html
    temps = []
    try:
      for i in xrange(2):
        out = self.pull_content('/sys/class/thermal/thermal_zone%d/temp' % i)
        temps.append(int(out))
    except (TypeError, ValueError):
      return None
    return temps

  def get_battery(self):
    """Returns details about the battery's state."""
    props = {}
    out = self.dumpsys('battery')
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

  def get_cpu_scale(self):
    """Returns the CPU scaling factor."""
    mapping = {
      'scaling_cur_freq': u'cur',
      'scaling_governor': u'governor',
    }
    out = {
      'max': self.cache.cpuinfo_max_freq,
      'min': self.cache.cpuinfo_min_freq,
    }
    out.update(
      (v, self.pull_content('/sys/devices/system/cpu/cpu0/cpufreq/' + k))
      for k, v in mapping.iteritems())
    return out

  def get_uptime(self):
    """Returns the device's uptime in second."""
    out = self.pull_content('/proc/uptime')
    if out:
      return float(out.split()[1])
    return None

  def get_disk(self):
    """Returns details about the battery's state."""
    props = {}
    out = self.dumpsys('diskstats')
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

  def get_imei(self):
    """Returns the phone's IMEI."""
    # Android <5.0.
    out = self.dumpsys('iphonesubinfo')
    if out:
      match = re.search('  Device ID = (.+)$', out)
      if match:
        return match.group(1)

    # Android >= 5.0.
    out, _ = self.shell('service call iphonesubinfo 1')
    if out:
      lines = out.splitlines()
      if len(lines) >= 4 and lines[0] == 'Result: Parcel(':
        # Process the UTF-16 string.
        chars = _parcel_to_list(lines[1:])[4:-1]
        return u''.join(map(unichr, (int(i, 16) for i in chars)))
    return None

  def get_ip(self):
    """Returns the IP address."""
    # If ever needed, read wifi.interface from /system/build.prop if a device
    # doesn't use wlan0.
    ip, _ = self.shell('getprop dhcp.wlan0.ipaddress')
    if not ip:
      return None
    return ip.strip()

  def get_last_uid(self):
    """Returns the highest UID on the device."""
    # pylint: disable=line-too-long
    # Applications are set in the 10000-19999 UID space. The UID are not reused.
    # So after installing 10000 apps, including stock apps, ignoring uninstalls,
    # then the device becomes unusable. Oops!
    # https://android.googlesource.com/platform/frameworks/base/+/master/api/current.txt
    # or:
    # curl -sSLJ \
    #   https://android.googlesource.com/platform/frameworks/base/+/master/api/current.txt?format=TEXT \
    #   | base64 --decode | grep APPLICATION_UID
    out = self.pull_content('/data/system/packages.list')
    if not out:
      return None
    return max(int(l.split(' ', 2)[1]) for l in out.splitlines() if len(l) > 2)

  def list_packages(self):
    """Returns the list of packages installed."""
    out, _ = self.shell('pm list packages')
    if not out:
      return None
    return [l.split(':', 1)[1] for l in out.strip().splitlines()]

  def install_apk(self, destdir, apk):
    """Installs apk to destdir directory."""
    # TODO(maruel): Test.
    # '/data/local/tmp/'
    dest = posixpath.join(destdir, os.path.basename(apk))
    if not self.push(apk, dest):
      return False
    return self.shell('pm install -r %s' % pipes.quote(dest))[1] is 0

  def uninstall_apk(self, package):
    """Uninstalls the package."""
    return self.shell('pm uninstall %s' % pipes.quote(package))[1] is 0

  def get_application_path(self, package):
    # TODO(maruel): Test.
    out, _ = self.shell('pm path %s' % pipes.quote(package))
    return out.strip() if out else out

  def get_application_version(self, package):
    # TODO(maruel): Test.
    out, _ = self.shell('dumpsys package %s' % pipes.quote(package))
    return out

  def wait_for_device(self, timeout=180):
    """Waits for the device to be responsive."""
    start = time.time()
    while (time.time() - start) < timeout:
      try:
        if self.shell_raw('echo \'hi\'')[1] == 0:
          return True
      except self._ERRORS:
        pass
    return False

  def push_keys(self):
    """Pushes all the keys on the file system to the device.

    This is necessary when the device just got wiped but still has
    authorization, as soon as it reboots it'd lose the authorization.

    It never removes a previously trusted key, only adds new ones. Saves writing
    to the device if unnecessary.
    """
    if not _ADB_KEYS_RAW:
      # E.g. adb was never run locally and initialize(None, None) was used.
      return False
    keys = set(_ADB_KEYS_RAW)
    assert all('\n' not in k for k in keys), keys
    old_content = self.pull_content('/data/misc/adb/adb_keys')
    if old_content:
      old_keys = set(old_content.strip().splitlines())
      if keys.issubset(old_keys):
        return True
      keys = keys | old_keys
    assert all('\n' not in k for k in keys), keys
    if self.shell('mkdir -p /data/misc/adb')[1] != 0:
      return False
    if self.shell('restorecon /data/misc/adb')[1] != 0:
      return False
    if not self.push_content(
        '/data/misc/adb/adb_keys', ''.join(k + '\n' for k in sorted(keys))):
      return False
    if self.shell('restorecon /data/misc/adb/adb_keys')[1] != 0:
      return False
    return True

  def mkstemp(self, content, prefix='swarming', suffix=''):
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
      mode, _, _ = self.stat(name)
      if mode:
        continue
      if self.push_content(name, content):
        return name

  def run_shell_wrapped(self, commands):
    """Creates a temporary shell script, runs it then return the data.

    This is needed when:
    - the expected command is more than ~500 characters
    - the expected output is more than 32k characters

    Returns:
      tuple(stdout and stderr merged, exit_code).
    """
    content = ''.join(l + '\n' for l in commands)
    script = self.mkstemp(content, suffix='.sh')
    if not script:
      return False
    try:
      outfile = self.mkstemp('', suffix='.txt')
      if not outfile:
        return False
      try:
        _, exit_code = self.shell('sh %s &> %s' % (script, outfile))
        out = self.pull_content(outfile)
        return out, exit_code
      finally:
        self.shell('rm %s' % outfile)
    finally:
      self.shell('rm %s' % script)

  def get_prop(self, prop):
    out, exit_code = self.shell('getprop %s' % pipes.quote(prop))
    if exit_code != 0:
      return None
    return out.rstrip()

  def dumpsys(self, arg):
    """dumpsys is a native android tool that returns inconsistent semi
    structured data.

    It acts as a directory service but each service return their data without
    any real format, and will happily return failure.
    """
    out, exit_code = self.shell('dumpsys ' + arg)
    if exit_code != 0 or out.startswith('Can\'t find service: '):
      return None
    return out
