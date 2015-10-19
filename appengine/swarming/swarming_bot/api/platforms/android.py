# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Android specific utility functions.

This file serves as an API to bot_config.py. bot_config.py can be replaced on
the server to allow additional server-specific functionality.
"""

import inspect
import logging
import os
import re
import subprocess
import sys
import time

# This file must be imported from swarming_bot.zip (or with '../..' in
# sys.path). libusb1 must have been put in the path already.

import adb
import adb.adb_commands


# Find abs path to third_party/ dir in the bot root dir.
import third_party
THIRD_PARTY_DIR = os.path.dirname(os.path.abspath(third_party.__file__))
sys.path.insert(0, os.path.join(THIRD_PARTY_DIR, 'rsa'))
sys.path.insert(0, os.path.join(THIRD_PARTY_DIR, 'pyasn1'))

import rsa

from pyasn1.codec.der import decoder
from pyasn1.type import univ
from rsa import pkcs1


### Private stuff.


# python-rsa lib hashes all messages it signs. ADB does it already, we just
# need to slap a signature on top of already hashed message. Introduce "fake"
# hashing algo for this.
class _Accum(object):
  def __init__(self):
    self._buf = ''
  def update(self, msg):
    self._buf += msg
  def digest(self):
    return self._buf
pkcs1.HASH_METHODS['SHA-1-PREHASHED'] = _Accum
pkcs1.HASH_ASN1['SHA-1-PREHASHED'] = pkcs1.HASH_ASN1['SHA-1']


# Both following are set when ADB is initialized.
# _ADB_KEYS is set to a list of _PythonRSASigner instances. It contains one or
# multiple key used to authenticate to Android debug protocol (adb).
_ADB_KEYS = None
# _ADB_KEYS_RAW is set to a dict(pub: priv) when initialized, with the raw
# content of each key.
_ADB_KEYS_RAW = None


# Cache of /system/build.prop on Android devices connected to this host.
_BUILD_PROP_ANDROID = {}


class _PythonRSASigner(object):
  """Implements adb_protocol.AuthSigner using http://stuvel.eu/rsa."""
  def __init__(self, pub, priv):
    self.priv_key = _load_rsa_private_key(priv)
    self.pub_key = pub

  def Sign(self, data):
    return rsa.sign(data, self.priv_key, 'SHA-1-PREHASHED')

  def GetPublicKey(self):
    return self.pub_key


def _load_rsa_private_key(pem):
  """PEM encoded PKCS#8 private key -> rsa.PrivateKey."""
  # ADB uses private RSA keys in pkcs#8 format. 'rsa' library doesn't support
  # them natively. Do some ASN unwrapping to extract naked RSA key
  # (in der-encoded form). See https://www.ietf.org/rfc/rfc2313.txt.
  # Also http://superuser.com/a/606266.
  try:
    der = rsa.pem.load_pem(pem, 'PRIVATE KEY')
    keyinfo, _ = decoder.decode(der)
    if keyinfo[1][0] != univ.ObjectIdentifier('1.2.840.113549.1.1.1'):
        raise ValueError('Not a DER-encoded OpenSSL private RSA key')
    private_key_der = keyinfo[2].asOctets()
  except IndexError:
    raise ValueError('Not a DER-encoded OpenSSL private RSA key')
  return rsa.PrivateKey.load_pkcs1(private_key_der, format='DER')


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
  assert isinstance(handle, adb.common.UsbHandle), handle
  port_path = '/'.join(map(str, handle.port_path))
  try:
    # If this succeeds, this initializes handle._handle, which is a
    # usb1.USBDeviceHandle.
    handle.Open()
  except adb.common.usb1.USBErrorNoDevice as e:
    logging.warning('Got USBErrorNoDevice for %s: %s', port_path, e)
    # Do not kill adb, it just means the USB host is likely resetting and the
    # device is temporarily unavailable.We can't use handle.serial_number since
    # this communicates with the device.
    # TODO(maruel): In practice we'd like to retry for a few seconds.
    handle = None
  except adb.common.usb1.USBErrorBusy as e:
    logging.warning('Got USBErrorBusy for %s. Killing adb: %s', port_path, e)
    kill_adb()
    try:
      # If it throws again, it probably means another process holds a handle to
      # the USB ports or group acl (plugdev) hasn't been setup properly.
      handle.Open()
    except adb.common.usb1.USBErrorBusy as e:
      logging.warning(
          'USB port for %s is already open (and failed to kill ADB) '
          'Try rebooting the host: %s', port_path, e)
      handle = None
  except adb.common.usb1.USBErrorAccess as e:
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
      cmd = adb.adb_commands.AdbCommands.Connect(
          handle, banner='swarming', rsa_keys=_ADB_KEYS,
          auth_timeout_ms=10000)
    except adb.usb_exceptions.DeviceAuthError as e:
      logging.warning('AUTH FAILURE: %s: %s', port_path, e)
    except adb.usb_exceptions.ReadFailedError as e:
      logging.warning('READ FAILURE: %s: %s', port_path, e)
    except adb.usb_exceptions.WriteFailedError as e:
      logging.warning('WRITE FAILURE: %s: %s', port_path, e)
    except ValueError as e:
      # This is generated when there's a protocol level failure, e.g. the data
      # is garbled.
      logging.warning(
          'Trying unpluging and pluging it back: %s: %s', port_path, e)
    finally:
      # Do not leak the USB handle when we can't talk to the device.
      if not cmd:
        handle.Close()
  # Always create a Device, even if it points to nothing. It makes using the
  # client code much easier.
  return Device(bot, cmd, port_path)


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
  # - ValueError means that there was a protocol level failure. For example the
  #   returned data is garbage.
  _ERRORS = (
    adb.usb_exceptions.CommonUsbError, adb.common.libusb1.USBError, ValueError)

  def __init__(self, bot, adb_cmd, port_path):
    if adb_cmd:
      assert isinstance(adb_cmd, adb.adb_commands.AdbCommands), adb_cmd
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

  def shell(self, cmd):
    """Runs a command on an Android device while swallowing exceptions.

    Traps all kinds of USB errors so callers do not have to handle this.

    Returns:
      tuple(stdout, exit_code)
      - stdout is as unicode if it ran, None if an USB error occurred.
      - exit_code is set if ran.
    """
    try:
      return self.shell_raw(cmd)
    except (
        adb.usb_exceptions.CommonUsbError,
        adb.common.libusb1.USBError,
        ValueError) as e:
      # Trap all USB exceptions.
      # - CommonUsbError means that device I/O failed, e.g. a write or a read
      #   call returned an error.
      # - USBError means that a bus I/O failed, e.g. the device path is not
      #   present anymore.
      # - ValueError means that there was a protocol level failure. For example
      #   the returned data is garbage.
      # In each case, try to do a device reset, it may help. Sadly, this likely
      # mean that the device may not be accessible anymore until the next
      # discovery but it's better than leaving it in a bad state.
      logging.error('%s.shell(%s): %s', self.serial, cmd, e)
      if not self._has_reset:
        self._has_reset = True
        # TODO(maruel): Intentionally do not trap exceptions here, as we want to
        # see how it (mis-)behaves in the field.
        # http://libusb.org/static/api-1.0/group__dev.html#ga7321bd8dc28e9a20b411bf18e6d0e9aa
        self.adb_cmd.handle.resetDevice()
        self.close()
      return None, None

  def shell_raw(self, cmd):
    """Runs a command on an Android device.

    Returns:
      tuple(stdout, exit_code)
      - stdout is as unicode if it ran, None if an USB error occurred.
      - exit_code is set if ran.
    """
    if not self.adb_cmd:
      return None, None
    # The adb protocol doesn't return the exit code, so embed it inside the
    # command.
    out = self.adb_cmd.Shell(cmd + ' ; echo $?').decode('utf-8', 'replace')
    # Protect against & or other bash conditional execution that wouldn't make
    # the 'echo $?' command to run.
    if not out:
      return out, None
    # TODO(maruel): Remove and handle if this is ever trapped.
    assert out[-1] == '\n', out
    # Strip the last line to extract the exit code.
    parts = out[:-1].rsplit('\n', 1)
    if len(parts) == 2:
      return parts[0] + '\n', int(parts[1])
    # The command itself generated no output.
    return '', int(parts[0])

  def _set_serial_number(self):
    """Tries to set self.serial to the serial number of the device.

    This means communicating to the USB device, so it may throw.
    """
    for _ in xrange(self._tries):
      try:
        # The serial number is attached to adb.common.UsbHandle, no
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
    _ADB_KEYS.append(_PythonRSASigner(pub_key, priv_key))
    _ADB_KEYS_RAW[pub_key] = priv_key

  # Try to add local adb keys if available.
  path = os.path.expanduser('~/.android/adbkey')
  if os.path.isfile(path) and os.path.isfile(path + '.pub'):
    with open(path + '.pub', 'rb') as f:
      pub = f.read().strip()
    with open(path, 'rb') as f:
      priv = f.read().strip()
    _ADB_KEYS.append(_PythonRSASigner(pub, priv))
    _ADB_KEYS_RAW[pub] = priv

  return bool(_ADB_KEYS)


def kill_adb():
  """Stops the adb daemon.

  The Swarming bot doesn't use the Android's SDK adb. It uses python-adb to
  directly communicate with the devices. This works much better when a lot of
  devices are connected to the host.

  adb's stability is less than stellar. Kill it with fire.
  """
  if not adb:
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
  if not adb or not _ADB_KEYS:
    return None

  handles = []
  generator = adb.common.UsbHandle.FindDevices(
      adb.adb_commands.DeviceIsAvailable, timeout_ms=60000)
  while True:
    try:
      # Use manual iterator handling instead of "for handle in generator" to
      # catch USB exception explicitly.
      handle = generator.next()
    except adb.common.usb1.USBErrorOther as e:
      logging.warning(
          'Failed to open USB device, is user in group plugdev? %s', e)
      continue
    except StopIteration:
      break
    handles.append(handle)

  devices = []
  for handle in handles:
    # Open the device and do the initial adb-connect.
    device = _load_device(bot, handle)
    devices.append(device)

  # Remove any /system/build.prop cache so if a device is disconnected,
  # reflashed then reconnected, the cache isn't invalid.
  for key in _BUILD_PROP_ANDROID.keys():
    dev = devices.get(key)
    if not dev or not dev.is_valid:
      _BUILD_PROP_ANDROID.pop(key)
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
  _, exit_code = device.shell(
      'echo "%s">/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor' %
      governor)
  return exit_code == 0


def set_cpu_scaling(device, speed):
  """Enforces strict CPU speed and disable the CPU scaling governor.

  Returns:
    True on success.
  """
  assert isinstance(device, Device), device
  assert isinstance(speed, int), speed
  assert 10000 <= speed <= 10000000, speed
  success = set_cpu_scaling_governor(device, 'userspace')
  _, exit_code = device.shell(
      'echo "%d">/sys/devices/system/cpu/cpu0/cpufreq/scaling_setspeed' %
      speed)
  return success and exit_code == 0


def get_build_prop(device):
  """Returns the system properties for a device.

  This isn't really changing through the lifetime of a bot. One corner case is
  when the device is flashed or disconnected.
  """
  if device.serial not in _BUILD_PROP_ANDROID:
    properties = {}
    out, _ = device.shell('cat /system/build.prop')
    if not out:
      properties = None
    else:
      for line in out.splitlines():
        if line.startswith(u'#') or not line:
          continue
        key, value = line.split(u'=', 1)
        properties[key] = value
    _BUILD_PROP_ANDROID[device.serial] = properties
  return _BUILD_PROP_ANDROID[device.serial]


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
      out, _ = device.shell('cat /sys/class/thermal/thermal_zone%d/temp' % i)
      temps.append(int(out))
  except ValueError:
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
    out[key] = int(props[key])
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
  return {
    v: device.shell('cat /sys/devices/system/cpu/cpu0/cpufreq/' + k)[0]
    for k, v in mapping.iteritems()
  }


def get_uptime(device):
  """Returns the device's uptime in second."""
  assert isinstance(device, Device), device
  out, _ = device.shell('cat /proc/uptime')
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
