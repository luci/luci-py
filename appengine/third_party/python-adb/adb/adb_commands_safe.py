# Copyright 2015 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Defines AdbCommandsSafe, an exception safe version of AdbCommands."""

import cStringIO
import inspect
import logging
import socket
import subprocess
import time


from adb import adb_commands
from adb import common
from adb import usb_exceptions


### Public API.


# Make adb_commands_safe a drop-in replacement for adb_commands.
from adb.adb_commands import DeviceIsAvailable


def KillADB():
  """Stops the adb daemon.

  It's possible that adb daemon could be alive on the same host where python-adb
  is used. The host keeps the USB devices open so it's not possible for other
  processes to open it. Gently stop adb so this process can access the USB
  devices.

  adb's stability is less than stellar. Kill it with fire.
  """
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


class AdbCommandsSafe(object):
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

  def __init__(
      self, port_path, handle, banner, rsa_keys, on_error,
      default_timeout_ms=10000, auth_timeout_ms=10000):
    """Constructs an AdbCommandsSafe.

    Arguments:
    - port_path: str addressing the device on the USB bus, e.g. '1/2'.
    - handle: common.UsbHandle or None.
    - banner: How the app present itself to the device. This affects
          authentication so it is better to use an hardcoded constant.
    - rsa_keys: list of AuthSigner.
    - on_error: callback to call in case of error.
    - default_timeout_ms: Timeout for adbd to reply to a command.
    - auth_timeout_ms: Timeout for the user to accept the dialog.
    """
    assert isinstance(auth_timeout_ms, int), auth_timeout_ms
    assert isinstance(default_timeout_ms, int), default_timeout_ms
    assert isinstance(banner, str), banner
    assert on_error is None or callable(on_error), on_error
    assert all(isinstance(p, int) for p in port_path), port_path
    assert handle is None or isinstance(handle, common.UsbHandle), handle

    # Immutable.
    self._auth_timeout_ms = auth_timeout_ms
    self._default_timeout_ms = default_timeout_ms
    self._banner = banner or socket.gethostname()
    self._on_error = on_error
    self._port_path = '/'.join(map(str, port_path))
    self._rsa_keys = rsa_keys

    # State.
    self._adb_cmd = None
    self._serial = None
    self._handle = handle
    self._has_reset = False
    self._tries = 1

  @classmethod
  def ConnectDevice(cls, port_path, **kwargs):
    """Return a AdbCommandsSafe for a USB device referenced by the port path.

    Arguments:
    - port_path: str in form '1/2' to refer to a connected but unopened USB
          device.
    - The rest are the same as __init__().
    """
    obj = cls(port_path=port_path, handle=None, **kwargs)
    obj._Find()
    obj._OpenHandle()
    obj._Connect()
    return obj

  @classmethod
  def ConnectHandle(cls, handle, **kwargs):
    """Return a AdbCommandsSafe for a USB device referenced by the unopened
    handle.

    Arguments:
    - handle: an *unopened* common.UsbHandle.
    - The rest are the same as __init__().
    """
    obj = cls(port_path=handle.port_path, handle=handle, **kwargs)
    obj._OpenHandle()
    obj._Connect()
    return obj

  @classmethod
  def Connect(cls, handle, **kwargs):
    """Return a AdbCommandsSafe for a USB device referenced by the opened
    handle.

    Arguments:
    - handle: an *opened* common.UsbHandle.
    - The rest are the same as __init__().

    Returns:
      AdbCommandsSafe.
    """
    obj = cls(port_path=handle.port_path, handle=handle, **kwargs)
    obj._Connect()
    return obj

  @property
  def is_valid(self):
    return bool(self._adb_cmd)

  @property
  def port_path(self):
    return self._port_path

  @property
  def serial(self):
    return self._serial or self._port_path

  def Close(self):
    if self._adb_cmd:
      self._adb_cmd.Close()
      self._adb_cmd = None
      self._handle = None
    elif self._handle:
      self._handle.Close()
      self._handle = None

  def List(self, destdir):
    """List a directory on the device."""
    assert destdir.startswith('/'), destdir
    if self._adb_cmd:
      for _ in xrange(self._tries):
        try:
          return self._adb_cmd.List(destdir)
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s): %s', destdir, e)
    return None

  def Stat(self, dest):
    """Stats a file/dir on the device. It's likely faster than shell().

    Returns:
      tuple(mode, size, mtime)
    """
    assert dest.startswith('/'), dest
    if self._adb_cmd:
      for _ in xrange(self._tries):
        try:
          return self._adb_cmd.Stat(dest)
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s): %s', dest, e)
    return None, None, None

  def Pull(self, remotefile, dest):
    """Retrieves a file from the device to dest on the host.

    Returns True on success.
    """
    assert remotefile.startswith('/'), remotefile
    if self._adb_cmd:
      for _ in xrange(self._tries):
        try:
          self._adb_cmd.Pull(remotefile, dest)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s, %s): %s', remotefile, dest, e)
    return False

  def PullContent(self, remotefile):
    """Reads a file from the device.

    Returns content on success as str, None on failure.
    """
    assert remotefile.startswith('/'), remotefile
    if self._adb_cmd:
      # TODO(maruel): Distinction between file is not present and I/O error.
      for _ in xrange(self._tries):
        try:
          return self._adb_cmd.Pull(remotefile, None)
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s): %s', remotefile, e)
    return None

  def Push(self, localfile, dest, mtime='0'):
    """Pushes a local file to dest on the device.

    Returns True on success.
    """
    assert dest.startswith('/'), dest
    if self._adb_cmd:
      for _ in xrange(self._tries):
        try:
          self._adb_cmd.Push(localfile, dest, mtime)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s, %s): %s', localfile, dest, e)
    return False

  def PushContent(self, dest, content, mtime='0'):
    """Writes content to dest on the device.

    Returns True on success.
    """
    assert dest.startswith('/'), dest
    if self._adb_cmd:
      for _ in xrange(self._tries):
        try:
          self._adb_cmd.Push(cStringIO.StringIO(content), dest, mtime)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(%s, %s): %s', dest, content, e)
    return False

  def Reboot(self):
    """Reboot the device. Doesn't wait for it to be rebooted"""
    if self._adb_cmd:
      for _ in xrange(self._tries):
        try:
          # Use 'bootloader' to switch to fastboot.
          out = self._adb_cmd.Reboot()
          logging.info('reboot: %s', out)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(): %s', e)
    return False

  def Remount(self):
    """Remount / as read-write."""
    if self._adb_cmd:
      for _ in xrange(self._tries):
        try:
          out = self._adb_cmd.Remount()
          logging.info('remount: %s', out)
          return True
        except usb_exceptions.AdbCommandFailureException:
          break
        except self._ERRORS as e:
          self._try_reset('(): %s', e)
    return False

  def Shell(self, cmd):
    """Runs a command on an Android device while swallowing exceptions.

    Traps all kinds of USB errors so callers do not have to handle this.

    Returns:
      tuple(stdout, exit_code)
      - stdout is as unicode if it ran, None if an USB error occurred.
      - exit_code is set if ran.
    """
    for _ in xrange(self._tries):
      try:
        return self.ShellRaw(cmd)
      except self._ERRORS as e:
        self._try_reset('(%s): %s', cmd, e)
    return None, None

  def ShellRaw(self, cmd):
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
    if not self._adb_cmd:
      return None, None
    # The adb protocol doesn't return the exit code, so embed it inside the
    # command.
    complete_cmd = cmd + ' ;echo -e "\n$?"'
    assert len(complete_cmd) <= 512, 'Command is too long: %s' % complete_cmd
    out = self._adb_cmd.Shell(complete_cmd).decode('utf-8', 'replace')
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

  def Root(self):
    """If adbd on the device is not root, ask it to restart as root.

    This causes the USB device to disapear momentarily, which causes a big mess,
    as we cannot communicate with it for a moment. So try to be clever and
    reenumerate the device until the device is back, then reinitialize the
    communication, all synchronously.
    """
    for _ in xrange(self._tries):
      try:
        out = self._adb_cmd.Root()
        logging.info('reset_adbd_as_root() = %s', out)
        # Hardcoded strings in platform_system_core/adb/services.cpp
        if out == 'adbd is already running as root\n':
          return True
        if out == 'adbd cannot run as root in production builds\n':
          return False
        # 'restarting adbd as root\n'
        if not self.IsRoot():
          logging.error('Failed to id after reset_adbd_as_root()')
          return False
        return True
      except self._ERRORS as e:
        self._try_reset('(): %s', e)
    return False

  def Unroot(self):
    """If adbd on the device is root, ask it to restart as user."""
    for _ in xrange(self._tries):
      try:
        # It's defined in the adb code but doesn't work on 4.4.
        #out = self._adb_cmd.conn.Command(service='unroot')
        out = self.cmd.Shell(
            'setprop service.adb.root 0; setprop ctl.restart adbd')
        logging.info('reset_adbd_as_user() = %s', out)
        # Hardcoded strings in platform_system_core/adb/services.cpp
        # 'adbd not running as root\n' or 'restarting adbd as non root\n'
        if self.IsRoot():
          logging.error('Failed to id after reset_adbd_as_user()')
          return False
        return True
      except self._ERRORS as e:
        self._try_reset('(): %s', e)
    return False

  def IsRoot(self):
    """Returns True if adbd is running as root.

    Returns None if it can't give a meaningful answer.

    Technically speaking this function is "high level" but is needed because
    reset_adbd_as_*() calls are asynchronous, so there is a race condition while
    adbd triggers the internal restart and its socket waiting for new
    connections; the previous (non-switched) server may accept connection while
    it is shutting down so it is important to repeatedly query until connections
    go to the new restarted adbd process.
    """
    out, exit_code = self.Shell('id')
    if exit_code != 0 or not out:
      return None
    return out.startswith('out=0(root)')

  def _Find(self):
    """Initializes self._handle from self._port_path."""
    assert not self._handle
    assert not self._adb_cmd
    # TODO(maruel): Add support for TCP/IP communication.
    self._handle = common.UsbHandle.Find(
        adb_commands.DeviceIsAvailable, port_path=self._port_path,
        timeout_ms=self._default_timeout_ms)

  def _OpenHandle(self):
    """Opens the unopened self._handle."""
    if self._handle:
      try:
        # If this succeeds, this initializes self._handle._handle, which is a
        # usb1.USBDeviceHandle.
        self._handle.Open()
      except common.usb1.USBErrorNoDevice as e:
        logging.warning('Got USBErrorNoDevice for %s: %s', self._port_path, e)
        # Do not kill adb, it just means the USB host is likely resetting and
        # the device is temporarily unavailable. We can't use
        # handle.serial_number since this communicates with the device.
        # TODO(maruel): In practice we'd like to retry for a few seconds.
        self._handle = None
      except common.usb1.USBErrorBusy as e:
        logging.warning(
            'Got USBErrorBusy for %s. Killing adb: %s', self._port_path, e)
        KillADB()
        try:
          # If it throws again, it probably means another process holds a handle
          # to the USB ports or group acl (plugdev) hasn't been setup properly.
          self._handle.Open()
        except common.usb1.USBErrorBusy as e:
          logging.warning(
              'USB port for %s is already open (and failed to kill ADB) '
              'Try rebooting the host: %s', self._port_path, e)
          self._handle = None
      except common.usb1.USBErrorAccess as e:
        # Do not try to use serial_number, since we can't even access the port.
        logging.warning('Try rebooting the host: %s: %s', self._port_path, e)
        self._handle = None
    return bool(self._handle)

  def _Connect(self):
    """Initializes self._adb_cmd from the opened self._handle."""
    assert not self._adb_cmd
    if self._handle:
      # On the first access with an open handle, try to set self._serial to the
      # serial number of the device. This means communicating to the USB
      # device, so it may throw.
      if not self._serial:
        try:
          # The serial number is attached to common.UsbHandle, no
          # adb_commands.AdbCommands.
          self._serial = self._handle.serial_number
        except self._ERRORS as e:
          self._try_reset('(): %s', e)

      try:
        # TODO(maruel): A better fix would be to change python-adb to continue
        # the authentication dance from where it stopped. This is left as a
        # follow up.
        self._adb_cmd = adb_commands.AdbCommands.Connect(
            self._handle, banner=self._banner, rsa_keys=self._rsa_keys,
            auth_timeout_ms=self._auth_timeout_ms)
      except usb_exceptions.DeviceAuthError as e:
        logging.warning('AUTH FAILURE: %s: %s', self._port_path, e)
      except usb_exceptions.LibusbWrappingError as e:
        logging.warning('I/O FAILURE: %s: %s', self._port_path, e)
      finally:
        # Do not leak the USB handle when we can't talk to the device.
        if not self._adb_cmd:
          self._handle.Close()
          self._handle = None
    return bool(self._adb_cmd)

  def _try_reset(self, fmt, *args):
    """When a self._ERRORS occurred, try to reset the device."""
    items = [self.port_path, inspect.stack()[1][3]]
    items.extend(args)
    msg = ('%s.%s' + fmt) % tuple(items)
    logging.error(msg)
    if self._on_error:
      self._on_error(msg)
    if not self._has_reset:
      self._has_reset = True
      # TODO(maruel): self._Reset()
    return msg

  def _Reset(self):
    """Resets the adbd and USB connections with a new connection."""
    self.Close()
    self._Find()
    self._OpenHandle()
    self._Connect()

  def __repr__(self):
    return '<Device %s %s>' % (
        self.port_path, self.serial if self.is_valid else '(broken)')
