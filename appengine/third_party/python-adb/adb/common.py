# Copyright 2014 Google Inc. All rights reserved.
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
"""Common code for ADB and Fastboot.

Common usb browsing, and usb communication.
"""

import logging
import socket
import threading
import traceback

import libusb1
import usb1

from adb import usb_exceptions


DEFAULT_TIMEOUT_MS = 1000

_LOG = logging.getLogger('adb.usb')
_LOG.setLevel(logging.ERROR)


def GetInterface(setting):
  """Get the class, subclass, and protocol for the given USB setting."""
  return (setting.getClass(), setting.getSubClass(), setting.getProtocol())


def InterfaceMatcher(clazz, subclass, protocol):
  """Returns a matcher that returns the setting with the given interface."""
  interface = (clazz, subclass, protocol)
  def Matcher(device):
    for setting in device.iterSettings():
      if GetInterface(setting) == interface:
        return setting
  return Matcher


class UsbHandle(object):
  """USB communication object. Not thread-safe.

  Handles reading and writing over USB with the proper endpoints, exceptions,
  and interface claiming.

  Important methods:
    FlushBuffers()
    BulkRead(int length)
    BulkWrite(bytes data)
  """

  _HANDLE_CACHE = {}
  _HANDLE_CACHE_LOCK = threading.Lock()

  def __init__(self, device, setting, usb_info=None, timeout_ms=None):
    """Initialize USB Handle.

    Arguments:
      device: libusb_device to connect to.
      setting: libusb setting with the correct endpoints to communicate with.
      usb_info: String describing the usb path/serial/device, for debugging.
      timeout_ms: Timeout in milliseconds for all I/O.
    """
    # Immutable.
    self._setting = setting
    self._device = device
    self._usb_info = usb_info or ''
    self._timeout_ms = timeout_ms or DEFAULT_TIMEOUT_MS

    # State.
    self._handle = None
    self._port_path = None
    self._serial_number = None
    self._read_endpoint = None
    self._write_endpoint = None
    self._interface_number = None
    self._max_read_packet_len = None

  @property
  def usb_info(self):
    try:
      sn = self.serial_number
    except libusb1.USBError:
      sn = ''
    if sn and sn != self._usb_info and self._usb_info:
      return '%s %s' % (self._usb_info, sn)
    return self._usb_info

  def Open(self):
    """Opens the USB device for this setting, and claims the interface."""
    # Make sure we close any previous handle open to this usb device.
    port_path = self.port_path
    _LOG.info('%s.Open()', self.port_path_str)

    with self._HANDLE_CACHE_LOCK:
      # Safely recover from USB handle leaks.
      # TODO(maruel): Eventually turn this into an hard failure.
      previous = self._HANDLE_CACHE.pop(port_path, None)
      if previous:
        _LOG.error(
            '%s.Open(): Found already opened port:\n%s',
            self.port_path_str, previous[1])
        previous[0].Close()

    try:
      for endpoint in self._setting.iterEndpoints():
        address = endpoint.getAddress()
        if address & libusb1.USB_ENDPOINT_DIR_MASK:
          self._read_endpoint = address
          self._max_read_packet_len = endpoint.getMaxPacketSize()
        else:
          self._write_endpoint = address

      assert self._read_endpoint is not None
      assert self._write_endpoint is not None

      self._handle = self._device.open()
      self._interface_number = self._setting.getNumber()
      try:
        if self._handle.kernelDriverActive(self._interface_number):
          self._handle.detachKernelDriver(self._interface_number)
      except libusb1.USBError as e:
        if e.value == libusb1.LIBUSB_ERROR_NOT_FOUND:
          _LOG.warning(
              '%s.Open(): Kernel driver not found for interface: %s.',
              self.port_path_str, self._interface_number)
          self.Close()
        else:
          raise
      self._handle.claimInterface(self._interface_number)

      stack = ''.join(traceback.format_stack()[:-2])
      with self._HANDLE_CACHE_LOCK:
        self._HANDLE_CACHE[port_path] = (self, stack)
    except Exception as e:
      self.Close()
      raise

  @property
  def is_open(self):
    return bool(self._handle)

  @property
  def serial_number(self):
    if not self._serial_number:
      self._serial_number = self._device.getSerialNumber()
    return self._serial_number

  @property
  def port_path(self):
    if not self._port_path:
      self._port_path = (
          self._device.getBusNumber(), self._device.getDeviceAddress())
    return self._port_path

  @property
  def port_path_str(self):
    return '/'.join(str(p) for p in self.port_path)

  def Close(self):
    port_path = self.port_path
    _LOG.info('%s.Close()', self.port_path_str)
    with self._HANDLE_CACHE_LOCK:
      self._HANDLE_CACHE.pop(port_path, None)
    if self._handle is None:
      return
    try:
      if self._interface_number:
        self._handle.releaseInterface(self._interface_number)
      self._handle.close()
    except libusb1.USBError as e:
      _LOG.info('%s.Close(): USBError: %s', self.port_path_str, e)
    finally:
      self._handle = None
      self._read_endpoint = None
      self._write_endpoint = None
      self._interface_number = None
      self._max_read_packet_len = None

  def Timeout(self, timeout_ms):
    return timeout_ms if timeout_ms is not None else self._timeout_ms

  def FlushBuffers(self):
    while True:
      try:
        self.BulkRead(self._max_read_packet_len, timeout_ms=10)
      except usb_exceptions.ReadFailedError as e:
        if e.usb_error.value == libusb1.LIBUSB_ERROR_TIMEOUT:
          break
        raise

  def BulkWrite(self, data, timeout_ms=None):
    if self._handle is None:
      raise usb_exceptions.WriteFailedError(
          'This handle has been closed, probably due to another being opened.',
          None)
    try:
      return self._handle.bulkWrite(
          self._write_endpoint, data, timeout=self.Timeout(timeout_ms))
    except libusb1.USBError as e:
      raise usb_exceptions.WriteFailedError(
          'Could not send data to %s (timeout %sms)' % (
              self.usb_info, self.Timeout(timeout_ms)), e)

  def BulkRead(self, length, timeout_ms=None):
    if self._handle is None:
      raise usb_exceptions.ReadFailedError(
          'This handle has been closed, probably due to another being opened.',
          None)
    try:
      return self._handle.bulkRead(
          self._read_endpoint, length, timeout=self.Timeout(timeout_ms))
    except libusb1.USBError as e:
      raise usb_exceptions.ReadFailedError(
          'Could not receive data from %s (timeout %sms)' % (
              self.usb_info, self.Timeout(timeout_ms)), e)

  @classmethod
  def PortPathMatcher(cls, port_path):
    """Returns a device matcher for the given port path."""
    if isinstance(port_path, basestring):
      # Convert from sysfs path to port_path.
      port_path = [int(i) for i in port_path.split('/')]
    port_path = tuple(port_path)
    return lambda device: device.port_path == port_path

  @classmethod
  def SerialMatcher(cls, serial):
    """Returns a device matcher for the given serial."""
    return lambda device: device.serial_number == serial

  @classmethod
  def FindAndOpen(cls, setting_matcher,
                  port_path=None, serial=None, timeout_ms=None):
    dev = cls.Find(
        setting_matcher, port_path=port_path, serial=serial,
        timeout_ms=timeout_ms)
    dev.Open()
    dev.FlushBuffers()
    return dev

  @classmethod
  def Find(cls, setting_matcher, port_path=None, serial=None, timeout_ms=None):
    """Gets the first device that matches according to the keyword args."""
    if port_path:
      device_matcher = cls.PortPathMatcher(port_path)
      usb_info = port_path
    elif serial:
      device_matcher = cls.SerialMatcher(serial)
      usb_info = serial
    else:
      device_matcher = None
      usb_info = 'first'
    return cls.FindFirst(setting_matcher, device_matcher,
                         usb_info=usb_info, timeout_ms=timeout_ms)

  @classmethod
  def FindFirst(cls, setting_matcher, device_matcher=None, **kwargs):
    """Find and return the first matching device.

    Args:
      setting_matcher: See cls.FindDevices.
      device_matcher: See cls.FindDevices.
      **kwargs: See cls.FindDevices.

    Returns:
      An instance of UsbHandle.

    Raises:
      DeviceNotFoundError: Raised if the device is not available.
    """
    try:
      return next(cls.FindDevices(
          setting_matcher, device_matcher=device_matcher, **kwargs))
    except StopIteration:
      raise usb_exceptions.DeviceNotFoundError(
          'No device available, or it is in the wrong configuration.')

  @classmethod
  def FindDevices(cls, setting_matcher, device_matcher=None,
                  usb_info='', timeout_ms=None):
    """Find and yield the devices that match.

    Args:
      setting_matcher: Function that returns the setting to use given a
        usb1.USBDevice, or None if the device doesn't have a valid setting.
      device_matcher: Function that returns True if the given UsbHandle is
        valid. None to match any device.
      usb_info: Info string describing device(s).
      timeout_ms: Default timeout of commands in milliseconds.

    Yields:
      Unopened UsbHandle instances
    """
    ctx = usb1.USBContext()
    for device in ctx.getDeviceList(skip_on_error=True):
      setting = setting_matcher(device)
      if setting is None:
        continue

      handle = cls(device, setting, usb_info=usb_info, timeout_ms=timeout_ms)
      if device_matcher is None or device_matcher(handle):
        yield handle

  @classmethod
  def FindDevicesSafe(cls, *args, **kwargs):
    """Safe version of FindDevicesSafe.

    Use manual iterator handling instead of "for handle in generator" to catch
    USB exception explicitly.
    """
    generator = cls.FindDevices(*args, **kwargs)
    while True:
      try:
        handle = generator.next()
      except usb1.USBErrorOther as e:
        logging.error(
            'Failed to open USB device, is user in group plugdev? %s', e)
        continue
      yield handle


class TcpHandle(object):
  """TCP connection object.

     Provides same interface as UsbHandle but ignores timeout."""

  def __init__(self, serial):
    """Initialize the TCP Handle.
    Arguments:
      serial: Android device serial of the form host or host:port.

    Host may be an IP address or a host name.
    """
    if ':' in serial:
      (host, port) = serial.split(':')
    else:
      host = serial
      port = 5555
    self._serial_number = '%s:%s' % (host, port)

    self._connection = socket.create_connection((host, port))

  @property
  def serial_number(self):
    return self._serial_number

  def BulkWrite(self, data, timeout=None):  # pylint: disable=unused-argument
    return self._connection.sendall(data)

  def BulkRead(self, numbytes, timeout=None):  # pylint: disable=unused-argument
    return self._connection.recv(numbytes)

  def Timeout(self, timeout_ms):
    return timeout_ms

  def Close(self):
    return self._connection.close()
