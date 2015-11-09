"""Stubs for tests using common's usb handling."""

import binascii
import string
import threading

from adb import adb_protocol
from adb import usb_exceptions


PRINTABLE_DATA = set(string.printable) - set(string.whitespace)


class Failure(Exception):
  pass


def _Dotify(data):  # pragma: no cover
  # This code is not exercised unless there's a test case failure.
  try:
    return adb_protocol._AdbMessageHeader.Unpack(data)
  except adb_protocol.InvalidResponseError:
    return ''.join(char if char in PRINTABLE_DATA else '.' for char in data)


class MockUsb(object):
  """UsbHandle mock."""

  def __init__(self):
    # Immutable.
    self.timeout_ms = 0
    self.port_path = 'stub'

    # Mutable.
    self._lock = threading.Lock()
    self._expected_io = []

  def Close(self):
    with self._lock:
      assert not self._expected_io, 'Expected I/O not processed:\n' + '\n'.join(
          '- %s: %s' % (i[0], _Dotify(i[1])) for i in self._expected_io)

  def BulkWrite(self, data, unused_timeout_ms=None):
    with self._lock:
      if not self._expected_io:
        raise Failure('No more excepted I/O')  # pragma: no cover
      if self._expected_io[0][0] != 'write':
        raise Failure('I/O mismatch:\n- expected read %s\n- got write %s' %
            (_Dotify(self._expected_io[0][1]),
              _Dotify(data)))  # pragma: no cover

      expected_data = self._expected_io.pop(0)[1]
      if expected_data != data:
        raise Failure('Mismatch:\n- expected %s\n - got %s' %
            (_Dotify(expected_data), _Dotify(data)))  # pragma: no cover

  def BulkRead(self, length,
               timeout_ms=None):  # pylint: disable=unused-argument
    with self._lock:
      if not self._expected_io:
        raise usb_exceptions.ReadFailedError(None, None)  # pragma: no cover
      if self._expected_io[0][0] != 'read':
        # Nothing to read for now.
        raise usb_exceptions.ReadFailedError(None, None)  # pragma: no cover
      data = self._expected_io.pop(0)[1]
      if length < len(data):
        raise ValueError(
            'Unexpected read length. Read %d bytes, got %d bytes' %
            (length, len(data)))  # pragma: no cover
      return data

  def ExpectWrite(self, data):
    with self._lock:
      self._expected_io.append(('write', data))

  def ExpectRead(self, data):
    with self._lock:
      self._expected_io.append(('read', data))

  def Timeout(self, timeout_ms):
    return timeout_ms if timeout_ms is not None else self.timeout_ms
