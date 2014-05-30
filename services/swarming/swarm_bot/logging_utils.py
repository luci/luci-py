# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utility relating to logging.

TODO(maruel): Merge buffering and output related code from client/utils/tools.py
in a single file.
"""

import logging
import logging.handlers
import os
import sys
import tempfile
import time


class CaptureLogs(object):
  """Captures all the logs in a context."""
  def __init__(self, prefix, root=None):
    handle, self._path = tempfile.mkstemp(prefix=prefix, suffix='.log')
    os.close(handle)
    self._handler = logging.FileHandler(self._path, 'w')
    self._handler.setLevel(logging.DEBUG)
    formatter = UTCFormatter('%(asctime)s: %(levelname)-5s %(message)s')
    self._handler.setFormatter(formatter)
    self._root = root or logging.getLogger()
    self._root.addHandler(self._handler)
    assert self._root.isEnabledFor(logging.DEBUG)

  def read(self):
    """Returns the current content of the logs.

    This also closes the log capture so future logs will not be captured.
    """
    self._disconnect()
    assert self._path
    try:
      with open(self._path, 'rb') as f:
        return f.read()
    except IOError as e:
      return 'Failed to read %s: %s' % (self._path, e)

  def close(self):
    """Closes and delete the log."""
    self._disconnect()
    if self._path:
      try:
        os.remove(self._path)
      except OSError as e:
        logging.error('Failed to delete log file %s: %s', self._path, e)
      self._path = None

  def __enter__(self):
    return self

  def __exit__(self, _exc_type, _exc_value, _traceback):
    self.close()

  def _disconnect(self):
    if self._handler:
      self._root.removeHandler(self._handler)
      self._handler.close()
      self._handler = None


class UTCFormatter(logging.Formatter):
  converter = time.gmtime

  def formatTime(self, record, datefmt=None):
    """Change is ',' to '.'."""
    ct = self.converter(record.created)
    if datefmt:
      return time.strftime(datefmt, ct)
    else:
      t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
      return "%s.%03d" % (t, record.msecs)


def find_stderr(root=None):
  """Returns the logging.handler streaming to stderr, if any."""
  for log in (root or logging.getLogger()).handlers:
    if getattr(log, 'stream', None) is sys.stderr:
      return log


def prepare_logging(filename, root=None):
  """Prepare logging for scripts.

  Makes it log in UTC all the time. Prepare a rotating file based log.
  """
  assert not find_stderr(root)
  formatter = UTCFormatter('%(asctime)s: %(levelname)-5s %(message)s')

  # It is a requirement that the root logger is set to DEBUG, so the messages
  # are not lost. It defaults to WARNING otherwise.
  logger = root or logging.getLogger()
  logger.setLevel(logging.DEBUG)

  stderr = logging.StreamHandler()
  stderr.setFormatter(formatter)
  # Default to ERROR.
  stderr.setLevel(logging.ERROR)
  logger.addHandler(stderr)

  # Setup up logging to a constant file so we can debug issues where
  # the results aren't properly sent to the result URL.
  if filename:
    rotating_file = logging.handlers.RotatingFileHandler(
        filename, maxBytes=10 * 1024 * 1024, backupCount=5)
    rotating_file.setLevel(logging.DEBUG)
    rotating_file.setFormatter(formatter)
    logger.addHandler(rotating_file)


def set_console_level(level, root=None):
  """Reset the console (stderr) logging level."""
  handler = find_stderr(root)
  handler.setLevel(level)
