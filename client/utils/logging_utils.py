# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utility relating to logging."""

from __future__ import print_function

import argparse
import codecs
import ctypes
import logging
import logging.handlers
import optparse
import os
import sys
import tempfile
import time

from utils import file_path

# This works around file locking issue on Windows specifically in the case of
# long lived child processes.
#
# Python opens files with inheritable handle and without file sharing by
# default. This causes the RotatingFileHandler file handle to be duplicated in
# the subprocesses even if the log file is not used in it. Because of this
# handle in the child process, when the RotatingFileHandler tries to os.rename()
# the file in the parent process, it fails with:
#     WindowsError: [Error 32] The process cannot access the file because
#     it is being used by another process
if sys.platform == 'win32':
  import ctypes
  import msvcrt  # pylint: disable=F0401

  FILE_ATTRIBUTE_NORMAL = 0x80
  FILE_SHARE_READ = 1
  FILE_SHARE_WRITE = 2
  FILE_SHARE_DELETE = 4
  GENERIC_READ = 0x80000000
  GENERIC_WRITE = 0x40000000
  OPEN_ALWAYS = 4

  # TODO(maruel): Make it work in cygwin too if necessary. This would have to
  # use ctypes.cdll.kernel32 instead of msvcrt.


  def shared_open(path):
    """Opens a file with full sharing mode and without inheritance.

    The file is open for both read and write.

    See https://bugs.python.org/issue15244 for inspiration.
    """
    handle = ctypes.windll.kernel32.CreateFileW(
        path,
        GENERIC_READ|GENERIC_WRITE,
        FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
        None,
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        None)
    ctr_handle = msvcrt.open_osfhandle(handle, os.O_BINARY | os.O_NOINHERIT)
    return os.fdopen(ctr_handle, 'r+b')


  class NoInheritRotatingFileHandler(logging.handlers.RotatingFileHandler):
    def _open(self):
      """Opens the log file without handle inheritance but with file sharing.

      Ignores self.mode.
      """
      f = shared_open(self.baseFilename)
      if self.encoding:
        # Do the equivalent of
        # codecs.open(self.baseFilename, self.mode, self.encoding)
        info = codecs.lookup(self.encoding)
        f = codecs.StreamReaderWriter(
            f, info.streamreader, info.streamwriter, 'replace')
        f.encoding = self.encoding
      return f


else:  # Not Windows.


  NoInheritRotatingFileHandler = logging.handlers.RotatingFileHandler

# INFO has a priority of 20
USER_LOGS = logging.INFO + 5

# Levels used for logging.
LEVELS = [logging.ERROR, logging.INFO, logging.DEBUG]


class CaptureLogs:
  """Captures all the logs in a context."""
  def __init__(self, prefix, root=None):
    handle, self._path = tempfile.mkstemp(prefix=prefix, suffix='.log')
    os.close(handle)
    self._handler = logging.FileHandler(self._path, 'w')
    self._handler.setLevel(logging.DEBUG)
    formatter = UTCFormatter(
        '%(process)d %(asctime)s: %(levelname)-5s %(message)s')
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
    t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
    return "%s.%03d" % (t, record.msecs)


class SeverityFilter(logging.Filter):
  """Adds fields used by the infra-specific formatter.

  Fields added:
  - 'severity': one-letter indicator of log level (first letter of levelname).
  """

  def filter(self, record):
    record.severity = record.levelname[0]
    return True


def find_stderr(root=None):
  """Returns the logging.handler streaming to stderr, if any."""
  for log in (root or logging.getLogger()).handlers:
    if getattr(log, 'stream', None) is sys.stderr:
      return log


def new_rotating_file_handler(
    filename,
    utc_formatter=UTCFormatter(
        '%(process)d %(asctime)s %(severity)s: %(message)s')):
  file_path.ensure_tree(os.path.dirname(os.path.abspath(filename)))
  rotating_file = NoInheritRotatingFileHandler(filename,
                                               maxBytes=10 * 1024 * 1024,
                                               backupCount=5,
                                               encoding='utf-8')
  rotating_file.setLevel(logging.DEBUG)
  rotating_file.setFormatter(utc_formatter)
  rotating_file.addFilter(SeverityFilter())
  return rotating_file


def prepare_logging(filename, root=None):
  """Prepare logging for scripts.

  Makes it log in UTC all the time. Prepare a rotating file based log.
  """
  assert not find_stderr(root)

  # It is a requirement that the root logger is set to DEBUG, so the messages
  # are not lost. It defaults to WARNING otherwise.
  logger = root or logging.getLogger()
  if not logger:
    # Better print insanity than crash.
    print('OMG NO ROOT', file=sys.stderr)
    return
  logger.setLevel(logging.DEBUG)

  utc_formatter = UTCFormatter(
      '%(process)d %(asctime)s %(severity)s: %(message)s')

  stderr = logging.StreamHandler()
  stderr.setFormatter(utc_formatter)
  stderr.addFilter(SeverityFilter())
  # Default to ERROR.
  stderr.setLevel(logging.ERROR)
  logger.addHandler(stderr)

  # Setup up logging to a constant file so we can debug issues where
  # the results aren't properly sent to the result URL.
  if filename:
    try:
      logger.addHandler(new_rotating_file_handler(filename, utc_formatter))
    except Exception:
      # May happen on cygwin. Do not crash.
      logging.exception('Failed to open %s', filename)


def set_console_level(level, root=None):
  """Reset the console (stderr) logging level."""
  handler = find_stderr(root)
  if not handler:
    # Better print insanity than crash.
    print('OMG NO STDERR', file=sys.stderr)
    return
  handler.setLevel(level)


class ExactLevelFilter(logging.Filter):
  """
  A log filter which filters out log messages
  which are not an exact level.
  """

  def __init__(self, level):
    super(ExactLevelFilter, self).__init__()
    self._level = level

  def filter(self, record):
    return record.levelno == self._level


def set_user_level_logging(logger=None):
  """
  Creates a logger which writes logger.level = USER_LOGS to stdout.
  Some scripts (run_isolated.py) have all their output displayed to
  users in the swarming UI. As a result we may want to display some
  logging as more important than others. These are not errors so should
  not be logged to stderr.

  USER_LOGS are seen as more important than INFO logs but less
  important than error logs.

  That may lead to double logging if say another stream is already
  configured to listen to logging for stdout.
  """
  if logger is None:
    logger = logging.getLogger()

  logging.addLevelName(USER_LOGS, "USER_LOGS")
  user_logs_formatter = UTCFormatter(
      "swarming_bot_logs: %(asctime)s: %(message)s")

  stdout_handler = logging.StreamHandler(stream=sys.stdout)
  stdout_handler.setLevel(USER_LOGS)
  stdout_handler.addFilter(ExactLevelFilter(USER_LOGS))
  stdout_handler.addFilter(SeverityFilter())
  stdout_handler.setFormatter(user_logs_formatter)
  logger.addHandler(stdout_handler)


def user_logs(message, *args, **kwargs):
  logging.log(USER_LOGS, message, *args, **kwargs)


class OptionParserWithLogging(optparse.OptionParser):
  """Adds --verbose option."""

  # Set to True to enable --log-file options.
  enable_log_file = True

  # Set in unit tests.
  logger_root = None

  def __init__(self, verbose=0, log_file=None, **kwargs):
    kwargs.setdefault('description', sys.modules['__main__'].__doc__)
    optparse.OptionParser.__init__(self, **kwargs)
    self.group_logging = optparse.OptionGroup(self, 'Logging')
    self.group_logging.add_option(
        '-v', '--verbose',
        action='count',
        default=verbose,
        help='Use multiple times to increase verbosity')
    if self.enable_log_file:
      self.group_logging.add_option(
          '-l', '--log-file',
          default=log_file,
          help='The name of the file to store rotating log details')
      self.group_logging.add_option(
          '--no-log', action='store_const', const='', dest='log_file',
          help='Disable log file')

  def parse_args(self, *args, **kwargs):
    # Make sure this group is always the last one.
    self.add_option_group(self.group_logging)

    options, args = optparse.OptionParser.parse_args(self, *args, **kwargs)
    prepare_logging(self.enable_log_file and options.log_file, self.logger_root)
    set_console_level(
        LEVELS[min(len(LEVELS) - 1, options.verbose)], self.logger_root)

    return options, args


class ArgumentParserWithLogging(argparse.ArgumentParser):
  """Adds --verbose option."""

  # Set to True to enable --log-file options.
  enable_log_file = True

  def __init__(self, verbose=0, log_file=None, **kwargs):
    kwargs.setdefault('description', sys.modules['__main__'].__doc__)
    kwargs.setdefault('conflict_handler', 'resolve')
    self.__verbose = verbose
    self.__log_file = log_file
    super(ArgumentParserWithLogging, self).__init__(**kwargs)

  def _add_logging_group(self):
    group = self.add_argument_group('Logging')
    group.add_argument(
        '-v', '--verbose',
        action='count',
        default=self.__verbose,
        help='Use multiple times to increase verbosity')
    if self.enable_log_file:
      group.add_argument(
          '-l', '--log-file',
          default=self.__log_file,
          help='The name of the file to store rotating log details')
      group.add_argument(
          '--no-log', action='store_const', const='', dest='log_file',
          help='Disable log file')

  def parse_args(self, *args, **kwargs):
    # Make sure this group is always the last one.
    self._add_logging_group()

    args = super(ArgumentParserWithLogging, self).parse_args(*args, **kwargs)
    prepare_logging(self.enable_log_file and args.log_file, self.logger_root)
    set_console_level(
        LEVELS[min(len(LEVELS) - 1, args.verbose)], self.logger_root)

    return args
