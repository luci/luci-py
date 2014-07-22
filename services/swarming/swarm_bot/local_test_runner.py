#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs a Swarming task.

It uploads all results back to the Swarming server.
"""

__version__ = '0.2'

import logging
import logging.handlers
import optparse
import os
import Queue
import subprocess
import sys
import threading
import time
import zipfile

# pylint: disable-msg=W0403
import logging_utils
import url_helper
import zipped_archive
from common import test_request_message


# Path to this file or the zip containing this file.
THIS_FILE = os.path.abspath(zipped_archive.get_main_script_path())

# Root directory containing this file or the zip containing this file.
ROOT_DIR = os.path.dirname(THIS_FILE)


# The amount of characters to read in each pass inside _RunCommand,
# this helps to ensure that the _RunCommand function doesn't ignore
# its other functions because it is too busy reading input.
CHARACTERS_TO_READ_PER_PASS = 2000


def EnqueueOutput(out, queue):
  """Read all the output from the given handle and insert it into the queue."""
  while True:
    # This readline will block until there is either new input or the handle
    # is closed. Readline will only return None once the handle is close, so
    # even if the output is being produced slowly, this function won't exit
    # early.
    # The potential dealock here is acceptable because this isn't run on the
    # main thread.
    data = out.readline()
    if not data:
      break
    queue.put(data, block=True)
  out.close()


def _TimedOut(time_out, time_out_start):
  """Returns true if we reached the timeout.

  This function makes it easy to mock out timeouts in tests.

  Args:
    time_out: The amount of time required to time out.
    time_out_start: The start of the time out clock.

  Returns:
    True if the given values have timed out.
  """
  current_time = time.time()
  if current_time < time_out_start:
    logging.warning('The current time is earlier than the time out start (%s '
                    'vs %s). Potential error in setting the time out start '
                    'values', current_time, time_out_start)
  return time_out != 0 and time_out_start + time_out < current_time


class Error(Exception):
  """Simple error exception properly scoped here."""
  pass


def _ParseRequestFile(request_file_name):
  """Parses and validates the given request file and store the result test_run.

  Args:
    request_file_name: The name of the request file to parse and validate.

  Returns:
    TestRun instance.
  """
  try:
    with open(request_file_name, 'rb') as f:
      content = f.read()
  except IOError as e:
    raise Error('Missing Request File %s: %s' % (request_file_name, e))

  try:
    return test_request_message.TestRun.FromJSON(content)
  except test_request_message.Error as e:
    raise Error(
        'Invalid Request File %s: %s\n%s' % (request_file_name, e, content))


class LocalTestRunner(object):
  """Dowloads files, runs the commands and uploads results back.

  Based on the information provided in the request file, the LocalTestRunner
  can download data from the URL provided in the test request file and unzip
  it locally. Then, it can execute the set of requested commands.
  """

  def __init__(self, request_file_name, log=None):
    """Inits LocalTestRunner with a request file.

    Args:
      request_file_name: path to the file containing the request.
      log: CaptureLogs instance that collects logs.

    Raises:
      Error: When request_file_name is invalid.
    """
    self._log = log
    self.data_dir = os.path.join(ROOT_DIR, 'work')
    self.last_ping_time = time.time()

    self.test_run = _ParseRequestFile(request_file_name)

    if os.path.exists(self.data_dir) and not os.path.isdir(self.data_dir):
      raise Error('The specified data folder already exists, but is a regular '
                  'file rather than a folder.')
    if not os.path.exists(self.data_dir):
      os.mkdir(self.data_dir)

  def _RunCommand(self, command, hard_time_out, io_time_out, env):
    """Runs the given command.

    Args:
      command: A list containing the command to execute and its arguments.
          These will be expanded looking for environment variables.
      hard_time_out: The maximum number of seconds to run this command for. If
          the command takes longer than this to finish, we kill the process
          and return an error.
      io_time_out: The number of seconds to wait for output from this command.
          If the command doesn't produce any output for |time_out| seconds,
          then we kill the process and return an error.
      env: A dictionary containing environment variables to be used when running
          the command.

    Returns:
      Tuple containing the exit code and the stdout/stderr of the execution.
    """
    assert isinstance(hard_time_out, (int, float))
    assert isinstance(io_time_out, (int, float))

    logging.info('Executing: %s\ncwd: %s', command, self.data_dir)
    try:
      proc = subprocess.Popen(
          command, stdout=subprocess.PIPE,
          env=env, bufsize=1, stderr=subprocess.STDOUT,
          stdin=subprocess.PIPE, universal_newlines=True,
          cwd=self.data_dir)
    except OSError as e:
      logging.exception('Execution of %s raised exception.', command)
      return (1, e)

    stdout_queue = Queue.Queue()
    stdout_thread = threading.Thread(target=EnqueueOutput,
                                     args=(proc.stdout, stdout_queue))
    stdout_thread.daemon = True  # Ensure this exits if the parent dies
    stdout_thread.start()

    hard_time_out_start_time = time.time()
    hit_hard_time_out = False
    io_time_out_start_time = time.time()
    hit_io_time_out = False
    stdout_string = ''

    while not hit_hard_time_out and not hit_io_time_out:
      try:
        exit_code = proc.poll()
      except OSError as e:
        logging.exception('Polling execution of %s raised exception.', command)
        return (1, e)

      # TODO(maruel): Add back support to stream content but only to the
      # Swarming server this time.
      current_content = ''
      got_output = False
      for _ in range(CHARACTERS_TO_READ_PER_PASS):
        try:
          current_content += stdout_queue.get_nowait()
          got_output = True
        except Queue.Empty:
          break

      # Some output was produced so reset the timeout counter.
      if got_output:
        io_time_out_start_time = time.time()

      # If enough time has passed, let the server know that we are still
      # alive.
      if self.last_ping_time + self.test_run.ping_delay < time.time():
        if url_helper.UrlOpen(self.test_run.ping_url) is not None:
          self.last_ping_time = time.time()

      # If the process has ended, then read all the output that it generated.
      if exit_code:
        while stdout_thread.isAlive() or not stdout_queue.empty():
          try:
            current_content += stdout_queue.get(block=True, timeout=1)
          except Queue.Empty:
            # Queue could still potentially contain more input later.
            pass

      if current_content:
        logging.info(current_content)

      stdout_string += current_content

      if exit_code is not None:
        return (exit_code, stdout_string)

      # We sleep a little to give the child process a chance to move forward
      # before we poll it again.
      time.sleep(0.1)

      if _TimedOut(hard_time_out, hard_time_out_start_time):
        hit_hard_time_out = True

      if _TimedOut(io_time_out, io_time_out_start_time):
        hit_io_time_out = True

    # If we get here, it's because we timed out.
    if hit_hard_time_out:
      error_string = ('Execution of %s with pid: %d encountered a hard time '
                      'out after %fs' % (command, proc.pid, hard_time_out))
    else:
      error_string = ('Execution of %s with pid: %d timed out after %fs of no '
                      'output!' % (command, proc.pid, io_time_out))

    logging.error(error_string)

    if not stdout_string:
      stdout_string = 'No output!'

    stdout_string += '\n' + error_string
    return (1, stdout_string)

  def DownloadAndExplodeData(self):
    """Download and explode the zip files enumerated in the test run data.

    Returns:
      True if we succeeded, False otherwise.
    """
    for data in self.test_run.data:
      assert isinstance(data, (list, tuple))
      (data_url, file_name) = data
      local_file = os.path.join(self.data_dir, file_name)
      logging.info('Downloading: %s from %s', local_file, data_url)
      if not url_helper.DownloadFile(local_file, data_url):
        return False

      zip_file = None
      try:
        zip_file = zipfile.ZipFile(local_file)
        zip_file.extractall(self.data_dir)
      except (zipfile.error, zipfile.BadZipfile, IOError, RuntimeError):
        logging.exception('Failed to unzip %s.', local_file)
        return False
      if zip_file:
        zip_file.close()
    return True

  def RunTests(self):
    """Run the tests specified in the test run tests list and output results.

    Returns:
      Tuple (result_codes, result_string) to identify the result codes and also
      provide a detailed result_string.
    """
    # Apply the test_run/config environment variables for all tests.
    env_vars = os.environ.copy()
    if self.test_run.env_vars:
      env_vars.update(
          dict(
            (k.encode('utf-8'), v.encode('utf-8'))
            for k, v in self.test_run.env_vars.iteritems()
          ))

    # Any True will make everything wrapped up.
    decorate_output = any(t.decorate_output for t in self.test_run.tests)

    result_string = ''
    result_codes = []
    test_run_start_time = time.time()
    for test in self.test_run.tests:
      test_case_start_time = time.time()

      if decorate_output:
        if result_string:
          result_string += '\n'
        result_string += '[==========] Running: %s' % ' '.join(test.action)
      (exit_code, stdout_string) = self._RunCommand(test.action,
                                                    test.hard_time_out,
                                                    test.io_time_out,
                                                    env=env_vars)
      encoding = 'utf-8'
      try:
        stdout_string = stdout_string.decode(encoding)
      except UnicodeDecodeError:
        stdout_string = (
            '! Output contains characters not valid in %s encoding !\n%s'
            % (encoding, stdout_string.decode(encoding, 'replace')))

      # We always accumulate the test output and exit code.
      if result_string:
        result_string += '\n'
      result_string += stdout_string
      result_codes.append(exit_code)

      if exit_code:
        logging.warning('Execution error %d: %s', exit_code, stdout_string)

      if decorate_output:
        test_case_timing = time.time() - test_case_start_time
        result_string += '\n(Step: %d ms)' % (test_case_timing * 1000)

    # This is for the timing of running ALL tests.
    if decorate_output:
      test_run_timing = time.time() - test_run_start_time
      result_string += '\n(Total: %d ms)' % (test_run_timing * 1000)

    # And append their total number before returning the result string.
    return result_codes, result_string

  def PublishResults(self, result_codes, result_string):
    """Publish the given result string to the result_url if any.

    Args:
      result_codes: The array of exit codes to be published, one per action.
      result_string: The result to be published.

    Returns:
      True if we succeeded or had nothing to do, False otherwise.
    """
    logging.debug('Publishing Results')
    data = {
      'o': result_string,
      'x': ', '.join(str(i) for i in result_codes),
    }
    url_results = url_helper.UrlOpen(
        self.test_run.result_url,
        data=data,
        max_tries=15,
        method='POST')
    if url_results is None:
      logging.error('Failed to publish results to given url, %s',
                    self.test_run.result_url)
      return False
    return True

  def PublishInternalErrors(self):
    """Get the current log data and publish it."""
    logging.debug('Publishing internal errors')
    self.PublishResults([], self._log.read())

  def RetrieveDataAndRunTests(self):
    """Get the data required to run the tests, then run and publish the results.

    Returns:
      Process exit code to use.
    """
    if not self.DownloadAndExplodeData():
      return False

    result_codes, result_string = self.RunTests()

    if not self.PublishResults(result_codes, result_string):
      return 1
    return max(result_codes)


def main(args):
  parser = optparse.OptionParser(
      description=sys.modules[__name__].__doc__,
      version=__version__)
  parser.add_option(
      '-f', '--request_file_name',
      help='name of the request file')
  parser.add_option(
      '-v', '--verbose', action='store_true',
      help='Set logging level to INFO')

  (options, args) = parser.parse_args(args)
  if not options.request_file_name:
    parser.error('You must provide the request file name.')
  if args:
    parser.error('Unknown args: %s' % args)

  # Setup the logger for the console ouput.
  logging_utils.set_console_level(
      logging.INFO if options.verbose else logging.ERROR)

  try:
    with logging_utils.CaptureLogs('local_test_runner') as log:
      runner = LocalTestRunner(options.request_file_name, log=log)
      try:
        return runner.RetrieveDataAndRunTests()
      except Exception:
        # We want to catch all so that we can report all errors, even internal
        # ones.
        logging.exception('Failed to run test')

      try:
        runner.PublishInternalErrors()
      except Exception:
        logging.exception('Unable to publish internal errors')
      return 1
  except Exception:
    logging.exception('Internal failure')
    return 1


if __name__ == '__main__':
  logging_utils.prepare_logging('local_test_runner.log')
  sys.exit(main(None))
