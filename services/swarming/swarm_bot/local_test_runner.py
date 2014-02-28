#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs a Swarming task.

It uploads all results back to the Swarming server.
"""

__version__ = '0.1'

import logging
import logging.handlers
import optparse
import os
import Queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import zipfile

# pylint: disable-msg=W0403
from common import swarm_constants
from common import test_request_message
from common import url_helper


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# The amount of characters to read in each pass inside _RunCommand,
# this helps to ensure that the _RunCommand function doesn't ignore
# its other functions because it is too busy reading input.
CHARACTERS_TO_READ_PER_PASS = 2000


# The file name of the local rotating log file to store all test results to.
LOCAL_TEST_RUNNER_CONSTANT_LOG_FILE = 'local_test_runner.log'


# Common log format to use.
LOG_FMT = '%(name)-12s %(levelname)-5s %(message)s'


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


def _DeleteFileOrDirectory(name):
  """Deletes a file/directory, trying several times in case we need to wait.

  Args:
    name: The name of the file or directory to delete.

  Returns:
    True if the file or directory is successfully deleted.
  """
  # TODO(maruel): Reuse the code from run_isolated.py.
  for _ in range(5):
    try:
      if os.path.exists(name):
        if os.path.isdir(name):
          shutil.rmtree(name)
        else:
          os.remove(name)
      break
    except OSError:
      logging.exception('Exception deleting "%s"', name)
      time.sleep(1)
  if os.path.exists(name):
    logging.error('File not deleted: %s', name)
    return False
  return True


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
      return test_request_message.TestRun.FromJSON(f.read())
  except test_request_message as e:
    raise Error('Invalid Request File %s: %s' % (request_file_name, e))
  except IOError as e:
    raise Error('Missing Request File %s: %s' % (request_file_name, e))
  except test_request_message.Error as e:
    raise Error('Invalid Request File %s: %s' % (request_file_name, e))


def _ExpandEnv(argument, env):
  """Expands any environment variables that may exist in argument.

  Any '%%env%%' will be replaced by the corresponding environment variable.

  Args:
    argument: The command line argument that may contain an environment
        variable.
    env: The dictionary of environment variables to use for the expansion.

  Returns:
    The expanded argument with environment variables replaced by their value.
  """
  for match in re.findall(r'%(\S+)%', argument):
    value = env.get(match, None)
    if value is not None:
      argument = argument.replace('%%' + match + '%%', value)
  return argument


class CaptureLogs(object):
  """Captures all the logs in a context."""
  def __init__(self):
    (handle, self._file_name) = tempfile.mkstemp(
        prefix='local_test_runner', suffix='.log')
    os.close(handle)
    self._logging_handler = logging.FileHandler(self._file_name, 'w')
    self._logging_handler.setLevel(logging.DEBUG)
    self._logging_handler.setFormatter(
        logging.Formatter('%(asctime)s ' + LOG_FMT))
    logging.getLogger().addHandler(self._logging_handler)
    assert logging.getLogger().isEnabledFor(logging.DEBUG)

  def get_log(self):
    """Returns the current content of the logs.

    This also closes the log capture so future logs will not be captured.
    """
    self._disconnect()
    assert self._file_name
    try:
      with open(self._file_name, 'rb') as f:
        return f.read()
    except IOError as e:
      return 'Failed to read %s: %s' % (self._file_name, e)

  def close(self):
    """Closes and delete the log."""
    self._disconnect()
    if self._file_name:
      if not _DeleteFileOrDirectory(self._file_name):
        logging.error('Could not delete file "%s"', self._file_name)
      self._file_name = None

  def __enter__(self):
    return self

  def __exit__(self, _exc_type, _exc_value, _traceback):
    self.close()

  def _disconnect(self):
    if self._logging_handler:
      self._logging_handler.close()
      logging.getLogger().removeHandler(self._logging_handler)
      self._logging_handler = None


class LocalTestRunner(object):
  """A Local Test Runner to dowload files and run commands.

  Based on the information provided in the request file, the LocalTestRunner
  can download data from the URL provided in the test request file and unzip
  it locally. Then, it can execute the set of requested commands.

  Attributes:
    test_run: The information about the tests to run as
        described on http://goto/gforce/test-request-format.
  """
  # An array to properly index the success/failure decorated text based on
  # "not exit_code".
  _SUCCESS_DISPLAY_STRING = [' FAILED ', '      OK']

  # An array to properly index the pending/success/failure CGI strings.
  _SUCCESS_CGI_STRING = ['success', 'failure', 'pending']

  def __init__(self, request_file_name, log=None, data_folder_name='',
               max_url_retries=1, restart_on_failure=False):
    """Inits LocalTestRunner with a request file.

    Args:
      request_file_name: path to the file containing the request.
      log: CaptureLogs instance that collects logs.
      data_folder_name: optional name of a subdirectory where to explode the
          downloaded zip data so that they can be cleaned by the 'data' option
          of the cleanup field of a TestRun object in a Swarming file.
      max_url_retries: maximum number of times any urlopen call will get
          retried if it encounters an error.
      restart_on_failure: True to have this machine restart if any of the tests
         fail (although it waits for all the tests to run and the results to
         have been uploaded first).

    Raises:
      Error: When request_file_name or data_folder_name is invalid.
    """
    if any(badchar in data_folder_name for badchar in r'.\/'):
      raise Error('The specified data folder name must be a simple non-empty '
                  'string with no periods or slashes.')

    self._log = log
    self.data_dir = None
    self.max_url_retries = max_url_retries
    self.restart_on_failure = restart_on_failure
    self.success = False
    self.data_dir = (
        os.path.join(BASE_DIR, data_folder_name)
        if data_folder_name else BASE_DIR)
    self.last_ping_time = time.time()

    self.test_run = _ParseRequestFile(request_file_name)

    if not data_folder_name and self.test_run.cleanup == 'data':
      raise Error('You must specify a data folder name if you want to cleanup '
                  'data and not the rest of the root folder content.')

    if os.path.exists(self.data_dir) and not os.path.isdir(self.data_dir):
      raise Error('The specified data folder already exists, but is a regular '
                  'file rather than a folder.')
    if not os.path.exists(self.data_dir):
      os.mkdir(self.data_dir)

  def __enter__(self):
    return self

  def __exit__(self, _exc_type, _exc_value, _traceback):
    self.close()

  def close(self):
    # 'data' implies cleanup zip.
    if self.test_run.cleanup == 'data':
      if not _DeleteFileOrDirectory(self.data_dir):
        logging.error('Could not delete data directory "%s"', self.data_dir)

  def _RunCommand(self, command, hard_time_out, io_time_out, env=None):
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
          the command. Defaults to None.
    Returns:
      A tuple containing the exit code and the stdout/stderr of the execution.
    """
    assert isinstance(hard_time_out, (int, float))
    assert isinstance(io_time_out, (int, float))
    parsed_command = [_ExpandEnv(arg, env) for arg in command]

    logging.info('Executing: %s\ncwd: %s', parsed_command, self.data_dir)
    try:
      proc = subprocess.Popen(
          parsed_command, stdout=subprocess.PIPE,
          env=env, bufsize=1, stderr=subprocess.STDOUT,
          stdin=subprocess.PIPE, universal_newlines=True,
          cwd=self.data_dir)
    except OSError as e:
      logging.exception('Execution of %s raised exception.', parsed_command)
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
        logging.exception(
            'Polling execution of %s raised exception.', parsed_command)
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
                      'out after %fs' % (parsed_command, proc.pid,
                                         hard_time_out))
    else:
      error_string = ('Execution of %s with pid: %d timed out after %fs of no '
                      'output!' % (parsed_command, proc.pid, io_time_out))

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
    logging.info('Test case: %s starting to download data',
                 self.test_run.test_run_name)
    for data in self.test_run.data:
      if isinstance(data, (list, tuple)):
        (data_url, file_name) = data
      else:
        data_url = data
        file_name = data_url[data_url.rfind('/') + 1:]
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

      if self.test_run.cleanup == 'zip':  # Implied by cleanup data.
        try:
          os.remove(local_file)
        except OSError:
          logging.exception('Couldn\'t remove %s.', local_file)
    return True

  def RunTests(self):
    """Run the tests specified in the test run tests list and output results.

    Returns:
      A (success, result_codes, result_string) tuple to identify success,
      the result codes and also provide a detailed result_string
    """
    logging.info('Running tests from %s test case',
                 self.test_run.test_run_name)

    # Apply the test_run/config environment variables for all tests.
    env_vars = os.environ.items()
    if self.test_run.env_vars:
      env_vars += self.test_run.env_vars.items()
    if self.test_run.configuration.env_vars:
      env_vars += self.test_run.configuration.env_vars.items()

    # Write the header of the whole test run
    tests_to_run = self.test_run.tests
    result_string = '[==========] Running %d tests from %s test run.' % (
        len(tests_to_run), self.test_run.test_run_name)

    # We will accumulate the individual tests result codes.
    result_codes = []

    # We want to time to whole test run.
    test_run_start_time = time.time()
    decorate_output = None
    for test in tests_to_run:
      logging.info('Test %s', test.test_name)
      decorate_output = decorate_output or test.decorate_output
      if test.decorate_output:
        test_case_start_time = time.time()
        result_string = ('%s\n[ RUN      ] %s.%s' %
                         (result_string, self.test_run.test_run_name,
                          test.test_name))
      test_env_vars = env_vars[:]
      if test.env_vars:
        test_env_vars += test.env_vars.items()

      # Windows can't accept environment variables that are unicode.
      if sys.platform in ('win32', 'cygwin'):
        test_env_vars = [(str(x[0]), str(x[1])) for x in test_env_vars]

      (exit_code, stdout_string) = self._RunCommand(test.action,
                                                    test.hard_time_out,
                                                    test.io_time_out,
                                                    env=dict(test_env_vars))

      try:
        stdout_string = stdout_string.decode(self.test_run.encoding)
      except UnicodeDecodeError:
        stdout_string = (
            '! Output contains characters not valid in %s encoding !\n%s'
            % (self.test_run.encoding, stdout_string.decode(
                self.test_run.encoding,
                'replace')))

      # We always accumulate the test output and exit code.
      result_string = '%s\n%s' % (result_string, stdout_string)
      result_codes.append(exit_code)

      if exit_code:
        logging.warning('Execution error %d: %s', exit_code, stdout_string)

      if test.decorate_output:
        # TODO(maruel): Remove all the gtest faking outputs.
        # https://code.google.com/p/swarming/issues/detail?id=87
        test_case_timing = time.time() - test_case_start_time
        result_string = ('%s\n[ %s ] %s.%s (%d ms)' %
                         (result_string,
                          self._SUCCESS_DISPLAY_STRING[not exit_code],
                          self.test_run.test_run_name,
                          test.test_name, test_case_timing * 1000))

    # This is for the timing of running ALL tests.
    test_run_timing = time.time() - test_run_start_time

    # We MUST have as many results as we have tests, and they must all be int.
    num_results = len(result_codes)
    assert num_results == len(tests_to_run)
    assert sum([1 for result_code in result_codes
                if not isinstance(result_code, int)]) is 0

    # We sum the number of exit codes that were non-zero for success.
    num_failures = num_results - sum([not int(x) for x in result_codes])

    if decorate_output:
      result_string = '%s\n\n[----------] %s summary' % (
          result_string, self.test_run.test_run_name)
      result_string = '%s\n[==========] %d tests ran. (%d ms total)' % (
          result_string, num_results, test_run_timing * 1000)

      result_string = '%s\n[  PASSED  ] %d tests.' % (
          result_string, num_results - num_failures)
      result_string = '%s\n[  FAILED  ] %d tests' % (
          result_string, num_failures)
      if num_failures > 0:
        result_string = '%s, listed below:' % result_string

      # We finish by enumerating all failed individual tests.
      for index in range(min(len(result_codes), len(tests_to_run))):
        if result_codes[index] is not 0:
          result_string = '%s\n[  FAILED  ] %s.%s' % (
              result_string,
              self.test_run.test_run_name,
              tests_to_run[index].test_name)

      result_string += '\n\n %d FAILED TESTS\n' % num_failures
    # Record the success or failure.
    self.success = (num_failures == 0)

    # And append their total number before returning the result string.
    return (self.success, result_codes, result_string)

  def PublishResults(self, success, result_codes, result_string,
                     overwrite=False):
    """Publish the given result string to the result_url if any.

    Args:
      success: True if we must specify [?|&]s=true. False otherwise.
      result_codes: The array of exit codes to be published, one per action.
      result_string: The result to be published.
      overwrite: True if we should signal the server to overwrite any old
          result data it may have.

    Returns:
      True if we succeeded or had nothing to do, False otherwise.
    """
    logging.debug('Publishing Results')
    if not self.test_run.result_url:
      return True
    data = {
        'c': self.test_run.configuration.config_name,
        'n': self.test_run.test_run_name,
        'o': overwrite,
        's': success,
        # TODO(maruel): Keep as int.
        'x': ', '.join(str(i) for i in result_codes),
    }
    # Pass the output as a file to ensure the server handler doesn't
    # incorrectly convert the output to unicode.
    key = swarm_constants.RESULT_STRING_KEY
    url_results = url_helper.UrlOpen(
        self.test_run.result_url,
        data=data,
        files=[(key, key, result_string)],
        max_tries=self.max_url_retries,
        method='POSTFORM')
    if url_results is None:
      logging.error('Failed to publish results to given url, %s',
                    self.test_run.result_url)
      return False
    return True

  def PublishInternalErrors(self):
    """Get the current log data and publish it."""
    logging.debug('Publishing internal errors')
    self.PublishResults(False, [], self._log.get_log(), overwrite=True)

  def RetrieveDataAndRunTests(self):
    """Get the data required to run the tests, then run and publish the results.

    Returns:
      True if we we got the data, ran the tests and successfully published
      the results.
    """
    if not self.DownloadAndExplodeData():
      return False

    (success, result_codes, result_string) = self.RunTests()

    return self.PublishResults(success, result_codes, result_string)

  def ReturnExitCode(self, return_value):
    """Return the restart exit code if the machine should restart.

    If the machine shouldn't restart then just return the value passed in.
    The machine is restarted if restart on failure was enable and at least
    one test failed.

    Args:
      return_value: The value this function returns if the machine shouldn't
          restart.

    Returns:
      return_value: Either the restart exit code or |return_value|.
    """
    if return_value == swarm_constants.RESTART_EXIT_CODE:
      logging.error('return_value and restart exit code are the same, unable '
                    'to signal no restart')

    logging.info('Checking if restart required.')
    if self.restart_on_failure and not self.success:
      logging.info('Restart required.')
      return_value = swarm_constants.RESTART_EXIT_CODE
    else:
      logging.info('No restart required.')
    return return_value


def main():
  parser = optparse.OptionParser(
      description=sys.modules['__main__'].__doc__,
      version=__version__)
  parser.add_option(
      '-f', '--request_file_name',
      help='name of the request file')
  parser.add_option(
      '-d', '--data_folder_name', default='',
      help='name of a subdirectory to use to dump the task inputs into')
  parser.add_option(
      '-r', '--max_url_retries', default=15, type='int',
      help='maximum number of HTTP request retries. Default: %default')
  parser.add_option(
      '--restart_on_failure', action='store_true',
      help='tries to restart the machine if the task fails')
  parser.add_option(
      '-v', '--verbose', action='store_true',
      help='Set logging level to INFO')

  (options, args) = parser.parse_args()
  if not options.request_file_name:
    parser.error('You must provide the request file name.')
  if args:
    logging.warning('Ignoring unknown args: %s', args)

  # Setup the logger for the console ouput.
  console = logging.StreamHandler()
  console.setFormatter(logging.Formatter(LOG_FMT))
  console.setLevel(logging.INFO if options.verbose else logging.ERROR)
  logging.getLogger().addHandler(console)

  try:
    with CaptureLogs() as log:
      with LocalTestRunner(
          options.request_file_name,
          log=log,
          data_folder_name=options.data_folder_name,
          max_url_retries=options.max_url_retries,
          restart_on_failure=options.restart_on_failure) as runner:
        try:
          if runner.RetrieveDataAndRunTests():
            return runner.ReturnExitCode(0)
        except Exception:
          # We want to catch all so that we can report all errors, even internal
          # ones.
          logging.exception('Failed to run test')

        try:
          runner.PublishInternalErrors()
        except Exception:
          logging.exception('Unable to publish internal errors')
        return runner.ReturnExitCode(1)
  except Exception:
    logging.exception('Internal failure')
    return 1


def PrepareLogging():
  # It is a requirement that the root logger is set to DEBUG, so the messages
  # are not lost.
  logging.getLogger().setLevel(logging.DEBUG)

  # Setup up logging to a constant file so we can debug issues where
  # the results aren't properly sent to the result URL.
  rotating_file = logging.handlers.RotatingFileHandler(
      LOCAL_TEST_RUNNER_CONSTANT_LOG_FILE,
      maxBytes=10 * 1024 * 1024, backupCount=5)
  rotating_file.setLevel(logging.DEBUG)
  rotating_file.setFormatter(logging.Formatter('%(asctime)s ' + LOG_FMT))
  logging.getLogger().addHandler(rotating_file)


if __name__ == '__main__':
  PrepareLogging()
  sys.exit(main())
