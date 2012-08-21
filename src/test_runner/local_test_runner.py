#!/usr/bin/python2.6
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""A command line script/class to run tests on a local configuration.

Given a test request file with information about a set of tests to run
on a given configuration with a set of URL to zip files to download,
the LocalTestRunner takes care of downloading the necessary files,
run the tests and saves the output at a specified location.

You can find more details about the test runner here:
http://goto/gforce//test-runner

The test request file format is described in more details here:
http://goto/gforce/test-request-format

The decorated output has the following format:

--------------------------------------------------------------------------------
For each test in the test_run:
[ RUN      ] <test_run_name>.<test_name>
<actions output>
If test action returned 0:
[       OK ] <test_run_name>.<test_name> (XX ms)
If test action returned a non-0 exit code:
[  FAILED  ] <test_run_name>.<test_name> (XX ms)

And at the end:
[----------] <test_run_name> summary
[==========] WW tests ran. (XX ms total)
[  PASSED  ] YY tests.
[  FAILED  ] ZZ tests, listed below:

for each test action that returned a non-0 exit code :
[  FAILED  ] <test_run_name>.<test_name>

 ZZ FAILED TESTS
--------------------------------------------------------------------------------

This is highly inspired by the gtest output format
(http://code.google.com/p/googletest/wiki/GoogleTestAdvancedGuide).
Some tests may identify that they don't want their output to be decorated since
they already follow the gtest format.

Running this file from the command line, you must specify the request file name
using the -f or --request_file_name command line argument.

You can also import this file as a module and use the LocalTestRunner class
on its own. You must initialize it with a valid request file name (otherwise it
will raise an Error exception). After that, you can simply ask it to download
and exploded the data specified in the test format file and then execute the
commands, also found within the test request file.

Since the most common usage of this file is to upload it on a remote server to
execute tests on a given configuration, we try to minimize its dependencies on
home grown modules. It currently only depends on the downloader.py file which
must also be uploaded to the server so that the LocalTestRunner can download
the data needed to run the tests locally.

Classes:
  Error: A simple error exception properly scoped to this module.
  LocalTestRunner: Parses a text file, downloads the data and runs the tests.

Top level Functions:
  main: Parses the command line output to properly initialize an instance of
        the LocalTestRunner and then calls DownloadAndExplodeData on it as well
        as RunTests.
"""





import exceptions
import logging
import logging.handlers
import os
from os import path
import Queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib2
import urlparse
import zipfile

from common import test_request_message
from common import url_helper
from test_runner import downloader


# The file name of the local rotating log file to store all test results to.
LOCAL_TEST_RUNNER_CONSTANT_LOG_FILE = 'local_test_runner.log'


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


class Error(Exception):
  """Simple error exception properly scoped here."""
  pass


class LocalTestRunner(object):
  """A Local Test Runner to dowload files and run commands.

  Based on the information provided in the request file, the LocalTestRunner
  can download data from the URL provided in the test request file and unzip
  it locally. Then, it can execute the set of requested commands.

  Attributes:
    test_run: The information about the tests to run as
        described on http://goto/gforce/test-request-format.
  """
  # A cached regular expresion used to find environment variables.
  _ENV_VAR_RE = re.compile(r'%(\S+)%')

  # An array to properly index the success/failure decorated text based on
  # "not exit_code".
  _SUCCESS_DISPLAY_STRING = [' FAILED ', '      OK']

  # An array to properly index the pending/success/failure CGI strings.
  _SUCCESS_CGI_STRING = ['success', 'failure', 'pending']

  def __init__(self, request_file_name, verbose=False, data_folder_name=None,
               max_url_retries=1):
    """Inits LocalTestRunner with a request file.

    Args:
      request_file_name: The path to the file containing the request.
      verbose: True to get INFO level logging, False to get ERROR level.
      data_folder_name: The name of an optional subfolder where to explode the
          downloaded zip data so that they can be cleaned by the 'data' option
          of the cleanup field of a test run object in a Swarm file.
      max_url_retries: The maximum number of times any urlopen call will get
          retried if it encounters an error.

    Raises:
      Error: When request_file_name or data_folder_name is invalid.
    """
    # Set up logging to file so we can send our errors to the result URL.
    logging.getLogger().setLevel(logging.DEBUG)

    (log_file_descriptor, self.log_file_name) = tempfile.mkstemp()
    os.close(log_file_descriptor)
    self.logging_file_handler = logging.FileHandler(self.log_file_name, 'w')
    self.logging_file_handler.setLevel(logging.DEBUG)
    self.logging_file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
    logging.getLogger('').addHandler(self.logging_file_handler)

    # Setup the logger for the console ouput.
    logging_console = logging.StreamHandler()
    logging_console.setFormatter(
        logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s'))
    if verbose:
      logging_console.setLevel(logging.INFO)
    else:
      logging_console.setLevel(logging.ERROR)
    logging.getLogger('').addHandler(logging_console)

    if not self._ParseRequestFile(request_file_name):
      raise Error('Invalid Request File: %s' % request_file_name)

    if not data_folder_name and self.test_run.cleanup == 'data':
      raise Error('You must specify a data folder name if you want to cleanup '
                  'data and not the rest of the root folder content.')
    if (data_folder_name and
        any([badchar in data_folder_name for badchar in r'.\/'])):
      raise Error('The specified data folder name must be a simple non-empty '
                  'string with no periods or slashes.')
    self.data_dir = os.path.abspath(os.path.dirname(__file__))
    if data_folder_name:
      self.data_dir = os.path.join(self.data_dir, data_folder_name)
    if path.exists(self.data_dir) and not path.isdir(self.data_dir):
      raise Error('The specified data folder already exists, but is a regular '
                  'file rather than a folder.')
    if not path.exists(self.data_dir):
      os.mkdir(self.data_dir)

    self.max_url_retries = max_url_retries

  def __del__(self):
    # TODO(user): We may want to keep these around, even after the run, for
    # postmortem investigations, but would they accumulate too much disk space?
    # If we decide to keep them, we should provide a recognizable name when we
    # create them in the call to mkstemp above.
    if self.log_file_name:
      self.logging_file_handler.close()  # In case it hasn't been closed yet.
      if not self._DeleteFileOrDirectory(self.log_file_name):
        logging.error('Could not delete file "%s"', self.log_file_name)

    if self.test_run.cleanup == 'data':  # Implies cleanup zip.
      if not self._DeleteFileOrDirectory(self.data_dir):
        logging.error('Could not delete data directory "%s"', self.data_dir)

  def _DeleteFileOrDirectory(self, name):
    """Deletes a file/directory, trying several times in case we need to wait.

    Args:
      name: The name of the file or directory to delete.
    """
    for _ in range(5):
      try:
        if os.path.exists(name):
          if os.path.isdir(name):
            shutil.rmtree(name)
          else:
            os.remove(name)
        break
      except (OSError, exceptions.WindowsError) as e:
        logging.exception('Exception deleting "%s": %s', name, e)
        time.sleep(1)
    if os.path.exists(name):
      logging.error('File not deleted: ' + name)

  def _ExpandEnv(self, argument, env):
    """Expands any environment variables that may exist in argument.

    For example self._ExpandEnv('%%programfiles%%\Google\Chrome')
    would return 'c:\program files\internet explorer\iexplore.exe'
    As mentioned in the documentation, we must use double % (e.g., %%ENV_VAR%%)
    so that the env-var doesn't get confisued with a Swarm Format Variable.
    see http://goto/swarm/design-doc/test-request-format for more details.

    Args:
      argument: The command line argument that may contain an environment
          variable.
      env: The dictionary of environment variables to use for the expansion.

    Returns:
      The expanded argument with environment variables replaced by their value.
    """
    matches = self._ENV_VAR_RE.findall(argument)
    for match in matches:
      env_var = '%%%%%s%%%%' % match
      if match in env:
        value = env[match]
        if value is not None:
          argument = argument.replace(env_var, value)
    return argument

  def _ParseRequestFile(self, request_file_name):
    """Parse and validate the given request file and store the result test_run.

    Args:
      request_file_name: The name of the request file to parse and validate.

    Returns:
      True if the parsed request file was validated, False othewise.
    """
    request_file = None
    try:
      request_file = open(request_file_name, 'r')
      request_data = request_file.read()
    except IOError, e:
      logging.exception('Failed to open file %s.\nException: %s',
                        request_file_name, e)
      return False
    finally:
      if request_file:
        request_file.close()
    try:
      self.test_run = test_request_message.TestRun()
      errors = []
      if not self.test_run.ParseTestRequestMessageText(request_data, errors):
        logging.error('Errors while parsing text file: %s', errors)
        return False
    except test_request_message.Error, e:
      logging.exception('Failed to evaluate %s\'s file content.\nException: %s',
                        request_file_name, e)
      return False
    return True

  def _PostOutput(self, upload_url, output, result):
    """Posts incremental output.

    Args:
      upload_url: Where to post the output.
      output: the output to be posted.
      result: the value of the CGI param 'r' which should be from the
          self._SUCCESS_CGI_STRING array.
    """
    data = {'n': self.test_run.test_run_name,
            'c': self.test_run.configuration.config_name,
            'r': output, 's': result}
    if (hasattr(self.test_run, 'instance_index') and
        self.test_run.instance_index is not None):
      assert hasattr(self.test_run, 'num_instances')
      assert self.test_run.num_instances is not None
      data['i'] = self.test_run.instance_index
      data['m'] = self.test_run.num_instances

    url_helper.UrlOpen(upload_url, data, self.max_url_retries)

  def _RunCommand(self, command, time_out, env=None):
    """Runs the given command.

    Args:
      command: A list containing the command to execute and its arguments.
          These will be expanded looking for environment variables.

      time_out: The number of seconds to wait for output from this command.
          If the command doesn't produce any output for |time_out| seconds,
          then we kill the process and return an error.

      env: A dictionary containing environment variables to be used when running
          the command. Defaults to None.
    Returns:
      A tuple containing the exit code and the stdout/stderr of the execution.
    """
    assert isinstance(time_out, (int, float))
    parsed_command = [self._ExpandEnv(arg, env) for arg in command]

    # Temporarily change to the specified data directory in order to run
    # the command, then change back afterward.  We cannot use the "cwd"
    # parameter of Popen() for this because this changes the working directory
    # for the subprocess only after it starts running.  In order to invoke the
    # command in the first place (which is assumed to be specified relative to
    # the data directory), the current working directory must already be set to
    # the data directory.
    orig_dir = os.getcwd()
    os.chdir(self.data_dir)
    try:
      proc = subprocess.Popen(parsed_command, stdout=subprocess.PIPE,
                              env=env, bufsize=1, stderr=subprocess.STDOUT,
                              stdin=subprocess.PIPE, universal_newlines=True)
    except OSError, e:
      logging.exception('Execution of %s raised exception: %s.',
                        parsed_command, e)
      return (1, e)
    finally:
      os.chdir(orig_dir)

    stdout_queue = Queue.Queue()
    stdout_thread = threading.Thread(target=EnqueueOutput,
                                     args=(proc.stdout, stdout_queue))
    stdout_thread.daemon = True  # Ensure this exits if the parent dies
    stdout_thread.start()

    timeout_start_time = time.time()
    stdout_string = ''
    current_chunk_to_upload = ''
    upload_chunk_size = 0
    upload_url = None
    if (self.test_run.output_destination and
        'url' in self.test_run.output_destination):
      upload_url = self.test_run.output_destination['url']
      if 'size' in self.test_run.output_destination:
        upload_chunk_size = self.test_run.output_destination['size']
    while time_out == 0 or timeout_start_time + time_out > time.time():
      try:
        exit_code = proc.poll()
      except OSError, e:
        logging.exception('Polling execution of %s raised exception: %s.',
                          parsed_command, e)
        return (1, e)

      current_content = ''
      while True:
        try:
          # Some output was produced so reset the timeout counter.
          if not stdout_queue.empty():
            timeout_start_time = time.time()
          current_content += stdout_queue.get_nowait()
        except Queue.Empty:
          break

      # If the process has ended, then read all the output that it generated.
      if exit_code:
        while stdout_thread.isAlive() or not stdout_queue.empty():
          try:
            current_content += stdout_queue.get(block=True, timeout=1)
          except Queue.Empty:
            # Queue could still potentially contain more input later.
            pass

      # Give some local feedback of progress and potentially upload to
      # output_destination if any.
      if current_content:
        logging.info(current_content)

      if upload_url and upload_chunk_size > 0:
        current_chunk_to_upload += current_content
        if ((exit_code is not None and len(current_chunk_to_upload)) or
            len(current_chunk_to_upload) >= upload_chunk_size):
          self._PostOutput(upload_url, current_chunk_to_upload,
                           self._SUCCESS_CGI_STRING[2])
          current_chunk_to_upload = ''
      else:
        stdout_string += current_content

      if exit_code is not None:
        if not stdout_string:
          stdout_string = 'No output!'
        if upload_url and upload_chunk_size <= 0:
          self._PostOutput(upload_url, stdout_string,
                           self._SUCCESS_CGI_STRING[2])
          stdout_string = 'No output!'

        return (exit_code, stdout_string)

      # We sleep a little to give the child process a chance to move forward
      # before we poll it again.
      time.sleep(0.1)

    # If we get here, it's because we timed out.
    error_string = ('Execution of %s with pid: %d timed out after %fs!' %
                    (parsed_command, proc.pid, time_out))
    logging.error(error_string)

    if not stdout_string:
      stdout_string = 'No ouput!'

    stdout_string += '\n' + error_string
    return (1, stdout_string)

  def DownloadAndExplodeData(self):
    """Download and explode the zip files enumerated in the test run data.

    Returns:
      True if we succeeded, False otherwise.
    """
    logging.info('Test case: %s starting to download data',
                 self.test_run.test_run_name)
    for data_url in self.test_run.data:
      local_file = path.join(self.data_dir,
                             data_url[data_url.rfind('/') + 1:])
      logging.info('Downloading: %s from %s', local_file, data_url)
      try:
        downloader.DownloadFile(local_file, data_url)
      except (urllib2.URLError, IOError), e:
        logging.exception('Failed to download data file from: %s.\n'
                          'Exception: %s', data_url, e)
        return False

      zip_file = None
      try:
        zip_file = zipfile.ZipFile(local_file)
        zip_file.extractall(self.data_dir)
      except (zipfile.error, zipfile.BadZipfile, IOError, RuntimeError), e:
        logging.exception('Failed to unzip %s\nException: %s', local_file, e)
        return False
      if zip_file:
        zip_file.close()

      if self.test_run.cleanup == 'zip':  # Implied by cleanup data.
        try:
          os.remove(local_file)
        except OSError, e:
          logging.exception('Couldn\'t remove %s.\nException: %s',
                            local_file, e)
    return True

  def RunTests(self):
    """Run the tests specified in the test run tests list and output results.

    Returns:
      A (success, result_string) tuple to identify a True/False sucess and also
      provide a detailed result_string
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
    for test in tests_to_run:
      logging.info('Test %s', test.test_name)
      if test.decorate_output:
        test_case_start_time = time.time()
        result_string = ('%s\n[ RUN      ] %s.%s' %
                         (result_string, self.test_run.test_run_name,
                          test.test_name))
      test_env_vars = env_vars[:]
      if test.env_vars:
        test_env_vars += test.env_vars.items()
      (exit_code, stdout_string) = self._RunCommand(test.action, test.time_out,
                                                    env=dict(test_env_vars))

      # We always accumulate the test output and exit code.
      result_string = '%s\n%s' % (result_string, stdout_string)
      result_codes.append(exit_code)

      if exit_code:  # 0 is SUCCESS
        logging.warning('Execution error %d: %s', exit_code, stdout_string)

      if test.decorate_output:
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

    # We always decorate the whole test run.
    result_string = ('%s\n\n[----------] %s summary' %
                     (result_string, self.test_run.test_run_name))
    result_string = ('%s\n[==========] %d tests ran. (%d ms total)' %
                     (result_string, num_results, test_run_timing * 1000))

    # We sum the number of exit codes that were non-zero for success.
    num_failures = num_results - sum([not int(x) for x in result_codes])
    result_string = '%s\n[  PASSED  ] %d tests.' % (result_string,
                                                    num_results - num_failures)
    result_string = '%s\n[  FAILED  ] %d tests' % (result_string, num_failures)
    if num_failures > 0:
      result_string = '%s, listed below:' % result_string

    # We finish by enumerating all failed individual tests.
    for index in range(min(len(result_codes), len(tests_to_run))):
      if result_codes[index] is not 0:
        result_string = ('%s\n[  FAILED  ] %s.%s' %
                         (result_string, self.test_run.test_run_name,
                          tests_to_run[index].test_name))

    # And append their total number before returning the result string.
    return (num_failures == 0, result_codes,
            '%s\n\n %d FAILED TESTS\n' % (result_string, num_failures))

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
    if (self.test_run.output_destination and
        'url' in self.test_run.output_destination):
      self._PostOutput(self.test_run.output_destination['url'], '',
                       self._SUCCESS_CGI_STRING[not success])
    if not self.test_run.result_url:
      return True
    result_url_parts = urlparse.urlsplit(self.test_run.result_url)
    if result_url_parts[0] == 'http' or result_url_parts[0] == 'https':
      data = {'n': self.test_run.test_run_name,
              'c': self.test_run.configuration.config_name,
              'x': ', '.join([str(i) for i in result_codes]),
              's': success,
              'r': result_string,
              'o': overwrite}

      url_results = url_helper.UrlOpen(self.test_run.result_url, data,
                                       self.max_url_retries)
      if url_results is None:
        logging.error('Failed to publish results to given url, %s',
                      self.test_run.result_url)
        return False
    elif result_url_parts[0] == 'file':
      file_path = '%s%s' % (result_url_parts[1], result_url_parts[2])
      output_file = None
      try:
        output_file = open(file_path, 'w')
        output_file.write(result_string)
      except IOError, e:
        logging.exception('Can\'t write result to file %s.\nException: %s',
                          file_path, e)
        return False
      finally:
        if output_file:
          output_file.close()
    elif result_url_parts[0] == 'mailto':
      # TODO(user): Implement this!
      pass
    else:
      assert False  # We should have validated that in TestRun
      return False
    return True

  def PublishInternalErrors(self):
    # Make sure all logging is done and flushed. Logging in the PublishResults
    # call below will simply be ignored...
    self.logging_file_handler.flush()
    self.logging_file_handler.close()
    # We let exceptions go through since there isn't much we can do with them.
    log_file = open(self.log_file_name)
    self.PublishResults(False, [], log_file.read(), overwrite=True)
    log_file.close()

  def TestLogException(self, message):
    """Logs the given message as an exception. This is useful for tests.

    Args:
      message: The message to log as an exception.
    """
    logging.exception(message)


def main():
  """For when the script is used directly on the command line."""
  # Here so that it isn't imported for nothing if we are imported as a module.
  # pylint: disable-msg=C6204
  import optparse
  # pylint: enable-msg=C6204
  parser = optparse.OptionParser()
  parser.add_option('-f', '--request_file_name',
                    help='The name of the request file.')
  parser.add_option('-d', '--data_folder_name',
                    help='The name of a subfolder to create in the directory '
                    'containing the test runner to use for setting up and '
                    'running the tests. Defaults to None.')
  parser.add_option('-r', '--max_url_retries', default=15,
                    help='The maximum number of times url messages will '
                    'attemp to be sent before accepting failure. Defaults to '
                    '%default')
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Set logging level to INFO. Optional. Defaults to '
                    'ERROR level.', default=False)

  # Setup up logging to a constant file so we can debug issues where
  # the results aren't properly sent to the result URL.
  logging_rotating_file = logging.handlers.RotatingFileHandler(
      LOCAL_TEST_RUNNER_CONSTANT_LOG_FILE,
      maxBytes=10 * 1024 * 1024, backupCount=5)
  logging_rotating_file.setLevel(logging.DEBUG)
  logging_rotating_file.setFormatter(logging.Formatter(
      '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
  logging.getLogger('').addHandler(logging_rotating_file)

  (options, args) = parser.parse_args()
  if not options.request_file_name:
    parser.error('You must provide the request file name.')
  if args:
    logging.warning('Ignoring unknown args: %s', args)

  runner = None
  try:
    runner = LocalTestRunner(options.request_file_name, verbose=options.verbose,
                             data_folder_name=options.data_folder_name,
                             max_url_retries=options.max_url_retries)
  except Error, e:
    logging.exception('Can\'t create TestRunner with file: %s.\nException: %s',
                      options.request_file_name, e)
    published = False
    if runner:
      published = runner.PublishInternalErrors()
    return int(not published)

  try:
    if runner.DownloadAndExplodeData():
      (success, result_codes, result_string) = runner.RunTests()
      if runner.PublishResults(success, result_codes, result_string):
        return 0
  except Exception, e:  # pylint: disable-msg=W0703
    # We want to catch all so that we can report all errors, even internal ones.
    logging.exception(e)
    return not runner.PublishInternalErrors()

  return 1


if __name__ == '__main__':
  sys.exit(main())
