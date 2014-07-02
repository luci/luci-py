#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming bot.

This is the program that communicates with the Swarming server, ensures the code
is always up to date and executes a child process to run tasks and upload
results back.
"""

import json
import logging
import logging.handlers
import optparse
import os
import subprocess
import sys
import time
import zipfile

# pylint: disable-msg=W0403
import logging_utils
import os_utilities
import url_helper
import zipped_archive
from common import rpc
from common import swarm_constants


# Path to this file or the zip containing this file.
THIS_FILE = os.path.abspath(zipped_archive.get_main_script_path())

# Root directory containing this file or the zip containing this file.
ROOT_DIR = os.path.dirname(THIS_FILE)


# TODO(maruel): Temporary to be compatible with Chromium's start_slave.py. I'll
# send a separate CL to update it, then I'll remove this line.
Restart = os_utilities.restart


class SlaveError(Exception):
  """Simple error exception properly scoped here."""
  pass


class SlaveRPCError(Exception):
  """Simple error exception properly scoped here."""
  pass


def ValidateBasestring(x, error_prefix='', errors=None):
  """Validate the given variable as a valid basestring.

  Args:
    x: The object to evaluate.
    error_prefix: A string to append to the start of every error message.
    errors: An array where we can append error messages.

  Returns:
    True if the variable is a valid basestring.
  """
  if not isinstance(x, basestring):
    if errors is not None:
      errors.append('%sInvalid type: %s instead of %s' %
                    (error_prefix, type(x), basestring))
    return False
  return True


def ValidateNonNegativeFloat(x, error_prefix='', errors=None):
  """Validate the given variable as a non-negative float.

  Args:
    x: The object to evaluate.
    error_prefix: A string to append to the start of every error message.
    errors: An array where we can append error messages.

  Returns:
    True if the variable is a non-negative float.
  """
  if not isinstance(x, float):
    if errors is not None:
      errors.append('%sInvalid type: %s instead of %s' %
                    (error_prefix, type(x), float))
    return False

  if x < 0:
    if errors is not None:
      errors.append('%s: Invalid negative float' % error_prefix)
    return False
  return True


def ValidateNonNegativeInteger(x, error_prefix='', errors=None):
  """Validate the given variable as a non-negative integer.

  Args:
    x: The object to evaluate.
    error_prefix: A string to append to the start of every error message.
    errors: An array where we can append error messages.

  Returns:
    True if the variable is a non-negative integer.
  """
  if not isinstance(x, int):
    if errors is not None:
      errors.append('%sInvalid type: %s instead of %s'
                    % (error_prefix, type(x), int))
    return False

  if x < 0:
    if errors is not None:
      errors.append('%sInvalid negative integer' % error_prefix)
    return False
  return True


def ValidateCommand(commands, error_prefix='', errors=None):
  """Validate the given commands are the valid.

  Args:
    commands: The object to evaluate.
    error_prefix: A string to append to the start of every error message.
    errors: An array where we can append error messages.

  Returns:
    True if commands are a list and each element in the list is a
    valid RPC command.
  """
  if not isinstance(commands, list):
    if errors is not None:
      errors.append('%sInvalid type: %s instead of %s' %
                    (error_prefix, type(commands), list))
    return False

  valid = True
  for command in commands:
    try:
      rpc.ParseRPC(command)
    except rpc.RPCError as e:
      errors.append('%sError when parsing RPC: %s' % (error_prefix, e))
      valid = False

  return valid


def _store_file(filepath, content):
  """Stores a file and create directories if needed."""
  path = os.path.dirname(filepath)
  if not os.path.isdir(path):
    os.makedirs(path)
  with open(filepath, 'wb') as f:
    f.write(content)


class SlaveMachine(object):
  """Creates a slave that continuously polls the Swarm server for jobs."""

  def __init__(self, url, attributes, max_url_tries=1):
    """Sets the parameters of the slave.

    Args:
      url: URL of the Swarm server.
      attributes: A dict of the attributes of the machine. Should include
          machine dimensions as well.
      max_url_tries: The maximum number of consecutive url errors to accept
          before throwing an exception.
    """
    self._url = url.rstrip('/')
    self._attributes = attributes.copy()
    self._result_url = None
    self._attributes['try_count'] = 0
    self._come_back = 0
    self._max_url_tries = max_url_tries
    self._attributes['version'] = zipped_archive.generate_version()

  def Start(self, iterations):
    """Starts the slave, which polls the Swarm server for jobs until it dies.

    Args:
      iterations: Purely used for test to make it cleanly exit. -1 indicates
          infinitely. Failing to connect to the server DOES NOT count as an
          iteration. This is useful for testing the slave and having an exit
          condition.

    Raises:
      SlaveError: If the slave in unable to connect to the provided URL after
      the given number of tries.
    """
    # Ping the swarm server before trying to find the fqdn below to ensure
    # that we have acquired our fqdn (otherwise getfqdn() below maybe return
    # an incorrect value).
    ping_url = self._url + '/server_ping'
    if url_helper.UrlOpen(ping_url, method='GET') is None:
      logging.error('Unable to make initial connection to the swarm server. '
                    'Aborting.')
      return

    url = self._url + '/poll_for_test'

    while True:
      # Reset the result_url to avoid posting to the wrong place.
      self._result_url = None

      request = {'attributes': json.dumps(self._attributes)}
      response_str = url_helper.UrlOpen(url, data=request,
                                        max_tries=self._max_url_tries)

      if response_str is None:
        raise SlaveError('Error when connecting to Swarm server, %s, failed to '
                         'connect after %d attempts.'
                         % (url, self._max_url_tries))

      try:
        response = json.loads(response_str)
      except ValueError:
        self._PostFailedExecuteResults('Invalid response: ' + response_str)
      else:
        logging.debug('Valid server response:\n %s', response_str)
        self._ProcessResponse(response)

      if iterations > 0:
        iterations -= 1
        if not iterations:
          break

  def _ProcessResponse(self, response):
    """Deals with processing the response sent to slave machine.

    Args:
      response: Response dict sent by Swarm server.
    """
    if not self._ValidateResponse(response):
      return

    # Parse values in response and get commands if provided.
    commands = self._ParseResponse(response)
    if not commands:
      logging.debug('No commands to execute - will call back in %f s',
                    self._come_back)
      assert self._come_back >= 0
      time.sleep(self._come_back)
    else:
      # Run the commands.
      for rpc_packet in commands:
        function_name, args = rpc.ParseRPC(rpc_packet)
        try:
          # Execute the function.
          fn = getattr(self, 'rpc_%s' % function_name)
          fn(args)
        except SlaveRPCError as e:
          self._PostFailedExecuteResults(str(e))
          break
        except AttributeError:
          self._PostFailedExecuteResults(
              'Unsupported RPC function name: ' + function_name)
          break

  def _ParseResponse(self, response):
    """Stores relevant fields from response to slave machine.

    Args:
      response: Response dict returned by _ValidateResponse.

    Returns:
      List of commands to execute, None if none specified by the server.
    """
    # Store try_count assigned by Swarm server to send it back in next request.
    self._attributes['try_count'] = int(response['try_count'])
    logging.debug('received try_count: %d', self._attributes['try_count'])

    commands = None
    if 'commands' not in response:
      self._come_back = float(response['come_back'])
    else:
      commands = response['commands']

    return commands

  def _ValidateResponse(self, response):
    """Tries to parse given response and validate data types.

    Args:
      response: A dict representing the response sent from the Swarm server.

    Returns:
      True if the response is valid, False otherwise.
    """
    # As part of error handling, we need a result URL. So try to get it
    # from the response, but don't fail if we are unable to.
    if ('result_url' in response and
        isinstance(response['result_url'], basestring)):
      self._result_url = response['result_url']

    # Validate fields in the response. A response should have 'try_count'
    # and only either one of ('come_back') or ('commands', 'result_url').
    required_fields = {'try_count': ValidateNonNegativeInteger}

    if 'commands' in response:
      required_fields['commands'] = ValidateCommand
      required_fields['result_url'] = ValidateBasestring
    else:
      required_fields['come_back'] = ValidateNonNegativeFloat

    # We allow extra fields in the response, but ignore them.
    missing_fields = set(required_fields).difference(set(response))
    if missing_fields:
      message = 'Missing fields in response: %s' % missing_fields
      self._PostFailedExecuteResults(message)
      return False

    # Validate fields.
    errors = []
    for key, validate_function in required_fields.iteritems():
      validate_function(response[key],
                        'Failed to validate %s with value "%s": ' %
                        (key, response[key]),
                        errors=errors)

    if errors:
      self._PostFailedExecuteResults(str(errors))
      return False

    return True

  # TODO(user): Implement mechanism for slave to give up after a
  # certain number of consecutive failures.
  def _PostFailedExecuteResults(self, result_string, result_code=-1):
    """Will post given results to result URL *ONLY* in the case of a failure.

      When a Swarm server sends commands to a slave machine, even though they
      could be completely wrong, the server assumes the job as running. Thus
      this function acts as the exception handler for incoming commands from
      the Swarming server. If for any reason the local test runner script can
      not be run successfully, this function is invoked.

    Args:
      result_string: String representing the output of the error.
      result_code: Numeric code representing error.
    """
    logging.error('Error [code: %d]: %s', result_code, result_string)

    if not self._result_url:
      logging.error('No URL to send results to!')
      return

    data = {'x': str(result_code), 's': False}
    files = [(swarm_constants.RESULT_STRING_KEY,
              swarm_constants.RESULT_STRING_KEY,
              result_string)]
    url_helper.UrlOpen(self._result_url, data=data, files=files,
                       max_tries=self._max_url_tries,
                       method='POSTFORM')

  @staticmethod
  def rpc_RunManifest(args):
    """Checks type of args to be correct.

    Args:
      args: json serialized TestRun.

    Raises:
      SlaveRPCError: If args are invalid will include an error message, or
      if executing the commands fails.
    """
    # Validate args.
    if not isinstance(args, basestring):
      raise SlaveRPCError('Invalid RunManifest arg: %r (expected str)' % args)
    try:
      json.loads(args)
    except ValueError as e:
      raise SlaveRPCError('Invalid json data for TestRun: %s' % e)

    path = os.path.join('work', 'test_run.json')
    _store_file(path, args)
    command = [sys.executable, THIS_FILE, 'local_test_runner', '-f', path]
    logging.debug('Running command: %s', command)
    try:
      subprocess.check_call(command, cwd=ROOT_DIR)
    except (OSError, subprocess.CalledProcessError) as e:
      logging.info('local_test_runner failed, restarting: %s', e)
      os_utilities.restart()
    logging.debug('done!')
    # At this point the script called by subprocess has handled any further
    # communication with the swarm server.

  @staticmethod
  def rpc_UpdateSlave(args):
    """Download the current version of the slave code and then run it.

    Use an alterning methods; first load swarming_bot.1.zip, then
    swarming_bot.2.zip, never touching swarming_bot.zip which was the originally
    bootstrapped file.

    Args:
      args: The url for the slave code.

    Raises:
      SlaveRPCError: If args are invalid.
    """
    if not isinstance(args, basestring):
      raise SlaveRPCError(
          'Invalid arg types to UpdateSlave: %s (expected str or unicode)' %
          str(type(args)))

    # Alternate between .1.zip and .2.zip.
    this_file = os.path.basename(THIS_FILE)
    new_zip = 'swarming_bot.1.zip'
    if this_file == new_zip:
      new_zip = 'swarming_bot.2.zip'

    # Download as a new file.
    if not url_helper.DownloadFile(new_zip, args):
      logging.error('Unable to download %s from %s.', new_zip, args)
      return

    logging.info('Restarting to %s.', new_zip)
    sys.stdout.flush()
    sys.stderr.flush()

    cmd = [sys.executable, new_zip, 'start_slave', '--survive']
    if sys.platform in ('cygwin', 'win32'):
      # (Tentative) It is expected that subprocess.Popen() behave a tad better
      # on Windows than os.exec*(), which has to be emulated since there's no OS
      # provided implementation. This means processes will accumulate as the bot
      # is restarted, which be a problem long term.
      subprocess.call(cmd)
    else:
      # On OSX, launchd will be unhappy if we quit so the old code bot process
      # has to outlive the new code child process. os.exec*() replaces the
      # process so this is fine.
      os.execv(sys.executable, cmd)


def get_attributes():
  """Returns start_slave.py's get_attributes() dict."""
  # Importing this administrator provided script could have side-effects on
  # startup. That is why it is imported late.
  try:
    import start_slave
    return start_slave.get_attributes()
  except Exception as e:
    logging.exception('Failed to call start_slave.get_attributes()')
    # Catch all exceptions here so the bot doesn't die on startup, which is
    # annoying to recover. In that case, we set a special property to catch
    # these and help the admin fix the swarming_bot code more quickly.
    # TODO(maruel): This should be part of the 'health check' and the bot
    # shouldn't allow itself to upgrade in this condition.
    # https://code.google.com/p/swarming/issues/detail?id=112
    dimensions = {
      'get_attributes_failed': '1',
      'error': str(e),
    }
    return {
      'dimensions': dimensions,
      'id': os_utilities.get_hostname().split('.', 1)[0],
      'ip': os_utilities.get_ip(),
    }


def get_config():
  """Returns the data from config.json.

  First try the config.json inside the zip. If not present or not running inside
  swarming_bot.zip, use the one beside the file.
  """
  with zipfile.ZipFile(THIS_FILE, 'r') as f:
    return json.load(f.open('config.json'))

  with open('config.json', 'r') as f:
    return json.load(f)


def main(args):
  # Add SWARMING_HEADLESS into environ so subcommands know that they are running
  # in a headless (non-interactive) mode.
  os.environ['SWARMING_HEADLESS'] = '1'

  # TODO(maruel): Get rid of all flags and support no option at all.
  # https://code.google.com/p/swarming/issues/detail?id=111
  parser = optparse.OptionParser(
      usage='%prog [options]',
      description=sys.modules[__name__].__doc__)
  # TODO(maruel): Always True.
  parser.add_option('-v', '--verbose', action='count', default=0,
                    help='Set logging level to INFO, twice for DEBUG.')

  # TODO(maruel): Remove these once callers are fixed.
  parser.add_option('-a', '--address', help=optparse.SUPPRESS_HELP)
  parser.add_option('-p', '--port', help=optparse.SUPPRESS_HELP)
  parser.add_option(
      '-r', '--max_url_tries', type='int', help=optparse.SUPPRESS_HELP)
  parser.add_option(
      '--keep_alive', action='store_true', help=optparse.SUPPRESS_HELP)
  parser.add_option('-d', '--directory', help=optparse.SUPPRESS_HELP)
  parser.add_option('-l', '--log_file', help=optparse.SUPPRESS_HELP)
  (options, args) = parser.parse_args(args)

  # Parser handles exiting this script after logging the error.
  if len(args) > 1:
    parser.error('Must specify only one filename')

  levels = [logging.WARNING, logging.INFO, logging.DEBUG]
  logging_utils.set_console_level(levels[min(options.verbose, len(levels)-1)])

  attributes = get_attributes()
  config = get_config()
  slave = SlaveMachine(config['server'], attributes, max_url_tries=40)

  while True:
    # Start requesting jobs.
    try:
      slave.Start(-1)
    except (rpc.RPCError, SlaveError) as e:
      logging.exception('Slave start threw an exception:\n%s', e)
    logging.warning('Slave returned. Starting again.')
  print >> sys.stderr, 'Should never reach here.'
  return 1


if __name__ == '__main__':
  logging_utils.prepare_logging('swarming_bot.log')
  os.chdir(ROOT_DIR)
  sys.exit(main(None))
