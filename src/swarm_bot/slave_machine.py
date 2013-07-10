#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Slave Machine.

A machine running this script is identified as a slave machine that may be
used to run Swarm tests on. It knows the protocol of how to connect to the
Swarm server, get tests and binaries, run them locally and post the results.

The slave needs to be told about its attributes and capabilities which can be
read from a given file or stdin. They are described using strings formatted
as a subset of the python syntax to a dictionary object. See
http://code.google.com/p/swarming/wiki/MachineProvider for complete details.
"""


import json
import logging
import logging.handlers
import optparse
import os
import subprocess
import sys
import time

# pylint: disable=g-import-not-at-top
from common import swarm_constants
from common import url_helper
from common import version
# pylint: enable=g-import-not-at-top


# The default name of the text file containing the machine id of this machine.
DEFAULT_MACHINE_ID_FILE = 'swarm_bot.id'

# The zip file to contain the zipped slave code.
ZIPPED_SLAVE_FILES = 'slave_files.zip'

# The name of a user added file that can be used to specify conditions under
# which a slave should stop querying for work.
CHECK_REQUIREMENTS_FILE = 'check_requirements.py'

# The code to unzip the slave code and start the slave back up. This needs to be
# a separate script so that the update process can overwrite it and then run
# it without the rest of the slave running, otherwise the slave files will be
# protected and it won't be possible to replace them with the new zipped file
# version.
SLAVE_SETUP_SCRIPT = """
import os
import sys
import zipfile

f = zipfile.ZipFile('%(zipped_slave_files)s')
try:
  f.extractall()
finally:
  f.close()

os.execl(sys.executable, sys.executable, 'start_slave.py')
""" % {'zipped_slave_files': ZIPPED_SLAVE_FILES}


def Restart():
  """Restarts this machine.

  Raises:
    Exception: When it is unable to restart the machine.
  """
  restart_cmd = None
  if sys.platform == 'win32':
    restart_cmd = ['shutdown', '-r', '-f', '-t', '1']
  elif sys.platform == 'cygwin':
    restart_cmd = ['shutdown', '-r', '-f', '1']
  elif sys.platform == 'linux2' or sys.platform == 'darwin':
    restart_cmd = ['sudo', '/sbin/shutdown', '-r', 'now']

  if restart_cmd:
    logging.info('Restarting machine with command %s', restart_cmd)
    try:
      subprocess.call(restart_cmd)
    except OSError as e:
      logging.exception(e)

  # Sleep for 300 seconds to ensure we don't try to do anymore work while
  # the OS is preparing to shutdown.
  time.sleep(300)

  # The machine should be shutdown by now.
  raise SlaveError('Unable to restart machine')


def ShouldRun(remaining_iterations):
  """Do some basic checks to determine if the slave should run.

  Args:
    remaining_iterations: The remaining number of iterations to do (or -1 to
        never stop).

  Returns:
     True if this machine should continue running.
  """
  if remaining_iterations == 0:
    return False

  # If a CHECK_REQUIREMENT_FILE is present, call it to give slaves a way of
  # determining if they should still communicate with the server or not.
  if os.path.exists(CHECK_REQUIREMENTS_FILE):
    return not bool(subprocess.call([sys.executable, CHECK_REQUIREMENTS_FILE]))

  return True


# pylint: disable=dangerous-default-value
def ValidateBasestring(x, error_prefix='', errors=[]):
  """Validate the given variable as a valid basestring.

  Args:
    x: The object to evaluate.
    error_prefix: A string to append to the start of every error message.
    errors: An array where we can append error messages.

  Returns:
    True if the variable is a valid basestring.
  """
  if not isinstance(x, basestring):
    errors.append('%sInvalid type: %s instead of %s' %
                  (error_prefix, type(x), basestring))
    return False
  return True


# pylint: disable=dangerous-default-value
def ValidateNonNegativeFloat(x, error_prefix='', errors=[]):
  """Validate the given variable as a non-negative float.

  Args:
    x: The object to evaluate.
    error_prefix: A string to append to the start of every error message.
    errors: An array where we can append error messages.

  Returns:
    True if the variable is a non-negative float.
  """
  if not isinstance(x, float):
    errors.append('%sInvalid type: %s instead of %s' %
                  (error_prefix, type(x), float))
    return False

  if x < 0:
    errors.append('%s: Invalid negative float' % error_prefix)
    return False
  return True


# pylint: disable=dangerous-default-value
def ValidateNonNegativeInteger(x, error_prefix='', errors=[]):
  """Validate the given variable as a non-negative integer.

  Args:
    x: The object to evaluate.
    error_prefix: A string to append to the start of every error message.
    errors: An array where we can append error messages.

  Returns:
    True if the variable is a non-negative integer.
  """
  if not isinstance(x, int):
    errors.append('%sInvalid type: %s instead of %s'
                  % (error_prefix, type(x), int))
    return False

  if x < 0:
    errors.append('%sInvalid negative integer' % error_prefix)
    return False
  return True


# pylint: disable=dangerous-default-value
def ValidateCommand(commands, error_prefix='', errors=[]):
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
    errors.append('%sInvalid type: %s instead of %s' %
                  (error_prefix, type(commands), list))
    return False

  valid = True
  for command in commands:
    try:
      ParseRPC(command)
    except SlaveError as e:
      errors.append('%sError when parsing RPC: %s' % (error_prefix, e))
      valid = False

  return valid


class SlaveMachine(object):
  """Creates a slave that continuously polls the Swarm server for jobs."""

  def __init__(self, url='https://localhost:443', attributes=None,
               max_url_tries=1, id_filename=None):
    """Sets the parameters of the slave.

    Args:
      url: URL of the Swarm server.
      attributes: A dict of the attributes of the machine. Should include
          machine dimensions as well.
      max_url_tries: The maximum number of consecutive url errors to accept
          before throwing an exception.
      id_filename: The name of the file where the initial machine id should be
          load from, and where any changes should be saved to.

    """
    self._url = url
    self._attributes = attributes.copy() if attributes else {}
    self._result_url = None
    self._attributes['id'] = None
    self._attributes['try_count'] = 0
    self._attributes['version'] = version.GenerateSwarmSlaveVersion(__file__)
    self._come_back = 0
    self._id_filename = id_filename

    if self._id_filename and os.path.exists(self._id_filename):
      with open(self._id_filename, 'r') as f:
        # If this id is invalid the server will ignore it and generate a new
        # id for this slave.
        self._attributes['id'] = f.read()

    self._max_url_tries = max_url_tries

  def Start(self, iterations=-1):
    """Starts the slave, which polls the Swarm server for jobs until it dies.

    Args:
      iterations: Number of times to poll the Swarm server. -1 indicates
          infinitely. Failing to connect to the server DOES NOT count as an
          iteration. This is useful for testing the slave and having an exit
          condition.

    Raises:
      SlaveError: If the slave in unable to connect to the provided URL after
      the given number of tries, or an invalid number of iterations were
      requested.
    """
    url = self._url + '/poll_for_test'
    remaining_iterations = iterations

    while True:
      request = {
          'attributes': json.dumps(self._attributes)
          }

      # Reset the result_url to avoid posting to the wrong place.
      self._result_url = None

      logging.debug('Connecting to Swarm server: %s', self._url)
      logging.debug('Request: %s', str(request))

      response_str = url_helper.UrlOpen(url, data=request,
                                        max_tries=self._max_url_tries)

      if response_str is None:
        raise SlaveError('Error when connecting to Swarm server, %s, failed to '
                         'connect after %d attempts.'
                         % (url, self._max_url_tries))

      response = None
      try:
        response = json.loads(response_str)
      except ValueError:
        self._PostFailedExecuteResults('Invalid response: ' + response_str)
      else:
        logging.debug('Valid server response:\n %s', response_str)
        self._ProcessResponse(response)

      if remaining_iterations > 0:
        remaining_iterations -= 1

      if not ShouldRun(remaining_iterations):
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
      logging.debug('Commands received, executing now:\n%s', commands)
      # Run the commands.
      for rpc in commands:
        function_name, args = ParseRPC(rpc)
        try:
          self._ExecuteRPC(function_name, args)
        except SlaveRPCError as e:
          self._PostFailedExecuteResults(str(e))
          break
        except AttributeError:
          self._PostFailedExecuteResults(
              'Unsupported RPC function name: ' + function_name)
          break

  def _ExecuteRPC(self, name, args):
    """Execute the function with given args.

    Args:
      name: Function name to call.
      args: Arguments to pass to function.

    Returns:
      The result of the execute function.
    """
    return getattr(self, name)(args)

  def _ParseResponse(self, response):
    """Stores relevant fields from response to slave machine.

    Args:
      response: Response dict returned by _ValidateResponse.

    Returns:
      List of commands to execute, None if none specified by the server.
    """

    # Store id assigned by Swarm server so in the future they know this slave.
    if self._attributes['id'] != response['id']:
      self._attributes['id'] = response['id']
      if self._id_filename:
        with open(self._id_filename, 'w') as f:
          f.write(response['id'])

    logging.debug('received id: %s', self._attributes['id'])

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

    # Validate fields in the response. A response should have 'id', 'try_count',
    # and only either one of ('come_back') or ('commands', 'result_url').
    required_fields = {
        'id': ValidateBasestring,
        'try_count': ValidateNonNegativeInteger
        }

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
      the Swarm server. If for any reason the local test runner script can not
      be run successfully, this function is invoked.

    Args:
      result_string: String representing the output of the error.
      result_code: Numeric code representing error.
    """
    logging.error('Error [code: %d]: %s', result_code, result_string)

    if not self._result_url:
      logging.error('No URL to send results to!')
      return

    url_helper.UrlOpen(self._result_url,
                       {'x': str(result_code),
                        's': False,
                        url_helper.RESULT_STRING_KEY: result_string},
                       max_tries=self._max_url_tries)

  def LogRPC(self, args):
    """Logs given args to logging.debug.

    Args:
      args: A string or unicode to be logged.

    Raises:
      SlaveRPCError: If args are invalid will include an error message.
    """
    # Validate args.
    if not isinstance(args, basestring):
      raise SlaveRPCError(
          'Invalid arg types to LogRPC: %s (expected str or unicode)' %
          str(type(args)))

    # Execute functionality.
    logging.info(args)

  def StoreFiles(self, args):
    """Stores the given file contents to specified directory.

    Args:
      args: A list of string tuples: (file path, file name, file contents).

    Raises:
      SlaveRPCError: If args are invalid will include an error message, or
      if any of the files can't be stored in given folder, or the directory
      can't be created.
    """
    # Validate args.
    if not isinstance(args, list):
      raise SlaveRPCError(
          'Invalid StoreFiles arg type: %s (expected list of str or unicode'
          ' tuples)' % str(type(args)))

    for file_tuple in args:
      if not isinstance(file_tuple, list):
        raise SlaveRPCError(
            'Invalid element type in StoreFiles args: %s (expected str or'
            ' unicode tuple)' % str(type(file_tuple)))
      if len(file_tuple) != 3:
        raise SlaveRPCError(
            'Invalid element len (%d != 3) in StoreFiles args: %s' %
            (len(file_tuple), str(file_tuple)))

      for string in file_tuple:
        if not isinstance(string, basestring):
          raise SlaveRPCError(
              'Invalid tuple element type: %s (expected str or unicode)' %
              str(type(string)))

    # Execute functionality.
    for file_path, file_name, file_contents in args:
      logging.debug('Received file name: ' + file_name)

      try:
        self._MakeDirectory(file_path)
      except os.error as e:
        raise SlaveRPCError('MakeDirectory exception: ' + str(e))

      try:
        self._StoreFile(file_path, file_name, file_contents)
      except IOError as e:
        raise SlaveRPCError('StoreFile exception: ' + str(e))

  def _MakeDirectory(self, path):
    """Creates requested folder if it doesn't exist.

    Args:
      path: The folder path to create recursively.

    Raises:
      os.error: If the directory can't be created.
    """
    if path and not os.path.exists(path):
      os.makedirs(path)
      logging.debug('Created file path: ' + path)

  def _StoreFile(self, file_path, file_name, file_contents):
    """Stores file_contents in given path and name.

    Args:
      file_path: The folder the file should go in.
      file_name: The file name.
      file_contents: Contents of the file to store.

    Raises:
      IOError: the file can't be opened.
    """
    full_name = os.path.join(file_path, file_name)

    file_p = open(full_name, 'wb')
    file_p.write(file_contents)
    file_p.close()

    logging.debug('File stored: ' + full_name)

  def RunCommands(self, args):
    """Checks type of args to be correct.

    Args:
      args: A list of strings to pass to the python executable to run.

    Raises:
      SlaveRPCError: If args are invalid will include an error message, or
      if executing the commands fails.
    """
    # Validate args.
    if not isinstance(args, list):
      raise SlaveRPCError(
          'Invalid RunCommands arg type: %s (expected list of str or'
          ' unicode)'%str(type(args)))

    for command in args:
      if not isinstance(command, basestring):
        raise SlaveRPCError(
            'Invalid element type in RunCommands args: %s (expected'
            ' str or unicode)'% str(type(command)))

    # Execute functionality.
    commands = [sys.executable] + args

    try:
      logging.debug('Running command: %s', commands)
      subprocess.check_call(commands)
    except subprocess.CalledProcessError as e:
      if e.returncode == swarm_constants.RESTART_EXIT_CODE:
        Restart()
      # The exception message will contain the commands that were
      # run and error code returned.
      raise SlaveRPCError(str(e))
    else:
      logging.debug('done!')
      # At this point the script called by subprocess has handled any further
      # communication with the swarm server.

  def UpdateSlave(self, args):
    """Download the current version of the slave code and then run it.

    Args:
      args: The url for the slave code.

    Raises:
      SlaveRPCError: If args are invalid.
    """
    if not isinstance(args, basestring):
      raise SlaveRPCError(
          'Invalid arg types to UpdateSlave: %s (expected str or unicode)' %
          str(type(args)))

    if not url_helper.DownloadFile(ZIPPED_SLAVE_FILES, args):
      logging.error('Unable to download required slave files.')
      return

    with open('slave_setup.py', 'w') as f:
      f.write(SLAVE_SETUP_SCRIPT)

    logging.info('New slave code downloaded, replacing this process to allow '
                 'updating these files. After the files are replaced this '
                 'slave will be restarted through start_slave.py')

    sys.stdout.flush()
    sys.stderr.flush()
    # Repeat sys.executable since the first one is what we call, and the
    # second one is arg[0].
    os.execl(sys.executable, sys.executable, 'slave_setup.py')


# TODO(user): Move function to another file.
def BuildRPC(func_name, args):
  """Builds a dictionary of an operation that needs to be executed.

  Args:
    func_name: a string of the function name to execute on the remote host.
    args: arguments to be passed to the function.

  Returns:
    A dictionary containing them function name and args.
  """

  return {'function': func_name, 'args': args}


# TODO(user): Move function to another file.
def ParseRPC(rpc):
  """Parses RPC created by BuildRPC into a tuple.

  Args:
    rpc: dictionary containing function name and args.

  Returns:
    A tuple of (str, args) of function name and args.

  Raises:
    SlaveError: with human readable string.
  """

  if not isinstance(rpc, dict):
    raise SlaveError('Invalid RPC container')

  fields = ['function', 'args']
  for key in rpc:
    try:
      fields.remove(key)
    except ValueError:
      raise SlaveError('Invalid extra arg to RPC: ' + key)

  if fields:
    raise SlaveError('Missing mandatory field to RPC: ' + str(fields))

  function = rpc['function']
  args = rpc['args']

  if not isinstance(function, basestring):
    raise SlaveError('Invalid RPC call function name type')

  logging.debug('rpc function name: ' + function)
  logging.debug('rpc function arg type: ' + str(type(args)))
  logging.debug('rpc function args: %s', str(args))

  return (function, args)


class SlaveError(Exception):
  """Simple error exception properly scoped here."""
  pass


class SlaveRPCError(Exception):
  """Simple error exception properly scoped here."""
  pass


def main():
  parser = optparse.OptionParser(
      usage='%prog [options] [filename]',
      description='Initialize the machine as a swarm slave. The dimensions of '
      'the machine are either given through a file (if provided) or read from '
      'stdin. See http://code.google.com/p/swarming/wiki/MachineProvider for '
      'complete details.')
  parser.add_option('-a', '--address', default='https://localhost',
                    help='Address of the Swarm server to connect to. '
                    'Defaults to %default. ')
  parser.add_option('-p', '--port', default='443', type='int',
                    help='Port of the Swarm server. '
                    'Defaults to %default, which is the default https port.')
  parser.add_option('-r', '--max_url_tries', default=10, type='int',
                    help='The maximum number of times url messages will '
                    'attempt to be sent before accepting failure. Defaults '
                    'to %default')
  parser.add_option('--keep_alive', action='store_true',
                    help='Have the slave swallow all exceptions and run'
                    'forever.')
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Set logging level to DEBUG. Optional. Defaults to '
                    'ERROR level.')
  parser.add_option('-i', '--iterations', default=-1, type='int',
                    help='Number of iterations to request jobs from '
                    'Swarm server. Defaults to %default (infinite).')
  parser.add_option('-d', '--directory', default='.',
                    help='Sets the working directory of the slave. '
                    'Defaults to %default. ')
  parser.add_option('-l', '--log_file', default='slave_machine.log',
                    help='Set the name of the file to log to. '
                    'Defaults to %default.')
  parser.add_option('--id_filename', default=DEFAULT_MACHINE_ID_FILE,
                    help='The file to load the machine id from. If the file '
                    'doesn\'t exist a new file will be create with a new ID '
                    'retrieved from the swarm server. Defaults to %default')
  (options, args) = parser.parse_args()

  # Parser handles exiting this script after logging the error.
  if len(args) > 1:
    parser.error('Must specify only one filename')

  if options.iterations < -1 or options.iterations == 0:
    parser.error('Number of iterations must be -1 or a positive number')

  logging.basicConfig()
  log_file = logging.handlers.RotatingFileHandler(options.log_file,
                                                  maxBytes=10 * 1024 *1024,
                                                  backupCount=5)
  log_file.setFormatter(
      logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
  logging.getLogger().addHandler(log_file)

  if options.verbose:
    logging.getLogger().setLevel(logging.DEBUG)
  else:
    logging.getLogger().setLevel(logging.ERROR)

  # Open the specified file, or stdin.
  if not args:
    source = sys.stdin
  else:
    filename = args[0]
    try:
      source = open(filename)
    except IOError:
      logging.error('Cannot open file: %s', filename)
      return

  # Read machine informations.
  attributes = json.load(source)
  source.close()

  url = '%s:%d' % (options.address, options.port)
  slave = SlaveMachine(url=url, attributes=attributes,
                       max_url_tries=options.max_url_tries,
                       id_filename=options.id_filename)

  # Change the working directory to specified path.
  os.chdir(options.directory)

  while True:
    # Start requesting jobs.
    try:
      slave.Start(iterations=options.iterations)
    except SlaveError as e:
      logging.exception('Slave start threw an exception:\n%s', e)

    if not options.keep_alive:
      break
    logging.info('Slave is set to stay alive, starting again.')


if __name__ == '__main__':
  sys.exit(main())
