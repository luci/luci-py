#!/usr/bin/python2.4
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


import logging
import optparse
import os
import subprocess
import sys
import time
import urllib
import urllib2

try:
  import simplejson as json  # pylint: disable-msg=C6204
except ImportError:
  import json  # pylint: disable-msg=C6204

# Number of times in a row to try connect to the Swarm server before giving up.
CONNECTION_RETRIES = 10

# Number of seconds to wait between two consecutive tries.
DELAY_BETWEEN_RETRIES = 2


class SlaveMachine(object):
  """Creates a slave that continuously polls the Swarm server for jobs."""

  def __init__(self, url='http://localhost:8080', attributes=None):
    """Sets the parameters of the slave.

    Args:
      url: URL of the Swarm server.
      attributes: A dict of the attributes of the machine. Should include
      machine dimensions as well.
    """
    self._url = url
    self._attributes = attributes.copy() if attributes else {}
    self._result_url = None
    self._attributes['id'] = None
    self._attributes['try_count'] = 0

    # Each RPC has (1) optional function to validate args (2) function to
    # execute. The validate function, if given, only checks the format of the
    # args and returns an error message if there is a problem with args, or
    # None. The execute function should throw a SlaveRPCError if anything goes
    # wrong while executing the RPC command.
    self._rpc_map = {
        'LogRPC': (self._LogRPCValidateArgs, self._LogRPCExecute),
        'StoreFiles': (self._StoreFilesRPCValidate,
                       self._StoreFilesRPCExecute),
        'RunCommands': (self._RunCommandsRPCValidateArgs,
                        self._RunCommandsRPCExecute),
        }

  def Start(self, iterations=-1):
    """Starts the slave, which polls the Swarm server for jobs until it dies.

    Args:
      iterations: Number of times to poll the Swarm server. -1 indicates
      infinitely. Failing to connect to the server DOES NOT count as an
      iteration. This is useful for testing the slave and having an exit
      condition.

    Raises:
      SlaveError: If the slave in unable to connect to the provided URL after
      a few retries, or an invalid number of iterations were requested.
    """

    url = self._url + '/poll_for_test'
    done_iterations = 0
    try:
      iterations = int(iterations)
    except ValueError:
      raise SlaveError(
          'Invalid iterations provided: ' + str(iterations))

    connection_retries = CONNECTION_RETRIES

    # Loop for requested number of iterations.
    while True:
      request = {
          'attributes': json.dumps(self._attributes)
          }

      # Reset the result_url to avoid posting to the wrong place.
      self._result_url = None

      logging.debug('Connecting to Swarm server: ' + self._url)
      logging.debug('Request: ' + str(request))

      try:
        server_response = urllib2.urlopen(url, data=urllib.urlencode(request))
        response_str = server_response.read()
      except urllib2.URLError as e:
        connection_retries -= 1
        if connection_retries == 0:
          raise SlaveError('Error when connecting to Swarm server: ' + str(e))
        else:
          logging.info('Unable to connect to Swarm server'
                       ' - retrying %d more times (error: %s)',
                       connection_retries, str(e))

          # Wait a specified amount of time before retrying (secs).
          time.sleep(DELAY_BETWEEN_RETRIES)
          continue

      # If a successful connection is made, reset the counter.
      connection_retries = CONNECTION_RETRIES

      response = None
      try:
        response = json.loads(response_str)
      except ValueError:
        self._PostFailedExecuteResults('Invalid response: ' + response_str)
      else:
        self._ProcessResponse(response)

      # Continuously loop until we hit the requested number of iterations.
      if iterations != -1:
        done_iterations += 1
        if done_iterations >= iterations:
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
      for rpc in commands:
        function_name, args = ParseRPC(rpc)
        try:
          self._ExecuteRPC(function_name, args)
        except SlaveRPCError as e:
          self._PostFailedExecuteResults(str(e))
          break

  def _ExecuteRPC(self, name, args):
    """Execute the function with given args.

    Args:
      name: Function name to call.
      args: Arguments to pass to function.

    Returns:
      The result of the execute function.
    """
    return self._rpc_map[name][1](args)

  def _ValidateRPCArgs(self, name, args):
    """Validate the given args to an RPC function.

    Args:
      name: Function name to validate.
      args: Arguments to pass to function to validate.

    Returns:
      The result of the validation function.
    """
    if self._rpc_map[name][0]:
      return self._rpc_map[name][0](args)

    return None

  def _ParseResponse(self, response):
    """Stores relevant fields from response to slave machine.

    Args:
      response: Response dict returned by _ValidateResponse.

    Returns:
      List of commands to execute, None if none specified by the server.
    """

    # Store id assigned by Swarm server so in the future they know this slave.
    self._attributes['id'] = str(response['id'])
    logging.debug('received id: ' + str(self._attributes['id']))

    # Store try_count assigned by Swarm server to send it back in next request.
    self._attributes['try_count'] = int(response['try_count'])
    logging.debug('received try_count: ' + str(self._attributes['try_count']))

    commands = None
    if not 'commands' in response:
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
        isinstance(response['result_url'], (str, unicode))):
      self._result_url = str(response['result_url'])

    # Validate fields in the response. A response should have 'id', 'try_count',
    # and only either one of ('come_back') or ('commands', 'result_url').
    required_fields = ['id', 'try_count']
    if 'commands' in response:
      required_fields += ['commands', 'result_url']
    else:
      required_fields += ['come_back']

    # We allow extra fields in the response, but ignore them.
    for field in response:
      if field in required_fields:
        required_fields.remove(field)

    # Make sure we're not missing anything and don't have extras.
    if required_fields:
      message = ('Missing fields in response: ' + str(required_fields))
      self._PostFailedExecuteResults(message)
      return False

    # Validate ID type.
    if not isinstance(response['id'], (str, unicode)):
      self._PostFailedExecuteResults('Invalid ID type: ' +
                                     str(type(response['id'])))
      return False

    # Validate try_count type.
    if not isinstance(response['try_count'], int):
      self._PostFailedExecuteResults('Invalid try_count type: ' +
                                     str(type(response['try_count'])))
      return False

    # try_count can not be negative.
    if int(response['try_count']) < 0:
      self._PostFailedExecuteResults('Invalid negative try_count value: %d' %
                                     int(response['try_count']))
      return False

    if 'commands' in response:
      # Validate result URL type.
      if not isinstance(response['result_url'], (str, unicode)):
        self._PostFailedExecuteResults('Invalid result URL type: ' +
                                       str(type(response['result_url'])))
        return False

      # Validate commands type.
      if not isinstance(response['commands'], list):
        self._PostFailedExecuteResults('Invalid commands type: ' +
                                       str(type(response['commands'])))
        return False

      # Validate rpc commands.
      for rpc in response['commands']:
        # Validate format.
        try:
          function_name, args = ParseRPC(rpc)
        except SlaveError as e:
          self._PostFailedExecuteResults('Error when parsing RPC: ' + str(e))
          return False

        # Validate function name.
        if function_name not in self._rpc_map:
          self._PostFailedExecuteResults('Unsupported RPC function name: '
                                         + function_name)
          return False

        # Call the validate function of the RPC.
        error_message = self._ValidateRPCArgs(function_name, args)
        if error_message:
          self._PostFailedExecuteResults(error_message)
          return False

    else:
      # If the slave recieves no command, then it will just try again
      # at a later time and when the Swarm server told it to.
      if not isinstance(response['come_back'], float):
        self._PostFailedExecuteResults('Invalid come_back type: ' +
                                       str(type(response['come_back'])))
        return False

      if float(response['come_back']) < 0:
        self._PostFailedExecuteResults('Invalid negative come_back value: %f'%
                                       float(response['come_back']))
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

    try:
      # Simply specifying data to urlopen makes it a POST.
      urllib2.urlopen(
          self._result_url, urllib.urlencode(
              (('x', str(result_code)),
               ('s', False),
               ('r', result_string))))
    except urllib2.URLError as e:
      logging.exception('Can\'t post result to url %s.\nError: %s',
                        self._result_url, str(e))

  def _LogRPCValidateArgs(self, args):
    """Checks type of args to be correct.

    Args:
      args: Provided by Swarm server.

    Returns:
      If args are invalid, will return an error message. None otherwise.
    """
    if not isinstance(args, (str, unicode)):
      return ('Invalid arg types to LogRPC: %s (expected str or unicode)'%
              str(type(args)))

    return None

  def _LogRPCExecute(self, args):
    """Logs given args to logging.debug."""
    logging.info(args)

  def _StoreFilesRPCValidate(self, args):
    """Checks type of args to be correct.

    Args:
      args: Provided by Swarm server. Should be a list of string
      tuples formatted (file path, file name, file contents).

    Returns:
      If args are invalid, will return an error message. None otherwise.
    """
    if not isinstance(args, list):
      return ('Invalid StoreFiles arg type: %s (expected list of'
              ' str or unicode tuples)'%str(type(args)))

    for file_tuple in args:
      if not isinstance(file_tuple, list):
        return ('Invalid element type in StoreFiles args: %s'
                ' (expected str or unicode tuple)'% str(type(file_tuple)))
      if len(file_tuple) != 3:
        return ('Invalid element len (%d != 3) in StoreFiles args:'
                ' %s'%(len(file_tuple), str(file_tuple)))

      for string in file_tuple:
        if not isinstance(string, (str, unicode)):
          return ('Invalid tuple element type: %s (expected str or unicode)'%
                  str(type(string)))

    return None

  def _StoreFilesRPCExecute(self, args):
    """Stores the given file contents to specified directory.

    Args:
      args: A list of string tuples: (file path, file name, file contents).
    Raises:
      SlaveRPCError: If any of the files can't be stored in given folder, or
      the directory can't be created.
    """
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

  def _RunCommandsRPCValidateArgs(self, args):
    """Checks type of args to be correct.

    Args:
      args: Should be a list of strings to pass to python.

    Returns:
      If args are invalid, will return an error message. None otherwise.
    """
    if not isinstance(args, list):
      return ('Invalid RunCommands arg type: %s (expected list of str or'
              ' unicode)'%str(type(args)))

    for command in args:
      if not isinstance(command, (str, unicode)):
        return ('Invalid element type in RunCommands args: %s (expected'
                ' str or unicode)'% str(type(command)))

    return None

  def _RunCommandsRPCExecute(self, args):
    """Executes the given command in args.

    Args:
      args: A list of strings to run.
    Raises:
      SlaveRPCError: If executing the commands fails.
    """
    commands = [sys.executable] + args

    try:
      subprocess.check_call(commands)
    except subprocess.CalledProcessError as e:
      # The exception message will contain the commands that were
      # run and error code returned.
      self._PostFailedExecuteResults(str(e))
    else:
      logging.debug('done!')
      # At this point the script called by subprocess is responsible for
      # notifying the Swarm server if anything goes wrong, so now the job
      # is done.


def BuildRPC(func_name, args):
  """Builds a dictionary of an operation that needs to be executed.

  Args:
    func_name: a string of the function name to execute on the remote host.
    args: arguments to be passed to the function.

  Returns:
    A dictionary containing them function name and args.
  """

  return {'function': func_name, 'args': args}


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

  if not isinstance(function, (str, unicode)):
    raise SlaveError('Invalid RPC call function name type')

  logging.debug('rpc function name: ' + function)
  logging.debug('rpc function arg type: ' + str(type(args)))

  return (function, args)


class SlaveError(Exception):
  """Simple error exception properly scoped here."""
  pass


class SlaveRPCError(Exception):
  """Simple error exception properly scoped here."""
  pass


def main():
  parser = optparse.OptionParser()
  parser.add_option('-a', '--address', dest='address',
                    help='Address of the Swarm server to connect to. '
                    'Defaults to localhost. ', default='localhost')
  parser.add_option('-p', '--port', dest='port',
                    help='Port of the Swarm server. '
                    'Defaults to 8080. ', default='8080')
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Set logging level to DEBUG. Optional. Defaults to '
                    'ERROR level.')
  parser.add_option('-i', '--iterations', default=-1, dest='iterations',
                    help='Number of iterations to request jobs from '
                    'Swarm server. Defaults to -1 (infinite).')
  (options, args) = parser.parse_args()

  if not args:
    args.append('-')
  elif len(args) > 1:
    parser.error('Must specify only one filename')

  if options.verbose:
    logging.getLogger().setLevel(logging.DEBUG)
  else:
    logging.getLogger().setLevel(logging.ERROR)

  filename = args[0]
  # Open the specified file, or stdin.
  if filename == '-':
    source = sys.stdin
  else:
    try:
      source = open(filename)
    except IOError:
      print 'Cannot open file: ' + filename
      return

  # Read machine informations.
  attributes_str = source.read()
  source.close()
  attributes = json.loads(attributes_str)

  url = 'http://'+options.address+':'+options.port
  slave = SlaveMachine(url=url,
                       attributes=attributes)

  # Start requesting jobs.
  slave.Start(iterations=options.iterations)


if __name__ == '__main__':
  sys.exit(main())
