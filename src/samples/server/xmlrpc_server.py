#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""An XmlRpcServer serving as entry points to a remote machine."""


import logging
import optparse
import os
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from SimpleXMLRPCServer import SimpleXMLRPCServer
import subprocess
import tempfile
import threading
import xmlrpclib


class _RemoteExecutor(object):
  """Implements all methods exposed by a remote machine."""

  def __init__(self):
    """Default initialization.

    Attributes:
      _sid: The ID of the server.
      _autoupdate: The Autoupdate state, True for need update, False otherwise.
    """

    # The value that the sid method should return.
    self._sid = 1
    # The value that the autoupdate method should return.
    self._autoupdate = False

  def exists(self, os_path):  # pylint: disable-msg=C6409
    """Returns True if os_path exists.

    Args:
      os_path: The path to test existance of.

    Returns:
      True if os_path exists. False otherwise.
    """
    return os.path.exists(os_path)

  def makedirs(self, os_path):  # pylint: disable-msg=C6409
    """Creates all the inexistent dirs in os_path.

    Args:
      os_path: The path of dirs to make.

    Returns:
      0. Because the XmlRpcServer must return something, not None...
    """
    os.makedirs(os_path)
    return 0

  def upload(self, os_path, content):  # pylint: disable-msg=C6409
    """Upload content to os_path.

    Args:
      os_path: The path where to upload content.
      content: The content to upload to os_path. Can be of type xmlrpclib.Binary
          or a string.

    Returns:
      The os_path where the content was uploaded.
    """
    new_file = open(os_path, 'wb')
    if isinstance(content, xmlrpclib.Binary):
      new_file.write(content.data)
    else:
      new_file.write(content.encode('utf8', 'replace'))
    new_file.close()
    return os_path

  def start(self, command, args,  # pylint: disable-msg=C6409
            unused_wait_for_completion, capture):
    """Starts the command with the given args and potentially wait for results.

    Args:
      command: The command to execute.
      args: The list of arguments to pass to the command.
      unused_wait_for_completion: We don't use it, we only rely on capture.
      capture: Specfifies if we should wait to capture all results.

    Returns:
      The identifier of the started process if we don't capture the results.
      -1 in case of errors.
      If the results are captured, we return a tuple containing the exit code
      and stdout. (-1, <error text>) in case of errors.
    """
    logging.debug('starting process: %s', [command] + args)

    if capture:
      # Use a temporary file descriptor for the stdout pipe out of Popen so that
      # we can read that file with another file object and not interfere. Also
      # avoiding the known deadlock bugs with subprocess default PIPE.
      (stdout_file_descriptor, stdout_file_name) = tempfile.mkstemp(text=True)
      stdout_file = open(stdout_file_name)

      def CleanupTempFiles():
        os.close(stdout_file_descriptor)
        stdout_file.close()
        os.remove(stdout_file_name)

    else:
      stdout_file_descriptor = subprocess.PIPE

    try:
      proc = subprocess.Popen([command] + args, stdout=stdout_file_descriptor,
                              bufsize=1, stderr=subprocess.STDOUT,
                              stdin=subprocess.PIPE)
    except OSError as e:
      logging.exception('Execution of %s raised exception: %s.',
                        str([command] + args), e)
      if capture:
        CleanupTempFiles()
        return (-1, e)
      else:
        return -1

    logging.debug('process id: %s', proc.pid)
    if capture:
      logging.debug('waiting to capture')
      proc.wait()
      logging.debug('done!')
      stdout_text = stdout_file.read()
      CleanupTempFiles()
      return (proc.returncode, stdout_text)
    else:
      return proc.pid

  def sid(self):  # pylint: disable-msg=C6409
    """Returns the server ID.

    Returns:
      The ID of the server.
    """
    return self._sid

  def autoupdate(self):  # pylint: disable-msg=C6409
    """Returns the autoupdate state.

    Returns:
      True if we should autoupdate, False otherwise.
    """
    return self._autoupdate


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  """A specialized request handler to specify RPC2."""

  rpc_paths = ('/RPC2',)


class XmlRpcServer(object):
  """The class running the server so we can import and start it manually."""

  def __init__(self):
    """Default initialization.

    Attributes:
      _server: A SimpleXMLRPCServer so make our objects and method available.
      _thread: A Thread in which to run the shutdown code to avoid deadlocks.
    """
    self._server = None
    self._thread = None

  def _ShutdownServer(self):
    """Shutting down the server from another thread."""
    assert self._thread.ident == threading.currentThread().ident

    if self._server:
      logging.debug('XmlRpcServer shutting down the server')
      self._server.shutdown()
      self._server.server_close()
      self._server = None
      logging.debug('XmlRpcServer _Shutdown done')

  def Shutdown(self):
    """Shuts down the server.

    Returns:
      True.
    """
    # We must do this from another thread since it blocks and would create
    # a deadlock while the server waits for this request to be handled.
    if self._server:
      self._thread = threading.Thread(target=self._ShutdownServer)
      logging.debug('shutdown pending')
      self._thread.start()
    return True

  def Start(self, verbose, started_event=None, done_event=None):
    """Start the server.

    Args:
      verbose: True for verbose logging, False for errors only.
      started_event: An event to signal when we are ready to start.
          Used mainly for testing purposes. Defaults to None.
      done_event: An event to signal when we are done shutting down.
          Used mainly for testing purposes. Defaults to None.
    """
    if verbose:
      logging.getLogger().setLevel(logging.DEBUG)
    else:
      logging.getLogger().setLevel(logging.ERROR)

    logging.debug('Creating server')
    #TODO(user): Dynamically choose a port.
    self._server = SimpleXMLRPCServer(('', 7399), logRequests=verbose,
                                      requestHandler=RequestHandler)

    logging.debug('registering RemoteExecutor')
    self._server.register_instance(_RemoteExecutor())

    logging.debug('registering introspection functions')
    self._server.register_introspection_functions()

    logging.debug('registering Shutdown function')
    self._server.register_function(self.Shutdown)

    if started_event:
      started_event.set()

    # Run the server's main loop
    logging.debug('Serving until Shutdown is called.')
    self._server.serve_forever()
    logging.debug('shutdown complete')

    if self._thread.isAlive():
      logging.debug('joining shutdown thread')
      self._thread.join()
      logging.debug('joined shutdown thread')
    self._thread = None

    if done_event:
      logging.debug('signaling done event')
      done_event.set()


if __name__ == '__main__':
  _DESCRIPTION = ('This script runs an xmlrpc server to be used by Swarm.')

  parser = optparse.OptionParser(usage='%prog [options] [filename]',
                                 description=_DESCRIPTION)
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Set logging level to DEBUG. Optional. '
                    'Defaults to ERROR level.')
  (options, other_args) = parser.parse_args()

  XmlRpcServer().Start(options.verbose)
