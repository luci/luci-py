#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""A fake XmlRpcServer mocking a provided machine."""




import logging
import optparse
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from SimpleXMLRPCServer import SimpleXMLRPCServer
import threading


class _MockMachine(object):
  """Mocks for all the methods exposed by a provided machine."""

  def __init__(self):
    # The list of existing paths that the exists method recognizes as existant.
    self._existing_paths = []
    # To get the makedirs call to fail.
    self._fail_makedirs = False
    # To get the upload call to fail.
    self._fail_upload = False
    # The content that has been uploaded with the name of the file as a key
    # and the content of the file as a value.
    self._uploaded_content = {}
    # Instruct the upload method to return an empty destination path.
    self._upload_return_empty = False
    # Collects the commands that have been sent to the start method.
    self._started_commands = []
    # A list of command results that the start method should return
    # sequentially, which means that when a result is returned it is removed
    # from the list.
    self._seq_command_results = []
    # The value that the sid method should return.
    self._sid = False
    # The value that the autoupdate method should return.
    self._autoupdate = False

  def AddExistingPath(self, os_path):
    if os_path:
      self._existing_paths.append(os_path)
    return 0

  def RemoveExistingPath(self, os_path):
    self._existing_paths.remove(os_path)
    return 0

  def exists(self, os_path):  # pylint: disable-msg=C6409
    return os_path in self._existing_paths

  def FailMakedirs(self, fail):
    self._fail_makedirs = fail
    return 0

  def makedirs(self, os_path):  # pylint: disable-msg=C6409
    if self._fail_makedirs:
      raise OSError(2, 'Failed to makedirs')
    self.AddExistingPath(os_path)
    return 0

  def SetFailUpload(self, fail):
    self._fail_upload = fail
    return 0

  def SetUploadReturnEmpty(self, empty):
    self._upload_return_empty = empty
    return 0

  def UploadedContent(self, os_path):
    return self._uploaded_content[os_path]

  def upload(self, os_path, content):  # pylint: disable-msg=C6409
    self._uploaded_content[os_path] = content
    if self._fail_upload:
      raise OSError(1, 'Failed!')
    if self._upload_return_empty:
      return ''
    else:
      return os_path

  def AddCommandResults(self, results):
    self._seq_command_results.insert(0, results)
    return True

  def StartedCommands(self):
    return self._started_commands

  def start(self, command, args,  # pylint: disable-msg=C6409
            unused_wait_for_completion, capture):
    self._started_commands.append((command, args, capture))
    if self._seq_command_results:
      return self._seq_command_results.pop()
    else:
      return 42

  def SetSid(self, sid):
    self._sid = sid
    return True

  def sid(self):  # pylint: disable-msg=C6409
    return self._sid

  def SetAutoupdate(self, autoupdate):
    self._autoupdate = autoupdate
    return True

  def autoupdate(self):  # pylint: disable-msg=C6409
    return self._autoupdate


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)


class XmlRpcServer(object):
  """The class running the server so we can import and start it manually."""

  def __init__(self):
    self._server = None
    self._thread = None

  def __del__(self):
    # If we started a server, shut it down or wait for the thread doing it.
    if self._server:
      if not self._thread:
        self._server.shutdown()
      else:
        self._thread.join()

  def Shutdown(self):
    # We must do this from another thread since it blocks and would create
    # a deadlock while the server waits for this request to be handled.
    if self._server:
      self._thread = threading.Thread(target=self._server.shutdown).start()
    return True

  def Start(self, verbose):
    """Start the server.

    Args:
      verbose: True for verbose logging, False for errors only.
    """
    if verbose:
      logging.getLogger().setLevel(logging.DEBUG)
    else:
      logging.getLogger().setLevel(logging.ERROR)

    logging.debug('Creating server')
    #TODO(user): Find a way to dynamically choose a port.
    self._server = SimpleXMLRPCServer(('localhost', 7399), logRequests=verbose,
                                      requestHandler=RequestHandler)

    logging.debug('registering MockMachine')
    self._server.register_instance(_MockMachine())

    logging.debug('registering introspection functions')
    self._server.register_introspection_functions()

    logging.debug('registering Shutdown function')
    self._server.register_function(self.Shutdown)

    # Run the server's main loop
    logging.debug('Serving until Shutdown is called.')
    self._server.serve_forever()


if __name__ == '__main__':
  _DESCRIPTION = ('This script runs a fake xmlrpc server used to test Swarm.')

  parser = optparse.OptionParser(
      usage='%prog [options] [filename]',
      description=_DESCRIPTION)
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Set logging level to DEBUG. Optional. '
                    'Defaults to ERROR level.')
  (options, other_args) = parser.parse_args()

  XmlRpcServer().Start(options.verbose)
