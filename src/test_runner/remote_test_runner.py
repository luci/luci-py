#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.




import httplib
import logging
import os.path
import sys
import time
import xmlrpclib


# Global scope so we can mock it for testing...
class Transport(xmlrpclib.Transport):
  """Use our own transport so that we can set the connection timeout."""

  def __init__(self, use_datetime=0):
    xmlrpclib.Transport.__init__(self, use_datetime)

  def make_connection(self, host):  # pylint: disable-msg=C6409
    """Override the creation of the connection.

    Args:
      host: The host to connect to.
    Returns:
      A connection HTTP object.
    """

    class MyHTTP(httplib.HTTP):
      """Override the HTTP to specify a timeout on the connection."""

      # We explicitly bypass the base class __init__ method so that we can
      # add our timeout. Thus the pylint disabling...
      # pylint: disable-msg=W0231
      def __init__(self, host='', port=None, strict=None):
        """Everythin is copied from base class, except for the added timeout.

        Args:
          host: The host to connect to.
          port: The port to use.
          strict: To be strict about valid HTTP/1.0 or 1.1 status line.
        """

        # some joker passed 0 explicitly, meaning default port
        if port == 0:
          port = None

        # Note that we may pass an empty string as the host; this will throw
        # an error when we attempt to connect. Presumably, the client code
        # will call connect before then, with a proper host.
        self._setup(self._connection_class(host, port, strict, timeout=60))
      # pylint: enable-msg=W0231

    # Same thing as base class, except for using MyHTTP and anonymizing the
    # unused return values with _.
    host, _, _ = self.get_host_info(host)
    return MyHTTP(host)


class RemoteTestRunner(object):
  """A remote test runner using an XML RPC server.

  Given the URL of an xmlrpclib.ServerProxy, this class can upload local files
  and/or textual content to remote files and then execute commands on the
  server.
  """

  def __init__(self, server_url='', remote_root=r'swarm_tests',
               text_to_upload=None, file_pairs_to_upload=None,
               commands_to_execute=None):
    """Inits RemoteTestRunner with a a set of attributes."""
    self._xml_rpc_server = None
    self.SetServerProxyUrl(server_url)

    self._text_to_upload = []
    if text_to_upload:
      self.SetTextToUpload(text_to_upload)

    self._file_pairs_to_upload = []
    if file_pairs_to_upload:
      self.SetFilePairsToUpload(file_pairs_to_upload)

    self._commands_to_execute = []
    if commands_to_execute:
      self.SetCommandsToExecute(commands_to_execute)

    self._remote_root = None
    self.SetRemoteRoot(remote_root)

    # To allow tests to customize sleep/retry behavior.
    self.xmlrpc_autoupdate_retries = 60
    self.xmlrpc_autoupdate_sleeps = 5

    self.xmlrpc_startup_retries = 4
    self.xmlrpc_startup_sleeps = 30

  @staticmethod
  def _ValidateListContent(list_objects, validator):
    """Validates the content of the given list with the given validator.

    Args:
      list_objects: A list of objects to validate using validator.
      validator: A function pointer to validate each element of list_objects.

    Returns:
      True is the list is valid, False otherwise.
    """
    if not isinstance(list_objects, list):
      return False
    for list_object in list_objects:
      if not validator(list_object):
        return False
    return True

  @staticmethod
  def _ValidateTextToUpload(text_to_upload):
    """Validates a given text to upload tuple.

    A valid text to upload must be a tuple with two string entries.

    Args:
      text_to_upload: The tuple to be validated.

    Returns:
      True is text_to_upload is valid, False otherwise.
    """
    if (not isinstance(text_to_upload, (tuple, list)) or
        len(text_to_upload) is not 2 or
        not isinstance(text_to_upload[0], str) or
        not isinstance(text_to_upload[1], str)):
      logging.error('Text to upload isn\'t valid: %s', text_to_upload)
      return False
    return True

  @staticmethod
  def _ValidateFilePairToUpload(file_pair_to_upload):
    """Validates a given file to be uploaded.

    file_pair_to_upload is either a list or tuple with exactly two items. The
    first is an absolute path to the local file to be uploaded, and the
    second is a relative path, relative to the remote_root directory.

    Args:
      file_pair_to_upload: The tuple to be validated.

    Returns:
      True is file_pair_to_upload is valid, False otherwise.
    """
    # We have a very similar condition set to _ValidateTextToUpload().
    if (not RemoteTestRunner._ValidateTextToUpload(file_pair_to_upload) or
        not os.path.exists(os.path.abspath(file_pair_to_upload[0])) or
        os.path.isabs(file_pair_to_upload[1])):
      logging.error('File to upload isn\'t a tuple with a valid local path and '
                    'relative destination path: %s', file_pair_to_upload)
      return False
    return True

  def _AutoUpdateServer(self):
    """Autoupdate the self._xml_rpc_server.

    Returns:
      True is self._xml_rpc_server is valid and autoupdated.
    """
    if getattr(self, 'autoupdated_server', False):
      return True

    if not self.EnsureValidServer():
      return False

    try:
      # We use the sid to identify when the autoupdate is finished.
      logging.info('Checking installed version if we need to autoupdate.')
      existing_sid = self._xml_rpc_server.sid()
      update_available = self._xml_rpc_server.autoupdate()
      if update_available:
        logging.info('Update available. Looking for version beyond: %s',
                     existing_sid)
        # Wait for tehelper to restart.
        num_autoupdate_retries_left = self.xmlrpc_autoupdate_retries
        while (num_autoupdate_retries_left > 0 and
               self._xml_rpc_server.sid() == existing_sid):
          num_autoupdate_retries_left -= 1
          time.sleep(self.xmlrpc_autoupdate_sleeps)
        if self._xml_rpc_server.sid() == existing_sid:
          assert self._xml_rpc_server_url is not None
          logging.exception('Failed to autoupdate XmlRpc Server at URL %s.',
                            self._xml_rpc_server_url)
          return False
    # We need to catch all since AppEngine may throw specific things that
    # we won't be able to specify outside of the appengine context.
    except Exception, e:  # pylint: disable-msg=W0703
      assert self._xml_rpc_server_url is not None
      logging.exception('Failed to autoupdate XmlRpc Server  at URL %s.\n'
                        'Exceptions:\n%s', self._xml_rpc_server_url, e)
      return False
    # Remember that we successfully autoupdated the server.
    logging.info('Autoupdate done!')
    self.autoupdated_server = True
    return True

  def _ReadFromFile(self, file_path):
    """Wrapper for opening and reading files to make it easier to mock.

    Args:
      file_path: The path of the file to read.

    Returns:
      The text content of the file.
    """
    file_content = None
    try:
      file_handle = None
      try:
        file_handle = open(file_path, 'rb')
        file_content = file_handle.read()
      except IOError, e:
        logging.exception('Could not load %s.\nException: %s', file_path, e)
        return None
    finally:
      if file_handle:
        file_handle.close()
    return file_content

  def EnsureValidServer(self):
    """Validates the self._xml_rpc_server.

    Returns:
      True is self._xml_rpc_server is valid.
    """
    if getattr(self, 'validated_server', False):
      return True

    if self._xml_rpc_server is None:
      logging.error('You must specify an XmlRpc Server URL.')
      return False

    # The server might not be quite ready even though the machine is ready, so
    # we try a few times.
    num_startup_retries_left = self.xmlrpc_startup_retries
    exceptions = []
    while num_startup_retries_left > 0:
      num_startup_retries_left -= 1
      try:
        logging.info('Validating XMLRPC server by asking for its SID()')
        self._xml_rpc_server.sid()
        # Remember that we validated the server.
        # It would have raised an error if it wasn't valid.
        self.validated_server = True
        return True
      # We need to catch all since AppEngine may throw specific things that
      # we won't be able to specify outside of the appengine context.
      except Exception, e:  # pylint: disable-msg=W0703
        exceptions.append(e)
        # Give some time for the initial startup to properly complete.
        time.sleep(self.xmlrpc_startup_sleeps)
    # If we get out of the loop it means we couldn't access the server at all.
    assert self._xml_rpc_server_url is not None
    logging.exception('%s is not a valid XmlRpc Server URL.\nExceptions:\n%s',
                      self._xml_rpc_server_url, exceptions)
    return False

  def SetServerProxyUrl(self, server_url):
    """Sets the URL to the xmlrpclib.ServerProxy() and open it.

    Args:
      server_url: The URL to the xmlrpclib.ServerProxy().
    """
    if server_url:
      try:
        logging.info('Creating XMLRPC server proxy to: %s', server_url)
        self._xml_rpc_server = xmlrpclib.ServerProxy(server_url,
                                                     transport=Transport())
      except IOError, e:
        logging.exception('%s is not a valid XmlRpc Server URL.\nException: %s',
                          server_url, e)

      # Remember the URL for logging purposes.
      self._xml_rpc_server_url = server_url

  def AddTextToUpload(self, remote_file, text_to_upload):
    """Adds a new text to upload as a file to the XML RPC server.

    Args:
      remote_file: The relative path of the file to upload to.
      text_to_upload: The text to upload to the remote file.
    """
    if self._ValidateTextToUpload((remote_file, text_to_upload)):
      self._text_to_upload.append((remote_file, text_to_upload))

  def AddFilePairToUpload(self, file_pair_to_upload):
    """Adds a new file to upload to the XML RPC server.

    Args:
      file_pair_to_upload: The relative path to the file to upload.
    """
    if self._ValidateFilePairToUpload(file_pair_to_upload):
      self._file_pairs_to_upload.append(file_pair_to_upload)

  def AddCommandToExecute(self, command_to_execute):
    """Adds a new command to execute on the XML RPC server.

    Args:
      command_to_execute: The command to execute on the XML RPC server.
    """
    if (not command_to_execute or
        not self._ValidateListContent(command_to_execute,
                                      lambda x: isinstance(x, str))):
      logging.error('Invalid command: %s', command_to_execute)
      return
    self._commands_to_execute.append(command_to_execute)

  def SetTextToUpload(self, text_to_upload):
    """Sets the array of text to upload tuple.

    A text to upload tuple consist of a remote file path and the text to fill
    it with. E.g., ('folder/hello.txt', 'Hello World! Wherever you are...').

    Args:
      text_to_upload: An array of text to upload tuple.
    """
    if not self._ValidateListContent(text_to_upload,
                                     self._ValidateTextToUpload):
      logging.error('Invalid text list: %s', text_to_upload)
      return
    self._text_to_upload = text_to_upload

  def SetFilePairsToUpload(self, file_pairs_to_upload):
    """Sets the array of file pairs to upload to the XML RPC server.

    The file pairs to upload must be either lists or tuples with exactly two
    items. The first is an absolute path to the local file to be uploaded,
    and the second is a relative path, relative to the remote_root directory.

    Args:
      file_pairs_to_upload: An array of file pairs to upload.
    """
    if not self._ValidateListContent(file_pairs_to_upload,
                                     self._ValidateFilePairToUpload):
      logging.error('Invalid files list: %s', file_pairs_to_upload)
      return
    self._file_pairs_to_upload = file_pairs_to_upload

  def SetCommandsToExecute(self, commands_to_execute):
    """Sets the array of commands to execute on the XML RPC server.

    The commands to execute are arrays where the first element in the array is
    the command and the rest are arguments to that command.

    Args:
      commands_to_execute: An array of commands to execute.
    """
    validator = (
        lambda x: self._ValidateListContent(x, lambda y: isinstance(y, str)))
    if not self._ValidateListContent(commands_to_execute, validator):
      logging.error('Invalid commands list: %s', commands_to_execute)
      return
    self._commands_to_execute = commands_to_execute

  def SetRemoteRoot(self, remote_root):
    """Sets the remote root where to upload and execute on the XML RPC server.

    The remote root must be an absolute path. But we can't validate it using
    the Server Proxy, and we can't use os.path since we may be running on
    a non-Windows platform.

    Args:
      remote_root: The remote root to create (if inexistent) on the XML RPC
                   server.
    """
    if not isinstance(remote_root, str):
      logging.error('The remote root must be a string: %s', remote_root)
      return
    self._remote_root = remote_root

  def RemotePython26Path(self):
    """Returns the path of python2.6 on the remote machine.

    Currently only tries /python26, and it it's there, will return
    /python26/python, and if it's not there, simply returns python, hoping
    that the python executable on the path is python 2.6.

    TODO(user): Validate that the python executable on the path is indeed 2.6
    by executing python --version using start and capture output.

    Returns:
      A string containing the path to the python 2.6 executable.
    """
    if not self.EnsureValidServer():
      return False

    if self._xml_rpc_server.exists(r'/python26'):
      return r'/python26/python'
    else:
      return 'python'

  def _EnsureRemoteFolderCreated(self, remote_folder):
    try:
      if not self._xml_rpc_server.exists(remote_folder):
        logging.info('Making dirs to: %s', remote_folder)
        self._xml_rpc_server.makedirs(remote_folder)
    except xmlrpclib.Error, e:
      logging.exception('Can\'t create remote folder: %s\nException: %s',
                        remote_folder, e)
      return False
    return True

  def UploadFile(self, remote_path, file_data):
    """Uploads a single file to the remote server.

    Args:
      remote_path: The local file path on the remote server.
      file_data: The data to upload to the file.

    Returns:
      True on success, False otherwise.
    """
    last_sep = remote_path.replace('\\', '/').rfind('/')
    if last_sep is not -1:
      self._EnsureRemoteFolderCreated(remote_path[:last_sep])
    try:
      logging.info('Uploading data to: %s', remote_path)
      result_path = self._xml_rpc_server.upload(remote_path, file_data)
    except xmlrpclib.Error, e:
      logging.exception('Can\'t upload %s\nException: %s',
                        remote_path, e)
      return False

    def NormalizedPath(path):
      return os.path.normpath(path).replace('\\\\', '\\').replace('\\', '/')

    if NormalizedPath(result_path) != NormalizedPath(remote_path):
      logging.error('upload returned "%s" instead of "%s"', result_path,
                    remote_path)
      return False
    return True

  def UploadFiles(self):
    """Uploads all the local files and specified text to the remote server.

    Returns:
      False if something failed, True otherwise
    """
    if not self.EnsureValidServer():
      return False

    if not self._EnsureRemoteFolderCreated(self._remote_root):
      return False

    for text_to_upload in self._text_to_upload:
      destination_file = os.path.join(self._remote_root, text_to_upload[0])
      logging.info('Uploading "%s" to %s.', text_to_upload[1],
                   text_to_upload[0])
      if not self.UploadFile(destination_file,
                             xmlrpclib.Binary(text_to_upload[1])):
        return False

    for file_pair_to_upload in self._file_pairs_to_upload:
      logging.info('uploading: %s', file_pair_to_upload)
      file_content = self._ReadFromFile(os.path.abspath(file_pair_to_upload[0]))
      if file_content is None:
        return False
      destination_file = os.path.join(self._remote_root, file_pair_to_upload[1])
      if not self.UploadFile(destination_file, xmlrpclib.Binary(file_content)):
        return False
    return True

  def RunCommands(self, capture_output=False):
    """Runs all the requested commands on the remote server.

    Args:
      capture_output: Identifies whether we want to capture the output or not.

    Returns:
      If the RemoteTestRunner is not able to communicate with the RPC server,
      this function returns None.  Otherwise the return value is a list that
      contains as many items as there are items in the commands to execute list.
      The type of the items in the list depends on the capture_output
      arguments:

        - If capture_output evaluates to True, each item is a tuple where the
          first item of the tuple is the exit_code of the command and the second
          is the command's output as a string.
        - If capture_output evaluates to False, each item is an integer
          representing the process id of the command.  If the command could
          not be started, the item is -1.
    """
    if not self.EnsureValidServer():
      return None

    # If we want to capture the output, we must make sure that we are using the
    # latest version of the xmlrpc server. There used to be a version that would
    # deadlock when we try to capture and some images might still be using it.
    if capture_output:
      if not self._AutoUpdateServer():
        return None

    commands_results = []
    for command in self._commands_to_execute:
      if command:
        logging.info('Executing %s', command)
        try:
          # TODO(user): If capture_output is True, we must make sure that we
          # have a recent enough version TEHelper, because versions prior to
          # 4.2.12.1 have problems capturing the output.
          result = self._xml_rpc_server.start(
              command[0], command[1:], capture_output, capture_output)
        except xmlrpclib.Error, e:
          logging.exception('Can\'t execute command %s\nException: %s',
                            command, e)
          if capture_output:
            result = [1, str(e)]
          else:
            result = -1

        if capture_output:
          if len(result) < 2:
            result.append('No output.')
          if result[0] != 0:  # 0 is SUCCESS
            logging.error('Execution error code %d: %s', result[0],
                          '\n'.join(result[1:]))

        # If capture_output evaluate to False, we only got a pid and we return
        # an array of pids, otherwise we return the array of tuples as doc'd.
        commands_results.append(result)
    return commands_results


def main():
  """For when the script is used directly on the command line."""
  # Here so that it isn't imported for nothing if we are imported as a module.
  # pylint: disable-msg=C6204
  import optparse
  # pylint: enable-msg=C6204
  parser = optparse.OptionParser()
  parser.add_option('-s', '--server_url', dest='server_url',
                    help='The URL to the xml rpc server.')
  parser.add_option('-r', '--remote_root', dest='remote_root',
                    help='The optional remote folder root.\nDefaults to '
                    r'swarm_tests.')
  parser.add_option('-f', '--file_pairs_to_upload', dest='file_pairs_to_upload',
                    help='An optional ; separated list of relative paths '
                    'to files to upload. To use different paths for the local '
                    'and destination files, you must provide two paths, the '
                    'local absolute one and a relative to the remote root '
                    'destination path, both being separated by a comma, '
                    'e.g., <local1>,<dest1>;<same>;<local2>,<dest2>')
  parser.add_option('-t', '--text_to_upload', dest='text_to_upload',
                    help='Text to upload in a given file with the following '
                    'format: <file_name1>,<text1>;<file_name2>,<text2>...')
  parser.add_option('-c', '--commands', dest='commands',
                    help='The optional commands to execute on the server. '
                    'Command arguments are comma seperated and the '
                    'commands are separated by semicolons.')
  parser.add_option('-w', '--wait_and_capture', action='store_true',
                    dest='wait_and_capture',
                    help='Specify if you want to wait for the commands to '
                    'complete and capture their output. Defaults to no.')
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Set logging level to INFO. Optional. Defaults to '
                    'ERROR level.')

  (options, args) = parser.parse_args()
  if not options.server_url:
    parser.error('You must provide the URL to the xml rpc server.')
  if args:
    print 'Ignoring unknown args:', args

  if options.verbose:
    logging.getLogger().setLevel(logging.INFO)
  else:
    logging.getLogger().setLevel(logging.ERROR)

  runner = RemoteTestRunner(server_url=options.server_url)
  if options.remote_root:
    runner.SetRemoteRoot(options.remote_root)
  if options.file_pairs_to_upload:
    file_pairs_to_upload = []
    for file_to_upload in options.file_pairs_to_upload.split(';'):
      file_pair_to_upload = file_to_upload.split(',')
      if len(file_pair_to_upload) is 1:
        file_pair_to_upload.append(file_pair_to_upload[0])
      assert len(file_pair_to_upload) is 2
      file_pairs_to_upload.append(file_pair_to_upload)
    runner.SetFilePairsToUpload(file_pairs_to_upload)
  commands = []
  if options.commands:
    commands.extend([command.split(',')
                     for command in options.commands.split(';')])
    runner.SetCommandsToExecute(commands)

  text_to_upload = []
  if options.text_to_upload:
    text_to_upload.extend(
        [one_text.split(',') for one_text in options.text_to_upload.split(';')])
    runner.SetTextToUpload(text_to_upload)

  exit_code = 0
  if not runner.UploadFiles():
    exit_code = 1
  else:
    if not options.wait_and_capture:
      options.wait_and_capture = False
    results = runner.RunCommands(options.wait_and_capture)
    if not results:
      exit_code = 1
    else:
      assert len(results) == len(commands)
      for index in range(min([len(results), len(commands)])):
        print 'Command: %s' % ' '.join(commands[index])
        if options.wait_and_capture:
          if results[index][0]:
            exit_code = 1
          print 'Exit code: %d' % results[index][0]
          print '\n'.join(results[index][1])
        else:
          if results[index] > 0:
            print 'Started proces: %d' % results[index]
          else:
            print 'Failed to start!'
            exit_code = 1
  return exit_code


if __name__ == '__main__':
  sys.exit(main())
