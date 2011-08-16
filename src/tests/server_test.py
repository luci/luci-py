#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Drives test on the Swarm server with a test machine provider."""




import cookielib
import httplib
import json
import logging
import optparse
import os
import subprocess
import time
import unittest
import urllib
import urllib2
import urlparse
import xmlrpclib


# TODO(user): Find a way to avoid spawning other processes so that we can
# get code coverage, which doesn't work for code in other processes in Python.
class _ProcessWrapper(object):
  """This class controls the life span of a child process.

  Attributes:
    command_line: An array with the command to execute followed by its args.
  """

  def __init__(self, command_line):
    self.wrapped_process = subprocess.Popen(command_line)

  def __del__(self):
    if hasattr(self, 'wrapped_process') and self.wrapped_process.poll() is None:
      self.wrapped_process.terminate()


class _SwarmTestCase(unittest.TestCase):
  """Test case class for Swarm integration tests."""

  def GetAdminURl(self, url):
    """"Converts the given url to an admin privilege one.

    Args:
      url: The url to be converted.
    Returns:
      The converted url.
    """
    return urlparse.urljoin(self._swarm_server_url, '_ah/login?'
                            'email=john@doe.com&admin=True&action=Login&'
                            'continue=' + urllib2.quote(url))

  def setUp(self):
    self._xmlrpc_server_process = None
    self._swarm_server_process = None

    # TODO(user): find a better way to choose the port.
    self._swarm_server_url = 'http://localhost:8181'

    _SwarmTestProgram.options.appengine_cmds.extend(
        ['-c', '-p 8181', '--skip_sdk_update_check',
         _SwarmTestProgram.options.swarm_path])

    xmlrpc_process_cmds = ['python', os.path.join(os.path.dirname(__file__),
                                                  'xmlrpc_server.py')]
    if _SwarmTestProgram.options.verbose:
      xmlrpc_process_cmds.append('-v')
    self._xmlrpc_server_process = _ProcessWrapper(xmlrpc_process_cmds)
    logging.info('Started XmlRpc Process with pid: %s',
                 self._xmlrpc_server_process.wrapped_process.pid)
    # TODO(user): find a better way to choose the port.
    self._xmlrpc_server = xmlrpclib.ServerProxy('http://localhost:7399')

    self._swarm_server_process = _ProcessWrapper(
        _SwarmTestProgram.options.appengine_cmds)
    logging.info('Started Swarm server Process with pid: %s',
                 self._swarm_server_process.wrapped_process.pid)

    # Wait for the Swarm server to be ready
    ready = False
    time_out = _SwarmTestProgram.options.swarm_server_start_timeout
    started = time.time()
    while not ready and time_out > time.time() - started:
      try:
        urllib2.urlopen(self._swarm_server_url)
        ready = True
      except urllib2.URLError:
        time.sleep(1)
    self.assertTrue(ready, 'The swarm server could not be started')

  def tearDown(self):
    if self._xmlrpc_server.Shutdown():
      self._xmlrpc_server_process.wrapped_process.wait()
    try:
      logging.info('Quitting Swarm server')
      cj = cookielib.CookieJar()
      opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
      opener.open(self.GetAdminURl(urlparse.urljoin(self._swarm_server_url,
                                                    'tasks/quitquitquit')))
      self._swarm_server_process.wrapped_process.wait()
    except httplib.BadStatusLine:
      # We expect this to throw since the quitquitquit request caused the
      # server to terminate and thus it won't be able to complete the request.
      pass

  def testHandlingSwarmFiles(self):
    """This test sends a series of Swarm files to the server."""
    swarm_files = []
    logging.info('Walking %s, looking for Swarm files.',
                 os.path.join(os.environ['TEST_SRCDIR'],
                              _SwarmTestProgram.options.tests_path))
    for dirpath, _, filenames in os.walk(os.path.join(
        os.environ['TEST_SRCDIR'], _SwarmTestProgram.options.tests_path)):
      for filename in filenames:
        if os.path.splitext(filename)[1].lower() == '.swarm':
          swarm_files.append(os.path.join(dirpath, filename))

    logging.info('Will send these files to Swarm server: %s', swarm_files)

    swarm_server_test_url = urlparse.urljoin(self._swarm_server_url, 'test')
    running_test_keys = []
    for swarm_file in swarm_files:
      logging.info('Sending content of %s to Swarm server.', swarm_file)
      # Build the URL for sending the request.
      test_request = open(swarm_file)
      output = None
      try:
        output = urllib2.urlopen(swarm_server_test_url,
                                 data=test_request.read()).read()
      except urllib2.URLError, ex:
        self.fail('Error: %s' % str(ex))

      # Check that we can read the output as a JSON string
      try:
        test_keys = json.loads(output)
      except (ValueError, TypeError), e:
        self.fail('Swarm Request failed: %s' % output)

      logging.info(test_keys)

      try:
        logging.info('Polling Swarm server')
        cj = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        opener.open(self.GetAdminURl(urlparse.urljoin(self._swarm_server_url,
                                                      'tasks/poll')))
        # We need to poll twice, once for assigning the machine and a second
        # one to start the test. TODO(user): This should be fixed in the server.
        # No need for admin URL the second time...
        opener.open(urlparse.urljoin(self._swarm_server_url, 'tasks/poll'))
      except urllib2.URLError, ex:
        self.fail('Error: %s' % str(ex))

      # Make sure the machines got the Swarm file correctly, since there once
      # was a bug that broke the string conversion of the test request by
      # creating and invalid test run because AppEngine's db.IntegerProper() is
      # actually a long and not an int, and the test request message was
      # validating for int's and thus the test run wasn't created and the
      # uploaded file was empty...
      test_run_content = self._xmlrpc_server.UploadedContent(
          'c:\swarm_tests/test_run.swarm')  # TODO(user): Fix path thingy...
      self.assertNotEqual(test_run_content, '')

      for test_key in test_keys['test_keys']:
        running_test_keys.append(test_key)
        if _SwarmTestProgram.options.verbose:
          logging.info('Config: %s, index: %s/%s, test key: %s',
                       test_key['config_name'],
                       int(test_key['instance_index']) + 1,
                       test_key['num_instances'],
                       test_key['test_key'])

    # TODO(user): This code below and a big chunk of the parts above were stolen
    # from the post_test.py script under the tools folder. We should find a
    # way to extract and reuse it somehow.
    test_result_output = ''
    # TODO(user): Maybe collect all failures so that we can enumerate them at
    # the end as the local test runner and gtest does.
    test_all_succeeded = True
    for running_test_key in running_test_keys:
      try:
        result_url = urlparse.urljoin(
            self._swarm_server_url, 'result?k=' + running_test_key['test_key'])
        logging.info('Opening URL %s', result_url)
        urllib2.urlopen(result_url, urllib.urlencode((('s', True),
                                                      ('r', '0 FAILED TESTS'))))
        key_url = urlparse.urljoin(
            self._swarm_server_url,
            'get_result?r=' + running_test_key['test_key'])
        logging.info('Opening URL %s', key_url)
        output = urllib2.urlopen(key_url).read()
        assert output
        if _SwarmTestProgram.options.verbose:
          test_result_output = (
              '%s\n=======================\nConfig: %s\n%s' %
              (test_result_output, running_test_key['config_name'], output))
        if test_all_succeeded and '0 FAILED TESTS' not in output:
          test_all_succeeded = False
        if _SwarmTestProgram.options.verbose:
          logging.info('Test done for %s', running_test_key['config_name'])
      except urllib2.HTTPError, e:
        self.fail('Calling %s threw %s' % (result_url, e))
    if _SwarmTestProgram.options.verbose:
      logging.info(test_result_output)
      logging.info('=======================')
    if test_all_succeeded:
      logging.info('All test succeeded')
    else:
      self.fail('At least one test failed')


class _SwarmTestProgram(unittest.TestProgram):
  """The Swarm specific test program so that we can have specialised options."""

  # Command line options
  options = None

  # TODO(user): Find a way to also display base class command line args for -h.
  _DESCRIPTION = ('This script starts a Swarm server with a test machine '
                  'provider and runs a server mocking provided machines.')

  def parseArgs(self, argv):
    """Overloaded from base class so we can add our own options."""
    parser = optparse.OptionParser(usage='%prog [options] [filename]',
                                   description=_SwarmTestProgram._DESCRIPTION)
    parser.add_option('-a', '--appengine', dest='appengine_cmds',
                      action='append', help='The command(s) to start the '
                      'AppEngine launcher. The -c, -p, and '
                      '--skip_sdk_update_check arguments will '
                      'be added to the command(s) you specify.')
    parser.add_option('-s', '--swarm', dest='swarm_path',
                      help='The root path of the Swarm server code.'
                      'Defaults to the parent folder of where this script is.')
    parser.add_option('-t', '--tests', dest='tests_path',
                      help='The path where the test files can be found.'
                      'Defaults to the ./test_files folder.')
    parser.add_option('-o', '--swarm_server_start_timeout',
                      dest='swarm_server_start_timeout', type=int,
                      help='How long should we wait (in seconds) for the '
                      'Swarm server to start? Defaults to 90 seconds.')
    parser.add_option('-v', '--verbose', action='store_true',
                      help='Set logging level to INFO. Optional. Defaults to '
                      'ERROR level.')
    parser.set_default('swarm_path', '..')
    parser.set_default('swarm_server_start_timeout', 90)
    parser.set_default('tests_path', 'test_files')

    (_SwarmTestProgram.options, other_args) = parser.parse_args(args=argv[1:])
    if not _SwarmTestProgram.options.appengine_cmds:
      parser.error('You must specify the AppEngine command(s) to start the '
                   'AppEngine launcher.')

    if _SwarmTestProgram.options.verbose:
      logging.getLogger().setLevel(logging.INFO)
    else:
      logging.getLogger().setLevel(logging.ERROR)

    test_srcdir = os.environ['TEST_SRCDIR']
    if test_srcdir:
      _SwarmTestProgram.options.appengine_cmds[0] = os.path.join(
          test_srcdir, _SwarmTestProgram.options.appengine_cmds[0])
      _SwarmTestProgram.options.swarm_path = os.path.join(
          test_srcdir, _SwarmTestProgram.options.swarm_path)
    super(_SwarmTestProgram, self).parseArgs(other_args)


if __name__ == '__main__':
  if 'TEST_SRCDIR' not in os.environ:
    os.environ['TEST_SRCDIR'] = ''
  _SwarmTestProgram()
