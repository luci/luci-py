#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Test cases for the xmlrpc server code."""

import logging
import optparse
import os
import tempfile
import threading
import unittest
import xmlrpclib


from samples.server import xmlrpc_server


def RunXmlRpcServer(started_event, done_event):
  logging.debug('starting server')
  xmlrpc_server.XmlRpcServer().Start(_XmlRpcServerTestProgram.options.verbose,
                                     started_event, done_event)
  logging.debug('exited server')


class _XmlRpcServerTestRunner(unittest.TestCase):

  def setUp(self):
    logging.info('Launching server process')
    started_event = threading.Event()
    self._done_event = threading.Event()
    self._thread = threading.Thread(target=RunXmlRpcServer,
                                    args=(started_event, self._done_event))
    logging.debug('starting thread')
    self._thread.start()
    logging.debug('started thread')

    started_event.wait()
    logging.debug('Creating proxy client')
    self._server = xmlrpclib.ServerProxy('http://localhost:7399')

    self._tempfolder = tempfile.gettempdir()
    self._tempfile_name = os.path.join(self._tempfolder, 'bla')

    self._invalid_path = '::'

  def tearDown(self):
    logging.debug('starting shutdown')
    if self._server.Shutdown():
      self._done_event.wait()
      self._server = None
      if self._thread.isAlive():
        logging.debug('joining thread')
        self._thread.join()
        logging.debug('joined thread')
      self._thread = None
    else:
      logging.error('Failed to shutdown!!!')

  def testExists(self):
    logging.debug('Testing the exists method')
    if os.path.exists(self._tempfile_name):
      try:
        os.remove(self._tempfile_name)
      except:  # pylint: disable-msg=W0702
        os.rmdir(self._tempfile_name)
    self.assertFalse(self._server.exists(self._tempfile_name),
                     '%s should not exist' % self._tempfile_name)
    open(self._tempfile_name, 'w').close()
    self.assertTrue(self._server.exists(self._tempfile_name),
                    '%s should exist' % self._tempfile_name)
    os.remove(self._tempfile_name)

  def testMakedirs(self):
    logging.debug('Testing the makedirs method')
    self.assertRaises(xmlrpclib.Fault, self._server.makedirs,
                      self._invalid_path)

    self._server.makedirs(self._tempfile_name)
    self.assertTrue(self._server.exists(self._tempfile_name),
                    '%s should exist' % self._tempfile_name)
    os.rmdir(self._tempfile_name)

  def testUpload(self):
    logging.debug('Testing the upload method')
    self.assertRaises(xmlrpclib.Fault, self._server.upload, self._invalid_path,
                      'stuff')
    uploaded_content = 'I uploaded this content'
    result = self._server.upload(self._tempfile_name, uploaded_content)
    self.assertEqual(result, self._tempfile_name)
    tempfile_handle = open(self._tempfile_name, 'r')
    tempfile_content = tempfile_handle.read()
    tempfile_handle.close()

    self.assertEqual(uploaded_content, tempfile_content,
                     'should have uploaded %s' % uploaded_content)

  def testStart(self):
    logging.debug('Testing the start method')
    current_path = os.path.dirname(__file__)
    print_lines_script = os.path.join(current_path, 'print_lines.py')

    # Try without capture first
    exit_code = 42
    results = self._server.start('python', [print_lines_script, '-n 1', '-t 1',
                                            '-x %s' % exit_code], False, False)
    self.assertTrue(isinstance(results, int),
                    'When we don\'t capture, we only get the pid as an int.')
    self.assertTrue(results > 0, 'Should return a pid > 0')
    # There isn't much more we can do to validate the PID, unless you know
    # something I don't... At least not in a platform independent way... right?

    # Try capturing a lot of data, to make sure we don't hit the known dead
    # lock on Windows that we try to avoid in the code we are testing.
    results = self._server.start('python', [print_lines_script, '-n 1000',
                                            '-t 0.001'], True, True)
    self.assertTrue(isinstance(results, list),
                    'When we capture, we get both the exit code and stdout.')
    logging.debug(results)
    self.assertEqual(len(results), 2)
    self.assertEqual(0, results[0], 'Should return exit code 0, got %s' %
                     results[0])
    self.assertTrue('line number 0' in results[1])
    self.assertTrue('line number 999' in results[1])

  def testAutoupdateAndSid(self):
    # test autoupdate
    logging.debug('Testing the autoupdate method')
    self.assertFalse(self._server.autoupdate(), 'Wrong autoupdate value')

    # test sid
    logging.debug('Testing the sid method')
    sid = self._server.sid()
    self.assertEqual(1, int(sid), 'Wrong sid %d != 1' % sid)


class _XmlRpcServerTestProgram(unittest.TestProgram):
  options = None
  _DESCRIPTION = ('This script runs tests for the xmlrpc server.')

  def parseArgs(self, argv):
    parser = optparse.OptionParser(
        usage='%prog [options] [filename]',
        description=_XmlRpcServerTestProgram._DESCRIPTION)
    parser.add_option('-v', '--verbose', action='store_true',
                      help='Set logging level to INFO. Optional. Defaults to '
                      'ERROR level.')
    parser.add_option('-o', '--xmlrpc_server_start_timeout',
                      dest='xmlrpc_server_start_timeout', type=int,
                      help='How long should we wait (in seconds) for the '
                      'XmlRpc server to start? Defaults to 60 seconds.')
    parser.set_default('xmlrpc_server_start_timeout', 60)

    (_XmlRpcServerTestProgram.options, other_args) = parser.parse_args(argv[1:])
    if _XmlRpcServerTestProgram.options.verbose:
      logging.getLogger().setLevel(logging.DEBUG)
    else:
      logging.getLogger().setLevel(logging.ERROR)
    super(_XmlRpcServerTestProgram, self).parseArgs(other_args)


if __name__ == '__main__':
  _XmlRpcServerTestProgram()
