#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Test cases for the xmlrpc server code."""




import logging
import optparse
import os
import socket
import threading
import time
import unittest
import xmlrpclib


import xmlrpc_server


def RunXmlRpcServer():
  logging.debug('starting server')
  xmlrpc_server.XmlRpcServer().Start(_XmlRpcServerTestProgram.options.verbose)
  logging.debug('exited server')


class _XmlRpcServerTestRunner(unittest.TestCase):

  def setUp(self):
    logging.info('Launching server process')
    self._thread = threading.Thread(target=RunXmlRpcServer)
    logging.debug('starting thread')
    self._thread.start()
    logging.debug('started thread')

    logging.debug('Creating proxy client')
    self._server = xmlrpclib.ServerProxy('http://localhost:7399')

    # Wait for server to be ready
    ready = False
    time_out = _XmlRpcServerTestProgram.options.xmlrpc_server_start_timeout
    started = time.time()
    while not ready and time_out > time.time() - started:
      try:
        logging.debug('calling sid')
        self._server.sid()
        logging.debug('called sid')
        ready = True
      except socket.error:
        time.sleep(1)
    if not ready:
      logging.error('The XmlRpc server could not be started')
      raise Exception('XmlRpc server not ready')

  def tearDown(self):
    logging.debug('starting shutdown')
    if self._server.Shutdown():
      logging.debug('joining thread')
      self._thread.join()
      logging.debug('joined thread')

  def testXmlRpcServerMethods(self):
    # Test exists
    logging.debug('Testing the exists method')
    self.assertFalse(self._server.exists('bla'), 'bla should not exist')
    self._server.AddExistingPath('bla')
    self.assertTrue(self._server.exists('bla'), 'bla should exist')
    self._server.RemoveExistingPath('bla')
    self.assertFalse(self._server.exists('bla'), 'bla should not exist')

    # Test makedirs
    logging.debug('Testing the makedirs method')
    self._server.FailMakedirs(True)
    self.assertRaises(xmlrpclib.Fault, self._server.makedirs, 'blabla')

    self._server.FailMakedirs(False)
    self._server.makedirs('blabla')
    self.assertTrue(self._server.exists('blabla'), 'blabla should exist')
    self._server.RemoveExistingPath('blabla')
    self.assertFalse(self._server.exists('blabla'), 'blabla should not exist')

    # test upload
    logging.debug('Testing the upload method')
    self._server.SetFailUpload(True)
    self.assertRaises(xmlrpclib.Fault, self._server.upload, 'blabla', 'blublu')
    self._server.SetFailUpload(False)
    self._server.SetUploadReturnEmpty(True)
    result = self._server.upload('blabla', 'blublu')
    self.assertFalse(result, 'upload should have returned empty')

    self._server.SetUploadReturnEmpty(False)
    result = self._server.upload('blabla', 'blublu')
    self.assertEqual('blabla', result, 'upload should have returned blabla')
    result = self._server.UploadedContent('blabla')
    self.assertEqual('blublu', result, 'should have uploaded blublu content')

    # test start
    logging.debug('Testing the start method')
    results = self._server.start('cmd1', ['arg11', 'arg12'], True, True)
    self.assertEqual(42, results, 'Should return proper results')
    self.assertEqual([['cmd1', ['arg11', 'arg12'], True]],
                     self._server.StartedCommands(),
                     'Should return started command')
    self._server.AddCommandResults((0, 'result'))
    results = self._server.start('cmd2', ['arg21', 'arg22'], False, False)
    self.assertEqual([0, 'result'], results, 'Should return proper results')
    started_commands = self._server.StartedCommands()
    self.assertEqual(2, len(started_commands))
    self.assertEqual(['cmd2', ['arg21', 'arg22'], False], started_commands[1],
                     'Should return started command')
    results = self._server.start('cmd3', ['arg31', 'arg32'], False, False)
    self.assertEqual(42, results, 'Should return proper results')
    started_commands = self._server.StartedCommands()
    self.assertEqual(3, len(started_commands))
    self.assertEqual(['cmd3', ['arg31', 'arg32'], False], started_commands[2],
                     'Should return started command')

    # test sid
    logging.debug('Testing the sid method')
    self._server.SetSid('ble')
    self.assertEqual('ble', self._server.sid(), 'Wrong sid')

    # test autoupdate
    logging.debug('Testing the autoupdate method')
    self._server.SetAutoupdate('bli')
    self.assertEqual(self._server.autoupdate(), 'bli', 'Wrong sid')


class _XmlRpcServerTestProgram(unittest.TestProgram):
  options = None
  _DESCRIPTION = ('This script starts tests the xmlrpc server used to test '
                  'Swarm.')

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
      logging.getLogger().setLevel(logging.INFO)
    else:
      logging.getLogger().setLevel(logging.ERROR)
    super(_XmlRpcServerTestProgram, self).parseArgs(other_args)


if __name__ == '__main__':
  if 'TEST_SRCDIR' not in os.environ:
    os.environ['TEST_SRCDIR'] = ''
  _XmlRpcServerTestProgram()
