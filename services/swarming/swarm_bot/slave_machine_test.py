#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import json
import logging
import os
import socket
import subprocess
import sys
import time
import unittest

# Import them first before manipulating sys.path to ensure they can load fine.
import os_utilities
import slave_machine
import url_helper
import zipped_archive

from common import rpc
from common import swarm_constants

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from depot_tools import auto_stub
from server import bot_archive
from third_party.mox import mox

# pylint: disable=W0212


def _CreateResponse(come_back=None, try_count=1, commands=None, result_url=None,
                    extra_arg=None):
  response = {}

  if come_back is not None:
    response['come_back'] = come_back
  if try_count is not None:
    response['try_count'] = try_count
  if commands is not None:
    response['commands'] = commands
  if extra_arg is not None:
    response['extra_arg'] = extra_arg
  if result_url is not None:
    response['result_url'] = result_url

  return json.dumps(response)


def _SetPollJobAndPostFailExpectations(response, result_url, result_string,
                                       result_code=-1, bad_url=False):
  """Setup mox expectations for slave behavior under errors.

  A url_helper.UrlOpen to request a job, and one to tell the server something
  went wrong with the response it received.
  """
  # Original register machine request.
  UrlOpenExpectations(response, mox.IgnoreArg(), mox.IgnoreArg())
  data = {'x': str(result_code), 's': False}
  files = [(swarm_constants.RESULT_STRING_KEY,
            swarm_constants.RESULT_STRING_KEY,
            result_string)]

  url_helper.UrlOpen(result_url, data=data, files=files,
                     max_tries=mox.IgnoreArg(), method='POSTFORM').AndReturn(
                         None if bad_url else 'Success')


def UrlOpenExpectations(response, url, data):
  url_helper.UrlOpen(
      url, data=data, max_tries=mox.IgnoreArg()).AndReturn(response)


class TestSlaveMachine(auto_stub.TestCase):
  """Test class for the SlaveMachine class."""

  def setUp(self):
    super(TestSlaveMachine, self).setUp()
    self._mox = mox.Mox()
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    self._mox.StubOutWithMock(time, 'sleep')
    self._mox.StubOutWithMock(subprocess, 'call')
    self._mox.StubOutWithMock(subprocess, 'check_call')
    self._mox.StubOutWithMock(os_utilities, 'restart')
    self._mox.StubOutWithMock(slave_machine, '_MakeDirectory')
    self._mox.StubOutWithMock(slave_machine, '_StoreFile')
    self.mock(logging, 'warning', lambda *_: None)
    self.mock(logging, 'error', lambda *_: None)
    self.mock(logging, 'exception', lambda *_: None)

    with open(os.path.join(BASE_DIR, 'start_slave.py'), 'rb') as f:
      start_slave_contents = f.read()
    additionals = {'start_slave.py': start_slave_contents}
    self.version = bot_archive.get_swarming_bot_version(
        BASE_DIR, 'http://localhost', additionals)
    self.attributes = {
      'dimensions': {'os': ['Linux']},
      'version': self.version,
    }
    # slave_machine.generate_version() is tested via the smoke test.
    self.mock(zipped_archive, 'generate_version', lambda: self.version)

  def tearDown(self):
    self._mox.UnsetStubs()
    super(TestSlaveMachine, self).tearDown()

  def _CreateValidAttribs(self, try_count=0):
    attributes = self.attributes.copy()
    attributes['id'] = socket.getfqdn().lower()
    attributes['try_count'] = try_count
    return {'attributes': json.dumps(attributes)}

  # Mock slave_machine._PostFailedExecuteResults.
  def _MockPostFailedExecuteResults(self, slave, result_string):
    self._mox.StubOutWithMock(slave, '_PostFailedExecuteResults')
    slave._PostFailedExecuteResults(result_string)

  @staticmethod
  def _MockMakeDirectory(path, exception_message=None):
    if exception_message:
      slave_machine._MakeDirectory(path).AndRaise(os.error(exception_message))
    else:
      slave_machine._MakeDirectory(path)

  @staticmethod
  def _MockStoreFile(path, name, contents, exception_message=None):
    if exception_message:
      slave_machine._StoreFile(
          path, name, contents).AndRaise(IOError(exception_message))
    else:
      slave_machine._StoreFile(path, name, contents)

  @staticmethod
  def _MockSubprocessCheckCall(args, exit_code=0):
    if exit_code:
      subprocess.check_call(args, cwd=BASE_DIR).AndRaise(
          subprocess.CalledProcessError(exit_code, args))
    else:
      subprocess.check_call(args, cwd=BASE_DIR)

  # Test with an invalid URL and try until it raises an exception.
  def testInvalidURLWithException(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    max_url_tries = 5

    url_helper.UrlOpen(mox.IgnoreArg(), data=mox.IgnoreArg(),
                       max_tries=max_url_tries
                      ).AndReturn(None)

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {}, max_url_tries)

    expected_exception_str = (r'Error when connecting to Swarm server, '
                              'http://localhost/poll_for_test, failed to '
                              'connect after 5 attempts.')
    with self.assertRaisesRegexp(
        slave_machine.SlaveError, expected_exception_str):
      slave.Start(iterations=-1)

    self._mox.VerifyAll()

  def testAttributesFormatBadString(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    data = self._CreateValidAttribs()

    UrlOpenExpectations(
        'blah blah blah', 'http://localhost/poll_for_test', data)
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', self.attributes)
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test how values specified in the constructor are reflected in the request.
  def testConstructor(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    data = self._CreateValidAttribs()

    UrlOpenExpectations(
        'blah blah blah', 'http://localhost/poll_for_test', data)
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', self.attributes)
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with missing mandatory fields in response: come_back.
  def testMissingAll(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(_CreateResponse(), mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with missing mandatory fields in response: try_count.
  def testMissingTryCount(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(
        _CreateResponse(come_back=2, try_count=None), mox.IgnoreArg(),
        mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with both commands and come_back missing.
  def testMissingComeback(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(_CreateResponse(), mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with an extra argument in response. It should accept this
  # without an error.
  def testExtraResponseArgs(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    come_back = 11.0
    UrlOpenExpectations(
        _CreateResponse(come_back=come_back, extra_arg='INVALID'),
        mox.IgnoreArg(), mox.IgnoreArg())
    time.sleep(come_back)
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with missing mandatory fields in response: result_url.
  def testMissingResultURL(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(
        _CreateResponse(commands=[rpc.BuildRPC('a', None)]),
        mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with bad type for result_url.
  def testBadResultURLType(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(
        _CreateResponse(commands=[rpc.BuildRPC('a', None)],
                        result_url=['localhost']),
        mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with bad type for commands.
  def testBadCommands(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    _SetPollJobAndPostFailExpectations(
        _CreateResponse(commands='do this', result_url='localhost'),
        'localhost',
        '[u\'Failed to validate commands with value "do this": '
        "Invalid type: <type \\'unicode\\'> instead of <type \\'list\\'>\']")
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with wrong RPC format for commands.
  def testBadCommandsParseRPCFormat(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    _SetPollJobAndPostFailExpectations(
        _CreateResponse(commands=['do this'], result_url='localhost'),
        'localhost',
        '[\'Failed to validate commands with value "[u\\\'do this\\\']": '
        'Error when parsing RPC: Invalid RPC container\']')

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with wrong RPC function name.
  def testBadCommandsParseRPCFunctionName(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    commands = [rpc.BuildRPC('WrongFunc', None)]
    _SetPollJobAndPostFailExpectations(
        _CreateResponse(commands=commands, result_url='localhost'),
        'localhost', 'Unsupported RPC function name: WrongFunc')
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with both fields in response: come_back and commands.
  def testInvalidBothCommandsAndComeback(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(
        _CreateResponse(come_back=3, commands='do this'),
        mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test invalid come_back type.
  def testInvalidComebackType(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(
        _CreateResponse(come_back='3'), mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test invalid come_back value.
  def testInvalidComebackValue(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(
        _CreateResponse(come_back=-3.0), mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test invalid try_count type.
  def testInvalidTryCountType(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(
        _CreateResponse(come_back=3.0, try_count='1'),
        mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test invalid try_count value.
  def testInvalidTryCountValue(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    UrlOpenExpectations(
        _CreateResponse(come_back=3.0, try_count=-1),
        mox.IgnoreArg(), mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test 2 iterations of requests with nothing to do + 1 bad response.
  def testComeBack(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    come_back = 1.0
    try_count = 0
    message = 'blah blah blah'
    response = [_CreateResponse(come_back=come_back, try_count=try_count),
                _CreateResponse(come_back=come_back, try_count=try_count),
                _CreateResponse(come_back=come_back, try_count=try_count),
                message]

    for i in range(len(response)):
      if i < len(response) - 1:
        url_helper.UrlOpen(
            mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
            ).AndReturn(response[i])
        time.sleep(come_back)
      else:
        UrlOpenExpectations(response[i], mox.IgnoreArg(), mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=len(response))

    self._mox.VerifyAll()

  def testSetStoreFilesRPCValidate(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    function_name = 'StoreFiles'
    invalid_args = [None, u'some arg', [u'another arg'], [[123, 1]],
                    [('113', 113, '311')]]

    expected_error = [
        ('Invalid %s arg type: %s (expected list of str or'
         ' unicode tuples)'% (function_name, str(type(invalid_args[0])))),
        ('Invalid %s arg type: %s (expected list of str or'
         ' unicode tuples)'% (function_name, str(type(invalid_args[1])))),
        ('Invalid element type in %s args: %s (expected str or'
         ' unicode tuple)'% (function_name, str(type(invalid_args[2][0])))),
        ('Invalid element len (%d != 3) in %s args: %s'%
         (len(invalid_args[3][0]), function_name, str(invalid_args[3][0]))),
        ('Invalid tuple element type: %s (expected str or unicode)'%
         str(type(invalid_args[4][0][1])))]

    self.assertEqual(len(invalid_args), len(expected_error))

    for i in range(0, len(invalid_args)):
      commands = [rpc.BuildRPC(function_name, invalid_args[i])]
      response = _CreateResponse(commands=commands, result_url='localhost')

      _SetPollJobAndPostFailExpectations(
          response, 'localhost', expected_error[i])

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=len(invalid_args))

    self._mox.VerifyAll()

  def testSetStoreFilesRPCExecuteMakeDirException(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    slave = slave_machine.SlaveMachine('http://localhost', {})
    function_name = 'StoreFiles'
    args = [(u'file path', u'file name', u'file contents')]

    commands = [rpc.BuildRPC(function_name, args)]
    response = _CreateResponse(commands=commands, result_url='localhost')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    exception_message = 'makedirs exception'
    self._MockMakeDirectory(args[0][0], exception_message=exception_message)
    self._MockPostFailedExecuteResults(
        slave, 'MakeDirectory exception: %s' % exception_message)

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testSetStoreFilesRPCExecuteStoreFileException(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    slave = slave_machine.SlaveMachine('http://localhost', {})
    function_name = 'StoreFiles'
    args = [(u'file path', u'file name', u'file contents')]

    commands = [rpc.BuildRPC(function_name, args)]
    response = _CreateResponse(commands=commands, result_url='localhost')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    exception_message = 'storefile exception'
    self._MockMakeDirectory(args[0][0])
    self._MockStoreFile(
        args[0][0], args[0][1], args[0][2], exception_message=exception_message)
    self._MockPostFailedExecuteResults(
        slave, 'StoreFile exception: %s' % exception_message)

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testSetStoreFilesRPCExecuteNoException(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    slave = slave_machine.SlaveMachine('http://localhost', {})
    function_name = 'StoreFiles'
    args = [(u'file path', u'file name', u'file contents')]

    commands = [rpc.BuildRPC(function_name, args)]
    response = _CreateResponse(commands=commands, result_url='localhost')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    self._MockMakeDirectory(args[0][0])
    self._MockStoreFile(args[0][0], args[0][1], args[0][2])

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testRunManifestRPCValidate(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    commands = [rpc.BuildRPC('RunManifest', None)]
    _SetPollJobAndPostFailExpectations(
        _CreateResponse(commands=commands, result_url='localhost'),
        'localhost',
        'Invalid RunManifest arg: None (expected str)')
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testRunManifestRPCExecuteSubprocessException(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    function_name = 'RunManifest'
    args = u'language'

    slave = slave_machine.SlaveMachine('http://localhost', {})
    commands = [rpc.BuildRPC(function_name, args)]
    response = _CreateResponse(commands=commands, result_url='localhost')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    # Mock subprocess to raise exception.
    full_command = [
      sys.executable,
      os.path.abspath(sys.argv[0]),
      'local_test_runner',
      '-f', args,
    ]
    self._MockSubprocessCheckCall(full_command, exit_code=-1)

    os_utilities.restart().AndReturn(None)

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testRunManifestRPCExecuteNoException(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    function_name = 'RunManifest'
    args = u'language'

    slave = slave_machine.SlaveMachine('http://localhost', {})
    commands = [rpc.BuildRPC(function_name, args)]
    response = _CreateResponse(commands=commands, result_url='localhost')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    # Mock subprocess to raise exception.
    expected = [
      sys.executable,
      os.path.abspath(sys.argv[0]),
      'local_test_runner',
      '-f', args,
    ]
    self._MockSubprocessCheckCall(expected)

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testRunManifestRPCRestartFails(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    function_name = 'RunManifest'
    args = 'language'

    slave = slave_machine.SlaveMachine('http://localhost', {})
    commands = [rpc.BuildRPC(function_name, args)]
    response = _CreateResponse(commands=commands, result_url='localhost')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    # Mock subprocess to raise exception and signal a restart.
    expected = [
      sys.executable,
      os.path.abspath(sys.argv[0]),
      'local_test_runner',
      '-f', args,
    ]
    self._MockSubprocessCheckCall(
        expected,
        exit_code=swarm_constants.RESTART_EXIT_CODE)

    # Mock out the the restart attempt to raise an exception, otherwise it would
    # start an infinite loop.
    class Foo(Exception):
      pass
    os_utilities.restart().AndRaise(Foo)

    self._mox.ReplayAll()

    with self.assertRaises(Foo):
      slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test to make sure the result url of the slave is correctly reset each time a
  # job is requested.
  def testResultURLReset(self):
    # Initial server ping.
    url_helper.UrlOpen(mox.IgnoreArg(), method='GET').AndReturn('')

    commands = [rpc.BuildRPC('WrongFunc', None)]
    response = [_CreateResponse(commands=commands, result_url='here1.com'),
                _CreateResponse(commands=commands),
                _CreateResponse(commands=commands, result_url='here2.com')]

    _SetPollJobAndPostFailExpectations(
        response[0], 'here1.com', 'Unsupported RPC function name: WrongFunc')
    # This response has no result_url. So it shouldn't use the result_url given
    # in the first response.
    UrlOpenExpectations(response[1], mox.IgnoreArg(), mox.IgnoreArg())
    _SetPollJobAndPostFailExpectations(
        response[2], 'here2.com', 'Unsupported RPC function name: WrongFunc')

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine('http://localhost', {})
    slave.Start(iterations=3)

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  #logging.disable(logging.FATAL)
  unittest.main()
