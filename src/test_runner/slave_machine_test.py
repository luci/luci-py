#!/usr/bin/python2.7
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Unittest to exercise the code in slave_machine.py."""



import logging
import os
import subprocess
import sys
import time
import unittest


# pylint: disable-msg=C6204
try:
  import simplejson as json
except ImportError:
  import json

from common import url_helper
from third_party.mox import mox
from test_runner import slave_machine
# pylint: enable-msg=C6204

MACHINE_ID_1 = '12345678-12345678-12345678-12345678'
MACHINE_ID_2 = '87654321-87654321-87654321-87654321'
VALID_ATTRIBUTES = {'dimensions': {'os': ['Linux']}}


class TestSlaveMachine(unittest.TestCase):
  """Test class for the SlaveMachine class."""

  def setUp(self):
    self._mox = mox.Mox()
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    self._mox.StubOutWithMock(time, 'sleep')
    self._mox.StubOutWithMock(logging, 'error')

  def tearDown(self):
    self._mox.UnsetStubs()
    self._mox.ResetAll()

  def _CreateValidAttribs(self, machine_id=None, try_count=0):
    attributes = VALID_ATTRIBUTES.copy()
    attributes['id'] = machine_id
    attributes['try_count'] = try_count
    return {'attributes': json.dumps(attributes)}

  def _CreateResponse(self, machine_id=MACHINE_ID_1, come_back=None,
                      try_count=1, commands=None, result_url=None,
                      extra_arg=None):
    response = {}

    if machine_id is not None:
      response['id'] = machine_id
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

  # Setup mox expectations for slave behavior under errors: a
  # url_helper.UrlOpen to request a job, and one to tell the server something
  # went wrong with the response it received.
  def _SetPollJobAndPostFailExpectations(self, response,
                                         result_url, result_string,
                                         result_code=-1, bad_url=False):
    # Original register machine request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    slave_machine.logging.error(
        'Error [code: %d]: %s', result_code, result_string)

    data = {'x': str(result_code),
            's': False,
            'r': result_string}

    url_helper.UrlOpen(result_url, data, max_tries=mox.IgnoreArg()
                      ).AndReturn(None if bad_url else 'Success')

  # Setup mox expectations for slave behavior under errors: a
  # url_helper.UrlOpen to request a job, and 2 calls to logging.error with
  # proper error message. This function is called instead of
  # _SetPollJobAndPostFailExpectations when the slave has no where to
  # send its error message.
  def _SetPollJobAndLogFailExpectations(self, response,
                                        result_string,
                                        url, data):
    # Original register machine request.
    url_helper.UrlOpen(
        url, data=data, max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    # Logging due to lack of result url.
    slave_machine.logging.error('Error [code: %d]: %s', -1, result_string)
    slave_machine.logging.error('No URL to send results to!')

  # Mock slave_machine._PostFailedExecuteResults.
  def _MockPostFailedExecuteResults(self, slave, result_string):
    self._mox.StubOutWithMock(slave, '_PostFailedExecuteResults')

    slave._PostFailedExecuteResults(result_string)

  # Mocks slave_machine._MakeDirectory to either throw exception or not.
  def _MockMakeDirectory(self, slave, path, exception=False,
                         exception_message='Some error message'):
    self._mox.StubOutWithMock(slave, '_MakeDirectory')

    if exception:
      slave._MakeDirectory(path).AndRaise(os.error(exception_message))
    else:
      slave._MakeDirectory(path)

  # Mocks slave_machine._StoreFile to either throw exception or not.
  def _MockStoreFile(self, slave, path, name, contents, exception=False,
                     exception_message='Some error message'):
    self._mox.StubOutWithMock(slave, '_StoreFile')

    if exception:
      slave._StoreFile(
          path, name, contents).AndRaise(IOError(exception_message))
    else:
      slave._StoreFile(path, name, contents)

  # Mocks subprocess.check_call and raises exception if specified.
  def _MockSubprocessCheckCall(self, commands, exception=False):
    self._mox.StubOutWithMock(subprocess, 'check_call')

    if exception:
      subprocess.check_call(
          commands).AndRaise(subprocess.CalledProcessError(-1, commands))
    else:
      subprocess.check_call(commands)

  # Test with an invalid URL and try until it raises an exception.
  def testInvalidURLWithException(self):
    max_url_tries = 5

    url_helper.UrlOpen(mox.IgnoreArg(), data=mox.IgnoreArg(),
                       max_tries=max_url_tries
                      ).AndReturn(None)

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine(max_url_tries=max_url_tries)

    expected_exception_str = (r'Error when connecting to Swarm server, '
                              'https://localhost:443/poll_for_test, failed to '
                              'connect after 5 attempts.')
    self.assertRaisesRegexp(slave_machine.SlaveError,
                            expected_exception_str,
                            slave.Start,
                            iterations=-1)
    self._mox.VerifyAll()

  def testAttributesFormatBadString(self):
    data = self._CreateValidAttribs()

    response = 'blah blah blah'
    self._SetPollJobAndLogFailExpectations(response,
                                           'Invalid response: blah blah blah',
                                           url='https://localhost:443/'
                                           'poll_for_test',
                                           data=data)

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine(attributes=VALID_ATTRIBUTES)
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test how values specified in the constructor are reflected in the request.
  def testConstructor(self):
    data = self._CreateValidAttribs()

    response = 'blah blah blah'
    self._SetPollJobAndLogFailExpectations(response,
                                           'Invalid response: blah blah blah',
                                           url='http://www.google.ca/'
                                           'poll_for_test',
                                           data=data)
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine(url='http://www.google.ca',
                                       attributes=VALID_ATTRIBUTES)
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with missing mandatory fields in response: come_back.
  def testMissingAll(self):
    response = self._CreateResponse()

    self._SetPollJobAndLogFailExpectations(response,
                                           'Missing fields in response: '
                                           "set(['come_back'])",
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with missing mandatory fields in response: id.
  def testMissingID(self):
    response = self._CreateResponse(machine_id=None, come_back=2)

    self._SetPollJobAndLogFailExpectations(response,
                                           'Missing fields in response: '
                                           "set(['id'])",
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with missing mandatory fields in response: try_count.
  def testMissingTryCount(self):
    response = self._CreateResponse(come_back=2, try_count=None)

    self._SetPollJobAndLogFailExpectations(response,
                                           'Missing fields in response: '
                                           "set(['try_count'])",
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with both commands and come_back missing.
  def testMissingComeback(self):
    response = self._CreateResponse()

    self._SetPollJobAndLogFailExpectations(response,
                                           'Missing fields in response: '
                                           "set(['come_back'])",
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with an extra argument in response. It should accept this
  # without an error.
  def testExtraResponseArgs(self):
    come_back = 11.0
    response = self._CreateResponse(come_back=come_back,
                                    extra_arg='INVALID')

    url_helper.UrlOpen(mox.IgnoreArg(), data=mox.IgnoreArg(),
                       max_tries=mox.IgnoreArg()
                      ).AndReturn(response)
    time.sleep(come_back)
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with missing mandatory fields in response: result_url.
  def testMissingResultURL(self):
    response = self._CreateResponse(
        commands=[slave_machine.BuildRPC('a', None)])

    self._SetPollJobAndLogFailExpectations(response,
                                           'Missing fields in response: '
                                           "set(['result_url'])",
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with bad type for result_url.
  def testBadResultURLType(self):
    response = self._CreateResponse(
        commands=[slave_machine.BuildRPC('a', None)],
        result_url=['here.com'])

    self._SetPollJobAndLogFailExpectations(response,
                                           mox.StrContains(
                                               'Failed to validate result_url'),
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())
    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with bad type for commands.
  def testBadCommands(self):
    response = self._CreateResponse(commands='do this', result_url='here.com')

    self._SetPollJobAndPostFailExpectations(
        response, 'here.com',
        '[u\'Failed to validate commands with value "do this": '
        "Invalid type: <type \\'unicode\\'> instead of <type \\'list\\'>\']")

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with wrong RPC format for commands.
  def testBadCommandsParseRPCFormat(self):
    response = self._CreateResponse(commands=['do this'], result_url='here.com')

    self._SetPollJobAndPostFailExpectations(
        response, 'here.com',
        '[\'Failed to validate commands with value "[u\\\'do this\\\']": '
        'Error when parsing RPC: Invalid RPC container\']')

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with wrong RPC function name.
  def testBadCommandsParseRPCFunctionName(self):
    commands = [slave_machine.BuildRPC('WrongFunc', None)]
    response = self._CreateResponse(commands=commands, result_url='here.com')

    self._SetPollJobAndPostFailExpectations(
        response, 'here.com', 'Unsupported RPC function name: WrongFunc')

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with correct RPC function name but wrong arg type.
  def testBadCommandsParseRPCArgType(self):
    commands = [slave_machine.BuildRPC('LogRPC', None)]
    response = self._CreateResponse(commands=commands, result_url='here.com')

    self._SetPollJobAndPostFailExpectations(
        response, 'here.com', "Invalid arg types to LogRPC: <type 'NoneType'>"
        ' (expected str or unicode)')

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with correct RPC function name and arg type.
  def testGoodCommands(self):
    function_name = 'LogRPC'
    args = 'these are some arg not argS'

    self._mox.StubOutWithMock(logging, 'info')

    commands = [slave_machine.BuildRPC(function_name, args)]
    response = self._CreateResponse(commands=commands, result_url='here.com')

    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    logging.info(args)

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test with both fields in response: come_back and commands.
  def testInvalidBothCommandsAndComeback(self):
    response = self._CreateResponse(come_back=3, commands='do this')

    self._SetPollJobAndLogFailExpectations(response,
                                           'Missing fields in response: '
                                           "set(['result_url'])",
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test invalid come_back type.
  def testInvalidComebackType(self):
    response = self._CreateResponse(come_back='3')

    self._SetPollJobAndLogFailExpectations(response,
                                           mox.StrContains(
                                               'Failed to validate come_back'),
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test invalid come_back value.
  def testInvalidComebackValue(self):
    response = self._CreateResponse(come_back=-3.0)

    self._SetPollJobAndLogFailExpectations(response,
                                           mox.StrContains(
                                               'Failed to validate come_back'),
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test invalid try_count type.
  def testInvalidTryCountType(self):
    response = self._CreateResponse(come_back=3.0, try_count='1')

    self._SetPollJobAndLogFailExpectations(response,
                                           mox.StrContains(
                                               'Failed to validate try_count'),
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test invalid try_count value.
  def testInvalidTryCountValue(self):
    response = self._CreateResponse(come_back=3.0, try_count=-1)

    self._SetPollJobAndLogFailExpectations(response,
                                           mox.StrContains(
                                               'Failed to validate try_count'),
                                           url=mox.IgnoreArg(),
                                           data=mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test 2 iterations of requests with nothing to do + 1 bad response.
  def testComeBack(self):
    come_back = 1.0
    try_count = 0
    message = 'blah blah blah'
    response = [self._CreateResponse(come_back=come_back, try_count=try_count),
                self._CreateResponse(come_back=come_back, try_count=try_count),
                self._CreateResponse(come_back=come_back, try_count=try_count),
                message]

    for i in range(len(response)):
      if i < len(response) - 1:
        url_helper.UrlOpen(
            mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
            ).AndReturn(response[i])
        time.sleep(come_back)
      else:
        self._SetPollJobAndLogFailExpectations(
            response[i], 'Invalid response: ' + message,
            mox.IgnoreArg(), mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=len(response))

    self._mox.VerifyAll()

  # Make sure the slave keeps echoing the id given to them
  # every time, not just the first time.
  def testEchoValues(self):
    come_back = 2.0
    try_count = 1
    message = 'blah blah blah'
    response = [self._CreateResponse(come_back=come_back, try_count=try_count),
                self._CreateResponse(come_back=come_back, try_count=try_count),
                self._CreateResponse(come_back=come_back, try_count=try_count),
                message]

    # The calls we expect. The last three ensure the slave echos
    # back the id assigned by the server.
    call = [self._CreateValidAttribs(),
            self._CreateValidAttribs(MACHINE_ID_1, try_count=try_count),
            self._CreateValidAttribs(MACHINE_ID_1, try_count=try_count),
            self._CreateValidAttribs(MACHINE_ID_1, try_count=try_count)]

    for i in range(len(response)):
      if i < len(response) - 1:
        url_helper.UrlOpen(
            mox.IgnoreArg(), data=call[i], max_tries=mox.IgnoreArg()
            ).AndReturn(response[i])
        time.sleep(come_back)
      else:
        self._SetPollJobAndLogFailExpectations(
            response[i], 'Invalid response: ' + message,
            mox.IgnoreArg(), mox.IgnoreArg())

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine(attributes=VALID_ATTRIBUTES)
    slave.Start(iterations=len(response))

    self._mox.VerifyAll()

  def testBuildRPCParseRPC(self):
    # Should accept empty args without an error.
    input_function = 'some function'
    input_args = None
    rpc = slave_machine.BuildRPC(input_function, input_args)
    function, args = slave_machine.ParseRPC(rpc)
    self.assertEqual(function, input_function)
    self.assertEqual(args, input_args)

    # Make sure the functions have reverse functionality of each other
    # with list as arguments.
    input_function = 'function name'
    input_args = ['123', 123, 'some text']
    rpc = slave_machine.BuildRPC(input_function, input_args)
    function, args = slave_machine.ParseRPC(rpc)
    self.assertEqual(function, input_function)
    self.assertEqual(args, input_args)

    # Make sure the functions have reverse functionality of each other
    # with string as only argument.
    input_function = 'function'
    input_args = 'some text'
    rpc = slave_machine.BuildRPC(input_function, input_args)
    function, args = slave_machine.ParseRPC(rpc)
    self.assertEqual(function, input_function)
    self.assertEqual(args, input_args)

  def testRPCParseFormat(self):
    # Wrong container type.
    rpc = ['function', 'args']
    self.assertRaisesRegexp(slave_machine.SlaveError,
                            r'Invalid RPC container',
                            slave_machine.ParseRPC,
                            rpc)

    # Missing function name.
    rpc = {'args': [1, 2, 3]}
    self.assertRaisesRegexp(slave_machine.SlaveError,
                            r"Missing mandatory field to RPC: \['function'\]",
                            slave_machine.ParseRPC,
                            rpc)

    # Missing args.
    rpc = {'function': 'func'}
    self.assertRaisesRegexp(slave_machine.SlaveError,
                            r"Missing mandatory field to RPC: \['args'\]",
                            slave_machine.ParseRPC,
                            rpc)

    # Extra args.
    rpc = {'function': 'func', 'args': None, 'extra_args': 'invalid'}
    self.assertRaisesRegexp(slave_machine.SlaveError,
                            r'Invalid extra arg to RPC: extra_args',
                            slave_machine.ParseRPC,
                            rpc)

    # Bad function name.
    rpc = {'function': [1234], 'args': [1234]}
    self.assertRaisesRegexp(slave_machine.SlaveError,
                            r'Invalid RPC call function name type',
                            slave_machine.ParseRPC,
                            rpc)

  def testSetStoreFilesRPCValidate(self):
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
      commands = [slave_machine.BuildRPC(function_name, invalid_args[i])]
      response = self._CreateResponse(commands=commands, result_url='here.com')

      self._SetPollJobAndPostFailExpectations(
          response, 'here.com', expected_error[i])

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=len(invalid_args))

    self._mox.VerifyAll()

  def testSetStoreFilesRPCExecuteMakeDirException(self):
    slave = slave_machine.SlaveMachine()
    function_name = 'StoreFiles'
    args = [(u'file path', u'file name', u'file contents')]

    commands = [slave_machine.BuildRPC(function_name, args)]
    response = self._CreateResponse(commands=commands, result_url='here.com')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    exception_message = 'makedirs exception'
    self._MockMakeDirectory(
        slave, args[0][0], exception=True, exception_message=exception_message)
    self._MockPostFailedExecuteResults(
        slave, 'MakeDirectory exception: %s' % exception_message)

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testSetStoreFilesRPCExecuteStoreFileException(self):
    slave = slave_machine.SlaveMachine()
    function_name = 'StoreFiles'
    args = [(u'file path', u'file name', u'file contents')]

    commands = [slave_machine.BuildRPC(function_name, args)]
    response = self._CreateResponse(commands=commands, result_url='here.com')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    exception_message = 'storefile exception'
    self._MockMakeDirectory(slave, args[0][0], exception=False)
    self._MockStoreFile(slave, args[0][0], args[0][1], args[0][2],
                        exception=True, exception_message=exception_message)
    self._MockPostFailedExecuteResults(
        slave, 'StoreFile exception: %s' % exception_message)

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testSetStoreFilesRPCExecuteNoException(self):
    slave = slave_machine.SlaveMachine()
    function_name = 'StoreFiles'
    args = [(u'file path', u'file name', u'file contents')]

    commands = [slave_machine.BuildRPC(function_name, args)]
    response = self._CreateResponse(commands=commands, result_url='here.com')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    self._MockMakeDirectory(slave, args[0][0], exception=False)
    self._MockStoreFile(
        slave, args[0][0], args[0][1], args[0][2], exception=False)

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testRunCommandsRPCValidate(self):
    function_name = 'RunCommands'
    invalid_args = [None, u'some arg', [[u'another arg']], ['123', 1]]

    expected_error = [
        ('Invalid %s arg type: %s (expected list of str or'
         ' unicode)'% (function_name, str(type(invalid_args[0])))),
        ('Invalid %s arg type: %s (expected list of str or'
         ' unicode)'% (function_name, str(type(invalid_args[1])))),
        ('Invalid element type in %s args: %s (expected str or unicode)'%
         (function_name, str(type(invalid_args[2][0])))),
        ('Invalid element type in %s args: %s (expected str or unicode)'%
         (function_name, str(type(invalid_args[3][1]))))]

    self.assertEqual(len(invalid_args), len(expected_error))

    for i in range(0, len(invalid_args)):
      commands = [slave_machine.BuildRPC(function_name, invalid_args[i])]
      response = self._CreateResponse(commands=commands, result_url='here.com')

      self._SetPollJobAndPostFailExpectations(
          response, 'here.com', expected_error[i])

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=len(invalid_args))

    self._mox.VerifyAll()

  def testRunCommandsRPCExecuteSubprocessException(self):
    function_name = 'RunCommands'
    args = [u'is an', u'awesome', u'language']

    slave = slave_machine.SlaveMachine()
    commands = [slave_machine.BuildRPC(function_name, args)]
    response = self._CreateResponse(commands=commands, result_url='here.com')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    # Mock subprocess to raise exception.
    full_commands = [sys.executable] + args
    self._MockSubprocessCheckCall(commands=full_commands, exception=True)

    # Mock the call to post failed results.
    self._MockPostFailedExecuteResults(
        slave, "Command '%s' returned non-zero exit status -1"
        % str(full_commands))

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  def testRunCommandsRPCExecuteNoException(self):
    function_name = 'RunCommands'
    args = [u'is an', u'awesome', u'language']

    slave = slave_machine.SlaveMachine()
    commands = [slave_machine.BuildRPC(function_name, args)]
    response = self._CreateResponse(commands=commands, result_url='here.com')

    # Mock initial job request.
    url_helper.UrlOpen(
        mox.IgnoreArg(), data=mox.IgnoreArg(), max_tries=mox.IgnoreArg()
        ).AndReturn(response)

    # Mock subprocess to raise exception.
    self._MockSubprocessCheckCall(commands=[sys.executable]+args)

    self._mox.ReplayAll()

    slave.Start(iterations=1)

    self._mox.VerifyAll()

  # Test to make sure the result url of the slave is correctly reset each time a
  # job is requested.
  def testResultURLReset(self):
    commands = [slave_machine.BuildRPC('WrongFunc', None)]
    response = [self._CreateResponse(commands=commands, result_url='here1.com'),
                self._CreateResponse(commands=commands),
                self._CreateResponse(commands=commands, result_url='here2.com')]

    self._SetPollJobAndPostFailExpectations(
        response[0], 'here1.com', 'Unsupported RPC function name: WrongFunc')
    # This response has no result_url. So it shouldn't use the result_url given
    # in the first response.
    self._SetPollJobAndLogFailExpectations(
        response[1], "Missing fields in response: set(['result_url'])",
        mox.IgnoreArg(), mox.IgnoreArg())
    self._SetPollJobAndPostFailExpectations(
        response[2], 'here2.com', 'Unsupported RPC function name: WrongFunc')

    self._mox.ReplayAll()

    slave = slave_machine.SlaveMachine()
    slave.Start(iterations=3)

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.FATAL)
  unittest.main()
