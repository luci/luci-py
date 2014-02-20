#!/usr/bin/env python
# coding=utf-8
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import StringIO
import logging
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile

# Import everything that does not require sys.path hack first.

import local_test_runner

from common import swarm_constants
from common import test_request_message

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from depot_tools import auto_stub
from third_party.mox import mox

DATA_FILE_REGEX = r'\S*/%s/%s'
DATA_FOLDER_REGEX = r'\S*/%s'


class TestLocalTestRunner(auto_stub.TestCase):
  """Test class for the LocalTestRunner class."""

  def setUp(self):
    super(TestLocalTestRunner, self).setUp()
    # We don't want the application logs to interfere with our own messages.
    # You can comment it out or change for more information when debugging.
    # This is a global variable because some tests need to know what this
    # value is.
    self.logging_level = logging.FATAL
    logging.disable(self.logging_level)
    self.result_url = 'http://a.com/result'
    self.ping_url = 'http://a.com/ping'
    self.ping_delay = 10
    self.output_destination = {}
    self.test_run_name = 'TestRunName'
    self.config_name = 'ConfigName'
    self._mox = mox.Mox()
    (data_file_descriptor, self.data_file_name) = tempfile.mkstemp()
    os.close(data_file_descriptor)
    self.files_to_remove = [self.data_file_name]
    os.mkdir = lambda _: None

    self.result_string = 'This is a result string'
    self.result_codes = [1, 42, 3, 0]

    self.test_name1 = 'test1'
    self.test_name2 = 'test2'

  def tearDown(self):
    self._mox.UnsetStubs()
    for file_to_remove in self.files_to_remove:
      os.remove(file_to_remove)
    super(TestLocalTestRunner, self).tearDown()

  def CreateValidFile(self, test_objects_data=None, test_run_data=None,
                      test_run_cleanup=None, config_env=None,
                      test_run_env=None, test_encoding=None):
    """Creates a text file that the local_test_runner can load.

    Args:
      test_objects_data: An array of 6-tuples with (test_name, action,
          decorate_output, env_vars, hard_time_out, io_time_out) values them.
          A TestObject will be created for each of them.
      test_run_data: The data list to be passed to the TestRun object.
      test_run_cleanup: The cleanup string to be passed to the TestRun object.
      config_env: The dictionary to be used for the configuration's env_vars.
      test_run_env: The dictionary to be used for the test run's env_vars.
      test_encoding: The enocding to use for the test's output.
    """
    if not test_objects_data:
      test_objects_data = [('a', ['a'], None, None, 0, 0)]
    if not test_run_data:
      test_run_data = []

    if not test_encoding:
      test_encoding = 'ascii'

    test_config = test_request_message.TestConfiguration(
        env_vars=config_env, config_name=self.config_name, os='a', browser='a',
        cpu='a')
    test_objects = []
    for test_object_data in test_objects_data:
      test_objects.append(test_request_message.TestObject(
          test_name=test_object_data[0], action=test_object_data[1],
          decorate_output=test_object_data[2], env_vars=test_object_data[3],
          hard_time_out=test_object_data[4], io_time_out=test_object_data[5]))
    test_run = test_request_message.TestRun(
        test_run_name=self.test_run_name, env_vars=test_run_env,
        data=test_run_data, configuration=test_config,
        result_url=self.result_url, ping_url=self.ping_url,
        ping_delay=self.ping_delay, output_destination=self.output_destination,
        tests=test_objects, cleanup=test_run_cleanup, encoding=test_encoding)

    # Check that the message is valid, otherwise the test will fail when trying
    # to load it.
    errors = []
    self.assertTrue(test_run.IsValid(errors), errors)

    data = test_request_message.Stringize(test_run, json_readable=True)
    with open(self.data_file_name, 'wb') as f:
      f.write(data.encode('utf-8'))

  def testInvalidTestRunFiles(self):
    def TestInvalidContent(file_content):
      with open(self.data_file_name, 'wb') as f:
        f.write(file_content)
      self.assertRaises(local_test_runner.Error,
                        local_test_runner.LocalTestRunner,
                        self.data_file_name)
    TestInvalidContent('')
    TestInvalidContent('{}')
    TestInvalidContent('{\'a\':}')
    TestInvalidContent('{\'test_run_name\': \'name\'}')
    TestInvalidContent('{\'test_run_name\': \'name\', \'data\': []}')
    TestInvalidContent('{\'test_run_name\': \'name\', \'data\': [\'a\']}')
    TestInvalidContent('{\'test_run_name\': \'name\', \'data\': [\'a\'],'
                       '\'result_url\':\'http://a.com\'}')
    TestInvalidContent('{\'test_run_name\': \'name\', \'data\': [\'a\'],'
                       '\'result_url\':\'http://a.com\', \'tests\': [\'a\']}')

  def testValidTestRunFiles(self):
    self.CreateValidFile()
    # This will raise an exception, which will make the test fail,
    # if there is a problem.
    _ = local_test_runner.LocalTestRunner(self.data_file_name)

  def testInvalidDataFolderName(self):
    def TestInvalidName(name):
      self.assertRaises(local_test_runner.Error,
                        local_test_runner.LocalTestRunner,
                        self.data_file_name, data_folder_name=name)
    self.CreateValidFile()
    TestInvalidName('.')
    TestInvalidName('..')
    TestInvalidName('c:\\tests')
    TestInvalidName('data/')

  def PrepareRunCommandCall(self, cmd):
    self.mock_proc = self._mox.CreateMock(subprocess.Popen)
    self._mox.StubOutWithMock(local_test_runner.subprocess, 'Popen')
    local_test_runner.subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=None,
        bufsize=1, stdin=subprocess.PIPE,
        universal_newlines=True).AndReturn(self.mock_proc)

    (output_pipe, input_pipe) = os.pipe()
    self.mock_proc.stdin_handle = os.fdopen(input_pipe, 'wb')

    stdout_handle = os.fdopen(output_pipe)
    self.mock_proc.stdout = stdout_handle

  def testRunCommand(self):
    cmd = ['a']
    exit_code = 42
    self.PrepareRunCommandCall(cmd)

    self.mock_proc.stdin_handle.write(self.result_string)
    self.mock_proc.poll().AndReturn(None)
    self.mock_proc.poll().WithSideEffects(
        self.mock_proc.stdin_handle.close()).AndReturn(exit_code)
    self._mox.StubOutWithMock(os, 'chdir')
    os.chdir(mox.IsA(str)).AndReturn(None)
    os.chdir(mox.IsA(str)).AndReturn(None)
    self._mox.ReplayAll()

    self.CreateValidFile()
    runner = local_test_runner.LocalTestRunner(self.data_file_name)
    (exit_code_ret, result_string_ret) = runner._RunCommand(cmd, 0, 0)
    self.assertEqual(exit_code_ret, exit_code)
    self.assertEqual(result_string_ret, self.result_string)

    self._mox.VerifyAll()

  def testRunCommandHardTimeout(self):
    cmd = ['a']
    self.PrepareRunCommandCall(cmd)

    self.mock_proc.pid = 42
    self.mock_proc.poll().AndReturn(None)
    self._mox.StubOutWithMock(os, 'chdir')
    os.chdir(mox.IsA(str)).AndReturn(None)
    os.chdir(mox.IsA(str)).AndReturn(None)

    self._mox.StubOutWithMock(local_test_runner, '_TimedOut')
    # Ensure the hard limit is hit.
    local_test_runner._TimedOut(1, mox.IgnoreArg()).AndReturn(
        True)
    # Ensure the IO time limit isn't hit.
    local_test_runner._TimedOut(0, mox.IgnoreArg()).AndReturn(
        True)

    self._mox.ReplayAll()

    self.CreateValidFile()
    runner = local_test_runner.LocalTestRunner(self.data_file_name)
    (exit_code, result_string) = runner._RunCommand(cmd, 1, 0)
    self.assertNotEqual(exit_code, 0)
    self.assertTrue(result_string)
    self.assertIn(str(self.mock_proc.pid), result_string)

    self._mox.VerifyAll()

  def testRunCommandIOTimeout(self):
    cmd = ['a']
    self.PrepareRunCommandCall(cmd)

    self.mock_proc.pid = 42
    self.mock_proc.poll().AndReturn(None)
    self._mox.StubOutWithMock(os, 'chdir')
    os.chdir(mox.IsA(str)).AndReturn(None)
    os.chdir(mox.IsA(str)).AndReturn(None)

    self._mox.StubOutWithMock(local_test_runner, '_TimedOut')
    # The hard limit isn't hit
    local_test_runner._TimedOut(1, mox.IgnoreArg()).AndReturn(
        False)
    # Ensure the IO time limit is hit.
    local_test_runner._TimedOut(0, mox.IgnoreArg()).AndReturn(
        True)

    self._mox.ReplayAll()

    self.CreateValidFile()
    runner = local_test_runner.LocalTestRunner(self.data_file_name)
    (exit_code, result_string) = runner._RunCommand(cmd, 1, 0)
    self.assertNotEqual(exit_code, 0)
    self.assertTrue(result_string)
    self.assertIn(str(self.mock_proc.pid), result_string)

    self._mox.VerifyAll()

  def testRunCommandAndPing(self):
    cmd = ['a']
    exit_code = 42
    self.PrepareRunCommandCall(cmd)

    self._mox.StubOutWithMock(os, 'chdir')
    os.chdir(mox.IsA(str)).AndReturn(None)
    os.chdir(mox.IsA(str)).AndReturn(None)

    # Ensure that the first the server is pinged after both poll because
    # the require ping delay will have elapsed.
    self._mox.StubOutWithMock(local_test_runner.url_helper, 'UrlOpen')
    self.mock_proc.stdin_handle.write(self.result_string)
    self.mock_proc.poll().AndReturn(None)
    local_test_runner.url_helper.UrlOpen(self.ping_url).AndReturn('')
    self.mock_proc.poll().WithSideEffects(
        self.mock_proc.stdin_handle.close()).AndReturn(exit_code)
    local_test_runner.url_helper.UrlOpen(self.ping_url).AndReturn('')
    self._mox.ReplayAll()

    # Set the ping delay to 0 to ensure we get a ping for this runner.
    self.ping_delay = 0

    self.CreateValidFile()
    runner = local_test_runner.LocalTestRunner(self.data_file_name)
    (exit_code_ret, result_string_ret) = runner._RunCommand(cmd, 0, 0)
    self.assertEqual(exit_code_ret, exit_code)
    self.assertEqual(result_string_ret, self.result_string)

    self._mox.VerifyAll()

  def PrepareDownloadCall(self, cleanup=None, data_folder_name=None):
    self.local_file1 = 'foo'
    self.local_file2 = 'bar'
    data_url = 'http://a.com/%s' % self.local_file1
    data_url_with_local_name = ('http://b.com/download_key',
                                self.local_file2)
    self.CreateValidFile(test_run_data=[data_url, data_url_with_local_name],
                         test_run_cleanup=cleanup)
    self.runner = local_test_runner.LocalTestRunner(
        self.data_file_name, data_folder_name=data_folder_name)

    self._mox.StubOutWithMock(local_test_runner, 'url_helper')
    data_dir = os.path.basename(self.runner.data_dir)
    local_test_runner.url_helper.DownloadFile(
        mox.Regex(DATA_FILE_REGEX % (data_dir, self.local_file1)),
        data_url).AndReturn(True)
    local_test_runner.url_helper.DownloadFile(
        mox.Regex(DATA_FILE_REGEX % (data_dir, self.local_file2)),
        data_url_with_local_name[0]).AndReturn(True)

    self._mox.StubOutWithMock(local_test_runner, 'zipfile')
    self.mock_zipfile = self._mox.CreateMock(zipfile.ZipFile)
    local_test_runner.zipfile.ZipFile(
        mox.Regex(DATA_FILE_REGEX %
                  (data_dir, self.local_file1))).AndReturn(self.mock_zipfile)
    local_test_runner.zipfile.ZipFile(
        mox.Regex(DATA_FILE_REGEX %
                  (data_dir, self.local_file2))).AndReturn(self.mock_zipfile)
    self.mock_zipfile.extractall(mox.Regex(DATA_FOLDER_REGEX % data_dir))
    self.mock_zipfile.close()
    self.mock_zipfile.extractall(mox.Regex(DATA_FOLDER_REGEX % data_dir))
    self.mock_zipfile.close()

  def testDownloadAndExplode(self):
    self.PrepareDownloadCall()

    self._mox.ReplayAll()
    self.assertTrue(self.runner.DownloadAndExplodeData())

    self._mox.VerifyAll()

  def testDownloadExplodeAndCleanupZip(self):
    self.PrepareDownloadCall(cleanup='zip')
    self._mox.StubOutWithMock(local_test_runner, 'os')
    data_dir = os.path.basename(self.runner.data_dir)
    local_test_runner.os.remove(
        mox.Regex(DATA_FILE_REGEX % (data_dir,
                                     self.local_file1))).AndReturn(None)
    local_test_runner.os.remove(
        mox.Regex(DATA_FILE_REGEX % (data_dir,
                                     self.local_file2))).AndReturn(None)

    self._mox.ReplayAll()
    self.assertTrue(self.runner.DownloadAndExplodeData())

    self._mox.VerifyAll()

  def testDownloadExplodeAndCleanupData(self):
    self.PrepareDownloadCall(cleanup='data', data_folder_name='data')
    self._mox.StubOutWithMock(local_test_runner, '_DeleteFileOrDirectory')
    local_test_runner._DeleteFileOrDirectory(mox.IsA(str)).AndReturn(None)
    local_test_runner._DeleteFileOrDirectory(mox.IsA(str)).AndReturn(None)

    self._mox.ReplayAll()
    self.assertTrue(self.runner.DownloadAndExplodeData())
    self.runner.__del__()

    self._mox.VerifyAll()

  def PrepareRunTestsCall(self, decorate_output=None, results=None,
                          encoding=None, test_names=None, test_run_env=None,
                          config_env=None, slave_test_env=None):
    if not decorate_output:
      decorate_output = [False, False]
    if not results:
      results = [(0, 'success'), (0, 'success')]
    self.action1 = ['foo']
    self.action2 = ['bar', 'foo', 'bar']
    if not test_names:
      test_names = [self.test_name1, self.test_name2]
    self.assertEqual(len(results), 2)
    test0_env = test1_env = None
    if slave_test_env:
      assert len(slave_test_env) == 2
      test0_env = slave_test_env[0]
      test1_env = slave_test_env[1]
    test_objects_data = [
        (test_names[0], self.action1, decorate_output[0], test0_env, 60, 0),
        (test_names[1], self.action2, decorate_output[1], test1_env, 60, 9)
    ]
    self.assertEqual(len(decorate_output), len(results))
    self.CreateValidFile(test_objects_data=test_objects_data,
                         test_run_env=test_run_env, config_env=config_env,
                         test_encoding=encoding)
    self.runner = local_test_runner.LocalTestRunner(self.data_file_name)

    self._mox.StubOutWithMock(self.runner, '_RunCommand')
    env_items = os.environ.items()
    if config_env:
      env_items += config_env.items()
    if test_run_env:
      env_items += test_run_env.items()
    env = dict(env_items)

    if test0_env:
      env = dict(env_items + test0_env.items())
    self.runner._RunCommand(self.action1, 60, 0, env=env).AndReturn(results[0])

    if test1_env:
      env = dict(env_items + test1_env.items())
    self.runner._RunCommand(self.action2, 60, 9, env=env).AndReturn(results[1])

  def testRunTests(self):
    self.PrepareRunTestsCall(decorate_output=[True, True])
    self._mox.ReplayAll()
    (success, result_codes, result_string) = self.runner.RunTests()
    self.assertTrue(success)
    self.assertEqual([0, 0], result_codes)
    self.assertIn('[==========] Running 2 tests from %s test run.' %
                  self.test_run_name, result_string)
    self.assertIn('success', result_string)
    self.assertIn('[----------] %s summary' % self.test_run_name, result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[  PASSED  ] 2 tests.', result_string)
    self.assertIn('[  FAILED  ] 0 tests', result_string)
    self.assertIn('0 FAILED TESTS', result_string)

    self._mox.VerifyAll()

  def _RunTestsWithEnvVars(self, platform):
    config_env = {'var1': 'value1', 'var2': 'value2'}
    test_run_env = {'var3': 'value3', 'var4': 'value4'}
    test0_env = {'var5': 'value5', 'var6': 'value6'}
    test1_env = {'var7': 'value7', 'var8': 'value8'}
    self._mox.StubOutWithMock(local_test_runner.sys, 'platform')
    local_test_runner.sys.platform = platform
    self.PrepareRunTestsCall(decorate_output=[True, True],
                             config_env=config_env, test_run_env=test_run_env,
                             slave_test_env=[test0_env, test1_env])

    self._mox.ReplayAll()
    (success, result_codes, result_string) = self.runner.RunTests()
    self.assertTrue(success)
    self.assertEqual([0, 0], result_codes)
    self.assertIn('[==========] Running 2 tests from %s test run.' %
                  self.test_run_name, result_string)
    self.assertIn('success', result_string)
    self.assertIn('[----------] %s summary' % self.test_run_name, result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[  PASSED  ] 2 tests.', result_string)
    self.assertIn('[  FAILED  ] 0 tests', result_string)
    self.assertIn('0 FAILED TESTS', result_string)

    self._mox.VerifyAll()

  def testRunTestsWithEnvVarsOnWindows(self):
    self._RunTestsWithEnvVars('win32')

  def testRunTestsWithEnvVarsOnLinux(self):
    self._RunTestsWithEnvVars('linux2')

  def testRunTestsWithEnvVarsOnMac(self):
    self._RunTestsWithEnvVars('darwin')

  def testRunFailedTests(self):
    ok_str = 'OK'
    not_ok_str = 'NOTOK'
    self.PrepareRunTestsCall(results=[(1, not_ok_str), (0, ok_str)])
    self._mox.ReplayAll()

    (success, result_codes, result_string) = self.runner.RunTests()
    self.assertFalse(success)
    self.assertEqual([1, 0], result_codes)
    self.assertIn('[==========] Running 2 tests from %s test run.' %
                  self.test_run_name, result_string)
    self.assertIn(not_ok_str, result_string)
    self.assertIn(ok_str, result_string)
    # Should NOT be decorated:
    self.assertNotIn('[ RUN      ] %s.%s' % (self.test_run_name,
                                             self.test_name1), result_string)
    self.assertNotIn('[ RUN      ] %s.%s' % (self.test_run_name,
                                             self.test_name2), result_string)
    self.assertNotIn('[----------] %s summary' % self.test_run_name,
                     result_string)
    self.assertNotIn('[==========] 2 tests ran.', result_string)
    self.assertNotIn('[  PASSED  ] 1 tests.', result_string)
    self.assertNotIn('[  FAILED  ] 1 tests, listed below:', result_string)
    self.assertNotIn('[  FAILED  ] %s.%s' % (self.test_run_name,
                                             self.test_name1),
                     result_string)
    self.assertNotIn('1 FAILED TESTS', result_string)

    self._mox.VerifyAll()

  def testRunDecoratedTests(self):
    ok_str = 'OK'
    not_ok_str = 'NOTOK'
    self.PrepareRunTestsCall(results=[(1, not_ok_str), (0, ok_str)],
                             decorate_output=[False, True])
    self._mox.ReplayAll()

    (success, result_codes, result_string) = self.runner.RunTests()
    self.assertFalse(success)
    self.assertEqual([1, 0], result_codes)
    self.assertIn('[==========] Running 2 tests from %s test run.' %
                  self.test_run_name, result_string)
    self.assertIn(not_ok_str, result_string)
    # Should NOT be decorated:
    self.assertNotIn('[ RUN      ] %s.%s' % (self.test_run_name,
                                             self.test_name1), result_string)
    self.assertIn('[ RUN      ] %s.%s' % (self.test_run_name, self.test_name2),
                  result_string)
    self.assertIn(ok_str, result_string)
    self.assertIn('[       OK ] %s.%s' % (self.test_run_name, self.test_name2),
                  result_string)
    self.assertIn('[----------] %s summary' % self.test_run_name, result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[  PASSED  ] 1 tests.', result_string)
    self.assertIn('[  FAILED  ] 1 tests, listed below:', result_string)
    # Should be there even when not decorated.
    self.assertIn('[  FAILED  ] %s.%s' % (self.test_run_name, self.test_name1),
                  result_string)
    self.assertIn('1 FAILED TESTS', result_string)
    self._mox.VerifyAll()

  def testRunTestsWithNonAsciiOutput(self):
    # This result string must be a byte string, not unicode, because that is
    # how the subprocess returns its output. If this string is changed to
    # unicode then the test will pass, but real world tests can still crash.
    unicode_string_valid_utf8 = b'Output with \xe2\x98\x83 â'
    unicode_string_invalid_utf8 = b'Output with invalid \xa0\xa1'

    test_names = [u'Testâ-\xe2\x98\x83',
                  u'Test1â-\xe2\x98\x83']
    self.test_run_name = u'Unicode Test Runâ-\xe2\x98\x83'
    encoding = 'utf-8'

    self.PrepareRunTestsCall(decorate_output=[True, False],
                             results=[(0, unicode_string_valid_utf8),
                                      (0, unicode_string_invalid_utf8)],
                             test_names=test_names,
                             encoding=encoding)
    self._mox.ReplayAll()

    (success, result_codes, result_string) = self.runner.RunTests()
    self.assertTrue(success)
    self.assertEqual([0, 0], result_codes)
    self.assertEqual(unicode, type(result_string))

    # The valid utf-8 output should still be present and unchanged, but the
    # invalid string should have the invalid characters replaced.
    self.assertIn(unicode_string_valid_utf8.decode(encoding), result_string)

    string_invalid_output = (
        '! Output contains characters not valid in %s encoding !\n%s' %
        (encoding, unicode_string_invalid_utf8.decode(encoding, 'replace')))
    self.assertIn(string_invalid_output, result_string)

    self._mox.VerifyAll()

  def testPostIncrementalOutput(self):
    cmd = ['a']
    exit_code = 42
    self.PrepareRunCommandCall(cmd)

    # Setup incremental content to be written to the stdout file used to
    # communicate between the command execution and the runner.
    stdout_contents = ['initial_content', 'other_content',
                       'more_content', 'even_more_content']
    stdout_contents_stack = stdout_contents[:]

    def WriteStdoutContent():
      # This will be called everytime we poll for the status of the command.
      self.mock_proc.stdin_handle.write(stdout_contents_stack.pop(0))

      if not stdout_contents_stack:
        self.mock_proc.stdin_handle.close()

    for _ in range(len(stdout_contents) - 1):
      self.mock_proc.poll().WithSideEffects(WriteStdoutContent).AndReturn(None)
    self.mock_proc.poll().WithSideEffects(WriteStdoutContent).AndReturn(
        exit_code)
    self._mox.StubOutWithMock(os, 'chdir')
    os.chdir(mox.IsA(str)).AndReturn(None)
    os.chdir(mox.IsA(str)).AndReturn(None)
    self._mox.ReplayAll()

    self.output_destination['url'] = 'http://blabla.com'
    self.output_destination['size'] = 1
    self.result_url = None
    self.CreateValidFile()

    self.streamed_output = ''

    def ValidateUrlData(data):
      self.assertEqual(self.test_run_name, data['n'])
      self.assertEqual(self.config_name, data['c'])
      self.assertEqual('pending', data['s'])
      return True

    def ValidateUrlFiles(files):
      self.assertEqual(1, len(files))
      self.assertEqual(3, len(files[0]))

      self.assertEqual(swarm_constants.RESULT_STRING_KEY, files[0][0])
      self.assertEqual(swarm_constants.RESULT_STRING_KEY, files[0][1])

      self.streamed_output += files[0][2]
      return True

    self._mox.StubOutWithMock(local_test_runner.url_helper, 'UrlOpen')
    local_test_runner.url_helper.UrlOpen(self.output_destination['url'],
                                         data=mox.Func(ValidateUrlData),
                                         files=mox.Func(ValidateUrlFiles),
                                         max_tries=1,
                                         method='POSTFORM').AndReturn(
                                             'Accepted')

    self._mox.ReplayAll()

    runner = local_test_runner.LocalTestRunner(self.data_file_name)
    (exit_code_ret, result_string_ret) = runner._RunCommand(cmd, 0, 0)
    self.assertEqual(exit_code_ret, exit_code)
    self.assertEqual(result_string_ret, 'No output!')
    self.assertEqual(self.streamed_output, ''.join(stdout_contents))

    self._mox.VerifyAll()

  def testPostFinalOutput(self):
    self.output_destination['url'] = 'http://blabla.com'
    self.result_url = None
    self.CreateValidFile()
    self._mox.StubOutWithMock(local_test_runner.url_helper, 'UrlOpen')
    data = {'n': self.test_run_name, 'c': self.config_name, 's': 'success'}
    files = [(swarm_constants.RESULT_STRING_KEY,
              swarm_constants.RESULT_STRING_KEY,
              '')]
    max_url_retries = 1
    local_test_runner.url_helper.UrlOpen(
        self.output_destination['url'],
        data=data.copy(),
        files=files[:],
        max_tries=max_url_retries,
        method='POSTFORM').AndReturn('')
    data['s'] = 'failure'
    local_test_runner.url_helper.UrlOpen(
        ('%s?1=2' % self.output_destination['url']),
        data=data.copy(),
        files=files[:],
        max_tries=max_url_retries,
        method='POSTFORM').AndReturn('')
    self._mox.ReplayAll()

    self.runner = local_test_runner.LocalTestRunner(
        self.data_file_name, max_url_retries=max_url_retries)
    self.assertTrue(self.runner.PublishResults(True, [], 'Not used'))

    # Also test with other CGI param in the URL.
    self.output_destination['url'] = '%s?1=2' % self.output_destination['url']
    self.CreateValidFile()  # Recreate the request file with new value.
    self.runner = local_test_runner.LocalTestRunner(self.data_file_name)
    self.assertTrue(self.runner.PublishResults(False, [], 'Not used'))

    self._mox.VerifyAll()

  def testPublishResults(self):
    self.CreateValidFile()
    self._mox.StubOutWithMock(local_test_runner.url_helper, 'UrlOpen')
    max_url_retries = 1
    local_test_runner.url_helper.UrlOpen(
        self.result_url,
        data={'n': self.test_run_name,
              'c': self.config_name,
              'x': ', '.join([str(i) for i in self.result_codes]),
              's': True,
              'o': False},
        files=[(swarm_constants.RESULT_STRING_KEY,
                swarm_constants.RESULT_STRING_KEY,
                self.result_string)],
        max_tries=max_url_retries,
        method='POSTFORM').AndReturn('')
    local_test_runner.url_helper.UrlOpen(
        '%s?1=2' % self.result_url,
        data={'n': self.test_run_name,
              'c': self.config_name,
              'x': ', '.join([str(i) for i in self.result_codes]),
              's': False,
              'o': False},
        files=[(swarm_constants.RESULT_STRING_KEY,
                swarm_constants.RESULT_STRING_KEY,
                self.result_string)],
        method='POSTFORM',
        max_tries=max_url_retries).AndReturn('')
    self._mox.ReplayAll()

    self.runner = local_test_runner.LocalTestRunner(
        self.data_file_name, max_url_retries=max_url_retries)
    self.assertTrue(self.runner.PublishResults(True, self.result_codes,
                                               self.result_string))

    # Also test with other CGI param in the URL.
    self.result_url = '%s?1=2' % self.result_url
    self.CreateValidFile()  # Recreate the request file with new value.
    self.runner = local_test_runner.LocalTestRunner(self.data_file_name)
    self.assertTrue(self.runner.PublishResults(False, self.result_codes,
                                               self.result_string))

    self._mox.VerifyAll()

  def testPublishResultsUnableToReachResultUrl(self):
    self.CreateValidFile()
    self._mox.StubOutWithMock(local_test_runner.url_helper, 'UrlOpen')
    max_url_retries = 1
    local_test_runner.url_helper.UrlOpen(
        self.result_url,
        data={'n': self.test_run_name,
              'c': self.config_name,
              'x': ', '.join([str(i) for i in self.result_codes]),
              's': True,
              'o': False},
        files=[(swarm_constants.RESULT_STRING_KEY,
                swarm_constants.RESULT_STRING_KEY,
                self.result_string)],
        max_tries=max_url_retries,
        method='POSTFORM').AndReturn(None)
    self._mox.ReplayAll()

    self.runner = local_test_runner.LocalTestRunner(
        self.data_file_name, max_url_retries=max_url_retries)
    self.assertFalse(self.runner.PublishResults(True, self.result_codes,
                                                self.result_string))

    self._mox.VerifyAll()

  def testPublishResultsHTTPS(self):
    self.CreateValidFile()
    self._mox.StubOutWithMock(local_test_runner.url_helper, 'UrlOpen')
    self.result_url = 'https://secure.com/result'

    max_url_retries = 1
    local_test_runner.url_helper.UrlOpen(
        self.result_url,
        data={'n': self.test_run_name,
              'c': self.config_name,
              'x': ', '.join([str(i) for i in self.result_codes]),
              's': True,
              'o': False},
        files=[(swarm_constants.RESULT_STRING_KEY,
                swarm_constants.RESULT_STRING_KEY,
                self.result_string)],
        max_tries=max_url_retries,
        method='POSTFORM').AndReturn('')
    self._mox.ReplayAll()

    self.CreateValidFile()
    self.runner = local_test_runner.LocalTestRunner(
        self.data_file_name, max_url_retries=max_url_retries)
    self.assertTrue(self.runner.PublishResults(True, self.result_codes,
                                               self.result_string))

    self._mox.VerifyAll()

  def testPublishResultsToFile(self):
    (result_file_descriptor, result_file_path) = tempfile.mkstemp()
    self.files_to_remove.append(result_file_path)
    os.close(result_file_descriptor)
    self.result_url = 'file://%s' % result_file_path.replace('\\', '/')
    self.CreateValidFile()  # Recreate the request file with new value.
    self.runner = local_test_runner.LocalTestRunner(self.data_file_name)
    self.assertTrue(self.runner.PublishResults(True, [], self.result_string))
    with open(result_file_path, 'rb') as f:
      self.assertEqual(f.read(), self.result_string)

  def testPublishInternalErrors(self):
    try:
      # This test requires logging to be enabled.
      logging.disable(logging.NOTSET)
      for i in logging.getLogger().handlers:
        self.mock(i, 'stream', StringIO.StringIO())

      self.CreateValidFile()
      self._mox.StubOutWithMock(local_test_runner.url_helper, 'UrlOpen')
      exception_text = 'Bad MAD, no cookie!'
      max_url_retries = 1

      # We use this function to check if exception_text is properly published
      # and that the overwrite value is True.
      def ValidateInternalErrorsResult(url_files):
        if len(url_files) != 1:
          return False

        if (url_files[0][0] != swarm_constants.RESULT_STRING_KEY or
            url_files[0][1] != swarm_constants.RESULT_STRING_KEY):
          return False

        return exception_text in url_files[0][2]

      local_test_runner.url_helper.UrlOpen(
          unicode(self.result_url),
          data=mox.ContainsKeyValue('o', True),
          files=mox.Func(ValidateInternalErrorsResult),
          max_tries=max_url_retries,
          method='POSTFORM').AndReturn('')
      self._mox.ReplayAll()

      self.runner = local_test_runner.LocalTestRunner(
          self.data_file_name, max_url_retries=max_url_retries)
      self.runner.TestLogException(exception_text)
      self.runner.PublishInternalErrors()

      self._mox.VerifyAll()
    finally:
      logging.disable(self.logging_level)

  def testShutdownOrReturn(self):
    self.CreateValidFile()
    runner = local_test_runner.LocalTestRunner(self.data_file_name,
                                               restart_on_failure=False)
    return_value = 0

    # No test failures and restart on failure disabled.
    runner.success = True
    self.assertEqual(return_value, runner.ReturnExitCode(return_value))

    # Test failures with restart on failure disabled.
    self.assertEqual(return_value, runner.ReturnExitCode(return_value))

    # No test failures with restart on failure enabled.
    runner.restart_on_failure = True
    self.assertEqual(return_value, runner.ReturnExitCode(return_value))

    # Test failures with restart of feature enabled.
    runner.success = False
    self.assertEqual(swarm_constants.RESTART_EXIT_CODE,
                     runner.ReturnExitCode(return_value))


if __name__ == '__main__':
  unittest.main()
