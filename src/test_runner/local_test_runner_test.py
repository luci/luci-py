#!/usr/bin/python2.7
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Unittest to exercise the code in local_test_runner.py."""





import logging
import os
import subprocess
import tempfile
import unittest
import zipfile


from common import test_request_message
from common import url_helper
from third_party.mox import mox
from test_runner import local_test_runner

DATA_FILE_REGEX = r'\S*/%s/%s'
DATA_FOLDER_REGEX = r'\S*/%s'


class TestLocalTestRunner(unittest.TestCase):
  """Test class for the LocalTestRunner class."""

  def setUp(self):
    self.result_url = 'http://a.com/result'
    self.ping_url = 'http://a.com/ping'
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

  def tearDown(self):
    self._mox.UnsetStubs()
    self._mox.ResetAll()
    for file_to_remove in self.files_to_remove:
      os.remove(file_to_remove)

  def CreateValidFile(self, test_objects_data=None, test_run_data=None,
                      test_run_cleanup=None, config_env=None,
                      test_run_env=None):
    """Creates a text file that the local_test_runner can load.

    Args:
      test_objects_data: An array of 5-tuples with (test_name, action,
          decorate_output, time_out) values them. A TestObject will be created
          for each of them.
      test_run_data: The data list to be passed to the TestRun object.
      test_run_cleanup: The cleanup string to be passed to the TestRun object.
      config_env: The dictionary to be used for the configuration's env_vars.
      test_run_env: The dictionary to be used for the test run's env_vars.
    """
    if not test_objects_data:
      test_objects_data = [('a', ['a'], None, None, 0)]
    if not test_run_data:
      test_run_data = []

    data_file = open(self.data_file_name, mode='w+b')
    test_config = test_request_message.TestConfiguration(
        env_vars=config_env, config_name=self.config_name, os='a', browser='a',
        cpu='a')
    test_objects = []
    for test_object_data in test_objects_data:
      test_objects.append(test_request_message.TestObject(
          test_name=test_object_data[0], action=test_object_data[1],
          decorate_output=test_object_data[2], env_vars=test_object_data[3],
          time_out=test_object_data[4]))
    test_run = test_request_message.TestRun(
        test_run_name=self.test_run_name, env_vars=test_run_env,
        data=test_run_data, configuration=test_config,
        result_url=self.result_url, ping_url=self.ping_url,
        output_destination=self.output_destination, tests=test_objects,
        cleanup=test_run_cleanup)
    data_file.write(test_request_message.Stringize(test_run,
                                                   json_readable=True))
    data_file.close()

  def testInvalidTestRunFiles(self):
    def TestInvalidContent(file_content):
      data_file = open(self.data_file_name, mode='w+b')
      data_file.write(file_content)
      data_file.close()
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
    self.mock_proc.stdin_handle = os.fdopen(input_pipe, 'w')

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
    (exit_code_ret, result_string_ret) = runner._RunCommand(cmd, time_out=0)
    self.assertEqual(exit_code_ret, exit_code)
    self.assertEqual(result_string_ret, self.result_string)

    self._mox.VerifyAll()

  def testRunCommandTimeout(self):
    cmd = ['a']
    self.PrepareRunCommandCall(cmd)
    self.mock_proc.poll().MultipleTimes().AndReturn(None)
    self.mock_proc.pid = 42
    self._mox.StubOutWithMock(os, 'chdir')
    os.chdir(mox.IsA(str)).AndReturn(None)
    os.chdir(mox.IsA(str)).AndReturn(None)
    self._mox.ReplayAll()

    self.CreateValidFile()
    runner = local_test_runner.LocalTestRunner(self.data_file_name)
    (exit_code, result_string) = runner._RunCommand(cmd, time_out=0.2)
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
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    self.mock_proc.stdin_handle.write(self.result_string)
    self.mock_proc.poll().AndReturn(None)
    url_helper.UrlOpen(self.ping_url).AndReturn('')
    self.mock_proc.poll().WithSideEffects(
        self.mock_proc.stdin_handle.close()).AndReturn(exit_code)
    url_helper.UrlOpen(self.ping_url).AndReturn('')
    self._mox.ReplayAll()

    # Make the delay between pings negative to ensure we get a ping
    # for this runner.
    local_test_runner.DELAY_BETWEEN_PINGS = -100

    self.CreateValidFile()
    runner = local_test_runner.LocalTestRunner(self.data_file_name)
    (exit_code_ret, result_string_ret) = runner._RunCommand(cmd, time_out=0)
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

    self._mox.StubOutWithMock(local_test_runner, 'downloader')
    data_dir = os.path.basename(self.runner.data_dir)
    local_test_runner.downloader.DownloadFile(
        mox.Regex(DATA_FILE_REGEX % (data_dir, self.local_file1)),
        data_url).AndReturn(None)
    local_test_runner.downloader.DownloadFile(
        mox.Regex(DATA_FILE_REGEX % (data_dir, self.local_file2)),
        data_url_with_local_name[0]).AndReturn(None)

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
    self._mox.StubOutWithMock(self.runner, '_DeleteFileOrDirectory')
    self.runner._DeleteFileOrDirectory(mox.IsA(str)).AndReturn(None)
    self.runner._DeleteFileOrDirectory(mox.IsA(str)).AndReturn(None)

    self._mox.ReplayAll()
    self.assertTrue(self.runner.DownloadAndExplodeData())
    self.runner.__del__()

    self._mox.VerifyAll()

  def PrepareRunTestsCall(self, decorate_output=None, results=None,
                          test_run_env=None, config_env=None, test_env=None):
    if not decorate_output:
      decorate_output = [False, False]
    if not results:
      results = [(0, 'success'), (0, 'success')]
    self.action1 = ['foo']
    self.action2 = ['bar', 'foo', 'bar']
    self.test_name1 = 'test1'
    self.test_name2 = 'test2'
    self.assertEqual(len(results), 2)
    test0_env = test1_env = None
    if test_env:
      assert len(test_env) == 2
      test0_env = test_env[0]
      test1_env = test_env[1]
    test_objects_data = [
        (self.test_name1, self.action1, decorate_output[0], test0_env, 0),
        (self.test_name2, self.action2, decorate_output[1], test1_env, 9)
    ]
    self.assertEqual(len(decorate_output), len(results))
    self.CreateValidFile(test_objects_data=test_objects_data,
                         test_run_env=test_run_env, config_env=config_env)
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
    self.runner._RunCommand(self.action1, 0, env=env).AndReturn((results[0]))

    if test1_env:
      env = dict(env_items + test1_env.items())
    self.runner._RunCommand(self.action2, 9, env=env).AndReturn((results[1]))

  def testRunTests(self):
    self.PrepareRunTestsCall()
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
    self.PrepareRunTestsCall(config_env=config_env, test_run_env=test_run_env,
                             test_env=[test0_env, test1_env])

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
    self.assertIn('[----------] %s summary' % self.test_run_name, result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[==========] 2 tests ran.', result_string)
    self.assertIn('[  PASSED  ] 1 tests.', result_string)
    self.assertIn('[  FAILED  ] 1 tests, listed below:', result_string)
    self.assertIn('[  FAILED  ] %s.%s' % (self.test_run_name, self.test_name1),
                  result_string)
    self.assertIn('1 FAILED TESTS', result_string)

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

      self.streamed_output += data['r']
      return True

    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    url_helper.UrlOpen(self.output_destination['url'],
                       mox.Func(ValidateUrlData),
                       1).AndReturn('Accepted')

    self._mox.ReplayAll()

    runner = local_test_runner.LocalTestRunner(self.data_file_name)
    (exit_code_ret, result_string_ret) = runner._RunCommand(cmd, time_out=0)
    self.assertEqual(exit_code_ret, exit_code)
    self.assertEqual(result_string_ret, 'No output!')
    self.assertEqual(self.streamed_output, ''.join(stdout_contents))

    self._mox.VerifyAll()

  def testPostFinalOutput(self):
    self.output_destination['url'] = 'http://blabla.com'
    self.result_url = None
    self.CreateValidFile()
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    data = {'n': self.test_run_name, 'c': self.config_name, 's': 'success',
            'r': ''}
    max_url_retries = 1
    url_helper.UrlOpen(self.output_destination['url'],
                       data.copy(),
                       max_url_retries).AndReturn('')
    data['s'] = 'failure'
    url_helper.UrlOpen('%s?1=2' % self.output_destination['url'],
                       data.copy(),
                       max_url_retries).AndReturn('')
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
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    max_url_retries = 1
    url_helper.UrlOpen(
        self.result_url,
        {'n': self.test_run_name, 'c': self.config_name,
         'x': ', '.join([str(i) for i in self.result_codes]),
         's': True, 'r': self.result_string, 'o': False},
        max_url_retries).AndReturn('')
    url_helper.UrlOpen(
        '%s?1=2' % self.result_url,
        {'n': self.test_run_name, 'c': self.config_name,
         'x': ', '.join([str(i) for i in self.result_codes]),
         's': False, 'r': self.result_string, 'o': False},
        max_url_retries).AndReturn('')
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
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    max_url_retries = 1
    url_helper.UrlOpen(
        self.result_url,
        {'n': self.test_run_name, 'c': self.config_name,
         'x': ', '.join([str(i) for i in self.result_codes]),
         's': True, 'r': self.result_string, 'o': False},
        max_url_retries).AndReturn(None)
    self._mox.ReplayAll()

    self.runner = local_test_runner.LocalTestRunner(
        self.data_file_name, max_url_retries=max_url_retries)
    self.assertFalse(self.runner.PublishResults(True, self.result_codes,
                                                self.result_string))

    self._mox.VerifyAll()

  def testPublishResultsHTTPS(self):
    self.CreateValidFile()
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    self.result_url = 'https://secure.com/result'

    max_url_retries = 1
    url_helper.UrlOpen(
        self.result_url,
        {'n': self.test_run_name, 'c': self.config_name,
         'x': ', '.join([str(i) for i in self.result_codes]),
         's': True, 'r': self.result_string, 'o': False},
        max_url_retries).AndReturn('')
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
    result_file = open(result_file_path)
    self.assertEqual(result_file.read(), self.result_string)
    result_file.close()

  def testPublishInternalErrors(self):
    self.CreateValidFile()
    self._mox.StubOutWithMock(url_helper, 'UrlOpen')
    exception_text = 'Bad MAD, no cookie!'
    max_url_retries = 1

    # We use this function to check if exception_text is properly published and
    # that the overwrite value is True.
    def ValidateInternalErrorsResult(url_data):
      if not url_data['o']:
        return False

      return exception_text in str(url_data)

    url_helper.UrlOpen(self.result_url, mox.Func(ValidateInternalErrorsResult),
                       max_url_retries).AndReturn('')
    self._mox.ReplayAll()

    self.runner = local_test_runner.LocalTestRunner(
        self.data_file_name, max_url_retries=max_url_retries)
    self.runner.TestLogException(exception_text)
    self.runner.PublishInternalErrors()

    self._mox.VerifyAll()

  def testShutdownOrReturn(self):
    self.CreateValidFile()

    self._mox.StubOutWithMock(local_test_runner, 'Restart')

    restart_message = 'Restarting machine'
    local_test_runner.Restart().AndRaise(local_test_runner.Error(
        restart_message))

    self._mox.ReplayAll()

    runner = local_test_runner.LocalTestRunner(self.data_file_name,
                                               restart_on_failure=False)
    return_value = 0

    # No test failures and restart on failure disabled.
    runner.success = True
    self.assertEqual(return_value, runner.ShutdownOrReturn(return_value))

    # Test failures with restart on failure disabled.
    self.assertEqual(return_value, runner.ShutdownOrReturn(return_value))

    # No test failures with restart on failure enabled.
    runner.restart_on_failure = True
    self.assertEqual(return_value, runner.ShutdownOrReturn(return_value))

    # Test failures with restart of feature enabled.
    runner.success = False
    self.assertRaisesRegexp(local_test_runner.Error,
                            restart_message,
                            runner.ShutdownOrReturn,
                            return_value)

    self._mox.VerifyAll()


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out or change for more information when debugging.
  logging.getLogger().setLevel(logging.CRITICAL)
  unittest.main()
