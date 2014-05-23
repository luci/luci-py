#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration test for the Swarming server and Swarming bot.

It starts both a Swarming server and a swarming bot and triggers mock tests to
ensure the system works end to end.
"""

import cookielib
import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib
import urllib2
import urlparse

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, APP_DIR)
sys.path.insert(0, os.path.join(APP_DIR, 'swarm_bot'))

import test_env
test_env.setup_test_env()

from common import bot_archive

import url_helper

from support import gae_sdk_utils

# The script to start the slave with. The python script is passed in because
# during tests, sys.executable was sometimes failing to find python.
# Starting up /path/to/slave_machine.py will be printed exactly twice. Once for
# the initial start up, a second time after the upgrade.
START_SLAVE = (
    "import os\n"
    "import subprocess\n"
    "import sys\n"
    "\n"
    "print('Starting up %(slave_script)s')\n"
    "sys.stdout.flush()\n"
    "cmd = [\n"
    "    sys.executable, '%(slave_script)s',\n"
    "    '-a', '%(server_address)s', '-p', '%(server_port)s',\n"
    "    '-d', '%(slave_directory)s',\n"
    "    '-l', '%(log_file)s',\n"
    "    '-v',\n"
    "    %(extra_args)s\n"
    "    '%(config_file)s',\n"
    "]\n"
    "sys.exit(subprocess.call(cmd))\n")


VERBOSE = False

# Timeout for slow operations.
TIMEOUT = 30


def is_port_free(host, port):
  """Returns True if the listening port number is available."""
  s = socket.socket()
  try:
    return s.connect_ex((host, port)) == 0
  finally:
    s.close()


def find_free_port(host, base_port):
  """Finds a listening port free to listen to."""
  while base_port < (2<<16):
    if not is_port_free(host, base_port):
      return base_port
    base_port += 1
  assert False, 'Failed to find an available port starting at %d' % base_port


def wait_for_server_up(server_url):
  started = time.time()
  while TIMEOUT > time.time() - started:
    try:
      urllib2.urlopen(server_url)
      return True
    except urllib2.URLError:
      time.sleep(0.1)
  return False


def get_admin_url(server_url):
  """"Returns url to login an admin user."""
  return urlparse.urljoin(
      server_url, '_ah/login?email=john@doe.com&admin=True&action=Login')


def whitelist_and_install_cookie_jar(server_url):
  """Whitelists the machine to be allowed to run tests."""
  cj = cookielib.CookieJar()
  opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
  # Make this opener the default so we can use url_helper.UrlOpen and still
  # have the cookies present.
  urllib2.install_opener(opener)
  opener.open(get_admin_url(server_url))
  opener.open(urlparse.urljoin(server_url, 'secure/change_whitelist'),
              urllib.urlencode({'a': True}))


def setup_bot(swarm_bot_dir, start_slave_content):
  """Setups the slave code in a temporary directory so it can be modified."""
  for i in bot_archive.FILES:
    dst = os.path.join(swarm_bot_dir, i)
    dstdir = os.path.dirname(dst)
    if not os.path.isdir(dstdir):
      os.mkdir(dstdir)
    src = os.path.join(APP_DIR, 'swarm_bot', i)
    shutil.copyfile(src, dst)

  # Remove the local test runner script to ensure the slave is out of date
  # and is updated.
  os.remove(os.path.join(swarm_bot_dir, 'local_test_runner.py'))

  with open(os.path.join(swarm_bot_dir, 'start_slave.py'), 'wb') as f:
    f.write(start_slave_content)


class SwarmingTestCase(unittest.TestCase):
  """Test case class for Swarming integration tests."""
  def setUp(self):
    super(SwarmingTestCase, self).setUp()
    self._server_proc = None
    self._bot_proc = None
    self.tmpdir = tempfile.mkdtemp(prefix='swarming')
    self.swarm_bot_dir = os.path.join(self.tmpdir, 'bot')
    self.log_dir = os.path.join(self.tmpdir, 'logs')
    os.mkdir(self.swarm_bot_dir)
    os.mkdir(self.log_dir)

    server_addr = 'http://localhost'
    server_port = find_free_port('localhost', 9000)
    self.server_url = '%s:%s' % (server_addr, server_port)

    gaedb_dir = os.path.join(self.tmpdir, 'gaedb')
    os.mkdir(gaedb_dir)
    cmd = [
      os.path.join(gae_sdk_utils.find_gae_sdk(), 'dev_appserver.py'),
      '--port', str(server_port),
      '--admin_port', str(find_free_port('localhost', server_port + 1)),
      '--storage', gaedb_dir,
      '--skip_sdk_update_check', 'True',
      # Note: The random policy will provide the same consistency every time
      # the test is run because the random generator is always given the
      # same seed.
      '--datastore_consistency_policy', 'random',
      '--log_level', 'debug' if VERBOSE else 'info',
      APP_DIR,
    ]

    # Start the server first since it is a tad slow to start.
    # TODO(maruel): Use CREATE_NEW_PROCESS_GROUP on Windows.
    with open(os.path.join(self.log_dir, 'server.log'), 'wb') as f:
      self._server_proc = subprocess.Popen(
          cmd, cwd=self.tmpdir, preexec_fn=os.setsid,
          stdout=f, stderr=subprocess.STDOUT)

    start_slave_content = START_SLAVE % {
      'config_file': os.path.join(APP_DIR, 'tests', 'machine_config.txt'),
      'extra_args': "'-v'," if VERBOSE else '',
      'log_file': os.path.join(self.log_dir, 'slave_machine.log'),
      'server_address': server_addr,
      'server_port': server_port,
      'slave_directory': self.swarm_bot_dir,
      'slave_script': os.path.join(self.swarm_bot_dir, 'slave_machine.py'),
    }
    setup_bot(self.swarm_bot_dir, start_slave_content)

    self.assertTrue(
        wait_for_server_up(self.server_url), 'Failed to start server')
    whitelist_and_install_cookie_jar(self.server_url)

    # Upload the start slave script to the server.
    url_helper.UrlOpen(
        urlparse.urljoin(self.server_url, 'upload_start_slave'),
        files=[('script', 'script', start_slave_content)], method='POSTFORM')

    # Start the slave machine script to start polling for tests.
    cmd = [sys.executable, os.path.join(self.swarm_bot_dir, 'start_slave.py')]
    if VERBOSE:
      cmd.append('-v')
    with open(os.path.join(self.log_dir, 'start_slave.log'), 'wb') as f:
      self._bot_proc = subprocess.Popen(
          cmd, cwd=self.swarm_bot_dir, preexec_fn=os.setsid,
          stdout=f, stderr=subprocess.STDOUT)

  def tearDown(self):
    # Kill bot, kill server, print logs if failed, delete tmpdir, call super.
    try:
      try:
        try:
          try:
            if self._bot_proc:
              if self._bot_proc.poll() is None:
                try:
                  # TODO(maruel): os.killpg() doesn't exist on Windows.
                  os.killpg(self._bot_proc.pid, signal.SIGKILL)
                  self._bot_proc.wait()
                except OSError:
                  pass
              else:
                # The bot should have quit normally when it self-updates.
                self.assertEqual(0, self._bot_proc.returncode)
          finally:
            if self._server_proc and self._server_proc.poll() is None:
              try:
                os.killpg(self._server_proc.pid, signal.SIGKILL)
                self._server_proc.wait()
              except OSError:
                pass
        finally:
          if self.has_failed() or VERBOSE:
            # Print out the logs before deleting them.
            for i in sorted(os.listdir(self.log_dir)):
              sys.stderr.write('\n%s:\n' % i)
              with open(os.path.join(self.log_dir, i), 'rb') as f:
                for l in f:
                  sys.stderr.write('  ' + l)
      finally:
        # In the end, delete the temporary directory.
        shutil.rmtree(self.tmpdir)
    finally:
      super(SwarmingTestCase, self).tearDown()

  def has_failed(self):
    # pylint: disable=E1101
    return not self._resultForDoCleanups.wasSuccessful()

  def get_swarm_files(self):
    swarm_files = []
    # Location of the test files.
    test_data_dir = os.path.join(APP_DIR, 'tests', 'test_files')
    for dirpath, _, filenames in os.walk(test_data_dir):
      for filename in filenames:
        if os.path.splitext(filename)[1].lower() == '.swarm':
          swarm_files.append(os.path.join(dirpath, filename))
    self.assertTrue(swarm_files, 'No swarm files found in %s' % test_data_dir)
    logging.info('Found: %s', ' '.join(swarm_files))
    return swarm_files

  def trigger_swarm_file(self, swarm_file, running_tests, tests_to_cancel):
    logging.info('trigger_swarm_file(%s)', swarm_file)
    with open(swarm_file, 'rb') as f:
      request = f.read()

    # Trigger the test.
    data = urllib.urlencode({'request': request})
    url = urlparse.urljoin(self.server_url, 'test')
    test_keys = json.load(urllib2.urlopen(url, data=data))

    current_test_keys = []
    for test_key in test_keys['test_keys']:
      current_test_keys.append(test_key['test_key'])
      if 'To Cancel' in test_keys['test_case_name']:
        tests_to_cancel.append(test_key)
      else:
        running_tests.append(test_key)
      logging.info('Config: %s, index: %s/%s, test key: %s',
                    test_key['config_name'],
                    int(test_key['instance_index']) + 1,
                    test_key['num_instances'],
                    test_key['test_key'])

    # Make sure that we can actually find the keys from just the test names.
    # Loop because the index is eventually consistent, so the initial request(s)
    # could fail and return nothing.
    data = urllib.urlencode({'name': test_keys['test_case_name']})
    for i in xrange(10):
      # Append the data to the url so the request is a GET request as required.
      url = urlparse.urljoin(
          self.server_url, 'get_matching_test_cases') + '?' + data
      try:
        matching_keys = json.load(urllib2.urlopen(url))
      except urllib2.HTTPError:
        matching_keys = None
      if matching_keys:
        break
      # Last sleep is 2 seconds.
      time.sleep(0.1 * (2*(i+1)))
    self.assertEqual(set(matching_keys), set(current_test_keys))

  def test_integration(self):
    # Sends a series of .swarm files to the server.
    running_tests = []
    tests_to_cancel = []
    for swarm_file in self.get_swarm_files():
      self.trigger_swarm_file(swarm_file, running_tests, tests_to_cancel)

    # Cancel all the tests that are suppose to be cancelled.
    url = urlparse.urljoin(self.server_url, 'secure/cancel')
    for test in tests_to_cancel:
      data = urllib.urlencode({'r': test['test_key']})
      resp = urllib2.urlopen(url, data=data).read()
      self.assertEqual('Runner canceled.', resp)

    # The slave machine is running along with this test. Thus it may take
    # some time before all the tests complete. We will keep polling the results
    # with delays between them. If after TIMEOUT seconds all the tests are still
    # not completed, we report a failure.
    triggered_retry = False
    started = time.time()
    while running_tests and TIMEOUT > time.time() - started:
      for running_test_key in running_tests[:]:
        url = urlparse.urljoin(
            self.server_url,
            'secure/get_result?r=' + running_test_key['test_key'])
        response = urllib2.urlopen(url).read()
        try:
          results = json.loads(response)
        except ValueError:
          self.fail('Failed to parse: %r' % response)
        if not results['exit_codes']:
          # The test hasn't finished yet
          continue

        logging.info('Test done for %s', running_test_key['config_name'])
        if '0 FAILED TESTS' not in results['output']:
          self.fail('Test failed.\n%s' % results)

        # If we haven't retried a runner yet, do that with this runner.
        if not triggered_retry:
          logging.info('Retrying test %s', running_test_key['test_key'])
          url = urlparse.urljoin(self.server_url, 'secure/retry')
          data = urllib.urlencode({'r': running_test_key['test_key']})
          urllib2.urlopen(url, data=data)
          triggered_retry = True
        else:
          running_tests.remove(running_test_key)

      if running_tests:
        # Throttle query rate in verbose to reduce the noise.
        time.sleep(2 if VERBOSE else 0.1)
    self.assertEqual([], running_tests)


if __name__ == '__main__':
  logging.disable(logging.CRITICAL)
  VERBOSE = '-v' in sys.argv
  logging.basicConfig(level=logging.INFO if VERBOSE else logging.ERROR)
  if VERBOSE:
    unittest.TestCase.maxDiff = None
  unittest.main()
