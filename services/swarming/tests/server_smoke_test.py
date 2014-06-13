#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration test for the Swarming server and Swarming bot.

It starts both a Swarming server and a swarming bot and triggers mock tests to
ensure the system works end to end.
"""

import cookielib
import glob
import json
import logging
import os
import re
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
BOT_DIR = os.path.join(APP_DIR, 'swarm_bot')
sys.path.insert(0, APP_DIR)
sys.path.insert(0, BOT_DIR)

import test_env
test_env.setup_test_env()

import url_helper
from server import bot_archive
from support import gae_sdk_utils


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
  # smoke-test@example.com is added to admin group in bootstrap_dev_server_acls.
  return urlparse.urljoin(
      server_url,
      '_ah/login?email=smoke-test@example.com&admin=True&action=Login')


def install_cookie_jar(server_url):
  """Whitelists the machine to be allowed to run tests."""
  cj = cookielib.CookieJar()
  opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
  # Make this opener the default so we can use url_helper.UrlOpen and still
  # have the cookies present.
  urllib2.install_opener(opener)
  # Do a dev_appserver login.
  opener.open(get_admin_url(server_url))

  # Get the XSRF token.
  req = urllib2.Request(
      urlparse.urljoin(server_url, '/auth/api/v1/accounts/self/xsrf_token'), '')
  req.add_header('X-XSRF-Token-Request', '1')
  # Add it to every request by default. It does no harm even if not needed.
  xsrf_token = json.load(opener.open(req))['xsrf_token']
  opener.addheaders.append(('X-XSRF-Token', xsrf_token))


def setup_bot(swarming_bot_dir, host):
  """Setups the slave code in a temporary directory so it can be modified."""
  with open(os.path.join(BOT_DIR, 'start_slave.py'), 'rb') as f:
    start_slave_content = f.read()

  # Creates a functional but invalid swarming_bot.zip.
  zip_content = bot_archive.get_swarming_bot_zip(
      BOT_DIR, host, {'start_slave.py': start_slave_content, 'invalid': 'foo'})

  swarming_bot_zip = os.path.join(swarming_bot_dir, 'swarming_bot.zip')
  with open(swarming_bot_zip, 'wb') as f:
    f.write(zip_content)
  logging.info(
      'Generated %s (%d bytes)',
      swarming_bot_zip, os.stat(swarming_bot_zip).st_size)


class SwarmingTestCase(unittest.TestCase):
  """Test case class for Swarming integration tests."""
  def setUp(self):
    super(SwarmingTestCase, self).setUp()
    self._server_proc = None
    self._bot_proc = None
    self.tmpdir = tempfile.mkdtemp(prefix='swarming')
    self.swarming_bot_dir = os.path.join(self.tmpdir, 'swarming_bot')
    self.log_dir = os.path.join(self.tmpdir, 'logs')
    os.mkdir(self.swarming_bot_dir)
    os.mkdir(self.log_dir)

    server_addr = 'http://localhost'
    server_port = find_free_port('localhost', 9000)
    self.server_url = '%s:%s' % (server_addr, server_port)

    # TODO(maruel): Use tools/run_dev_appserver.py.
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
    server_log = os.path.join(self.log_dir, 'server.log')
    logging.info('Server log: %s', server_log)
    with open(server_log, 'wb') as f:
      f.write('Running: %s\n' % cmd)
      f.flush()
      self._server_proc = subprocess.Popen(
          cmd, cwd=self.tmpdir, preexec_fn=os.setsid,
          stdout=f, stderr=subprocess.STDOUT)

    setup_bot(self.swarming_bot_dir, self.server_url)

    self.assertTrue(
        wait_for_server_up(self.server_url), 'Failed to start server')

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
            for i in sorted(
                glob.glob(os.path.join(self.swarming_bot_dir, '*.log'))):
              sys.stderr.write('\n%s:\n' % i)
              with open(os.path.join(self.log_dir, i), 'rb') as f:
                for l in f:
                  sys.stderr.write('  ' + l)
      finally:
        # In the end, delete the temporary directory.
        shutil.rmtree(self.tmpdir)
    finally:
      super(SwarmingTestCase, self).tearDown()

  def finish_setup(self):
    """Uploads slave code and starts a slave.

    Should be called from test_* method (not from setUp), since if setUp fails
    tearDown is not getting called (and finish_setup can fail because it uses
    various server endpoints).
    """
    install_cookie_jar(self.server_url)

    # Upload the start slave script to the server. Uploads the exact code in the
    # tree + a new line. This invalidates the bot's code.
    with open(os.path.join(BOT_DIR, 'start_slave.py'), 'rb') as f:
      start_slave_content = f.read() + '\n'
    url_helper.UrlOpen(
        urlparse.urljoin(self.server_url, '/restricted/upload_start_slave'),
        files=[('script', 'script', start_slave_content)], method='POSTFORM')

    # Start the slave machine script to start polling for tests.
    cmd = [
      sys.executable,
      os.path.join(self.swarming_bot_dir, 'swarming_bot.zip'),
      'start_slave',
    ]
    if VERBOSE:
      cmd.append('-v')
    with open(os.path.join(self.log_dir, 'start_slave_stdout.log'), 'wb') as f:
      f.write('Running: %s\n' % cmd)
      f.flush()
      self._bot_proc = subprocess.Popen(
          cmd, cwd=self.swarming_bot_dir, preexec_fn=os.setsid,
          stdout=f, stderr=subprocess.STDOUT)

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
    # TODO(maruel): Stop hacking this up and use swarming.py trigger.
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
    self.finish_setup()

    # Sends a series of .swarm files to the server.
    running_tests = []
    tests_to_cancel = []
    for swarm_file in self.get_swarm_files():
      self.trigger_swarm_file(swarm_file, running_tests, tests_to_cancel)

    # Cancel all the tests that are suppose to be cancelled.
    url = urlparse.urljoin(self.server_url, '/restricted/cancel')
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
            self.server_url, '/get_result?r=' + running_test_key['test_key'])
        response = urllib2.urlopen(url).read()
        try:
          results = json.loads(response)
        except ValueError:
          self.fail('Failed to parse: %r' % response)
        if not results['exit_codes']:
          # The test hasn't finished yet
          continue

        logging.info('Test done for %s', running_test_key['config_name'])
        expected = (
          re.escape('[==========] Running: dir\n') +
          re.escape('test_run.json\n') +
          re.escape('\n') +
          r'\(Step: \d+ ms\)' + '\n' +
          re.escape('[==========] Running: dir\n') +
          re.escape('test_run.json\n') +
          re.escape('\n') +
          r'\(Step\: \d+ ms\)' + '\n' +
          r'\(Total\: \d+ ms\)')
        self.assertTrue(
            re.match(expected, results['output']), repr(results['output']))

        # If we haven't retried a runner yet, do that with this runner.
        if not triggered_retry:
          logging.info('Retrying test %s', running_test_key['test_key'])
          url = urlparse.urljoin(self.server_url, '/restricted/retry')
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
  VERBOSE = '-v' in sys.argv
  logging.basicConfig(level=logging.INFO if VERBOSE else logging.ERROR)
  if VERBOSE:
    unittest.TestCase.maxDiff = None
  unittest.main()
