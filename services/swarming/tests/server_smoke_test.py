#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration test for the Swarming server and Swarming bot.

It starts both a Swarming server and a swarming bot and triggers mock tests to
ensure the system works end to end.
"""

import glob
import json
import logging
import os
import shutil
import signal
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
from support import local_app


# Modified in "if __name__ == '__main__'" below.
VERBOSE = False

# Timeout for slow operations.
TIMEOUT = 30


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
    self._bot_proc = None
    self._server = local_app.LocalApplication(APP_DIR, 9050)

    self.tmpdir = tempfile.mkdtemp(prefix='swarming')
    self.swarming_bot_dir = os.path.join(self.tmpdir, 'swarming_bot')
    self.log_dir = os.path.join(self.tmpdir, 'logs')
    os.mkdir(self.swarming_bot_dir)
    os.mkdir(self.log_dir)

    # Start the server first since it is a tad slow to start.
    self._server.start()
    setup_bot(self.swarming_bot_dir, self.server_url)
    self._server.ensure_serving()

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
            self._server.stop()
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
            self._server.dump_log()
      finally:
        # In the end, delete the temporary directory.
        shutil.rmtree(self.tmpdir)
    finally:
      super(SwarmingTestCase, self).tearDown()

  @property
  def server_url(self):
    """Localhost URL of swarming dev_appserver."""
    return self._server.url

  @property
  def is_bot_alive(self):
    """Returns True if bot process is still running."""
    return self._bot_proc and self._bot_proc.poll() is None

  def finish_setup(self):
    """Uploads slave code and starts a slave.

    Should be called from test_* method (not from setUp), since if setUp fails
    tearDown is not getting called (and finish_setup can fail because it uses
    various server endpoints).
    """
    # Obtain admin cookie.
    self._server.client.login_as_admin('smoke-test@example.com')

    # Replace default URL opener with one that has admin cookies, so we can use
    # url_helper.net.url_read and still have the cookies present. Add XSRF token
    # to every request. It does no harm even if not needed.
    opener = self._server.client.url_opener
    opener.addheaders.append(('X-XSRF-Token', self._server.client.xsrf_token))
    urllib2.install_opener(opener)

    # Upload the start slave script to the server. Uploads the exact code in the
    # tree + a new line. This invalidates the bot's code.
    with open(os.path.join(BOT_DIR, 'start_slave.py'), 'rb') as f:
      start_slave_content = f.read() + '\n'
    res = url_helper.net.url_read(
        urlparse.urljoin(self.server_url, 'restricted/upload_start_slave'),
        data={'script': start_slave_content})
    self.assertTrue(res)

    # Start the slave machine script to start polling for tests.
    cmd = [
      sys.executable,
      os.path.join(self.swarming_bot_dir, 'swarming_bot.zip'),
      'start_slave',
    ]
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
    url = urlparse.urljoin(self.server_url, 'restricted/cancel')
    for test in tests_to_cancel:
      data = urllib.urlencode({'r': test['test_key']})
      resp = urllib2.urlopen(url, data=data).read()
      self.assertEqual('Runner canceled.', resp)

    # The slave machine is running along with this test. Thus it may take
    # some time before all the tests complete. We will keep polling the results
    # with delays between them. If after TIMEOUT seconds all the tests are still
    # not completed, we report a failure.
    started = time.time()
    deadline = started + TIMEOUT
    while running_tests and time.time() < deadline and self.is_bot_alive:
      for running_test_key in running_tests[:]:
        url = urlparse.urljoin(
            self.server_url, 'get_result?r=' + running_test_key['test_key'])
        response = urllib2.urlopen(url).read()
        try:
          results = json.loads(response)
        except ValueError:
          self.fail('Failed to parse: %r' % response)
        if not results['exit_codes']:
          # The test hasn't finished yet
          continue

        logging.info('Test done for %s', running_test_key['config_name'])
        expected = u'test_run.json\n\ntest_run.json\n'
        self.assertEqual(expected, results['output'])
        running_tests.remove(running_test_key)

      if running_tests:
        # Throttle query rate in verbose to reduce the noise.
        time.sleep(2 if VERBOSE else 0.1)

    if running_tests and not self.is_bot_alive:
      self.fail('Swarm bot failed to start or unexpectedly died')
    self.assertEqual([], running_tests)


if __name__ == '__main__':
  VERBOSE = '-v' in sys.argv
  logging.basicConfig(level=logging.INFO if VERBOSE else logging.ERROR)
  if VERBOSE:
    unittest.TestCase.maxDiff = None
  unittest.main()
