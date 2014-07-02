#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""High level test for Primary <-> Replica replication logic.

It launches two local services (Primary and Replica) via dev_appserver and sets
up auth db replication between them.
"""

import collections
import cookielib
import ctypes
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib2


# services/auth_service/tests/.
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
# service/auth_service/
SERVICE_APP_DIR = os.path.dirname(THIS_DIR)
# services/auth_service/tests/replica_app/.
REPLICA_APP_DIR = os.path.join(THIS_DIR, 'replica_app')


# Add services/components/ directory, to import 'support'.
sys.path.append(os.path.join(os.path.dirname(SERVICE_APP_DIR), 'components'))
from support import gae_sdk_utils


def terminate_with_parent():
  """Sets up current process to receive SIGTERM when its parent dies.

  Works on Linux only. On Win and Mac it's noop.
  """
  try:
    libc = ctypes.CDLL('libc.so.6')
  except OSError:
    return
  PR_SET_PDEATHSIG = 1
  SIGTERM = 15
  try:
    libc.prctl(PR_SET_PDEATHSIG, SIGTERM)
  except AttributeError:
    return


def is_port_free(host, port):
  """Returns True if the listening port number is available."""
  s = socket.socket()
  try:
    # connect_ex returns 0 on success (i.e. port is being listened to).
    return bool(s.connect_ex((host, port)))
  finally:
    s.close()


def find_free_ports(host, base_port, count):
  """Finds several consecutive listening ports free to listen to."""
  while base_port < (2<<16):
    candidates = range(base_port, base_port + count)
    if all(is_port_free(host, port) for port in candidates):
      return candidates
    base_port += len(candidates)
  assert False, (
      'Failed to find %d available ports starting at %d' % (count, base_port))


class DevServerApplication(object):
  """GAE application running via dev_appserver.py."""

  def __init__(self, app_dir, base_port):
    self._app = gae_sdk_utils.Application(app_dir)
    self._base_port = base_port
    self._client = None
    self._exit_code = None
    self._log = None
    self._port = None
    self._proc = None
    self._serving = False
    self._temp_root = None

  @property
  def app_id(self):
    """Application ID as specified in app.yaml."""
    return self._app.app_id

  @property
  def port(self):
    """Main HTTP port that serves requests to 'default' module.

    Valid only after app has started.
    """
    return self._port

  @property
  def url(self):
    """Host URL."""
    return 'http://localhost:%d' % self._port

  @property
  def client(self):
    """HttpClient that can be used to make requests to the instance."""
    return self._client

  def start(self):
    """Starts dev_appserver process."""
    assert not self._proc, 'Already running'

    # Clear state.
    self._client = None
    self._exit_code = None
    self._log = None
    self._serving = False

    # Find available ports, one per module + one for app admin.
    free_ports = find_free_ports(
        'localhost', self._base_port, len(self._app.modules) + 1)
    self._port = free_ports[0]

    # Create temp directories where dev_server keeps its state.
    self._temp_root = tempfile.mkdtemp(prefix=self.app_id)
    os.makedirs(os.path.join(self._temp_root, 'storage'))

    # Launch the process.
    log_file = os.path.join(self._temp_root, 'dev_appserver.log')
    logging.info(
        'Launching %s at %s, log is %s', self.app_id, self.url, log_file)
    with open(log_file, 'wb') as f:
      self._proc = self._app.spawn_dev_appserver(
          [
            '--port', str(self._port),
            '--admin_port', str(free_ports[-1]),
            '--storage_path', os.path.join(self._temp_root, 'storage'),
            '--automatic_restart', 'no',
            # Note: The random policy will provide the same consistency every
            # time the test is run because the random generator is always given
            # the same seed.
            '--datastore_consistency_policy', 'random',
          ],
          stdout=f,
          stderr=subprocess.STDOUT,
          preexec_fn=terminate_with_parent)

    # Create a client that can talk to the service.
    self._client = HttpClient(self.url)

  def ensure_serving(self, timeout=5):
    """Waits for the service to start responding."""
    if self._serving:
      return
    if not self._proc:
      self.start()
    logging.info('Waiting for %s to become ready...', self.app_id)
    deadline = time.time() + timeout
    alive = False
    while self._proc.poll() is None and time.time() < deadline:
      try:
        urllib2.urlopen(self.url + '/_ah/warmup')
        alive = True
        break
      except urllib2.URLError as exc:
        if isinstance(exc, urllib2.HTTPError):
          alive = True
          break
      time.sleep(0.05)
    if not alive:
      logging.error('Service %s did\'t come online', self.app_id)
      self.stop()
      self.dump_log()
      raise Exception('Failed to start %s' % self.app_id)
    logging.info('Service %s is ready.', self.app_id)
    self._serving = True

  def stop(self):
    """Stops dev_appserver, collects its log."""
    if not self._proc:
      return
    self._exit_code = self._proc.poll()
    try:
      logging.info('Stopping %s', self.app_id)
      if self._proc.poll() is None:
        try:
          self._proc.terminate()
        except OSError:
          pass
        deadline = time.time() + 5
        while self._proc.poll() is None and time.time() < deadline:
          time.sleep(0.05)
        self._exit_code = self._proc.poll()
        if self._exit_code is None:
          logging.error('Leaking PID %d', self._proc.pid)
    finally:
      with open(os.path.join(self._temp_root, 'dev_appserver.log'), 'r') as f:
        self._log = f.read()
      shutil.rmtree(self._temp_root)
      self._client = None
      self._port = None
      self._proc = None
      self._serving = False
      self._temp_root = None

  def dump_log(self):
    """Prints dev_appserver log to stderr, works only if app is stopped."""
    assert self._log is not None
    print >> sys.stderr, '-' * 60
    print >> sys.stderr, 'dev_appserver.py log for %s' % self.app_id
    print >> sys.stderr, '-' * 60
    print >> sys.stderr, self._log
    print >> sys.stderr, '-' * 60


class HttpClient(object):
  """Makes HTTP requests to some instance of dev_appserver."""

  # Return value of request(...) and json_request.
  HttpResponse = collections.namedtuple(
      'HttpResponse', ['http_code', 'body', 'headers'])

  def __init__(self, url):
    self._url = url
    self._opener = urllib2.build_opener(
        urllib2.HTTPCookieProcessor(cookielib.CookieJar()))
    self._xsrf_token = None

  def login_as_admin(self, user='test@example.com'):
    """Performs dev_appserver login as admin, modifies cookies."""
    self.request('/_ah/login?email=%s&admin=True&action=Login' % user)

  def request(self, resource, body=None, headers=None):
    """Sends HTTP request."""
    if not resource.startswith(self._url):
      assert resource.startswith('/')
      resource = self._url + resource
    req = urllib2.Request(resource, body, headers=(headers or {}))
    resp = self._opener.open(req)
    return self.HttpResponse(resp.getcode(), resp.read(), resp.info())

  def json_request(self, resource, body=None, headers=None):
    """Sends HTTP request and returns deserialized JSON."""
    if body is not None:
      body = json.dumps(body)
      headers = (headers or {}).copy()
      headers['Content-Type'] = 'application/json; charset=UTF-8'
    resp = self.request(resource, body, headers=headers)
    return self.HttpResponse(
        resp.http_code, json.loads(resp.body), resp.headers)

  @property
  def xsrf_token(self):
    """Returns XSRF token for the service, fetching it if necessary."""
    if self._xsrf_token is None:
      resp = self.json_request(
          '/auth/api/v1/accounts/self/xsrf_token',
          body={},
          headers={'X-XSRF-Token-Request': '1'})
      self._xsrf_token = resp.body['xsrf_token'].encode('ascii')
    return self._xsrf_token


class ReplicationTest(unittest.TestCase):
  def setUp(self):
    super(ReplicationTest, self).setUp()
    self.auth_service = DevServerApplication(SERVICE_APP_DIR, 9500)
    self.replica = DevServerApplication(REPLICA_APP_DIR, 9600)
    # Launch both first, only then wait for them to come online.
    apps = [self.auth_service, self.replica]
    for app in apps:
      app.start()
    for app in apps:
      app.ensure_serving()
      app.client.login_as_admin()

  def tearDown(self):
    self.auth_service.stop()
    self.replica.stop()
    if self.has_failed():
      self.auth_service.dump_log()
      self.replica.dump_log()
    super(ReplicationTest, self).tearDown()

  def has_failed(self):
    # pylint: disable=E1101
    return not self._resultForDoCleanups.wasSuccessful()

  def test_replication(self):
    """Tests Replica <-> Primary linking flow."""
    # Verify initial state: no linked services on primary.
    linked_services = self.auth_service.client.json_request(
        '/auth_service/api/v1/services').body
    self.assertEqual([], linked_services['services'])

    # Step 1. Generate a link to associate |replica| to |auth_service|.
    app_id = '%s@localhost:%d' % (self.replica.app_id, self.replica.port)
    response = self.auth_service.client.json_request(
        resource='/auth_service/api/v1/services/%s/linking_url' % app_id,
        body={},
        headers={'X-XSRF-Token': self.auth_service.client.xsrf_token})
    self.assertEqual(201, response.http_code)

    # URL points to HTML page on the replica.
    linking_url = response.body['url']
    self.assertTrue(
        linking_url.startswith('%s/auth/link?t=' % self.replica.url))

    # Step 2. "Click" this link. It should associates Replica with Primary via
    # behind-the-scenes service <-> service URLFetch call.
    response = self.replica.client.request(
        resource=linking_url,
        body='',
        headers={'X-XSRF-Token': self.replica.client.xsrf_token})
    self.assertEqual(200, response.http_code)
    self.assertIn('Success!', response.body)

    # Verify primary knows about new replica now.
    linked_services = self.auth_service.client.json_request(
        '/auth_service/api/v1/services').body
    self.assertEqual(1, len(linked_services['services']))
    service = linked_services['services'][0]
    self.assertEqual(self.replica.app_id, service['app_id'])
    self.assertEqual(self.replica.url, service['replica_url'])

    # Verify replica knows about the primary now.
    # TODO(vadimsh): Test once implemented.


if __name__ == '__main__':
  sdk_path = gae_sdk_utils.find_gae_sdk()
  if not sdk_path:
    print >> sys.stderr, 'Couldn\'t find GAE SDK.'
    sys.exit(1)
  gae_sdk_utils.setup_gae_sdk(sdk_path)

  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
