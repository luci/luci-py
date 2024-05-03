#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""High level test for Primary <-> Replica linking.

It launches two local services (Primary and Replica) via dev_appserver and
registers a replica to receive updates from a primary service.

Note: replication of the AuthDB to the replica is no longer included in this
smoke test, as replication is handled by another service entirely (Auth Service
v2, the Go rewrite).
"""

import logging
import os
import shutil
import sys
import tempfile
import unittest

from tool_support import gae_sdk_utils
from tool_support import local_app

# /appengine/auth_service/.
APP_DIR = os.path.dirname(os.path.abspath(__file__))
# /appengine/auth_service/test_replica_app/.
REPLICA_APP_DIR = os.path.join(APP_DIR, 'test_replica_app')


class ReplicationTest(unittest.TestCase):
  def setUp(self):
    super(ReplicationTest, self).setUp()
    self.root = tempfile.mkdtemp(prefix='replication_smoke_test')
    self.auth_service = local_app.LocalApplication(
        APP_DIR, 9500, False, self.root)
    self.replica = local_app.LocalApplication(
        REPLICA_APP_DIR, 9600, False, self.root)
    # Launch both first, only then wait for them to come online.
    apps = [self.auth_service, self.replica]
    for app in apps:
      app.start()
    for app in apps:
      app.ensure_serving()
      app.client.login_as_admin('test@example.com')

  def tearDown(self):
    try:
      self.auth_service.stop()
      self.replica.stop()
      shutil.rmtree(self.root)
      if self.has_failed() or self.maxDiff is None:
        self.auth_service.dump_log()
        self.replica.dump_log()
    finally:
      super(ReplicationTest, self).tearDown()

  def has_failed(self):
    # pylint: disable=E1101
    return not self._resultForDoCleanups.wasSuccessful()

  def test_link_replica_to_primary(self):
    """Tests linking replica to primary."""
    logging.info('Linking replica to primary')

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
    replica_state = self.replica.client.json_request(
        '/auth/api/v1/server/state').body
    self.assertEqual('replica', replica_state['mode'])
    self.assertEqual(
        self.auth_service.app_id,
        replica_state['replication_state']['primary_id'])
    self.assertEqual(
        self.auth_service.url,
        replica_state['replication_state']['primary_url'])


if __name__ == '__main__':
  gae_sdk_utils.setup_gae_env()
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
