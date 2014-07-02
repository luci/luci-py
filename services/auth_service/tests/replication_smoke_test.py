#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""High level test for Primary <-> Replica replication logic.

It launches two local services (Primary and Replica) via dev_appserver and sets
up auth db replication between them.
"""

import logging
import os
import sys
import unittest


# services/auth_service/tests/.
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
# service/auth_service/
SERVICE_APP_DIR = os.path.dirname(THIS_DIR)
# services/auth_service/tests/replica_app/.
REPLICA_APP_DIR = os.path.join(THIS_DIR, 'replica_app')

# Add services/components/ directory, to import 'support'.
sys.path.append(os.path.join(os.path.dirname(SERVICE_APP_DIR), 'components'))

from support import gae_sdk_utils
from support import local_app


class ReplicationTest(unittest.TestCase):
  def setUp(self):
    super(ReplicationTest, self).setUp()
    self.auth_service = local_app.LocalApplication(SERVICE_APP_DIR, 9500)
    self.replica = local_app.LocalApplication(REPLICA_APP_DIR, 9600)
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
