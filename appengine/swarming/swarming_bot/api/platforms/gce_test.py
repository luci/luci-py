#!/usr/bin/env vpython3
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import json
import logging
import sys
import time
import unittest

from nose2.tools import params
import mock

import test_env_platforms
test_env_platforms.setup_test_env()

from depot_tools import auto_stub

from utils import tools

from api.platforms import gce


class TestGCE(auto_stub.TestCase):
  _METADATA = {
      u'project': {
          u'projectId': u'test-proj',
          u'numericProjectId': 123456
      },
      u'instance': {
          u'cpuPlatform': u'Intel Broadwell',
          u'scheduling': {
              u'automaticRestart': u'TRUE',
              u'preemptible': u'FALSE',
              u'onHostMaintenance': u'MIGRATE'
          },
          u'hostname': u'host.name',
          u'machineType': u'projects/123456/machineTypes/n1-standard-8',
          u'description': u'',
          u'zone': u'projects/123456/zones/us-west1-c',
          u'maintenanceEvent': u'NONE',
          u'image': u'projects/test-proj/global/images/test-image',
          u'disks': [{
              u'interface': u'SCSI',
              u'deviceName': u'persistent-disk-0',
              u'type': u'PERSISTENT',
              u'mode': u'READ_WRITE',
              u'index': 0
          }],
          u'licenses': [{
              u'id': u'1000001'
          }],
          u'preempted': u'FALSE',
          u'remainingCpuTime': -1,
          u'virtualClock': {
              u'driftToken': u'0'
          },
          u'legacyEndpointAccess': {
              u'0.1': 0,
              u'v1beta1': 0
          },
          u'serviceAccounts': {
              u'default': {
                  u'scopes': [
                      u'https://www.googleapis.com/auth/compute',
                      u'https://www.googleapis.com/auth/logging.write',
                      u'https://www.googleapis.com/auth/monitoring',
                  ],
                  u'email': u'test@test-proj.iam.gserviceaccount.com',
                  u'aliases': [u'default']
              },
              u'test@test-proj.iam.gserviceaccount.com': {
                  u'scopes': [
                      u'https://www.googleapis.com/auth/compute',
                      u'https://www.googleapis.com/auth/logging.write',
                      u'https://www.googleapis.com/auth/monitoring',
                  ],
                  u'email': u'test@test-proj.iam.gserviceaccount.com',
                  u'aliases': [u'default']
              }
          },
          u'networkInterfaces': [],
          u'guestAttributes': {},
          u'id': 987654,
          u'tags': [u'tag1', u'tag2'],
          u'name': u'test-inst'
      },
      u'oslogin': {
          u'authenticate': {
              u'sessions': {}
          }
      }
  }

  def setUp(self):
    super(TestGCE, self).tearDown()
    self.mock_get_metadata = mock.patch(
        'api.platforms.gce.get_metadata', return_value=self._METADATA).start()

  def tearDown(self):
    super(TestGCE, self).tearDown()
    mock.patch.stopall()
    tools.clear_cache_all()

  def test_oauth2_available_scopes(self):
    self.assertEqual(gce.oauth2_available_scopes(), [
        'https://www.googleapis.com/auth/compute',
        'https://www.googleapis.com/auth/logging.write',
        'https://www.googleapis.com/auth/monitoring',
    ])

  def test_oauth2_available_scopes(self):
    self.assertTrue(gce.can_send_metric('default'))
    self.assertFalse(gce.can_send_metric('foo'))

  def test_get_image(self):
    self.assertEqual(gce.get_image(), 'test-image')

  def test_get_zone(self):
    self.assertEqual(gce.get_zone(), 'us-west1-c')

  def test_get_zones(self):
    expected = ['us', 'us-west', 'us-west1', 'us-west1-c']
    self.assertEqual(gce.get_zones(), expected)

  def test_get_zones_europe(self):
    self.mock_get_metadata.return_value = {
        'instance': {
            'zone': 'europe-west1-b'
        }
    }
    expected = ['europe', 'europe-west', 'europe-west1', 'europe-west1-b']
    self.assertEqual(gce.get_zones(), expected)

  def test_get_gcp(self):
    self.assertEqual(gce.get_gcp(), ['test-proj'])

  def test_get_machine_type(self):
    self.assertEqual(gce.get_machine_type(), 'n1-standard-8')

  def test_get_cpuinfo(self):
    self.assertEqual(gce.get_cpuinfo(), {
        'name': 'Intel(R) Xeon(R) CPU Broadwell GCE',
        'vendor': 'GenuineIntel'
    })

  def test_get_cpuinfo_amd(self):
    self.mock_get_metadata.return_value = {
        'instance': {
            'cpuPlatform': 'AMD Rome'
        }
    }
    self.assertEqual(gce.get_cpuinfo(), {
        'name': 'AMD Rome GCE',
        'vendor': 'AuthenticAMD'
    })

  def test_get_tags(self):
    self.assertEqual(gce.get_tags(), ['tag1', 'tag2'])


@unittest.skip('TODO(crbug.com/1100226): add test')
class TestMetadata(auto_stub.TestCase):

  def test_get_metadata_uncached(self):
    pass

  def test_wait_for_metadata(self):
    pass

  def test_get_metadata(self):
    pass


@unittest.skip('TODO(crbug.com/1100226): add test')
class TestOauth2AccessToken(auto_stub.TestCase):

  def test_oauth2_access_token_with_expiration(self):
    pass

  def test_oauth2_access_token(self):
    pass


class TestSignedMetadataToken(auto_stub.TestCase):
  def setUp(self):
    super(TestSignedMetadataToken, self).setUp()
    self.now = 1541465089.0
    self.mock(time, 'time', lambda: self.now)
    self.mock(gce, 'is_gce', lambda: True)

  def test_works(self):
    # JWTs are '<header>.<payload>.<signature>'. We care only about payload.
    bytes_json = bytes(
        json.dumps({
            'iat': self.now - 600,
            'exp': self.now + 3000,  # 1h after 'iat'
        }).encode('utf-8'))
    jwt = b'unimportant.' + base64.urlsafe_b64encode(
        bytes_json) + b'.unimportant'

    metadata_calls = []
    def mocked_raw_metadata_request(path):
      metadata_calls.append(path)
      return jwt

    self.mock(gce, '_raw_metadata_request', mocked_raw_metadata_request)

    tok, exp = gce.signed_metadata_token('https://example.com')
    self.assertEqual(tok, jwt)
    self.assertEqual(exp, self.now + 3600)
    self.assertEqual(metadata_calls, [
      '/computeMetadata/v1/instance/service-accounts/default/'
      'identity?audience=https%3A%2F%2Fexample.com&format=full',
    ])

    # Hitting the cache now.
    tok, exp = gce.signed_metadata_token('https://example.com')
    self.assertEqual(tok, jwt)
    self.assertEqual(exp, self.now + 3600)
    self.assertEqual(len(metadata_calls), 1)  # still same 1 call

    # 1h later cache has expired.
    self.now += 3600
    tok, exp = gce.signed_metadata_token('https://example.com')
    self.assertEqual(tok, jwt)
    self.assertEqual(exp, self.now + 3600)
    self.assertEqual(len(metadata_calls), 2)  # have made a new call


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
