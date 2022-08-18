#!/usr/bin/env vpython3
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import json
import logging
import sys
import threading
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
      'project': {
          'projectId': 'test-proj',
          'numericProjectId': 123456
      },
      'instance': {
          'cpuPlatform':
          'Intel Broadwell',
          'scheduling': {
              'automaticRestart': 'TRUE',
              'preemptible': 'FALSE',
              'onHostMaintenance': 'MIGRATE'
          },
          'hostname':
          'host.name',
          'machineType':
          'projects/123456/machineTypes/n1-standard-8',
          'description':
          '',
          'zone':
          'projects/123456/zones/us-west1-c',
          'maintenanceEvent':
          'NONE',
          'image':
          'projects/test-proj/global/images/test-image',
          'disks': [{
              'interface': 'SCSI',
              'deviceName': 'persistent-disk-0',
              'type': 'PERSISTENT',
              'mode': 'READ_WRITE',
              'index': 0
          }],
          'licenses': [{
              'id': '1000001'
          }],
          'preempted':
          'FALSE',
          'remainingCpuTime':
          -1,
          'virtualClock': {
              'driftToken': '0'
          },
          'legacyEndpointAccess': {
              '0.1': 0,
              'v1beta1': 0
          },
          'serviceAccounts': {
              'default': {
                  'scopes': [
                      'https://www.googleapis.com/auth/compute',
                      'https://www.googleapis.com/auth/logging.write',
                      'https://www.googleapis.com/auth/monitoring',
                  ],
                  'email':
                  'test@test-proj.iam.gserviceaccount.com',
                  'aliases': ['default']
              },
              'test@test-proj.iam.gserviceaccount.com': {
                  'scopes': [
                      'https://www.googleapis.com/auth/compute',
                      'https://www.googleapis.com/auth/logging.write',
                      'https://www.googleapis.com/auth/monitoring',
                  ],
                  'email':
                  'test@test-proj.iam.gserviceaccount.com',
                  'aliases': ['default']
              }
          },
          'networkInterfaces': [{
              'network': 'some/network1'
          }, {
              'network': 'some/network2'
          }],
          'guestAttributes': {},
          'id':
          987654,
          'tags': ['tag1', 'tag2'],
          'name':
          'test-inst'
      },
      'oslogin': {
          'authenticate': {
              'sessions': {}
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

  def test_get_cpuinfo_ampere(self):
    self.mock_get_metadata.return_value = {
        'instance': {
            'cpuPlatform': 'Ampere Altra',
        }
    }
    self.assertEqual(gce.get_cpuinfo(), {
        'name': 'Ampere Altra GCE',
        'vendor': 'ARM'
    })

  def test_get_tags(self):
    self.assertEqual(gce.get_tags(), ['tag1', 'tag2'])

  def test_get_networks(self):
    self.assertEqual(gce.get_networks(), ['some/network1', 'some/network2'])


@unittest.skipUnless(gce.is_gce(), 'TestMetadata runs only on GCE machines')
class TestMetadata(auto_stub.TestCase):

  def test_get_metadata_uncached(self):
    meta = gce.get_metadata_uncached()
    # assert if all metadata used in gce.py exist.
    self.assertIsNotNone(meta['instance']['cpuPlatform'])
    self.assertIsNotNone(meta['instance']['image'])
    self.assertIsNotNone(meta['instance']['machineType'])
    self.assertIsNotNone(meta['instance']['serviceAccounts'])
    self.assertIsNotNone(meta['instance']['tags'])
    self.assertIsNotNone(meta['instance']['zone'])
    self.assertIsNotNone(meta['project']['projectId'])

  def test_wait_for_metadata(self):
    quit_bit = threading.Event()

    # It should cache the result.
    gce.wait_for_metadata(quit_bit)
    self.assertIsNotNone(gce._CACHED_METADATA[0])

  def test_get_metadata(self):
    # First call should cache the result.
    meta = gce.get_metadata()

    # Second call should not call get_metadata_uncached().
    with mock.patch('api.platforms.gce.get_metadata_uncached'
                   ) as mock_get_metadata_uncached:
      meta_cached = gce.get_metadata()
      mock_get_metadata_uncached.assert_not_called()

    self.assertEqual(meta, meta_cached)


@unittest.skipUnless(gce.is_gce(),
                     'TestOauth2AccessToken runs only on GCE machines')
class TestOauth2AccessToken(auto_stub.TestCase):

  def test_oauth2_access_token_with_expiration(self):
    token, exp = gce.oauth2_access_token_with_expiration('default')
    self.assertIsNotNone(token)
    self.assertIsNotNone(exp)

  def test_oauth2_access_token(self):
    token = gce.oauth2_access_token()
    self.assertIsNotNone(token)


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
            'name': 'test jwt',  # this is for testing _padded_b64_decode()
            'iat': self.now - 600,
            'exp': self.now + 3000,  # 1h after 'iat'
        }).encode('utf-8'))
    jwt = b'unimportant.' + base64.urlsafe_b64encode(bytes_json).rstrip(
        b'=') + b'.unimportant'

    metadata_calls = []
    def mocked_raw_metadata_request(path):
      metadata_calls.append(path)
      return jwt

    self.mock(gce, '_raw_metadata_request', mocked_raw_metadata_request)

    tok, exp = gce.signed_metadata_token('https://example.com')
    self.assertEqual(tok, jwt.decode())
    self.assertEqual(exp, self.now + 3600)
    self.assertEqual(metadata_calls, [
      '/computeMetadata/v1/instance/service-accounts/default/'
      'identity?audience=https%3A%2F%2Fexample.com&format=full',
    ])

    # Hitting the cache now.
    tok, exp = gce.signed_metadata_token('https://example.com')
    self.assertEqual(tok, jwt.decode())
    self.assertEqual(exp, self.now + 3600)
    self.assertEqual(len(metadata_calls), 1)  # still same 1 call

    # 1h later cache has expired.
    self.now += 3600
    tok, exp = gce.signed_metadata_token('https://example.com')
    self.assertEqual(tok, jwt.decode())
    self.assertEqual(exp, self.now + 3600)
    self.assertEqual(len(metadata_calls), 2)  # have made a new call


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
