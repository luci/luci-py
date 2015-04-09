#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64

import test_env
test_env.setup_test_env()

from components import auth
from test_support import test_case
import endpoints
import mock

from proto import service_config_pb2
import acl
import api
import storage


class ApiTest(test_case.EndpointsTestCase):
  api_service_cls = api.ConfigApi

  def setUp(self):
    super(ApiTest, self).setUp()
    self.mock(acl, 'can_read_config_set', mock.Mock())
    acl.can_read_config_set.return_value = True

  ##############################################################################
  ## get_config

  def test_get_config(self):
    self.mock(storage, 'get_config_hash', mock.Mock())
    self.mock(storage, 'get_config_by_hash', mock.Mock())
    storage.get_config_hash.return_value = 'deadbeef', 'abc0123'
    storage.get_config_by_hash.return_value = 'config text'

    req = {
      'config_set': 'services/luci-config',
      'path': 'my.cfg',
      'revision': 'deadbeef',
    }
    resp = self.call_api('get_config', req).json_body

    self.assertEqual(resp, {
      'content': base64.b64encode('config text'),
      'content_hash': 'abc0123',
      'revision': 'deadbeef',
      'status': 'SUCCESS',
    })
    storage.get_config_hash.assert_called_once_with(
      'services/luci-config', 'my.cfg', revision='deadbeef')
    storage.get_config_by_hash.assert_called_once_with('abc0123')

  def test_get_config_hash_only(self):
    self.mock(storage, 'get_config_hash', mock.Mock())
    self.mock(storage, 'get_config_by_hash', mock.Mock())
    storage.get_config_hash.return_value = 'deadbeef', 'abc0123'

    req = {
      'config_set': 'services/luci-config',
      'hash_only': True,
      'path': 'my.cfg',
      'revision': 'deadbeef',
    }
    resp = self.call_api('get_config', req).json_body

    self.assertEqual(resp, {
      'content_hash': 'abc0123',
      'revision': 'deadbeef',
      'status': 'SUCCESS',
    })
    self.assertFalse(storage.get_config_by_hash.called)

  def test_get_config_blob_not_found(self):
    self.mock(storage, 'get_config_hash', mock.Mock())
    self.mock(storage, 'get_config_by_hash', mock.Mock())
    storage.get_config_hash.return_value = 'deadbeef', 'abc0123'
    storage.get_config_by_hash.return_value = None

    req = {
      'config_set': 'services/luci-config',
      'path': 'my.cfg',
    }
    resp = self.call_api('get_config', req).json_body

    self.assertEqual(resp, {
      'content_hash': 'abc0123',
      'revision': 'deadbeef',
      'status': 'CONFIG_NOT_FOUND',
    })

  def test_get_config_not_found(self):
    self.mock(storage, 'get_config_hash', lambda *_, **__: (None, None))

    req = {
      'config_set': 'services/x',
      'path': 'a.cfg',
    }
    resp = self.call_api('get_config', req).json_body

    self.assertEqual(resp, {'status': 'CONFIG_NOT_FOUND'})

  def test_get_wrong_config_set(self):
    acl.can_read_config_set.side_effect = ValueError

    req = {
      'config_set': 'xxx',
      'path': 'my.cfg',
      'revision': 'deadbeef',
    }
    resp = self.call_api('get_config', req).json_body

    self.assertEqual(resp, {'status': 'INVALID_CONFIG_SET'})

  def test_get_config_without_permissions(self):
    acl.can_read_config_set.return_value = False
    self.mock(storage, 'get_config_hash', mock.Mock())

    req = {
      'config_set': 'services/luci-config',
      'path': 'projects.cfg',
    }
    resp = self.call_api('get_config', req).json_body

    self.assertEqual(resp, {'status': 'CONFIG_NOT_FOUND'})
    self.assertFalse(storage.get_config_hash.called)

  ##############################################################################
  ## get_config_by_hash

  def test_get_config_by_hash(self):
    self.mock(storage, 'get_config_by_hash', mock.Mock())
    storage.get_config_by_hash.return_value = 'some content'

    req = {'content_hash': 'deadbeef'}
    resp = self.call_api('get_config_by_hash', req).json_body

    self.assertEqual(resp, {
      'content': base64.b64encode('some content'),
      'status': 'SUCCESS',
    })

    storage.get_config_by_hash.return_value = None
    resp = self.call_api('get_config_by_hash', req).json_body
    self.assertEqual(resp, {'status': 'CONFIG_NOT_FOUND'})


if __name__ == '__main__':
  test_env.main()
