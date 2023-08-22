#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import cStringIO
import datetime
import gzip
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

import mock

from google.appengine.ext import ndb
from google.protobuf import field_mask_pb2

from components import auth
from components import net
from components.config import remote
from components.prpc import client
from components.prpc import codes
from test_support import test_case

from .proto import config_service_pb2

import test_config_pb2


class RemoteTestCase(test_case.TestCase):
  def setUp(self):
    super(RemoteTestCase, self).setUp()
    self.mock(net, 'json_request_async', mock.Mock())
    net.json_request_async.side_effect = self.json_request_async

    self.provider = remote.Provider('luci-config.appspot.com')
    provider_future = ndb.Future()
    provider_future.set_result(self.provider)
    self.mock(remote, 'get_provider_async', lambda: provider_future)

    # Inject a prpc config v2 client.
    self.v2_cient_mock = mock.Mock()
    mock.patch.object(
        self.provider,
        '_config_v2_client').start().return_value = self.v2_cient_mock

  @ndb.tasklet
  def json_request_async(self, url, **kwargs):
    assert kwargs['scopes']
    URL_PREFIX = 'https://luci-config.appspot.com/_ah/api/config/v1/'
    if url == URL_PREFIX + 'config_sets/services%2Ffoo/config/bar.cfg':
      assert kwargs['params']['hash_only']
      raise ndb.Return({
        'content_hash': 'deadbeef',
        'revision': 'aaaabbbb',
      })
    if url == URL_PREFIX + 'config_sets/services%2Ffoo/config/baz.cfg':
      assert kwargs['params']['hash_only']
      raise ndb.Return({
        'content_hash': 'badcoffee',
        'revision': 'aaaabbbb',
      })

    if url == URL_PREFIX + 'config/deadbeef':
      raise ndb.Return({
        'content':  base64.b64encode('a config'),
      })
    if url == URL_PREFIX + 'config/badcoffee':
      raise ndb.Return({
        'content':  base64.b64encode('param: "qux"'),
      })

    if url == URL_PREFIX + 'projects':
      raise ndb.Return({
        'projects': [
          {
           'id': 'chromium',
           'repo_type': 'GITILES',
           'repo_url': 'https://chromium.googlesource.com/chromium/src',
           'name': 'Chromium browser'
          },
          {
           'id': 'infra',
           'repo_type': 'GITILES',
           'repo_url': 'https://chromium.googlesource.com/infra/infra',
          },
        ]
      })
    self.fail('Unexpected url: %s' % url)

  def test_get_async_v1(self):
    revision, content = self.provider.get_async(
        'services/foo', 'bar.cfg').get_result()
    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(content, 'a config')

    # Memcache coverage
    net.json_request_async.reset_mock()
    revision, content = self.provider.get_async(
        'services/foo', 'bar.cfg').get_result()
    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(content, 'a config')
    self.assertFalse(net.json_request_async.called)

  def test_get_async_v2(self):
    self.provider.service_hostname = 'luci-config-v2.com'
    self.v2_cient_mock.GetConfig.side_effect = [
        future(
            config_service_pb2.Config(revision='aaaabbbb',
                                      content_sha256='sha256hash')),
        future(config_service_pb2.Config(raw_content=b'a config')),
    ]

    revision, content = self.provider.get_async('services/foo',
                                                'bar.cfg').get_result()

    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(content, 'a config')
    self.v2_cient_mock.GetConfig.assert_has_calls([
        mock.call(
            config_service_pb2.GetConfigRequest(
                config_set='services/foo',
                path='bar.cfg',
                fields=field_mask_pb2.FieldMask(
                    paths=['revision', 'content_sha256'])),
            credentials=mock.ANY,
        ),
        mock.call(
            config_service_pb2.GetConfigRequest(
                content_sha256='sha256hash',
                fields=field_mask_pb2.FieldMask(paths=['content']),
            ),
            credentials=mock.ANY,
        ),
    ])

    # Memcache coverage
    self.v2_cient_mock.reset_mock()
    revision, content = self.provider.get_async('services/foo',
                                                'bar.cfg').get_result()
    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(content, 'a config')
    self.assertFalse(self.v2_cient_mock.called)

  def test_get_async_v2_content_in_signed_url(self):
    self.provider.service_hostname = 'luci-config-v2.com'
    self.v2_cient_mock.GetConfig.side_effect = [
        future(
            config_service_pb2.Config(revision='aaaabbbb',
                                      content_sha256='sha256hash')),
        future(config_service_pb2.Config(signed_url='signed_url')),
    ]

    @ndb.tasklet
    def mock_request_async(url, **kwargs):
      assert kwargs.get('headers', {}).get('Accept-Encoding') == 'gzip'
      assert isinstance(kwargs['response_headers'], dict)
      kwargs['response_headers'].update({'Content-Encoding': 'gzip'})
      return _gzip_compress('a large config')

    self.mock(net, 'request_async', mock.Mock())
    net.request_async.side_effect = mock_request_async

    revision, content = self.provider.get_async('services/foo',
                                                'bar.cfg').get_result()

    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(content, 'a large config')
    self.v2_cient_mock.GetConfig.assert_has_calls([
        mock.call(
            config_service_pb2.GetConfigRequest(
                config_set='services/foo',
                path='bar.cfg',
                fields=field_mask_pb2.FieldMask(
                    paths=['revision', 'content_sha256'])),
            credentials=mock.ANY,
        ),
        mock.call(
            config_service_pb2.GetConfigRequest(
                content_sha256='sha256hash',
                fields=field_mask_pb2.FieldMask(paths=['content']),
            ),
            credentials=mock.ANY,
        ),
    ])

    # Memcache coverage
    self.v2_cient_mock.reset_mock()
    revision, content = self.provider.get_async('services/foo',
                                                'bar.cfg').get_result()
    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(content, 'a large config')
    self.assertFalse(self.v2_cient_mock.called)

  def test_get_async_v2_not_found(self):
    self.provider.service_hostname = 'luci-config-v2.com'
    self.v2_cient_mock.GetConfig.side_effect = client.RpcError(
        'Config Not Found', codes.StatusCode.NOT_FOUND, {})

    revision, content = self.provider.get_async('services/foo',
                                                'bar.cfg').get_result()

    self.assertEqual(revision, None)
    self.assertEqual(content, None)

  def test_get_async_v2_rpc_err(self):
    self.provider.service_hostname = 'luci-config-v2.com'
    self.v2_cient_mock.GetConfig.side_effect = client.RpcError(
        'Internal Error', codes.StatusCode.INTERNAL, {})

    with self.assertRaises(client.RpcError) as err:
      self.provider.get_async('services/foo', 'bar.cfg').get_result()
    self.assertEqual(err.exception.status_code, codes.StatusCode.INTERNAL)

  def test_get_async_with_revision(self):
    revision, content = self.provider.get_async(
        'services/foo', 'bar.cfg', revision='aaaabbbb').get_result()
    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(content, 'a config')

    net.json_request_async.assert_any_call(
        'https://luci-config.appspot.com/_ah/api/config/v1/'
        'config_sets/services%2Ffoo/config/bar.cfg',
        params={'hash_only': True, 'revision': 'aaaabbbb'},
        scopes=net.EMAIL_SCOPE)

    # Memcache coverage
    net.json_request_async.reset_mock()
    revision, content = self.provider.get_async(
        'services/foo', 'bar.cfg', revision='aaaabbbb').get_result()
    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(content, 'a config')
    self.assertFalse(net.json_request_async.called)

  def test_last_good(self):
    revision, content = self.provider.get_async(
        'services/foo', 'bar.cfg', store_last_good=True).get_result()
    self.assertIsNone(revision)
    self.assertIsNone(content)

    self.assertTrue(remote.LastGoodConfig.get_by_id('services/foo:bar.cfg'))
    remote.LastGoodConfig(
        id='services/foo:bar.cfg',
        content='a config',
        content_hash='deadbeef',
        revision='aaaaaaaa').put()

    revision, content = self.provider.get_async(
        'services/foo', 'bar.cfg', store_last_good=True).get_result()
    self.assertEqual(revision, 'aaaaaaaa')
    self.assertEqual(content, 'a config')

    self.assertFalse(net.json_request_async.called)
    self.assertFalse(self.v2_cient_mock.called)

  def test_get_projects_v1(self):
    projects = self.provider.get_projects_async().get_result()
    self.assertEqual(projects, [
      {
       'id': 'chromium',
       'repo_type': 'GITILES',
       'repo_url': 'https://chromium.googlesource.com/chromium/src',
       'name': 'Chromium browser'
      },
      {
       'id': 'infra',
       'repo_type': 'GITILES',
       'repo_url': 'https://chromium.googlesource.com/infra/infra',
      },
    ])

  def test_get_projects_v2(self):
    self.provider.service_hostname = 'luci-config-v2.com'
    self.v2_cient_mock.ListConfigSets.return_value = future(
        config_service_pb2.ListConfigSetsResponse(config_sets=[
            config_service_pb2.ConfigSet(
                name='projects/chromium',
                url='https://chromium.googlesource.com/chromium/src'),
            config_service_pb2.ConfigSet(
                name='projects/infra',
                url='https://chromium.googlesource.com/infra/infra'),
        ]))

    projects = self.provider.get_projects_async().get_result()
    self.assertEqual(projects, [
        {
            'id': 'chromium',
            'repo_type': 'GITILES',
            'repo_url': 'https://chromium.googlesource.com/chromium/src',
            'name': 'chromium'
        },
        {
            'id': 'infra',
            'repo_type': 'GITILES',
            'repo_url': 'https://chromium.googlesource.com/infra/infra',
            'name': 'infra'
        },
    ])
    self.v2_cient_mock.ListConfigSets.assert_called_once_with(
        config_service_pb2.ListConfigSetsRequest(domain='PROJECT'),
        credentials=mock.ANY,
    )

  def test_get_projects_v2_rpc_err(self):
    self.provider.service_hostname = 'luci-config-v2.com'
    self.v2_cient_mock.ListConfigSets.side_effect = client.RpcError(
        'Internal Error', codes.StatusCode.INTERNAL, {})

    with self.assertRaises(client.RpcError) as err:
      self.provider.get_projects_async().get_result()
    self.assertEqual(err.exception.status_code, codes.StatusCode.INTERNAL)

  def test_get_project_configs_async_receives_404_v1(self):
    net.json_request_async.side_effect = net.NotFoundError(
        'Not found', 404, None)
    with self.assertRaises(net.NotFoundError):
      self.provider.get_project_configs_async('cfg').get_result()

  def test_get_project_configs_async_v1(self):
    self.mock(net, 'json_request_async', mock.Mock())
    net.json_request_async.return_value = ndb.Future()
    net.json_request_async.return_value.set_result({
      'configs': [
        {
          'config_set': 'projects/chromium',
          'content_hash': 'deadbeef',
          'path': 'cfg',
          'revision': 'aaaaaaaa',
        }
      ]
    })
    self.mock(self.provider, 'get_config_by_hash_async', mock.Mock())
    self.provider.get_config_by_hash_async.return_value = ndb.Future()
    self.provider.get_config_by_hash_async.return_value.set_result('a config')

    configs = self.provider.get_project_configs_async('cfg').get_result()

    self.assertEqual(configs, {'projects/chromium': ('aaaaaaaa', 'a config')})

  def test_get_project_configs_async_v2(self):
    self.provider.service_hostname = 'luci-config-v2.com'
    self.v2_cient_mock.GetProjectConfigs.return_value = future(
        config_service_pb2.GetProjectConfigsResponse(configs=[
            config_service_pb2.Config(config_set='projects/chromium',
                                      revision='aaaaaaaa',
                                      content_sha256='deadbeef')
        ]))
    self.v2_cient_mock.GetConfig.return_value = future(
        config_service_pb2.Config(raw_content=b'a config'))

    configs = self.provider.get_project_configs_async('cfg').get_result()

    self.assertEqual(configs, {'projects/chromium': ('aaaaaaaa', 'a config')})
    self.v2_cient_mock.GetProjectConfigs.assert_called_once_with(
        config_service_pb2.GetProjectConfigsRequest(
            path='cfg',
            fields=field_mask_pb2.FieldMask(
                paths=['config_set', 'revision', 'content_sha256'])),
        credentials=mock.ANY,
    )
    self.v2_cient_mock.GetConfig.assert_called_once_with(
        config_service_pb2.GetConfigRequest(
            content_sha256='deadbeef',
            fields=field_mask_pb2.FieldMask(paths=['content'])),
        credentials=mock.ANY,
    )

    # The 2nd call hits Memcache
    self.v2_cient_mock.reset_mock()
    self.assertEqual(configs, {'projects/chromium': ('aaaaaaaa', 'a config')})
    self.assertFalse(self.v2_cient_mock.GetConfig.called)

  def test_get_project_configs_async_v2_not_found(self):
    self.provider.service_hostname = 'luci-config-v2.com'
    self.v2_cient_mock.GetProjectConfigs.return_value = future(
        config_service_pb2.GetProjectConfigsResponse())

    configs = self.provider.get_project_configs_async('cfg').get_result()
    self.assertEqual(configs, {})

  def test_get_project_configs_async_v2_rpc_err(self):
    self.provider.service_hostname = 'luci-config-v2.com'
    self.v2_cient_mock.GetProjectConfigs.side_effect = client.RpcError(
        'Internal Error', codes.StatusCode.INTERNAL, {})

    with self.assertRaises(client.RpcError) as err:
      self.provider.get_project_configs_async('cfg').get_result()
    self.assertEqual(err.exception.status_code, codes.StatusCode.INTERNAL)

  def test_get_config_set_location_async(self):
    self.mock(net, 'json_request_async', mock.Mock())
    net.json_request_async.return_value = ndb.Future()
    net.json_request_async.return_value.set_result({
      'mappings': [
        {
          'config_set': 'services/abc',
          'location': 'http://example.com',
        },
      ],
    })
    r = self.provider.get_config_set_location_async('services/abc').get_result()
    self.assertEqual(r, 'http://example.com')
    net.json_request_async.assert_called_once_with(
        'https://luci-config.appspot.com/_ah/api/config/v1/mapping',
        scopes=net.EMAIL_SCOPE,
        params={'config_set': 'services/abc'})

  def test_cron_update_last_good_configs(self):
    self.provider.get_async(
        'services/foo', 'bar.cfg', store_last_good=True).get_result()
    self.provider.get_async(
        'services/foo', 'baz.cfg', dest_type=test_config_pb2.Config,
        store_last_good=True).get_result()

    # Will be removed.
    old_cfg = remote.LastGoodConfig(
        id='projects/old:foo.cfg',
        content_hash='aaaa',
        content='content',
        last_access_ts=datetime.datetime(2010, 1, 1))
    old_cfg.put()

    remote.cron_update_last_good_configs()

    revision, config = self.provider.get_async(
        'services/foo', 'bar.cfg', store_last_good=True).get_result()
    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(config, 'a config')

    revision, config = self.provider.get_async(
        'services/foo', 'baz.cfg', dest_type=test_config_pb2.Config,
        store_last_good=True).get_result()
    self.assertEqual(revision, 'aaaabbbb')
    self.assertEqual(config.param, 'qux')

    baz_cfg = remote.LastGoodConfig.get_by_id(id='services/foo:baz.cfg')
    self.assertIsNotNone(baz_cfg)
    self.assertEquals(baz_cfg.content_binary, config.SerializeToString())

    self.assertIsNone(old_cfg.key.get())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()


def future(result):
  f = ndb.Future()
  f.set_result(result)
  return f


def _gzip_compress(blob):
  out = cStringIO.StringIO()
  with gzip.GzipFile(fileobj=out, mode='w') as f:
    f.write(blob)
  return out.getvalue()
