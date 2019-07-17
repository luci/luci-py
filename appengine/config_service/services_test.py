#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

from test_env import future
import test_env
test_env.setup_test_env()

import mock
import logging

from components import net
from components.config.proto import service_config_pb2
from test_support import test_case

import common
import services
import storage


class ProjectsTestCase(test_case.TestCase):
  def setUp(self):
    super(ProjectsTestCase, self).setUp()
    self.mock(storage, 'get_self_config_async', mock.Mock())
    storage.get_self_config_async.return_value = future(
        service_config_pb2.ServicesCfg(
            services=[
              service_config_pb2.Service(
                  id='foo', metadata_url='https://foo.com/metadata'),
              service_config_pb2.Service(id='metadataless'),
            ]
        ))

  def mock_metadata_entity(self):
    dct = {
      'version': '1.0',
      'validation': {
        'url': 'https://a.com/validate',
        'patterns': [
          {'config_set': 'projects/foo', 'path': 'bar.cfg'},
          {'config_set': 'regex:services/.+', 'path': 'regex:.+'},
        ]
      }
    }
    mck_meta = (services._dict_to_dynamic_metadata(dct).SerializeToString())
    storage.ServiceDynamicMetadata(
        id='deadbeef',
        metadata=mck_meta,
    ).put()

  @staticmethod
  def service_proto(
      sid='deadbeef',
      metadata_url='https://a.com/metadata',
      jwt_auth=None):
    return service_config_pb2.Service(
        id=sid,
        metadata_url=metadata_url,
        jwt_auth=jwt_auth)

  def test_dict_to_dynamic_metadata(self):
    with self.assertRaises(services.DynamicMetadataError):
      services._dict_to_dynamic_metadata([])

    self.assertEqual(
      services._dict_to_dynamic_metadata({
        'version': '1.0',
        'validation': {
          'url': 'https://a.com/validate',
          'patterns': [
            {'config_set': 'projects/foo', 'path': 'bar.cfg'},
            {'config_set': 'regex:services/.+', 'path': 'regex:.+'},
          ]
        }
      }),
      service_config_pb2.ServiceDynamicMetadata(
          validation=service_config_pb2.Validator(
              url='https://a.com/validate',
              patterns=[
                service_config_pb2.ConfigPattern(
                    config_set='projects/foo', path='bar.cfg'),
                service_config_pb2.ConfigPattern(
                    config_set='regex:services/.+', path='regex:.+'),
              ]
          )
      )
    )

  def test_get_metadata_async_not_found(self):
    with self.assertRaises(services.DynamicMetadataError):
      services.get_metadata_async('non-existent').get_result()

  def test_get_metadata_async_no_metadata(self):
    storage.ServiceDynamicMetadata(id='metadataless').put()
    metadata = services.get_metadata_async('metadataless').get_result()
    self.assertIsNotNone(metadata)
    self.assertFalse(metadata.validation.patterns)

  def test_update_service_metadata_async_same(self):
    self.mock_metadata_entity()
    self.mock(net, 'json_request_async', mock.Mock())
    dct = {
      'version': '1.0',
      'validation': {
        'url': 'https://a.com/validate',
        'patterns': [
          {'config_set': 'projects/foo', 'path': 'bar.cfg'},
          {'config_set': 'regex:services/.+', 'path': 'regex:.+'},
        ]
      }
    }

    net.json_request_async.return_value = future(dct)
    self.mock(logging, 'info', mock.Mock())
    services._update_service_metadata_async(self.service_proto()).get_result()
    self.assertFalse(logging.info.called)

    net.json_request_async.assert_called_once_with(
        'https://a.com/metadata',
        method='GET',
        payload=None,
        scopes=net.EMAIL_SCOPE,
        use_jwt_auth=False,
        audience=None)

  def test_update_service_metadata_async_jwt(self):
    self.mock_metadata_entity()
    self.mock(net, 'json_request_async', mock.Mock())
    dct = {
      'version': '1.0',
      'validation': {
        'url': 'https://a.com/validate',
        'patterns': [
          {'config_set': 'projects/foo', 'path': 'bar.cfg'},
          {'config_set': 'regex:services/.+', 'path': 'regex:.+'},
        ]
      }
    }

    net.json_request_async.return_value = future(dct)
    self.mock(logging, 'info', mock.Mock())
    svc = self.service_proto(
        jwt_auth=service_config_pb2.Service.JWTAuth(
            audience='https://service.example.com'))
    services._update_service_metadata_async(svc).get_result()
    self.assertFalse(logging.info.called)

    net.json_request_async.assert_called_once_with(
        'https://a.com/metadata',
        method='GET',
        payload=None,
        scopes=None,
        use_jwt_auth=True,
        audience='https://service.example.com')

  def test_update_service_metadata_async_different(self):
    self.mock_metadata_entity()
    self.mock(net, 'json_request_async', mock.Mock())
    dct = {
      'version': '1.0',
      'validation': {
        'url': 'https://a.com/different_validate',
        'patterns': [
          {'config_set': 'projects/bar', 'path': 'foo.cfg'},
          {'config_set': 'regex:services/.+', 'path': 'regex:.+'},
        ]
      }
    }

    net.json_request_async.return_value = future(dct)
    self.mock(logging, 'info', mock.Mock())
    services._update_service_metadata_async(self.service_proto()).get_result()
    self.assertTrue(logging.info.called)

    md = services.get_metadata_async('deadbeef').get_result()
    self.assertEqual(md.validation.url, 'https://a.com/different_validate')

  def test_update_service_metadata_no_service_url(self):
    self.mock_metadata_entity()
    self.mock(logging, 'info', mock.Mock())
    svc = self.service_proto(metadata_url='')
    services._update_service_metadata_async(svc).get_result()
    self.assertTrue(logging.info.called)


if __name__ == '__main__':
  test_env.main()
