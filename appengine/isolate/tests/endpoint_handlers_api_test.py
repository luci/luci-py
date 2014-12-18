#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import json
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

import dev_appserver
dev_appserver.fix_sys_path()

import endpoints
from protorpc.remote import protojson
from support import test_case

import webtest

from components import auth_testing
import endpoint_handlers_api
from endpoint_handlers_api import Digest
from endpoint_handlers_api import DigestCollection
import model


def hash_content(content, namespace):
  """Create and return the hash of some content in a given namespace."""
  hash_algo = model.get_hash_algo(namespace)
  hash_algo.update(content)
  return hash_algo.hexdigest()


def generate_digest(content, namespace):
  """Create a Digest from content (in a given namespace) for preupload.

  Arguments:
    content: the content to be hashed
    namespace: the namespace in which the content will be hashed

  Returns:
    a Digest corresponding to the content/ namespace pair
  """
  return Digest(digest=hash_content(content, namespace), size=len(content))


### Isolate Service Test


class IsolateServiceTest(test_case.EndpointsTestCase):
  """Test the IsolateService's API methods."""
  # TODO(cmassaro): this should eventually inherit from endpointstestcase

  preupload_url = '/_ah/spi/IsolateService.preupload'

  def setUp(self):
    super(IsolateServiceTest, self).setUp()
    auth_testing.mock_get_current_identity(self)
    self.testbed.setup_env(current_version_id='testbed.version')
    self.testbed.init_all_stubs()
    self.source_ip = '127.0.0.1'
    self.app_api = webtest.TestApp(
        endpoints.api_server([endpoint_handlers_api.IsolateService],
                             restricted=False),
        extra_environ={'REMOTE_ADDR': self.source_ip})

  @classmethod
  def message_to_dict(cls, message):
    """Returns a JSON-ish dictionary corresponding to the RPC message."""
    return json.loads(protojson.encode_message(message))

  def test_pre_upload_ok(self):
    """Assert that preupload correctly posts a valid DigestCollection."""
    good_digests = DigestCollection()
    good_digests.items.append(generate_digest('a pony',
                                              good_digests.namespace))
    response = self.app_api.post_json(
        IsolateServiceTest.preupload_url, self.message_to_dict(good_digests))
    self.assertNotEqual(response, None)  # TODO(cmassaro): better check

  def test_pre_upload_invalid_hash(self):
    """Assert that status 400 is returned when the digest is invalid."""
    bad_digest = hash_content('some stuff', 'default')
    bad_digest = 'g' + bad_digest[1:]  # that's not hexadecimal!
    bad_collection = DigestCollection(items=[
        Digest(digest=bad_digest, size=10)])
    with self.call_should_fail('400'):
      _response = self.app_api.post_json(
          IsolateServiceTest.preupload_url, self.message_to_dict(
              bad_collection))

  def test_pre_upload_invalid_namespace(self):
    """Assert that status 400 is returned when the namespace is invalid."""
    bad_collection = DigestCollection(namespace='~tildewhatevs', items=[
        generate_digest('pangolin', 'default')])
    with self.call_should_fail('400'):
      _response = self.app_api.post_json(
          IsolateServiceTest.preupload_url, self.message_to_dict(
              bad_collection))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
