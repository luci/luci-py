#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
import json
import logging
import os
import re
import sys
import unittest
from Crypto.PublicKey import RSA

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import test_env
test_env.setup_test_env()

import dev_appserver
dev_appserver.fix_sys_path()

import endpoints
from protorpc.remote import protojson
from support import test_case

import webapp2
import webtest

from components import auth_testing
from components import utils
import config
import endpoint_handlers_api
from endpoint_handlers_api import Digest
from endpoint_handlers_api import DigestCollection
import handlers_backend
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
  gs_prefix = 'localhost:80/content-gs/store/'
  store_prefix = 'https://isolateserver-dev.storage.googleapis.com/'

  APP_DIR = ROOT_DIR

  def setUp(self):
    super(IsolateServiceTest, self).setUp()
    auth_testing.mock_get_current_identity(self)
    version = utils.get_app_version()
    self.mock(utils, 'get_task_queue_host', lambda: version)
    self.testbed.setup_env(current_version_id='testbed.version')
    self.source_ip = '127.0.0.1'
    self.app_api = webtest.TestApp(
        endpoints.api_server([endpoint_handlers_api.IsolateService],
                             restricted=False),
        extra_environ={'REMOTE_ADDR': self.source_ip})
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(handlers_backend.get_routes(), debug=True),
        extra_environ={'REMOTE_ADDR': self.source_ip})

  @staticmethod
  def message_to_dict(message):
    """Returns a JSON-ish dictionary corresponding to the RPC message."""
    return json.loads(protojson.encode_message(message))

  @staticmethod
  def make_private_key():
    """Add a private key to config.settings() to create finalize URLs."""
    new_key = RSA.generate(2048)
    pem_key = base64.b64encode(new_key.exportKey('PEM'))
    config.settings().gs_private_key = pem_key

  def test_pre_upload_ok(self):
    """Assert that preupload correctly posts a valid DigestCollection."""
    good_digests = DigestCollection()
    good_digests.items.append(
        generate_digest('a pony', good_digests.namespace))
    response = self.app_api.post_json(
        IsolateServiceTest.preupload_url, self.message_to_dict(good_digests))
    message = json.loads(response.body).get(u'items', [{}])[0]
    self.assertTrue(message.get(u'upload_url', '').startswith(self.gs_prefix))

  def test_finalize_url_ok(self):
    """Assert that a finalize_url is generated when should_push_to_gs."""
    digests = DigestCollection()

    # add a private key; the URLSigner is initialized from config.settings()
    self.make_private_key()
    digests.items.append(generate_digest('duckling.' * 70, digests.namespace))
    response = self.app_api.post_json(
        IsolateServiceTest.preupload_url, self.message_to_dict(digests))
    message = json.loads(response.body).get(u'items', [{}])[0]
    self.assertTrue(message.get(u'upload_url', '').startswith(
        self.store_prefix))
    self.assertTrue(message.get(u'finalize_url', '').startswith(self.gs_prefix))

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

  def test_check_existing_finds_existing_entities(self):
    """Assert that existence check is working."""
    collection = DigestCollection()
    collection.items.extend([
        generate_digest('small content', collection.namespace),
        generate_digest('larger content', collection.namespace),
        generate_digest('biggest content', collection.namespace)])
    key = model.entry_key(collection.namespace, collection.items[0].digest)

    # guarantee that one digest already exists in the datastore
    model.new_content_entry(key, content=collection.items[0].digest).put()
    response = self.app_api.post_json(
        IsolateServiceTest.preupload_url, self.message_to_dict(collection))

    # we should see one enqueued task and two new URLs in the response
    self.assertEqual(2, len(json.loads(response.body)['items']))

    # remove tasks so tearDown doesn't complain
    _ = self.execute_tasks()

  def test_check_existing_enqueues_tasks(self):
    """Assert that existent entities are enqueued."""
    collection = DigestCollection()
    collection.items.extend([
        generate_digest('some content', collection.namespace)])
    key = model.entry_key(collection.namespace, collection.items[0].digest)

    # guarantee that one digest already exists in the datastore
    model.new_content_entry(key, content=collection.items[0].digest).put()
    _response = self.app_api.post_json(
        IsolateServiceTest.preupload_url, self.message_to_dict(collection))

    # find enqueued tasks
    enqueued_tasks = self.execute_tasks()
    self.assertEqual(1, enqueued_tasks)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
