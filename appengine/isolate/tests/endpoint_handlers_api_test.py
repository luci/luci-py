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
from endpoint_handlers_api import DigestCollection
import handlers_backend
import model


def make_private_key():
  new_key = RSA.generate(2048)
  pem_key = base64.b64encode(new_key.exportKey('PEM'))
  config.settings().gs_private_key = pem_key


def hash_content(content, namespace):
  """Create and return the hash of some content in a given namespace."""
  hash_algo = model.get_hash_algo(namespace)
  hash_algo.update(content)
  return hash_algo.hexdigest()


def generate_digest(
    content, namespace=endpoint_handlers_api.Namespace().namespace):
  """Create a Digest from content (in a given namespace) for preupload.

  Arguments:
    content: the content to be hashed
    namespace: the namespace in which the content will be hashed

  Returns:
    a Digest corresponding to the content/ namespace pair
  """
  return endpoint_handlers_api.Digest(
      digest=hash_content(content, namespace), size=len(content))


def generate_embedded(namespace, digest):
  return {
      'c': namespace.compression,
      'd': digest.digest,
      'h': namespace.digest_hash,
      'i': str(int(digest.is_isolated)),
      'n': namespace.namespace,
      's': str(digest.size),
  }


validate = endpoint_handlers_api.TokenSigner.validate


def generate_store_request(content):
  namespace = endpoint_handlers_api.Namespace()
  digest = generate_digest(content, namespace.namespace)
  return endpoint_handlers_api.StorageRequest(
      upload_ticket=endpoint_handlers_api.IsolateService.generate_ticket(
          digest, namespace),
      content=content)


### Isolate Service Test


class IsolateServiceTest(test_case.EndpointsTestCase):
  """Test the IsolateService's API methods."""
  # TODO(cmassaro): this should eventually inherit from endpointstestcase

  api_service_cls = endpoint_handlers_api.IsolateService
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
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(handlers_backend.get_routes(), debug=True),
        extra_environ={'REMOTE_ADDR': self.source_ip})
    # add a private key; signing depends on config.settings()
    make_private_key()

  @staticmethod
  def message_to_dict(message):
    """Returns a JSON-ish dictionary corresponding to the RPC message."""
    return json.loads(protojson.encode_message(message))

  def test_pre_upload_ok(self):
    """Assert that preupload correctly posts a valid DigestCollection.

    TODO(cmassaro): verify the upload_ticket is correct
    """
    good_digests = DigestCollection(
        namespace=endpoint_handlers_api.Namespace())
    good_digests.items.append(generate_digest('a pony', good_digests.namespace))
    response = self.call_api(
        'preupload', self.message_to_dict(good_digests), 200)
    message = json.loads(response.body).get(u'items', [{}])[0]
    self.assertEqual('', message.get(u'gs_upload_url', ''))
    self.assertEqual(
        validate(message.get(u'upload_ticket', ''), 'datastore'),
        generate_embedded(good_digests.namespace, good_digests.items[0]))
    self.assertEqual(good_digests.items[0].digest, message.get('digest', ''))

  def test_finalize_url_ok(self):
    """Assert that a finalize_url is generated when should_push_to_gs.

    TODO(cmassaro): verify upload_ticket
    """
    digests = DigestCollection(namespace=endpoint_handlers_api.Namespace())
    digests.items.append(generate_digest('duckling.' * 70, digests.namespace))
    response = self.call_api(
        'preupload', self.message_to_dict(digests), 200)
    message = json.loads(response.body).get(u'items', [{}])[0]
    self.assertTrue(message.get(u'gs_upload_url', '').startswith(
        self.store_prefix))

  def test_pre_upload_invalid_hash(self):
    """Assert that status 400 is returned when the digest is invalid."""
    bad_collection = DigestCollection(
        namespace=endpoint_handlers_api.Namespace())
    bad_digest = hash_content('some stuff', bad_collection.namespace.namespace)
    bad_digest = 'g' + bad_digest[1:]  # that's not hexadecimal!
    bad_collection.items.append(
        endpoint_handlers_api.Digest(digest=bad_digest, size=10))
    with self.call_should_fail('400'):
      _response = self.call_api(
          'preupload', self.message_to_dict(bad_collection), 200)

  def test_pre_upload_invalid_namespace(self):
    """Assert that status 400 is returned when the namespace is invalid."""
    bad_collection = DigestCollection(
        namespace=endpoint_handlers_api.Namespace(namespace='~tildewhatevs'),
        items=[generate_digest('pangolin', endpoint_handlers_api.Namespace())])
    with self.call_should_fail('400'):
      _response = self.call_api(
          'preupload', self.message_to_dict(bad_collection), 200)

  def test_check_existing_finds_existing_entities(self):
    """Assert that existence check is working."""
    collection = DigestCollection(namespace=endpoint_handlers_api.Namespace())
    collection.items.extend([
        generate_digest('small content', collection.namespace),
        generate_digest('larger content', collection.namespace),
        generate_digest('biggest content', collection.namespace)])
    key = model.entry_key(
        collection.namespace.namespace, collection.items[0].digest)

    # guarantee that one digest already exists in the datastore
    model.new_content_entry(key, content=collection.items[0].digest).put()
    response = self.call_api(
        'preupload', self.message_to_dict(collection), 200)

    # we should see one enqueued task and two new URLs in the response
    self.assertEqual(2, len(json.loads(response.body)['items']))

    # remove tasks so tearDown doesn't complain
    _ = self.execute_tasks()

  def test_check_existing_enqueues_tasks(self):
    """Assert that existent entities are enqueued."""
    collection = DigestCollection(namespace=endpoint_handlers_api.Namespace())
    collection.items.append(
        generate_digest('some content', collection.namespace))
    key = model.entry_key(
        collection.namespace.namespace, collection.items[0].digest)

    # guarantee that one digest already exists in the datastore
    model.new_content_entry(key, content=collection.items[0].digest).put()
    _response = self.call_api(
        'preupload', self.message_to_dict(collection), 200)

    # find enqueued tasks
    enqueued_tasks = self.execute_tasks()
    self.assertEqual(1, enqueued_tasks)

  def test_store_inline_ok(self):
    """Assert that inline content storage completes successfully."""
    request = generate_store_request('sibilance')
    embedded = validate(request.upload_ticket, 'datastore')
    key = model.entry_key(embedded['n'], embedded['d'])

    # assert that store_inline puts the correct entity into the datastore
    _response = self.call_api(
        'store_inline', self.message_to_dict(request), 200)
    stored = key.get()
    self.assertEqual(key, stored.key)

    # assert that expected (digest, size) pair is generated by stored content
    self.assertEqual(
        (embedded['d'].encode('utf-8'), int(embedded['s'])),
        endpoint_handlers_api.hash_content(stored.content, embedded['n']))

  def test_store_inline_bad_mac(self):
    """Assert that inline content storage fails when token is altered.

    TODO(cmassaro): more comprehensive tests (expired tokens, etc.)?
    """
    request = generate_store_request('sonority')
    request.upload_ticket += '7'
    with self.call_should_fail('400'):
      _response = self.call_api(
          'store_inline', self.message_to_dict(request), 200)

  def test_store_inline_bad_digest(self):
    """Assert that inline content storage fails when data do not match."""
    request = generate_store_request('anseres sacri')
    request.content = ':)' + request.content[2:]
    with self.call_should_fail('400'):
      _response = self.call_api(
          'store_inline', self.message_to_dict(request), 200)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
