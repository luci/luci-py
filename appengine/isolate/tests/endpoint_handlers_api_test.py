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
from endpoint_handlers_api import UPLOAD_MESSAGES
import gcs
import handlers_backend
import model


def make_private_key():
  new_key = RSA.generate(1024)
  pem_key = base64.b64encode(new_key.exportKey('PEM'))
  config.settings().gs_private_key = pem_key


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
  return endpoint_handlers_api.Digest(
      digest=hash_content(content, namespace), size=len(content))


def generate_collection(contents, namespace=None):
  if namespace is None:
    namespace = endpoint_handlers_api.Namespace()
  return DigestCollection(
      namespace=namespace,
      items=[generate_digest(content, namespace.namespace)
             for content in contents])


def generate_embedded(namespace, digest):
  return {
      'c': namespace.compression,
      'd': digest.digest,
      'h': namespace.digest_hash,
      'i': str(int(digest.is_isolated)),
      'n': namespace.namespace,
      's': str(digest.size),
  }


def preupload_status_to_request(preupload_status, content):
  """Create a Storage/FinalizeRequest corresponding to a PreuploadStatus."""
  ticket = preupload_status.get('upload_ticket')
  url = preupload_status.get('gs_upload_url', None)
  if url is not None:
    return endpoint_handlers_api.FinalizeRequest(upload_ticket=ticket)
  return endpoint_handlers_api.StorageRequest(
      upload_ticket=ticket, content=content)


validate = endpoint_handlers_api.TokenSigner.validate


def pad_string(string, size=endpoint_handlers_api.MIN_SIZE_FOR_DIRECT_GS):
  pad = ''.join('0' for unused_i in xrange(size + 1 - len(string)))
  return string + pad


class FileInfaux(object):
  """Fake file info to mock GCS retrieval."""

  def __init__(self, content):
    self.size = len(content)


def get_file_info_factory(content=None):
  """Return a function to mock gcs.get_file_info."""
  result = None if content is None else FileInfaux(content)
  return lambda unused_bucket, unused_id: result


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

  def store_request(self, content):
    """Generate a Storage/FinalizeRequest via preupload status."""
    collection = generate_collection([content])
    response = self.call_api(
        'preupload', self.message_to_dict(collection), 200)
    message = json.loads(response.body).get(u'items', [{}])[0]
    return preupload_status_to_request(message, content)

  def test_pre_upload_ok(self):
    """Assert that preupload correctly posts a valid DigestCollection.

    TODO(cmassaro): verify the upload_ticket is correct
    """
    good_digests = generate_collection(['a pony'])
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
    digests = generate_collection([pad_string('duckling')])
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
        namespace=endpoint_handlers_api.Namespace(namespace='~tildewhatevs'))
    bad_collection.items.append(
        generate_digest('pangolin', bad_collection.namespace.namespace))
    with self.call_should_fail('400'):
      _response = self.call_api(
          'preupload', self.message_to_dict(bad_collection), 200)

  def test_check_existing_finds_existing_entities(self):
    """Assert that existence check is working."""
    collection = generate_collection(
        ['small content', 'larger content', 'biggest content'])
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
    request = self.store_request('sibilance')
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[0])
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
    request = self.store_request(pad_string('sonority'))
    request.upload_ticket += '7'
    with self.call_should_fail('400'):
      _response = self.call_api(
          'store_inline', self.message_to_dict(request), 200)

  def test_store_inline_bad_digest(self):
    """Assert that inline content storage fails when data do not match."""
    request = self.store_request('anseres sacri')
    request.content = ':)' + request.content[2:]
    with self.call_should_fail('400'):
      _response = self.call_api(
          'store_inline', self.message_to_dict(request), 200)

  def test_finalized_data_in_gs(self):
    """Assert that data are actually in GS when finalized."""
    # create content
    content = pad_string('huge, important data')
    request = self.store_request(content)

    # this should succeed
    self.mock(gcs, 'get_file_info', get_file_info_factory(content))
    _response = self.call_api(
        'finalize_gs_upload', self.message_to_dict(request), 200)

    # this should fail
    self.mock(gcs, 'get_file_info', get_file_info_factory())
    with self.call_should_fail('400'):
      _response = self.call_api(
          'finalize_gs_upload', self.message_to_dict(request), 200)
    self.assertEqual(1, self.execute_tasks())

  def test_finalize_gs_creates_content_entry(self):
    """Assert that finalize_gs_upload creates a content entry."""
    content = pad_string('empathy')
    request = self.store_request(content)
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[1])
    key = model.entry_key(embedded['n'], embedded['d'])

    # finalize_gs_upload should put a new ContentEntry into the database
    self.mock(gcs, 'get_file_info', get_file_info_factory(content))
    _response = self.call_api(
        'finalize_gs_upload', self.message_to_dict(request), 200)
    stored = key.get()
    self.assertEqual(key, stored.key)

    # assert that expected attributes are present
    self.assertEqual('', stored.content)
    self.assertEqual(int(embedded['s']), stored.expanded_size)

    # ensure that verification occurs
    self.mock(gcs, 'read_file', lambda _bucket, _key: content)
    self.assertFalse(stored.key.get().is_verified)
    self.assertEqual(1, self.execute_tasks())
    self.assertTrue(stored.key.get().is_verified)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
