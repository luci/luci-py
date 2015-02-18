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

from google.appengine.api import memcache
from google.appengine.api import urlfetch


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
  return string + '0' * (size - len(string))


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

  api_service_cls = endpoint_handlers_api.IsolateService
  gs_prefix = 'localhost:80/content-gs/store/'
  store_prefix = 'https://isolateserver-dev.storage.googleapis.com/'

  APP_DIR = ROOT_DIR

  def setUp(self):
    super(IsolateServiceTest, self).setUp()
    self.testbed.init_blobstore_stub()
    self.testbed.init_urlfetch_stub()
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
    message = response.json.get(u'items', [{}])[0]
    return preupload_status_to_request(message, content)

  def mock_side_effect(self, original_object, attribute_name, side_effect):
    """Add a side effect to a mocked attribute, preserving functionality."""
    original_function = getattr(original_object, attribute_name)
    def original_with_side_effect(*args, **kwargs):
      side_effect()
      return original_function(*args, **kwargs)
    self.mock(original_object, attribute_name, original_with_side_effect)

  def test_pre_upload_ok(self):
    """Assert that preupload correctly posts a valid DigestCollection."""
    good_digests = generate_collection(['a pony'])
    response = self.call_api(
        'preupload', self.message_to_dict(good_digests), 200)
    message = response.json.get(u'items', [{}])[0]
    self.assertEqual('', message.get(u'gs_upload_url', ''))
    self.assertEqual(
        validate(message.get(u'upload_ticket', ''), UPLOAD_MESSAGES[0]),
        generate_embedded(good_digests.namespace, good_digests.items[0]))

  def test_finalize_url_ok(self):
    """Assert that a finalize_url is generated when should_push_to_gs."""
    digests = generate_collection([pad_string('duckling')])
    response = self.call_api(
        'preupload', self.message_to_dict(digests), 200)
    message = response.json.get(u'items', [{}])[0]
    self.assertTrue(message.get(u'gs_upload_url', '').startswith(
        self.store_prefix))
    self.assertEqual(
        validate(message.get(u'upload_ticket', ''), UPLOAD_MESSAGES[1]),
        generate_embedded(digests.namespace, digests.items[0]))

  def test_pre_upload_invalid_hash(self):
    """Assert that status 400 is returned when the digest is invalid."""
    bad_collection = DigestCollection(
        namespace=endpoint_handlers_api.Namespace())
    bad_digest = hash_content('some stuff', bad_collection.namespace.namespace)
    bad_digest = 'g' + bad_digest[1:]  # that's not hexadecimal!
    bad_collection.items.append(
        endpoint_handlers_api.Digest(digest=bad_digest, size=10))
    with self.call_should_fail('400'):
      self.call_api(
          'preupload', self.message_to_dict(bad_collection), 200)

  def test_pre_upload_invalid_namespace(self):
    """Assert that status 400 is returned when the namespace is invalid."""
    bad_collection = DigestCollection(
        namespace=endpoint_handlers_api.Namespace(namespace='~tildewhatevs'))
    bad_collection.items.append(
        generate_digest('pangolin', bad_collection.namespace.namespace))
    with self.call_should_fail('400'):
      self.call_api(
          'preupload', self.message_to_dict(bad_collection), 200)

  def test_check_existing_finds_existing_entities(self):
    """Assert that existence check is working."""
    collection = generate_collection(
        ['small content', 'larger content', 'biggest content'])
    key = model.entry_key(
        collection.namespace.namespace, collection.items[0].digest)

    # guarantee that one digest already exists in the datastore
    model.new_content_entry(key).put()
    response = self.call_api(
        'preupload', self.message_to_dict(collection), 200)

    # we should see one enqueued task and two new URLs in the response
    items = response.json['items']
    self.assertEqual(2, len(items))
    self.assertEqual([1, 2], [int(item['index']) for item in items])
    for item in items:
      self.assertIsNotNone(item.get('upload_ticket'))

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
    model.new_content_entry(key).put()
    self.call_api(
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
    self.call_api(
        'store_inline', self.message_to_dict(request), 200)
    stored = key.get()
    self.assertEqual(key, stored.key)

    # assert that expected (digest, size) pair is generated by stored content
    self.assertEqual(
        (embedded['d'].encode('utf-8'), int(embedded['s'])),
        endpoint_handlers_api.hash_content(stored.content, embedded['n']))

  def test_store_inline_empty_content(self):
    """Assert that inline content storage works when content is empty."""
    request = self.store_request('')
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[0])
    key = model.entry_key(embedded['n'], embedded['d'])

    # assert that store_inline puts the correct entity into the datastore
    self.call_api(
        'store_inline', self.message_to_dict(request), 200)
    stored = key.get()
    self.assertEqual(key, stored.key)

    # assert that expected (digest, size) pair is generated by stored content
    self.assertEqual(
        (embedded['d'].encode('utf-8'), int(embedded['s'])),
        endpoint_handlers_api.hash_content(stored.content, embedded['n']))

  def test_store_inline_bad_mac(self):
    """Assert that inline content storage fails when token is altered."""
    request = self.store_request('sonority')
    request.upload_ticket += '7'
    with self.call_should_fail('400'):
      self.call_api(
          'store_inline', self.message_to_dict(request), 200)

  def test_store_inline_no_upload_ticket(self):
    """Assert that inline content storage fails when there is no ticket."""
    request = self.store_request('silence')
    request.upload_ticket = None
    with self.call_should_fail('400'):
      self.call_api(
          'store_inline', self.message_to_dict(request), 200)

  def test_store_inline_bad_digest(self):
    """Assert that inline content storage fails when data do not match."""
    request = self.store_request('anseres sacri')
    request.content = ':)' + request.content[2:]
    with self.call_should_fail('400'):
      self.call_api(
          'store_inline', self.message_to_dict(request), 200)

  def test_finalized_data_in_gs(self):
    """Assert that data are actually in GS when finalized."""
    # create content
    content = pad_string('huge, important data')
    request = self.store_request(content)

    # this should succeed
    self.mock(gcs, 'get_file_info', get_file_info_factory(content))
    self.call_api(
        'finalize_gs_upload', self.message_to_dict(request), 200)

    # this should fail
    self.mock(gcs, 'get_file_info', get_file_info_factory())
    with self.call_should_fail('400'):
      self.call_api(
          'finalize_gs_upload', self.message_to_dict(request), 200)
    self.assertEqual(1, self.execute_tasks())

  def test_finalized_no_upload_ticket(self):
    """Assert that GS finalization fails when there is no ticket."""
    request = self.store_request(pad_string('silence'))
    request.upload_ticket = None
    with self.call_should_fail('400'):
      self.call_api(
          'finalize_gs_upload', self.message_to_dict(request), 200)

  def test_finalize_gs_creates_content_entry(self):
    """Assert that finalize_gs_upload creates a content entry."""
    content = pad_string('empathy')
    request = self.store_request(content)
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[1])
    key = model.entry_key(embedded['n'], embedded['d'])

    # finalize_gs_upload should put a new ContentEntry into the database
    self.mock(gcs, 'get_file_info', get_file_info_factory(content))
    self.call_api(
        'finalize_gs_upload', self.message_to_dict(request), 200)
    stored = key.get()
    self.assertEqual(key, stored.key)

    # assert that expected attributes are present
    self.assertEqual(None, stored.content)
    self.assertEqual(int(embedded['s']), stored.expanded_size)

    # ensure that verification occurs
    self.mock(gcs, 'read_file', lambda _bucket, _key: content)

    # add a side effect in execute_tasks()
    # TODO(cmassaro): there must be a better way than this
    def set_verified():
      stored_entry = stored.key.get()
      if not stored_entry.is_verified:
        stored_entry.is_verified = True
    self.mock_side_effect(self._taskqueue_stub, 'DeleteTask', set_verified)

    # assert that verification occurs in the taskqueue
    self.assertFalse(stored.key.get().is_verified)
    self.assertEqual(1, self.execute_tasks())
    self.assertTrue(stored.key.get().is_verified)

  def test_storage_wrong_type(self):
    """Assert that GS and inline storage fail when the wrong type is sent."""
    small = 'elephant'
    large = pad_string('mouse')

    small_request = self.store_request(small)
    large_request = self.store_request(large)

    with self.call_should_fail('400'):
      self.call_api(
          'finalize_gs_upload', self.message_to_dict(small_request), 200)
    with self.call_should_fail('400'):
      self.call_api(
          'store_inline', self.message_to_dict(large_request), 200)

  def test_retrieve_memcache_ok(self):
    """Assert that content retrieval goes off swimmingly in the normal case."""
    content = 'Grecian Urn'
    request = self.store_request(content)
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[0])
    self.call_api(
        'store_inline', self.message_to_dict(request), 200)
    retrieve_request = endpoint_handlers_api.RetrieveRequest(
        digest=embedded['d'], namespace=endpoint_handlers_api.Namespace())
    response = self.call_api(
        'retrieve', self.message_to_dict(retrieve_request), 200)
    retrieved = response.json
    self.assertEqual(content, base64.b64decode(retrieved.get(u'content', '')))

  def test_retrieve_db_ok(self):
    """Assert that content retrieval works for non-memcached DB entities."""
    content = 'Isabella, or the Pot of Basil'
    request = self.store_request(content)
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[0])
    self.call_api(
        'store_inline', self.message_to_dict(request), 200)
    retrieve_request = endpoint_handlers_api.RetrieveRequest(
        digest=embedded['d'], namespace=endpoint_handlers_api.Namespace())
    memcache.flush_all()
    response = self.call_api(
        'retrieve', self.message_to_dict(retrieve_request), 200)
    retrieved = response.json
    self.assertEqual(content, base64.b64decode(retrieved.get(u'content', '')))

  def test_retrieve_gs_url_ok(self):
    """Assert that URL retrieval works for GS entities."""

    # get URL via preupload
    content = pad_string('Lycidas')
    collection = generate_collection([content])
    preupload_status = self.call_api(
        'preupload', self.message_to_dict(collection), 200)
    message = preupload_status.json.get(u'items', [{}])[0]

    # finalize GS upload
    request = preupload_status_to_request(message, content)
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[1])
    self.mock(gcs, 'get_file_info', get_file_info_factory(content))
    self.call_api(
        'finalize_gs_upload', self.message_to_dict(request), 200)

    # retrieve the upload URL
    retrieve_request = endpoint_handlers_api.RetrieveRequest(
        digest=embedded['d'], namespace=endpoint_handlers_api.Namespace())
    retrieved_response = self.call_api(
        'retrieve', self.message_to_dict(retrieve_request), 200)
    retrieved = retrieved_response.json
    self.assertNotEqual(message.get(u'gs_upload_url', ''), '')
    self.assertNotEqual(retrieved.get(u'url', ''), '')
    self.assertTrue(retrieved.get(u'url', '').startswith(self.store_prefix))

    # clear the taskqueue
    self.assertEqual(1, self.execute_tasks())

  def test_retrieve_partial_ok(self):
    """Assert that content retrieval works when a range is specified."""
    content = 'Song of the Andoumboulou'
    offset = 5
    request = self.store_request(content)
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[0])
    self.call_api(
        'store_inline', self.message_to_dict(request), 200)
    retrieve_request = endpoint_handlers_api.RetrieveRequest(
        digest=embedded['d'],
        namespace=endpoint_handlers_api.Namespace(),
        offset=offset)  # TODO(cmassaro): determine where offsets come from
    response = self.call_api(
        'retrieve', self.message_to_dict(retrieve_request), 200)
    retrieved = response.json
    self.assertEqual(content[offset:], base64.b64decode(retrieved.get(
        u'content', '')))

  def test_retrieve_partial_bad_offset_fails(self):
    """Assert that retrieval fails with status 416 when offset is invalid."""
    content = 'Of Man\'s first Disobedience, and the Fruit'
    request = self.store_request(content)
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[0])
    self.call_api(
        'store_inline', self.message_to_dict(request), 200)
    requests = [endpoint_handlers_api.RetrieveRequest(
        digest=embedded['d'],
        namespace=endpoint_handlers_api.Namespace(),
        offset=offset) for offset in [-1, len(content) + 1]]
    for request in requests:
      with self.call_should_fail('400'):
        self.call_api('retrieve', self.message_to_dict(request), 200)

  def test_retrieve_not_found(self):
    """Assert that HTTP 404 response is served when content is absent."""

    # get a valid digest
    content = """\xe1\xbc\x84\xce\xbd\xce\xb4\xcf\x81\xce\xb1
        \xce\xbc\xce\xbf\xce\xb9
        \xe1\xbc\x94\xce\xbd\xce\xbd\xce\xb5\xcf\x80\xce\xb5"""
    collection = generate_collection([content])
    preupload_status = self.call_api(
        'preupload', self.message_to_dict(collection), 200)
    message = preupload_status.json.get(u'items', [{}])[0]

    # get the digest
    request = preupload_status_to_request(message, content)
    embedded = validate(request.upload_ticket, UPLOAD_MESSAGES[0])

    # don't upload data; try to retrieve
    retrieve_request = endpoint_handlers_api.RetrieveRequest(
        digest=embedded['d'], namespace=endpoint_handlers_api.Namespace())
    with self.call_should_fail('404'):
      self.call_api(
          'retrieve', self.message_to_dict(retrieve_request), 200)

  def test_server_details_ok(self):
    """Assert that server_details returns the correct version."""
    response = self.call_api('server_details', {}, 200).json
    self.assertEqual(utils.get_app_version(), response['server_version'])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
