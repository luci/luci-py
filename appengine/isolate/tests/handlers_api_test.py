#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import sys
import time
import unittest
import urllib

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

import webapp2
import webtest

from components import auth
from components import utils
from test_support import test_case

import acl
import config
import gcs
import handlers_api
import handlers_backend
import model

# Access to a protected member _XXX of a client class
# pylint: disable=W0212


def hash_item(content):
  h = model.get_hash_algo('default')
  h.update(content)
  return h.hexdigest()


def gen_item(content):
  """Returns data to send to /pre-upload to upload 'content'."""
  return {
    'h': hash_item(content),
    'i': 0,
    's': len(content),
  }


class MainTest(test_case.TestCase):
  """Tests the handlers."""
  APP_DIR = ROOT_DIR

  def setUp(self):
    """Creates a new app instance for every test case."""
    super(MainTest, self).setUp()
    self.testbed.init_user_stub()

    # When called during a taskqueue, the call to get_app_version() may fail so
    # pre-fetch it.
    version = utils.get_app_version()
    self.mock(utils, 'get_task_queue_host', lambda: version)
    self.source_ip = '192.168.0.1'
    self.app_api = webtest.TestApp(
        webapp2.WSGIApplication(handlers_api.get_routes(), debug=True),
        extra_environ={'REMOTE_ADDR': self.source_ip})
    # Do not use handlers_backend.create_application() because it also
    # initializes ereporter2 cron jobs, which requires templates. We want to
    # make sure templates are not needed for APIs.
    self.app_backend = webtest.TestApp(
        webapp2.WSGIApplication(handlers_backend.get_routes(), debug=True),
        extra_environ={'REMOTE_ADDR': self.source_ip})
    # Tasks are enqueued on the backend.
    self.app = self.app_backend

  def whitelist_self(self):
    auth.bootstrap_ip_whitelist(auth.BOTS_IP_WHITELIST, [self.source_ip])

  def mock_acl_checks(self):
    known_groups = (
      acl.FULL_ACCESS_GROUP,
      acl.READONLY_ACCESS_GROUP,
    )
    def is_group_member_mock(group):
      if auth.get_current_identity().is_anonymous:
        return False
      return group in known_groups
    self.mock(auth, 'is_group_member', is_group_member_mock)

  def handshake(self):
    self.whitelist_self()
    self.mock_acl_checks()
    data = {
      'client_app_version': '0.2',
      'fetcher': True,
      'protocol_version': handlers_api.ISOLATE_PROTOCOL_VERSION,
      'pusher': True,
    }
    req = self.app_api.post_json('/content-gs/handshake', data)
    return urllib.quote(req.json['access_token'])

  def mock_delete_files(self):
    deleted = []
    def delete_files(bucket, files, ignore_missing=False):
      # pylint: disable=W0613
      self.assertEquals('isolateserver-dev', bucket)
      deleted.extend(files)
      return []
    self.mock(gcs, 'delete_files', delete_files)
    return deleted

  def put_content(self, url, content):
    """Simulare isolateserver.py archive."""
    req = self.app_api.put(
        url, content_type='application/octet-stream', params=content)
    self.assertEqual(200, req.status_code)
    self.assertEqual({'entry':{}}, req.json)

  # Test cases.

  def test_pre_upload_ok(self):
    req = self.app_api.post_json(
        '/content-gs/pre-upload/a?token=%s' % self.handshake(),
        [gen_item('foo')])
    self.assertEqual(1, len(req.json))
    self.assertEqual(2, len(req.json[0]))
    # ['url', None]
    self.assertTrue(req.json[0][0])
    self.assertEqual(None, req.json[0][1])

  def test_pre_upload_invalid_namespace(self):
    req = self.app_api.post_json(
        '/content-gs/pre-upload/[?token=%s' % self.handshake(),
        [gen_item('foo')],
        expect_errors=True)
    self.assertTrue(
        'Invalid namespace; allowed keys must pass regexp "[a-z0-9A-Z\-._]+"' in
        req.body)

  def test_upload_tag_expire(self):
    # Complete integration test that ensures tagged items are properly saved and
    # non tagged items are dumped.
    # Use small objects so it doesn't exercise the GS code path.
    deleted = self.mock_delete_files()
    items = ['bar', 'foo']
    now = datetime.datetime(2012, 01, 02, 03, 04, 05, 06)
    self.mock(utils, 'utcnow', lambda: now)
    self.mock(ndb.DateTimeProperty, '_now', lambda _: now)
    self.mock(ndb.DateProperty, '_now', lambda _: now.date())

    r = self.app_api.post_json(
        '/content-gs/pre-upload/default?token=%s' % self.handshake(),
        [gen_item(i) for i in items])
    self.assertEqual(len(items), len(r.json))
    self.assertEqual(0, len(list(model.ContentEntry.query())))

    for content, urls in zip(items, r.json):
      self.assertEqual(2, len(urls))
      self.assertEqual(None, urls[1])
      self.put_content(urls[0], content)
    self.assertEqual(2, len(list(model.ContentEntry.query())))
    expiration = config.settings().default_expiration
    self.assertEqual(0, self.execute_tasks())

    # Advance time, tag the first item.
    now += datetime.timedelta(seconds=2*expiration)
    r = self.app_api.post_json(
        '/content-gs/pre-upload/default?token=%s' % self.handshake(),
        [gen_item(items[0])])
    self.assertEqual(200, r.status_code)
    self.assertEqual([None], r.json)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(2, len(list(model.ContentEntry.query())))

    # 'bar' was kept, 'foo' was cleared out.
    headers = {'X-AppEngine-Cron': 'true'}
    resp = self.app_backend.get(
        '/internal/cron/cleanup/trigger/old', headers=headers)
    self.assertEqual(200, resp.status_code)
    self.assertEqual([None], r.json)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(1, len(list(model.ContentEntry.query())))
    self.assertEqual('bar', model.ContentEntry.query().get().content)

    # Advance time and force cleanup. This deletes 'bar' too.
    now += datetime.timedelta(seconds=2*expiration)
    headers = {'X-AppEngine-Cron': 'true'}
    resp = self.app_backend.get(
        '/internal/cron/cleanup/trigger/old', headers=headers)
    self.assertEqual(200, resp.status_code)
    self.assertEqual([None], r.json)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(0, len(list(model.ContentEntry.query())))

    # Advance time and force cleanup.
    now += datetime.timedelta(seconds=2*expiration)
    headers = {'X-AppEngine-Cron': 'true'}
    resp = self.app_backend.get(
        '/internal/cron/cleanup/trigger/old', headers=headers)
    self.assertEqual(200, resp.status_code)
    self.assertEqual([None], r.json)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(0, len(list(model.ContentEntry.query())))

    # All items expired are tried to be deleted from GS. This is the trade off
    # between having to fetch the items vs doing unneeded requests to GS for the
    # inlined objects.
    expected = sorted('default/' + hash_item(i) for i in items)
    self.assertEqual(expected, sorted(deleted))

  def test_trim_missing(self):
    deleted = self.mock_delete_files()
    def gen_file(i, t=0):
      return (i, gcs.cloudstorage.GCSFileStat(i, 100, 'etag', t))
    mock_files = [
        # Was touched.
        gen_file('d/' + '0' * 40),
        # Is deleted.
        gen_file('d/' + '1' * 40),
        # Too recent.
        gen_file('d/' + '2' * 40, time.time() - 60),
    ]
    self.mock(gcs, 'list_files', lambda _: mock_files)

    model.ContentEntry(key=model.entry_key('d', '0' * 40)).put()
    headers = {'X-AppEngine-Cron': 'true'}
    resp = self.app_backend.get(
        '/internal/cron/cleanup/trigger/trim_lost', headers=headers)
    self.assertEqual(200, resp.status_code)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(['d/' + '1' * 40], deleted)

  def test_verify(self):
    # Upload a file larger than MIN_SIZE_FOR_DIRECT_GS and ensure the verify
    # task works.
    data = '0' * handlers_api.MIN_SIZE_FOR_DIRECT_GS
    req = self.app_api.post_json(
        '/content-gs/pre-upload/default?token=%s' % self.handshake(),
        [gen_item(data)])
    self.assertEqual(1, len(req.json))
    self.assertEqual(2, len(req.json[0]))
    # ['url', 'url']
    self.assertTrue(req.json[0][0])
    # Fake the upload by calling the second function.
    self.mock(gcs, 'get_file_info', lambda _b, _f: gcs.FileInfo(size=len(data)))
    req = self.app_api.post(req.json[0][1], '')

    self.mock(gcs, 'read_file', lambda _b, _f: [data])
    self.assertEqual(1, self.execute_tasks())

    # Assert the object is still there.
    self.assertEqual(1, len(list(model.ContentEntry.query())))

  def test_verify_corrupted(self):
    # Upload a file larger than MIN_SIZE_FOR_DIRECT_GS and ensure the verify
    # task works.
    data = '0' * handlers_api.MIN_SIZE_FOR_DIRECT_GS
    req = self.app_api.post_json(
        '/content-gs/pre-upload/default?token=%s' % self.handshake(),
        [gen_item(data)])
    self.assertEqual(1, len(req.json))
    self.assertEqual(2, len(req.json[0]))
    # ['url', 'url']
    self.assertTrue(req.json[0][0])
    # Fake the upload by calling the second function.
    self.mock(gcs, 'get_file_info', lambda _b, _f: gcs.FileInfo(size=len(data)))
    req = self.app_api.post(req.json[0][1], '')

    # Fake corruption
    data_corrupted = '1' * handlers_api.MIN_SIZE_FOR_DIRECT_GS
    self.mock(gcs, 'read_file', lambda _b, _f: [data_corrupted])
    deleted = self.mock_delete_files()
    self.assertEqual(1, self.execute_tasks())

    # Assert the object is gone.
    self.assertEqual(0, len(list(model.ContentEntry.query())))
    self.assertEqual(['default/' + hash_item(data)], deleted)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
