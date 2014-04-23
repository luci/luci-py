#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
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

# From components/third_party/
import webtest

# From components/tools/, for TestCase.
import test_case

import handlers

from components import auth

# Access to a protected member _XXX of a client class
# pylint: disable=W0212


def _ErrorRecord(**kwargs):
  """Returns an ErrorRecord filled with default dummy values."""
  default_values = {
      'request_id': 'a',
      'start_time': None,
      'exception_time': None,
      'latency': 0,
      'mcycles': 0,
      'ip': '0.0.1.0',
      'nickname': None,
      'referrer': None,
      'user_agent': 'Comodore64',
      'host': 'localhost',
      'resource': '/foo',
      'method': 'GET',
      'task_queue_name': None,
      'was_loading_request': False,
      'version': 'v1',
      'module': 'default',
      'handler_module': 'main.app',
      'gae_version': '1.9.0',
      'instance': '123',
      'status': 200,
      'message': 'Failed',
  }
  default_values.update(kwargs)
  return handlers.ereporter2.ErrorRecord(**default_values)


def hash_item(content):
  h = handlers.get_hash_algo('default')
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
  def setUp(self):
    """Creates a new app instance for every test case."""
    super(MainTest, self).setUp()
    self.testbed.init_modules_stub()
    self.testbed.init_taskqueue_stub()
    self.taskqueue_stub = self.testbed.get_stub(
        test_case.testbed.TASKQUEUE_SERVICE_NAME)
    self.taskqueue_stub._root_path = ROOT_DIR

    # When called during a taskqueue, the call to get_app_version() may fail so
    # pre-fetch it.
    version = handlers.config.get_app_version()
    self.mock(handlers.config, 'get_task_queue_host', lambda: version)
    self.source_ip = '127.0.0.1'
    self.testapp = webtest.TestApp(
        handlers.CreateApplication(debug=True),
        extra_environ={'REMOTE_ADDR': self.source_ip})

  def whitelist_self(self):
    handlers.acl.WhitelistedIP(
        id=handlers.acl.ip_to_str(*handlers.acl.parse_ip(self.source_ip)),
        ip='127.0.0.1').put()

  def handshake(self):
    self.whitelist_self()
    data = {
      'client_app_version': '0.2',
      'fetcher': True,
      'protocol_version': handlers.ISOLATE_PROTOCOL_VERSION,
      'pusher': True,
    }
    req = self.testapp.post_json('/content-gs/handshake', data)
    return urllib.quote(req.json['access_token'])

  def execute_tasks(self):
    """Executes enqueued tasks that are ready to run and return the number run.

    A task may trigger another task.

    Sadly, taskqueue_stub implementation does not provide a nice way to run
    them so run the pending tasks manually.
    """
    self.assertEqual([None], self.taskqueue_stub._queues.keys())
    ran_total = 0
    while True:
      # Do multiple loops until no task was run.
      ran = 0
      for queue in self.taskqueue_stub.GetQueues():
        for task in self.taskqueue_stub.GetTasks(queue['name']):
          # Remove 2 seconds for jitter.
          eta = task['eta_usec'] / 1e6 - 2
          if eta >= time.time():
            continue
          self.assertEqual('POST', task['method'])
          logging.info('Task: %s', task['url'])

          # Not 100% sure why the Content-Length hack is needed:
          body = base64.b64decode(task['body'])
          headers = dict(task['headers'])
          headers['Content-Length'] = str(len(body))
          try:
            response = self.testapp.post(task['url'], body, headers=headers)
          except:
            logging.error(task)
            raise
          # TODO(maruel): Implement task failure.
          self.assertEqual(200, response.status_code)
          self.taskqueue_stub.DeleteTask(queue['name'], task['name'])
          ran += 1
      if not ran:
        return ran_total
      ran_total += ran

  def mock_delete_files(self):
    deleted = []
    def delete_files(bucket, files, ignore_missing=False):
      # pylint: disable=W0613
      self.assertEquals('isolateserver-dev', bucket)
      deleted.extend(files)
      return []
    self.mock(handlers.gcs, 'delete_files', delete_files)
    return deleted

  def put_content(self, url, content):
    """Simulare isolateserver.py archive."""
    req = self.testapp.put(
        url, content_type='application/octet-stream', params=content)
    self.assertEqual(200, req.status_code)
    self.assertEqual({'entry':{}}, req.json)

  # Test cases.

  def test_internal_cron_ereporter2_mail_not_cron(self):
    response = self.testapp.get(
        '/internal/cron/ereporter2/mail', expect_errors=True)
    self.assertEqual(response.status_int, 403)
    self.assertEqual(
        response.normal_body,
        '403 Forbidden Access was denied to this resource. Must be a cron '
        'request. ')
    self.assertEqual(response.content_type, 'text/plain')
    # Verify no email was sent.
    self.assertEqual([], self.mail_stub.get_sent_messages())

  def test_internal_cron_ereporter2_mail(self):
    data = [_ErrorRecord()]
    self.mock(
        handlers.ereporter2, '_extract_exceptions_from_logs', lambda *_: data)
    headers = {'X-AppEngine-Cron': 'true'}
    response = self.testapp.get(
        '/internal/cron/ereporter2/mail', headers=headers)
    self.assertEqual(response.status_int, 200)
    self.assertEqual(response.normal_body, 'Success.')
    self.assertEqual(response.content_type, 'text/plain')
    # Verify the email was sent.
    messages = self.mail_stub.get_sent_messages()
    self.assertEqual(1, len(messages))
    message = messages[0]
    self.assertFalse(hasattr(message, 'to'))
    expected_text = (
      '1 occurrences of 1 errors across 1 versions.\n\n'
      'Failed@v1\nmain.app\nGET localhost/foo (HTTP 200)\nFailed\n'
      '1 occurrences: Entry \n\n')
    self.assertEqual(expected_text, message.body.payload)

  def test_known_auth_resources(self):
    # This test is supposed to catch typos and new types of auth resources.
    # It walks over all AuthenticatedHandler routes and ensures @require
    # decorator use resources from this set.
    expected = {
      'auth/management',
      'auth/management/groups/{group}',
      'isolate/management',
      'isolate/namespaces/',
      'isolate/namespaces/{namespace}',
    }
    for route in auth.get_authenticated_routes(handlers.CreateApplication()):
      per_method = route.handler.get_methods_permissions()
      for method, permissions in per_method.iteritems():
        self.assertTrue(
            expected.issuperset(resource for _, resource in permissions),
            msg='Unexpected auth resource in %s of %s: %s' %
                (method, route, permissions))

  def test_pre_upload_ok(self):
    req = self.testapp.post_json(
        '/content-gs/pre-upload/a?token=%s' % self.handshake(),
        [gen_item('foo')])
    self.assertEqual(1, len(req.json))
    self.assertEqual(2, len(req.json[0]))
    # ['url', None]
    self.assertTrue(req.json[0][0])
    self.assertEqual(None, req.json[0][1])

  def test_pre_upload_invalid_namespace(self):
    req = self.testapp.post_json(
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
    self.mock(handlers, 'utcnow', lambda: now)
    self.mock(handlers.ndb.DateTimeProperty, '_now', lambda _: now)
    self.mock(handlers.ndb.DateProperty, '_now', lambda _: now.date())

    r = self.testapp.post_json(
        '/content-gs/pre-upload/default?token=%s' % self.handshake(),
        [gen_item(i) for i in items])
    self.assertEqual(len(items), len(r.json))
    self.assertEqual(0, len(list(handlers.ContentEntry.query())))

    for content, urls in zip(items, r.json):
      self.assertEqual(2, len(urls))
      self.assertEqual(None, urls[1])
      self.put_content(urls[0], content)
    self.assertEqual(2, len(list(handlers.ContentEntry.query())))
    expiration = 7*24*60*60
    self.assertEqual(0, self.execute_tasks())

    # Advance time, tag the first item.
    now += datetime.timedelta(seconds=2*expiration)
    self.assertEqual(
        datetime.datetime(2012, 01, 16, 03, 04, 05, 06), handlers.utcnow())
    r = self.testapp.post_json(
        '/content-gs/pre-upload/default?token=%s' % self.handshake(),
        [gen_item(items[0])])
    self.assertEqual(200, r.status_code)
    self.assertEqual([None], r.json)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(2, len(list(handlers.ContentEntry.query())))

    # 'bar' was kept, 'foo' was cleared out.
    resp = self.testapp.get('/internal/cron/cleanup/trigger/old')
    self.assertEqual(200, resp.status_code)
    self.assertEqual([None], r.json)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(1, len(list(handlers.ContentEntry.query())))
    self.assertEqual('bar', handlers.ContentEntry.query().get().content)

    # Advance time and force cleanup. This deletes 'bar' too.
    now += datetime.timedelta(seconds=2*expiration)
    resp = self.testapp.get('/internal/cron/cleanup/trigger/old')
    self.assertEqual(200, resp.status_code)
    self.assertEqual([None], r.json)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(0, len(list(handlers.ContentEntry.query())))

    # Advance time and force cleanup.
    now += datetime.timedelta(seconds=2*expiration)
    resp = self.testapp.get('/internal/cron/cleanup/trigger/old')
    self.assertEqual(200, resp.status_code)
    self.assertEqual([None], r.json)
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(0, len(list(handlers.ContentEntry.query())))

    # All items expired are tried to be deleted from GS. This is the trade off
    # between having to fetch the items vs doing unneeded requests to GS for the
    # inlined objects.
    expected = sorted('default/' + hash_item(i) for i in items)
    self.assertEqual(expected, sorted(deleted))

  def test_ancestor_assumption(self):
    prefix = '1234'
    suffix = 40 - len(prefix)
    c = handlers.create_entry(handlers.entry_key('n', prefix + '0' * suffix))
    self.assertEqual(0, len(list(handlers.ContentEntry.query())))
    c.put()
    self.assertEqual(1, len(list(handlers.ContentEntry.query())))

    c = handlers.create_entry(handlers.entry_key('n', prefix + '1' * suffix))
    self.assertEqual(1, len(list(handlers.ContentEntry.query())))
    c.put()
    self.assertEqual(2, len(list(handlers.ContentEntry.query())))

    actual_prefix = c.key.parent().id()
    k = handlers.datastore_utils.shard_key(
        actual_prefix, len(actual_prefix), 'ContentShard')
    self.assertEqual(2, len(list(handlers.ContentEntry.query(ancestor=k))))

  def test_trim_missing(self):
    deleted = self.mock_delete_files()
    def gen_file(i, t=0):
      return (i, handlers.gcs.cloudstorage.GCSFileStat(i, 100, 'etag', t))
    mock_files = [
        # Was touched.
        gen_file('d/' + '0' * 40),
        # Is deleted.
        gen_file('d/' + '1' * 40),
        # Too recent.
        gen_file('d/' + '2' * 40, time.time() - 60),
    ]
    self.mock(handlers.gcs, 'list_files', lambda _: mock_files)

    handlers.ContentEntry(key=handlers.entry_key('d', '0' * 40)).put()
    self.testapp.get('/internal/cron/cleanup/trigger/trim_lost')
    self.assertEqual(1, self.execute_tasks())
    self.assertEqual(['d/' + '1' * 40], deleted)

  def test_verify(self):
    # Upload a file larger than MIN_SIZE_FOR_DIRECT_GS and ensure the verify
    # task works.
    data = '0' * handlers.MIN_SIZE_FOR_DIRECT_GS
    req = self.testapp.post_json(
        '/content-gs/pre-upload/default?token=%s' % self.handshake(),
        [gen_item(data)])
    self.assertEqual(1, len(req.json))
    self.assertEqual(2, len(req.json[0]))
    # ['url', 'url']
    self.assertTrue(req.json[0][0])
    # Fake the upload by calling the second function.
    self.mock(
        handlers.gcs, 'get_file_info',
        lambda _b, _f: handlers.gcs.FileInfo(size=len(data)))
    req = self.testapp.post(req.json[0][1], '')

    self.mock(handlers.gcs, 'read_file', lambda _b, _f: [data])
    self.assertEqual(1, self.execute_tasks())

    # Assert the object is still there.
    self.assertEqual(1, len(list(handlers.ContentEntry.query())))

  def test_verify_corrupted(self):
    # Upload a file larger than MIN_SIZE_FOR_DIRECT_GS and ensure the verify
    # task works.
    data = '0' * handlers.MIN_SIZE_FOR_DIRECT_GS
    req = self.testapp.post_json(
        '/content-gs/pre-upload/default?token=%s' % self.handshake(),
        [gen_item(data)])
    self.assertEqual(1, len(req.json))
    self.assertEqual(2, len(req.json[0]))
    # ['url', 'url']
    self.assertTrue(req.json[0][0])
    # Fake the upload by calling the second function.
    self.mock(
        handlers.gcs, 'get_file_info',
        lambda _b, _f: handlers.gcs.FileInfo(size=len(data)))
    req = self.testapp.post(req.json[0][1], '')

    # Fake corruption
    data_corrupted = '1' * handlers.MIN_SIZE_FOR_DIRECT_GS
    self.mock(handlers.gcs, 'read_file', lambda _b, _f: [data_corrupted])
    deleted = self.mock_delete_files()
    self.assertEqual(1, self.execute_tasks())

    # Assert the object is gone.
    self.assertEqual(0, len(list(handlers.ContentEntry.query())))
    self.assertEqual(['default/' + hash_item(data)], deleted)

  def _gen_stats(self):
    # Generates data for the last 10 days, last 10 hours and last 10 minutes.
    now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock(handlers.stats.stats_framework, 'utcnow', lambda: now)
    handler = handlers.stats.get_stats_handler()
    for i in xrange(10):
      s = handlers.stats.Snapshot(requests=100 + i)
      day = (now - datetime.timedelta(days=i)).date()
      handler.stats_day_cls(key=handler.day_key(day), values=s).put()

    for i in xrange(10):
      s = handlers.stats.Snapshot(requests=10 + i)
      timestamp = (now - datetime.timedelta(hours=i))
      handler.stats_hour_cls(key=handler.hour_key(timestamp), values=s).put()

    for i in xrange(10):
      s = handlers.stats.Snapshot(requests=1 + i)
      timestamp = (now - datetime.timedelta(minutes=i))
      handler.stats_minute_cls(
          key=handler.minute_key(timestamp), values=s).put()

  def test_stats(self):
    self._gen_stats()
    response = self.testapp.get('/stats')
    # Just ensure something is returned.
    self.assertGreater(response.content_length, 4000)

  def test_api_stats_days(self):
    self._gen_stats()
    # It's cheezy but at least it asserts that the data makes sense.
    expected = (
        'google.visualization.Query.setResponse({"status":"ok","table":{"rows":'
        '[{"c":[{"v":"Date(2010,0,2)"},{"v":100},{"v":100},{"v":0},{"v":0},{"v"'
        ':0},{"v":0},{"v":0},{"v":0},{"v":0}]}],"cols":[{"type":"date","id":"ke'
        'y","label":"Day"},{"type":"number","id":"requests","label":"Total"},{"'
        'type":"number","id":"other_requests","label":"Other"},{"type":"number"'
        ',"id":"failures","label":"Failures"},{"type":"number","id":"uploads","'
        'label":"Uploads"},{"type":"number","id":"downloads","label":"Downloads'
        '"},{"type":"number","id":"contains_requests","label":"Lookups"},{"type'
        '":"number","id":"uploads_bytes","label":"Uploaded"},{"type":"number","'
        'id":"downloads_bytes","label":"Downloaded"},{"type":"number","id":"con'
        'tains_lookups","label":"Items looked up"}]},"reqId":"0","version":"0.6'
        '"});')
    response = self.testapp.get('/isolate/api/v1/stats/days?duration=1')
    self.assertEqual(expected, response.body)

  def test_api_stats_hours(self):
    self._gen_stats()
    # It's cheezy but at least it asserts that the data makes sense.
    expected = (
        'google.visualization.Query.setResponse({"status":"ok","table":{"rows":'
        '[{"c":[{"v":"Date(2010,0,2,3,0,0)"},{"v":10},{"v":10},{"v":0},{"v":0},'
        '{"v":0},{"v":0},{"v":0},{"v":0},{"v":0}]}],"cols":[{"type":"datetime",'
        '"id":"key","label":"Time"},{"type":"number","id":"requests","label":"T'
        'otal"},{"type":"number","id":"other_requests","label":"Other"},{"type"'
        ':"number","id":"failures","label":"Failures"},{"type":"number","id":"u'
        'ploads","label":"Uploads"},{"type":"number","id":"downloads","label":"'
        'Downloads"},{"type":"number","id":"contains_requests","label":"Lookups'
        '"},{"type":"number","id":"uploads_bytes","label":"Uploaded"},{"type":"'
        'number","id":"downloads_bytes","label":"Downloaded"},{"type":"number",'
        '"id":"contains_lookups","label":"Items looked up"}]},"reqId":"0","vers'
        'ion":"0.6"});')
    response = self.testapp.get('/isolate/api/v1/stats/hours?duration=1&now=')
    self.assertEqual(expected, response.body)

  def test_api_stats_minutes(self):
    self._gen_stats()
    # It's cheezy but at least it asserts that the data makes sense.
    expected = (
        'google.visualization.Query.setResponse({"status":"ok","table":{"rows":'
        '[{"c":[{"v":"Date(2010,0,2,3,4,0)"},{"v":1},{"v":1},{"v":0},{"v":0},{"'
        'v":0},{"v":0},{"v":0},{"v":0},{"v":0}]}],"cols":[{"type":"datetime","i'
        'd":"key","label":"Time"},{"type":"number","id":"requests","label":"Tot'
        'al"},{"type":"number","id":"other_requests","label":"Other"},{"type":"'
        'number","id":"failures","label":"Failures"},{"type":"number","id":"upl'
        'oads","label":"Uploads"},{"type":"number","id":"downloads","label":"Do'
        'wnloads"},{"type":"number","id":"contains_requests","label":"Lookups"}'
        ',{"type":"number","id":"uploads_bytes","label":"Uploaded"},{"type":"nu'
        'mber","id":"downloads_bytes","label":"Downloaded"},{"type":"number","i'
        'd":"contains_lookups","label":"Items looked up"}]},"reqId":"0","versio'
        'n":"0.6"});')
    response = self.testapp.get('/isolate/api/v1/stats/minutes?duration=1')
    self.assertEqual(expected, response.body)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
