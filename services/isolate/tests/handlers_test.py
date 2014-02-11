#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest
import urllib

import test_env
test_env.setup_test_env()

# From tools/third_party/
import webtest

# For TestCase.
import test_case

import handlers

from components import auth


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


class MainTest(test_case.TestCase):
  """Tests the handlers."""
  def setUp(self):
    """Creates a new app instance for every test case."""
    super(MainTest, self).setUp()
    self.testbed.init_modules_stub()
    app = handlers.CreateApplication()
    self.source_ip = '127.0.0.1'
    self.testapp = webtest.TestApp(
        app, extra_environ={'REMOTE_ADDR': self.source_ip})

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
    return req.json['access_token']

  @staticmethod
  def preupload_foo():
    """Returns data to send to /pre-upload to upload 'foo'."""
    h = handlers.get_hash_algo('default')
    h.update('foo')
    return [
      {
        'h': h.hexdigest(),
        's': 3,
        'i': 0,
      },
    ]

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
        '/content-gs/pre-upload/a?token=%s' % urllib.quote(self.handshake()),
        self.preupload_foo())
    self.assertEqual(1, len(req.json))
    self.assertEqual(2, len(req.json[0]))
    # ['url', None]
    self.assertTrue(req.json[0][0])
    self.assertEqual(None, req.json[0][1])

  def test_pre_upload_invalid_namespace(self):
    req = self.testapp.post_json(
        '/content-gs/pre-upload/[?token=%s' % urllib.quote(self.handshake()),
        self.preupload_foo(),
        expect_errors=True)
    self.assertTrue(
        'Invalid namespace; allowed keys must pass regexp "[a-z0-9A-Z\-._]+"' in
        req.body)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
