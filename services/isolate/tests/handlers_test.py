#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

import test_env
test_env.setup_test_env()

# From tools/third_party/
import webtest

# For TestCase.
import test_case

import handlers


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
    self.testapp = webtest.TestApp(app)

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


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
