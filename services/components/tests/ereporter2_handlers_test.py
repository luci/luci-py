#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import json
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

import webapp2

from google.appengine.api import logservice

# From components/third_party/
import webtest

from components import auth
from components import template
from components.ereporter2 import handlers
from components.ereporter2 import logscraper
from components.ereporter2 import models
from components.ereporter2 import on_error
from components.ereporter2 import testing
from components.ereporter2 import ui
from support import test_case


# Access to a protected member XXX of a client class - pylint: disable=W0212


def ErrorRecord(**kwargs):
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
  return logscraper._ErrorRecord(**default_values)


class Base(test_case.TestCase):
  def setUp(self):
    super(Base, self).setUp()
    self._now = datetime.datetime(2014, 6, 24, 20, 19, 42, 653775)
    testing.mock_now(self, self._now, 0)
    ui.configure(lambda *_: False)

  def tearDown(self):
    template.reset()
    super(Base, self).tearDown()


class Ereporter2FrontendTest(Base):
  def setUp(self):
    super(Ereporter2FrontendTest, self).setUp()
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(handlers.get_frontend_routes(), debug=True),
        extra_environ={'REMOTE_ADDR': '127.0.0.1'})

  def test_frontend(self):
    def is_group_member_mock(group, identity=None):
      return group == auth.model.ADMIN_GROUP or original(group, identity)
    original = self.mock(auth.api, 'is_group_member', is_group_member_mock)

    exception = (
      '/ereporter2/api/v1/on_error',
      r'/restricted/ereporter2/errors/<error_id:\d+>',
      '/restricted/ereporter2/request/<request_id:[0-9a-fA-F]+>',
    )
    for route in handlers.get_frontend_routes():
      if not route.template in exception:
        self.app.get(route.template, status=200)

    def gen_request(_request_id):
      # TODO(maruel): Fill up with fake data if found necessary to test edge
      # cases.
      return logservice.RequestLog()
    self.mock(logscraper, '_log_request_id', gen_request)
    self.app.get('/restricted/ereporter2/request/123', status=200)

  def test_on_error_handler(self):
    self.mock(logging, 'error', lambda *_: None)
    data = {
      'foo': 'bar',
    }
    for key in on_error.VALID_ERROR_KEYS:
      data[key] = 'bar %s' % key
    data['category'] = 'auth'
    data['duration'] = 2.3
    data['source'] = 'run_isolated'
    params = {
      'r': data,
      'v': '1',
    }
    response = self.app.post(
        '/ereporter2/api/v1/on_error', json.dumps(params), status=200,
        content_type='application/json; charset=utf-8').json

    self.assertEqual(1, models.Error.query().count())
    error_id = models.Error.query().get().key.integer_id()
    expected = {
      'id': error_id,
      'url': u'http://localhost/restricted/ereporter2/errors/%d' % error_id,
    }
    self.assertEqual(expected, response)

    def is_group_member_mock(group, identity=None):
      return group == auth.model.ADMIN_GROUP or original(group, identity)
    original = self.mock(auth.api, 'is_group_member', is_group_member_mock)
    self.app.get('/restricted/ereporter2/errors')
    self.app.get('/restricted/ereporter2/errors/%d' % error_id)

  def test_on_error_handler_denied(self):
    self.app.get('/ereporter2/api/v1/on_error', status=405)

  def test_on_error_handler_bad_type(self):
    self.mock(logging, 'error', lambda *_: None)
    params = {
      # 'args' should be a list.
      'r': {'args': 'bar'},
      'v': '1',
    }
    response = self.app.post(
        '/ereporter2/api/v1/on_error', json.dumps(params), status=200,
        content_type='application/json; charset=utf-8').json
    # There's still a response but it will be an error about the error.
    self.assertEqual(1, models.Error.query().count())
    error_id = models.Error.query().get().key.integer_id()
    self.assertEqual(response.get('id'), error_id)


class Ereporter2BackendTest(Base):
  def setUp(self):
    super(Ereporter2BackendTest, self).setUp()
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(handlers.get_backend_routes(), debug=True),
        extra_environ={'REMOTE_ADDR': '127.0.0.1'})

  def test_cron_ereporter2_mail_not_cron(self):
    self.mock(logging, 'error', lambda *_: None)
    response = self.app.get(
        '/internal/cron/ereporter2/mail', expect_errors=True)
    self.assertEqual(response.status_int, 403)
    self.assertEqual(response.content_type, 'text/plain')
    # Verify no email was sent.
    self.assertEqual([], self.mail_stub.get_sent_messages())

  def test_cron_ereporter2_mail(self):
    data = [ErrorRecord()]
    self.mock(logscraper, '_extract_exceptions_from_logs', lambda *_: data)
    self.mock(ui, '_get_recipients', lambda: ['joe@localhost'])
    headers = {'X-AppEngine-Cron': 'true'}
    response = self.app.get(
        '/internal/cron/ereporter2/mail', headers=headers)
    self.assertEqual(response.status_int, 200)
    self.assertEqual(response.normal_body, 'Success.')
    self.assertEqual(response.content_type, 'text/plain')
    # Verify the email was sent.
    messages = self.mail_stub.get_sent_messages()
    self.assertEqual(1, len(messages))
    message = messages[0]
    self.assertTrue(hasattr(message, 'to'), message.html)
    expected_text = (
      '1 occurrences of 1 errors across 1 versions.\n\n'
      'Failed@v1\nmain.app\nGET localhost/foo (HTTP 200)\nFailed\n'
      '1 occurrences: Entry \n\n')
    self.assertEqual(expected_text, message.body.payload)

  def test_cron_old_errors(self):
    self.mock(logging, 'error', lambda *_: None)
    kwargs = dict((k, k) for k in on_error.VALID_ERROR_KEYS)
    kwargs['category'] = 'exception'
    kwargs['duration'] = 2.3
    kwargs['source'] = 'bot'
    kwargs['source_ip'] = '0.0.0.0'
    on_error.log(**kwargs)

    # First call shouldn't delete the error since its not stale yet.
    headers = {'X-AppEngine-Cron': 'true'}
    response = self.app.get(
        '/internal/cron/ereporter2/cleanup', headers=headers)
    self.assertEqual('0', response.body)
    self.assertEqual(1, models.Error.query().count())

    # Set the current time to the future, but not too much.
    now = self._now + on_error.ERROR_TIME_TO_LIVE
    testing.mock_now(self, now, -60)

    headers = {'X-AppEngine-Cron': 'true'}
    response = self.app.get(
        '/internal/cron/ereporter2/cleanup', headers=headers)
    self.assertEqual('0', response.body)
    self.assertEqual(1, models.Error.query().count())

    # Set the current time to the future.
    now = self._now + on_error.ERROR_TIME_TO_LIVE
    testing.mock_now(self, now, 60)

    # Second call should remove the now stale error.
    headers = {'X-AppEngine-Cron': 'true'}
    response = self.app.get(
        '/internal/cron/ereporter2/cleanup', headers=headers)
    self.assertEqual('1', response.body)
    self.assertEqual(0, models.Error.query().count())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
