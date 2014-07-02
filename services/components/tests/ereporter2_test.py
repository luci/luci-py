#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import json
import logging
import os
import platform
import re
import sys
import unittest

import test_env
test_env.setup_test_env()

import webapp2

from google.appengine.api import logservice
from google.appengine.ext import ndb

# From components/third_party/
import webtest

from components import auth
from components import template
from components.ereporter2 import api
from components.ereporter2 import handlers
from components.ereporter2 import on_error
from components.ereporter2 import ui
from support import test_case


ABS_PATH = os.path.abspath(__file__)
ROOT_DIR = os.path.dirname(ABS_PATH)
ON_ERROR_PATH = os.path.abspath(on_error.__file__)


# Access to a protected member XXX of a client class - pylint: disable=W0212
# Method could be a function - pylint: disable=R0201

class ErrorRecordStub(object):
  """Intentionally thin stub to test should_ignore_error_record()."""
  def __init__(self, message, exception_type):
    self.message = message
    self.exception_type = exception_type


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
  return api.ErrorRecord(**default_values)


def get_backend():
  return webtest.TestApp(
      webapp2.WSGIApplication(handlers.get_backend_routes(), debug=True),
      extra_environ={'REMOTE_ADDR': '127.0.0.1'})


def get_frontend():
  return webtest.TestApp(
      webapp2.WSGIApplication(handlers.get_frontend_routes(), debug=True),
      extra_environ={'REMOTE_ADDR': '127.0.0.1'})


def ignorer(error_record):
  return api.should_ignore_error_record(
      ['Process terminated because the request deadline was exceeded during a '
        'loading request.',],
      ['DeadlineExceededError'],
      error_record)


def mock_now(test, now, seconds):
  """Mocks _utcnow() and ndb properties.

  In particular handles when auto_now and auto_now_add are used.
  """
  now = now + datetime.timedelta(seconds=seconds)
  test.mock(api, '_utcnow', lambda: now)
  test.mock(ndb.DateTimeProperty, '_now', lambda _: now)
  test.mock(ndb.DateProperty, '_now', lambda _: now.date())


class Ereporter2Test(test_case.TestCase):
  def setUp(self):
    super(Ereporter2Test, self).setUp()
    self.mock(ui, '_LOG_FILTER', None)
    self.mock(ui, '_get_end_time_for_email', lambda: 1383000000)
    self.mock(ui, 'get_recipients', lambda: ['foo@localhost'])
    ui.configure(ignorer)

  def tearDown(self):
    template.reset()
    super(Ereporter2Test, self).tearDown()

  def assertContent(self, message):
    self.assertEqual(
        u'no_reply@sample-app.appspotmail.com', message.sender)
    self.assertEqual(u'Exceptions on "sample-app"', message.subject)
    expected_html = (
        '<html><body><h3><a href="http://foo/report?start=0&end=1383000000">1 '
        'occurrences of 1 errors across 1 versions.</a></h3>\n\n'
        '<span style="font-size:130%">Failed@v1</span><br>\nmain.app<br>\n'
        'GET localhost/foo (HTTP 200)<br>\n<pre>Failed</pre>\n'
        '1 occurrences: <a href="http://foo/request/a">Entry</a> <p>\n<br>\n'
        '</body></html>')
    self.assertEqual(
        expected_html.splitlines(), message.html.payload.splitlines())
    expected_text = (
        '1 occurrences of 1 errors across 1 versions.\n\n'
        'Failed@v1\nmain.app\nGET localhost/foo (HTTP 200)\nFailed\n'
        '1 occurrences: Entry \n\n')
    self.assertEqual(expected_text, message.body.payload)

  def test_email_no_recipients(self):
    data = [
      ErrorRecord(),
    ]
    self.mock(api, '_extract_exceptions_from_logs', lambda *_: data)
    result = ui.generate_and_email_report(
        module_versions=[],
        ignorer=ignorer,
        recipients=None,
        request_id_url='http://foo/request/',
        report_url='http://foo/report',
        title_template_name='ereporter2/report_title.html',
        content_template_name='ereporter2/report_content.html',
        extras={})
    self.assertEqual(True, result)

    # Verify the email that was sent.
    messages = self.mail_stub.get_sent_messages()
    self.assertEqual(1, len(messages))
    message = messages[0]
    self.assertFalse(hasattr(message, 'to'))
    self.assertContent(message)

  def test_email_recipients(self):
    data = [
      ErrorRecord(),
    ]
    self.mock(api, '_extract_exceptions_from_logs', lambda *_: data)
    result = ui.generate_and_email_report(
        module_versions=[],
        ignorer=ignorer,
        recipients='joe@example.com',
        request_id_url='http://foo/request/',
        report_url='http://foo/report',
        title_template_name='ereporter2/report_title.html',
        content_template_name='ereporter2/report_content.html',
        extras={})
    self.assertEqual(True, result)

    # Verify the email that was sent.
    messages = self.mail_stub.get_sent_messages()
    self.assertEqual(1, len(messages))
    message = messages[0]
    self.assertEqual(u'joe@example.com', message.to)
    self.assertContent(message)

  def test_signatures(self):
    messages = [
      (
        ('\nTraceback (most recent call last):\n'
        '  File \"appengine/runtime/wsgi.py\", line 239, in Handle\n'
        '    handler = _config_handle.add_wsgi_middleware(self._LoadHandler())'
            '\n'
        '  File \"appengine/ext/ndb/utils.py\", line 28, in wrapping\n'
        '    def wrapping_wrapper(wrapper):\n'
        'DeadlineExceededError'),
        'DeadlineExceededError@utils.py:28',
        'DeadlineExceededError',
        True
      ),
      (
        ('/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
        'appengine/_internal/django/template/__init__.py:729: UserWarning: '
        'api_milliseconds does not return a meaningful value\n'
        '  current = current()'),
        '/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
            'appengine/_internal/django/template/__init__.py:729: UserWarning: '
            'api_milliseconds does not return a meaningful value',
        None,
        True,
      ),
      (
        ('\'error\' is undefined\n'
        'Traceback (most recent call last):\n'
        '  File \"tp/webapp2-2.5/webapp2.py\", line 1535, in __call__\n'
        '    rv = self.handle_exception(request, response, e)\n'
        '  File \"tp/jinja2-2.6/jinja2/environment.py\", line 894, in render\n'
        '    return self.environment.handle_exception(exc_info, True)\n'
        '  File \"<template>\", line 6, in top-level template code\n'
        '  File \"tp/jinja2-2.6/jinja2/environment.py\", line 372, in getattr\n'
        '    return getattr(obj, attribute)\n'
        'UndefinedError: \'error\' is undefined'),
        'UndefinedError@environment.py:372',
        'UndefinedError',
        False,
      ),
      (
        ('\nTraceback (most recent call last):\n'
        '  File \"api.py\", line 74\n'
        '    class ErrorReportingInfo(ndb.Model):\n'
        '        ^\n'
        'SyntaxError: invalid syntax'),
        'SyntaxError@api.py:74',
        'SyntaxError',
        False,
      ),
    ]

    IGNORED_LINES = [
      '/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
          'appengine/_internal/django/template/__init__.py:729: UserWarning: '
          'api_milliseconds does not return a meaningful value',
    ]
    IGNORED_EXCEPTIONS = [
      'DeadlineExceededError',
    ]
    for (message, expected_signature, excepted_exception,
         expected_ignored) in messages:
      signature, exception_type = api._signature_from_message(message)
      self.assertEqual(expected_signature, signature)
      self.assertEqual(excepted_exception, exception_type)
      result = api.should_ignore_error_record(
          IGNORED_LINES,
          IGNORED_EXCEPTIONS,
          ErrorRecordStub(message, exception_type))
      self.assertEqual(expected_ignored, result, message)

  def assertEqualObj(self, a, b):
    """Makes complex objects easier to diff."""
    a_str = json.dumps(
        api.serialize(a), indent=2, sort_keys=True).splitlines()
    b_str = json.dumps(
        api.serialize(b), indent=2, sort_keys=True).splitlines()
    self.assertEqual(a_str, b_str)

  def test_generate_report(self):
    msg = api._STACK_TRACE_MARKER + '\nDeadlineExceededError'
    data = [
      ErrorRecord(),
      ErrorRecord(message=msg),
      ErrorRecord(),
    ]
    self.mock(api, '_extract_exceptions_from_logs', lambda *_: data)
    report, ignored = api.generate_report(10, 20, None, ignorer)
    expected_report = api._ErrorCategory(
        'Failed@v1', 'v1', 'default', 'Failed', '/foo')
    expected_report.events = api._CappedList(
        api._ERROR_LIST_HEAD_SIZE,
        api._ERROR_LIST_TAIL_SIZE,
        [
          ErrorRecord(),
          ErrorRecord(),
        ],
    )
    self.assertEqualObj([expected_report], report)
    expected_ignored = api._ErrorCategory(
        'DeadlineExceededError@None:-1@v1', 'v1', 'default', msg, '/foo')
    expected_ignored.events = api._CappedList(
        api._ERROR_LIST_HEAD_SIZE,
        api._ERROR_LIST_TAIL_SIZE,
        [
          ErrorRecord(message=msg),
        ],
    )
    self.assertEqualObj([expected_ignored], ignored)

  def test_report_to_html(self):
    msg = api._STACK_TRACE_MARKER + '\nDeadlineExceededError'
    data = [
      ErrorRecord(),
      ErrorRecord(message=msg),
      ErrorRecord(),
    ]
    self.mock(api, '_extract_exceptions_from_logs', lambda *_: data)
    module_versions = [('foo', 'bar')]
    report, ignored = api.generate_report(
        10, 20, module_versions, ignorer)
    env = ui.get_template_env(10, 20, module_versions)
    out = ui.report_to_html(
        report, ignored,
        'ereporter2/report_header.html',
        'ereporter2/report_content.html',
        'http://foo/request_id', env)
    expected = (
      '<h2>Report for 1970-01-01 00:00:10 (10) to 1970-01-01 00:00:20 '
      '(20)</h2>\nModules-Versions:\n<ul><li>foo - bar</li>\n'
      '</ul><h3>2 occurrences of 1 errors across 1 versions.</h3>\n\n'
      '<span style="font-size:130%">Failed@v1</span><br>\nmain.app<br>\n'
      'GET localhost/foo (HTTP 200)<br>\n<pre>Failed</pre>\n'
      '2 occurrences: <a href="http://foo/request_ida">Entry</a> '
      '<a href="http://foo/request_ida">Entry</a> <p>\n<br>\n<hr>\n'
      '<h2>Ignored reports</h2>\n<h3>1 occurrences of 1 errors across 1 '
      'versions.</h3>\n\n<span style="font-size:130%">DeadlineExceededError@'
      'None:-1@v1</span><br>\nmain.app<br>\nGET localhost/foo (HTTP 200)<br>\n'
      '<pre>Traceback (most recent call last):\nDeadlineExceededError</pre>\n'
      '1 occurrences: <a href="http://foo/request_ida">Entry</a> <p>\n<br>\n')
    self.assertEqual(expected, out)

  def test_capped_list(self):
    l = api._CappedList(5, 10)

    # Grow a bit, should go to head.
    for i in xrange(5):
      l.append(i)
    self.assertFalse(l.has_gap)
    self.assertEqual(5, l.total_count)
    self.assertEqual(range(5), l.head)
    self.assertEqual(0, len(l.tail))

    # Start growing a tail, still not long enough to start evicting items.
    for i in xrange(5, 15):
      l.append(i)
    self.assertFalse(l.has_gap)
    self.assertEqual(15, l.total_count)
    self.assertEqual(range(5), l.head)
    self.assertEqual(range(5, 15), list(l.tail))

    # Adding one more item should evict oldest one ('5') from tail.
    l.append(15)
    self.assertTrue(l.has_gap)
    self.assertEqual(16, l.total_count)
    self.assertEqual(range(5), l.head)
    self.assertEqual(range(6, 16), list(l.tail))

  def test_relative_path(self):
    data = [
      os.getcwd(),
      os.path.dirname(os.path.dirname(os.path.dirname(api.runtime.__file__))),
      os.path.dirname(os.path.dirname(os.path.dirname(api.webapp2.__file__))),
      os.path.dirname(os.getcwd()),
      '.',
    ]
    for value in data:
      i = os.path.join(value, 'foo')
      self.assertEqual('foo', api.relative_path(i))

    self.assertEqual('bar/foo', api.relative_path('bar/foo'))

  def test_frontend(self):

    def is_group_member_mock(group, identity=None):
      return group == auth.model.ADMIN_GROUP or original(group, identity)
    original = self.mock(auth.api, 'is_group_member', is_group_member_mock)

    app_frontend = get_frontend()

    exception = (
      '/ereporter2/api/v1/on_error',
      r'/restricted/ereporter2/errors/<error_id:\d+>',
      '/restricted/ereporter2/request/<request_id:[0-9a-fA-F]+>',
    )
    for route in handlers.get_frontend_routes():
      if not route.template in exception:
        app_frontend.get(route.template, status=200)

    def gen_request(_request_id):
      # TODO(maruel): Fill up with fake data if found necessary to test edge
      # cases.
      return logservice.RequestLog()
    self.mock(api, 'log_request_id', gen_request)
    app_frontend.get('/restricted/ereporter2/request/123', status=200)

  def test_internal_cron_ereporter2_mail_not_cron(self):
    app_backend = get_backend()
    response = app_backend.get(
        '/internal/cron/ereporter2/mail', expect_errors=True)
    self.assertEqual(response.status_int, 403)
    self.assertEqual(response.content_type, 'text/plain')
    # Verify no email was sent.
    self.assertEqual([], self.mail_stub.get_sent_messages())

  def test_internal_cron_ereporter2_mail(self):
    app_backend = get_backend()
    data = [ErrorRecord()]
    self.mock(api, '_extract_exceptions_from_logs', lambda *_: data)
    headers = {'X-AppEngine-Cron': 'true'}
    response = app_backend.get(
        '/internal/cron/ereporter2/mail', headers=headers)
    self.assertEqual(response.status_int, 200)
    self.assertEqual(response.normal_body, 'Success.')
    self.assertEqual(response.content_type, 'text/plain')
    # Verify the email was sent.
    messages = self.mail_stub.get_sent_messages()
    self.assertEqual(1, len(messages))
    message = messages[0]
    self.assertTrue(hasattr(message, 'to'))
    expected_text = (
      '1 occurrences of 1 errors across 1 versions.\n\n'
      'Failed@v1\nmain.app\nGET localhost/foo (HTTP 200)\nFailed\n'
      '1 occurrences: Entry \n\n')
    self.assertEqual(expected_text, message.body.payload)


class Ereporter2RecipientsTest(test_case.TestCase):
  def test_recipients_from_auth_group(self):
    fake_group = [
      auth.Identity(auth.IDENTITY_USER, 'a@example.com'),
      auth.Identity(auth.IDENTITY_USER, 'b@example.com'),
      auth.Identity(auth.IDENTITY_SERVICE, 'blah-service'),
    ]
    self.mock(auth, 'list_group', lambda _: fake_group)
    self.assertEqual(['a@example.com', 'b@example.com'], ui.get_recipients())


class ErrorsTestNoAuth(test_case.TestCase):
  def setUp(self):
    super(ErrorsTestNoAuth, self).setUp()
    self.mock(logging, 'error', lambda *_, **_kwargs: None)
    self._now = datetime.datetime(2014, 6, 24, 20, 19, 42, 653775)
    mock_now(self, self._now, 0)

  def test_log(self):
    # It must work even if auth is not initialized.
    error_id = on_error.log(
        source='bot', category='task_failure', message='Dang')
    self.assertEqual(1, on_error.Error.query().count())
    self.assertEqual(error_id, on_error.Error.query().get().key.integer_id())
    expected = {
      'args': [],
      'category': u'task_failure',
      'created_ts': self._now,
      'cwd': None,
      'duration': None,
      'endpoint': None,
      'env': None,
      'exception_type': None,
      'hostname': None,
      'identity': None,
      'message': u'Dang',
      'os': None,
      'python_version': None,
      'source': u'bot',
      'source_ip': None,
      'stack': None,
      'user': None,
      'version': None,
    }
    self.assertEqual(expected, on_error.Error.query().get().to_dict())


class ErrorsTest(test_case.TestCase):
  def setUp(self):
    super(ErrorsTest, self).setUp()
    self.mock(logging, 'error', lambda *_, **_kwargs: None)
    self._now = datetime.datetime(2014, 6, 24, 20, 19, 42, 653775)
    mock_now(self, self._now, 0)
    auth.configure([
        auth.oauth_authentication,
        auth.cookie_authentication,
        auth.service_to_service_authentication,
    ])

    self.mock(ui, '_LOG_FILTER', None)
    ui.configure(lambda *_: False)

  def tearDown(self):
    template.reset()
    super(ErrorsTest, self).tearDown()

  def test_log(self):
    kwargs = dict((k, k) for k in on_error.VALID_ERROR_KEYS)
    kwargs['args'] = ['args']
    kwargs['category'] = 'exception'
    kwargs['duration'] = 2.3
    kwargs['env'] = {'foo': 'bar'}
    kwargs['source'] = 'bot'
    kwargs['source_ip'] = '0.0.0.0'
    on_error.log(**kwargs)
    self.assertEqual(1, on_error.Error.query().count())
    expected = {
      'args': [u'args'],
      'category': u'exception',
      'created_ts': self._now,
      'cwd': u'cwd',
      'duration': 2.3,
      'endpoint': u'endpoint',
      'env': {u'foo': u'bar'},
      'exception_type': u'exception_type',
      'hostname': u'hostname',
      'identity': None,
      'message': u'message',
      'os': u'os',
      'python_version': u'python_version',
      'source': u'bot',
      'source_ip': u'0.0.0.0',
      'stack': u'stack',
      'user': u'user',
      'version': u'version',
    }
    self.assertEqual(expected, on_error.Error.query().get().to_dict())

  def test_log_server(self):
    # version is automatiaclly added.
    on_error.log(source='server')
    self.assertEqual(1, on_error.Error.query().count())
    expected = dict((k, None) for k in on_error.VALID_ERROR_KEYS)
    expected['args'] = []
    expected['created_ts'] = self._now
    expected['identity'] = None
    expected['python_version'] = unicode(platform.python_version())
    expected['source'] = u'server'
    expected['source_ip'] = None
    expected['version'] = u'v1a'
    self.assertEqual(expected, on_error.Error.query().get().to_dict())

  def test_ignored_flag(self):
    on_error.log(foo='bar')
    self.assertEqual(1, on_error.Error.query().count())
    expected = {
      'args': [],
      'category': None,
      'created_ts': self._now,
      'cwd': None,
      'duration': None,
      'endpoint': None,
      'env': None,
      'exception_type': None,
      'hostname': None,
      'identity': None,
      'message': None,
      'os': None,
      'python_version': None,
      'source': u'unknown',
      'source_ip': None,
      'stack': None,
      'user': None,
      'version': None,
    }
    self.assertEqual(expected, on_error.Error.query().get().to_dict())

  def test_exception(self):
    on_error.log(env='str')
    self.assertEqual(1, on_error.Error.query().count())
    relpath_on_error = api.relative_path(ON_ERROR_PATH)
    expected = {
      'args': [],
      'category': u'exception',
      'created_ts': self._now,
      'cwd': None,
      'duration': None,
      'endpoint': None,
      'env': None,
      'exception_type': u'<type \'exceptions.TypeError\'>',
      'hostname': None,
      'identity': None,
      'message':
          u'log({\'env\': \'str\'}) caused: JSON property must be a '
          u'<type \'dict\'>',
      'os': None,
      'python_version': None,
      'source': u'server',
      'source_ip': None,
      'stack':
          u'Traceback (most recent call last):\n'
          u'  File "%s", line 0, in log\n'
          u'    error = Error(identity=identity, **kwargs)\n'
          u'  File "appengine/ext/ndb/model.py", line 0, in __init__\n' %
            relpath_on_error.replace('.pyc', '.py'),
      'user': None,
      'version': None,
    }
    actual = on_error.Error.query().get().to_dict()
    # Zap out line numbers to 0, it's annoying otherwise to update the unit test
    # just for line move. Only keep the first 4 lines because json_dict
    # verification is a tad deep insode ndb/model.py.
    actual['stack'] = ''.join(
        re.sub(r' \d+', ' 0', actual['stack']).splitlines(True)[:4])
    # Also make no distinction between *.pyc and *.py files.
    actual['stack'] = actual['stack'].replace('.pyc', '.py')
    self.assertEqual(expected, actual)

  def test_con_old_errors(self):
    kwargs = dict((k, k) for k in on_error.VALID_ERROR_KEYS)
    kwargs['category'] = 'exception'
    kwargs['duration'] = 2.3
    kwargs['source'] = 'bot'
    kwargs['source_ip'] = '0.0.0.0'
    on_error.log(**kwargs)

    # First call shouldn't delete the error since its not stale yet.
    app_backend = get_backend()
    headers = {'X-AppEngine-Cron': 'true'}
    response = app_backend.get(
        '/internal/cron/ereporter2/cleanup', headers=headers)
    self.assertEqual('0', response.body)
    self.assertEqual(1, on_error.Error.query().count())

    # Set the current time to the future, but not too much.
    now = self._now + on_error.ERROR_TIME_TO_LIVE
    mock_now(self, now, -60)

    headers = {'X-AppEngine-Cron': 'true'}
    response = app_backend.get(
        '/internal/cron/ereporter2/cleanup', headers=headers)
    self.assertEqual('0', response.body)
    self.assertEqual(1, on_error.Error.query().count())

    # Set the current time to the future.
    now = self._now + on_error.ERROR_TIME_TO_LIVE
    mock_now(self, now, 60)

    # Second call should remove the now stale error.
    headers = {'X-AppEngine-Cron': 'true'}
    response = app_backend.get(
        '/internal/cron/ereporter2/cleanup', headers=headers)
    self.assertEqual('1', response.body)
    self.assertEqual(0, on_error.Error.query().count())

  def test_on_error_handler(self):
    app_frontend = get_frontend()
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
    response = app_frontend.post(
        '/ereporter2/api/v1/on_error', json.dumps(params), status=200,
        content_type='application/json; charset=utf-8').json

    self.assertEqual(1, on_error.Error.query().count())
    error_id = on_error.Error.query().get().key.integer_id()
    expected = {
      'id': error_id,
      'url': u'http://localhost/restricted/ereporter2/errors/%d' % error_id,
    }
    self.assertEqual(expected, response)

    def is_group_member_mock(group, identity=None):
      return group == auth.model.ADMIN_GROUP or original(group, identity)
    original = self.mock(auth.api, 'is_group_member', is_group_member_mock)
    app_frontend.get('/restricted/ereporter2/errors')
    app_frontend.get('/restricted/ereporter2/errors/%d' % error_id)

  def test_on_error_handler_denied(self):
    app_frontend = get_frontend()
    app_frontend.get('/ereporter2/api/v1/on_error', status=405)

  def test_on_error_handler_bad_type(self):
    app_frontend = get_frontend()

    params = {
      # 'args' should be a list.
      'r': {'args': 'bar'},
      'v': '1',
    }
    response = app_frontend.post(
        '/ereporter2/api/v1/on_error', json.dumps(params), status=200,
        content_type='application/json; charset=utf-8').json
    # There's still a response but it will be an error about the error.
    self.assertEqual(1, on_error.Error.query().count())
    error_id = on_error.Error.query().get().key.integer_id()
    self.assertEqual(response.get('id'), error_id)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
