#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import json
import sys
import unittest

import test_env
test_env.setup_test_env()

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
from google.appengine.ext import testbed
# pylint: enable=E0611,F0401

from components import ereporter2
from depot_tools import auto_stub


# W0212: Access to a protected member XXX of a client class
# pylint: disable=W0212


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
  return ereporter2.ErrorRecord(**default_values)


def ignorer(error_record):
  return ereporter2.should_ignore_error_record(
      ['Process terminated because the request deadline was exceeded during a '
        'loading request.',],
      ['DeadlineExceededError'],
      error_record)

class TestCase(auto_stub.TestCase):
  """Support class to enable google.appengine.api.mail.send_mail_to_admins()."""
  def setUp(self):
    super(TestCase, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    # Using init_all_stubs() costs ~10ms more to run all the tests.
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_mail_stub()
    self.mail_stub = self.testbed.get_stub(testbed.MAIL_SERVICE_NAME)
    self.old_send_to_admins = self.mock(
        self.mail_stub, '_Dynamic_SendToAdmins', self._SendToAdmins)

  def tearDown(self):
    self.testbed.deactivate()
    super(TestCase, self).tearDown()

  def _SendToAdmins(self, request, *args, **kwargs):
    """Make sure the request is logged.

    See google_appengine/google/appengine/api/mail_stub.py around line 299,
    MailServiceStub._SendToAdmins().
    """
    self.mail_stub._CacheMessage(request)
    return self.old_send_to_admins(request, *args, **kwargs)


class Ereporter2Test(TestCase):
  def assertContent(self, message):
    self.assertEqual(
        u'no_reply@isolateserver-dev.appspotmail.com', message.sender)
    self.assertEqual(u'Exceptions on "isolateserver-dev"', message.subject)
    expected_html = (
        u'<html><body><h3><a href="http://foo/report?start=10&end=20">1 '
        'occurrences of 1 errors across 1 versions.</a></h3>\n\n'
        '<span style="font-size:130%">Failed@v1</span><br>\nmain.app<br>\n'
        'GET localhost/foo (HTTP 200)<br>\n<pre>Failed</pre>\n'
        '1 occurrences: <a href="http://foo/request/a">Entry</a> <p>\n<br>\n'
        '</body></html>')
    self.assertEqual(expected_html, message.html.payload)
    expected_text = (
        '1 occurrences of 1 errors across 1 versions.\n\n'
        'Failed@v1\nmain.app\nGET localhost/foo (HTTP 200)\nFailed\n'
        '1 occurrences: Entry \n\n')
    self.assertEqual(expected_text, message.body.payload)

  def test_email_no_recipients(self):
    data = [
      ErrorRecord(),
    ]
    self.mock(ereporter2, '_extract_exceptions_from_logs', lambda *_: data)
    result = ereporter2.generate_and_email_report(
        start_time=10,
        end_time=20,
        module_versions=[],
        ignorer=ignorer,
        recipients=None,
        request_id_url='http://foo/request/',
        report_url='http://foo/report',
        title_template=ereporter2.REPORT_TITLE_TEMPLATE,
        content_template=ereporter2.REPORT_CONTENT_TEMPLATE,
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
    self.mock(ereporter2, '_extract_exceptions_from_logs', lambda *_: data)
    result = ereporter2.generate_and_email_report(
        start_time=10,
        end_time=20,
        module_versions=[],
        ignorer=ignorer,
        recipients='joe@example.com',
        request_id_url='http://foo/request/',
        report_url='http://foo/report',
        title_template=ereporter2.REPORT_TITLE_TEMPLATE,
        content_template=ereporter2.REPORT_CONTENT_TEMPLATE,
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
        '  File \"ereporter2.py\", line 74\n'
        '    class ErrorReportingInfo(ndb.Model):\n'
        '        ^\n'
        'SyntaxError: invalid syntax'),
        'SyntaxError@ereporter2.py:74',
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
      signature, exception_type = ereporter2.signature_from_message(message)
      self.assertEqual(expected_signature, signature)
      self.assertEqual(excepted_exception, exception_type)
      result = ereporter2.should_ignore_error_record(
          IGNORED_LINES,
          IGNORED_EXCEPTIONS,
          ErrorRecordStub(message, exception_type))
      self.assertEqual(expected_ignored, result, message)

  def assertEqualObj(self, a, b):
    """Makes complex objects easier to diff."""
    a_str = json.dumps(
        ereporter2.serialize(a), indent=2, sort_keys=True).splitlines()
    b_str = json.dumps(
        ereporter2.serialize(b), indent=2, sort_keys=True).splitlines()
    self.assertEqual(a_str, b_str)

  def test_generate_report(self):
    msg = ereporter2.STACK_TRACE_MARKER + '\nDeadlineExceededError'
    data = [
      ErrorRecord(),
      ErrorRecord(message=msg),
      ErrorRecord(),
    ]
    self.mock(ereporter2, '_extract_exceptions_from_logs', lambda *_: data)
    report, ignored = ereporter2.generate_report(10, 20, None, ignorer)
    expected_report = ereporter2.ErrorCategory(
        'Failed@v1', 'v1', 'default', 'Failed', '/foo')
    expected_report.events = [
      ErrorRecord(),
      ErrorRecord(),
    ]
    self.assertEqualObj([expected_report], report)
    expected_ignored = ereporter2.ErrorCategory(
        'DeadlineExceededError@None:-1@v1', 'v1', 'default', msg, '/foo')
    expected_ignored.events = [
      ErrorRecord(message=msg),
    ]
    self.assertEqualObj([expected_ignored], ignored)

  def test_report_to_html(self):
    msg = ereporter2.STACK_TRACE_MARKER + '\nDeadlineExceededError'
    data = [
      ErrorRecord(),
      ErrorRecord(message=msg),
      ErrorRecord(),
    ]
    self.mock(ereporter2, '_extract_exceptions_from_logs', lambda *_: data)
    module_versions = [('foo', 'bar')]
    report, ignored = ereporter2.generate_report(
        10, 20, module_versions, ignorer)
    env = ereporter2.get_template_env(10, 20, module_versions)
    out = ereporter2.report_to_html(
        report, ignored,
        ereporter2.REPORT_TITLE_TEMPLATE,
        ereporter2.REPORT_HEADER_TEMPLATE,
        ereporter2.REPORT_CONTENT_TEMPLATE,
        'http://foo/request_id', env)
    expected = (
      '<html>\n<head><title>Exceptions on "isolateserver-dev"</title></head>\n'
      '<body><h2>Report for 1970-01-01 00:00:10 (10) to 1970-01-01 00:00:20 '
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
      '1 occurrences: <a href="http://foo/request_ida">Entry</a> <p>\n<br>\n'
      '</body></html>')
    self.assertEqual(expected, out)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
