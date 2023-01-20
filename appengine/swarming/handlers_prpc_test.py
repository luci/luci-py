#!/usr/bin/env vpython
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import os
import random
import sys
import unittest

import mock

import test_env_handlers

import webapp2
import webtest

from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.ext import ndb

from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2
from google.protobuf import duration_pb2

from test_support import test_case

from components import auth
from components import utils
from components import prpc
from components.prpc import encoding

from proto.api import swarming_pb2  # pylint: disable=no-name-in-module
from proto.api.internal.bb import backend_pb2
from proto.api.internal.bb import common_pb2
from proto.api.internal.bb import launcher_pb2
from proto.config import config_pb2

from server import config
from server import task_queues
from server import task_request
from server import task_result
from server import task_scheduler
from server import pools_config
from server import realms
from server import service_accounts
import handlers_bot
import handlers_prpc


def _decode(raw, dst):
  # Skip escaping characters.
  assert raw[:5] == ')]}\'\n', raw[:5]
  return encoding.get_decoder(encoding.Encoding.JSON)(raw[5:], dst)


def _encode(d):
  # Skip escaping characters.
  raw = encoding.get_encoder(encoding.Encoding.JSON)(d)
  assert raw[:5] == ')]}\'\n', raw[:5]
  return raw[5:]


class TaskBackendAPIServiceTest(test_env_handlers.AppTestBase):
  # These test fail with 'Unknown bot ID, not in config'
  # Need to run in sequential_test_runner.py
  no_run = 1

  def setUp(self):
    super(TaskBackendAPIServiceTest, self).setUp()
    s = prpc.Server()
    s.add_interceptor(auth.prpc_interceptor)
    s.add_service(handlers_prpc.TaskBackendAPIService())
    # TODO(crbug/1236848) call handlers_prpc.get_routes() when
    # the Backend is ready and added.
    routes = s.get_routes() + handlers_bot.get_routes()
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(routes + handlers_bot.get_routes(), debug=True),
        extra_environ={
            'REMOTE_ADDR': self.source_ip,
            'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        },
    )
    self._headers = {
        'Content-Type': encoding.Encoding.JSON[1],
        'Accept': encoding.Encoding.JSON[1],
    }

    now = datetime.datetime(2019, 1, 2, 3)
    test_case.mock_now(self, now, 0)

    self.mock_tq_tasks()

  # Test helpers.
  def _req_dim_prpc(self, key, value, exp_secs=None):
    # type: (str, str, Optional[int]) -> common_pb2.RequestedDimension
    dim = common_pb2.RequestedDimension(key=key, value=value)
    if exp_secs is not None:
      dim.expiration.seconds = exp_secs
    return dim

  def _basic_run_task_request(self):
    return backend_pb2.RunTaskRequest(
        secrets=launcher_pb2.BuildSecrets(build_token='tok'),
        realm='some:realm',
        build_id='42423',
        agent_args=['-fantasia', 'pegasus'],
        backend_config=struct_pb2.Struct(
            fields={
                'priority':
                    struct_pb2.Value(number_value=1),
                'wait_for_capacity':
                    struct_pb2.Value(bool_value=True),
                'bot_ping_tolerance':
                    struct_pb2.Value(number_value=70),
                'service_account':
                    struct_pb2.Value(string_value='who@serviceaccount.com'),
                'agent_binary_cipd_filename':
                    struct_pb2.Value(string_value='agent'),
                'agent_binary_cipd_pkg':
                    struct_pb2.Value(string_value='agent/package/${platform}'),
                'agent_binary_cipd_vers':
                    struct_pb2.Value(string_value='latest'),
            }),
        grace_period=duration_pb2.Duration(seconds=60),
        execution_timeout=duration_pb2.Duration(seconds=60),
        start_deadline=timestamp_pb2.Timestamp(
            seconds=int(utils.time_time() + 120)),
        dimensions=[self._req_dim_prpc('pool', 'default')],
        backend_token='token-token-token',
        buildbucket_host='cow-buildbucket.appspot.com',
    )

  # Tests
  def test_run_task(self):
    self.set_as_user()

    # Mocks for process_task_requests()
    # adds pool configs for which user is a `scheduling_user`
    self.mock_default_pool_acl([])
    self.mock(realms, 'check_tasks_create_in_realm', lambda *_: True)
    self.mock(realms, 'check_pools_create_task', lambda *_: True)
    self.mock(realms, 'check_tasks_act_as', lambda *_: True)
    self.mock(service_accounts, 'has_token_server', lambda: True)

    request = self._basic_run_task_request()
    request_id = 'cf60878f-8f2a-4f1e-b1f5-8b5ec88813a9'
    request.request_id = request_id
    self.app.post('/prpc/swarming.backend.TaskBackend/RunTask',
                  _encode(request), self._headers)
    self.assertEqual(1, task_request.TaskRequest.query().count())
    self.assertEqual(1, task_request.BuildToken.query().count())
    self.assertEqual(1, task_request.SecretBytes.query().count())
    request_idempotency_key = 'request_id/%s/%s' % (
        request_id, auth.get_current_identity().to_bytes())
    self.assertIsNotNone(
        memcache.get(request_idempotency_key, namespace='backend_run_task'))

    # Test requests are correctly deduped if `request_id` matches.
    self.app.post('/prpc/swarming.backend.TaskBackend/RunTask',
                  _encode(request), self._headers)
    self.assertEqual(1, task_request.TaskRequest.query().count())
    self.assertEqual(1, task_request.BuildToken.query().count())
    self.assertEqual(1, task_request.SecretBytes.query().count())

    # Test tasks with different `request_id`s are not deduped.
    request.request_id = 'cf60878f-8f2a-4f1e-b1f5-8b5ec88813a8'
    self.app.post('/prpc/swarming.backend.TaskBackend/RunTask',
                  _encode(request), self._headers)
    self.assertEqual(2, task_request.TaskRequest.query().count())
    self.assertEqual(2, task_request.BuildToken.query().count())
    self.assertEqual(2, task_request.SecretBytes.query().count())

  def test_run_task_exceptions_bad_conversion(self):
    self.set_as_user()

    request = backend_pb2.RunTaskRequest()
    raw_resp = self.app.post(
        '/prpc/swarming.backend.TaskBackend/RunTask',
        _encode(request),
        self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertIn(('X-Prpc-Grpc-Code', '3'), raw_resp._headerlist)
    self.assertIn('must be a valid package', raw_resp.body)

  def test_run_task_exceptions_schedule_request_error(self):
    self.set_as_user()

    # Mocks for process_task_requests()
    # adds pool configs for which user is a `scheduling_user`
    self.mock_default_pool_acl([])
    self.mock(realms, 'check_tasks_create_in_realm', lambda *_: True)
    self.mock(realms, 'check_pools_create_task', lambda *_: True)
    self.mock(realms, 'check_tasks_act_as', lambda *_: True)
    self.mock(service_accounts, 'has_token_server', lambda: True)

    # pylint: disable=unused-argument
    def mocked_schedule_request(_,
                                start_time=0,
                                secret_bytes=None,
                                build_token=None):
      raise TypeError('chicken')

    self.mock(task_scheduler, 'schedule_request', mocked_schedule_request)
    request = self._basic_run_task_request()
    raw_resp = self.app.post(
        '/prpc/swarming.backend.TaskBackend/RunTask',
        _encode(request),
        self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertIn(('X-Prpc-Grpc-Code', '3'), raw_resp._headerlist)
    self.assertEqual(raw_resp.body, 'chicken')

  def test_run_task_exceptions_auth_error(self):
    request = self._basic_run_task_request()
    raw_resp = self.app.post(
        '/prpc/swarming.backend.TaskBackend/RunTask',
        _encode(request),
        self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '403 Forbidden')
    self.assertIn(('X-Prpc-Grpc-Code', '7'), raw_resp._headerlist)
    self.assertEqual(raw_resp.body, 'User cannot create tasks.')

  @mock.patch('components.utils.enqueue_task')
  def test_cancel_tasks(self, mocked_enqueue_task):
    self.mock_default_pool_acl([])
    mocked_enqueue_task.return_value = True

    # Create bot
    self.set_as_bot()
    self.bot_poll()

    # Create two tasks, one COMPLETED, one PENDING

    # first request
    self.set_as_user()
    _, first_id = self.client_create_task_raw(
        name='first',
        tags=['project:yay', 'commit:post'],
        properties=dict(idempotent=True))
    self.set_as_bot()
    self.bot_run_task()

    # second request
    self.set_as_user()
    _, second_id = self.client_create_task_raw(
        name='second',
        user='jack@localhost',
        tags=['project:yay', 'commit:pre'])

    request = backend_pb2.CancelTasksRequest(task_ids=[
        backend_pb2.TaskID(id=str(first_id)),
        backend_pb2.TaskID(id=str(second_id)),
        backend_pb2.TaskID(id='1d69b9f088008810'),  # Does not exist.
    ])

    target = 'swarming://%s' % app_identity.get_application_id()
    expected_response = backend_pb2.CancelTasksResponse(tasks=[
      backend_pb2.Task(
            id=backend_pb2.TaskID(target=target, id=first_id),
            status=common_pb2.SUCCESS),
      # Task to cancel this should be enqueued
      backend_pb2.Task(
          id=backend_pb2.TaskID(target=target, id=second_id),
          status=common_pb2.SCHEDULED),
      backend_pb2.Task(
          id=backend_pb2.TaskID(target=target, id='1d69b9f088008810'),
          summary_html='Swarming task 1d69b9f088008810 not found',
          status=common_pb2.INFRA_FAILURE),
    ])

    self.mock_auth_db([auth.Permission('swarming.pools.cancelTask')])
    raw_resp = self.app.post(
        '/prpc/swarming.backend.TaskBackend/CancelTasks',
        _encode(request),
        self._headers,
        expect_errors=False)

    resp = backend_pb2.CancelTasksResponse()
    _decode(raw_resp.body, resp)
    self.assertEqual(resp, expected_response)

    utils.enqueue_task.assert_has_calls([
        mock.call(
            '/internal/taskqueue/important/tasks/cancel-children-tasks',
            'cancel-children-tasks',
            payload=mock.ANY),
        mock.call(
            '/internal/taskqueue/important/tasks/cancel',
            'cancel-tasks',
            payload='{"kill_running": true, "tasks": ["%s"]}' % second_id),
    ])

  def test_cancel_tasks_permission_denied(self):
    self.mock_default_pool_acl([])

    # Create task
    self.set_as_user()
    _, first_id = self.client_create_task_raw(
        name='first',
        tags=['project:yay', 'commit:post'],
        properties=dict(idempotent=True))

    request = backend_pb2.CancelTasksRequest(task_ids=[
        backend_pb2.TaskID(id=str(first_id)),
    ])

    self.mock_auth_db([])
    raw_resp = self.app.post(
        '/prpc/swarming.backend.TaskBackend/CancelTasks',
        _encode(request),
        self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '403 Forbidden')
    self.assertIn(('X-Prpc-Grpc-Code', '7'), raw_resp._headerlist)
    self.assertEqual(raw_resp.body, ('user "user@example.com" does not have '
                                     'permission "swarming.pools.cancelTask"'))

  @mock.patch('handlers_prpc._CANCEL_TASKS_LIMIT', 2)
  def test_cancel_tasks_too_many(self):
    self.set_as_user()

    request = backend_pb2.CancelTasksRequest(task_ids=[
        backend_pb2.TaskID(id=str("1")),
        backend_pb2.TaskID(id=str("2")),
        backend_pb2.TaskID(id=str("3")),
    ])
    raw_resp = self.app.post(
        '/prpc/swarming.backend.TaskBackend/CancelTasks',
        _encode(request),
        self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertIn(('X-Prpc-Grpc-Code', '3'), raw_resp._headerlist)
    self.assertEqual(raw_resp.body, ('Requesting 3 tasks for cancellation '
                                     'when the allowed max is 2.'))


  def test_fetch_tasks(self):
    self.mock_default_pool_acl([])

    # Create bot
    self.set_as_bot()
    self.bot_poll()

    # Create two tasks, one COMPLETED, one PENDING
    self.set_as_user()
    # first request
    _, first_id = self.client_create_task_raw(
        name='first',
        tags=['project:yay', 'commit:post'],
        properties=dict(idempotent=True))
    self.set_as_bot()
    self.bot_run_task()

    # Clear cache to test fetching from datastore path.
    ndb.get_context().clear_cache()
    memcache.flush_all()

    # second request
    self.set_as_user()
    _, second_id = self.client_create_task_raw(
        name='second',
        user='jack@localhost',
        tags=['project:yay', 'commit:pre'])

    request = backend_pb2.FetchTasksRequest(task_ids=[
        backend_pb2.TaskID(id=str(first_id)),
        backend_pb2.TaskID(id=str(second_id)),
        backend_pb2.TaskID(id='1d69b9f088008810'),  # Does not exist.
    ])

    target = 'swarming://%s' % app_identity.get_application_id()
    expected_response = backend_pb2.FetchTasksResponse(tasks=[
        backend_pb2.Task(
            id=backend_pb2.TaskID(target=target, id=first_id),
            status=common_pb2.SUCCESS),
        backend_pb2.Task(
            id=backend_pb2.TaskID(target=target, id=second_id),
            status=common_pb2.SCHEDULED),
        backend_pb2.Task(
            id=backend_pb2.TaskID(target=target, id='1d69b9f088008810'),
            summary_html='Swarming task 1d69b9f088008810 not found',
            status=common_pb2.INFRA_FAILURE),
    ])

    self.mock_auth_db([auth.Permission('swarming.pools.listTasks')])
    raw_resp = self.app.post('/prpc/swarming.backend.TaskBackend/FetchTasks',
                             _encode(request), self._headers)
    resp = backend_pb2.FetchTasksResponse()
    _decode(raw_resp.body, resp)
    self.assertEqual(resp, expected_response)

  def test_fetch_tasks_forbidden(self):
    self.mock_default_pool_acl([])

    # Create task
    self.set_as_user()
    # first request
    _, first_id = self.client_create_task_raw(
        name='first',
        tags=['project:yay', 'commit:post'],
        properties=dict(idempotent=True))

    request = backend_pb2.FetchTasksRequest(task_ids=[
        backend_pb2.TaskID(id=str(first_id)),
    ])

    self.mock_auth_db([])
    raw_resp = self.app.post(
        '/prpc/swarming.backend.TaskBackend/FetchTasks',
        _encode(request),
        self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '403 Forbidden')
    self.assertIn(('X-Prpc-Grpc-Code', '7'), raw_resp._headerlist)
    self.assertEqual(raw_resp.body, ('user "user@example.com" does not have '
                                     'permission "swarming.pools.listTasks"'))

  @mock.patch('handlers_prpc._FETCH_TASKS_LIMIT', 2)
  def test_fetch_tasks_too_many(self):
    self.set_as_user()

    request = backend_pb2.FetchTasksRequest(task_ids=[
        backend_pb2.TaskID(id='1'),
        backend_pb2.TaskID(id='2'),
        backend_pb2.TaskID(id='3'),
    ])

    self.mock_auth_db([])
    raw_resp = self.app.post(
        '/prpc/swarming.backend.TaskBackend/FetchTasks',
        _encode(request),
        self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertIn(('X-Prpc-Grpc-Code', '3'), raw_resp._headerlist)
    self.assertEqual(raw_resp.body,
                     'Requesting 3 tasks when the allowed max is 2.')

  def test_validate_configs(self):
    request = backend_pb2.ValidateConfigsRequest(
        configs=[
            backend_pb2.ValidateConfigsRequest.ConfigContext(
                target='the-one-with-bad-values',
                config_json=struct_pb2.Struct(fields={
                    'priority':
                        struct_pb2.Value(
                            number_value=task_request.MAXIMUM_PRIORITY + 1),
                    'bot_ping_tolerance':
                        struct_pb2.Value(
                            number_value=\
                            task_request._MAX_BOT_PING_TOLERANCE_SECS),
                    'service_account':
                        struct_pb2.Value(string_value='bot'),
                    'agent_binary_cipd_filename':
                        struct_pb2.Value(string_value='agent'),
                    'agent_binary_cipd_pkg':
                    struct_pb2.Value(
                        string_value='agent/package/${platform}'),
                    'agent_binary_cipd_vers':
                        struct_pb2.Value(string_value='3'),
                })),
    ])

    raw_resp = self.app.post(
        '/prpc/swarming.backend.TaskBackend/ValidateConfigs',
        _encode(request), self._headers)
    resp = backend_pb2.ValidateConfigsResponse()
    _decode(raw_resp.body, resp)

    self.assertEqual(
        resp,
        backend_pb2.ValidateConfigsResponse(
        config_errors=[
            backend_pb2.ValidateConfigsResponse.ErrorDetail(
                index=0,
                error="priority (256) must be between 0 and 255 (inclusive)")]
    ))

class PRPCTest(test_env_handlers.AppTestBase):
  # These test fail with 'Unknown bot ID, not in config'
  # Need to run in sequential_test_runner.py
  no_run = 1

  """Tests the pRPC handlers."""
  def setUp(self):
    super(PRPCTest, self).setUp()
    # handlers_bot is necessary to run fake tasks.
    routes = handlers_prpc.get_routes() + handlers_bot.get_routes()
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(routes, debug=True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        },
    )
    self._headers = {
      'Content-Type': encoding.Encoding.JSON[1],
      'Accept': encoding.Encoding.JSON[1],
    }
    self.now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(self.now)
    self.mock_default_pool_acl([])
    self.mock_tq_tasks()

  def _test_bot_events_simple(self, request):
    self.set_as_bot()
    self.do_handshake()
    self.set_as_user()
    raw_resp = self.app.post(
        '/prpc/swarming.v1.BotAPI/Events', _encode(request), self._headers)
    expected = swarming_pb2.BotEventsResponse(
      events=[
        swarming_pb2.BotEvent(
          event_time=timestamp_pb2.Timestamp(seconds=1262401445),
          bot=swarming_pb2.Bot(
            bot_id='bot1',
            pools=['default'],
            info=swarming_pb2.BotInfo(
              supplemental=struct_pb2.Struct(
                fields={
                  'running_time': struct_pb2.Value(number_value=1234.0),
                  'sleep_streak': struct_pb2.Value(number_value=0),
                  'started_ts': struct_pb2.Value(number_value=1410990411.11),
                }),
              external_ip='192.168.2.2',
              authenticated_as='bot:whitelisted-ip',
              version='123',
              ),
            dimensions=[
              swarming_pb2.StringListPair(key='id', values=['bot1']),
              swarming_pb2.StringListPair(key='os', values=['Amiga']),
              swarming_pb2.StringListPair(key='pool', values=['default']),
            ]),
          event=swarming_pb2.BOT_NEW_SESSION,
        ),
      ])
    resp = swarming_pb2.BotEventsResponse()
    _decode(raw_resp.body, resp)
    self.assertEqual(unicode(expected), unicode(resp))

  def test_botevents_empty(self):
    # Minimum request, all optional fields left out.
    self._test_bot_events_simple(swarming_pb2.BotEventsRequest(bot_id=u'bot1'))

  def test_botevents_empty_time(self):
    msg = swarming_pb2.BotEventsRequest(bot_id=u'bot1')
    msg.start_time.FromDatetime(self.now)
    msg.end_time.FromDatetime(self.now + datetime.timedelta(seconds=1))
    self._test_bot_events_simple(msg)

  def test_botevents_missing(self):
    # No such bot.
    msg = swarming_pb2.BotEventsRequest(bot_id=u'unknown')
    raw_resp = self.app.post(
        '/prpc/swarming.v1.BotAPI/Events', _encode(msg), self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '404 Not Found')
    self.assertEqual(raw_resp.body, 'Bot does not exist')

  def test_botevents_invalid_page_size(self):
    msg = swarming_pb2.BotEventsRequest(bot_id=u'bot1', page_size=-1)
    raw_resp = self.app.post(
        '/prpc/swarming.v1.BotAPI/Events', _encode(msg), self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertEqual(raw_resp.body, 'page_size must be positive')

  def test_botevents_invalid_bot_id(self):
    # Missing bot_id
    msg = swarming_pb2.BotEventsRequest()
    raw_resp = self.app.post(
        '/prpc/swarming.v1.BotAPI/Events', _encode(msg), self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertEqual(raw_resp.body, 'specify bot_id')

  def test_botevents_start_end(self):
    msg = swarming_pb2.BotEventsRequest(bot_id=u'bot1')
    msg.start_time.FromDatetime(self.now)
    msg.end_time.FromDatetime(self.now)
    raw_resp = self.app.post(
        '/prpc/swarming.v1.BotAPI/Events', _encode(msg), self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertEqual(raw_resp.body, 'start_time must be before end_time')

  def test_botevents(self):
    # Run one task.
    self.mock(random, 'getrandbits', lambda _: 0x88)

    self.set_as_bot()
    self.mock_now(self.now, 0)
    params = self.do_handshake()
    self.mock_now(self.now, 30)
    self.bot_poll(params=params)
    self.set_as_user()
    now_60 = self.mock_now(self.now, 60)
    self.client_create_task_raw()
    self.set_as_bot()
    self.mock_now(self.now, 120)
    res = self.bot_poll(params=params)
    now_180 = self.mock_now(self.now, 180)
    response = self.bot_complete_task(task_id=res['manifest']['task_id'])
    self.assertEqual({u'must_stop': False, u'ok': True}, response)
    self.mock_now(self.now, 240)
    params['event'] = 'bot_rebooting'
    params['message'] = 'for the best'
    # TODO(maruel): https://crbug.com/913953
    response = self.post_json('/swarming/api/v1/bot/event', params)
    self.assertEqual({}, response)

    # Do not filter by time.
    self.set_as_privileged_user()
    msg = swarming_pb2.BotEventsRequest(bot_id=u'bot1', page_size=1001)
    raw_resp = self.app.post(
        '/prpc/swarming.v1.BotAPI/Events', _encode(msg), self._headers)
    resp = swarming_pb2.BotEventsResponse()
    _decode(raw_resp.body, resp)

    dimensions = [
      swarming_pb2.StringListPair(key='id', values=['bot1']),
      swarming_pb2.StringListPair(key='os', values=['Amiga']),
      swarming_pb2.StringListPair(key='pool', values=['default']),
    ]
    common_info = swarming_pb2.BotInfo(
        supplemental=struct_pb2.Struct(
            fields={
              'bot_group_cfg_version': struct_pb2.Value(string_value='default'),
              'running_time': struct_pb2.Value(number_value=1234.0),
              'sleep_streak': struct_pb2.Value(number_value=0),
              'started_ts': struct_pb2.Value(number_value=1410990411.11),
            }),
        external_ip='192.168.2.2',
        authenticated_as='bot:whitelisted-ip',
        version=self.bot_version,
    )
    events = [
        swarming_pb2.BotEvent(
            event_time=timestamp_pb2.Timestamp(seconds=1262401685),
            bot=swarming_pb2.Bot(bot_id='bot1',
                                 pools=[u'default'],
                                 info=common_info,
                                 dimensions=dimensions),
            event=swarming_pb2.BOT_REBOOTING_HOST,
            event_msg='for the best',
        ),
        swarming_pb2.BotEvent(
            event_time=timestamp_pb2.Timestamp(seconds=1262401625),
            bot=swarming_pb2.Bot(bot_id='bot1',
                                 pools=[u'default'],
                                 status=swarming_pb2.BUSY,
                                 current_task_id='5cfcee8008811',
                                 info=common_info,
                                 dimensions=dimensions),
            event=swarming_pb2.TASK_COMPLETED,
        ),
        swarming_pb2.BotEvent(
            event_time=timestamp_pb2.Timestamp(seconds=1262401565),
            bot=swarming_pb2.Bot(bot_id='bot1',
                                 pools=[u'default'],
                                 current_task_id='5cfcee8008811',
                                 status=swarming_pb2.BUSY,
                                 info=common_info,
                                 dimensions=dimensions),
            event=swarming_pb2.INSTRUCT_START_TASK,
        ),
        swarming_pb2.BotEvent(
            event_time=timestamp_pb2.Timestamp(seconds=1262401475),
            bot=swarming_pb2.Bot(bot_id='bot1',
                                 pools=[u'default'],
                                 status=swarming_pb2.IDLE,
                                 info=common_info,
                                 dimensions=dimensions),
            event=swarming_pb2.INSTRUCT_IDLE,
        ),
        swarming_pb2.BotEvent(
            event_time=timestamp_pb2.Timestamp(seconds=1262401445),
            bot=swarming_pb2.Bot(
                bot_id='bot1',
                pools=[u'default'],
                info=swarming_pb2.BotInfo(
                    supplemental=struct_pb2.Struct(
                        fields={
                            'running_time':
                            struct_pb2.Value(number_value=1234.0),
                            'sleep_streak':
                            struct_pb2.Value(number_value=0),
                            'started_ts':
                            struct_pb2.Value(number_value=1410990411.11),
                        }),
                    external_ip='192.168.2.2',
                    authenticated_as='bot:whitelisted-ip',
                    version='123',
                ),
                dimensions=dimensions),
            event=swarming_pb2.BOT_NEW_SESSION,
        ),
    ]
    self.assertEqual(len(events), len(resp.events))
    for i, event in enumerate(events):
      self.assertEqual(unicode(event), unicode(resp.events[i]))

    # Now test with a subset. It will retrieve events 1 and 2.
    msg = swarming_pb2.BotEventsRequest(bot_id=u'bot1')
    msg.start_time.FromDatetime(now_60)
    msg.end_time.FromDatetime(now_180 + datetime.timedelta(seconds=1))
    raw_resp = self.app.post(
        '/prpc/swarming.v1.BotAPI/Events', _encode(msg), self._headers)
    resp = swarming_pb2.BotEventsResponse()
    _decode(raw_resp.body, resp)
    self.assertEqual(
        unicode(swarming_pb2.BotEventsResponse(events=events[1:3])),
        unicode(resp))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
