#!/usr/bin/env vpython
# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import os
import cgi
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
from google.rpc import status_pb2

from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2
from google.protobuf import duration_pb2

from test_support import test_case

from components import auth
from components import utils
from components import prpc
from components.prpc import codes
from components.prpc import encoding
from server import task_pack
from server import task_result

from bb.go.chromium.org.luci.buildbucket.proto import backend_pb2
from bb.go.chromium.org.luci.buildbucket.proto import common_pb2
from bb.go.chromium.org.luci.buildbucket.proto import launcher_pb2
from bb.go.chromium.org.luci.buildbucket.proto import task_pb2

from server import task_request
from server import task_scheduler
from server import realms
from server import service_accounts
import handlers_bot
from handlers_task_backend import TaskBackendAPIService


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
    s.add_service(TaskBackendAPIService())
    # TODO(crbug/1236848) call handlers_prpc.get_routes() when
    # the Backend is ready and added.
    routes = s.get_routes()
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
    self.mock(app_identity, 'get_application_id', lambda: 'test-swarming')

  # Test helpers.
  def _req_dim_prpc(self, key, value, exp_secs=None):
    # type: (str, str, Optional[int]) -> common_pb2.RequestedDimension
    dim = common_pb2.RequestedDimension(key=key, value=value)
    if exp_secs is not None:
      dim.expiration.seconds = exp_secs
    return dim

  def _basic_run_task_request(self):
    return backend_pb2.RunTaskRequest(
        target="swarming://test-swarming",
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
                'agent_binary_cipd_server':
                struct_pb2.Value(string_value='cipdserver'),
            }),
        grace_period=duration_pb2.Duration(seconds=60),
        execution_timeout=duration_pb2.Duration(seconds=60),
        start_deadline=timestamp_pb2.Timestamp(seconds=int(utils.time_time() +
                                                           120)),
        dimensions=[self._req_dim_prpc('pool', 'default')],
        buildbucket_host='cow-buildbucket.appspot.com',
        pubsub_topic="my_topic")

  def _client_run_task(self, request):
    self.mock(realms, 'check_tasks_create_in_realm', lambda *_: True)
    self.mock(realms, 'check_pools_create_task', lambda *_: True)
    self.mock(realms, 'check_tasks_act_as', lambda *_: True)
    self.mock(service_accounts, 'has_token_server', lambda: True)

    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/RunTask',
                             _encode(request), self._headers)
    actual_resp = backend_pb2.RunTaskResponse()
    _decode(raw_resp.body, actual_resp)
    return actual_resp
  # Tests
  def test_run_task(self):
    self.set_as_project()

    # Mocks for process_task_requests()
    # adds pool configs for which user is a `scheduling_user`
    self.mock_default_pool_acl([])
    self.mock(realms, 'check_tasks_create_in_realm', lambda *_: True)
    self.mock(realms, 'check_pools_create_task', lambda *_: True)
    self.mock(realms, 'check_tasks_act_as', lambda *_: True)
    self.mock(service_accounts, 'has_token_server', lambda: True)

    request = self._basic_run_task_request()
    request.build_id = "8783198670850745761"
    request.request_id = "123"
    self.mock(random, 'getrandbits', lambda _: 0x86)
    self.mock(utils, 'time_time_ns', lambda: 1546398020)
    actual_resp = self._client_run_task(request)
    expected_task_id = '4225526b80008610'
    l = 'https://test-swarming.appspot.com/task?id=%s&o=true&w=true'
    expected_response = backend_pb2.RunTaskResponse(
        task=task_pb2.Task(id=task_pb2.TaskID(
            id=expected_task_id, target='swarming://test-swarming'),
                           link=l % expected_task_id,
                           status=common_pb2.SCHEDULED,
                           update_id=1546398020))
    self.assertEqual(actual_resp, expected_response)
    self.assertEqual(1, task_request.TaskRequest.query().count())
    self.assertEqual(1, task_request.BuildTask.query().count())
    self.assertEqual(1, task_request.SecretBytes.query().count())

    # Test requests are correctly deduped if `request_id` matches.
    actual_resp = self._client_run_task(request)
    expected_response = backend_pb2.RunTaskResponse(
        task=task_pb2.Task(id=task_pb2.TaskID(
            id=expected_task_id, target='swarming://test-swarming'),
                           link=l % expected_task_id,
                           status=common_pb2.SCHEDULED,
                           update_id=1546398020))
    self.assertEqual(actual_resp, expected_response)
    self.assertEqual(1, task_request.TaskRequest.query().count())
    self.assertEqual(1, task_request.BuildTask.query().count())
    self.assertEqual(1, task_request.SecretBytes.query().count())

    # Test tasks with different `request_ids`s are not deduped.
    request.build_id = '8783198670850745761'
    request.request_id = "456"
    self.mock(random, 'getrandbits', lambda _: 0x87)
    actual_resp = self._client_run_task(request)
    new_expected_task_id = '4225526b80008710'
    expected_response = backend_pb2.RunTaskResponse(
        task=task_pb2.Task(id=task_pb2.TaskID(
            id=new_expected_task_id, target='swarming://test-swarming'),
                           link=l % new_expected_task_id,
                           status=common_pb2.SCHEDULED,
                           update_id=1546398020))
    self.assertEqual(actual_resp, expected_response)
    self.assertEqual(2, task_request.TaskRequest.query().count())
    self.assertEqual(2, task_request.BuildTask.query().count())
    self.assertEqual(2, task_request.SecretBytes.query().count())

  def test_run_task_bad_request(self):
    self.set_as_project()

    # No build_id
    request = backend_pb2.RunTaskRequest()
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/RunTask',
                             _encode(request),
                             self._headers,
                             expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertIn(('X-Prpc-Grpc-Code', '3'), raw_resp._headerlist)
    self.assertIn('build_id must be provided', raw_resp.body)

    # No target
    request.build_id = "1"
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/RunTask',
                             _encode(request),
                             self._headers,
                             expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertIn(('X-Prpc-Grpc-Code', '3'), raw_resp._headerlist)
    self.assertIn('target must be provided', raw_resp.body)

  def test_run_task_exceptions_bad_conversion(self):
    self.set_as_project()
    request = backend_pb2.RunTaskRequest(build_id="12345",
                                         target="swarming://test-swarming",
                                         pubsub_topic="my_topic")
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/RunTask',
                             _encode(request),
                             self._headers,
                             expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertIn(('X-Prpc-Grpc-Code', '3'), raw_resp._headerlist)
    self.assertIn('must be a valid package', raw_resp.body)

  def test_run_task_exceptions_schedule_request_error(self):
    self.set_as_project()

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
                                build_task=None):
      raise TypeError('chicken')

    self.mock(task_scheduler, 'schedule_request', mocked_schedule_request)
    request = self._basic_run_task_request()
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/RunTask',
                             _encode(request),
                             self._headers,
                             expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertIn(('X-Prpc-Grpc-Code', '3'), raw_resp._headerlist)
    self.assertEqual(raw_resp.body, 'chicken')

  def test_run_task_exceptions_auth_error(self):
    request = self._basic_run_task_request()
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/RunTask',
                             _encode(request),
                             self._headers,
                             expect_errors=True)
    self.assertEqual(raw_resp.status, '403 Forbidden')
    self.assertIn(('X-Prpc-Grpc-Code', '7'), raw_resp._headerlist)
    self.assertEqual(raw_resp.body, 'Access is denied.')

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

    self.set_as_project()
    request = backend_pb2.CancelTasksRequest(task_ids=[
        task_pb2.TaskID(id=str(first_id)),
        task_pb2.TaskID(id=str(second_id)),
        task_pb2.TaskID(id='1d69b9f088008810'),  # Does not exist.
    ])

    target = 'swarming://%s' % app_identity.get_application_id()
    expected_response = backend_pb2.CancelTasksResponse(tasks=[
        task_pb2.Task(id=task_pb2.TaskID(target=target, id=first_id),
                      status=common_pb2.SUCCESS),
        # Task to cancel this should be enqueued
        task_pb2.Task(id=task_pb2.TaskID(target=target, id=second_id),
                      status=common_pb2.SCHEDULED),
        task_pb2.Task(id=task_pb2.TaskID(target=target, id='1d69b9f088008810'),
                      summary_html='Swarming task 1d69b9f088008810 not found',
                      status=common_pb2.INFRA_FAILURE),
    ])

    self.mock_auth_db([auth.Permission('swarming.pools.cancelTask')])
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/CancelTasks',
                             _encode(request),
                             self._headers,
                             expect_errors=False)

    resp = backend_pb2.CancelTasksResponse()
    _decode(raw_resp.body, resp)
    self.assertEqual(resp, expected_response)

    utils.enqueue_task.assert_has_calls([
        mock.call('/internal/taskqueue/important/tasks/cancel-children-tasks',
                  'cancel-children-tasks',
                  payload=mock.ANY),
        mock.call('/internal/taskqueue/important/tasks/cancel',
                  'cancel-tasks',
                  payload='{"kill_running": true, "tasks": ["%s"]}' %
                  second_id),
    ])

  def test_cancel_tasks_permission_denied(self):
    self.mock_default_pool_acl([])

    # Create task
    self.set_as_user()
    _, first_id = self.client_create_task_raw(
        name='first',
        tags=['project:yay', 'commit:post'],
        properties=dict(idempotent=True))

    self.set_as_project()
    request = backend_pb2.CancelTasksRequest(task_ids=[
        task_pb2.TaskID(id=str(first_id)),
    ])

    self.mock_auth_db([])
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/CancelTasks',
                             _encode(request),
                             self._headers,
                             expect_errors=True)
    self.assertEqual(raw_resp.status, '403 Forbidden')
    self.assertIn(('X-Prpc-Grpc-Code', '7'), raw_resp._headerlist)
    self.assertEqual(
        raw_resp.body,
        cgi.escape(
            'project "luci-project" does not have '
            'permission "swarming.pools.cancelTask"',
            quote=True))

  @mock.patch('handlers_task_backend._CANCEL_TASKS_LIMIT', 2)
  def test_cancel_tasks_too_many(self):
    self.set_as_project()

    request = backend_pb2.CancelTasksRequest(task_ids=[
        task_pb2.TaskID(id=str("1")),
        task_pb2.TaskID(id=str("2")),
        task_pb2.TaskID(id=str("3")),
    ])
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/CancelTasks',
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

    # Create two tasks.
    self.set_as_project()
    request = self._basic_run_task_request()
    request.build_id = "8783198670850745761"
    request.request_id = "12345"
    self.mock(random, 'getrandbits', lambda _: 0x86)
    self.mock(utils, 'time_time_ns', lambda: 1546398020)
    actual_resp = self._client_run_task(request)
    first_id = actual_resp.task.id.id

    # Clear cache to test fetching from datastore path.
    ndb.get_context().clear_cache()
    memcache.flush_all()

    # second request
    request.build_id = '23895823794242'
    request.request_id = "67890"
    self.mock(random, 'getrandbits', lambda _: 0x87)
    self.mock(utils, 'time_time_ns', lambda: 1646398020)
    actual_resp = self._client_run_task(request)
    second_id = actual_resp.task.id.id

    request = backend_pb2.FetchTasksRequest(task_ids=[
        task_pb2.TaskID(id=first_id),
        task_pb2.TaskID(id=second_id),
        task_pb2.TaskID(id='1d69b9f088008810'),  # Does not exist.
    ])

    target = 'swarming://%s' % app_identity.get_application_id()
    expected_response = backend_pb2.FetchTasksResponse(responses=[
        backend_pb2.FetchTasksResponse.Response(task=task_pb2.Task(
            id=task_pb2.TaskID(target=target, id=first_id),
            status=common_pb2.SCHEDULED,
            update_id=1546398020,
            details=struct_pb2.Struct(),
        ), ),
        backend_pb2.FetchTasksResponse.Response(task=task_pb2.Task(
            id=task_pb2.TaskID(target=target, id=second_id),
            status=common_pb2.SCHEDULED,
            update_id=1646398020,
            details=struct_pb2.Struct(),
        ), ),
        backend_pb2.FetchTasksResponse.Response(error=status_pb2.Status(
            code=codes.StatusCode.NOT_FOUND.value,
            message='Swarming task 1d69b9f088008810 not found',
        ), ),
    ])

    self.mock_auth_db([auth.Permission('swarming.pools.listTasks')])
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/FetchTasks',
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

    self.set_as_project()
    request = backend_pb2.FetchTasksRequest(task_ids=[
        task_pb2.TaskID(id=str(first_id)),
    ])

    self.mock_auth_db([])
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/FetchTasks',
                             _encode(request),
                             self._headers,
                             expect_errors=True)
    self.assertEqual(raw_resp.status, '403 Forbidden')
    self.assertIn(('X-Prpc-Grpc-Code', '7'), raw_resp._headerlist)
    self.assertEqual(
        raw_resp.body,
        cgi.escape(
            'project "luci-project" does not have '
            'permission "swarming.pools.listTasks"',
            quote=True))

  @mock.patch('handlers_task_backend._FETCH_TASKS_LIMIT', 2)
  def test_fetch_tasks_too_many(self):
    self.set_as_project()

    request = backend_pb2.FetchTasksRequest(task_ids=[
        task_pb2.TaskID(id='1'),
        task_pb2.TaskID(id='2'),
        task_pb2.TaskID(id='3'),
    ])

    self.mock_auth_db([])
    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/FetchTasks',
                             _encode(request),
                             self._headers,
                             expect_errors=True)
    self.assertEqual(raw_resp.status, '400 Bad Request')
    self.assertIn(('X-Prpc-Grpc-Code', '3'), raw_resp._headerlist)
    self.assertEqual(raw_resp.body,
                     'Requesting 3 tasks when the allowed max is 2.')

  def test_validate_configs(self):
    self.set_as_project()
    request = backend_pb2.ValidateConfigsRequest(
        configs=[
            backend_pb2.ValidateConfigsRequest.ConfigContext(
                target='swarming://test-swarming',
                config_json=struct_pb2.Struct(fields={
                    'bot_ping_tolerance':
                        struct_pb2.Value(
                            number_value=\
                            task_request._MAX_BOT_PING_TOLERANCE_SECS),
                }))])

    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/ValidateConfigs',
                             _encode(request), self._headers)
    resp = backend_pb2.ValidateConfigsResponse()
    _decode(raw_resp.body, resp)

    self.assertEqual(resp,
                     backend_pb2.ValidateConfigsResponse(config_errors=[]))

  def test_validate_configs_error_in_config(self):
    self.set_as_project()
    request = backend_pb2.ValidateConfigsRequest(configs=[
        backend_pb2.ValidateConfigsRequest.ConfigContext(
            target='swarming://test-swarming',
            config_json=struct_pb2.Struct(
                fields={
                    'bot_ping_tolerance': struct_pb2.Value(number_value=8000),
                }))
    ])

    raw_resp = self.app.post('/prpc/buildbucket.v2.TaskBackend/ValidateConfigs',
                             _encode(request), self._headers)
    resp = backend_pb2.ValidateConfigsResponse()
    _decode(raw_resp.body, resp)

    self.assertEqual(
        resp,
        backend_pb2.ValidateConfigsResponse(config_errors=[
            backend_pb2.ValidateConfigsResponse.ErrorDetail(
                index=0,
                error="bot_ping_tolerance (8000) must range between 60 and 1200"
            )
        ]))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
