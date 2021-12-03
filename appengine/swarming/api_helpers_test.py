#!/usr/bin/env vpython
# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
import datetime
import logging
import sys
import unittest

import mock

import swarming_test_env
swarming_test_env.setup_test_env()

import api_helpers
from components import auth
from components import auth_testing
from components import utils
import handlers_exceptions
from proto.config import config_pb2
from server import acl
from server import config
from server import pools_config
from server import realms
from server import service_accounts
from server import task_request
from test_support import test_case

from proto.api.internal.bb import swarming_bb_pb2

class TestProcessTaskRequest(test_case.TestCase):

  def setUp(self):
    super(TestProcessTaskRequest, self).setUp()
    now = datetime.datetime(2019, 01, 02, 03)
    test_case.mock_now(self, now, 0)

    self._known_pools = None

  def basic_task_request(self):
    return task_request.TaskRequest(
        name='ChickenTask',
        realm='farm:chicken',
        created_ts=utils.utcnow(),
        task_slices=[
            task_request.TaskSlice(
                expiration_secs=60,
                properties=task_request.TaskProperties(
                    command=['print', 'chicken'],
                    execution_timeout_secs=120,
                    dimensions_data={
                        u'chicken': [u'egg1', u'egg2'],
                        u'pool': [u'default']
                    }))
        ])

  def test_process_task_request_BadRequest(self):
    tr = task_request.TaskRequest(
        created_ts=utils.utcnow(),
        task_slices=[
            task_request.TaskSlice(
                properties=task_request.TaskProperties(
                    dimensions_data={u'chicken': [u'egg1', u'egg2']}))
        ])

    # Catch init_new_request() ValueError exceptions.
    with self.assertRaisesRegexp(handlers_exceptions.BadRequestException,
                                 'missing expiration_secs'):
      api_helpers.process_task_request(tr, task_request.TEMPLATE_AUTO)
    tr.task_slices[0].expiration_secs = 60

    # Catch datastore.BadValueErrors.
    with self.assertRaisesRegexp(handlers_exceptions.BadRequestException,
                                 'name is missing'):
      api_helpers.process_task_request(tr, task_request.TEMPLATE_AUTO)
    tr = self.basic_task_request()

    # Catch no such pool.
    with self.assertRaisesRegexp(handlers_exceptions.PermissionException,
                                 'No such pool'):
      api_helpers.process_task_request(tr, task_request.TEMPLATE_AUTO)

    # Catch inconsistent enabling of realms and resultDB.
    self.mock(realms, 'check_tasks_create_in_realm', lambda *_: False)
    self.mock_pool_config('default')
    tr.resultdb = task_request.ResultDBCfg(enable=True)
    with self.assertRaisesRegexp(handlers_exceptions.BadRequestException,
                                 'ResultDB is enabled, but realm is not'):
      api_helpers.process_task_request(tr, task_request.TEMPLATE_AUTO)

  def test_process_task_request(self):
    self.mock_pool_config('default')
    tr = self.basic_task_request()

    expected_tr = self.basic_task_request()
    task_request.init_new_request(expected_tr,
                                  acl.can_schedule_high_priority_tasks(),
                                  task_request.TEMPLATE_AUTO)
    expected_tr.realms_enabled = True

    self.mock(realms, 'check_tasks_create_in_realm', lambda *_: True)
    self.mock(realms, 'check_pools_create_task', lambda *_: True)

    api_helpers.process_task_request(tr, task_request.TEMPLATE_AUTO)
    self.assertEqual(expected_tr, tr)

  def test_process_task_request_service_account(self):
    self.mock_pool_config('default')

    tr = self.basic_task_request()
    tr.service_account = 'service-account@example.com'

    expected_tr = self.basic_task_request()
    expected_tr.service_account = 'service-account@example.com'
    task_request.init_new_request(expected_tr,
                                  acl.can_schedule_high_priority_tasks(),
                                  task_request.TEMPLATE_AUTO)
    expected_tr.realms_enabled = True

    self.mock(realms, 'check_tasks_create_in_realm', lambda *_: True)
    self.mock(realms, 'check_pools_create_task', lambda *_: True)
    self.mock(realms, 'check_tasks_act_as', lambda *_: True)
    self.mock(service_accounts, 'has_token_server', lambda: True)

    api_helpers.process_task_request(tr, task_request.TEMPLATE_AUTO)

    self.assertEqual(expected_tr, tr)

  def test_process_task_request_service_account_legacy(self):
    self.mock_pool_config('default')

    tr = self.basic_task_request()
    tr.service_account = 'service-account@example.com'

    task_request.init_new_request(tr,
                                  acl.can_schedule_high_priority_tasks(),
                                  task_request.TEMPLATE_AUTO)

    self.mock(realms, 'check_tasks_create_in_realm', lambda *_: False)
    self.mock(realms, 'check_pools_create_task', lambda *_: True)
    self.mock(service_accounts, 'has_token_server', lambda: True)

    with self.assertRaises(handlers_exceptions.BadRequestException) as exc:
      api_helpers.process_task_request(tr, task_request.TEMPLATE_AUTO)
    self.assertIn(
        'only if the task is associated with a realm', exc.exception.message)

  def mock_pool_config(self, name):

    def mocked_get_pool_config(pool):
      if pool == name:
        return pools_config.init_pool_config(
            name=name,
            rev='rev',
        )
      return None

    self.mock(pools_config, 'get_pool_config', mocked_get_pool_config)


class TestCheckIdenticalRequest(test_case.TestCase):

  def setUp(self):
    super(TestCheckIdenticalRequest, self).setUp()
    self.now = test_case.mock_now(self, datetime.datetime(2019, 01, 02, 03), 0)

  def test_cache_hit(self):
    func = mock.Mock(return_value='ok')
    request_uuid = 'cf60878f-8f2a-4f1e-b1f5-8b5ec88813a9'

    self.assertEqual(('ok', False),
                     api_helpers.cache_request('test_request', request_uuid,
                                               func))
    func.assert_called_once()

    func.reset_mock()

    self.assertEqual(('ok', True),
                     api_helpers.cache_request('test_request', request_uuid,
                                               func))
    func.assert_not_called()

  def test_ttl(self):
    func = mock.Mock(return_value='ok')
    request_uuid = 'cf60878f-8f2a-4f1e-b1f5-8b5ec88813a9'

    self.assertEqual(('ok', False),
                     api_helpers.cache_request('test_request', request_uuid,
                                               func))
    func.assert_called_once()

    # the cache just got expired.
    self.mock_now(self.now, 10 * 60)
    func.reset_mock()

    self.assertEqual(('ok', False),
                     api_helpers.cache_request('test_request', request_uuid,
                                               func))
    func.assert_called_once()

  def test_namespace(self):
    request_uuid = 'cf60878f-8f2a-4f1e-b1f5-8b5ec88813a9'
    func1 = mock.Mock(return_value='ok')
    func2 = mock.Mock(return_value='great')

    self.assertEqual(('ok', False),
                     api_helpers.cache_request('test_request_1', request_uuid,
                                               func1))
    func1.assert_called_once()

    # the cache won't hit because this is in a different namespace.
    self.assertEqual(('great', False),
                     api_helpers.cache_request('test_request_2', request_uuid,
                                               func2))
    func2.assert_called_once()

  def test_invalid_request_uuid(self):
    func = mock.Mock(return_value='ok')

    with self.assertRaises(handlers_exceptions.BadRequestException):
      api_helpers.cache_request('test_request', 'foo', func)

  def test_tuple(self):
    func = mock.Mock(return_value=('ok', 'great'))
    request_uuid = 'cf60878f-8f2a-4f1e-b1f5-8b5ec88813a9'

    result = api_helpers.cache_request('test_request', request_uuid, func)
    self.assertEqual((('ok', 'great'), False), result)


  def test_validate_configs(self):
    configs = [
        swarming_bb_pb2.SwarmingBackendConfig(
            priority=task_request.MAXIMUM_PRIORITY + 1,
            bot_ping_tolerance=task_request._MAX_BOT_PING_TOLERANCE_SECS + 1,
            service_account='bokbok',
            parent_run_id='123',
            agent_binary_cipd_filename='agent',
            agent_binary_cipd_pkg='agent/package/${platform}??',
            agent_binary_cipd_vers='3'),
        swarming_bb_pb2.SwarmingBackendConfig(priority=0)
        ]
    errors = api_helpers.validate_backend_configs(configs)

    expected_errors = [
        (0, "priority (256) must be between 0 and 255 (inclusive)"),
        (0, "bot_ping_tolerance (1201) must range between 60 and 1200"),
        (0, "parent_run_id (123) got error: Invalid key u'12'"),
        (0, ("service_account must be an email, \"bot\" or \"none\""
         " string, got u\'bokbok\'")),
        (0, ("agent_binary_cipd_pkg must be a valid CIPD package name"
             " template, got \"agent/package/${platform}??\"")),
        (1, "bot_ping_tolerance (0) must range between 60 and 1200"),
        (1, ("agent_binary_cipd_pkg must be a valid CIPD package name"
             " template, got \"\"")),
        (1, "agent_binary_cipd_vers must be a valid package version, got \"\""),
        (1, "missing `agent_binary_cipd_filename`"),
    ]
    self.assertEqual(expected_errors, errors)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
