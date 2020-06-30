#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest
import uuid

import mock

import test_env
test_env.setup_test_env()

from google.appengine.api import app_identity
from google.appengine.ext import ndb

from test_support import test_case

from components import net

from server import config
from server import resultdb

from proto.config import config_pb2


class ResultDBTest(test_case.TestCase):
  # This test needs to be run independently.
  # run by test.py
  no_run = 1

  def setUp(self):
    super(ResultDBTest, self).setUp()
    mock.patch('google.appengine.api.app_identity.get_default_version_hostname'
              ).start().return_value = 'test-swarming.appspot.com'
    mock.patch('uuid.uuid4').start().return_value = uuid.UUID(int=0)

  def tearDown(self):
    super(ResultDBTest, self).tearDown()
    mock.patch.stopall()

  @ndb.tasklet
  def nop_async(self, *_args, **_kwargs):
    pass

  @ndb.tasklet
  def _mock_call_resultdb_recorder_api_async(self, _method, _request,
                                             response_headers, **_kwargs):
    response_headers['update-token'] = 'token'

  def test_create_invocation_async(self):

    with mock.patch(
        'server.resultdb._call_resultdb_recorder_api_async',
        mock.MagicMock(side_effect=self._mock_call_resultdb_recorder_api_async)
    ) as mock_call:

      update_token = resultdb.create_invocation_async('task001').get_result()
      self.assertEqual(update_token, 'token')
      mock_call.assert_called_once_with(
          'CreateInvocation', {
              'invocation': {
                  'producerResource':
                      '//test-swarming.appspot.com/tasks/task001',
                  'realm':
                      None,
              },
              'requestId': '00000000-0000-0000-0000-000000000000',
              'invocationId': 'task-test-swarming.appspot.com-task001'
          },
          project_id=None,
          response_headers=mock.ANY)

  def test_create_invocation_async_with_realm(self):

    with mock.patch(
        'server.resultdb._call_resultdb_recorder_api_async',
        mock.MagicMock(side_effect=self._mock_call_resultdb_recorder_api_async)
    ) as mock_call:

      update_token = resultdb.create_invocation_async('task001',
                                                      'infra:try').get_result()
      self.assertEqual(update_token, 'token')
      mock_call.assert_called_once_with(
          'CreateInvocation', {
              'invocation': {
                  'producerResource':
                      '//test-swarming.appspot.com/tasks/task001',
                  'realm':
                      'infra:try',
              },
              'requestId': '00000000-0000-0000-0000-000000000000',
              'invocationId': 'task-test-swarming.appspot.com-task001'
          },
          project_id='infra',
          response_headers=mock.ANY)

  def test_create_invocation_async_no_update_token(self):
    with mock.patch('server.resultdb._call_resultdb_recorder_api_async',
                    mock.Mock(side_effect=self.nop_async)):
      with self.assertRaisesRegexp(
          AssertionError,
          "^response_headers should have valid update-token: {}$"):
        resultdb.create_invocation_async(
            'task001',
            'infra:try',
        ).get_result()

  def test_finalize_invocation_async_success(self):

    with mock.patch(
        'server.resultdb._call_resultdb_recorder_api_async') as mock_call:
      mock_call.side_effect = self.nop_async
      resultdb.finalize_invocation_async('task001', 'secret').get_result()
      mock_call.assert_called_once_with(
          'FinalizeInvocation', {
              'name': 'invocations/task-test-swarming.appspot.com-task001',
          },
          headers={'update-token': 'secret'},
          scopes=None)

  def test_finalize_invocation_async_failed_precondition(self):
    with mock.patch(
        'server.resultdb._call_resultdb_recorder_api_async') as mock_call:
      mock_call.side_effect = net.Error(
          msg='error',
          status_code=400,
          response='error',
          headers={'X-Prpc-Grpc-Code': '9'})
      resultdb.finalize_invocation_async('task001', 'secret').get_result()
      mock_call.assert_called_once_with(
          'FinalizeInvocation', {
              'name': 'invocations/task-test-swarming.appspot.com-task001',
          },
          headers={'update-token': 'secret'},
          scopes=None)

  def test_finalize_invocation_async_failed(self):
    with mock.patch(
        'server.resultdb._call_resultdb_recorder_api_async') as mock_call:
      mock_call.side_effect = Exception('failed')
      with self.assertRaises(Exception):
        resultdb.finalize_invocation_async('task001', 'secret').get_result()
      mock_call.assert_called_once_with(
          'FinalizeInvocation', {
              'name': 'invocations/task-test-swarming.appspot.com-task001',
          },
          headers={'update-token': 'secret'},
          scopes=None)

  def test_call_resultdb_recorder_api(self):

    with mock.patch('server.config.settings') as mock_settings, mock.patch(
        'components.net.json_request_async') as mock_json_request_async:
      mock_settings.return_value = config_pb2.SettingsCfg(
          resultdb=config_pb2.ResultDBSettings(
              server='https://results.api.cr.dev'),)
      mock_json_request_async.side_effect = self.nop_async

      resultdb._call_resultdb_recorder_api_async('FinalizeInvocation',
                                                 {}).get_result()

      mock_settings.assert_called_once()
      mock_json_request_async.assert_called_once()

  def test_get_invocation_name(self):
    self.assertEqual(
        resultdb.get_invocation_name('run_result_id_01'),
        'invocations/task-test-swarming.appspot.com-run_result_id_01')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
