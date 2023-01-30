#!/usr/bin/env vpython
# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import unittest
import sys
import posixpath
import copy

import swarming_test_env
swarming_test_env.setup_test_env()

from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.protobuf import struct_pb2
from google.protobuf import duration_pb2
from google.protobuf import timestamp_pb2

from test_support import test_case

import backend_conversions
import handlers_exceptions
from components import utils
from server import task_request
from server import task_result
from server import task_pack

from bb.go.chromium.org.luci.buildbucket.proto import backend_pb2
from bb.go.chromium.org.luci.buildbucket.proto import common_pb2
from bb.go.chromium.org.luci.buildbucket.proto import launcher_pb2
from bb.go.chromium.org.luci.buildbucket.proto import project_config_pb2
from bb.go.chromium.org.luci.buildbucket.proto import task_pb2
from proto.api_v2 import swarming_pb2


def req_dim_prpc(key, value, exp_secs=None):
  # type: (str, str, Optional[int]) -> common_pb2.RequestedDimension
  dim = common_pb2.RequestedDimension(key=key, value=value)
  if exp_secs is not None:
    dim.expiration.seconds = exp_secs
  return dim


class TestBackendConversions(test_case.TestCase):

  def setUp(self):
    super(TestBackendConversions, self).setUp()
    now = datetime.datetime(2019, 1, 2, 3)
    test_case.mock_now(self, now, 0)

  def test_compute_task_request(self):
    exec_secs = 180
    grace_secs = 60
    start_deadline_secs = int(utils.time_time() + 120)
    parent_task_id = '1d69ba3ea8008810'
    # Mock parent_task_id validator to not raise ValueError.
    self.mock(task_pack, 'unpack_run_result_key', lambda _: None)

    run_task_req = backend_pb2.RunTaskRequest(
        secrets=launcher_pb2.BuildSecrets(build_token='tok'),
        realm='some:realm',
        build_id='4242',
        agent_args=['-fantasia', 'pegasus'],
        backend_config=struct_pb2.Struct(
            fields={
                'wait_for_capacity':
                    struct_pb2.Value(bool_value=True),
                'priority':
                    struct_pb2.Value(number_value=5),
                'bot_ping_tolerance':
                    struct_pb2.Value(number_value=70),
                'service_account':
                    struct_pb2.Value(string_value='who@serviceaccount.com'),
                'parent_run_id':
                    struct_pb2.Value(string_value=parent_task_id),
                'agent_binary_cipd_filename':
                    struct_pb2.Value(string_value='agent'),
                'agent_binary_cipd_pkg':
                    struct_pb2.Value(string_value='agent/package/${platform}'),
                'agent_binary_cipd_vers':
                    struct_pb2.Value(string_value='latest'),
            }),
        grace_period=duration_pb2.Duration(seconds=grace_secs),
        execution_timeout=duration_pb2.Duration(seconds=exec_secs),
        start_deadline=timestamp_pb2.Timestamp(seconds=start_deadline_secs),
        dimensions=[req_dim_prpc('required-1', 'req-1')],
        backend_token='token-token-token',
        buildbucket_host='cow-buildbucket.appspot.com',
    )

    expected_slice = task_request.TaskSlice(
        wait_for_capacity=True,
        expiration_secs=120,
        properties=task_request.TaskProperties(
            has_secret_bytes=True,
            execution_timeout_secs=exec_secs,
            grace_period_secs=grace_secs,
            dimensions_data={u'required-1': [u'req-1']},
            command=[
                u'agent', '-fantasia', 'pegasus', '-cache-base', 'cache',
                '-task-id', '${SWARMING_TASK_ID}'
            ],
            cipd_input=task_request.CipdInput(packages=[
                task_request.CipdPackage(
                    path='.',
                    package_name=u'agent/package/${platform}',
                    version=u'latest')
            ])))
    expected_tr = task_request.TaskRequest(
        created_ts=utils.utcnow(),
        task_slices=[expected_slice],
        expiration_ts=utils.timestamp_to_datetime(start_deadline_secs *
                                                  1000000),
        realm='some:realm',
        name='bb-4242',
        priority=5,
        bot_ping_tolerance_secs=70,
        service_account='who@serviceaccount.com',
        parent_task_id=parent_task_id,
        has_build_token=True)

    expected_sb = task_request.SecretBytes(
        secret_bytes=run_task_req.secrets.SerializeToString())
    expected_bt = task_request.BuildToken(
        build_id='4242',
        token='token-token-token',
        buildbucket_host='cow-buildbucket.appspot.com')

    actual_tr, actual_sb, actual_bt = backend_conversions.compute_task_request(
        run_task_req)
    self.assertEqual(expected_tr, actual_tr)
    self.assertEqual(expected_sb, actual_sb)
    self.assertEqual(expected_bt, actual_bt)

  def test_compute_task_slices(self):
    exec_secs = 180
    grace_secs = 60
    start_deadline_secs = int(utils.time_time() + 60)

    run_task_req = backend_pb2.RunTaskRequest(
        agent_args=['-chicken', '1', '-cow', '3'],
        backend_config=struct_pb2.Struct(
            fields={
                'wait_for_capacity':
                    struct_pb2.Value(bool_value=True),
                'agent_binary_cipd_filename':
                    struct_pb2.Value(string_value='agent'),
                'agent_binary_cipd_pkg':
                    struct_pb2.Value(string_value='agent/package/${platform}'),
                'agent_binary_cipd_vers':
                    struct_pb2.Value(string_value='latest'),
            }),
        caches=[
            project_config_pb2.BuilderConfig.CacheEntry(
                name='name_1',
                path='path_1'
            ),
            project_config_pb2.BuilderConfig.CacheEntry(
                name='name_2',
                path='path_2',
                wait_for_warm_cache_secs=180)
        ],
        grace_period=duration_pb2.Duration(seconds=grace_secs),
        execution_timeout=duration_pb2.Duration(seconds=exec_secs),
        start_deadline=timestamp_pb2.Timestamp(seconds=start_deadline_secs),
        dimensions=[
            req_dim_prpc('required-1', 'req-1'),
            req_dim_prpc('optional-1', 'opt-1', exp_secs=180),
            req_dim_prpc('optional-2', 'opt-2', exp_secs=180),
            req_dim_prpc('optional-3', 'opt-3', exp_secs=60),
            req_dim_prpc('required-2', 'req-2'),
            req_dim_prpc('required-1', 'req-1-2')
        ])
    backend_config = backend_conversions.ingest_backend_config(
        run_task_req.backend_config)

    # Create expected slices.
    base_slice = task_request.TaskSlice(
        wait_for_capacity=True,
        properties=task_request.TaskProperties(
            command=[
                u'agent', '-chicken', '1', '-cow', '3', '-cache-base', 'cache',
                '-task-id', '${SWARMING_TASK_ID}'
            ],
            has_secret_bytes=True,
            caches=[
                task_request.CacheEntry(
                    path=posixpath.join(backend_conversions._CACHE_DIR,
                                        'path_1'),
                    name='name_1'),
                task_request.CacheEntry(
                    path=posixpath.join(backend_conversions._CACHE_DIR,
                                        'path_2'),
                    name='name_2'),
            ],
            execution_timeout_secs=exec_secs,
            grace_period_secs=grace_secs,
            cipd_input=task_request.CipdInput(packages=[
                task_request.CipdPackage(
                    path='.',
                    package_name=u'agent/package/${platform}',
                    version=u'latest')
            ])))

    slice_1 = task_request.TaskSlice(
        expiration_secs=60, properties=copy.deepcopy(base_slice.properties))
    slice_1.properties.dimensions_data = {
        u'caches': [u'name_2'],
        u'required-1': [u'req-1', u'req-1-2'],
        u'required-2': [u'req-2'],
        u'optional-1': [u'opt-1'],
        u'optional-2': [u'opt-2'],
        u'optional-3': [u'opt-3']
    }

    slice_2 = task_request.TaskSlice(
        expiration_secs=120, properties=copy.deepcopy(base_slice.properties))
    slice_2.properties.dimensions_data = {
        u'caches': [u'name_2'],
        u'required-1': [u'req-1', u'req-1-2'],
        u'required-2': [u'req-2'],
        u'optional-1': [u'opt-1'],
        u'optional-2': [u'opt-2']
    }

    base_slice.expiration_secs = 60  # minimum allowed is 60s
    base_slice.properties.dimensions_data = {
        u'required-1': [u'req-1', u'req-1-2'],
        u'required-2': [u'req-2']
    }

    expected_slices = [slice_1, slice_2, base_slice]
    actual_slices = backend_conversions._compute_task_slices(
            run_task_req, backend_config, True)
    self.assertEqual(expected_slices, actual_slices)

  def test_compute_task_slices_base_slice_only(self):
    exec_secs = 180
    grace_secs = 60
    start_deadline_secs = int(utils.time_time() + 120)

    run_task_req = backend_pb2.RunTaskRequest(
        backend_config=struct_pb2.Struct(
            fields={
                'wait_for_capacity':
                    struct_pb2.Value(bool_value=True),
                'agent_binary_cipd_filename':
                    struct_pb2.Value(string_value='agent'),
                'agent_binary_cipd_pkg':
                    struct_pb2.Value(string_value='agent/package/${platform}'),
                'agent_binary_cipd_vers':
                    struct_pb2.Value(string_value='latest')
            }),
        grace_period=duration_pb2.Duration(seconds=grace_secs),
        execution_timeout=duration_pb2.Duration(seconds=exec_secs),
        start_deadline=timestamp_pb2.Timestamp(seconds=start_deadline_secs),
        dimensions=[
            req_dim_prpc('required-1', 'req-1'),
            req_dim_prpc('required-2', 'req-2'),
            req_dim_prpc('required-1', 'req-1-2')
        ])
    backend_config = backend_conversions.ingest_backend_config(
        run_task_req.backend_config)

    base_slice = task_request.TaskSlice(
        wait_for_capacity=True,
        expiration_secs=120,
        properties=task_request.TaskProperties(
            command=[
                u'agent', '-cache-base', 'cache', '-task-id',
                '${SWARMING_TASK_ID}'
            ],
            has_secret_bytes=False,
            execution_timeout_secs=exec_secs,
            grace_period_secs=grace_secs,
            dimensions_data={
                u'required-1': [u'req-1', u'req-1-2'],
                u'required-2': [u'req-2']
            },
            cipd_input=task_request.CipdInput(packages=[
                task_request.CipdPackage(
                    path='.',
                    package_name=u'agent/package/${platform}',
                    version=u'latest')
            ])),
    )

    self.assertEqual([base_slice],
                     backend_conversions._compute_task_slices(
                         run_task_req, backend_config, False))

  def test_compute_task_slices_exceptions(self):
    backend_config = swarming_pb2.SwarmingTaskBackendConfig()

    run_task_req = backend_pb2.RunTaskRequest(
        grace_period=duration_pb2.Duration(nanos=42))
    with self.assertRaisesRegexp(handlers_exceptions.BadRequestException,
                                 'grace_period.nanos'):
      backend_conversions._compute_task_slices(run_task_req, backend_config,
                                               False)

    run_task_req = backend_pb2.RunTaskRequest(
        execution_timeout=duration_pb2.Duration(nanos=42))
    with self.assertRaisesRegexp(handlers_exceptions.BadRequestException,
                                 'execution_timeout.nanos'):
      backend_conversions._compute_task_slices(run_task_req, backend_config,
                                               False)

    run_task_req = backend_pb2.RunTaskRequest(caches=[
        project_config_pb2.BuilderConfig.CacheEntry(
            wait_for_warm_cache_secs=1)
    ])
    with self.assertRaisesRegexp(handlers_exceptions.BadRequestException,
                                 'wait_for_warm_cache_secs'):
      backend_conversions._compute_task_slices(run_task_req, backend_config,
                                               False)

    run_task_req = backend_pb2.RunTaskRequest(dimensions=[
        common_pb2.RequestedDimension(
            expiration=duration_pb2.Duration(nanos=42))
    ])
    with self.assertRaisesRegexp(handlers_exceptions.BadRequestException,
                                 'expiration.nanos'):
      backend_conversions._compute_task_slices(run_task_req, backend_config,
                                               False)

  def test_convert_results_to_tasks(self):
    def test_task(status, task_id=None, summary=None, set_timeout=False,
                  set_exhaustion=False):
      task = task_pb2.Task(
          id=task_pb2.TaskID(target='swarming://%s' %
                                app_identity.get_application_id()),
          status=status)
      if task_id is not None:
        task.id.id = str(task_id)
      if summary is not None:
        task.summary_html = summary
      if set_timeout:
        task.status_details.timeout.SetInParent()
      if set_exhaustion:
        task.status_details.resource_exhaustion.SetInParent()
      return task

    # PENDING TaskResultSummary
    req_1 = task_request.TaskRequest(id=1230)
    req_1.key=task_request.new_request_key()
    res_sum_1 = task_result.TaskResultSummary(
        parent=req_1.key,
        state=task_result.State.PENDING)

    # EXPIRED TaskRunResult
    req_2 = task_request.TaskRequest(id=1220)
    req_2.key = task_request.new_request_key()
    res_sum_2 = task_result.TaskResultSummary(
        key=ndb.Key('TaskResultSummary', 1, parent=req_2.key))
    run_res_2 = task_result.TaskRunResult(
        key=ndb.Key('TaskRunResult', 1, parent=res_sum_2.key),
        state=task_result.State.EXPIRED)

    # CANCELED TaskRunResult
    req_3 = task_request.TaskRequest(id=1230)
    req_3.key=task_request.new_request_key()
    res_sum_3 = task_result.TaskResultSummary(
        key=ndb.Key('TaskResultSummary', 1, parent=req_3.key))
    run_res_3 = task_result.TaskRunResult(
        key=ndb.Key('TaskRunResult', 2,  parent=res_sum_3.key),
        state=task_result.State.CANCELED)

    # COMPLETED TaskRunResult failure
    req_4 = task_request.TaskRequest(id=1240)
    req_4.key = task_request.new_request_key()
    res_sum_4 = task_result.TaskResultSummary(
        key=ndb.Key('TaskResultSummary', 1, parent=req_4.key))
    run_res_4 = task_result.TaskRunResult(
        key=ndb.Key('TaskRunResult', 2, parent=res_sum_4.key),
        state=task_result.State.COMPLETED,
        exit_code=1)

    # COMPLETED TaskRunResult success
    req_5 = task_request.TaskRequest(id=1250)
    req_5.key = task_request.new_request_key()
    res_sum_5 = task_result.TaskResultSummary(
        key=ndb.Key('TaskResultSummary', 1, parent=req_5.key))
    run_res_5 = task_result.TaskRunResult(
        key=ndb.Key('TaskRunResult', 2, parent=res_sum_5.key),
        state=task_result.State.COMPLETED,
        exit_code=0)

    results = [None, res_sum_1, run_res_2, run_res_3, run_res_4, run_res_5]

    expected_tasks = [
        test_task(
            common_pb2.INFRA_FAILURE,
            task_id='1',
            summary='Swarming task 1 not found'),
        test_task(common_pb2.SCHEDULED, task_id=res_sum_1.task_id),
        test_task(
            common_pb2.INFRA_FAILURE,
            task_id=run_res_2.task_id,
            summary='Task expired.',
            set_timeout=True,
            set_exhaustion=True),
        test_task(common_pb2.CANCELED, task_id=run_res_3.task_id),
        test_task(
            common_pb2.FAILURE,
            task_id=run_res_4.task_id,
            summary='Task completed with failure.'),
        test_task(common_pb2.SUCCESS, task_id=run_res_5.task_id),
    ]

    self.assertEqual(
        expected_tasks,
        backend_conversions.convert_results_to_tasks(results, [
            '1', res_sum_1.task_id, run_res_2.task_id, run_res_3.task_id,
            run_res_4.task_id, run_res_5.task_id
        ]))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
