#!/usr/bin/env vpython
# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
import datetime
import mock
import sys
import unittest

# This must precede import of appengine libs as it adds third_party packages
# to path.
import swarming_test_env

swarming_test_env.setup_test_env()

import message_conversion_prpc
from proto.api_v2 import swarming_pb2
from test_support import test_case
from server import task_request

FAKE_UTCNOW = datetime.datetime(2016, 4, 7)


class TestMessageConversion(test_case.TestCase):
  no_run = 1

  def _create_default_new_task_request_proto(self):
    ntr = swarming_pb2.NewTaskRequest(bot_ping_tolerance_secs=600,
                                      name='job1',
                                      parent_task_id='1d69ba3ea8008810',
                                      priority=20,
                                      pubsub_auth_token='token',
                                      pubsub_topic='projects/a/topics/b',
                                      pubsub_userdata='userdata',
                                      realm='test:task_realm',
                                      service_account='some@example.com',
                                      tags=[u'a:tag'],
                                      user='joe@localhost')
    ntr.task_slices.extend([
        self._create_default_task_slice_proto('s1'),
        self._create_default_task_slice_proto('s2'),
    ])
    ntr.resultdb.enable = True

    return ntr

  def _create_default_task_slice_proto(self, arg='arg'):
    ts = swarming_pb2.TaskSlice(expiration_secs=180, wait_for_capacity=True)
    # Hack to get min line length.
    props = ts.properties
    props.caches.extend([
        swarming_pb2.CacheEntry(name='c1', path='p1'),
        swarming_pb2.CacheEntry(name='c2', path='p2'),
    ])
    props.cipd_input.client_package.package_name = ('infra/tools/'
                                                    'cipd/${platform}')
    props.cipd_input.client_package.version = 'git_revision:deadbeef'
    props.cipd_input.packages.extend([
        swarming_pb2.CipdPackage(package_name='rm',
                                 path='bin',
                                 version='git_revision:deadbeef')
    ])
    props.cipd_input.server = 'https://pool.config.cipd.example.com'
    props.command[:] = ['python', '-c', 'print(1)']
    props.containment.containment_type = swarming_pb2.ContainmentType.AUTO
    props.dimensions.extend([
        swarming_pb2.StringPair(key='os', value='Amiga'),
        swarming_pb2.StringPair(key='pool', value='default')
    ])
    props.execution_timeout_secs = 3600
    props.grace_period_secs = 30
    props.idempotent = False
    props.io_timeout_secs = 1200
    props.outputs[:] = ['foo', 'path/to/foobar']
    props.command[:] = ['python', 'run_test.py', arg]
    props.env_prefixes.extend([
        swarming_pb2.StringListPair(key=u'foo', value=[u'bar', u'baz']),
    ])
    props.env.extend([swarming_pb2.StringPair(key=u'foo', value=u'bar')])
    props.cas_input_root.cas_instance = ("projects/chromium-swarm-dev/ins"
                                         "tances/default_instance")
    props.cas_input_root.digest.hash = (b"d00b122490e024587990b55c248dc82d3"
                                        b"46c54d89019d6eff61ddc9743ea0442")
    props.cas_input_root.digest.size_bytes = 258

    return ts

  def _create_default_task_request_task_slice(self, arg='arg'):
    return task_request.TaskSlice(
        expiration_secs=180,
        properties=task_request.TaskProperties(
            caches=[
                task_request.CacheEntry(name='c1', path='p1'),
                task_request.CacheEntry(name='c2', path='p2'),
            ],
            cas_input_root=task_request.CASReference(
                cas_instance=
                "projects/chromium-swarm-dev/instances/default_instance",
                digest=task_request.Digest(
                    hash=(b"d00b122490e024587990b55c248dc82d346c54"
                          b"d89019d6eff61ddc9743ea0442"),
                    size_bytes=258)),
            cipd_input=task_request.CipdInput(
                client_package=task_request.CipdPackage(
                    package_name=u'infra/tools/cipd/${platform}',
                    path=None,
                    version=u'git_revision:deadbeef'),
                packages=[
                    task_request.CipdPackage(package_name=u'rm',
                                             path=u'bin',
                                             version=u'git_revision:deadbeef')
                ]),
            command=[u'python', u'run_test.py', arg],
            containment=task_request.Containment(containment_type=2),
            dimensions_data={
                u'os': [u'Amiga'],
                u'pool': [u'default']
            },
            env={u'foo': u'bar'},
            env_prefixes={u'foo': [u'bar', u'baz']},
            execution_timeout_secs=3600,
            grace_period_secs=30,
            has_secret_bytes=False,
            idempotent=False,
            inputs_ref=None,
            io_timeout_secs=1200,
            outputs=[u'foo', u'path/to/foobar'],
            relative_cwd=u''),
        wait_for_capacity=True)

  def _create_default_new_task_request(self):
    return task_request.TaskRequest(
        bot_ping_tolerance_secs=600,
        created_ts=FAKE_UTCNOW,
        manual_tags=[u'a:tag'],
        name=u'job1',
        parent_task_id='1d69ba3ea8008810',
        priority=20,
        pubsub_auth_token='token',
        pubsub_topic='projects/a/topics/b',
        pubsub_userdata='userdata',
        rbe_instance=None,
        realm=u'test:task_realm',
        resultdb=task_request.ResultDBCfg(enable=True),
        service_account=u'some@example.com',
        task_slices=[
            self._create_default_task_request_task_slice('s1'),
            self._create_default_task_request_task_slice('s2'),
        ],
        user=u'joe@localhost')

  @mock.patch('components.utils.utcnow', lambda: FAKE_UTCNOW)
  def test_task_request_from_new_task_request(self):
    ntr = self._create_default_new_task_request_proto()
    actual = message_conversion_prpc.new_task_request_from_rpc(ntr)
    expected = (self._create_default_new_task_request(), None, 'TEMPLATE_AUTO')
    self.assertEqual(expected, actual)

  def test_that_global_expiration_throws_error(self):
    ntr = self._create_default_new_task_request_proto()
    ntr.expiration_secs = 1000
    with self.assertRaises(ValueError) as ctx:
      message_conversion_prpc.new_task_request_from_rpc(ntr)

    self.assertTrue(
        'When using task_slices, do not specify a global expiration_secs' in
        str(ctx.exception))

  def test_that_different_secret_bytes_throws_error(self):
    ntr = self._create_default_new_task_request_proto()
    ts1 = self._create_default_task_slice_proto()
    ts1.properties.secret_bytes = b'123'
    ts2 = self._create_default_task_slice_proto()
    ts2.properties.secret_bytes = b'456'
    ntr.ClearField('task_slices')
    ntr.task_slices.extend([ts1, ts2])

    with self.assertRaises(ValueError) as ctx:
      message_conversion_prpc.new_task_request_from_rpc(ntr)

    self.assertTrue(
        'When using secret_bytes multiple times, all values must match' in str(
            ctx.exception))

  def test_that_duplicate_env_variables_throws_error(self):
    ntr = self._create_default_new_task_request_proto()
    pairs = [
        swarming_pb2.StringPair(key='k', value='v'),
        swarming_pb2.StringPair(key='k', value='v')
    ]
    ntr.task_slices[0].properties.env.extend(pairs)
    with self.assertRaises(ValueError) as ctx:
      message_conversion_prpc.new_task_request_from_rpc(ntr)
    self.assertTrue('same environment variable key cannot be specified twice' in
                    str(ctx.exception))

  def test_that_duplicate_env_prefixes_throws_error(self):
    ntr = self._create_default_new_task_request_proto()
    pairs = [
        swarming_pb2.StringListPair(key='k', value=['v']),
        swarming_pb2.StringListPair(key='k', value=['v'])
    ]
    ntr.task_slices[0].properties.env_prefixes.extend(pairs)
    with self.assertRaises(ValueError) as ctx:
      message_conversion_prpc.new_task_request_from_rpc(ntr)
    self.assertTrue('same environment prefix key cannot be specified twice' in
                    str(ctx.exception))

  def test_none_containment_type_does_not_break(self):
    props = self._create_default_task_request_task_slice().properties
    props.containment = None
    props_proto = message_conversion_prpc._task_properties(props)
    self.assertFalse(props_proto.HasField("containment"))

  def test_none_env_does_not_break(self):
    props = self._create_default_task_request_task_slice().properties
    props.env = None
    props_proto = message_conversion_prpc._task_properties(props)
    self.assertFalse(props_proto.env)

  def test_bot_ping_tolerance_is_filled_correctly(self):
    ntr = self._create_default_new_task_request_proto()
    ntr.bot_ping_tolerance_secs = 0
    actual, _, _ = message_conversion_prpc.new_task_request_from_rpc(ntr)
    self.assertEqual(actual.bot_ping_tolerance_secs,
                     task_request.DEFAULT_BOT_PING_TOLERANCE)

  def test_rbe_value_is_set(self):
    request = self._create_default_new_task_request()
    rbe = "some.rbe.instance"
    request.rbe_instance = rbe
    request.expiration_ts = datetime.datetime.now()
    proto = message_conversion_prpc.task_request_response(request)
    self.assertEqual(proto.rbe_instance, rbe)

  def test_log_task_request(self):
    ntr = self._create_default_new_task_request_proto()
    ntr.properties.secret_bytes = b'hello'
    for ts in ntr.task_slices:
      ts.properties.secret_bytes = b'hello'
    ntrc = message_conversion_prpc._log_new_task_request(ntr)
    self.assertEqual(ntrc.properties.secret_bytes, b'<REDACTED>')
    for ts in ntrc.task_slices:
      self.assertEqual(ts.properties.secret_bytes, b'<REDACTED>')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
