#!/usr/bin/env python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import threading
import time
import unittest

import test_env_bot_code
test_env_bot_code.setup_test_env()

from depot_tools import auto_stub

try:
  import remote_client_grpc
except ImportError as e:
  print('Could not import gRPC remote client, likely due to missing grpc '
        'library. Skipping tests.')
  sys.exit(0)


class FakeBotServiceStub(object):
  def __init__(self, testobj):
    self._testobj = testobj

  def TaskUpdate(self, request, **_kwargs):
    return self._testobj._handle_call('TaskUpdate', request)

  def Handshake(self, request, **_kwargs):
    return self._testobj._handle_call('Handshake', request)

  def Poll(self, request, **_kwargs):
    return self._testobj._handle_call('Poll', request)


class TestRemoteClientGrpc(auto_stub.TestCase):
  def setUp(self):
    super(TestRemoteClientGrpc, self).setUp()
    self._client = remote_client_grpc.RemoteClientGrpc('1.2.3.4:90')
    self._client._stub = FakeBotServiceStub(self)
    self._expected = []

  def _handle_call(self, method, request):
    """This is called by FakeBotServiceStub to implement fake calls"""
    # Pop off the first item on the list
    self.assertTrue(len(self._expected) > 0)
    expected, self._expected = self._expected[0], self._expected[1:]
    # Each element of the "expected" array should be a 3-tuple:
    #    * The name of the method (eg 'TaskUpdate')
    #    * The proto request
    #    * The proto response
    self.assertEqual(method, expected[0])
    self.assertEqual(request, expected[1])
    return expected[2]

  def get_bot_attributes_dict(self):
    """Gets the attributes of a basic bot"""
    return {
        'version': '123',
        'dimensions': {
            'mammal': ['ferrett', 'wombat'],
            'pool': ['dead'],
        },
        'state': {
            'audio': ['Rachmaninov', 'Stravinsky'],
            'cost_usd_hour': 3.141,
            'cpu': 'Intel 8080',
            'disks': {
                'mount': 'path/to/mount',
                'volume': 'turned-to-eleven',
            },
            'sleep_streak': 8,
        },
    }

  def get_bot_attributes_proto(self):
    """Gets the attributes proto of a basic bot; must match prior function"""
    attributes = remote_client_grpc.swarming_bot_pb2.Attributes()
    attributes.version = '123'
    # Note that dimensions are sorted
    d1 = attributes.dimensions.add()
    d1.name = 'mammal'
    d1.values.extend(['ferrett', 'wombat'])
    d2 = attributes.dimensions.add()
    d2.name = 'pool'
    d2.values.append('dead')
    attributes.state.audio.extend(['Rachmaninov', 'Stravinsky'])
    attributes.state.cost_usd_hour = 3.141
    attributes.state.cpu = 'Intel 8080'
    attributes.state.sleep_streak = 8
    disk1 = attributes.state.disks.fields['mount']
    disk1.string_value = 'path/to/mount'
    disk2 = attributes.state.disks.fields['volume']
    disk2.string_value = 'turned-to-eleven'
    return attributes

  def test_handshake(self):
    """Tests the handshake proto"""
    msg_req = remote_client_grpc.swarming_bot_pb2.HandshakeRequest()
    msg_req.attributes.CopyFrom(self.get_bot_attributes_proto())

    # Create proto response
    msg_rsp = remote_client_grpc.swarming_bot_pb2.HandshakeResponse()
    msg_rsp.server_version = '101'
    msg_rsp.bot_version = '102'
    d1 = msg_rsp.bot_group_cfg.dimensions.add()
    d1.name = 'mammal'
    d1.values.extend(['kangaroo', 'emu'])

    # Execute call and verify response
    expected_call = ('Handshake', msg_req, msg_rsp)
    self._expected.append(expected_call)
    response = self._client.do_handshake(self.get_bot_attributes_dict())
    self.assertEqual(response, {
        'server_version': u'101',
        'bot_version': u'102',
        'bot_group_cfg_version': u'',
        'bot_group_cfg': {
            'dimensions': {
                u'mammal': [u'kangaroo', u'emu'],
            },
        },
    })

  def test_poll_manifest(self):
    """Verifies that we can generate a reasonable manifest from a proto"""
    msg_req = remote_client_grpc.swarming_bot_pb2.PollRequest()
    msg_req.attributes.CopyFrom(self.get_bot_attributes_proto())

    # Create dict response
    dict_rsp = {
        'bot_id': u'baby_bot',
        'command': None,
        'dimensions': {
            u'mammal': u'emu',
            u'pool': u'dead',
        },
        'env': {
            u'co2': u'400ppm',
            u'n2': u'80%',
            u'o2': u'20%',
        },
        'extra_args': None,
        'grace_period': 1,
        'hard_timeout': 2,
        'io_timeout': 3,
        'isolated': {
            'namespace': u'default-gzip',
            'input': u'123abc',
            'server': '1.2.3.4:90', # from when client was created
        },
        'outputs': [u'foo.o', u'foo.log'],
        'task_id': u'ifyouchoosetoacceptit',
    }
    # Create proto response
    msg_rsp = remote_client_grpc.swarming_bot_pb2.PollResponse()
    msg_rsp.cmd = remote_client_grpc.swarming_bot_pb2.PollResponse.RUN
    msg_rsp.manifest.bot_id = 'baby_bot'
    msg_rsp.manifest.dimensions['mammal'] = 'emu'
    msg_rsp.manifest.dimensions['pool'] = 'dead'
    msg_rsp.manifest.env['co2'] = '400ppm'
    msg_rsp.manifest.env['n2'] = '80%'
    msg_rsp.manifest.env['o2'] = '20%'
    msg_rsp.manifest.grace_period = 1
    msg_rsp.manifest.hard_timeout = 2
    msg_rsp.manifest.io_timeout = 3
    msg_rsp.manifest.isolated.namespace = 'default-gzip'
    msg_rsp.manifest.isolated.input = '123abc'
    msg_rsp.manifest.outputs.extend(['foo.o', 'foo.log'])
    msg_rsp.manifest.task_id = 'ifyouchoosetoacceptit'

    # Execute call and verify response
    expected_call = ('Poll', msg_req, msg_rsp)
    self._expected.append(expected_call)
    cmd, value = self._client.poll(self.get_bot_attributes_dict())
    self.assertEqual(cmd, 'run')
    self.assertEqual(value, dict_rsp)

  def test_update_no_output_or_code(self):
    """Includes only a basic param set"""
    # Create params dict
    params = {
        'bot_overhead': 3.141,
        'cost_usd': 0.001,
    }

    # Create expected request proto
    msg_req = remote_client_grpc.swarming_bot_pb2.TaskUpdateRequest()
    msg_req.id = 'baby_bot'
    msg_req.task_id = 'abc123'
    msg_req.bot_overhead = 3.141
    msg_req.cost_usd = 0.001

    # Create proto response
    msg_rsp = remote_client_grpc.swarming_bot_pb2.TaskUpdateResponse()

    # Execute call and verify response
    expected_call = ('TaskUpdate', msg_req, msg_rsp)
    self._expected.append(expected_call)
    response = self._client.post_task_update('abc123', 'baby_bot', params)
    self.assertTrue(response)

  def test_update_final(self):
    """Includes output chunk, exit code and full param set"""
    # Create params dict
    params = {
        'outputs_ref': {
            'isolated': 'def987',
            'isolatedserver': 'foo',
            'namespace': 'default-gzip',
        },
        'isolated_stats': {
            'upload': {
                'duration': 0.5,
                'items_cold': 'YWJj', # b64(u'abc')
            },
            'download': {
                'initial_size': 1234567890,
            },
        },
    }

    # Create expected request proto
    msg_req = remote_client_grpc.swarming_bot_pb2.TaskUpdateRequest()
    msg_req.id = 'baby_bot'
    msg_req.task_id = 'abc123'
    msg_req.exit_status.code = -5
    msg_req.output_chunk.data = 'abc'
    msg_req.output_chunk.offset = 7
    msg_req.outputs_ref.isolated = 'def987'
    msg_req.outputs_ref.isolatedserver = 'foo'
    msg_req.outputs_ref.namespace = 'default-gzip'
    msg_req.isolated_stats.upload.duration = 0.5
    msg_req.isolated_stats.upload.items_cold = 'abc'
    msg_req.isolated_stats.download.initial_size = 1234567890

    # Create proto response
    msg_rsp = remote_client_grpc.swarming_bot_pb2.TaskUpdateResponse()

    # Execute call and verify response
    expected_call = ('TaskUpdate', msg_req, msg_rsp)
    self._expected.append(expected_call)
    response = self._client.post_task_update('abc123', 'baby_bot', params,
                                             ['abc', 7], -5)
    self.assertTrue(response)

if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.TestCase.maxDiff = None
  unittest.main()
