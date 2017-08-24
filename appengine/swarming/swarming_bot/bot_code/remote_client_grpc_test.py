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
import remote_client_grpc


class FakeGrpcProxy(object):
  def __init__(self, testobj):
    self._testobj = testobj

  @property
  def prefix(self):
    return 'inst'

  def call_unary(self, name, request):
    return self._testobj._handle_call(name, request)


class TestRemoteClientGrpc(auto_stub.TestCase):
  def setUp(self):
    super(TestRemoteClientGrpc, self).setUp()
    self._num_sleeps = 0
    def fake_sleep(_time):
      self._num_sleeps += 1
    self.mock(time, 'sleep', fake_sleep)
    self._client = remote_client_grpc.RemoteClientGrpc('', FakeGrpcProxy(self))
    self._expected = []
    self._error_codes = []

  def _handle_call(self, method, request):
    """This is called by FakeGrpcProxy to implement fake calls"""
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

  def _get_bot_attributes_dict(self):
    """Gets the attributes of a basic bot; must match next function"""
    return {
        'version': '123',
        'dimensions': {
            'id': ['robotcop'],
            'mammal': ['ferrett', 'wombat'],
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

  def _get_worker_proto(self):
    """Gets the worker proto of a basic bot; must match prior function"""
    worker = remote_client_grpc.worker_manager_pb2.Worker()
    worker.name = 'inst/workers/robotcop'
    devprops = worker.device.properties
    d1 = devprops.add()
    d1.key.custom = 'id'
    d1.value = 'robotcop'
    d2 = devprops.add()
    d2.key.custom = 'mammal'
    d2.value = 'ferrett'
    d3 = devprops.add()
    d3.key.custom = 'mammal'
    d3.value = 'wombat'
    props = worker.properties
    props.get_or_create_list('audio').extend(
        ['Rachmaninov', 'Stravinsky'])
    props['cost_usd_hour'] = 3.141
    props['cpu'] = 'Intel 8080'
    disks = props.get_or_create_struct('disks')
    disks['mount'] = 'path/to/mount'
    disks['volume'] = 'turned-to-eleven'
    props['sleep_streak'] = 8
    props['bot_version'] = '123'
    return worker

  def test_handshake(self):
    """Tests the handshake proto"""
    msg_req = remote_client_grpc.worker_manager_pb2.CreateWorkerRequest()
    msg_req.parent = 'inst'
    msg_req.worker.CopyFrom(self._get_worker_proto())

    # Create proto response
    msg_rsp = self._get_worker_proto()
    # Add a pool
    poolprop = msg_rsp.device.properties.add()
    poolprop.key.custom = 'pool'
    poolprop.value = 'dead'

    # Execute call and verify response
    expected_call = ('CreateWorker', msg_req, msg_rsp)
    self._expected.append(expected_call)
    response = self._client.do_handshake(self._get_bot_attributes_dict())
    self.assertEqual(response, {
        'bot_version': u'123',
        'bot_group_cfg': {
            'dimensions': {
                u'pool': [u'dead'],
            },
        },
    })


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.TestCase.maxDiff = None
  unittest.main()
