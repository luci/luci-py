#!/usr/bin/python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for lease_management.py."""

import json
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb
from protorpc.remote import protojson
import webtest

from components import machine_provider
from test_support import test_case

import lease_management


def rpc_to_json(rpc_message):
  """Converts the given RPC message to a POSTable JSON dict.

  Args:
    rpc_message: A protorpc.message.Message instance.

  Returns:
    A string representing a JSON dict.
  """
  return json.loads(protojson.encode_message(rpc_message))


class GetLeaseRequestsTest(test_case.TestCase):
  """Tests for lease_management.get_lease_requests."""

  def test_machine_type_not_found(self):
    requests = lease_management.get_lease_requests(
        ndb.Key(lease_management.MachineType, 'not-found'),
        'https://example.com',
    )
    self.failIf(requests)

  def test_machine_type_at_capacity(self):
    machine_type = lease_management.MachineType(
        id='at-capacity',
        leases=[
          lease_management.MachineLease(request_hash='fake-hash-1'),
          lease_management.MachineLease(request_hash='fake-hash-2'),
        ],
        mp_dimensions=machine_provider.Dimensions(
            os_family=machine_provider.OSFamily.LINUX,
        ),
        request_id_base='at-capacity',
        target_size=2,
    )
    machine_type.put()

    requests = lease_management.get_lease_requests(
        machine_type.key, 'https://example.com')
    self.failIf(requests)

  def test_machine_type_not_enabled(self):
    machine_type = lease_management.MachineType(
      id='not-enabled',
      enabled=False,
      mp_dimensions=machine_provider.Dimensions(
          os_family=machine_provider.OSFamily.LINUX,
      ),
      request_id_base='not-enabled',
      target_size=2,
    )
    machine_type.put()

    requests = lease_management.get_lease_requests(
        machine_type.key, 'https://example.com')
    self.failIf(requests)

  def test_need_one(self):
    machine_type = lease_management.MachineType(
      id='need-one',
      mp_dimensions=machine_provider.Dimensions(
          os_family=machine_provider.OSFamily.LINUX,
      ),
      request_id_base='need-one',
      target_size=1,
    )
    machine_type.put()

    requests = lease_management.get_lease_requests(
        machine_type.key, 'https://example.com')
    self.assertEqual(len(requests), 1)

  def test_ensure_correct_request_ids(self):
    machine_type = lease_management.MachineType(
      id='ensure-correct-request-ids',
      mp_dimensions=machine_provider.Dimensions(
          os_family=machine_provider.OSFamily.LINUX,
      ),
      request_count=2,
      request_id_base='ensure-correct-request-ids',
      target_size=2,
    )
    machine_type.put()

    requests = lease_management.get_lease_requests(
        machine_type.key, 'https://example.com')
    self.assertEqual(len(requests), 2)
    self.assertEqual(requests[0].request_id, 'ensure-correct-request-ids-3')
    self.assertEqual(requests[1].request_id, 'ensure-correct-request-ids-4')


class UpdateLeasesRequestsTest(test_case.TestCase):
  """Tests for lease_management.update_leases."""

  def test_machine_type_updated(self):
    machine_type = lease_management.MachineType(
      id='updated',
      leases=[],
      mp_dimensions=machine_provider.Dimensions(
          os_family=machine_provider.OSFamily.LINUX,
      ),
      request_id_base='updated',
      target_size=2,
    )
    machine_type.put()

    responses = rpc_to_json(machine_provider.BatchedLeaseResponse(responses=[
        machine_provider.LeaseResponse(
            client_request_id='already-updated-1',
            request_hash='fake-hash-1',
        ),
        machine_provider.LeaseResponse(
            client_request_id='already-updated-2',
            request_hash='fake-hash-2',
        ),
    ]))

    lease_management.update_leases(machine_type.key, responses)
    updated_machine_type = machine_type.key.get()
    self.assertEqual(updated_machine_type.request_count, 2)
    self.assertEqual(len(updated_machine_type.leases), 2)
    request_hashes = sorted(
        lease.request_hash for lease in updated_machine_type.leases)
    self.assertEqual(request_hashes[0], 'fake-hash-1')
    self.assertEqual(request_hashes[1], 'fake-hash-2')

  def test_machine_type_already_updated(self):
    machine_type = lease_management.MachineType(
      id='already-updated',
      leases=[
          lease_management.MachineLease(request_hash='fake-hash-1'),
          lease_management.MachineLease(request_hash='fake-hash-2'),
      ],
      mp_dimensions=machine_provider.Dimensions(
          os_family=machine_provider.OSFamily.LINUX,
      ),
      request_count=2,
      request_id_base='already-updated',
      target_size=2,
    )
    machine_type.put()

    responses = rpc_to_json(machine_provider.BatchedLeaseResponse(responses=[
        machine_provider.LeaseResponse(
            client_request_id='already-updated-1',
            request_hash='fake-hash-1',
        ),
        machine_provider.LeaseResponse(
            client_request_id='already-updated-2',
            request_hash='fake-hash-2',
        ),
    ]))

    lease_management.update_leases(machine_type.key, responses)
    updated_machine_type = machine_type.key.get()
    self.assertEqual(
        machine_type.request_count, updated_machine_type.request_count)
    self.assertEqual(machine_type.leases[0].request_hash,
                     updated_machine_type.leases[0].request_hash)
    self.assertEqual(machine_type.leases[1].request_hash,
                     updated_machine_type.leases[1].request_hash)


if __name__ == '__main__':
  unittest.main()
