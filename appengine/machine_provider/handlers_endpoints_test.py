#!/usr/bin/python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for handlers_endpoints.py."""

import json
import unittest

import test_env
test_env.setup_test_env()

from protorpc.remote import protojson
import webtest

from components import auth_testing
from test_support import test_case

import handlers_endpoints
import rpc_messages


def rpc_to_json(rpc_message):
  """Converts the given RPC message to a POSTable JSON dict.

  Args:
    rpc_message: A protorpc.message.Message instance.

  Returns:
    A string representing a JSON dict.
  """
  return json.loads(protojson.encode_message(rpc_message))


def jsonish_dict_to_rpc(dictionary, rpc_message_type):
  """Converts the given dict to the specified RPC message type.

  Args:
    dictionary: A dict instance containing only values which can be
      encoded as JSON.
    rpc_message_type: A type inheriting from protorpc.message.Message.

  Returns:
    An object of type rpc_message_type.
  """
  return protojson.decode_message(rpc_message_type, json.dumps(dictionary))


class CatalogTest(test_case.EndpointsTestCase):
  """Tests for handlers_endpoints.CatalogEndpoints."""
  api_service_cls = handlers_endpoints.CatalogEndpoints

  def setUp(self):
    super(CatalogTest, self).setUp()
    app = handlers_endpoints.create_endpoints_app()
    self.app = webtest.TestApp(app)

  def test_add(self):
    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    auth_testing.mock_get_current_identity(self)

    response = jsonish_dict_to_rpc(
        self.call_api('add_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.failIf(response.error)

  def test_add_no_hostname(self):
    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    auth_testing.mock_get_current_identity(self)

    response = jsonish_dict_to_rpc(
        self.call_api('add_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertEqual(
      response.error,
      rpc_messages.CatalogManipulationRequestError.UNSPECIFIED_HOSTNAME,
    )

  def test_add_duplicate(self):
    request_1 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    request_2 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    auth_testing.mock_get_current_identity(self)

    response_1 = jsonish_dict_to_rpc(
        self.call_api('add_machine', request_1).json,
        rpc_messages.CatalogManipulationResponse,
    )
    response_2 = jsonish_dict_to_rpc(
        self.call_api('add_machine', request_2).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.failIf(response_1.error)
    self.assertEqual(
        response_2.error,
        rpc_messages.CatalogManipulationRequestError.HOSTNAME_REUSE,
    )

  def test_add_batch_empty(self):
    request = rpc_to_json(rpc_messages.CatalogMachineBatchAdditionRequest())
    auth_testing.mock_get_current_identity(self)

    response = jsonish_dict_to_rpc(
        self.call_api('add_machines', request).json,
        rpc_messages.CatalogBatchManipulationResponse,
    )
    self.failIf(response.responses)

  def test_add_batch(self):
    request = rpc_to_json(rpc_messages.CatalogMachineBatchAdditionRequest(
        requests=[
            rpc_messages.CatalogMachineAdditionRequest(
                dimensions=rpc_messages.Dimensions(
                    hostname='fake-host-1',
                    os_family=rpc_messages.OSFamily.LINUX,
                ),
            ),
            rpc_messages.CatalogMachineAdditionRequest(
                dimensions=rpc_messages.Dimensions(
                    hostname='fake-host-2',
                    os_family=rpc_messages.OSFamily.WINDOWS,
                ),
            ),
            rpc_messages.CatalogMachineAdditionRequest(
                dimensions=rpc_messages.Dimensions(
                    hostname='fake-host-1',
                    os_family=rpc_messages.OSFamily.OSX,
                ),
            ),
        ],
    ))
    auth_testing.mock_get_current_identity(self)

    response = jsonish_dict_to_rpc(
        self.call_api('add_machines', request).json,
        rpc_messages.CatalogBatchManipulationResponse,
    )
    self.assertEqual(len(response.responses), 3)
    self.failIf(response.responses[0].error)
    self.failIf(response.responses[1].error)
    self.assertEqual(
        response.responses[2].error,
        rpc_messages.CatalogManipulationRequestError.HOSTNAME_REUSE,
    )

  def test_delete(self):
    request_1 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    request_2 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    request_3 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.WINDOWS,
        ),
    ))
    auth_testing.mock_get_current_identity(self)

    response_1 = jsonish_dict_to_rpc(
        self.call_api('add_machine', request_1).json,
        rpc_messages.CatalogManipulationResponse,
    )
    response_2 = jsonish_dict_to_rpc(
        self.call_api('delete_machine', request_2).json,
        rpc_messages.CatalogManipulationResponse,
    )
    response_3 = jsonish_dict_to_rpc(
        self.call_api('add_machine', request_3).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.failIf(response_1.error)
    self.failIf(response_2.error)
    self.failIf(response_3.error)

  def test_delete_invalid(self):
    request_1 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host-1',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    request_2 = rpc_to_json(rpc_messages.CatalogMachineDeletionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host-2',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    request_3 = rpc_to_json(rpc_messages.CatalogMachineDeletionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host-1',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    auth_testing.mock_get_current_identity(self)

    response_1 = jsonish_dict_to_rpc(
        self.call_api('add_machine', request_1).json,
        rpc_messages.CatalogManipulationResponse,
    )
    response_2 = jsonish_dict_to_rpc(
        self.call_api('delete_machine', request_2).json,
        rpc_messages.CatalogManipulationResponse,
    )
    response_3 = jsonish_dict_to_rpc(
        self.call_api('add_machine', request_3).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.failIf(response_1.error)
    self.assertEqual(
        response_2.error,
        rpc_messages.CatalogManipulationRequestError.ENTRY_NOT_FOUND,
    )
    self.assertEqual(
        response_3.error,
        rpc_messages.CatalogManipulationRequestError.HOSTNAME_REUSE,
    )

  def test_modify(self):
    request = rpc_to_json(rpc_messages.CatalogCapacityModificationRequest(
        count=1,
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.OSX,
        ),
    ))
    auth_testing.mock_get_current_identity(self)

    response = jsonish_dict_to_rpc(
        self.call_api('modify_capacity', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.failIf(response.error)


class MachineProviderLeaseTest(test_case.EndpointsTestCase):
  """Tests for handlers_endpoints.MachineProviderEndpoints.lease."""
  api_service_cls = handlers_endpoints.MachineProviderEndpoints

  def setUp(self):
    super(MachineProviderLeaseTest, self).setUp()
    app = handlers_endpoints.create_endpoints_app()
    self.app = webtest.TestApp(app)

  def test_lease(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        duration=1,
        request_id='abc',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.failIf(lease_response.error)

  def test_duplicate(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.OSX,
        ),
        duration=3,
        request_id='asdf',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response_1 = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    lease_response_2 = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.failIf(lease_response_1.error)
    self.failIf(lease_response_2.error)
    self.assertEqual(
        lease_response_1.request_hash,
        lease_response_2.request_hash,
    )

  def test_request_id_reuse(self):
    lease_request_1 = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.WINDOWS,
        ),
        duration=7,
        request_id='qwerty',
    ))
    lease_request_2 = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.WINDOWS,
        ),
        duration=189,
        request_id='qwerty',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response_1 = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request_1).json,
        rpc_messages.LeaseResponse,
    )
    lease_response_2 = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request_2).json,
        rpc_messages.LeaseResponse,
    )
    self.failIf(lease_response_1.error)
    self.assertEqual(
        lease_response_2.error,
        rpc_messages.LeaseRequestError.REQUEST_ID_REUSE,
    )
    self.assertNotEqual(
        lease_response_1.request_hash,
        lease_response_2.request_hash,
    )

  def test_invalid_topic(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.WINDOWS,
        ),
        duration=9,
        pubsub_topic='../../a-different-project/topics/my-topic',
        request_id='123',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertEqual(
        lease_response.error,
        rpc_messages.LeaseRequestError.INVALID_TOPIC,
    )


if __name__ == '__main__':
  unittest.main()
