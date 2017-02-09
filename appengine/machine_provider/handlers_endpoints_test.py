#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Unit tests for handlers_endpoints.py."""

import datetime
import json
import unittest

import test_env
test_env.setup_test_env()

from google.appengine import runtime

from protorpc.remote import protojson
import webtest

from components import auth_testing
from components import utils
from components.machine_provider import rpc_messages
from test_support import test_case

import acl
import handlers_endpoints
import models


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

  def mock_get_current_backend(self, backend=rpc_messages.Backend.DUMMY):
    self.mock(acl, 'get_current_backend', lambda *args, **kwargs: backend)

  def test_get(self):
    models.CatalogMachineEntry(
        key=models.CatalogMachineEntry._generate_key('DUMMY', 'fake-host'),
        dimensions=rpc_messages.Dimensions(hostname='fake-host'),
        lease_expiration_ts=utils.utcnow(),
    ).put()
    request = rpc_to_json(rpc_messages.CatalogMachineRetrievalRequest(
        hostname='fake-host',
    ))

    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('get', request).json,
        rpc_messages.CatalogMachineRetrievalResponse,
    )
    self.assertEqual(response.dimensions.hostname, 'fake-host')
    self.assertTrue(response.lease_expiration_ts)

  def test_get_mismatched_backend(self):
    models.CatalogMachineEntry(
        key=models.CatalogMachineEntry._generate_key('DUMMY', 'fake-host'),
        dimensions=rpc_messages.Dimensions(hostname='fake-host'),
    ).put()
    request = rpc_to_json(rpc_messages.CatalogMachineRetrievalRequest(
        backend=rpc_messages.Backend.GCE,
        hostname='fake-host',
    ))

    self.mock_get_current_backend()

    jsonish_dict_to_rpc(
        self.call_api('get', request, status=403).json,
        rpc_messages.CatalogMachineRetrievalResponse,
    )

  def test_get_backend_unspecified_by_admin(self):
    self.mock(acl, 'is_catalog_admin', lambda *args, **kwargs: True)

    models.CatalogMachineEntry(
        key=models.CatalogMachineEntry._generate_key('DUMMY', 'fake-host'),
        dimensions=rpc_messages.Dimensions(hostname='fake-host'),
    ).put()
    request = rpc_to_json(rpc_messages.CatalogMachineRetrievalRequest(
        hostname='fake-host',
    ))

    jsonish_dict_to_rpc(
        self.call_api('get', request, status=400).json,
        rpc_messages.CatalogMachineRetrievalResponse,
    )

  def test_get_not_found(self):
    request = rpc_to_json(rpc_messages.CatalogMachineRetrievalRequest(
        hostname='fake-host',
    ))

    self.mock_get_current_backend()

    jsonish_dict_to_rpc(
        self.call_api('get', request, status=404).json,
        rpc_messages.CatalogMachineRetrievalResponse,
    )

  def test_add(self):
    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_topic='fake-topic',
        ),
    ))
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('add_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertFalse(response.error)

  def test_mismatched_backend(self):
    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            backend=rpc_messages.Backend.GCE,
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_topic='fake-topic',
        ),
    ))
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('add_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertEqual(
        response.error,
        rpc_messages.CatalogManipulationRequestError.MISMATCHED_BACKEND,
    )

  def test_add_backend_unspecified_by_admin(self):
    self.mock(acl, 'is_catalog_admin', lambda *args, **kwargs: True)

    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_topic='fake-topic',
        ),
    ))

    response = jsonish_dict_to_rpc(
        self.call_api('add_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertEqual(
        response.error,
        rpc_messages.CatalogManipulationRequestError.UNSPECIFIED_BACKEND,
    )

  def test_add_project_no_topic(self):
    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
        ),
    ))
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('add_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertEqual(
        response.error,
        rpc_messages.CatalogManipulationRequestError.UNSPECIFIED_TOPIC,
    )

  def test_add_no_hostname(self):
    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='fake-topic',
        ),
    ))
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('add_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertEqual(
      response.error,
      rpc_messages.CatalogManipulationRequestError.UNSPECIFIED_HOSTNAME,
    )

  def test_add_invalid_topic(self):
    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='../../a-different-project/topics/my-topic',
        ),
    ))
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('add_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertEqual(
      response.error,
      rpc_messages.CatalogManipulationRequestError.INVALID_TOPIC,
    )

  def test_add_invalid_project(self):
    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_topic='my-topic',
            backend_project='my-project/topics/my-other-topic',
        ),
    ))
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('add_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertEqual(
      response.error,
      rpc_messages.CatalogManipulationRequestError.INVALID_PROJECT,
    )

  def test_add_duplicate(self):
    request_1 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='fake-topic',
        ),
    ))
    request_2 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='fake-topic',
        ),
    ))
    self.mock_get_current_backend()

    response_1 = jsonish_dict_to_rpc(
        self.call_api('add_machine', request_1).json,
        rpc_messages.CatalogManipulationResponse,
    )
    response_2 = jsonish_dict_to_rpc(
        self.call_api('add_machine', request_2).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertFalse(response_1.error)
    self.assertEqual(
        response_2.error,
        rpc_messages.CatalogManipulationRequestError.HOSTNAME_REUSE,
    )

  def test_add_batch_empty(self):
    request = rpc_to_json(rpc_messages.CatalogMachineBatchAdditionRequest())
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('add_machines', request).json,
        rpc_messages.CatalogBatchManipulationResponse,
    )
    self.assertFalse(response.responses)

  def test_add_batch(self):
    request = rpc_to_json(rpc_messages.CatalogMachineBatchAdditionRequest(
        requests=[
            rpc_messages.CatalogMachineAdditionRequest(
                dimensions=rpc_messages.Dimensions(
                    hostname='fake-host-1',
                    os_family=rpc_messages.OSFamily.LINUX,
                ),
                policies=rpc_messages.Policies(
                    backend_project='fake-project',
                    backend_topic='fake-topic',
                ),
            ),
            rpc_messages.CatalogMachineAdditionRequest(
                dimensions=rpc_messages.Dimensions(
                    hostname='fake-host-2',
                    os_family=rpc_messages.OSFamily.WINDOWS,
                ),
                policies=rpc_messages.Policies(
                    backend_project='fake-project',
                    backend_topic='fake-topic',
                ),
            ),
            rpc_messages.CatalogMachineAdditionRequest(
                dimensions=rpc_messages.Dimensions(
                    hostname='fake-host-1',
                    os_family=rpc_messages.OSFamily.OSX,
                ),
                policies=rpc_messages.Policies(
                    backend_project='fake-project',
                    backend_topic='fake-topic',
                ),
            ),
        ],
    ))
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('add_machines', request).json,
        rpc_messages.CatalogBatchManipulationResponse,
    )
    self.assertEqual(len(response.responses), 3)
    self.assertFalse(response.responses[0].error)
    self.assertFalse(response.responses[1].error)
    self.assertEqual(
        response.responses[2].error,
        rpc_messages.CatalogManipulationRequestError.HOSTNAME_REUSE,
    )

  def test_add_batch_error(self):
    request = rpc_to_json(rpc_messages.CatalogMachineBatchAdditionRequest(
        requests=[
            rpc_messages.CatalogMachineAdditionRequest(
                dimensions=rpc_messages.Dimensions(
                    backend=rpc_messages.Backend.GCE,
                    hostname='fake-host-1',
                    os_family=rpc_messages.OSFamily.LINUX,
                ),
                policies=rpc_messages.Policies(
                    backend_project='fake-project',
                    backend_topic='fake-topic',
                ),
            ),
        ],
    ))
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('add_machines', request).json,
        rpc_messages.CatalogBatchManipulationResponse,
    )
    self.assertEqual(len(response.responses), 1)
    self.assertEqual(
        response.responses[0].error,
        rpc_messages.CatalogManipulationRequestError.MISMATCHED_BACKEND,
    )

  def test_delete(self):
    request_1 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='fake-topic',
        ),
    ))
    request_2 = rpc_to_json(rpc_messages.CatalogMachineDeletionRequest(
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
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='fake-topic',
        ),
    ))
    self.mock_get_current_backend()

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
    self.assertFalse(response_1.error)
    self.assertFalse(response_2.error)
    self.assertFalse(response_3.error)

  def test_delete_error(self):
    request = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            backend=rpc_messages.Backend.GCE,
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='fake-topic',
        ),
    ))
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('delete_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertEqual(
        response.error,
        rpc_messages.CatalogManipulationRequestError.MISMATCHED_BACKEND,
    )

  def test_delete_leased(self):
    request = rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            backend=rpc_messages.Backend.DUMMY,
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='fake-topic',
        ),
    )
    key = models.CatalogMachineEntry(
        key=models.CatalogMachineEntry.generate_key(request.dimensions),
        dimensions=request.dimensions,
        lease_id='lease-id',
    ).put()
    request = rpc_to_json(request)
    self.mock_get_current_backend()

    response = jsonish_dict_to_rpc(
        self.call_api('delete_machine', request).json,
        rpc_messages.CatalogManipulationResponse,
    )
    self.assertEqual(
        response.error,
        rpc_messages.CatalogManipulationRequestError.LEASED,
    )
    self.assertTrue(key.get())

  def test_delete_invalid(self):
    request_1 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host-1',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='fake-topic',
        ),
    ))
    request_2 = rpc_to_json(rpc_messages.CatalogMachineDeletionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host-2',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
    ))
    request_3 = rpc_to_json(rpc_messages.CatalogMachineAdditionRequest(
        dimensions=rpc_messages.Dimensions(
            hostname='fake-host-1',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        policies=rpc_messages.Policies(
            backend_project='fake-project',
            backend_topic='fake-topic',
        ),
    ))
    self.mock_get_current_backend()

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
    self.assertFalse(response_1.error)
    self.assertEqual(
        response_2.error,
        rpc_messages.CatalogManipulationRequestError.ENTRY_NOT_FOUND,
    )
    self.assertEqual(
        response_3.error,
        rpc_messages.CatalogManipulationRequestError.HOSTNAME_REUSE,
    )


class MachineProviderReleaseTest(test_case.EndpointsTestCase):
  """Tests for handlers_endpoints.MachineProviderEndpoints.release."""
  api_service_cls = handlers_endpoints.MachineProviderEndpoints

  def setUp(self):
    super(MachineProviderReleaseTest, self).setUp()
    app = handlers_endpoints.create_endpoints_app()
    self.app = webtest.TestApp(app)

  def test_release(self):
    self.mock(
        handlers_endpoints.MachineProviderEndpoints,
        '_release',
        lambda *args, **kwargs: None,
    )

    request = rpc_to_json(rpc_messages.LeaseReleaseRequest(
        request_id='request-id',
    ))
    auth_testing.mock_get_current_identity(self)

    response = jsonish_dict_to_rpc(
        self.call_api('release', request).json,
        rpc_messages.LeaseReleaseResponse,
    )
    self.assertEqual(response.client_request_id, 'request-id')
    self.assertFalse(response.error)


class MachineProviderBatchedReleaseTest(test_case.EndpointsTestCase):
  """Tests for handlers_endpoints.MachineProviderEndpoints.batched_release."""
  api_service_cls = handlers_endpoints.MachineProviderEndpoints

  def setUp(self):
    super(MachineProviderBatchedReleaseTest, self).setUp()
    app = handlers_endpoints.create_endpoints_app()
    self.app = webtest.TestApp(app)

  def test_batch(self):
    ts = utils.utcnow()
    self.mock(utils, 'utcnow', lambda *args, **kwargs: ts)

    release_requests = rpc_to_json(rpc_messages.BatchedLeaseReleaseRequest(
        requests=[
            rpc_messages.LeaseReleaseRequest(
                request_id='request-id',
            ),
        ],
    ))
    auth_testing.mock_get_current_identity(self)

    release_responses = jsonish_dict_to_rpc(
        self.call_api('batched_release', release_requests).json,
        rpc_messages.BatchedLeaseReleaseResponse,
    )
    self.assertEqual(len(release_responses.responses), 1)
    self.assertEqual(
        release_responses.responses[0].client_request_id, 'request-id')
    self.assertEqual(
        release_responses.responses[0].error,
        rpc_messages.LeaseReleaseRequestError.NOT_FOUND,
    )

  def test_deadline_exceeded(self):
    class utcnow(object):
      def __init__(self, init_ts):
        self.last_ts = init_ts
      def __call__(self, *args, **kwargs):
        self.last_ts = self.last_ts + datetime.timedelta(seconds=60)
        return self.last_ts
    self.mock(utils, 'utcnow', utcnow(utils.utcnow()))

    release_requests = rpc_to_json(rpc_messages.BatchedLeaseReleaseRequest(
        requests=[
            rpc_messages.LeaseReleaseRequest(
                request_id='request-id',
            ),
        ],
    ))
    auth_testing.mock_get_current_identity(self)

    release_responses = jsonish_dict_to_rpc(
        self.call_api('batched_release', release_requests).json,
        rpc_messages.BatchedLeaseReleaseResponse,
    )
    self.assertEqual(len(release_responses.responses), 1)
    self.assertEqual(
        release_responses.responses[0].client_request_id, 'request-id')
    self.assertEqual(
        release_responses.responses[0].error,
        rpc_messages.LeaseReleaseRequestError.DEADLINE_EXCEEDED,
    )

  def test_exception(self):
    ts = utils.utcnow()
    self.mock(utils, 'utcnow', lambda *args, **kwargs: ts)

    def _release(*args, **kwargs):
      raise runtime.apiproxy_errors.CancelledError
    self.mock(handlers_endpoints.MachineProviderEndpoints, '_release', _release)

    release_requests = rpc_to_json(rpc_messages.BatchedLeaseReleaseRequest(
        requests=[
            rpc_messages.LeaseReleaseRequest(
                request_id='request-id',
            ),
        ],
    ))
    auth_testing.mock_get_current_identity(self)

    release_responses = jsonish_dict_to_rpc(
        self.call_api('batched_release', release_requests).json,
        rpc_messages.BatchedLeaseReleaseResponse,
    )
    self.assertEqual(len(release_responses.responses), 1)
    self.assertEqual(
        release_responses.responses[0].client_request_id, 'request-id')
    self.assertEqual(
        release_responses.responses[0].error,
        rpc_messages.LeaseReleaseRequestError.TRANSIENT_ERROR,
    )


class MachineProviderBatchedLeaseTest(test_case.EndpointsTestCase):
  """Tests for handlers_endpoints.MachineProviderEndpoints.batched_lease."""
  api_service_cls = handlers_endpoints.MachineProviderEndpoints

  def setUp(self):
    super(MachineProviderBatchedLeaseTest, self).setUp()
    app = handlers_endpoints.create_endpoints_app()
    self.app = webtest.TestApp(app)

  def test_batch(self):
    ts = utils.utcnow()
    self.mock(utils, 'utcnow', lambda *args, **kwargs: ts)

    lease_requests = rpc_to_json(rpc_messages.BatchedLeaseRequest(requests=[
        rpc_messages.LeaseRequest(
            dimensions=rpc_messages.Dimensions(
                os_family=rpc_messages.OSFamily.LINUX,
            ),
            duration=1,
            request_id='request-id',
        ),
    ]))
    auth_testing.mock_get_current_identity(self)

    lease_responses = jsonish_dict_to_rpc(
        self.call_api('batched_lease', lease_requests).json,
        rpc_messages.BatchedLeaseResponse,
    )
    self.assertEqual(len(lease_responses.responses), 1)
    self.assertEqual(
        lease_responses.responses[0].client_request_id, 'request-id')
    self.assertFalse(lease_responses.responses[0].error)

  def test_deadline_exceeded(self):
    class utcnow(object):
      def __init__(self, init_ts):
        self.last_ts = init_ts
      def __call__(self, *args, **kwargs):
        self.last_ts = self.last_ts + datetime.timedelta(seconds=60)
        return self.last_ts
    self.mock(utils, 'utcnow', utcnow(utils.utcnow()))

    lease_requests = rpc_to_json(rpc_messages.BatchedLeaseRequest(requests=[
        rpc_messages.LeaseRequest(
            dimensions=rpc_messages.Dimensions(
                os_family=rpc_messages.OSFamily.LINUX,
            ),
            duration=1,
            request_id='request-id',
        ),
    ]))
    auth_testing.mock_get_current_identity(self)

    lease_responses = jsonish_dict_to_rpc(
        self.call_api('batched_lease', lease_requests).json,
        rpc_messages.BatchedLeaseResponse,
    )
    self.assertEqual(len(lease_responses.responses), 1)
    self.assertEqual(
        lease_responses.responses[0].client_request_id, 'request-id')
    self.assertEqual(
        lease_responses.responses[0].error,
        rpc_messages.LeaseRequestError.DEADLINE_EXCEEDED,
    )

  def test_exception(self):
    ts = utils.utcnow()
    self.mock(utils, 'utcnow', lambda *args, **kwargs: ts)

    def _lease(*args, **kwargs):
      raise runtime.apiproxy_errors.CancelledError
    self.mock(handlers_endpoints.MachineProviderEndpoints, '_lease', _lease)

    lease_requests = rpc_to_json(rpc_messages.BatchedLeaseRequest(requests=[
        rpc_messages.LeaseRequest(
            dimensions=rpc_messages.Dimensions(
                os_family=rpc_messages.OSFamily.LINUX,
            ),
            duration=1,
            request_id='request-id',
        ),
    ]))
    auth_testing.mock_get_current_identity(self)

    lease_responses = jsonish_dict_to_rpc(
        self.call_api('batched_lease', lease_requests).json,
        rpc_messages.BatchedLeaseResponse,
    )
    self.assertEqual(len(lease_responses.responses), 1)
    self.assertEqual(
        lease_responses.responses[0].client_request_id, 'request-id')
    self.assertEqual(
        lease_responses.responses[0].error,
        rpc_messages.LeaseRequestError.TRANSIENT_ERROR,
    )


class MachineProviderLeaseTest(test_case.EndpointsTestCase):
  """Tests for handlers_endpoints.MachineProviderEndpoints.lease."""
  api_service_cls = handlers_endpoints.MachineProviderEndpoints

  def setUp(self):
    super(MachineProviderLeaseTest, self).setUp()
    app = handlers_endpoints.create_endpoints_app()
    self.app = webtest.TestApp(app)

  def test_lease_duration(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        duration=1,
        request_id='abc',
        pubsub_topic='topic',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertFalse(lease_response.error)

  def test_lease_duration_zero(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        duration=0,
        request_id='abc',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertEqual(
        lease_response.error,
        rpc_messages.LeaseRequestError.LEASE_LENGTH_UNSPECIFIED,
    )

  def test_lease_duration_negative(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        duration=-1,
        request_id='abc',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertEqual(
        lease_response.error,
        rpc_messages.LeaseRequestError.NONPOSITIVE_DEADLINE,
    )

  def test_lease_duration_negative(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        duration=-1,
        request_id='abc',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertEqual(
        lease_response.error,
        rpc_messages.LeaseRequestError.NONPOSITIVE_DEADLINE,
    )

  def test_lease_duration_and_lease_expiration_ts(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        duration=1,
        lease_expiration_ts=9999999999,
        request_id='abc',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertEqual(
        lease_response.error,
        rpc_messages.LeaseRequestError.MUTUAL_EXCLUSION_ERROR,
    )

  def test_lease_timestamp(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        lease_expiration_ts=9999999999,
        request_id='abc',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertFalse(lease_response.error)

  def test_lease_timestamp_passed(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        lease_expiration_ts=1,
        request_id='abc',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertEqual(
        lease_response.error,
        rpc_messages.LeaseRequestError.LEASE_EXPIRATION_TS_ERROR,
    )

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
    self.assertFalse(lease_response_1.error)
    self.assertFalse(lease_response_2.error)
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
    self.assertFalse(lease_response_1.error)
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

  def test_invalid_project(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.WINDOWS,
        ),
        duration=9,
        pubsub_topic='my-topic',
        pubsub_project='../../a-different-project/topics/my-other-topic',
        request_id='123',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertEqual(
        lease_response.error,
        rpc_messages.LeaseRequestError.INVALID_PROJECT,
    )

  def test_project_without_topic(self):
    lease_request = rpc_to_json(rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.WINDOWS,
        ),
        duration=9,
        pubsub_project='my-project',
        request_id='123',
    ))
    auth_testing.mock_get_current_identity(self)

    lease_response = jsonish_dict_to_rpc(
        self.call_api('lease', lease_request).json,
        rpc_messages.LeaseResponse,
    )
    self.assertEqual(
        lease_response.error,
        rpc_messages.LeaseRequestError.UNSPECIFIED_TOPIC,
    )


if __name__ == '__main__':
  unittest.main()
