# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Messages for the Machine Provider API."""

# pylint: disable=unused-wildcard-import, wildcard-import

from protorpc import messages

from components.machine_provider.dimensions import *
from components.machine_provider.instructions import *
from components.machine_provider.policies import *


class CatalogMachineAdditionRequest(messages.Message):
  """Represents a request to add a machine to the catalog.

  dimensions.backend must be specified.
  dimensions.hostname must be unique per backend.
  """
  # Dimensions instance specifying what sort of machine this is.
  dimensions = messages.MessageField(Dimensions, 1, required=True)
  # Policies instance specifying machine-specific configuration.
  policies = messages.MessageField(Policies, 2, required=True)


class CatalogMachineBatchAdditionRequest(messages.Message):
  """Represents a batched set of CatalogMachineAdditionRequests.

  dimensions.backend must be specified in each CatalogMachineAdditionRequest.
  dimensions.hostname must be unique per backend.
  """
  # CatalogMachineAdditionRequest instances to batch together.
  requests = messages.MessageField(
      CatalogMachineAdditionRequest, 1, repeated=True)


class CatalogMachineDeletionRequest(messages.Message):
  """Represents a request to delete a machine in the catalog."""
  # Dimensions instance specifying what sort of machine this is.
  dimensions = messages.MessageField(Dimensions, 1, required=True)


class CatalogCapacityModificationRequest(messages.Message):
  """Represents a request to modify machine capacity in the catalog."""
  # Dimensions instance specifying what sort of machine this is.
  dimensions = messages.MessageField(Dimensions, 1, required=True)
  # Amount of available capacity matching the specified dimensions.
  count = messages.IntegerField(2, required=True)


class CatalogManipulationRequestError(messages.Enum):
  """Represents an error in a catalog manipulation request."""
  # Per backend, hostnames must be unique in the catalog.
  HOSTNAME_REUSE = 1
  # Tried to lookup an entry that didn't exist.
  ENTRY_NOT_FOUND = 2
  # Didn't specify a backend.
  UNSPECIFIED_BACKEND = 3
  # Specified backend didn't match the backend originating the request.
  MISMATCHED_BACKEND = 4
  # Didn't specify a hostname.
  UNSPECIFIED_HOSTNAME = 5
  # Proposed Cloud Pub/Sub topic was invalid.
  INVALID_TOPIC = 6
  # Proposed Cloud Pub/Sub project was invalid.
  INVALID_PROJECT = 7
  # Didn't specify a Cloud Pub/Sub topic.
  UNSPECIFIED_TOPIC = 8


class CatalogManipulationResponse(messages.Message):
  """Represents a response to a catalog manipulation request."""
  # CatalogManipulationRequestError instance indicating an error with the
  # request, or None if there is no error.
  error = messages.EnumField(CatalogManipulationRequestError, 1)
  # CatalogMachineAdditionRequest this response is in reference to.
  machine_addition_request = messages.MessageField(
      CatalogMachineAdditionRequest, 2)
  # CatalogMachineDeletionRequest this response is in reference to.
  machine_deletion_request = messages.MessageField(
      CatalogMachineDeletionRequest, 3)
  # CatalogCapacityModificationRequest this response is in reference to.
  capacity_modification_request = messages.MessageField(
      CatalogCapacityModificationRequest, 4)


class CatalogBatchManipulationResponse(messages.Message):
  """Represents a response to a batched catalog manipulation request."""
  responses = messages.MessageField(
      CatalogManipulationResponse, 1, repeated=True)


class LeaseRequest(messages.Message):
  """Represents a request for a lease on a machine."""
  # Per-user unique ID used to deduplicate requests.
  request_id = messages.StringField(1, required=True)
  # Dimensions instance specifying what sort of machine to lease.
  dimensions = messages.MessageField(Dimensions, 2, required=True)
  # Desired length of the lease in seconds.
  duration = messages.IntegerField(3, required=True)
  # Cloud Pub/Sub topic name to communicate on regarding this request.
  pubsub_topic = messages.StringField(4)
  # Cloud Pub/Sub project name to communicate on regarding this request.
  pubsub_project = messages.StringField(5)
  # Instructions to give the machine once it's been leased.
  on_lease = messages.MessageField(Instruction, 6)


class BatchedLeaseRequest(messages.Message):
  """Represents a batched set of LeaseRequests."""
  # LeaseRequest instances to batch together.
  requests = messages.MessageField(LeaseRequest, 1, repeated=True)


class LeaseRequestError(messages.Enum):
  """Represents an error in a LeaseRequest."""
  # Request IDs are intended to be unique.
  # Reusing a request ID in a different request is an error.
  REQUEST_ID_REUSE = 1
  # Proposed Cloud Pub/Sub topic was invalid.
  INVALID_TOPIC = 2
  # Proposed Cloud Pub/Sub project was invalid.
  INVALID_PROJECT = 3
  # Didn't specify a Cloud Pub/Sub topic.
  UNSPECIFIED_TOPIC = 4
  # Request couldn't be processed in time.
  DEADLINE_EXCEEDED = 5
  # Miscellaneous transient error.
  TRANSIENT_ERROR = 6


class LeaseRequestState(messages.Enum):
  """Represents the state of a LeaseRequest."""
  # LeaseRequest has been received, but not processed yet.
  UNTRIAGED = 0
  # LeaseRequest is pending provisioning of additional capacity.
  PENDING = 1
  # LeaseRequest has been fulfilled.
  FULFILLED = 2
  # LeaseRequest has been denied.
  DENIED = 3


class LeaseResponse(messages.Message):
  """Represents a response to a LeaseRequest."""
  # SHA-1 identifying the LeaseRequest this response refers to.
  request_hash = messages.StringField(1)
  # LeaseRequestError instance indicating an error with the request, or None
  # if there is no error.
  error = messages.EnumField(LeaseRequestError, 2)
  # Request ID used by the client to generate the LeaseRequest.
  client_request_id = messages.StringField(3, required=True)
  # State of the LeaseRequest.
  state = messages.EnumField(LeaseRequestState, 4)
  # Hostname of the machine available for this request.
  hostname = messages.StringField(5)
  # Timestamp indicating lease expiration seconds from epoch in UTC.
  lease_expiration_ts = messages.IntegerField(6)


class BatchedLeaseResponse(messages.Message):
  """Represents a response to a batched lease request."""
  responses = messages.MessageField(LeaseResponse, 1, repeated=True)
