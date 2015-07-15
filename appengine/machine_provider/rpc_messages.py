# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Messages for the Machine Provider API."""

from protorpc import messages


class Backend(messages.Enum):
  """Lists valid backends."""
  DUMMY = 0
  GCE = 1


class OSFamily(messages.Enum):
  """Lists valid OS families."""
  LINUX = 1
  OSX = 2
  WINDOWS = 3


class Dimensions(messages.Message):
  """Represents the dimensions of a machine."""
  # The operating system family of this machine.
  os_family = messages.EnumField(OSFamily, 1)
  # The backend which should be used to spin up this machine. This should
  # generally be left unspecified so the Machine Provider selects the backend
  # on its own.
  backend = messages.EnumField(Backend, 2)
  # The hostname of this machine.
  hostname = messages.StringField(3)


class CatalogMachineAdditionRequest(messages.Message):
  """Represents a request to add a machine to the catalog.

  dimensions.backend must be specified.
  dimensions.hostname must be unique per backend.
  """
  # Dimensions instance specifying what sort of machine this is.
  dimensions = messages.MessageField(Dimensions, 1, required=True)


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


class CatalogManipulationResponse(messages.Message):
  """Represents a response to a catalog manipulation response."""
  # CatalogManipulationRequestError instance indicating an error with the
  # request, or None if there is no error.
  error = messages.EnumField(CatalogManipulationRequestError, 1)


class LeaseRequest(messages.Message):
  """Represents a request for a lease on a machine."""
  # Per-user unique ID used to deduplicate requests.
  request_id = messages.StringField(1, required=True)
  # Dimensions instance specifying what sort of machine to lease.
  dimensions = messages.MessageField(Dimensions, 2, required=True)
  # Desired length of the lease in seconds.
  duration = messages.IntegerField(3, required=True)
  # URL to post a LeaseResponse message to when a response is ready. If
  # unspecified, the caller is responsible for polling for the result.
  callback = messages.StringField(5)


class LeaseRequestError(messages.Enum):
  """Represents an error in a LeaseRequest."""
  # Request IDs are intended to be unique.
  # Reusing a request ID in a different request is an error.
  REQUEST_ID_REUSE = 1


class LeaseResponse(messages.Message):
  """Represents a response to a LeaseRequest."""
  # SHA-1 identifying the LeaseRequest this response refers to.
  request_hash = messages.StringField(1)
  # LeaseRequestError instance indicating an error with the request, or None
  # if there is no error.
  error = messages.EnumField(LeaseRequestError, 2)
