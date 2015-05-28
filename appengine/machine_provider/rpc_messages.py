# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Messages for the Machine Provider API."""

from protorpc import messages


class OSFamily(messages.Enum):
  """Lists valid OS families."""
  LINUX = 1
  OSX = 2
  WINDOWS = 3


class Dimensions(messages.Message):
  """Represents the dimensions of a machine.

  Args:
    os_family: The operating system family of this machine.
  """
  os_family = messages.EnumField(OSFamily, 1)


class LeaseRequest(messages.Message):
  """Represents a request for a lease on a machine."""
  # Per-user unique ID used to deduplicate requests.
  request_id = messages.StringField(1, required=True)
  # Dimensions instance specifying what sort of machine to lease.
  dimensions = messages.MessageField(Dimensions, 2, required=True)
  # Desired length of the lease in seconds.
  duration = messages.IntegerField(3, required=True)
  # Desired start time of the lease in seconds from now. Default to 0, or ASAP.
  start_time = messages.IntegerField(4, default=0)
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
