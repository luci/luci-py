# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import time

from components.prpc import codes


class ServicerContext(object):
  """A context object passed to method implementations."""

  def __init__(self):
    self._start_time = time.time()
    self.timeout = None
    self.active = True
    self.code = None
    self.details = None
    self.invocation_metadata = []  # list of (k, v) pairs, the key is lowercase

  def time_remaining(self):
    """Describes the length of allowed time remaining for the RPC.

    Returns:
      A nonnegative float indicating the length of allowed time in seconds
      remaining for the RPC to complete before it is considered to have timed
      out, or None if no deadline was specified for the RPC.
    """
    if self._timeout is None:
      return None
    now = time.time()
    return max(0, self._start_time + self._timeout - now)

  def cancel(self):
    """Cancels the RPC.

    Idempotent and has no effect if the RPC has already terminated.
    """
    self.active = False

  def set_code(self, code):
    """Accepts the status code of the RPC.

    This method need not be called by method implementations if they wish the
    gRPC runtime to determine the status code of the RPC.

    Args:
      code: One of StatusCode.* tuples that contains a status code of the RPC to
        be transmitted to the invocation side of the RPC.
    """
    assert code in codes.ALL_CODES, '%r is not StatusCode.*' % (code,)
    self.code = code

  def set_details(self, details):
    """Accepts the service-side details of the RPC.

    This method need not be called by method implementations if they have no
    details to transmit.

    Args:
      details: The details string of the RPC to be transmitted to
        the invocation side of the RPC.
    """
    assert isinstance(details, basestring), '%r is not string' % (details,)
    self.details = details
