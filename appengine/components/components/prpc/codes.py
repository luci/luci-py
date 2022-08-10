# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Definition of possible RPC response status codes."""

import collections
import httplib

StatusCodeBase = collections.namedtuple('StatusCodeBase', ['value', 'name'])

class StatusCode(StatusCodeBase):
  """Mirrors grpc.StatusCode in the gRPC Core.

  See https://grpc.io/grpc/python/grpc.html?highlight=status#grpc.StatusCode
  """
  OK                  = StatusCodeBase(0, 'ok')
  CANCELLED           = StatusCodeBase(1, 'cancelled')
  UNKNOWN             = StatusCodeBase(2, 'unknown')
  INVALID_ARGUMENT    = StatusCodeBase(3, 'invalid argument')
  DEADLINE_EXCEEDED   = StatusCodeBase(4, 'deadline exceeded')
  NOT_FOUND           = StatusCodeBase(5, 'not found')
  ALREADY_EXISTS      = StatusCodeBase(6, 'already exists')
  PERMISSION_DENIED   = StatusCodeBase(7, 'permission denied')
  RESOURCE_EXHAUSTED  = StatusCodeBase(8, 'resource exhausted')
  FAILED_PRECONDITION = StatusCodeBase(9, 'failed precondition')
  ABORTED             = StatusCodeBase(10, 'aborted')
  OUT_OF_RANGE        = StatusCodeBase(11, 'out of range')
  UNIMPLEMENTED       = StatusCodeBase(12, 'unimplemented')
  INTERNAL            = StatusCodeBase(13, 'internal error')
  UNAVAILABLE         = StatusCodeBase(14, 'unavailable')
  DATA_LOSS           = StatusCodeBase(15, 'data loss')
  UNAUTHENTICATED     = StatusCodeBase(16, 'unauthenticated')

  @staticmethod
  def to_http_code(status_code):
    httpCode = _PRPC_TO_HTTP_STATUS.get(status_code, None)
    if httpCode == None:
      raise ValueError('%s is not a valid StatusCode' % status_code)
    return httpCode

# Used in ServicerContext.set_code to assert that the code is known.
ALL_CODES = frozenset(
    getattr(StatusCode, k)
    for k in dir(StatusCode)
    if isinstance(getattr(StatusCode, k), StatusCodeBase))

INT_TO_CODE = {c[0]: c for c in ALL_CODES}

_PRPC_TO_HTTP_STATUS = {
    StatusCode.OK: httplib.OK,
    StatusCode.CANCELLED: httplib.NO_CONTENT,
    StatusCode.UNKNOWN: httplib.INTERNAL_SERVER_ERROR,
    StatusCode.INVALID_ARGUMENT: httplib.BAD_REQUEST,
    StatusCode.DEADLINE_EXCEEDED: httplib.SERVICE_UNAVAILABLE,
    StatusCode.NOT_FOUND: httplib.NOT_FOUND,
    StatusCode.ALREADY_EXISTS: httplib.CONFLICT,
    StatusCode.PERMISSION_DENIED: httplib.FORBIDDEN,
    StatusCode.RESOURCE_EXHAUSTED: httplib.SERVICE_UNAVAILABLE,
    StatusCode.FAILED_PRECONDITION: httplib.PRECONDITION_FAILED,
    StatusCode.ABORTED: httplib.CONFLICT,
    StatusCode.OUT_OF_RANGE: httplib.BAD_REQUEST,
    StatusCode.UNIMPLEMENTED: httplib.NOT_IMPLEMENTED,
    StatusCode.INTERNAL: httplib.INTERNAL_SERVER_ERROR,
    StatusCode.UNAVAILABLE: httplib.SERVICE_UNAVAILABLE,
    StatusCode.DATA_LOSS: httplib.GONE,
    StatusCode.UNAUTHENTICATED: httplib.UNAUTHORIZED,
}
