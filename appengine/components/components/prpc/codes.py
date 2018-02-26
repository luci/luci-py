# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Definition of possible RPC response status codes."""


class StatusCode(object):
  """Mirrors grpc.StatusCode in the gRPC Core.

  See https://grpc.io/grpc/python/grpc.html?highlight=status#grpc.StatusCode
  """
  OK                  = (0, 'ok')
  CANCELLED           = (1, 'cancelled')
  UNKNOWN             = (2, 'unknown')
  INVALID_ARGUMENT    = (3, 'invalid argument')
  DEADLINE_EXCEEDED   = (4, 'deadline exceeded')
  NOT_FOUND           = (5, 'not found')
  ALREADY_EXISTS      = (6, 'already exists')
  PERMISSION_DENIED   = (7, 'permission denied')
  RESOURCE_EXHAUSTED  = (8, 'resource exhausted')
  FAILED_PRECONDITION = (9, 'failed precondition')
  ABORTED             = (10, 'aborted')
  OUT_OF_RANGE        = (11, 'out of range')
  UNIMPLEMENTED       = (12, 'unimplemented')
  INTERNAL            = (13, 'internal error')
  UNAVAILABLE         = (14, 'unavailable')
  DATA_LOSS           = (15, 'data loss')
  UNAUTHENTICATED     = (16, 'unauthenticated')


# Used in ServicerContext.set_code to assert that the code is known.
ALL_CODES = frozenset(
    getattr(StatusCode, k) for k in dir(StatusCode) if not k.startswith('_'))
