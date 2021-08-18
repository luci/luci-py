# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Exceptions raised by methods called by prpc/endpoints handlers."""


class BadRequestException(Exception):
  """The request is invalid."""
  pass


class PermissionException(Exception):
  """Permission requirements are not fulfilled."""
  pass


class InternalException(Exception):
  """Unexpected error occurred."""
  pass
