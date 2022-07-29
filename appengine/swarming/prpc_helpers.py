# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""This module implements base functionality of Swarming prpc services."""

import cgi
import functools
import logging
import six
import sys

from google.appengine.api import datastore_errors

from components import auth
from components.prpc import codes

import handlers_exceptions

EXCEPTIONS_TO_CODE = {
    handlers_exceptions.BadRequestException: codes.StatusCode.INVALID_ARGUMENT,
    datastore_errors.BadValueError: codes.StatusCode.INVALID_ARGUMENT,
    handlers_exceptions.PermissionException: codes.StatusCode.PERMISSION_DENIED,
    handlers_exceptions.InternalException: codes.StatusCode.INTERNAL,
}


def PRPCMethod(func):

  @functools.wraps(func)
  def wrapper(self, request, prpc_context):
    return self.Run(func, request, prpc_context)

  wrapper.wrapped = func
  return wrapper


class SwarmingPRPCService(object):
  """Abstract base class for prpc API services."""

  def Run(self, handler, request, prpc_context):
    try:
      response = handler(self, request, prpc_context)
      return response
    except Exception as e:
      ProcessException(e, prpc_context)


def ProcessException(e, prpc_context):
  # type: (Exception, context.ServicerContext) -> None
  """Sets prpc codes for recognized exceptions, raises unrecognized ones."""
  logging.exception(e)
  exc_type = type(e)
  code = EXCEPTIONS_TO_CODE.get(exc_type)
  if code is None:
    prpc_context.set_code(codes.StatusCode.INTERNAL)
    prpc_context.set_details('Potential programming error.')
    six.reraise(e.__class__, e, sys.exc_info()[2])

  prpc_context.set_code(code)
  prpc_context.set_details(cgi.escape(e.message, quote=True))
