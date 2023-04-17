# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A library containing exception types used by Endpoints."""

from __future__ import absolute_import

from six.moves import http_client

from . import remote


class ServiceException(remote.ApplicationError):
  """Base class for request/service exceptions in Endpoints."""

  def __init__(self, message=None):
    super(ServiceException, self).__init__(message,
                                           http_client.responses[self.http_status])


class BadRequestException(ServiceException):
  """Bad request exception that is mapped to a 400 response."""
  http_status = http_client.BAD_REQUEST


class UnauthorizedException(ServiceException):
  """Unauthorized exception that is mapped to a 401 response."""
  http_status = http_client.UNAUTHORIZED


class ForbiddenException(ServiceException):
  """Forbidden exception that is mapped to a 403 response."""
  http_status = http_client.FORBIDDEN


class NotFoundException(ServiceException):
  """Not found exception that is mapped to a 404 response."""
  http_status = http_client.NOT_FOUND


class ConflictException(ServiceException):
  """Conflict exception that is mapped to a 409 response."""
  http_status = http_client.CONFLICT


class GoneException(ServiceException):
  """Resource Gone exception that is mapped to a 410 response."""
  http_status = http_client.GONE


class PreconditionFailedException(ServiceException):
  """Precondition Failed exception that is mapped to a 412 response."""
  http_status = http_client.PRECONDITION_FAILED


class RequestEntityTooLargeException(ServiceException):
  """Request entity too large exception that is mapped to a 413 response."""
  http_status = http_client.REQUEST_ENTITY_TOO_LARGE


class InternalServerErrorException(ServiceException):
  """Internal server exception that is mapped to a 500 response."""
  http_status = http_client.INTERNAL_SERVER_ERROR


class ApiConfigurationError(Exception):
  """Exception thrown if there's an error in the configuration/annotations."""


class InvalidNamespaceException(Exception):
  """Exception thrown if there's an invalid namespace declaration."""


class InvalidLimitDefinitionException(Exception):
  """Exception thrown if there's an invalid rate limit definition."""


class InvalidApiNameException(Exception):
  """Exception thrown if the api name does not match the required character set."""


class ToolError(Exception):
  """Exception thrown if there's a general error in the endpointscfg.py tool."""
