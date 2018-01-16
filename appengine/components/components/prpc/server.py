# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""A stripped-down implementation of the gRPC interface for Python on AppEngine.

The Server class itself also provides the bulk of the implementation which
actually runs on AppEngine (and therefore couldn't include Cython). It acts
as a webapp2.RequestHandler, and exposes a .get_routes() method for the host
application to call.

https://github.com/grpc/grpc/tree/master/src/python/grpcio/grpc
"""

import collections
import httplib
import logging
import webapp2

from google.protobuf import symbol_database

# Helpers are in separate modules so this one exposes only the public interface.
from components.prpc import encoding, headers
from components.prpc.context import ServicerContext

__all__ = ['Server', 'StatusCode']

# pylint: disable=pointless-string-statement

class StatusCode(object):
  """Mirrors grpc_status_code in the gRPC Core."""
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


_PRPC_TO_HTTP_STATUS = {
  StatusCode.OK: httplib.OK,
  StatusCode.CANCELLED: httplib.NO_CONTENT,
  StatusCode.INVALID_ARGUMENT: httplib.BAD_REQUEST,
  StatusCode.DEADLINE_EXCEEDED: httplib.SERVICE_UNAVAILABLE,
  StatusCode.NOT_FOUND: httplib.NOT_FOUND,
  StatusCode.ALREADY_EXISTS: httplib.CONFLICT,
  StatusCode.PERMISSION_DENIED: httplib.FORBIDDEN,
  StatusCode.RESOURCE_EXHAUSTED: httplib.SERVICE_UNAVAILABLE,
  StatusCode.FAILED_PRECONDITION: httplib.PRECONDITION_FAILED,
  StatusCode.OUT_OF_RANGE: httplib.BAD_REQUEST,
  StatusCode.UNIMPLEMENTED: httplib.NOT_IMPLEMENTED,
  StatusCode.INTERNAL: httplib.INTERNAL_SERVER_ERROR,
  StatusCode.UNAVAILABLE: httplib.SERVICE_UNAVAILABLE,
  StatusCode.UNAUTHENTICATED: httplib.UNAUTHORIZED,
}


_Service = collections.namedtuple('_Service', ['description', 'methods'])


class Server(object):
  """Server represents a pRPC server that handles a set of services.

  Server is intended to mimic gRPC's Server as an abstraction, but provides
  a simpler interface via add_service and get_routes.
  """

  def __init__(self):
    self._services = {}

  def add_service(self, servicer):
    """Registers a servicer for a service with the server.

    Args:
      servicer: A servicer which represents a service. It must have a
        DESCRIPTION of the service and implements handlers for each service
        method.

    Raises:
      ValueError: when trying to add another handler for the same service name.
    """
    sym_db = symbol_database.Default()
    pkg = servicer.DESCRIPTION['package']
    desc = servicer.DESCRIPTION['descriptor']

    # Construct handler.
    methods = {
      method.name: (
        # Fully-qualified proto type names will always begin with a '.' which
        # GetSymbol doesn't strip out.
        sym_db.GetSymbol(method.input_type[1:]),
        sym_db.GetSymbol(method.output_type[1:]),
        getattr(servicer, method.name),
      )
      for method in desc.method if hasattr(servicer, method.name)
    }

    full_name = desc.name
    if pkg:
      full_name = '%s.%s' % (pkg, desc.name)

    # Register handler with internal server state.
    if desc.name in self._services:
      raise ValueError(
          'Tried to double-register handlers for service %s' % desc.name)
    self._services[full_name] = _Service(desc, methods)

  def get_routes(self):
    """Returns a list of webapp2.Route for all the routes the API handles."""
    return [webapp2.Route('/prpc/<service>/<method>',
                          handler=self._handler(),
                          methods=['POST', 'OPTIONS'])]

  def _handler(self):
    """Returns a RequestHandler class with access to this server's data."""

    # Alias self as server here for readability.
    server = self

    class Handler(webapp2.RequestHandler):

      def post(self, service, method):
        """Writes body and headers of webapp2.Response.

        Args:
          service: the service being targeted by this pRPC call.
          method: the method being invoked by this pRPC call.

        Returns:
          response: a webapp2.Response.
        """

        context = ServicerContext()
        context.set_code(StatusCode.OK)

        content, accept = self._handle(context, service, method)
        origin = self.request.headers.get('Origin')
        if origin:
          self.response.headers['Access-Control-Allow-Origin'] = origin
          self.response.headers['Vary'] = 'Origin'
          self.response.headers['Access-Control-Allow-Credentials'] = 'true'
        self.response.status = _PRPC_TO_HTTP_STATUS[context.code]
        self.response.headers['X-Prpc-Grpc-Code'] = str(context.code[0])
        self.response.headers['Access-Control-Expose-Headers'] = (
            'X-Prpc-Grpc-Code')
        if content is not None:
          self.response.headers['Content-Type'] = encoding.Encoding.header(
              accept)
          self.response.out.write(content)
        elif context.details is not None:
          # webapp2 will automatically encode strings as utf-8.
          # http://webapp2.readthedocs.io/en/latest/guide/response.html
          #
          # TODO(nodir,mknyszek): Come up with an actual test for this.
          self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
          self.response.out.write(context.details)
        return self.response

      def _handle(self, context, service, method):
        """Generates the response content and sets the context appropriately.

        Args:
          context: a context.ServicerContext.
          service: the service being targeted by this pRPC call.
          method: the method being invoked by this pRPC call.

        Returns:
          content: the binary or textual content of the RPC response. Note
            that this may be None in the event that an error occurs.
          accept: an encoding.Encoding enum value for the encoding of the
            response.
        """

        try:
          content_type, accept = headers.process_headers(
              context, self.request.headers)
        except ValueError as e:
          logging.exception('Error processing headers')
          context.set_code(StatusCode.INVALID_ARGUMENT)
          context.set_details(e.message)
          return None, None

        if service not in server._services:
          context.set_code(StatusCode.UNIMPLEMENTED)
          context.set_details('Service %s does not exist' % service)
          return None, None
        rpc_handler = server._services[service].methods.get(method)
        if rpc_handler is None:
          context.set_code(StatusCode.UNIMPLEMENTED)
          context.set_details('Method %s does not exist' % method)
          return None, None
        request_message, response_message, handler = rpc_handler

        request = request_message()
        try:
          decoder = encoding.get_decoder(content_type)
          decoder(self.request.body, request)
        except Exception as e:
          logging.warning('Failed to decode request: %s' % e)
          context.set_code(StatusCode.INVALID_ARGUMENT)
          context.set_details('Error parsing request')
          return None, None

        try:
          # TODO(nodir,mknyszek): Poll for context to hit timeout or be
          # canceled.
          response = handler(request, context)
        except Exception:
          logging.exception('Service implementation threw an exception')
          context.set_code(StatusCode.INTERNAL)
          context.set_details('Service implementation threw an exception')
          return None, None

        if response is None:
          return None, None

        if not isinstance(response, response_message):
          logging.error('Service implementation response has incorrect type')
          context.set_code(StatusCode.INTERNAL)
          context.set_details('Service implementation returned invalid value')
          return None, None

        try:
          encoder = encoding.get_encoder(accept)
          content = encoder(response)
        except Exception:
          logging.exception('Failed to encode response')
          context.set_code(StatusCode.INTERNAL)
          context.set_details('Error serializing response')
          return None, None

        return content, accept

      def options(self, _service, _method):
        """Sends an empty response with headers for CORS for all requests."""
        origin = self.request.headers.get('Origin')
        if origin:
          self.response.headers['Access-Control-Allow-Origin'] = origin
          self.response.headers['Vary'] = 'Origin'
          self.response.headers['Access-Control-Allow-Credentials'] = 'true'
          self.response.headers['Access-Control-Allow-Headers'] = [
              'Origin', 'Content-Type', 'Accept', 'Authorization']
          self.response.headers['Access-Control-Allow-Methods'] = [
              'OPTIONS', 'POST']
          self.response.headers['Access-Control-Max-Age'] = '600'

    return Handler
