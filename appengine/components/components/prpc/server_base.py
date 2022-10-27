# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""A stripped-down implementation of the gRPC interface for Python on AppEngine.

The Server class itself also provides the bulk of the implementation which
actually runs on AppEngine (and therefore couldn't include Cython).
Subclasses should override get_routes() method for the host application to call.

https://github.com/grpc/grpc/tree/master/src/python/grpcio/grpc
"""

import collections
import logging

from google.protobuf import symbol_database

# Helpers are in separate modules so this one exposes only the public interface.
from components.prpc import encoding
from components.prpc import headers
from components.prpc.codes import StatusCode
from components.prpc.context import ServicerContext

# Helpers are in separate modules so this one exposes only the public interface.
from components.prpc import discovery
from components.prpc.codes import StatusCode

__all__ = [
    'HandlerCallDetails',
    'ServerBase',
    'StatusCode',
]

_Service = collections.namedtuple('_Service', ['servicer', 'methods'])

# Details about the RPC call passed to the interceptors.
HandlerCallDetails = collections.namedtuple(
    'HandlerCallDetails',
    [
        'method',  # full method name <service>.<method>
        # (k, v) pairs list with metadata sent by the client
        'invocation_metadata',
    ])


class ServerBase(object):
  """Server represents a pRPC server that handles a set of services.

  Server is intended to vaguely mimic gRPC's Server as an abstraction, but
  provides a simpler interface via add_service and get_routes.
  """

  def __init__(self, allow_cors=True, allowed_origins=None):
    """Initializes a new Server.

    Args:
      allow_cors: opttional boolean. True if CORS should be allowed.
      allowed_origins: optional collection of allowed origins. Only used
        when cors is allowed. If empty, all origins will be allowed, otherwise
        only listed origins will be allowed.

    """
    self._services = {}
    self._interceptors = ()
    self._discovery_service = discovery.Discovery()
    self.add_service(self._discovery_service)
    self.allow_cors = allow_cors
    self.allowed_origins = set(allowed_origins or [])

  def add_interceptor(self, interceptor):
    """Adds an interceptor to the interceptor chain.

    Interceptors can be used to examine or modify requests before they reach
    the real handlers. The API is vaguely similar to grpc.ServerInterceptor.

    An interceptor is a callback that accepts following arguments:
      request: deserialized request message.
      context: an instance of ServicerContext.
      call_details: an instance of HandlerCallDetails.
      continuation: a callback that resumes the processing, accepts
        (request, context, call_details).

    An interceptor may decide NOT to call the continuation if it handles the
    request itself.

    Args:
      interceptor: an interceptor callback to add to the chain.
    """
    self._interceptors = self._interceptors + (interceptor, )

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
    pkg = servicer.DESCRIPTION['file_descriptor'].package
    desc = servicer.DESCRIPTION['service_descriptor']

    # Construct handler.
    methods = {
        method.name: (
            # Fully-qualified proto type names will always begin with a '.'
            # which GetSymbol doesn't strip out.
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
      raise ValueError('Tried to double-register handlers for service %s' %
                       desc.name)
    self._services[full_name] = _Service(servicer, methods)

    self._discovery_service.add_service(servicer.DESCRIPTION)

  def get_routes(self, prefix=''):
    """Returns a list of routes the API handles."""
    raise NotImplementedError("Get_routes must be implemented")

  def _run_interceptors(self, request, context, call_details, handler, idx):
    """Runs the request via interceptors chain starting from given index.

    Args:
      request: deserialized request proto.
      context: a context.ServicerContext.
      handler: a final handler, given as callback (request, context): response.
      idx: an index in the interceptor chain to start from.

    Returns:
      Response message.
    """
    if idx == len(self._interceptors):
      return handler(request, context)

    def continuation(request, context, call_details):
      return self._run_interceptors(request, context, call_details, handler,
                                    idx + 1)

    interceptor = self._interceptors[idx]
    return interceptor(request, context, call_details, continuation)

  def _routes_handle(self, context, service, method, router_request):
    """Generates the response content and sets the context appropriately.

    Sets context._request_encoding and context._response_encoding.

    Args:
      context: a context.ServicerContext.
      service: the service being targeted by this pRPC call.
      method: the method being invoked by this pRPC call.
      router_request: the request from server

    Returns:
      content: the binary or textual content of the RPC response. Note
        that this may be None in the event that an error occurs.
    """
    try:
      parsed_headers = headers.parse_headers(router_request.headers)
      context._request_encoding = parsed_headers.content_type
      context._response_encoding = parsed_headers.accept
    except ValueError as e:
      logging.warning('Error parsing headers: %s', e)
      context.set_code(StatusCode.INVALID_ARGUMENT)
      context.set_details(e.message)
      return None

    if service not in self._services:
      context.set_code(StatusCode.UNIMPLEMENTED)
      context.set_details('Service %s does not exist' % service)
      return None
    rpc_handler = self._services[service].methods.get(method)
    if rpc_handler is None:
      context.set_code(StatusCode.UNIMPLEMENTED)
      context.set_details('Method %s does not exist' % method)
      return None
    request_message, response_message, handler = rpc_handler

    request = request_message()
    try:
      decoder = encoding.get_decoder(parsed_headers.content_type)
      if hasattr(router_request, 'body'):
        # handle webapp2 request
        decoder(router_request.body, request)
      else:
        # handle flask request
        decoder(router_request.get_data(), request)

    except Exception as e:
      logging.warning('Failed to decode request: %s', e, exc_info=True)
      context.set_code(StatusCode.INVALID_ARGUMENT)
      context.set_details('Error parsing request: %s' % e.message)
      return None

    context._timeout = parsed_headers.timeout
    context._invocation_metadata = parsed_headers.invocation_metadata

    # Only ipv6 addresses have ':' in them. Assume everything else is ipv4.
    if ':' in router_request.remote_addr:
      context._peer = 'ipv6:[%s]' % router_request.remote_addr
    else:
      context._peer = 'ipv4:%s' % router_request.remote_addr

    call_details = HandlerCallDetails(
        method='%s.%s' % (service, method),
        invocation_metadata=context.invocation_metadata())

    try:
      # TODO(nodir,mknyszek): Poll for context to hit timeout or be
      # canceled.
      response = self._run_interceptors(request, context, call_details, handler,
                                        0)
    except Exception:
      logging.exception('Service implementation threw an exception')
      context.set_code(StatusCode.INTERNAL)
      context.set_details('Service implementation threw an exception')
      return None

    if response is None:
      if context._code == StatusCode.OK:
        context.set_code(StatusCode.INTERNAL)
        context.set_details('Service implementation didn\'t return a response')
      return None

    if not isinstance(response, response_message):
      logging.error('Service implementation response has incorrect type')
      context.set_code(StatusCode.INTERNAL)
      context.set_details('Service implementation returned invalid value')
      return None

    try:
      encoder = encoding.get_encoder(parsed_headers.accept)
      content = encoder(response)
    except Exception:
      logging.exception('Failed to encode response')
      context.set_code(StatusCode.INTERNAL)
      context.set_details('Error serializing response')
      return None

    return content

  def _options_handler(self, request, response):
    """Sends an empty response with CORS headers for origins, if allowed."""
    origin = request.headers.get('Origin')

    if origin and self.allow_cors and (not self.allowed_origins
                                       or origin in self.allowed_origins):
      response.headers['Access-Control-Allow-Origin'] = origin
      response.headers['Vary'] = 'Origin'
      response.headers['Access-Control-Allow-Credentials'] = 'true'
      response.headers['Access-Control-Allow-Headers'] = \
              'Origin, Content-Type, Accept, Authorization'
      response.headers['Access-Control-Allow-Methods'] = \
              'OPTIONS, POST'
      response.headers['Access-Control-Max-Age'] = '600'

    return response

  def _post_handler(self, service, method, request, response):
    """Writes body and headers of Response.

    Args:
      service: the service being targeted by this pRPC call.
      method: the method being invoked by this pRPC call.

    Returns:
      response: response to be set to actual flask/webapp2 response
    """
    context = ServicerContext()
    content = self._routes_handle(context,
                                  service,
                                  method,
                                  router_request=request)
    origin = request.headers.get('Origin')
    if origin:
      response.headers['Access-Control-Allow-Origin'] = origin
      response.headers['Vary'] = 'Origin'
      response.headers['Access-Control-Allow-Credentials'] = 'true'
    self._response_body_and_status_writer(response,
                                          status=StatusCode.to_http_code(
                                              context._code))
    response.headers['X-Prpc-Grpc-Code'] = str(context._code.value)
    response.headers['Access-Control-Expose-Headers'] = ('X-Prpc-Grpc-Code')
    response.headers['X-Content-Type-Options'] = 'nosniff'
    if content is not None:
      response.headers['Content-Type'] = encoding.Encoding.media_type(
          context._response_encoding)
      self._response_body_and_status_writer(response, body=content)
    elif context._details is not None:
      # webapp2 will automatically encode strings as utf-8.
      # http://webapp2.readthedocs.io/en/latest/guide/response.html
      #
      # TODO(nodir,mknyszek): Come up with an actual test for this.
      response.headers['Content-Type'] = 'text/plain; charset=utf-8'
      self._response_body_and_status_writer(response, body=context._details)

  def _response_body_and_status_writer(self, response, body=None, status=None):
    raise NotImplementedError(
        "response body and status writer must be implemented")
