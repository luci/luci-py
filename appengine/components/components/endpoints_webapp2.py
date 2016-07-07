# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import endpoints
import httplib
import posixpath

from endpoints import protojson
from protorpc import message_types
from protorpc import messages
from protorpc import remote
import webapp2


PROTOCOL = protojson.EndpointsProtoJson()


def decode_field(field, value):
  """Like PROTOCOL.decode_field, but also supports booleans."""
  if isinstance(field, messages.BooleanField):
    value = value.lower()
    if value == 'true':
      return True
    elif value == 'false':
      return False
    else:
      raise ValueError('boolean field must be either "true" or "false"')
  return PROTOCOL.decode_field(field, value)


def decode_message(remote_method_info, request):
  """Decodes a protorpc message from an webapp2 request.

  If method accepts a resource container, parses field values from URL too.
  """
  req_msg = endpoints.ResourceContainer.get_request_message(remote_method_info)
  if isinstance(req_msg, endpoints.ResourceContainer):
    res_container = req_msg
    body_type = req_msg.body_message_class
  else:
    res_container = None
    body_type = remote_method_info.request_type

  body = PROTOCOL.decode_message(body_type, request.body)
  if not res_container:
    return body

  msg = res_container.combined_message_class()
  for f in body.all_fields():
    setattr(msg, f.name, getattr(body, f.name))

  # We expect fields in the resource container to be in the URL.
  for f in res_container.parameters_message_class.all_fields():
    if f.name in request.route_kwargs:
      values = [request.route_kwargs[f.name]]
    else:
      values = request.get_all(f.name)
    if values:
      values = [decode_field(f, v) for v in values]
      if f.repeated:
        getattr(msg, f.name).extend(values)
      else:
        setattr(msg, f.name, values[0])
  return msg


def path_handler(api_class, api_method, service_path):
  """Returns a webapp2.RequestHandler subclass for the API methods."""
  # Why return a class? Because webapp2 explicitly checks if handler that we
  # passed to Route is a class.

  class Handler(webapp2.RequestHandler):
    def dispatch(self):
      api = api_class()
      api.initialize_request_state(remote.HttpRequestState(
          remote_host=None,
          remote_address=self.request.remote_addr,
          server_host=self.request.host,
          server_port=self.request.server_port,
          http_method=self.request.method,
          service_path=service_path,
          headers=self.request.headers.items()))

      try:
        req = decode_message(api_method.remote, self.request)
      except (messages.DecodeError, messages.ValidationError, ValueError) as ex:
        self.response.write(ex.message)
        self.response.write('\n')
        self.response.set_status(httplib.BAD_REQUEST)
        return

      try:
        res = api_method(api, req)
      except endpoints.ServiceException as ex:
        self.response.write(ex.message)
        self.response.write('\n')
        self.response.set_status(ex.http_status)
        return

      res_encoded = PROTOCOL.encode_message(res)
      if isinstance(res, message_types.VoidMessage):
        self.response.set_status(204)
      self.response.content_type = 'application/json; charset=utf-8'
      self.response.out.write(res_encoded)

  return Handler


def path_template_to_webapp2(template):
  return template


def api_routes(api_class, base_path=None):
  base_path = base_path or '/api/%s/%s' % (
      api_class.api_info.name,
      api_class.api_info.version)
  routes = []
  for m in api_class.all_remote_methods().itervalues():
    info = m.method_info

    method_path = info.get_path(api_class.api_info)
    method_path = method_path.replace('{', '<').replace('}', '>')
    template = posixpath.join(base_path, method_path)

    http_method = info.http_method.upper() or 'POST'

    routes.append(webapp2.Route(
        template,
        path_handler(api_class, m, base_path),
        methods=[http_method]))
  return routes
