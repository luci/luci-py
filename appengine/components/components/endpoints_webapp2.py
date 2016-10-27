# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import endpoints
import httplib
import json
import logging
import posixpath
import urlparse

from endpoints import protojson
from google.appengine.api import memcache
from google.appengine.api import modules
from protorpc import message_types
from protorpc import messages
from protorpc import remote
import webapp2

from components import net


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
  if res_container:
    result = res_container.combined_message_class()
    for f in body.all_fields():
      setattr(result, f.name, getattr(body, f.name))
  else:
    result = body

  # Read field values from query string parameters or URL path.
  if res_container or request.method == 'GET':
    if request.method == 'GET':
      # In addition to standard ResourceContainer request type, we also support
      # GET request handlers that use Message instead of ResourceContainer,
      # because it is non-ambiguous (because GET requests cannot have body).
      param_fields = result.all_fields()
    else:
      param_fields = res_container.parameters_message_class.all_fields()
    for f in param_fields:
      if f.name in request.route_kwargs:
        values = [request.route_kwargs[f.name]]
      else:
        values = request.get_all(f.name)
      if values:
        values = [decode_field(f, v) for v in values]
        if f.repeated:
          getattr(result, f.name).extend(values)
        else:
          setattr(result, f.name, values[0])
  return result


def add_cors_headers(headers):
  headers['Access-Control-Allow-Origin'] = '*'
  headers['Access-Control-Allow-Headers'] = (
    'Origin, Authorization, Content-Type, Accept')
  headers['Access-Control-Allow-Methods'] = (
    'DELETE, GET, OPTIONS, POST, PUT')


class CorsHandler(webapp2.RequestHandler):
  def options(self):
    add_cors_headers(self.response.headers)


def path_handler(api_class, api_method, service_path):
  """Returns a webapp2.RequestHandler subclass for the API methods."""
  # Why return a class? Because webapp2 explicitly checks if handler that we
  # passed to Route is a class.

  class Handler(webapp2.RequestHandler):
    def dispatch(self):
      add_cors_headers(self.response.headers)

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


def api_routes(api_class, base_path=None):
  base_path = base_path or '/api/%s/%s' % (
      api_class.api_info.name,
      api_class.api_info.version)
  routes = []
  templates = set()
  for _, m in sorted(api_class.all_remote_methods().iteritems()):
    info = m.method_info

    method_path = info.get_path(api_class.api_info)
    method_path = method_path.replace('{', '<').replace('}', '>')
    template = posixpath.join(base_path, method_path)

    http_method = info.http_method.upper() or 'POST'

    handler = path_handler(api_class, m, base_path)
    routes.append(webapp2.Route(template, handler, methods=[http_method]))
    templates.add(template)
  for t in sorted(templates):
    routes.append(webapp2.Route(t, CorsHandler, methods=['OPTIONS']))
  return routes


class DiscoveryHandler(webapp2.RequestHandler):
  """Returns a discovery document for a service.

  Piggy-backs on real Cloud Endpoints discovery service, requires it.
  """

  def get_doc(self, service, version):
    cache_key = 'discovery_doc/%s/%s/%s' % (
      modules.get_current_version_name(), service, version)
    cached = memcache.get(cache_key)
    if cached:
      return cached[0]

    logging.info('Fetching actual discovery document')

    doc_url = '%s://%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (
      self.request.scheme,  # Needed for local devserver.
      self.request.host,
      service,
      version)
    try:
      doc = net.json_request(url=doc_url, deadline=45)
      logging.info('Fetched actual discovery document')
    except net.NotFoundError:
      doc = None

    if doc:
      for key in ('baseUrl', 'basePath', 'rootUrl'):
        url = urlparse.urlparse(doc.get(key))
        if url.path.startswith('/_ah/'):
          url = url._replace(path=url.path[len('/_ah'):])
        doc[key] = urlparse.urlunparse(url)

      if 'batchPath' in doc:
        del doc['batchPath']

    memcache.add(cache_key, (doc,))
    return doc

  def get(self, service, version):
    doc = self.get_doc(service, version)
    if not doc:
      self.abort(404, 'Not found')
    self.response.headers['Content-Type'] = 'application/json'
    json.dump(doc, self.response, separators=(',', ':'))


def discovery_service_route():
  return webapp2.Route(
      '/api/discovery/v1/apis/<service>/<version>/rest', DiscoveryHandler)
