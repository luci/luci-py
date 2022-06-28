# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# Helpers are in separate modules so this one exposes only the public interface.
from components.prpc.server_base import ServerBase

try:
  import flask
except ImportError:
  flask = None


class FlaskServer(ServerBase):
  def get_routes(self, prefix=''):
    return [('%s/prpc/<service>/<method>' % prefix, self._prpcHandler,
             ['POST', 'OPTIONS'])]

  def _response_body_and_status_writer(self, response, body=None, status=None):
    if body is not None:
      response.set_data(body)
    if status is not None:
      response.status_code = status

  def _prpcHandler(self, service, method):
    request = flask.request
    response = flask.make_response()

    if request.method == 'POST':
      self._post_handler(service, method, request, response)
    elif request.method == 'OPTIONS':
      self._options_handler(request, response)

    return response
