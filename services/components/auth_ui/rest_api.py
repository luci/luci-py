# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Auth management REST API."""

import json
import webapp2

from google.appengine.ext import ndb

from components.auth import api
from components.auth import handler
from components.auth import model

# Part of public API of 'auth_ui' component, exposed by this module.
__all__ = ['get_rest_api_routes']


def get_rest_api_routes():
  """Return a list of webapp2 routes with auth REST API handlers."""
  return [
    webapp2.Route('/auth/api/v1/accounts/self', SelfHandler),
    webapp2.Route('/auth/api/v1/accounts/self/xsrf_token', XSRFHandler),
    webapp2.Route('/auth/api/v1/groups', GroupsHandler),
    webapp2.Route('/auth/api/v1/server/oauth_config', OAuthConfigHandler),
  ]


class ApiHandler(handler.AuthenticatingHandler):
  """Parses JSON request body to a dict, serializes response to JSON."""

  # Content type of requests and responses.
  content_type = 'application/json; charset=UTF-8'

  def authentication_error(self, error):
    self.abort_with_error(401, text=str(error))

  def authorization_error(self, error):
    self.abort_with_error(403, text=str(error))

  def send_response(self, response, http_code=200, headers=None):
    """Sends successful reply and continues execution."""
    self.response.set_status(http_code)
    self.response.headers.update(headers or {})
    self.response.headers['Content-Type'] = self.content_type
    self.response.write(json.dumps(response))

  def abort_with_error(self, http_code, **kwargs):
    """Sends error reply and stops execution."""
    self.abort(
        http_code, json=kwargs, headers={'Content-Type': self.content_type})

  def parse_body(self):
    """Parse JSON body and verifies it's a dict."""
    content_type = self.request.headers.get('Content-Type')
    if content_type != self.content_type:
      msg = 'Expecting JSON body with content type \'%s\'' % self.content_type
      self.abort_with_error(400, text=msg)
    try:
      body = json.loads(self.request.body)
      if not isinstance(body, dict):
        raise ValueError()
    except ValueError:
      self.abort_with_error(400, text='Not a valid json dict body')
    return body


class SelfHandler(ApiHandler):
  """Returns identity of a caller."""

  @api.public
  def get(self):
    self.send_response({'identity': api.get_current_identity().to_bytes()})


class XSRFHandler(ApiHandler):
  """Generates XSRF token on demand.

  Should be used only by client scripts or Ajax calls. Requires header
  'X-XSRF-Token-Request' to be present (actual value doesn't matter).
  """

  # Don't enforce prior XSRF token, it might not be known yet.
  xsrf_token_enforce_on = ()

  @api.public
  def post(self):
    if not self.request.headers.get('X-XSRF-Token-Request'):
      raise api.AuthorizationError('Missing required XSRF request header')
    self.send_response({'xsrf_token': self.generate_xsrf_token()})


class GroupsHandler(ApiHandler):
  """Lists all registered groups.

  Returns a list of groups, sorted by name. Each entry in a list is a dict with
  all details about the group except the actual list of members
  (which may be large).
  """

  @api.require(model.READ, 'auth/management')
  def get(self):
    # Currently AuthGroup entity contains a list of group members in the entity
    # body. It's an implementation detail that should not be relied upon.
    # Generally speaking, fetching a list of group members can be an expensive
    # operation, and group listing call shouldn't do it all the time. So throw
    # away all fields that enumerate group members.
    groups = model.AuthGroup.query(ancestor=model.ROOT_KEY).fetch()
    self.send_response({
      'groups': [
        g.to_serializable_dict(
            with_id_as='name',
            exclude=('members', 'globs', 'nested'))
        for g in sorted(groups, key=lambda x: x.key.string_id())
      ],
    })


class OAuthConfigHandler(ApiHandler):
  """Returns client_id and client_secret to use for OAuth2 login on a client."""

  @api.public
  def get(self):
    client_id = None
    client_secret = None
    additional_ids = None
    cache_control = self.request.headers.get('Cache-Control')

    # Use most up-to-date data in datastore if requested. Used by management UI.
    if cache_control in ('no-cache', 'max-age=0'):
      global_config = model.ROOT_KEY.get()
      client_id = global_config.oauth_client_id
      client_secret = global_config.oauth_client_secret
      additional_ids = global_config.oauth_additional_client_ids
    else:
      # Faster call that uses cached config (that may be several minutes stale).
      # Used by all client side scripts that just want to authenticate.
      auth_db = api.get_request_auth_db()
      client_id, client_secret, additional_ids = auth_db.get_oauth_config()

    self.send_response({
      'additional_client_ids': additional_ids,
      'client_id': client_id,
      'client_not_so_secret': client_secret,
    })

  @api.require(model.UPDATE, 'auth/management')
  def post(self):
    body = self.parse_body()
    client_id = body['client_id']
    client_secret = body['client_not_so_secret']
    additional_client_ids = filter(bool, body['additional_client_ids'])

    @ndb.transactional
    def update():
      config = model.ROOT_KEY.get()
      config.populate(
          oauth_client_id=client_id,
          oauth_client_secret=client_secret,
          oauth_additional_client_ids=additional_client_ids)
      config.put()

    update()
    self.send_response({'ok': True})
