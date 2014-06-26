# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Auth management REST API."""

import json
import urllib
import webapp2

from google.appengine.ext import ndb

from components import utils

from .. import api
from .. import handler
from .. import model


def get_rest_api_routes():
  """Return a list of webapp2 routes with auth REST API handlers."""
  assert model.GROUP_NAME_RE.pattern[0] == '^'
  group_re = model.GROUP_NAME_RE.pattern[1:]
  return [
    webapp2.Route('/auth/api/v1/accounts/self', SelfHandler),
    webapp2.Route('/auth/api/v1/accounts/self/xsrf_token', XSRFHandler),
    webapp2.Route('/auth/api/v1/groups', GroupsHandler),
    webapp2.Route('/auth/api/v1/groups/<group:%s>' % group_re, GroupHandler),
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

  @api.require(api.is_admin)
  def get(self):
    # Currently AuthGroup entity contains a list of group members in the entity
    # body. It's an implementation detail that should not be relied upon.
    # Generally speaking, fetching a list of group members can be an expensive
    # operation, and group listing call shouldn't do it all the time. So throw
    # away all fields that enumerate group members.
    group_list = model.AuthGroup.query(ancestor=model.ROOT_KEY).fetch()
    self.send_response({
      'groups': [
        g.to_serializable_dict(
            with_id_as='name',
            exclude=('members', 'globs', 'nested'))
        for g in sorted(group_list, key=lambda x: x.key.string_id())
      ],
    })


class GroupHandler(ApiHandler):
  """Creating, reading, updating and deleting a single group."""

  @api.require(api.is_admin)
  def get(self, group):
    """Fetches all information about an existing group give its name."""
    obj = model.group_key(group).get()
    if not obj:
      self.abort_with_error(404, text='No such group')
    self.send_response(
        response={'group': obj.to_serializable_dict(with_id_as='name')},
        headers={'Last-Modified': utils.datetime_to_rfc2822(obj.modified_ts)})

  @api.require(api.is_admin)
  def put(self, group):
    """Updates an existing group."""
    # Deserialize and validate the body.
    try:
      body = self.parse_body()
      name = body.pop('name', None)
      if not name or name != group:
        raise ValueError('Missing or mismatching group name in request body')
      group_params = model.AuthGroup.convert_serializable_dict(body)
    except (TypeError, ValueError) as err:
      self.abort_with_error(400, text=str(err))

    @ndb.transactional
    def update(params, modified_by, expected_ts):
      # Missing?
      entity = model.group_key(group).get()
      if not entity:
        return None, {'http_code': 404, 'text': 'No such group'}

      # Check the precondition if required.
      if (expected_ts and
          utils.datetime_to_rfc2822(entity.modified_ts) != expected_ts):
        return None, {
          'http_code': 412,
          'text': 'Group was modified by someone else',
        }

      # If adding new nested groups, need to ensure they exist.
      added_nested_groups = set(params['nested']) - set(entity.nested)
      if added_nested_groups:
        missing = model.get_missing_groups(added_nested_groups)
        if missing:
          return None, {
            'http_code': 409,
            'text': 'Referencing a nested group that doesn\'t exist',
            'details': {'missing': missing},
          }

      # Update in place.
      entity.populate(**params)
      entity.modified_by = modified_by

      # Now make sure updated group is not a part of new group dependency cycle.
      if added_nested_groups:
        cycle = model.find_group_dependency_cycle(entity)
        if cycle:
          return None, {
            'http_code': 409,
            'text': 'Groups can not have cyclic dependencies',
            'details': {'cycle': cycle},
          }

      # Looks good.
      entity.put()
      model.replicate_auth_db()
      return entity, None

    # Run the transaction.
    entity, error_details = update(
        group_params,
        api.get_current_identity(),
        self.request.headers.get('If-Unmodified-Since'))
    if entity is None:
      self.abort_with_error(**error_details)

    self.send_response(
        response={'ok': True},
        http_code=200,
        headers={
          'Last-Modified': utils.datetime_to_rfc2822(entity.modified_ts),
        }
    )

  @api.require(api.is_admin)
  def post(self, group):
    """Creates a new group, ensuring it's indeed new (no overwrites)."""
    # Deserialize a body into the entity.
    try:
      body = self.parse_body()
      name = body.pop('name', None)
      if not name or name != group:
        raise ValueError('Missing or mismatching group name in request body')
      entity = model.AuthGroup.from_serializable_dict(
          serializable_dict=body,
          key=model.group_key(group),
          created_by=api.get_current_identity(),
          modified_by=api.get_current_identity())
    except (TypeError, ValueError) as err:
      self.abort_with_error(400, text=str(err))

    @ndb.transactional
    def put_new(entity):
      # The group should be new.
      if entity.key.get():
        return False, {'text': 'Such group already exists'}

      # All nested groups should exist.
      missing = model.get_missing_groups(entity.nested)
      if missing:
        return False, {
          'text': 'Referencing a nested group that doesn\'t exist',
          'details': {'missing': missing},
        }

      # Ok, good enough.
      entity.put()
      model.replicate_auth_db()
      return True, None

    success, error_details = put_new(entity)
    if not success:
      self.abort_with_error(409, **error_details)

    self.send_response(
        response={'ok': True},
        http_code=201,
        headers={
          'Last-Modified': utils.datetime_to_rfc2822(entity.modified_ts),
          'Location': '/auth/api/v1/groups/%s' % urllib.quote(entity.key.id()),
        }
    )

  @api.require(api.is_admin)
  @ndb.transactional
  def delete(self, group):
    """Deletes a group.

    Respects 'If-Unmodified-Since' header. If it is set, verifies the group
    has exact same modification timestamp as specified in the header before
    deleting it. Returns 'HTTP Error 412: Precondition failed' otherwise.

    Will also check that the group is not referenced in any ACL rules or other
    groups (as a nested group). If it does, request returns
    'HTTP Error 409: Conflict', providing information about what depends on
    this group in the response body.
    """
    entity = model.group_key(group).get()
    expected_ts = self.request.headers.get('If-Unmodified-Since')
    if entity is None:
      if expected_ts:
        # Precondition was used? Group was expected to exist.
        self.abort_with_error(412, text='Group was deleted by someone else')
      else:
        # Unconditionally deleting it, and it's already gone -> success.
        self.send_response({'ok': True})
      return

    # Check the precondition if required.
    if (expected_ts and
        utils.datetime_to_rfc2822(entity.modified_ts) != expected_ts):
      self.abort_with_error(412, text='Group was modified by someone else')

    # Check the group is not used by other groups.
    referencing_groups = model.find_referencing_groups(group)
    if referencing_groups:
      self.abort_with_error(
          http_code=409,
          text='Group is being referenced in other groups',
          details={
            'groups': list(referencing_groups),
          }
      )

    # Ok, good to delete.
    entity.key.delete()
    model.replicate_auth_db()
    self.send_response({'ok': True})


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

  @api.require(api.is_admin)
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
      model.replicate_auth_db()

    update()
    self.send_response({'ok': True})
