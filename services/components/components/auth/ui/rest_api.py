# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Auth management REST API."""

import base64
import functools
import logging
import urllib
import webapp2

from google.appengine.ext import ndb

from components import utils

from .. import api
from .. import handler
from .. import model
from .. import replication
from .. import signature
from ..proto import replication_pb2


def get_rest_api_routes():
  """Return a list of webapp2 routes with auth REST API handlers."""
  assert model.GROUP_NAME_RE.pattern[0] == '^'
  group_re = model.GROUP_NAME_RE.pattern[1:]
  return [
    webapp2.Route('/auth/api/v1/accounts/self', SelfHandler),
    webapp2.Route('/auth/api/v1/accounts/self/xsrf_token', XSRFHandler),
    webapp2.Route('/auth/api/v1/groups', GroupsHandler),
    webapp2.Route('/auth/api/v1/groups/<group:%s>' % group_re, GroupHandler),
    webapp2.Route('/auth/api/v1/internal/replication', ReplicationHandler),
    webapp2.Route('/auth/api/v1/server/certificates', CertificatesHandler),
    webapp2.Route('/auth/api/v1/server/oauth_config', OAuthConfigHandler),
    webapp2.Route('/auth/api/v1/server/state', ServerStateHandler),
  ]


def forbid_api_on_replica(method):
  """Decorator for methods that are not allowed to be called on Replica.

  If such method is called on a service in Replica mode, it would return
  HTTP 405 "Method Not Allowed".
  """
  @functools.wraps(method)
  def wrapper(self, *args, **kwargs):
    assert isinstance(self, webapp2.RequestHandler)
    if model.is_replica():
      self.abort(
          405,
          json={
            'primary_url': model.get_replication_state().primary_url,
            'text': 'Use Primary service for API requests',
          },
          headers={
            'Content-Type': 'application/json; charset=utf-8',
          })
    return method(self, *args, **kwargs)
  return wrapper


class SelfHandler(handler.ApiHandler):
  """Returns identity of a caller.

  Available in Standalone, Primary and Replica modes.
  """

  @api.public
  def get(self):
    self.send_response({'identity': api.get_current_identity().to_bytes()})


class XSRFHandler(handler.ApiHandler):
  """Generates XSRF token on demand.

  Should be used only by client scripts or Ajax calls. Requires header
  'X-XSRF-Token-Request' to be present (actual value doesn't matter).

  Available in Standalone, Primary and Replica modes.
  """

  # Don't enforce prior XSRF token, it might not be known yet.
  xsrf_token_enforce_on = ()

  @handler.require_xsrf_token_request
  @api.public
  def post(self):
    self.send_response({'xsrf_token': self.generate_xsrf_token()})


class GroupsHandler(handler.ApiHandler):
  """Lists all registered groups.

  Returns a list of groups, sorted by name. Each entry in a list is a dict with
  all details about the group except the actual list of members
  (which may be large).

  Available in Standalone, Primary and Replica modes.
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


class GroupHandler(handler.ApiHandler):
  """Creating, reading, updating and deleting a single group.

  GET is available in Standalone, Primary and Replica modes.
  Everything else is available only in Standalone and Primary modes.
  """

  @api.require(api.is_admin)
  def get(self, group):
    """Fetches all information about an existing group give its name."""
    obj = model.group_key(group).get()
    if not obj:
      self.abort_with_error(404, text='No such group')
    self.send_response(
        response={'group': obj.to_serializable_dict(with_id_as='name')},
        headers={'Last-Modified': utils.datetime_to_rfc2822(obj.modified_ts)})

  @forbid_api_on_replica
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
      entity.modified_ts = utils.utcnow()
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
    if not entity:
      self.abort_with_error(**error_details)

    self.send_response(
        response={'ok': True},
        http_code=200,
        headers={
          'Last-Modified': utils.datetime_to_rfc2822(entity.modified_ts),
        }
    )

  @forbid_api_on_replica
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

  @forbid_api_on_replica
  @api.require(api.is_admin)
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
    @ndb.transactional
    def delete(expected_ts):
      entity = model.group_key(group).get()
      if not entity:
        if expected_ts:
          # Precondition was used? Group was expected to exist.
          return False, {
            'http_code': 412, 'text': 'Group was deleted by someone else'}
        else:
          # Unconditionally deleting it, and it's already gone -> success.
          return True, None

      # Check the precondition if required.
      if (expected_ts and
          utils.datetime_to_rfc2822(entity.modified_ts) != expected_ts):
        return False, {
            'http_code': 412, 'text': 'Group was modified by someone else'}

      # Check the group is not used by other groups.
      referencing_groups = model.find_referencing_groups(group)
      if referencing_groups:
        return False, {
          'http_code': 409,
          'text': 'Group is being referenced in other groups',
          'details': {'groups': list(referencing_groups)},
        }

      # Ok, good to delete.
      entity.key.delete()
      model.replicate_auth_db()
      return True, None

    success, error_details = delete(
        self.request.headers.get('If-Unmodified-Since'))
    if not success:
      self.abort_with_error(**error_details)
    self.send_response({'ok': True})


class ReplicationHandler(handler.AuthenticatingHandler):
  """Accepts AuthDB push from Primary."""

  # Handler uses X-Appengine-Inbound-Appid header protected by GAE.
  xsrf_token_enforce_on = ()

  def send_response(self, response):
    """Sends serialized ReplicationPushResponse as a response."""
    assert isinstance(response, replication_pb2.ReplicationPushResponse)
    self.response.headers['Content-Type'] = 'application/octet-stream'
    self.response.write(response.SerializeToString())

  def send_error(self, error_code):
    """Sends ReplicationPushResponse with fatal error as a response."""
    response = replication_pb2.ReplicationPushResponse()
    response.status = replication_pb2.ReplicationPushResponse.FATAL_ERROR
    response.error_code = error_code
    self.send_response(response)

  # Check that request came from some GAE app. More thorough check is inside.
  @api.require(lambda: api.get_current_identity().is_service)
  def post(self):
    # Check that current service is a Replica.
    if not model.is_replica():
      self.send_error(replication_pb2.ReplicationPushResponse.NOT_A_REPLICA)
      return

    # Check that request came from expected Primary service.
    expected_ident = model.Identity(
        model.IDENTITY_SERVICE, model.get_replication_state().primary_id)
    if api.get_current_identity() != expected_ident:
      self.send_error(replication_pb2.ReplicationPushResponse.FORBIDDEN)
      return

    # Check the signature headers are present.
    key_name = self.request.headers.get('X-AuthDB-SigKey-v1')
    sign = self.request.headers.get('X-AuthDB-SigVal-v1')
    if not key_name or not sign:
      self.send_error(replication_pb2.ReplicationPushResponse.MISSING_SIGNATURE)
      return

    # Verify the signature.
    body = self.request.body
    sign = base64.b64decode(sign)
    if not replication.is_signed_by_primary(body, key_name, sign):
      self.send_error(replication_pb2.ReplicationPushResponse.BAD_SIGNATURE)
      return

    # Deserialize the request, check it is valid.
    request = replication_pb2.ReplicationPushRequest.FromString(body)
    if not request.HasField('revision') or not request.HasField('auth_db'):
      self.send_error(replication_pb2.ReplicationPushResponse.BAD_REQUEST)
      return

    # Handle it.
    logging.info('Received AuthDB push: rev %d', request.revision.auth_db_rev)
    applied, state = replication.push_auth_db(request.revision, request.auth_db)
    logging.info(
        'AuthDB push %s: rev is %d',
        'applied' if applied else 'skipped', state.auth_db_rev)

    # Send the response.
    response = replication_pb2.ReplicationPushResponse()
    if applied:
      response.status = replication_pb2.ReplicationPushResponse.APPLIED
    else:
      response.status = replication_pb2.ReplicationPushResponse.SKIPPED
    response.current_revision.primary_id = state.primary_id
    response.current_revision.auth_db_rev = state.auth_db_rev
    response.current_revision.modified_ts = utils.datetime_to_timestamp(
        state.modified_ts)
    self.send_response(response)


class CertificatesHandler(handler.ApiHandler):
  """Public certificates that service uses to sign blobs.

  May be used by other services when validating a signature of this service.
  Used by signature.get_service_public_certificates() method.
  """

  # Available to anyone, there's no secrets here.
  @api.public
  def get(self):
    self.send_response(signature.get_own_public_certificates())


class OAuthConfigHandler(handler.ApiHandler):
  """Returns client_id and client_secret to use for OAuth2 login on a client.

  GET is available in Standalone, Primary and Replica modes.
  POST is available only in Standalone and Primary modes.
  """

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

    # Grab URL of a primary service if running as a replica.
    replication_state = model.get_replication_state()
    primary_url = replication_state.primary_url if replication_state else None

    self.send_response({
      'additional_client_ids': additional_ids,
      'client_id': client_id,
      'client_not_so_secret': client_secret,
      'primary_url': primary_url,
    })

  @forbid_api_on_replica
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


class ServerStateHandler(handler.ApiHandler):
  """Reports replication state of a service."""

  @api.require(api.is_admin)
  def get(self):
    if model.is_primary():
      mode = 'primary'
    elif model.is_replica():
      mode = 'replica'
    else:
      assert model.is_standalone()
      mode = 'standalone'
    state = model.get_replication_state() or model.AuthReplicationState()
    self.send_response({
      'mode': mode,
      'replication_state': state.to_serializable_dict(),
    })
