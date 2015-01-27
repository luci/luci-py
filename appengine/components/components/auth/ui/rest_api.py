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
from .. import host_token
from .. import ipaddr
from .. import model
from .. import replication
from .. import signature
from .. import version
from ..proto import replication_pb2


def get_rest_api_routes():
  """Return a list of webapp2 routes with auth REST API handlers."""
  assert model.GROUP_NAME_RE.pattern[0] == '^'
  group_re = model.GROUP_NAME_RE.pattern[1:]
  assert model.IP_WHITELIST_NAME_RE.pattern[0] == '^'
  ip_whitelist_re = model.IP_WHITELIST_NAME_RE.pattern[1:]
  return [
    webapp2.Route('/auth/api/v1/accounts/self', SelfHandler),
    webapp2.Route('/auth/api/v1/accounts/self/xsrf_token', XSRFHandler),
    webapp2.Route('/auth/api/v1/groups', GroupsHandler),
    webapp2.Route('/auth/api/v1/groups/<name:%s>' % group_re, GroupHandler),
    webapp2.Route('/auth/api/v1/host_token', HostTokenHandler),
    webapp2.Route('/auth/api/v1/internal/replication', ReplicationHandler),
    webapp2.Route('/auth/api/v1/ip_whitelists', IPWhitelistsHandler),
    webapp2.Route(
        '/auth/api/v1/ip_whitelists/<name:%s>' % ip_whitelist_re,
        IPWhitelistHandler),
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


def has_read_access():
  """Returns True if current caller can read groups and other auth data.

  Used in @require(...) decorators of API handlers.
  """
  return api.is_admin() or api.is_group_member('groups-readonly-access')


def has_write_access():
  """Returns True if current caller can modify groups and other auth data.

  Used in @require(...) decorators of API handlers.
  """
  return api.is_admin()


class EntityOperationError(Exception):
  """Raised by do_* methods in EntityHandlerBase to indicate a conflict."""
  def __init__(self, message, details):
    super(EntityOperationError, self).__init__(message)
    self.message = message
    self.details = details


class EntityHandlerBase(handler.ApiHandler):
  """Handler for creating, reading, updating and deleting an entity in AuthDB.

  Implements optimistic concurrency control based on Last-Modified header.

  Subclasses must override class methods (see below). Entities being manipulated
  should implement datastore_utils.SerializableModelMixin and have following
  properties:
    created_by
    created_ts
    modified_by
    modified_ts

  GET handler is available in Standalone, Primary and Replica modes.

  Everything else is available only in Standalone and Primary modes.
  """

  # Root URL of this handler, e.g. '/auth/api/v1/groups/'.
  entity_url_prefix = None
  # Entity class being operated on, e.g. model.AuthGroup.
  entity_kind = None
  # Will show up as a key in response dicts, e.g. {<name>: ...}.
  entity_kind_name = None
  # Will show up in error messages, e.g. 'Failed to delete <title>'.
  entity_kind_title = None

  @classmethod
  def get_entity_key(cls, name):
    """Returns ndb.Key corresponding to entity with given name."""
    raise NotImplementedError()

  @classmethod
  def is_entity_writable(cls, _name):
    """Returns True to allow POST\PUT\DELETE."""
    return True

  @classmethod
  def do_create(cls, entity):
    """Called in transaction to validate and put a new entity.

    Raises:
      EntityOperationError in case of a conflict.
    """
    raise NotImplementedError()

  @classmethod
  def do_update(cls, entity, params, modified_by):
    """Called in transaction to update existing entity.

    Raises:
      EntityOperationError in case of a conflict.
    """
    raise NotImplementedError()

  @classmethod
  def do_delete(cls, entity):
    """Called in transaction to delete existing entity.

    Raises:
      EntityOperationError in case of a conflict.
    """
    raise NotImplementedError()

  # Actual handlers implemented in terms of do_* calls.

  @api.require(has_read_access)
  def get(self, name):
    """Fetches entity give its name."""
    obj = self.get_entity_key(name).get()
    if not obj:
      self.abort_with_error(404, text='No such %s' % self.entity_kind_title)
    self.send_response(
        response={
          self.entity_kind_name: obj.to_serializable_dict(with_id_as='name'),
        },
        headers={'Last-Modified': utils.datetime_to_rfc2822(obj.modified_ts)})

  @forbid_api_on_replica
  @api.require(has_write_access)
  def post(self, name):
    """Creates a new entity, ensuring it's indeed new (no overwrites)."""
    try:
      body = self.parse_body()
      name_in_body = body.pop('name', None)
      if not name_in_body or name_in_body != name:
        raise ValueError('Missing or mismatching name in request body')
      if not self.is_entity_writable(name):
        raise ValueError('This %s is not writable' % self.entity_kind_title)
      entity = self.entity_kind.from_serializable_dict(
          serializable_dict=body,
          key=self.get_entity_key(name),
          created_by=api.get_current_identity(),
          modified_by=api.get_current_identity())
    except (TypeError, ValueError) as e:
      self.abort_with_error(400, text=str(e))

    @ndb.transactional
    def create(entity):
      if entity.key.get():
        return False, {
          'http_code': 409,
          'text': 'Such %s already exists' % self.entity_kind_title,
        }
      try:
        self.do_create(entity)
      except EntityOperationError as exc:
        return False, {
          'http_code': 409,
          'text': exc.message,
          'details': exc.details,
        }
      except ValueError as exc:
        return False, {
          'http_code': 400,
          'text': str(exc),
        }
      return True, None

    success, error_details = create(entity)
    if not success:
      self.abort_with_error(**error_details)
    self.send_response(
        response={'ok': True},
        http_code=201,
        headers={
          'Last-Modified': utils.datetime_to_rfc2822(entity.modified_ts),
          'Location':
              '%s%s' % (self.entity_url_prefix, urllib.quote(entity.key.id())),
        }
    )

  @forbid_api_on_replica
  @api.require(has_write_access)
  def put(self, name):
    """Updates an existing entity."""
    try:
      body = self.parse_body()
      name_in_body = body.pop('name', None)
      if not name_in_body or name_in_body != name:
        raise ValueError('Missing or mismatching name in request body')
      if not self.is_entity_writable(name):
        raise ValueError('This %s is not writable' % self.entity_kind_title)
      entity_params = self.entity_kind.convert_serializable_dict(body)
    except (TypeError, ValueError) as e:
      self.abort_with_error(400, text=str(e))

    @ndb.transactional
    def update(params, modified_by, expected_ts):
      entity = self.get_entity_key(name).get()
      if not entity:
        return None, {
          'http_code': 404,
          'text': 'No such %s' % self.entity_kind_title,
        }
      if (expected_ts and
          utils.datetime_to_rfc2822(entity.modified_ts) != expected_ts):
        return None, {
          'http_code': 412,
          'text':
              '%s was modified by someone else' %
              self.entity_kind_title.capitalize(),
        }
      try:
        self.do_update(entity, params, modified_by)
      except EntityOperationError as exc:
        return None, {
          'http_code': 409,
          'text': exc.message,
          'details': exc.details,
        }
      except ValueError as exc:
        return False, {
          'http_code': 400,
          'text': str(exc),
        }
      return entity, None

    entity, error_details = update(
        entity_params,
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
  @api.require(has_write_access)
  def delete(self, name):
    """Deletes an entity."""
    if not self.is_entity_writable(name):
      self.abort_with_error(
          400, text='This %s is not writable' % self.entity_kind_title)

    @ndb.transactional
    def delete(expected_ts):
      entity = self.get_entity_key(name).get()
      if not entity:
        if expected_ts:
          return False, {
            'http_code': 412,
            'text':
                '%s was deleted by someone else' %
                self.entity_kind_title.capitalize(),
          }
        else:
          # Unconditionally deleting it, and it's already gone -> success.
          return True, None
      if (expected_ts and
          utils.datetime_to_rfc2822(entity.modified_ts) != expected_ts):
        return False, {
          'http_code': 412,
          'text':
              '%s was modified by someone else' %
              self.entity_kind_title.capitalize(),
        }
      try:
        self.do_delete(entity)
      except EntityOperationError as exc:
        return False, {
          'http_code': 409,
          'text': exc.message,
          'details': exc.details,
        }
      return True, None

    success, error_details = delete(
        self.request.headers.get('If-Unmodified-Since'))
    if not success:
      self.abort_with_error(**error_details)
    self.send_response({'ok': True})


class SelfHandler(handler.ApiHandler):
  """Returns identity of a caller and authentication related request properties.

  Available in Standalone, Primary and Replica modes.
  """

  @api.public
  def get(self):
    self.send_response({
      'host': api.get_current_identity_host(),
      'identity': api.get_current_identity().to_bytes(),
      'ip': ipaddr.ip_to_string(api.get_current_identity_ip()),
    })


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

  @api.require(has_read_access)
  def get(self):
    # Currently AuthGroup entity contains a list of group members in the entity
    # body. It's an implementation detail that should not be relied upon.
    # Generally speaking, fetching a list of group members can be an expensive
    # operation, and group listing call shouldn't do it all the time. So throw
    # away all fields that enumerate group members.
    group_list = model.AuthGroup.query(ancestor=model.root_key())
    self.send_response({
      'groups': [
        g.to_serializable_dict(
            with_id_as='name',
            exclude=('globs', 'members', 'nested'))
        for g in sorted(group_list, key=lambda x: x.key.string_id())
      ],
    })


class GroupHandler(EntityHandlerBase):
  """Creating, reading, updating and deleting a single group.

  GET is available in Standalone, Primary and Replica modes.
  Everything else is available only in Standalone and Primary modes.
  """
  entity_url_prefix = '/auth/api/v1/groups/'
  entity_kind = model.AuthGroup
  entity_kind_name = 'group'
  entity_kind_title = 'group'

  @classmethod
  def get_entity_key(cls, name):
    assert model.is_valid_group_name(name), name
    return model.group_key(name)

  @classmethod
  def is_entity_writable(cls, name):
    return not model.is_external_group_name(name)

  @classmethod
  def do_create(cls, entity):
    missing = model.get_missing_groups(entity.nested)
    if missing:
      raise EntityOperationError(
          message='Referencing a nested group that doesn\'t exist',
          details={'missing': missing})
    entity.put()
    model.replicate_auth_db()

  @classmethod
  def do_update(cls, entity, params, modified_by):
    # If adding new nested groups, need to ensure they exist.
    added_nested_groups = set(params['nested']) - set(entity.nested)
    if added_nested_groups:
      missing = model.get_missing_groups(added_nested_groups)
      if missing:
        raise EntityOperationError(
            message='Referencing a nested group that doesn\'t exist',
            details={'missing': missing})
    # Now make sure updated group is not a part of new group dependency cycle.
    entity.populate(**params)
    if added_nested_groups:
      cycle = model.find_group_dependency_cycle(entity)
      if cycle:
        raise EntityOperationError(
            message='Groups can not have cyclic dependencies',
            details={'cycle': cycle})
    # Good enough.
    entity.modified_ts = utils.utcnow()
    entity.modified_by = modified_by
    entity.put()
    model.replicate_auth_db()

  @classmethod
  def do_delete(cls, entity):
    referencing_groups = model.find_referencing_groups(entity.key.id())
    if referencing_groups:
      raise EntityOperationError(
          message='Group is being referenced in other groups',
          details={'groups': list(referencing_groups)})
    entity.key.delete()
    model.replicate_auth_db()


class HostTokenHandler(handler.ApiHandler):
  """Creates host tokens to put into X-Host-Token-V1 hander.

  See host_token.py for more info.

  Expected request format:
  {
    'host': 'host name to make a token for as string',
    'expiration_sec': <token lifetime in seconds as integer>
  }

  Response format:
  {
    'host_token': <base64 urlsafe host token>
  }
  """

  @forbid_api_on_replica
  @api.require(host_token.can_create_host_token)
  def post(self):
    body = self.parse_body()

    # Validate 'host'.
    if 'host' not in body:
      self.abort_with_error(400, text='Missing required \'host\' key')
    host = body['host']
    if not host_token.is_valid_host(host):
      self.abort_with_error(400, text='Invalid \'host\' value')

    # Validate 'expiration_sec'.
    if 'expiration_sec' not in body:
      self.abort_with_error(400, text='Missing required \'expiration_sec\' key')
    expiration_sec = body['expiration_sec']
    if not isinstance(expiration_sec, (int, long, float)):
      self.abort_with_error(400, text='\'expiration_sec\' should be number')
    expiration_sec = int(expiration_sec)
    if expiration_sec < 0:
      self.abort_with_error(400, text='\'expiration_sec\' can\'t be negative')

    token = host_token.create_host_token(host, expiration_sec)
    self.send_response({'host_token': token}, http_code=201)


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
    response.auth_code_version = version.__version__
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
    if request.HasField('auth_code_version'):
      logging.info(
          'Primary\'s auth component version: %s', request.auth_code_version)
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
    response.auth_code_version = version.__version__
    self.send_response(response)


class IPWhitelistsHandler(handler.ApiHandler):
  """Lists all IP whitelists.

  Available in Standalone, Primary and Replica modes. Replicas only have IP
  whitelists referenced in "account -> IP whitelist" mapping.
  """

  @api.require(has_read_access)
  def get(self):
    entities = model.AuthIPWhitelist.query(ancestor=model.root_key())
    self.send_response({
      'ip_whitelists': [
        e.to_serializable_dict(with_id_as='name')
        for e in sorted(entities, key=lambda x: x.key.id())
      ],
    })


class IPWhitelistHandler(EntityHandlerBase):
  """Creating, reading, updating and deleting a single IP whitelist.

  GET is available in Standalone, Primary and Replica modes.
  Everything else is available only in Standalone and Primary modes.
  """
  entity_url_prefix = '/auth/api/v1/ip_whitelists/'
  entity_kind = model.AuthIPWhitelist
  entity_kind_name = 'ip_whitelist'
  entity_kind_title = 'ip whitelist'

  @classmethod
  def get_entity_key(cls, name):
    assert model.is_valid_ip_whitelist_name(name), name
    return model.ip_whitelist_key(name)

  @classmethod
  def do_create(cls, entity):
    entity.put()
    model.replicate_auth_db()

  @classmethod
  def do_update(cls, entity, params, modified_by):
    entity.populate(**params)
    entity.modified_ts = utils.utcnow()
    entity.modified_by = modified_by
    entity.put()
    model.replicate_auth_db()

  @classmethod
  def do_delete(cls, entity):
    # TODO(vadimsh): Verify it isn't being referenced by whitelist assigments.
    entity.key.delete()
    model.replicate_auth_db()


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
      global_config = model.root_key().get()
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
  @api.require(has_write_access)
  def post(self):
    body = self.parse_body()
    client_id = body['client_id']
    client_secret = body['client_not_so_secret']
    additional_client_ids = filter(bool, body['additional_client_ids'])

    @ndb.transactional
    def update():
      config = model.root_key().get()
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

  @api.require(has_read_access)
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
      'auth_code_version': version.__version__,
      'mode': mode,
      'replication_state': state.to_serializable_dict(),
    })
