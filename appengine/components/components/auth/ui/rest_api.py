# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Auth management REST API."""

import base64
import functools
import logging
import textwrap
import urllib
import webapp2

from google.appengine.api import app_identity
from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from components import utils

from . import acl
from .. import api
from .. import change_log
from .. import handler
from .. import host_token
from .. import ipaddr
from .. import model
from .. import replication
from .. import signature
from .. import version
from ..proto import replication_pb2


# Set by set_config_locked.
_is_config_locked_cb = None


def get_rest_api_routes():
  """Return a list of webapp2 routes with auth REST API handlers."""
  assert model.GROUP_NAME_RE.pattern[0] == '^'
  group_re = model.GROUP_NAME_RE.pattern[1:]
  assert model.IP_WHITELIST_NAME_RE.pattern[0] == '^'
  ip_whitelist_re = model.IP_WHITELIST_NAME_RE.pattern[1:]
  return [
    webapp2.Route('/auth/api/v1/accounts/self', SelfHandler),
    webapp2.Route('/auth/api/v1/accounts/self/xsrf_token', XSRFHandler),
    webapp2.Route('/auth/api/v1/change_log', ChangeLogHandler),
    webapp2.Route('/auth/api/v1/groups', GroupsHandler),
    webapp2.Route('/auth/api/v1/groups/<name:%s>' % group_re, GroupHandler),
    webapp2.Route('/auth/api/v1/host_token', HostTokenHandler),
    webapp2.Route('/auth/api/v1/internal/replication', ReplicationHandler),
    webapp2.Route('/auth/api/v1/ip_whitelists', IPWhitelistsHandler),
    webapp2.Route(
        '/auth/api/v1/ip_whitelists/<name:%s>' % ip_whitelist_re,
        IPWhitelistHandler),
    webapp2.Route('/auth/api/v1/server/certificates', CertificatesHandler),
    webapp2.Route('/auth/api/v1/server/info', ServerInfoHandler),
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


def is_config_locked():
  """Returns True to forbid configuration changing API calls.

  If is_config_locked returns True API requests that change configuration will
  return HTTP 409 error.

  A configuration is subset of AuthDB that changes infrequently:
  * OAuth client_id whitelist
  * IP whitelist

  Used by auth_service that utilizes config_service for config management.
  """
  return _is_config_locked_cb() if _is_config_locked_cb else False


def set_config_locked(locked_callback):
  """Sets a function that returns True if configuration is locked."""
  global _is_config_locked_cb
  _is_config_locked_cb = locked_callback


class EntityOperationError(Exception):
  """Raised by do_* methods in EntityHandlerBase to indicate a conflict."""
  def __init__(self, message, details=None):
    super(EntityOperationError, self).__init__(message)
    self.message = message
    self.details = details


class EntityHandlerBase(handler.ApiHandler):
  """Handler for creating, reading, updating and deleting an entity in AuthDB.

  Implements optimistic concurrency control based on Last-Modified header.

  Subclasses must override class methods (see below). Entities being manipulated
  should implement datastore_utils.SerializableModelMixin and
  model.AuthVersionedEntityMixin and have following properties:
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

  def check_preconditions(self):
    """Called after initial has_access checks, but before actual handling."""

  @classmethod
  def get_entity_key(cls, name):
    """Returns ndb.Key corresponding to entity with given name."""
    raise NotImplementedError()

  @classmethod
  def is_entity_writable(cls, _name):
    """Returns True if entity can be modified (e.g. not an external group)."""
    return True

  @classmethod
  def entity_to_dict(cls, entity):
    """Converts an entity to a serializable dictionary."""
    return entity.to_serializable_dict(with_id_as='name')

  @classmethod
  def can_create(cls):
    """True if caller is allowed to create a new entity."""
    return api.is_admin()

  @classmethod
  def do_create(cls, entity):
    """Called in transaction to validate and put a new entity.

    Raises:
      EntityOperationError in case of a conflict.
    """
    raise NotImplementedError()

  @classmethod
  def can_update(cls, entity):  # pylint: disable=unused-argument
    """True if caller is allowed to update a given entity."""
    return api.is_admin()

  @classmethod
  def do_update(cls, entity, params):
    """Called in transaction to update existing entity.

    Raises:
      EntityOperationError in case of a conflict.
    """
    raise NotImplementedError()

  @classmethod
  def can_delete(cls, entity):  # pylint: disable=unused-argument
    """True if caller is allowed to delete a given entity."""
    return api.is_admin()

  @classmethod
  def do_delete(cls, entity):
    """Called in transaction to delete existing entity.

    Raises:
      EntityOperationError in case of a conflict.
    """
    raise NotImplementedError()

  # Actual handlers implemented in terms of do_* calls.

  @api.require(acl.has_access)
  def get(self, name):
    """Fetches entity give its name."""
    self.check_preconditions()
    obj = self.get_entity_key(name).get()
    if not obj:
      self.abort_with_error(404, text='No such %s' % self.entity_kind_title)
    self.send_response(
        response={self.entity_kind_name: self.entity_to_dict(obj)},
        headers={'Last-Modified': utils.datetime_to_rfc2822(obj.modified_ts)})

  @forbid_api_on_replica
  @api.require(acl.has_access)
  def post(self, name):
    """Creates a new entity, ensuring it's indeed new (no overwrites)."""
    self.check_preconditions()
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
          created_ts=utils.utcnow(),
          created_by=api.get_current_identity())
    except (TypeError, ValueError) as e:
      self.abort_with_error(400, text=str(e))

    # No need to enter a transaction (like in do_update) to check this.
    if not self.can_create():
      raise api.AuthorizationError(
          '"%s" has no permission to create a %s' %
          (api.get_current_identity().to_bytes(), self.entity_kind_title))

    @ndb.transactional
    def create(entity):
      if entity.key.get():
        return False, {
          'http_code': 409,
          'text': 'Such %s already exists' % self.entity_kind_title,
        }
      entity.record_revision(
          modified_by=api.get_current_identity(),
          modified_ts=utils.utcnow(),
          comment='REST API')
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
      model.replicate_auth_db()
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
  @api.require(acl.has_access)
  def put(self, name):
    """Updates an existing entity."""
    self.check_preconditions()
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
    def update(params, expected_ts):
      entity = self.get_entity_key(name).get()
      if not entity:
        return None, None, {
          'http_code': 404,
          'text': 'No such %s' % self.entity_kind_title,
        }
      if (expected_ts and
          utils.datetime_to_rfc2822(entity.modified_ts) != expected_ts):
        return None, None, {
          'http_code': 412,
          'text':
              '%s was modified by someone else' %
              self.entity_kind_title.capitalize(),
        }
      if not self.can_update(entity):
        # Raising from inside a transaction produces ugly logs. Just return the
        # exception to be raised outside.
        ident = api.get_current_identity()
        exc = api.AuthorizationError(
            '"%s" has no permission to update %s "%s"' %
            (ident.to_bytes(), self.entity_kind_title, name))
        return None, exc, None
      entity.record_revision(
          modified_by=api.get_current_identity(),
          modified_ts=utils.utcnow(),
          comment='REST API')
      try:
        self.do_update(entity, params)
      except EntityOperationError as exc:
        return None, None, {
          'http_code': 409,
          'text': exc.message,
          'details': exc.details,
        }
      except ValueError as exc:
        return None, None, {
          'http_code': 400,
          'text': str(exc),
        }
      model.replicate_auth_db()
      return entity, None, None

    entity, exc, error_details = update(
        entity_params, self.request.headers.get('If-Unmodified-Since'))
    if exc:
      raise exc  # pylint: disable=raising-bad-type
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
  @api.require(acl.has_access)
  def delete(self, name):
    """Deletes an entity."""
    self.check_preconditions()
    if not self.is_entity_writable(name):
      self.abort_with_error(
          400, text='This %s is not writable' % self.entity_kind_title)

    @ndb.transactional
    def delete(expected_ts):
      entity = self.get_entity_key(name).get()
      if not entity:
        if expected_ts:
          return None, {
            'http_code': 412,
            'text':
                '%s was deleted by someone else' %
                self.entity_kind_title.capitalize(),
          }
        else:
          # Unconditionally deleting it, and it's already gone -> success.
          return None, None
      if (expected_ts and
          utils.datetime_to_rfc2822(entity.modified_ts) != expected_ts):
        return None, {
          'http_code': 412,
          'text':
              '%s was modified by someone else' %
              self.entity_kind_title.capitalize(),
        }
      if not self.can_delete(entity):
        # Raising from inside a transaction produces ugly logs. Just return the
        # exception to be raised outside.
        ident = api.get_current_identity()
        exc = api.AuthorizationError(
            '"%s" has no permission to delete %s "%s"' %
            (ident.to_bytes(), self.entity_kind_title, name))
        return exc, None
      entity.record_deletion(
          modified_by=api.get_current_identity(),
          modified_ts=utils.utcnow(),
          comment='REST API')
      try:
        self.do_delete(entity)
      except EntityOperationError as exc:
        return None, {
          'http_code': 409,
          'text': exc.message,
          'details': exc.details,
        }
      model.replicate_auth_db()
      return None, None

    exc, error_details = delete(self.request.headers.get('If-Unmodified-Since'))
    if exc:
      raise exc  # pylint: disable=raising-bad-type
    if error_details:
      self.abort_with_error(**error_details)
    self.send_response({'ok': True})


class SelfHandler(handler.ApiHandler):
  """Returns identity of a caller and authentication related request properties.

  Available in Standalone, Primary and Replica modes.
  """

  @api.public
  def get(self):
    self.send_response({
      'host': api.get_peer_host(),
      'identity': api.get_current_identity().to_bytes(),
      'ip': ipaddr.ip_to_string(api.get_peer_ip()),
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
    token = self.generate_xsrf_token()
    self.send_response(
        {
          'expiration_sec': handler.XSRFToken.expiration_sec,
          'xsrf_token': token,
        })


class ChangeLogHandler(handler.ApiHandler):
  """Returns AuthDBChange log entries matching some query.

  Supported query parameters (with example):
    target='AuthGroup$A group' - limit changes to given target only (if given).
    auth_db_rev=123 - limit changes to given revision only (if given).
    limit=50 - how many changes to return in a single page (default: 50).
    cursor=.... - urlsafe datastore cursor for pagination.
  """

  # The list of indexes here is synchronized with auth_service/index.yaml.
  NEED_INDEX_ERROR_MESSAGE = textwrap.dedent(r"""
  Your GAE app doesn't have indexes required for "Change log" functionality.

  If you need this feature, add following indexes to index.yaml. You can do it
  any time: changes are collected, they are just not queriable until indexed.

  - kind: AuthDBChange
    ancestor: yes
    properties:
    - name: target
    - name: __key__
      direction: desc

  - kind: AuthDBChange
    ancestor: yes
    properties:
    - name: __key__
      direction: desc
  """).strip()

  @forbid_api_on_replica
  @api.require(acl.has_access)
  def get(self):
    target = self.request.get('target')
    if target and not change_log.TARGET_RE.match(target):
      self.abort_with_error(400, text='Invalid \'target\' param')

    auth_db_rev = self.request.get('auth_db_rev')
    if auth_db_rev:
      try:
        auth_db_rev = int(auth_db_rev)
        if auth_db_rev <= 0:
          raise ValueError('Outside of allowed range')
      except ValueError as exc:
        self.abort_with_error(
            400, text='Invalid \'auth_db_rev\' param: %s' % exc)

    try:
      limit = int(self.request.get('limit', 50))
      if limit <= 0 or limit > 1000:
        raise ValueError('Outside of allowed range')
    except ValueError as exc:
      self.abort_with_error(400, text='Invalid \'limit\' param: %s' % exc)

    try:
      cursor = datastore_query.Cursor(urlsafe=self.request.get('cursor'))
    except (datastore_errors.BadValueError, ValueError) as exc:
      self.abort_with_error(400, text='Invalid \'cursor\' param: %s' % exc)

    q = change_log.make_change_log_query(target=target, auth_db_rev=auth_db_rev)
    try:
      changes, cursor, more = q.fetch_page(limit, start_cursor=cursor)
    except datastore_errors.NeedIndexError:
      # This is expected for users of components.auth that did not update
      # index.yaml. Return a friendlier message pointing them to instructions.
      self.abort_with_error(500, text=self.NEED_INDEX_ERROR_MESSAGE)

    self.send_response({
      'changes': [c.to_jsonish() for c in changes],
      'cursor': cursor.urlsafe() if cursor and more else None,
    })


def caller_can_modify(group_dict):
  """True if given group (presented as dict) is modifiable by a caller."""
  if model.is_external_group_name(group_dict['name']):
    return False
  return api.is_admin() or api.is_group_member(group_dict['owners'])


class GroupsHandler(handler.ApiHandler):
  """Lists all registered groups.

  Returns a list of groups, sorted by name. Each entry in a list is a dict with
  all details about the group except the actual list of members
  (which may be large).

  Available in Standalone, Primary and Replica modes.
  """

  @staticmethod
  def cache_key(auth_db_rev):
    return 'api:v1:GroupsHandler/%d' % auth_db_rev

  @staticmethod
  def adjust_response_for_user(response):
    """Modifies response (in place) based on user ACLs."""
    for g in response['groups']:
      g['caller_can_modify'] = caller_can_modify(g)

  @api.require(acl.has_access)
  def get(self):
    # Try to find a cached response for the current revision.
    auth_db_rev = model.get_auth_db_revision()
    cached_response = memcache.get(self.cache_key(auth_db_rev))
    if cached_response is not None:
      self.adjust_response_for_user(cached_response)
      self.send_response(cached_response)
      return

    # Grab a list of groups and corresponding revision for cache key.
    def run():
      fut = model.AuthGroup.query(ancestor=model.root_key()).fetch_async()
      return model.get_auth_db_revision(), fut.get_result()
    auth_db_rev, group_list = ndb.transaction(run)

    # Currently AuthGroup entity contains a list of group members in the entity
    # body. It's an implementation detail that should not be relied upon.
    # Generally speaking, fetching a list of group members can be an expensive
    # operation, and group listing call shouldn't do it all the time. So throw
    # away all fields that enumerate group members.
    response = {
      'groups': [
        g.to_serializable_dict(
            with_id_as='name',
            exclude=('globs', 'members', 'nested'))
        for g in sorted(group_list, key=lambda x: x.key.string_id())
      ],
    }
    memcache.set(self.cache_key(auth_db_rev), response, time=24*3600)
    self.adjust_response_for_user(response)
    self.send_response(response)


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
  def entity_to_dict(cls, entity):
    g = super(GroupHandler, cls).entity_to_dict(entity)
    g['caller_can_modify'] = caller_can_modify(g)
    return g

  # Same as in the base class, repeated here just for clarity.
  @classmethod
  def can_create(cls):
    return api.is_admin()

  @classmethod
  def do_create(cls, entity):
    # Admin group is created during bootstrap, see ui.py BootstrapHandler.
    assert entity.key.id() != model.ADMIN_GROUP
    # Check that all references group (owning group, nested groups) exist. It is
    # ok for a new group to have itself as an owner.
    entity.owners = entity.owners or model.ADMIN_GROUP
    to_check = list(entity.nested)
    if entity.owners != entity.key.id() and entity.owners not in to_check:
      to_check.append(entity.owners)
    missing = model.get_missing_groups(to_check)
    if missing:
      raise EntityOperationError(
          message=
              'Some referenced groups don\'t exist: %s.' % ', '.join(missing),
          details={'missing': missing})
    entity.put()

  @classmethod
  def can_update(cls, entity):
    return api.is_admin() or api.is_group_member(entity.owners)

  @classmethod
  def do_update(cls, entity, params):
    # If changing an owner, ensure new owner exists. No need to do it if
    # the group owns itself (we know it exists).
    new_owners = params.get('owners', entity.owners)
    if new_owners != entity.owners and new_owners != entity.key.id():
      ent = model.group_key(new_owners).get()
      if not ent:
        raise EntityOperationError(
            message='Owners groups (%s) doesn\'t exist.' % new_owners,
            details={'missing': [new_owners]})
    # Admin group must be owned by itself.
    if entity.key.id() == model.ADMIN_GROUP and new_owners != model.ADMIN_GROUP:
      raise EntityOperationError(
          message='Can\'t change owner of \'%s\' group.' % model.ADMIN_GROUP)
    # If adding new nested groups, need to ensure they exist.
    added_nested_groups = None
    if 'nested' in params:
      added_nested_groups = set(params['nested']) - set(entity.nested)
      if added_nested_groups:
        missing = model.get_missing_groups(added_nested_groups)
        if missing:
          raise EntityOperationError(
              message=
                  'Some referenced groups don\'t exist: %s.'
                  % ', '.join(missing),
              details={'missing': missing})
    # Now make sure updated group is not a part of new group dependency cycle.
    entity.populate(**params)
    if added_nested_groups:
      cycle = model.find_group_dependency_cycle(entity)
      if cycle:
        # Make it clear that cycle starts from the group being modified.
        cycle = [entity.key.id()] + cycle
        as_str = ' -> '.join(cycle)
        raise EntityOperationError(
            message='Groups can not have cyclic dependencies: %s.' % as_str,
            details={'cycle': cycle})
    # Good enough.
    entity.put()

  @classmethod
  def can_delete(cls, entity):
    return api.is_admin() or api.is_group_member(entity.owners)

  @classmethod
  def do_delete(cls, entity):
    # Admin group is special, deleting it would be bad.
    if entity.key.id() == model.ADMIN_GROUP:
      raise EntityOperationError(
          message='Can\'t delete \'%s\' group.' % model.ADMIN_GROUP)
    # A group can be its own owner (but it can not "nest" itself, as checked by
    # find_group_dependency_cycle). It is OK to delete a self-owning group.
    referencing_groups = model.find_referencing_groups(entity.key.id())
    referencing_groups.discard(entity.key.id())
    if referencing_groups:
      grs = sorted(referencing_groups)
      raise EntityOperationError(
          message=(
              'This group is being referenced by other groups: %s.' %
                  ', '.join(grs)),
          details={'groups': grs})
    entity.key.delete()


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

  @api.require(acl.has_access)
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

  def check_preconditions(self):
    if self.request.method != 'GET' and is_config_locked():
      self.abort_with_error(409, text='The configuration is managed elsewhere')

  @classmethod
  def get_entity_key(cls, name):
    assert model.is_valid_ip_whitelist_name(name), name
    return model.ip_whitelist_key(name)

  @classmethod
  def do_create(cls, entity):
    entity.put()

  @classmethod
  def do_update(cls, entity, params):
    entity.populate(**params)
    entity.put()

  @classmethod
  def do_delete(cls, entity):
    # TODO(vadimsh): Verify it isn't being referenced by whitelist assigments.
    entity.key.delete()


class ServerInfoHandler(handler.ApiHandler):
  """Returns information about the service (app version, service account name).

  May be used by other services to know what account to add to ACLs.
  """

  # In 99% cases account name is guessable from appID anyway, so its fine to
  # have this public.
  @api.public
  def get(self):
    self.send_response({
      'app_id': app_identity.get_application_id(),
      'app_runtime': 'python27',
      'app_version': utils.get_app_version(),
      'service_account_name': app_identity.get_service_account_name(),
    })


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
  @api.require(api.is_admin)
  def post(self):
    if is_config_locked():
      self.abort_with_error(409, text='The configuration is managed elsewhere')

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
      config.record_revision(
          modified_by=api.get_current_identity(),
          modified_ts=utils.utcnow(),
          comment='REST API')
      config.put()
      model.replicate_auth_db()

    update()
    self.send_response({'ok': True})


class ServerStateHandler(handler.ApiHandler):
  """Reports replication state of a service."""

  @api.require(acl.has_access)
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
