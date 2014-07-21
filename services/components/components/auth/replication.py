# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Replica side of Primary <-> Replica protocol.

Also includes common code used by both Replica and Primary.
"""

import collections

from google.appengine.api import app_identity
from google.appengine.api import urlfetch
from google.appengine.ext import ndb

from components import utils

from . import model
from . import tokens
from .proto import replication_pb2


# Messages for error codes in ServiceLinkResponse.
LINKING_ERRORS = {
  replication_pb2.ServiceLinkResponse.TRANSPORT_ERROR: 'Transport error,',
  replication_pb2.ServiceLinkResponse.BAD_TICKET: 'The link has expired.',
  replication_pb2.ServiceLinkResponse.AUTH_ERROR: 'Authentication error.',
}


# Returned by new_auth_db_snapshot.
AuthDBSnapshot = collections.namedtuple(
    'AuthDBSnapshot', 'global_config, groups, secrets')


class ProtocolError(Exception):
  """Raised when request to primary fails."""
  def __init__(self, status_code, msg):
    super(ProtocolError, self).__init__(msg)
    self.status_code = status_code


def decode_link_ticket(encoded):
  """Returns replication_pb2.ServiceLinkTicket given base64 encoded blob."""
  return replication_pb2.ServiceLinkTicket.FromString(
      tokens.base64_decode(encoded))


def become_replica(ticket, initiated_by):
  """Converts current service to a replica of a primary specified in a ticket.

  Args:
    ticket: replication_pb2.ServiceLinkTicket passed from a primary.
    initiated_by: Identity of a user that accepted linking request, for logging.

  Raises:
    ProtocolError in case the request to primary fails.
  """
  assert model.is_standalone()

  # On dev appserver emulate X-Appengine-Inbound-Appid header.
  headers = {'Content-Type': 'application/octet-stream'}
  protocol = 'https'
  if utils.is_local_dev_server():
    headers['X-Appengine-Inbound-Appid'] = app_identity.get_application_id()
    protocol = 'http'

  # Pass back the ticket for primary to verify it, tell the primary to use
  # default version hostname to talk to us.
  link_request = replication_pb2.ServiceLinkRequest()
  link_request.ticket = ticket.ticket
  link_request.replica_url = (
      '%s://%s' % (protocol, app_identity.get_default_version_hostname()))
  link_request.initiated_by = initiated_by.to_bytes()

  # Primary will look at X-Appengine-Inbound-Appid and compare it to what's in
  # the ticket.
  try:
    result = urlfetch.fetch(
        url='%s/auth_service/api/v1/internal/link_replica' % ticket.primary_url,
        payload=link_request.SerializeToString(),
        method='POST',
        headers=headers,
        follow_redirects=False,
        deadline=30,
        validate_certificate=True)
  except urlfetch.Error as exc:
    raise ProtocolError(
        replication_pb2.ServiceLinkResponse.TRANSPORT_ERROR,
        'URLFetch error (%s): %s' % (exc.__class__.__name__, exc))

  # Protobuf based protocol is not using HTTP codes (handler always replies with
  # HTTP 200, providing error details if needed in protobuf serialized body).
  # So any other status code here means there was a transport level error.
  if result.status_code != 200:
    raise ProtocolError(
        replication_pb2.ServiceLinkResponse.TRANSPORT_ERROR,
        'Request to the primary failed with HTTP %d.' % result.status_code)

  link_response = replication_pb2.ServiceLinkResponse.FromString(result.content)
  if link_response.status != replication_pb2.ServiceLinkResponse.SUCCESS:
    message = LINKING_ERRORS.get(
        link_response.status,
        'Request to the primary failed with status %d.' % link_response.status)
    raise ProtocolError(link_response.status, message)

  # TODO(vadimsh): Enable this once replication mechanism is implemented.
  # Become replica. Auth DB will be overwritten on a first push from Primary.
  # state = model.AuthReplicationState(
  #     key=model.REPLICATION_STATE_KEY,
  #     primary_id=ticket.primary_id,
  #     primary_url=ticket.primary_url)
  # state.put()


@ndb.transactional
def new_auth_db_snapshot():
  """Makes a consistent snapshot of replicated subset of AuthDB entities.

  Returns:
    Tuple (AuthReplicationState, AuthDBSnapshot).
  """
  state = model.REPLICATION_STATE_KEY.get_async()
  global_config = model.ROOT_KEY.get_async()
  groups = model.AuthGroup.query(ancestor=model.ROOT_KEY).fetch_async()
  secrets = model.AuthSecret.query(
      ancestor=model.secret_scope_key('global')).fetch_async()
  snapshot = AuthDBSnapshot(
      global_config.get_result() or model.AuthGlobalConfig(key=model.ROOT_KEY),
      groups.get_result(),
      secrets.get_result())
  return state.get_result(), snapshot


def auth_db_snapshot_to_proto(snapshot, auth_db_proto=None):
  """Writes AuthDBSnapshot into replication_pb2.AuthDB message.

  Args:
    snapshot: instance of AuthDBSnapshot with entities to convert to protobuf.
    auth_db_proto: optional instance of replication_pb2.AuthDB to update.

  Returns:
    Instance of replication_pb2.AuthDB (same as |auth_db_proto| if passed).
  """
  auth_db_proto = auth_db_proto or replication_pb2.AuthDB()

  auth_db_proto.oauth_client_id = snapshot.global_config.oauth_client_id or ''
  auth_db_proto.oauth_client_secret = (
      snapshot.global_config.oauth_client_secret or '')
  if snapshot.global_config.oauth_additional_client_ids:
    auth_db_proto.oauth_additional_client_ids.extend(
        snapshot.global_config.oauth_additional_client_ids)

  for ent in snapshot.groups:
    msg = auth_db_proto.groups.add()
    msg.name = ent.key.id()
    msg.members.extend(ident.to_bytes() for ident in ent.members)
    msg.globs.extend(glob.to_bytes() for glob in ent.globs)
    msg.nested.extend(ent.nested)
    msg.description = ent.description
    msg.created_ts = utils.datetime_to_timestamp(ent.created_ts)
    msg.created_by = ent.created_by.to_bytes()
    msg.modified_ts = utils.datetime_to_timestamp(ent.modified_ts)
    msg.modified_by = ent.modified_by.to_bytes()

  for ent in snapshot.secrets:
    msg = auth_db_proto.secrets.add()
    msg.name = ent.key.id()
    msg.values.extend(ent.values)
    msg.modified_ts = utils.datetime_to_timestamp(ent.modified_ts)
    msg.modified_by = ent.modified_by.to_bytes()

  return auth_db_proto


def proto_to_auth_db_snapshot(auth_db_proto):
  """Given replication_pb2.AuthDB message returns AuthDBSnapshot."""
  # Explicit conversion to 'list' is needed here since protobuf magic doesn't
  # stack with NDB magic.
  global_config = model.AuthGlobalConfig(
      key=model.ROOT_KEY,
      oauth_client_id=auth_db_proto.oauth_client_id,
      oauth_client_secret=auth_db_proto.oauth_client_secret,
      oauth_additional_client_ids=list(
          auth_db_proto.oauth_additional_client_ids))

  groups = [
    model.AuthGroup(
        key=model.group_key(msg.name),
        members=[model.Identity.from_bytes(x) for x in msg.members],
        globs=[model.IdentityGlob.from_bytes(x) for x in msg.globs],
        nested=list(msg.nested),
        description=msg.description,
        created_ts=utils.timestamp_to_datetime(msg.created_ts),
        created_by=model.Identity.from_bytes(msg.created_by),
        modified_ts=utils.timestamp_to_datetime(msg.modified_ts),
        modified_by=model.Identity.from_bytes(msg.modified_by))
    for msg in auth_db_proto.groups
  ]

  secrets = [
    model.AuthSecret(
        id=msg.name,
        parent=model.secret_scope_key('global'),
        values=list(msg.values),
        modified_ts=utils.timestamp_to_datetime(msg.modified_ts),
        modified_by=model.Identity.from_bytes(msg.modified_by))
    for msg in auth_db_proto.secrets
  ]

  return AuthDBSnapshot(global_config, groups, secrets)


def get_changed_entities(new_entity_list, old_entity_list):
  """Returns subset of changed entites.

  Compares entites from |new_entity_list| with entities from |old_entity_list|
  with same key, returns all changed or added entities.
  """
  old_by_key = {x.key: x for x in old_entity_list}
  new_or_changed = []
  for new_entity in new_entity_list:
    old_entity = old_by_key.get(new_entity.key)
    if not old_entity or old_entity.to_dict() != new_entity.to_dict():
      new_or_changed.append(new_entity)
  return new_or_changed


def get_deleted_keys(new_entity_list, old_entity_list):
  """Returns list of keys of entities that were removed."""
  new_by_key = frozenset(x.key for x in new_entity_list)
  return [old.key for old in old_entity_list if old.key not in new_by_key]


def replace_auth_db(auth_db_rev, modified_ts, snapshot):
  """Replaces AuthDB in datastore if it's older than |auth_db_rev|.

  May return False in case of race conditions (i.e. if some other concurrent
  process happened to update AuthDB earlier). May be retried in that case.

  Args:
    auth_db_rev: revision number of |snapshot|.
    modified_ts: datetime timestamp of when |auth_db_rev| was created.
    snapshot: AuthDBSnapshot with entity to store.

  Returns:
    Tuple (True if update was applied, current AuthReplicationState value).
  """
  assert model.is_replica()
  assert all(
      secret.key.parent() == model.secret_scope_key('global')
      for secret in snapshot.secrets), 'Only global secrets can be replaced'

  # Quickly check current auth_db rev before doing heavy calls.
  current_state = model.get_replication_state()
  if current_state.auth_db_rev >= auth_db_rev:
    return False, current_state

  # Make a snapshot of existing state of AuthDB to figure out what to change.
  current_state, current = new_auth_db_snapshot()

  # Entities that needs to be updated or created.
  entites_to_put = []
  if snapshot.global_config.to_dict() != current.global_config.to_dict():
    entites_to_put.append(snapshot.global_config)
  entites_to_put.extend(get_changed_entities(snapshot.groups, current.groups))
  entites_to_put.extend(get_changed_entities(snapshot.secrets, current.secrets))

  # Keys of entities that needs to be removed.
  keys_to_delete = []
  keys_to_delete.extend(get_deleted_keys(snapshot.groups, current.groups))
  keys_to_delete.extend(get_deleted_keys(snapshot.secrets, current.secrets))

  @ndb.transactional
  def update_auth_db():
    # AuthDB changed since 'new_auth_db_snapshot' transaction? Back off.
    state = model.get_replication_state()
    if state.auth_db_rev != current_state.auth_db_rev:
      return False, state

    # Update auth_db_rev in AuthReplicationState.
    state.auth_db_rev = auth_db_rev
    state.modified_ts = modified_ts

    # Apply changes.
    futures = []
    futures.extend(ndb.put_multi_async([state] + entites_to_put))
    futures.extend(ndb.delete_multi_async(keys_to_delete))

    # Wait for all pending futures to complete. Aborting the transaction with
    # outstanding futures is a bad idea (ndb complains in log about that).
    ndb.Future.wait_all(futures)

    # Raise an exception, if any.
    for future in futures:
      future.check_success()

    # Success.
    return True, state

  # Do the transactional update.
  return update_auth_db()
