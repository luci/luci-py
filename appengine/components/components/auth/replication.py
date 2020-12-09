# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Replica side of Primary <-> Replica protocol.

Also includes common code used by both Replica and Primary.
"""

import collections
import cStringIO
import hashlib
import logging
import zlib

from google.appengine.api import app_identity
from google.appengine.api import urlfetch
from google.appengine.ext import ndb

from components import utils

from . import b64
from . import model
from . import realms
from . import signature
from .proto import replication_pb2


# Messages for error codes in ServiceLinkResponse.
LINKING_ERRORS = {
    replication_pb2.ServiceLinkResponse.TRANSPORT_ERROR: 'Transport error,',
    replication_pb2.ServiceLinkResponse.BAD_TICKET: 'The link has expired.',
    replication_pb2.ServiceLinkResponse.AUTH_ERROR: 'Authentication error.',
}


# Returned by new_auth_db_snapshot.
AuthDBSnapshot = collections.namedtuple(
    'AuthDBSnapshot',
    [
        'global_config',
        'groups',
        'ip_whitelists',
        'ip_whitelist_assignments',
        'realms_globals',
        'project_realms',
    ])


class ProtocolError(Exception):
  """Raised when request to primary fails."""
  def __init__(self, status_code, msg):
    super(ProtocolError, self).__init__(msg)
    self.status_code = status_code


def decode_link_ticket(encoded):
  """Returns replication_pb2.ServiceLinkTicket given base64 encoded blob."""
  return replication_pb2.ServiceLinkTicket.FromString(b64.decode(encoded))


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
  headers['X-URLFetch-Service-Id'] = utils.get_urlfetch_service_id()

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

  # Become replica. Auth DB will be overwritten on a first push from Primary.
  state = model.AuthReplicationState(
      key=model.replication_state_key(),
      primary_id=ticket.primary_id,
      primary_url=ticket.primary_url)
  state.put()


@ndb.transactional
def new_auth_db_snapshot():
  """Makes a consistent snapshot of replicated subset of AuthDB entities.

  Returns:
    Tuple (AuthReplicationState, AuthDBSnapshot).
  """
  # Start fetching stuff in parallel.
  state_future = model.replication_state_key().get_async()
  config_future = model.root_key().get_async()
  groups_future = model.AuthGroup.query(ancestor=model.root_key()).fetch_async()
  realms_globals_future = model.realms_globals_key().get_async()
  project_realms_future = model.AuthProjectRealms.query(
      ancestor=model.root_key()).fetch_async()

  # It's fine to block here as long as it's the last fetch.
  ip_whitelist_assignments, ip_whitelists = model.fetch_ip_whitelists()

  snapshot = AuthDBSnapshot(
      config_future.get_result() or model.AuthGlobalConfig(
          key=model.root_key()
      ),
      groups_future.get_result(),
      ip_whitelists,
      ip_whitelist_assignments,
      realms_globals_future.get_result() or model.AuthRealmsGlobals(
          key=model.realms_globals_key()
      ),
      project_realms_future.get_result(),
  )
  return state_future.get_result(), snapshot


def auth_db_snapshot_to_proto(snapshot, auth_db_proto=None):
  """Writes AuthDBSnapshot into replication_pb2.AuthDB message.

  Args:
    snapshot: instance of AuthDBSnapshot with entities to convert to protobuf.
    auth_db_proto: optional instance of replication_pb2.AuthDB to update.

  Returns:
    Instance of replication_pb2.AuthDB (same as |auth_db_proto| if passed).
  """
  auth_db_proto = auth_db_proto or replication_pb2.AuthDB()

  # Many fields in auth_db_proto were once 'required' and many clients still
  # expect them to be populated. Unfortunately setting a proto3 string field
  # to '' doesn't mark it as "set" from proto2 perspective. So inject some
  # sentinel values instead.
  #
  # TODO(vadimsh): Remove this hack when all services (including Gerrit plugin)
  # are switched to use proto3.
  auth_db_proto.oauth_client_id = (
      snapshot.global_config.oauth_client_id or 'empty')
  auth_db_proto.oauth_client_secret = (
      snapshot.global_config.oauth_client_secret or 'empty')
  if snapshot.global_config.oauth_additional_client_ids:
    auth_db_proto.oauth_additional_client_ids.extend(
        snapshot.global_config.oauth_additional_client_ids)

  auth_db_proto.token_server_url = (
      snapshot.global_config.token_server_url or 'empty')
  auth_db_proto.security_config = (
      snapshot.global_config.security_config or 'empty')

  for ent in snapshot.groups:
    msg = auth_db_proto.groups.add()
    msg.name = ent.key.id()
    msg.members.extend(ident.to_bytes() for ident in ent.members)
    msg.globs.extend(glob.to_bytes() for glob in ent.globs)
    msg.nested.extend(ent.nested)
    msg.description = ent.description or 'empty'
    msg.created_ts = utils.datetime_to_timestamp(ent.created_ts)
    msg.created_by = ent.created_by.to_bytes()
    msg.modified_ts = utils.datetime_to_timestamp(ent.modified_ts)
    msg.modified_by = ent.modified_by.to_bytes()
    msg.owners = ent.owners

  for ent in snapshot.ip_whitelists:
    msg = auth_db_proto.ip_whitelists.add()
    msg.name = ent.key.id()
    msg.subnets.extend(ent.subnets)
    msg.description = ent.description or 'empty'
    msg.created_ts = utils.datetime_to_timestamp(ent.created_ts)
    msg.created_by = ent.created_by.to_bytes()
    msg.modified_ts = utils.datetime_to_timestamp(ent.modified_ts)
    msg.modified_by = ent.modified_by.to_bytes()

  for ent in snapshot.ip_whitelist_assignments.assignments:
    msg = auth_db_proto.ip_whitelist_assignments.add()
    msg.identity = ent.identity.to_bytes()
    msg.ip_whitelist = ent.ip_whitelist
    msg.comment = ent.comment or 'empty'
    msg.created_ts = utils.datetime_to_timestamp(ent.created_ts)
    msg.created_by = ent.created_by.to_bytes()

  # Merge all per-project realms into a single realms_pb2.Realms. There are some
  # space savings there due to dedupping permissions lists.
  realms.merge(
      snapshot.realms_globals.permissions,
      {r.key.id(): r.realms for r in snapshot.project_realms},
      auth_db_proto.realms)

  return auth_db_proto


def is_signed_by_primary(blob, key_name, sig):
  """Verifies that |blob| was signed by Primary."""
  # Assert that running on Replica.
  state = model.get_replication_state()
  assert state and state.primary_url, state
  # Grab the cert from primary and verify the signature. We are signing SHA512
  # hashes, since AuthDB blob is too large.
  certs = signature.get_service_public_certificates(state.primary_url)
  digest = hashlib.sha512(blob).digest()
  return certs.check_signature(digest, key_name, sig)


def push_auth_db(revision, auth_db):
  """Accepts AuthDB push from Primary and applies it to the replica.

  TODO(vadimsh): Delete all AuthGroup etc. entities on replicas.

  Args:
    revision: replication_pb2.AuthDBRevision describing revision of pushed DB.
    auth_db: replication_pb2.AuthDB with pushed DB.

  Returns:
    Tuple (True if update was applied, stored or updated AuthReplicationState).
  """
  # Already up-to-date? Check it first before doing heavy calls.
  state = model.get_replication_state()
  if (state.primary_id != revision.primary_id or
      state.auth_db_rev >= revision.auth_db_rev):
    return False, state

  # Zero revision in AuthReplicationState is special, it means "waiting for the
  # first AuthDB push". The Primary must never try to send it.
  assert revision.auth_db_rev > 0

  # Store AuthDB message as is first by deflating it and splitting it up into
  # multiple AuthDBSnapshotShard messages. If it fails midway, no big deal. It
  # will just hang in the datastore as unreferenced garbage.
  shard_ids = store_sharded_auth_db(
      auth_db,
      state.primary_url,
      revision.auth_db_rev,
      512*1024)

  # Put shard IDs into AuthReplicationState, they are used in api.fetch_auth_db.
  @ndb.transactional
  def update_replication_state():
    state = model.get_replication_state()
    if (state.primary_id != revision.primary_id or
        state.auth_db_rev >= revision.auth_db_rev):
      return False, state  # already up-to-date or newer
    state.auth_db_rev = revision.auth_db_rev
    state.modified_ts = utils.timestamp_to_datetime(revision.modified_ts)
    state.shard_ids = shard_ids
    state.put()
    return True, state

  return update_replication_state()


@ndb.transactional
def store_sharded_auth_db(auth_db, primary_url, auth_db_rev, shard_size):
  """Creates a bunch of AuthDBSnapshotShard entities with deflated AuthDB.

  Runs transactionally to avoid updating the same entity group from multiple
  RPCs at once (instead there'll be only one transaction that touches it).

  Stores shards sequentially to avoid making a bunch of memory-hungry RPCs in
  parallel in already memory-constrained (but not performance critical) code
  path.

  Uses relatively low compression level to make decompression in performance
  critical 'load_sharded_auth_db' fast.

  Args:
    auth_db: replication_pb2.AuthDB with pushed DB.
    primary_url: URL of the primary auth service that pushed the DB.
    auth_db_rev: revision of AuthDB being pushed.
    shard_size: size in bytes of each shard.

  Returns:
    List of IDs of created AuthDBSnapshotShard entities.
  """
  ids = []
  blob = zlib.compress(auth_db.SerializeToString(), 3)
  while blob:
    shard, blob = blob[:shard_size], blob[shard_size:]
    ids.append(hashlib.sha256(shard).hexdigest()[:16])
    model.AuthDBSnapshotShard(
        key=model.snapshot_shard_key(primary_url, auth_db_rev, ids[-1]),
        blob=shard,
    ).put()
  return ids


@ndb.non_transactional
def load_sharded_auth_db(primary_url, auth_db_rev, shard_ids):
  """Reconstructs replication_pb2.AuthDB proto from shards in datastore.

  Runs in performance critical and memory-constrained code path.

  Logs an error and returns None if some shard is missing. Any other unexpected
  errors are raised as corresponding exceptions (there should be none).

  Args:
    primary_url: URL of the primary auth service that pushed the DB.
    auth_db_rev: revision of AuthDB to fetch.
    shard_ids: IDs of AuthDBSnapshotShard entities, must be non-empty.

  Returns:
    replication_pb2.AuthDB message.
  """
  shards = ndb.get_multi(
      model.snapshot_shard_key(primary_url, auth_db_rev, shard_id)
      for shard_id in shard_ids)

  missing = [sid for sid, shard in zip(shard_ids, shards) if not shard]
  if missing:
    logging.error(
        'Cannot reconstruct AuthDB from %s at rev %d due to missing '
        'AuthDBSnapshotShard: %r', primary_url, auth_db_rev, missing)
    return None

  out = cStringIO.StringIO()
  decompressor = zlib.decompressobj()
  for idx, shard in enumerate(shards):
    data = decompressor.decompress(shard.blob)
    # Release the memory held by the shard ASAP
    shards[idx] = None
    shard.blob = None
    out.write(data)
  out.write(decompressor.flush())
  del decompressor

  buf = out.getvalue()
  del out

  return replication_pb2.AuthDB.FromString(buf)
