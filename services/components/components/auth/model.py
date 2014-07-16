# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""NDB model classes used to model AuthDB relations.

Models defined here are used by central authentication service (that stores all
groups and secrets) and by services that implement some concrete functionality
protected with ACLs (like isolate and swarming services).

Applications that use auth component may work in 3 modes:
  1. Standalone. Application is self contained and manages its own groups.
     Useful when developing a new service or for simple installations.
  2. Replica. Application uses a central authentication service. An application
     can be dynamically switched from Standalone to Replica mode.
  3. Primary. Application IS a central authentication service. Only 'auth'
     service is running in this mode. 'configure_as_primary' call during startup
     switches application to that mode.

Central authentication service (Primary) holds authoritative copy of all auth
related information (groups, secrets, etc.) and acts as a single source of truth
for it. All other services (Replicas) hold copies of a relevant subset of
this information (that they use to perform authorization checks).

Primary service is responsible for updating replicas' configuration via
service-to-service push based replication protocol.

AuthDB holds a list of groups. Each group has a unique name and is defined
as union of 3 sets:
  1) Explicit enumeration of particular Identities e.g. 'user:alice@example.com'
  2) Set of glob-like identity patterns e.g. 'user:*@example.com'
  3) Set of nested Groups.

Identity defines an actor making an action (it can be a real person, a bot,
an AppEngine application or special 'anonymous' identity).

In addition to that, AuthDB stores small amount of authentication related
configuration data, such as OAuth2 client_id and client_secret and various
secret keys.
"""

import collections
import fnmatch
import logging
import os
import re

from google.appengine.api import app_identity
from google.appengine.ext import ndb

from components import datastore_utils

# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'ADMIN_GROUP',
  'Anonymous',
  'bootstrap_group',
  'configure_as_primary',
  'find_group_dependency_cycle',
  'find_referencing_groups',
  'get_missing_groups',
  'Identity',
  'IDENTITY_ANONYMOUS',
  'IDENTITY_BOT',
  'IDENTITY_SERVICE',
  'IDENTITY_USER',
  'IdentityProperty',
  'is_empty_group',
  'is_primary',
  'is_replica',
  'is_standalone',
  'replicate_auth_db',
]


# Name of a group whose members have access to Group management UI. It's the
# only group needed to bootstrap everything else.
ADMIN_GROUP = 'administrators'


# No identity information is provided. Identity name is always 'anonymous'.
IDENTITY_ANONYMOUS = 'anonymous'
# Using bot credentials. Identity name is bot's id.
IDENTITY_BOT = 'bot'
# Using App Engine service credentials. Identity name is app name.
IDENTITY_SERVICE = 'service'
# Using user credentials. Identity name is user's email.
IDENTITY_USER = 'user'

# All allowed identity kinds + regexps to validate identity name.
ALLOWED_IDENTITY_KINDS = {
  IDENTITY_ANONYMOUS: re.compile(r'^anonymous$'),
  IDENTITY_BOT: re.compile(r'^[0-9a-zA-Z_\-\.@]+$'),
  IDENTITY_SERVICE: re.compile(r'^[0-9a-zA-Z_\-]+$'),
  IDENTITY_USER: re.compile(r'^[0-9a-zA-Z_\-\.@]+$'),
}

# Regular expression that matches group names. ASCII only, no leading or
# trailing spaces allowed (spaces inside are fine).
GROUP_NAME_RE = re.compile(
    r'^[0-9a-zA-Z_][0-9a-zA-Z_\-\.\ ]{1,80}[0-9a-zA-Z_\-\.]$')
# Special group name that means 'All possible users' (including anonymous!).
GROUP_ALL = '*'


# Global root key of auth models entity group.
ROOT_KEY = ndb.Key('AuthGlobalConfig', 'root')
# Key of AuthReplicationState entity.
REPLICATION_STATE_KEY = ndb.Key('AuthReplicationState', 'self', parent=ROOT_KEY)


# Configuration of Primary service, set by 'configure_as_primary'.
_replication_callback = None


################################################################################
## Identity & IdentityGlob.


class Identity(
    datastore_utils.BytesSerializable,
    collections.namedtuple('Identity', 'kind, name')):
  """Represents a caller that makes requests. Immutable.

  A tuple of (kind, name) where 'kind' is one of IDENTITY_* constants and
  meaning of 'name' depends on a kind (see comments for IDENTITY_*).
  It generalizes accounts of real people, bot accounts and service-to-service
  accounts.

  It's a pure identity information. Any additional information that may be
  related to an identity (e.g. registration date, last access time, etc.) should
  be stored elsewhere using Identity.to_bytes() as a key.
  """

  # Inheriting from tuple requires use of __new__ instead of __init__. __init__
  # is called with object already 'frozen', so it's not possible to modify its
  # attributes in __init__.
  # See http://docs.python.org/2/reference/datamodel.html#object.__new__
  def __new__(cls, kind, name):
    if isinstance(name, unicode):
      try:
        name = name.encode('ascii')
      except UnicodeEncodeError:
        raise ValueError('Identity has invalid format: only ASCII is allowed')
    if (kind not in ALLOWED_IDENTITY_KINDS or
        not ALLOWED_IDENTITY_KINDS[kind].match(name)):
      raise ValueError('Identity has invalid format')
    return super(Identity, cls).__new__(cls, str(kind), name)

  def to_bytes(self):
    """Serializes this identity to byte buffer."""
    return '%s:%s' % (self.kind, self.name)

  @classmethod
  def from_bytes(cls, byte_buf):
    """Given a byte buffer returns corresponding Identity object."""
    kind, sep, name = byte_buf.partition(':')
    if not sep:
      raise ValueError('Missing \':\' separator in Identity string')
    return cls(kind, name)

  @property
  def is_anonymous(self):
    """True if this object represents anonymous identity."""
    return self.kind == IDENTITY_ANONYMOUS

  @property
  def is_bot(self):
    """True if this object represents bot account."""
    return self.kind == IDENTITY_BOT

  @property
  def is_service(self):
    """True if this object represents service account."""
    return self.kind == IDENTITY_SERVICE

  @property
  def is_user(self):
    """True if this object represents user account."""
    return self.kind == IDENTITY_USER


# Predefined Anonymous identity.
Anonymous = Identity(IDENTITY_ANONYMOUS, 'anonymous')


class IdentityProperty(datastore_utils.BytesSerializableProperty):
  """NDB model property for Identity values.

  Identities are stored as indexed short blobs internally.
  """
  _value_type = Identity
  _indexed = True


class IdentityGlob(
    datastore_utils.BytesSerializable,
    collections.namedtuple('IdentityGlob', 'kind, pattern')):
  """Glob-like pattern that matches subset of identities. Immutable.

  Tuple (kind, glob) where 'kind' is is one of IDENTITY_* constants and 'glob'
  defines pattern that identity names' should match. For example, IdentityGlob
  that matches all bots is (IDENTITY_BOT, '*') which is also can be written
  as 'bot:*'.
  """

  # See comment for Identity.__new__ regarding use of __new__ here.
  def __new__(cls, kind, pattern):
    if isinstance(pattern, unicode):
      try:
        pattern = pattern.encode('ascii')
      except UnicodeEncodeError:
        raise ValueError('Invalid IdentityGlob pattern: only ASCII is allowed')
    if not pattern:
      raise ValueError('No pattern is given')
    if kind not in ALLOWED_IDENTITY_KINDS:
      raise ValueError('Invalid Identity kind: %s' % kind)
    return super(IdentityGlob, cls).__new__(cls, str(kind), pattern)

  def to_bytes(self):
    """Serializes this identity glob to byte buffer."""
    return '%s:%s' % (self.kind, self.pattern)

  @classmethod
  def from_bytes(cls, byte_buf):
    """Given a byte buffer returns corresponding IdentityGlob object."""
    kind, sep, pattern = byte_buf.partition(':')
    if not sep:
      raise ValueError('Missing \':\' separator in IdentityGlob string')
    return cls(kind, pattern)

  def match(self, identity):
    """Return True if |identity| matches this pattern."""
    if identity.kind != self.kind:
      return False
    return fnmatch.fnmatchcase(identity.name, self.pattern)


class IdentityGlobProperty(datastore_utils.BytesSerializableProperty):
  """NDB model property for IdentityGlob values.

  IdentityGlobs are stored as short indexed blobs internally.
  """
  _value_type = IdentityGlob
  _indexed = True


################################################################################
## Singleton entities and replication related models.


def configure_as_primary(replication_callback):
  """Registers a callback to be called when AuthDB changes.

  Should be called during Primary application startup. The callback will be
  called as 'replication_callback(AuthReplicationState)' from inside transaction
  on ROOT_KEY entity group whenever replicate_auth_db() is called (i.e. on every
  change to auth db that should be replication to replicas).
  """
  global _replication_callback
  _replication_callback = replication_callback


def is_primary():
  """Returns True if current application was configured as Primary."""
  return bool(_replication_callback)


def is_replica():
  """Returns True if application is in Replica mode."""
  return not is_primary() and not is_standalone()


def is_standalone():
  """Returns True if application is in Standalone mode."""
  ent = get_replication_state()
  return not ent or not ent.primary_id


def get_replication_state():
  """Returns AuthReplicationState singleton entity if it exists."""
  return REPLICATION_STATE_KEY.get()


class AuthGlobalConfig(ndb.Model):
  """Acts as a root entity for auth models.

  There should be only one instance of this model in Datastore, with a key set
  to ROOT_KEY. A change to an entity group rooted at this key is a signal that
  AuthDB has to be refetched (see 'fetch_auth_db' in api.py).

  Entities that change often or associated with particular bot or user
  MUST NOT be in this entity group.

  Content of this particular entity is replicated from Primary service to all
  Replicas.

  Entities that belong to this entity group are:
   * AuthGroup
   * AuthReplicationState
   * AuthSecret
  """
  # OAuth2 client_id to use to mint new OAuth2 tokens.
  oauth_client_id = ndb.StringProperty(indexed=False)
  # OAuth2 client secret. Not so secret really, since it's passed to clients.
  oauth_client_secret = ndb.StringProperty(indexed=False)
  # Additional OAuth2 client_ids allowed to access the services.
  oauth_additional_client_ids = ndb.StringProperty(repeated=True, indexed=False)


class AuthReplicationState(ndb.Model):
  """Contains state used to control Primary -> Replica replication.

  It's a singleton entity with key REPLICATION_STATE_KEY (in same entity groups
  as ROOT_KEY). This entity should be small since it is updated (auth_db_rev is
  incremented) whenever AuthDB changes.

  Exists in any AuthDB (on Primary and Replicas). Primary updates it whenever
  changes to AuthDB are made, Replica updates it whenever it receives a push
  from Primary.
  """
  # For services in Standalone mode it is None.
  # For services in Primary mode: own GAE application ID.
  # For services in Replica mode it is a GAE application ID of Primary.
  primary_id = ndb.StringProperty(indexed=False)

  # For services in Replica mode, root URL of Primary, i.e https://<host>.
  primary_url = ndb.StringProperty(indexed=False)

  # Revision of auth DB. Increased by 1 with every change that should be
  # propagate to replicas. Only services in Standalone or Primary mode
  # update this property by themselves. Replicas receive it from Primary.
  auth_db_rev = ndb.IntegerProperty(default=0, indexed=False)

  # Last modification time. For informational purposes only.
  modified_ts = ndb.DateTimeProperty(auto_now=True, indexed=False)


def replicate_auth_db():
  """Increments auth_db_rev by one.

  It is a signal that Auth DB should be replicated to Replicas. If called from
  inside a transaction, it inherits it and updates auth_db_rev only once (even
  if called multiple times during that transaction).

  Should only be called for services in Standalone or Primary modes. Will raise
  ValueError if called on Replica. When called for service in Standalone mode,
  will update auth_db_rev but won't kick any replication. For services in
  Primary mode will also initiate replication by calling callback set in
  'configure_as_primary'.

  WARNING: This function relies on a valid transaction context. NDB hooks and
  asynchronous operations are known to be buggy in this regard: NDB hook for
  an async operation in a transaction may be called with a wrong context
  (main event loop context instead of transaction context). One way to work
  around that is to monkey patch NDB (as done here: https://goo.gl/1yASjL).
  Another is to not use hooks at all. There's no way to differentiate between
  sync and async modes of an NDB operation from inside a hook. And without a
  strict assert it's very easy to forget about "Do not use put_async" warning.
  For that reason _post_put_hook is NOT used and replicate_auth_db() should be
  called explicitly whenever relevant part of ROOT_KEY entity group is updated.
  """
  def increment_revision_and_update_replicas():
    """Does the actual job, called inside a transaction."""
    # Update auth_db_rev. REPLICATION_STATE_KEY is in same group as ROOT_KEY.
    state = REPLICATION_STATE_KEY.get()
    if not state:
      primary_id = app_identity.get_application_id() if is_primary() else None
      state = AuthReplicationState(
          key=REPLICATION_STATE_KEY,
          primary_id=primary_id,
          auth_db_rev=0)
    # Assert Primary or Standalone. Replicas can't increment auth db revision.
    if not is_primary() and state.primary_id:
      raise ValueError('Can\'t modify Auth DB on Replica')
    state.auth_db_rev += 1
    state.put()
    # Only Primary does active replication.
    if is_primary():
      _replication_callback(state)

  # If not in a transaction, start a new one.
  if not ndb.in_transaction():
    ndb.transaction(increment_revision_and_update_replicas)
    return

  # If in a transaction, use transaction context to store "already did this"
  # flag. Note that each transaction retry gets its own new transaction context,
  # see ndb/context.py, 'transaction' tasklet, around line 982 (for SDK 1.9.6).
  ctx = ndb.get_context()
  if not getattr(ctx, '_auth_db_inc_called', False):
    increment_revision_and_update_replicas()
    ctx._auth_db_inc_called = True


################################################################################
## Groups.


class AuthGroup(ndb.Model, datastore_utils.SerializableModelMixin):
  """A group of identities, entity id is a group name.

  Parent is AuthGlobalConfig entity keyed at ROOT_KEY.

  Primary service holds authoritative list of Groups, that gets replicated to
  all Replicas.
  """
  # How to convert this entity to or from serializable dict.
  serializable_properties = {
    'members': datastore_utils.READABLE | datastore_utils.WRITABLE,
    'globs': datastore_utils.READABLE | datastore_utils.WRITABLE,
    'nested': datastore_utils.READABLE | datastore_utils.WRITABLE,
    'description': datastore_utils.READABLE | datastore_utils.WRITABLE,
    'created_ts': datastore_utils.READABLE,
    'created_by': datastore_utils.READABLE,
    'modified_ts': datastore_utils.READABLE,
    'modified_by': datastore_utils.READABLE,
  }

  # List of members that are explicitly in this group. Indexed.
  members = IdentityProperty(repeated=True)
  # List of identity-glob expressions (like 'user:*@example.com'). Indexed.
  globs = IdentityGlobProperty(repeated=True)
  # List of nested group names. Indexed.
  nested = ndb.StringProperty(repeated=True)

  # Human readable description.
  description = ndb.StringProperty(indexed=False)

  # When the group was created.
  created_ts = ndb.DateTimeProperty(auto_now_add=True)
  # Who created the group.
  created_by = IdentityProperty()

  # When the group was modified last time.
  modified_ts = ndb.DateTimeProperty(auto_now=True)
  # Who modified the group last time.
  modified_by = IdentityProperty()


def group_key(group):
  """Returns ndb.Key for AuthGroup entity."""
  return ndb.Key(AuthGroup, group, parent=ROOT_KEY)


def is_empty_group(group):
  """Returns True if group is missing or completely empty."""
  group = group_key(group).get()
  return not group or not(group.members or group.globs or group.nested)


@ndb.transactional
def bootstrap_group(group, identity, description):
  """Makes a group (if not yet exists) and adds an |identity| to it as a member.

  Returns True if added |identity| to |group|, False if it is already there.
  """
  key = group_key(group)
  entity = key.get()
  if entity and identity in entity.members:
    return False
  if not entity:
    entity = AuthGroup(
        key=key,
        description=description,
        created_by=identity,
        modified_by=identity)
  entity.members.append(identity)
  entity.put()
  replicate_auth_db()
  return True


def find_referencing_groups(group):
  """Finds groups that reference the specified group as nested group.

  Used to verify that |group| is safe to delete, i.e. no other group is
  depending on it.

  Returns:
    Set of names of referencing groups.
  """
  referencing_groups = AuthGroup.query(
      AuthGroup.nested == group, ancestor=ROOT_KEY).fetch(keys_only=True)
  return set(key.id() for key in referencing_groups)


def get_missing_groups(groups):
  """Given a list of group names, returns a list of groups that do not exist."""
  # We need to iterate over |groups| twice. It won't work if |groups|
  # is a generator. So convert to list first.
  groups = list(groups)
  entities = ndb.get_multi(group_key(name) for name in groups)
  return [name for name, ent in zip(groups, entities) if not ent]


def find_group_dependency_cycle(group):
  """Searches for dependency cycle between nested groups.

  Traverses the dependency graph starting from |group|, fetching all necessary
  groups from datastore along the way.

  Args:
    group: instance of AuthGroup to start traversing from. It doesn't have to be
        committed to Datastore itself (but all its nested groups should be
        there already).

  Returns:
    List of names of groups that form a cycle or empty list if no cycles.
  """
  # It is a depth-first search on a directed graph with back edge detection.
  # See http://www.cs.nyu.edu/courses/summer04/G22.1170-001/6a-Graphs-More.pdf

  # Cache of already fetched groups.
  groups = {group.key.id(): group}

  # List of groups that are completely explored (all subtree is traversed).
  visited = []
  # Stack of groups that are being explored now. In case cycle is detected
  # it would contain that cycle.
  visiting = []

  def visit(group):
    """Recursively explores |group| subtree, returns True if finds a cycle."""
    assert group not in visiting
    assert group not in visited

    # Load bodies of nested groups not seen so far into |groups|.
    entities = ndb.get_multi(
        group_key(name) for name in group.nested if name not in groups)
    groups.update({entity.key.id(): entity for entity in entities if entity})

    visiting.append(group)
    for nested in group.nested:
      obj = groups.get(nested)
      # Do not crash if non-existent group is referenced somehow.
      if not obj:
        continue
      # Cross edge. Can happen in diamond-like graph, not a cycle.
      if obj in visited:
        continue
      # Back edge: |group| references its own ancestor -> cycle.
      if obj in visiting:
        return True
      # Explore subtree.
      if visit(obj):
        return True
    visiting.pop()

    visited.append(group)
    return False

  visit(group)
  return [group.key.id() for group in visiting]


################################################################################
## Secrets store.


class AuthSecretScope(ndb.Model):
  """Entity to act as parent entity for AuthSecret.

  Parent is AuthGlobalConfig entity keyed at ROOT_KEY.

  Id of this entity defines scope of secret keys that have this entity as
  a parent. Possible scopes are 'local' and 'global'.

  Secrets in 'local' scope never leave Datastore they are stored in and they
  are different for each service (even for Replicas). Only service that
  generated a local secret knows it.

  Secrets in 'global' scope are known to all services (via Primary -> Replica
  DB replication mechanism). Source of truth for global secrets is in Primary's
  Datastore.
  """


def secret_scope_key(scope):
  """Key of AuthSecretScope entity for a given scope ('global' or 'local')."""
  return ndb.Key(AuthSecretScope, scope, parent=ROOT_KEY)


class AuthSecret(ndb.Model):
  """Some service-wide named secret blob.

  Entity can be a child of:
    * Key(AuthSecretScope, 'global', parent=ROOT_KEY):
        Global secrets replicated across all services.
    * Key(AuthSecretScope, 'local', parent=ROOT_KEY):
        Secrets local to the current service.

  There should be only very limited number of AuthSecret entities around. AuthDB
  fetches them all at once. Do not use this entity for per-user secrets.

  Holds most recent value of a secret as well as several previous values. Most
  recent value is used to generate new tokens, previous values may be used to
  validate existing tokens. That way secret can be rotated without invalidating
  any existing outstanding tokens.
  """
  # Last several values of a secret, with current value in front.
  values = ndb.BlobProperty(repeated=True, indexed=False)

  # When secret was modified last time.
  modified_ts = ndb.DateTimeProperty(auto_now=True)
  # Who modified the secret last time.
  modified_by = IdentityProperty()

  @classmethod
  def bootstrap(cls, name, scope, length=32):
    """Creates a secret if it doesn't exist yet.

    Args:
      name: name of the secret.
      scope: 'local' or 'global', see doc string for AuthSecretScope. 'global'
          scope should only be used on Primary service.
      length: length of the secret to generate if secret doesn't exist yet.

    Returns:
      Instance of AuthSecret (creating it if necessary) with random secret set.
    """
    # Note that 'get_or_insert' is a bad fit here. With 'get_or_insert' we'd
    # have to call os.urandom every time we want to get a key. It's a waste of
    # time and entropy.
    if scope not in ('local', 'global'):
      raise ValueError('Invalid secret scope: %s' % scope)
    key = ndb.Key(cls, name, parent=secret_scope_key(scope))
    entity = key.get()
    if entity is not None:
      return entity
    @ndb.transactional
    def create():
      entity = key.get()
      if entity is not None:
        return entity
      logging.info('Creating new secret key %s in %s scope', name, scope)
      # Global keys can only be created on Primary or Standalone service.
      if scope == 'global' and is_replica():
        raise ValueError('Can\'t bootstrap global key on Replica')
      entity = cls(key=key, values=[os.urandom(length)])
      entity.put()
      # Only global keys are part of replicated state.
      if scope == 'global':
        replicate_auth_db()
      return entity
    return create()
