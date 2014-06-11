# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""NDB model classes used to model AuthDB relations.

Models defined here are used by central authentication service (that stores all
groups and bot credentials) and by services that implement some concrete
functionality protected with ACLs (like isolate and swarming services).

Central authentication service (called 'master service' below) holds
authoritative copy of all ACL configuration and acts as a single source of
truth for it. All other services (called 'slave service' below) hold copy of
relevant subset of configuration (that they use to perform ACL checks).

Master service is responsible for updating slave services' configuration via
simple service-to-service replication protocol.

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

# TODO(vadimsh): Implement ACL DB replication.

import collections
import fnmatch
import logging
import os
import re

from google.appengine.ext import ndb

from components import datastore_utils

# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'ADMIN_GROUP',
  'Anonymous',
  'Identity',
  'IDENTITY_ANONYMOUS',
  'IDENTITY_BOT',
  'IDENTITY_SERVICE',
  'IDENTITY_USER',
  'IdentityProperty',
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
## Main models: AuthGlobalConfig, AuthGroup.


class AuthGlobalConfig(ndb.Model):
  """Acts as a root entity for auth models.

  In particular, entities that belong to this entity group are:
   * AuthGroup
   * AuthSecretScope
   * AuthSecret

  There should be only one instance of this model in Datastore, with a key set
  to ROOT_KEY. A change to an entity group rooted at this key is a signal that
  AuthDB has to be refetched (see 'fetch_auth_db' below).

  Entities that change often or associated with particular bot or user
  (like bot's credentials) MUST NOT be in this entity group.

  Content of this particular entity is replicated from master service to all
  slave services.
  """
  # OAuth2 client_id to use to mint new OAuth2 tokens.
  oauth_client_id = ndb.StringProperty(indexed=False)
  # OAuth2 client secret. Not so secret really, since it's passed to clients.
  oauth_client_secret = ndb.StringProperty(indexed=False)
  # Additional OAuth2 client_ids allowed to access the services.
  oauth_additional_client_ids = ndb.StringProperty(repeated=True, indexed=False)


class AuthGroup(ndb.Model, datastore_utils.SerializableModelMixin):
  """A group of identities, entity id is a group name.

  Parent is AuthGlobalConfig entity keyed at ROOT_KEY.

  Master service holds authoritative list of Groups, that gets replicated to
  all slave services.
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

  # When group was modified last time.
  modified_ts = ndb.DateTimeProperty(auto_now=True)
  # Who modified the group last time.
  modified_by = IdentityProperty()


def group_key(group):
  """Returns ndb.Key for AuthGroup entity."""
  return ndb.Key(AuthGroup, group, parent=ROOT_KEY)


################################################################################
## Secrets store.


class AuthSecretScope(ndb.Model):
  """Entity to act as parent entity for AuthSecret.

  Parent is AuthGlobalConfig entity keyed at ROOT_KEY.

  Id of this entity defines scope of secret keys that have this entity as
  a parent. Possible scopes are 'local' and 'global'.

  Secrets in 'local' scope never leave Datastore they are stored in and they
  are different for each slave service. Only service that generated a local
  secret knows it.

  Secrets in 'global' scope are known to all services (via master -> slave
  DB replication mechanism). Source of truth for global secrets is in master's
  Datastore.
  """


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
          scope should only be used on master service.
      length: length of the secret to generate if secret doesn't exist yet.

    Returns:
      Instance of AuthSecret (creating it if necessary) with random secret set.
    """
    # Note that 'get_or_insert' is a bad fit here. With 'get_or_insert' we'd
    # have to call os.urandom every time we want to get a key. It's a waste of
    # time and entropy.
    if scope not in ('local', 'global'):
      raise ValueError('Invalid secret scope: %s' % scope)
    key = ndb.Key(AuthSecretScope, scope, cls, name, parent=ROOT_KEY)
    entity = key.get()
    if entity is not None:
      return entity
    @ndb.transactional
    def create():
      entity = key.get()
      if entity is not None:
        return entity
      logging.info('Creating new secret key %s in %s scope', name, scope)
      entity = cls(key=key, values=[os.urandom(length)])
      entity.put()
      return entity
    return create()

  def update(self, secret, identity, keep_previous=True, retention=1):
    """Updates secret value, optionally remembering previous one.

    Args:
      secret: new value for a secret (arbitrary str blob).
      identity: Identity that making this change.
      keep_previous: True to store current value of key so it can still be used
          to validate tokens, etc.
      retention: how many historical values to keep (in addition to
          current secret value).
    """
    values = list(self.values or [])
    if keep_previous:
      values = [secret] + values[:retention]
    else:
      if values:
        values[0] = secret
      else:
        values = [secret]
    self.values = values
    self.modified_by = identity
