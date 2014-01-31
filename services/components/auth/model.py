# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""NDB model classes used to model AuthDB relations.

Models defined here are used by central authentication service (that stores all
ACL rules, groups and bot credentials) and by services that implement some
concrete functionality protected with ACLs (like isolate and swarming services).

Central authentication service (called 'master service' below) holds
authoritative copy of all ACL configuration and acts as a single source of
truth for it. All other services (called 'slave service' below) hold copy of
relevant subset of configuration (that they use to perform ACL checks).

Master service is responsible for updating slave services' configuration via
simple service-to-service replication protocol.

Conceptually ACLs are defined as a set of 3-tuples (Identity, Resource, Action)
of allowed operations where:
  * Identity defines an actor making an action (it can be a real person, a bot,
    an AppEngine application or special 'anonymous' identity).
  * Resource is a string that identifies what object is being operated on. Its
    meaning is application specific and is not precisely defined here.
  * Action is what Identity wants to do with Resource: CREATE, READ, UPDATE or
    DELETE. Precise meaning is also application specific.

Each identity (including 'anonymous') can belong to zero or more Groups. Each
Group has a unique name and is defined as union of 3 sets:
  1) Explicit enumeration of particular Identities e.g. 'user:alice@example.com'
  2) Set of glob-like identity patterns e.g. 'user:*@example.com'
  3) Set of nested Groups.

ACL Rules represent ACL set in a compact form. ACL Rules is a list of
ALLOW or DENY rules, evaluated in order until first match. If first matched
rule is ALLOW rule, then access is granted, if it's DENY rule access is denied.

In particular, each rule is a tuple (Kind, Group, Actions, ResourceRegex):
  * Kind is 'ALLOW' for positive rule, 'DENY' for negative rule.
  * Group is a name of the group the rule applies to or '*' to apply to all
    identities (including 'anonymous').
  * Actions is list of actions the rule applies to, i.e. subset of ('CREATE',
    'READ', 'UPDATE', 'DELETE').
  * ResourceRegex is a regular expression that defines all resources the rule
    applies to.

For example rule ('ALLOW', 'Bots', ['READ'], 'isolate/namespaces/(.*)$') means
that all identities from group 'Bots' are allowed to perform 'READ' action with
any resource that matches 'isolate/namespaces/(.*)$'.

Another example is deny-all rule:
  ('DENY', '*', ['CREATE', 'READ', 'UPDATE', 'DELETE'], '.*')

Deny-all rule is always implicitly present at the end of ACL Rules list.

Each service has its own list of ACL Rules, but they all share same set of
Groups. Possible resource names are defined by each particular service
implementation.
"""

# TODO(vadimsh): Implement ACL DB replication.

import collections
import fnmatch
import logging
import os
import re

from google.appengine.ext import ndb

from components import utils

# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'Anonymous',
  'CREATE',
  'DELETE',
  'Identity',
  'IDENTITY_ANONYMOUS',
  'IDENTITY_BOT',
  'IDENTITY_SERVICE',
  'IDENTITY_USER',
  'IdentityProperty',
  'READ',
  'UPDATE',
]


# Rule types.
ALLOW_RULE = 'ALLOW'
DENY_RULE = 'DENY'

# Possible actions that can be applied to a resource.
CREATE = 'CREATE'
DELETE = 'DELETE'
READ = 'READ'
UPDATE = 'UPDATE'

# Set of all allowed actions.
ALLOWED_ACTIONS = (CREATE, DELETE, READ, UPDATE)

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
    utils.BytesSerializable,
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


class IdentityProperty(utils.BytesSerializableProperty):
  """NDB model property for Identity values.

  Identities are stored as indexed short blobs internally.
  """
  _value_type = Identity
  _indexed = True


class IdentityGlob(
    utils.BytesSerializable,
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


class IdentityGlobProperty(utils.BytesSerializableProperty):
  """NDB model property for IdentityGlob values.

  IdentityGlobs are stored as short indexed blobs internally.
  """
  _value_type = IdentityGlob
  _indexed = True


################################################################################
## AccessRule.


class AccessRule(
    utils.JsonSerializable,
    collections.namedtuple('AccessRule', 'kind, group, actions, resource')):
  """Single access rule definition. Immutable.

  Tuple (kind, group, actions, resource) where:
    kind - ALLOW_RULE for positive rule, DENY_RULE for negative rule.
    group - name of a user group this rule applies to, or '*' for all users.
    actions - set of actions this rule applies to, subset of ALLOWED_ACTIONS.
    resource - regular expression for resource names this rule applies to.
  """

  # See comment for Identity.__new__ regarding use of __new__ here.
  def __new__(cls, kind, group, actions, resource):
    # Validate kind, convert to str.
    if kind not in (ALLOW_RULE, DENY_RULE):
      raise ValueError('Invalid access rule kind: %s' % kind)
    kind = str(kind)

    # Validate group name.
    if not GROUP_NAME_RE.match(group) and group != GROUP_ALL:
      raise ValueError('Invalid group name: %s' % group)

    # Validate action list, convert to str.
    if any(a not in ALLOWED_ACTIONS for a in actions):
      raise ValueError('Invalid actions: %s' % (actions,))
    actions = tuple(sorted(map(str, set(actions))))
    if not actions:
      raise ValueError('Action list can not be empty')

    # Validate resource regular expression, prohibit open-ended patterns.
    if not resource.startswith('^') or not resource.endswith('$'):
      raise ValueError(
          'Resource pattern should start with \'^\' and end '
          'with \'$\': %s' % resource)
    try:
      re.compile(resource)
    except re.error:
      raise ValueError('Invalid resource regexp pattern: %s' % resource)

    return super(AccessRule, cls).__new__(
        cls, kind, str(group), actions, resource)

  def to_jsonish(self):
    return {
      'actions': self.actions,
      'group': self.group,
      'kind': self.kind,
      'resource': self.resource,
    }

  @classmethod
  def from_jsonish(cls, obj):
    try:
      return cls(obj['kind'], obj['group'], obj['actions'], obj['resource'])
    except KeyError as e:
      raise ValueError('Missing key \'%s\' in  %r' % (e, obj))


# Special rule 'deny all access' implicitly added to list of rules.
DenyAllRule = AccessRule(DENY_RULE, GROUP_ALL, ALLOWED_ACTIONS, '^.*$')
# Special rule 'allow all access' added to list of rules during bootstrap.
AllowAllRule = AccessRule(ALLOW_RULE, GROUP_ALL, ALLOWED_ACTIONS, '^.*$')


class AccessRuleProperty(utils.JsonSerializableProperty):
  """NDB model property for AccessRule values.

  Stored as JSON blob internally.
  """
  _value_type = AccessRule
  _indexed = False


################################################################################
## Main models: AuthGlobalConfig, AuthServiceConfig and AuthGroup.


class AuthGlobalConfig(ndb.Model):
  """Acts as a root entity for auth models.

  In particular, entities that belong to this entity group are:
   * AuthServiceConfig
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


class AuthServiceConfig(ndb.Model, utils.SerializableModelMixin):
  """Holds ACL Rules for a single service.

  Parent is AuthGlobalConfig entity keyed at ROOT_KEY.

  Master service have multiple entities of this kind for each slave service
  (with entity id set to service name), while each slave service has only one
  (with id set to 'local') that gets replicated from corresponding master's
  entity.

  Master service also have entity with id 'local', that defines ACL rules
  for Master service itself.
  """
  # How to convert this entity to or from serializable dict.
  serializable_properties = {
    'rules': utils.READABLE | utils.WRITABLE,
    'modified_ts': utils.READABLE,
    'modified_by': utils.READABLE,
   }

  # All ACL rules. Order is important.
  rules = AccessRuleProperty(repeated=True)
  # When entity was modified last time.
  modified_ts = ndb.DateTimeProperty(auto_now=True, indexed=False)
  # Who modified the entity last time.
  modified_by = IdentityProperty(indexed=False)


class AuthGroup(ndb.Model, utils.SerializableModelMixin):
  """A group of identities, entity id is a group name.

  Parent is AuthGlobalConfig entity keyed at ROOT_KEY.

  Master service holds authoritative list of Groups, that gets replicated to
  all slave services.
  """
  # How to convert this entity to or from serializable dict.
  serializable_properties = {
    'members': utils.READABLE | utils.WRITABLE,
    'globs': utils.READABLE | utils.WRITABLE,
    'nested': utils.READABLE | utils.WRITABLE,
    'description': utils.READABLE | utils.WRITABLE,
    'created_ts': utils.READABLE,
    'created_by': utils.READABLE,
    'modified_ts': utils.READABLE,
    'modified_by': utils.READABLE,
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
