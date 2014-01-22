# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines main bulk of public API of auth component."""

# Pylint doesn't like ndb.transactional(...).
# pylint: disable=E1120

import logging

from google.appengine.ext import ndb
from google.appengine.ext.ndb import metadata

from . import model


# This module doesn't export any public API yet.
__all__ = []


class AuthDB(object):
  """A read only in-memory database of ACL configuration for a service.

  Holds ACL rules, User Groups, all secret keys (local and global) and
  OAuth2 configuration.

  Each instance process holds AuthDB object in memory and shares it between all
  requests, occasionally refetching it from Datastore.
  """

  def __init__(
      self,
      global_config=None,
      service_config=None,
      groups=None,
      secrets=None,
      entity_group_version=None):
    """
    Args:
      global_config: instance of AuthGlobalConfig entity.
      service_config: instance of AuthServiceConfig with rules for a service.
      groups: list of AuthGroup entities.
      secrets: list of AuthSecret entities ('local' and 'global' in same list).
      entity_group_version: version of AuthGlobalConfig entity group at the
          moment when entities were fetched from it.
    """
    self.global_config = global_config or model.AuthGlobalConfig()
    self.service_config = service_config or model.AuthServiceConfig()
    self.groups = dict((g.key.string_id(), g) for g in (groups or []))
    self.secrets = {'local': {}, 'global': {}}
    self.entity_group_version = entity_group_version

    # Split |secrets| into local and global ones based on parent key id.
    for secret in (secrets or []):
      scope = secret.key.parent().string_id()
      assert scope in self.secrets, scope
      assert secret.key.string_id() not in self.secrets[scope], secret.key
      self.secrets[scope][secret.key.string_id()] = secret

  def is_group_member(self, identity, group):
    """Returns True if |identity| belongs to group |group|."""

    # While the code to add groups refuses to add cycle, this code ensures that
    # it doesn't go in a cycle by keeping track of the groups visited in |seen|.
    def is_group_member_internal(identity, group, seen):
      # Wildcard group that matches all identities (including anonymous!).
      if group == model.GROUP_ALL:
        return True

      # An unknown group is empty.
      group_obj = self.groups.get(group)
      if not group_obj:
        return False

      # Use |seen| to detect and avoid cycles in group nesting graph.
      if group in seen:
        logging.error('Cycle in a group graph\nInfo: %s, %s', group, seen)
        return False
      seen.add(group)

      # Globs first, there's usually a higher chance to find identity there.
      if any(glob.match(identity) for glob in group_obj.globs):
        return True

      # Explicit member list, it's fast.
      if any(identity == m for m in group_obj.members):
        return True

      # Slowest nested group check last.
      return any(
          is_group_member_internal(identity, nested, seen)
          for nested in group_obj.nested)

    return is_group_member_internal(identity, group, set())

  def get_groups(self, identity=None):
    """Returns a set of group names that contain |identity| as a member.

    If |identity| is None, returns all known groups.
    """
    if not identity:
      return set(self.groups)
    return set(g for g in self.groups if self.is_group_member(identity, g))

  def get_rules(self):
    """Returns all defined AccessRules."""
    return list(self.service_config.rules)

  def get_matching_rules(self, identity=None, action=None, resource=None):
    """Returns list of AccessRules related to given ACL query.

    Used in implementation of 'Rules sandbox' UI that allows users to explore
    which ACL rules are used to decide outcome of ACL query.

    Input is a tuple (Identity, Action, Resource) with some elements possibly
    omitted. This function returns all potential terminal rules that can show
    up during ACL evaluation for that triple.

    For example if ACL query is fully defined (i.e. all 3 elements are present),
    this function always returns one rule - first matched ALLOW or DENY rule
    that decides outcome of the ACL query.

    If, for instance, Identity is None, then this function will return all rules
    that mention Action and Resource (since any of them can potentially be a
    terminal rule for a query with some concrete Identity).

    Args:
      identity: instance of Identity or None.
      action: one of CREATE, READ, UPDATE, DELETE or None.
      resource: some resource name or None.

    Returns:
      List of AccessRule objects.
    """
    assert identity or identity is None
    assert action or action is None
    assert resource or resource is None
    rules = []
    for rule in self.service_config.rules:
      if action and action not in rule.actions:
        continue
      if resource and not rule.resource.match(resource):
        continue
      if identity and not self.is_group_member(identity, rule.group):
        continue
      # First matched rule terminates ACL evaluation, so if complete triple is
      # given do not even process the rest of rules.
      if identity and action and resource:
        return [rule]
      rules.append(rule)
    # No rules match -> return default implicit 'deny all' rule.
    return rules or [model.DenyAllRule]

  def has_permission(self, identity, action, resource):
    """True if |identity| can execute |action| against |resource|.

    Each ACL Rule is evaluated in turn until first match. If first matched rule
    is ALLOW rule, function returns True. If it's DENY rule, function returns
    False.

    ACL Rule (Kind, Group, Actions, Resource Regexp) matches a triple
    (identity, action, resource) if |identity| is in group Group (explicitly or
    via some nested group), |action| is in Actions set, and |resource| matches
    Resource Regexp.

    If no rules match, function returns False.
    """
    assert action in model.ALLOWED_ACTIONS
    for rule in self.service_config.rules:
      if (action in rule.actions and
          rule.resource.match(resource) and
          self.is_group_member(identity, rule.group)):
        return rule.kind == model.ALLOW_RULE
    return False

  def get_secret(self, name, scope):
    """Returns list of strings with last known values of a secret.

    Args:
      name: name of a secret, if it doesn't exist it will be created.
      scope: 'local' or 'global', see doc string for AuthSecretScope.
    """
    if scope not in self.secrets:
      raise ValueError('Invalid secret key scope')
    # There's a race condition here: multiple requests, that share same AuthDB
    # object, fetch same missing secret key. It's rare (since key bootstrap
    # process is rare) and not harmful (since AuthSecret.bootstrap is
    # implemented with transaction inside). We ignore it.
    if name not in self.secrets[scope]:
      self.secrets[scope][name] = model.AuthSecret.bootstrap(name, scope)
    entity = self.secrets[scope][name]
    return list(entity.values)

  def is_allowed_oauth_client_id(self, client_id):
    """True if given OAuth2 client_id can be used to authenticate the user."""
    if not client_id:
      return False
    allowed = set()
    if self.global_config.oauth_client_id:
      allowed.add(self.global_config.oauth_client_id)
    if self.global_config.oauth_additional_client_ids:
      allowed.update(self.global_config.oauth_additional_client_ids)
    return client_id in allowed

  def get_oauth_config(self):
    """Returns a tuple with OAuth2 config: (client_id, client_secret)."""
    if not self.global_config:
      return None, None
    return (
        self.global_config.oauth_client_id,
        self.global_config.oauth_client_secret)


# True if fetch_auth_db was called at least once and created all root entities.
_lazy_bootstrap_ran = False


def fetch_auth_db(known_version=None):
  """Returns instance of AuthDB with ACL rules for local service.

  If |known_version| is None, this function always returns a new instance.

  If |known_version| is not None, this function will compare |known_version| to
  current version of ROOT_KEY entity group, fetched by calling
  get_entity_group_version(). It they match, function will return None
  (meaning that there's no need to refetch AuthDB), otherwise it will fetch
  a fresh copy of AuthDB and return it.

  Runs in transaction to guarantee consistency of fetched data. Effectively it
  fetches momentary snapshot of subset of ROOT_KEY entity group.
  """
  # Entity group root. To reduce amount of typing.
  root_key = model.ROOT_KEY

  @ndb.non_transactional
  def bootstrap():
    # Assumption that root entities always exist make code simpler by removing
    # 'is not None' checks. So make sure they do, by running bootstrap code
    # at most once per lifetime of an instance. We do it lazily here (instead of
    # module scope) to ensure NDB calls are happening in a context of HTTP
    # request. Presumably it reduces probability of instance to stuck during
    # initial loading.
    global _lazy_bootstrap_ran
    if not _lazy_bootstrap_ran:
      logging.info('Ensuring AuthDB root entities exist')
      ndb.Future.wait_all([
        model.AuthGlobalConfig.get_or_insert_async(root_key.string_id()),
        model.AuthServiceConfig.get_or_insert_async('local', parent=root_key),
      ])
      # Update _lazy_bootstrap_ran only when DB calls successfully finish.
      _lazy_bootstrap_ran = True

  @ndb.transactional(propagation=ndb.TransactionOptions.INDEPENDENT)
  def fetch():
    # Don't fetch anything if |known_version| is up to date. On dev server
    # metadata.get_entity_group_version() always returns None, so on dev server
    # this optimization is effectively disabled.
    current_version = metadata.get_entity_group_version(root_key)
    if known_version is not None and current_version == known_version:
      return None

    # Fetch all stuff in parallel. Fetch ALL groups and ALL secrets.
    logging.info('Fetching AuthDB')
    global_config = root_key.get_async()
    service_config = model.AuthServiceConfig.get_by_id_async(
        'local', parent=root_key)
    groups = model.AuthGroup.query(ancestor=root_key).fetch_async()
    secrets = model.AuthSecret.query(ancestor=root_key).fetch_async()

    # Note that get_entity_group_version() uses same entity group (root_key)
    # internally and respects transactions. So all data fetched here does indeed
    # correspond to |current_version|.
    return AuthDB(
        global_config=global_config.get_result(),
        service_config=service_config.get_result(),
        groups=groups.get_result(),
        secrets=secrets.get_result(),
        entity_group_version=current_version)

  bootstrap()
  return fetch()
