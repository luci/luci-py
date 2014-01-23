# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines main bulk of public API of auth component."""

# Pylint doesn't like ndb.transactional(...).
# pylint: disable=E1120

import logging
import os
import threading
import time

from google.appengine.ext import ndb
from google.appengine.ext.ndb import metadata

from . import model


# This module doesn't export any public API yet.
__all__ = []


# How soon process-global AuthDB cache expires, sec.
PROCESS_CACHE_EXPIRATION_SEC = 30


# True if fetch_auth_db was called at least once and created all root entities.
_lazy_bootstrap_ran = False

# Protects _auth_db* globals below.
_auth_db_lock = threading.Lock()
# Currently cached instance of AuthDB.
_auth_db = None
# When current value of _auth_db should be refetched.
_auth_db_expiration = None
# Holds id of a thread that is currently fetching AuthDB (or None).
_auth_db_fetching_thread = None

# Thread local storage for RequestCache (see 'get_request_cache').
_thread_local = threading.local()


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


class RequestCache(object):
  """Holds authentication related information for the current request.

  Current request is a request being processed by currently running thread.
  A thread can handle at most one request at a time (as assumed by WSGI model).
  But same thread can be reused for another request later. In that case second
  request gets a new copy of RequestCache. See also 'get_request_cache'.
  """

  def __init__(self):
    self.current_identity = None
    self.auth_db = None
    self.is_admin = False

  def set_current_identity(self, current_identity, is_admin):
    """Called early during request processing to set identity for a request."""
    self.current_identity = current_identity
    self.is_admin = is_admin

  def close(self):
    """Helps GC to collect garbage faster."""
    self.current_identity = None
    self.auth_db = None


def get_request_cache():
  """Returns instance of RequestCache associated with the current request.

  A new instance is created for each new HTTP request. We determine
  that we're in a new request by inspecting os.environ, which is thread-local
  and reset at the start of each request.
  """
  # Properly close an object from previous request served by the current thread.
  request_cache = getattr(_thread_local, 'request_cache', None)
  if not os.getenv('__AUTH_CACHE__') and request_cache is not None:
    request_cache.close()
    _thread_local.request_cache = None
    request_cache = None
  # Make a new cache, put it in thread local state, put flag in os.environ.
  if request_cache is None:
    request_cache = RequestCache()
    _thread_local.request_cache = request_cache
    os.environ['__AUTH_CACHE__'] = '1'
  return request_cache


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

    # TODO(vadimsh): Add memcache keyed at |current_version| so only one
    # frontend instance have to pay the cost of fetching AuthDB from Datastore
    # via multiple RPCs. All other instances will fetch it via single
    # memcache 'get'.

    # Fetch all stuff in parallel. Fetch ALL groups and ALL secrets.
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


def get_process_auth_db():
  """Returns instance of AuthDB from process-global cache.

  Will refetch it if necessary. Two subsequent calls may return different
  instances if cache expires between the calls.
  """
  global _auth_db
  global _auth_db_expiration
  global _auth_db_fetching_thread

  known_auth_db = None
  known_auth_db_version = None

  with _auth_db_lock:
    # Cached copy is still fresh?
    if _auth_db and time.time() < _auth_db_expiration:
      return _auth_db

    # Fetching AuthDB for the first time ever? Do it under the lock because
    # there's nothing to return yet. All threads would have to wait for this
    # initial fetch to complete.
    if _auth_db is None:
      logging.info('Initial fetch of AuthDB')
      _auth_db = fetch_auth_db()
      _auth_db_expiration = time.time() + PROCESS_CACHE_EXPIRATION_SEC
      return _auth_db

    # We have a cached copy and it has expired. Maybe some thread is already
    # fetching it? Don't block an entire process on this, return a little bit
    # stale copy instead right away.
    if _auth_db_fetching_thread is not None:
      logging.info(
          'Using stale copy of AuthDB while tid %s is fetching a fresh one. '
          'Cached copy expired %.1f sec ago.',
          _auth_db_fetching_thread,
          time.time() - _auth_db_expiration)
      return _auth_db

    # No one is fetching AuthDB yet. Start the operation, release the lock so
    # other threads can figure this out and use stale copies instead of blocking
    # on the lock.
    _auth_db_fetching_thread = threading.current_thread()
    known_auth_db = _auth_db
    known_auth_db_version = _auth_db.entity_group_version
    logging.info('Refetching AuthDB, tid is %s', _auth_db_fetching_thread)

  # Do the actual fetch outside the lock. Be careful to handle any unexpected
  # exception by 'fixing' the global state before leaving this function.
  try:
    fresh_copy = fetch_auth_db(known_version=known_auth_db_version)
    if fresh_copy is None:
      # No changes, entity group versions match, reuse same object.
      fresh_copy = known_auth_db
  except Exception:
    # Failed. Be sure to allow other threads to try the fetch. Meanwhile log the
    # exception and return a stale copy of AuthDB. Better than nothing.
    logging.exception('Failed to refetch AuthDB, returning stale cached copy')
    with _auth_db_lock:
      assert _auth_db_fetching_thread == threading.current_thread()
      _auth_db_fetching_thread = None
      return _auth_db

  # Fetch has completed successfully. Update process cache now.
  with _auth_db_lock:
    assert _auth_db_fetching_thread == threading.current_thread()
    _auth_db_fetching_thread = None
    _auth_db = fresh_copy
    _auth_db_expiration = time.time() + PROCESS_CACHE_EXPIRATION_SEC
    return _auth_db


def get_request_auth_db():
  """Returns instance of AuthDB from request-local cache.

  In a context of a single request this function always returns same
  instance of AuthDB. So as long as request runs, ACL rules stay consistent and
  don't change beneath your feet.

  Effectively request handler uses a snapshot of AuthDB at the moment request
  starts. If it somehow makes a call that initiates another request that uses
  AuthDB (via task queue, or UrlFetch) that another request may see a different
  copy of AuthDB.
  """
  cache = get_request_cache()
  if cache.auth_db is None:
    cache.auth_db = get_process_auth_db()
  return cache.auth_db
