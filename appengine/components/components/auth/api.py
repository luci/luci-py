# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines main bulk of public API of auth component.

Functions defined here can be safely called often (multiple times per request),
since they are using in-memory read only cache of Auth DB entities.

Functions that operate on most current state of DB are in model.py. And they are
generally should not be used outside of Auth components implementation.
"""

# Pylint doesn't like ndb.transactional(...).
# pylint: disable=E1120

import collections
import functools
import logging
import os
import threading
import time
import urllib

from google.appengine.api import oauth
from google.appengine.ext import ndb
from google.appengine.ext.ndb import metadata
from google.appengine.runtime import apiproxy_errors

from . import config
from . import ipaddr
from . import model

# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'autologin',
  'AuthenticationError',
  'AuthorizationError',
  'disable_process_cache',
  'Error',
  'get_current_identity',
  'get_peer_identity',
  'get_peer_host',
  'get_peer_ip',
  'get_process_cache_expiration_sec',
  'get_secret',
  'is_admin',
  'is_group_member',
  'list_group',
  'public',
  'require',
  'SecretKey',
  'verify_ip_whitelisted',
  'warmup',
]


# How soon process-global AuthDB cache expires, sec.
_process_cache_expiration_sec = 30
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


################################################################################
## Exception classes.


class Error(Exception):
  """Base class for exceptions raised by auth component."""
  def __init__(self, message=None):
    super(Error, self).__init__(message or self.__doc__)


class AuthenticationError(Error):
  """Provided credentials are invalid."""


class AuthorizationError(Error):
  """Access is denied."""


################################################################################
## AuthDB and RequestCache.


# Name of a secret. Can be service-local (scope == 'local') or global across
# all services (scope == 'global'). Used by 'get_secret' function.
SecretKey = collections.namedtuple('SecretKey', ['name', 'scope'])


class AuthDB(object):
  """A read only in-memory database of auth configuration of a service.

  Holds user groups, all secret keys (local and global) and OAuth2
  configuration.

  Each instance process holds AuthDB object in memory and shares it between all
  requests, occasionally refetching it from Datastore.
  """

  def __init__(
      self,
      global_config=None,
      groups=None,
      secrets=None,
      ip_whitelist_assignments=None,
      ip_whitelists=None,
      entity_group_version=None):
    """
    Args:
      global_config: instance of AuthGlobalConfig entity.
      groups: list of AuthGroup entities.
      secrets: list of AuthSecret entities ('local' and 'global' in same list).
      ip_whitelists: list of AuthIPWhitelist entities.
      ip_whitelist_assignments: AuthIPWhitelistAssignments entity.
      entity_group_version: version of AuthGlobalConfig entity group at the
          moment when entities were fetched from it.
    """
    self.global_config = global_config or model.AuthGlobalConfig()
    self.groups = {g.key.string_id(): g for g in (groups or [])}
    self.secrets = {'local': {}, 'global': {}}
    self.ip_whitelists = {e.key.string_id(): e for e in (ip_whitelists or [])}
    self.ip_whitelist_assignments = (
        ip_whitelist_assignments or model.AuthIPWhitelistAssignments())
    self.entity_group_version = entity_group_version

    # Split |secrets| into local and global ones based on parent key id.
    for secret in (secrets or []):
      scope = secret.key.parent().string_id()
      assert scope in self.secrets, scope
      assert secret.key.string_id() not in self.secrets[scope], secret.key
      self.secrets[scope][secret.key.string_id()] = secret

  def is_group_member(self, group_name, identity):
    """Returns True if |identity| belongs to group |group_name|.

    Unknown groups are considered empty.
    """
    # While the code to add groups refuses to add cycle, this code ensures that
    # it doesn't go in a cycle by keeping track of the groups visited in |seen|.
    seen = set()

    def is_group_member_internal(group_name, identity):
      # Wildcard group that matches all identities (including anonymous!).
      if group_name == model.GROUP_ALL:
        return True

      # An unknown group is empty.
      group_obj = self.groups.get(group_name)
      if not group_obj:
        return False

      # Use |seen| to detect and avoid cycles in group nesting graph.
      if group_name in seen:
        logging.error('Cycle in a group graph\nInfo: %s, %s', group_name, seen)
        return False
      seen.add(group_name)

      # Globs first, there's usually a higher chance to find identity there.
      if any(glob.match(identity) for glob in group_obj.globs):
        return True

      # Explicit member list, it's fast.
      if any(identity == m for m in group_obj.members):
        return True

      # Slowest nested group check last.
      return any(
          is_group_member_internal(nested, identity)
          for nested in group_obj.nested)

    return is_group_member_internal(group_name, identity)

  def list_group(self, group_name, recursive=True):
    """Returns a set of all identities in a group.

    Args:
      group_name: name of a group to list.
      recursive: True to include nested group.

    Returns:
      Set of Identity objects. Unknown groups are considered empty.
    """
    if not recursive:
      group_obj = self.groups.get(group_name)
      return set(group_obj.members) if group_obj else set()

    # While the code to add groups refuses to add cycle, this code ensures that
    # it doesn't go in a cycle by keeping track of the groups visited in |seen|.
    seen = set()

    def list_group_internal(group_name):
      # An unknown group is empty.
      group_obj = self.groups.get(group_name)
      if not group_obj:
        return set()

      # Use |seen| to detect and avoid cycles in group nesting graph.
      if group_name in seen:
        logging.error('Cycle in a group graph\nInfo: %s, %s', group_name, seen)
        return set()
      seen.add(group_name)

      members = set(group_obj.members)
      for nested in group_obj.nested:
        members.update(list_group_internal(nested))
      return members

    return list_group_internal(group_name)

  def get_secret(self, secret_key):
    """Returns list of strings with last known values of a secret.

    If secret doesn't exist yet, it will be created.

    Args:
      secret_key: instance of SecretKey with name of a secret and a scope
          ('local' or 'global', see doc string for AuthSecretScope).
    """
    if secret_key.scope not in self.secrets:
      raise ValueError('Invalid secret key scope: %s' % secret_key.scope)
    # There's a race condition here: multiple requests, that share same AuthDB
    # object, fetch same missing secret key. It's rare (since key bootstrap
    # process is rare) and not harmful (since AuthSecret.bootstrap is
    # implemented with transaction inside). We ignore it.
    if secret_key.name not in self.secrets[secret_key.scope]:
      self.secrets[secret_key.scope][secret_key.name] = (
          model.AuthSecret.bootstrap(secret_key.name, secret_key.scope))
    entity = self.secrets[secret_key.scope][secret_key.name]
    return list(entity.values)

  def verify_ip_whitelisted(self, identity, ip, headers):
    """Verifies IP is in a whitelist assigned to Identity or in bots whitelist.

    Returns new bot Identity if request was authenticated as coming from
    IP whitelisted bot, or |identity| otherwise.

    Raises AuthorizationError if identity has an IP whitelist assigned and given
    IP address doesn't belong to it or X-Whitelisted-Bot-Id is used by
    non whitelisted bot.
    """
    assert isinstance(identity, model.Identity), identity

    # Check bots whitelist to authenticate anonymous request as coming from bot.
    bot_id_header = headers.get('X-Whitelisted-Bot-Id')
    if identity.is_anonymous:
      whitelist = self.ip_whitelists.get(model.BOTS_IP_WHITELIST)
      if whitelist and whitelist.is_ip_whitelisted(ip):
        bot_id = bot_id_header
        if not bot_id:
          # TODO(vadimsh): It is temporary bandaid until bots start using
          # X-Whitelisted-Bot-Id or switch to OAuth.
          bot_id = 'whitelisted-ip'
          # bot_id = ipaddr.ip_to_string(ip).replace(':', '-')
        return model.Identity(model.IDENTITY_BOT, bot_id)

    if bot_id_header:
      raise AuthorizationError(
          'X-Whitelisted-Bot-Id (%s) is used by bot not in the whitelist'
          % bot_id_header)

    # Find IP whitelist name in the assignment entity (if any).
    for assignment in self.ip_whitelist_assignments.assignments:
      if assignment.identity == identity:
        whitelist_id = assignment.ip_whitelist
        break
    else:
      return identity

    # IP whitelist MUST be there. But if it's missing, choose a safer
    # alternative: reject the request.
    whitelist = self.ip_whitelists.get(whitelist_id)
    if not whitelist:
      logging.error('Unknown IP whitelist: %s', whitelist_id)
      raise AuthorizationError('IP is not whitelisted')

    if not whitelist.is_ip_whitelisted(ip):
      logging.error(
          'IP is not whitelisted.\nIdentity: %s\nIP: %s\nWhitelist: %s',
          identity.to_bytes(), ipaddr.ip_to_string(ip), whitelist_id)
      raise AuthorizationError('IP is not whitelisted')

    return identity

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
    """Returns a tuple with OAuth2 config.

    Format of the tuple: (client_id, client_secret, additional client ids list).
    """
    if not self.global_config:
      return None, None, None
    return (
        self.global_config.oauth_client_id,
        self.global_config.oauth_client_secret,
        self.global_config.oauth_additional_client_ids)


def attempt_oauth_initialization(scope):
  """Attempts to perform GetOAuthUser RPC retrying deadlines.

  The result it cached in appengine.api.oauth guts. Never raises exceptions,
  just gives up letting subsequent oauth.* calls fail in a proper way.
  """
  # 4 attempts: ~20 sec (default RPC deadline is 5 sec).
  attempt = 0
  while attempt < 4:
    attempt += 1
    try:
      oauth.get_client_id(scope)
      return
    except apiproxy_errors.DeadlineExceededError as e:
      logging.warning('DeadlineExceededError: %s', e)
      continue
    except oauth.Error as e:
      # Next call to oauth.get_client_id() will trigger same error and it will
      # be handled for real.
      logging.warning('oauth.Error: %s', e)
      return


def extract_oauth_caller_identity(extra_client_ids=None):
  """Extracts and validates Identity of a caller for current request.

  Uses client_id whitelist fetched from the datastore to validate OAuth client
  used to build access_token. Also recognizes various types of service accounts
  and verifies that their client_id is what it should be. Service account's
  client_id doesn't have to be in client_id whitelist.

  Used by webapp2 request handlers and Cloud Endpoints API methods.

  Args:
    extra_client_ids: optional list of allowed client_ids, in addition to
      the whitelist in the datastore.

  Returns:
    Identity of the caller in case the request was successfully validated.

  Raises:
    AuthenticationError in case access_token is missing or invalid.
    AuthorizationError in case client_id is forbidden.
  """
  # OAuth2 scope a token should have.
  oauth_scope = 'https://www.googleapis.com/auth/userinfo.email'

  # Fetch OAuth request state with retries. oauth.* calls use it internally.
  attempt_oauth_initialization(oauth_scope)

  # Extract client_id and email from access token. That also validates the token
  # and raises OAuthRequestError if token is revoked or otherwise not valid.
  try:
    client_id = oauth.get_client_id(oauth_scope)
  except oauth.OAuthRequestError:
    raise AuthenticationError('Invalid OAuth token')

  # This call just reads data cached by oauth.get_client_id, and thus should
  # never fail.
  email = oauth.get_current_user(oauth_scope).email()

  # Is client_id in the explicit whitelist? Used with three legged OAuth. Detect
  # Google service accounts. No need to whitelist client_ids for each of them,
  # since email address uniquely identifies credentials used.
  good = (
      email.endswith('.gserviceaccount.com') or
      get_request_auth_db().is_allowed_oauth_client_id(client_id) or
      client_id in (extra_client_ids or []))

  if not good:
    raise AuthorizationError(
        'Unrecognized combination of email (%s) and client_id (%s). '
        'Is client_id whitelisted? Is it unrecognized service account?' %
        (email, client_id))
  try:
    return model.Identity(model.IDENTITY_USER, email)
  except ValueError:
    raise AuthenticationError('Unsupported user email: %s' % email)


class RequestCache(object):
  """Holds authentication related information for the current request.

  Current request is a request being processed by currently running thread.
  A thread can handle at most one request at a time (as assumed by WSGI model).
  But same thread can be reused for another request later. In that case second
  request gets a new copy of RequestCache.

  All members can be set only once, since they are not supposed to be changing
  during lifetime of a request.

  See also:
    * reinitialize_request_cache - to forcibly setup new RequestCache.
    * get_request_cache - to grab current thread-local RequestCache.
  """

  def __init__(self):
    self._auth_db = None
    self._current_identity = None
    self._peer_host = None
    self._peer_identity = None
    self._peer_ip = None

  @property
  def auth_db(self):
    """Returns request-local copy of AuthDB, fetching it if necessary."""
    if self._auth_db is None:
      self._auth_db = get_process_auth_db()
    return self._auth_db

  @property
  def current_identity(self):
    return self._current_identity or model.Anonymous

  @current_identity.setter
  def current_identity(self, current_identity):
    """Records identity to use for auth decisions.

    It may be delegated identity conveyed through delegation token.
    If delegation is not used, it is equal to peer identity.
    """
    assert current_identity
    assert not self._current_identity
    self._current_identity = current_identity

  @property
  def peer_host(self):
    return self._peer_host

  @peer_host.setter
  def peer_host(self, peer_host):
    """Called for requests that provide valid X-Host-Token header."""
    assert peer_host
    assert not self._peer_host
    self._peer_host = peer_host

  @property
  def peer_identity(self):
    return self._peer_identity or model.Anonymous

  @peer_identity.setter
  def peer_identity(self, peer_identity):
    """Records identity of whoever is making the request.

    It's an identity directly extracted from user credentials (ignoring
    delegation tokens).
    """
    assert peer_identity
    assert not self._peer_identity
    self._peer_identity = peer_identity

  @property
  def peer_ip(self):
    return self._peer_ip

  @peer_ip.setter
  def peer_ip(self, peer_ip):
    """Remembers caller's ipaddr.IP address."""
    assert isinstance(peer_ip, ipaddr.IP)
    assert not self._peer_ip
    self._peer_ip = peer_ip

  def close(self):
    """Helps GC to collect garbage faster."""
    self._auth_db = None
    self._current_identity = None
    self._peer_host = None
    self._peer_identity = None
    self._peer_ip = None


def disable_process_cache():
  """Disables in-process cache of AuthDB.

  Useful in tests.
  """
  global _process_cache_expiration_sec
  _process_cache_expiration_sec = 0


def get_process_cache_expiration_sec():
  """How long auth db is cached in process memory."""
  return _process_cache_expiration_sec


def reinitialize_request_cache():
  """Creates new RequestCache instance and puts it into thread local store.

  RequestCached used by the thread before this call (if any) is forcibly closed.
  """
  prev = getattr(_thread_local, 'request_cache', None)
  if prev:
    prev.close()
  request_cache = RequestCache()
  _thread_local.request_cache = request_cache
  return request_cache


def get_request_cache():
  """Returns instance of RequestCache associated with the current request.

  Creates a new empty one if necessary.
  """
  cache = getattr(_thread_local, 'request_cache', None)
  return cache or reinitialize_request_cache()


def fetch_auth_db(known_version=None):
  """Returns instance of AuthDB.

  If |known_version| is None, this function always returns a new instance.

  If |known_version| is not None, this function will compare |known_version| to
  current version of root_key() entity group, fetched by calling
  get_entity_group_version(). It they match, function will return None
  (meaning that there's no need to refetch AuthDB), otherwise it will fetch
  a fresh copy of AuthDB and return it.

  Runs in transaction to guarantee consistency of fetched data. Effectively it
  fetches momentary snapshot of subset of root_key() entity group.
  """
  # Entity group root. To reduce amount of typing.
  root_key = model.root_key()

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
      model.AuthGlobalConfig.get_or_insert(root_key.string_id())
      _lazy_bootstrap_ran = True

  @ndb.transactional(propagation=ndb.TransactionOptions.INDEPENDENT)
  def fetch():
    # Don't fetch anything if |known_version| is up to date. On dev server
    # metadata.get_entity_group_version() always returns None, so on dev server
    # this optimization is effectively disabled.
    current_version = metadata.get_entity_group_version(root_key)
    if known_version is not None and current_version == known_version:
      return None

    # TODO(vadimsh): Use auth_db_rev instead of entity group version. It is less
    # likely to change without any apparent reason (like entity group version
    # does).

    # TODO(vadimsh): Add memcache keyed at |current_version| so only one
    # frontend instance have to pay the cost of fetching AuthDB from Datastore
    # via multiple RPCs. All other instances will fetch it via single
    # memcache 'get'.

    # Fetch all stuff in parallel. Fetch ALL groups and ALL secrets.
    global_config_future = root_key.get_async()
    groups_future = model.AuthGroup.query(ancestor=root_key).fetch_async()
    secrets_future = model.AuthSecret.query(ancestor=root_key).fetch_async()

    # It's fine to block here as long as it's the last fetch.
    ip_whitelist_assignments, ip_whitelists = model.fetch_ip_whitelists()

    # Note that get_entity_group_version() uses same entity group (root_key)
    # internally and respects transactions. So all data fetched here does indeed
    # correspond to |current_version|.
    return AuthDB(
        global_config=global_config_future.get_result(),
        groups=groups_future.get_result(),
        secrets=secrets_future.get_result(),
        ip_whitelists=ip_whitelists,
        ip_whitelist_assignments=ip_whitelist_assignments,
        entity_group_version=current_version)

  bootstrap()
  return fetch()


def reset_local_state():
  """Resets all local caches to an initial state. Only for testing."""
  global _auth_db
  global _auth_db_expiration
  global _auth_db_fetching_thread
  global _lazy_bootstrap_ran
  _auth_db = None
  _auth_db_expiration = None
  _auth_db_fetching_thread = None
  _lazy_bootstrap_ran = False
  _thread_local.request_cache = None


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
    # initial fetch to complete. Also ensure 'auth' component is configured.
    if _auth_db is None:
      logging.info('Initial fetch of AuthDB')
      config.ensure_configured()
      _auth_db = fetch_auth_db()
      _auth_db_expiration = time.time() + _process_cache_expiration_sec
      return _auth_db

    # We have a cached copy and it has expired. Maybe some thread is already
    # fetching it? Don't block an entire process on this, return a little bit
    # stale copy instead right away.
    if _auth_db_fetching_thread is not None:
      logging.info(
          'Using stale copy of AuthDB while another thread is fetching '
          'a fresh one. Cached copy expired %.1f sec ago.',
          time.time() - _auth_db_expiration)
      return _auth_db

    # No one is fetching AuthDB yet. Start the operation, release the lock so
    # other threads can figure this out and use stale copies instead of blocking
    # on the lock.
    _auth_db_fetching_thread = threading.current_thread()
    known_auth_db = _auth_db
    known_auth_db_version = _auth_db.entity_group_version
    logging.debug('Refetching AuthDB')

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
    _auth_db_expiration = time.time() + _process_cache_expiration_sec
    return _auth_db


def warmup():
  """Can be called from /_ah/warmup handler to precache authentication DB."""
  get_process_auth_db()


################################################################################
## Identity retrieval, @public and @require decorators.


def get_request_auth_db():
  """Returns instance of AuthDB from request-local cache.

  In a context of a single request this function always returns same
  instance of AuthDB. So as long as request runs, auth config stay consistent
  and don't change beneath your feet.

  Effectively request handler uses a snapshot of AuthDB at the moment request
  starts. If it somehow makes a call that initiates another request that uses
  AuthDB (via task queue, or UrlFetch) that another request may see a different
  copy of AuthDB.
  """
  return get_request_cache().auth_db


def get_current_identity():
  """Returns Identity associated with the current request.

  Takes into account delegation tokens, e.g. it can return end-user identity
  delegated to caller via delegation token. Use get_peer_identity() to get
  ID of a real caller, disregarding delegation.

  Always returns instance of Identity (that can be Anonymous, but never None).

  Returns non-Anonymous only if authentication context is properly initialized:
    * For webapp2, handlers must inherit from handlers.AuthenticatingHandler.
    * For Cloud Endpoints see endpoints_support.py.
  """
  return _get_current_identity()


def _get_current_identity():
  """Actual implementation of get_current_identity().

  Exists to be mocked, since original get_current_identity symbol is copied by
  value to 'auth' package scope, and mocking the identity would require mocking
  both 'auth.get_current_identity' and 'auth.api.get_current_identity'. It's
  simpler to move implementation to a private mockable function.
  """
  return get_request_cache().current_identity


def get_peer_identity():
  """Returns Identity of whoever made the request (disregarding delegation).

  Always returns instance of Identity (that can be Anonymous, but never None).

  Returns non-Anonymous only if authentication context is properly initialized:
    * For webapp2 handlers must inherit from handlers.AuthenticatingHandler.
    * For Cloud Endpoints see endpoints_support.py.
  """
  return get_request_cache().peer_identity


def get_peer_host():
  """Returns hostname extracted from X-Host-Token header or None if missing."""
  return get_request_cache().peer_host


def get_peer_ip():
  """Returns ipaddr.IP address of a peer that sent current request."""
  return get_request_cache().peer_ip


def is_group_member(group_name, identity=None):
  """Returns True if |identity| (or current identity if None) is in the group.

  Unknown groups are considered empty.
  """
  return get_request_cache().auth_db.is_group_member(
      group_name, identity or get_current_identity())


def is_admin(identity=None):
  """Returns True if |identity| (or current identity if None) is an admin."""
  return is_group_member(model.ADMIN_GROUP, identity)


def list_group(group_name, recursive=True):
  """Returns a set of all Identity in a group."""
  return get_request_cache().auth_db.list_group(group_name, recursive)


def get_secret(secret_key):
  """Given an instance of SecretKey returns several last values of the secret.

  First item in the list is the current value of a secret (that can be used to
  validate and generate tokens), the rest are previous values (that can be used
  to validate older tokens, but shouldn't be used to create new ones).

  Creates a new secret if necessary.
  """
  return get_request_cache().auth_db.get_secret(secret_key)


def verify_ip_whitelisted(identity, ip, headers):
  """Verifies IP is in a whitelist assigned to Identity or in bots whitelist.

  Returns new bot Identity if request was authenticated as coming from
  IP whitelisted bot, or |identity| otherwise.

  Raises AuthorizationError if identity has an IP whitelist assigned and given
  IP address doesn't belong to it.
  """
  return get_request_cache().auth_db.verify_ip_whitelisted(
      identity, ip, headers)


def public(func):
  """Decorator that marks a function as available for anonymous access.

  Useful only in a context of AuthenticatingHandler subclass to mark method as
  explicitly open for anonymous access. Without it AuthenticatingHandler will
  complain:

  class MyHandler(auth.AuthenticatingHandler):
    @auth.public
    def get(self):
      ....
  """
  # @require decorator sets __auth_require attribute.
  if hasattr(func, '__auth_require'):
    raise TypeError('Can\'t use @public and @require on a same function')
  func.__auth_public = True
  return func


def require(callback):
  """Decorator that checks current identity's permissions.

  Args:
    callback: callback that is called without arguments and returns True
        to grant access to current identity (by calling decorated function) or
        False to forbid it (by raising AuthorizationError). It can
        use get_current_identity() (and other request state) to figure this out.

  Multiple @require decorators can be safely nested on top of each other to
  check multiple permissions. In that case a current identity needs to have all
  specified permissions to pass the check, i.e. permissions checks are combined
  using logical AND operation.

  It's safe to mix @require with NDB decorators such as @ndb.transactional.

  Usage example:

  class MyHandler(auth.AuthenticatingHandler):
    @auth.require(auth.is_admin)
    def get(self):
      ....
  """
  def decorator(func):
    # @public decorator sets __auth_public attribute.
    if hasattr(func, '__auth_public'):
      raise TypeError('Can\'t use @public and @require on same function')

    # When nesting multiple decorators the information (argspec, name) about
    # original function gets lost. __wrapped__ is used by NDB decorators
    # to preserve reference to original function. Use it too.
    original = getattr(func, '__wrapped__', func)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
      if not callback():
        raise AuthorizationError()
      return func(*args, **kwargs)

    # Propagate reference to original function, mark function as decorated.
    wrapper.__wrapped__ = original
    wrapper.__auth_require = True

    return wrapper

  return decorator


def autologin(func):
  """Decorator that autologin anonymous users via the web UI.

  This is meant to to used on handlers that require a non-anonymous user via
  @require(), so that the user is not served a 403 simply because he didn't have
  the cookie set yet. Do not use this decorator on APIs using anything else than
  AppEngine's user authentication mechanism or OpenID cookies mechanism provided
  by components.auth.

  Usage example:

  class MyHandler(auth.AuthenticatingHandler):
    @auth.autologin
    @auth.require(auth.is_admin)
    def get(self):
      ....
  """
  # @public decorator sets __auth_public attribute.
  if hasattr(func, '__auth_public'):
    raise TypeError('Can\'t use @public and @autolgin on same function')
  # When nesting multiple decorators the information (argspec, name) about
  # original function gets lost. __wrapped__ is used by NDB decorators
  # to preserve reference to original function. Use it too.
  original = getattr(func, '__wrapped__', func)
  if original.__name__ != 'get':
    raise TypeError('Only get() can be set as autologin')

  @functools.wraps(func)
  def wrapper(self, *args, **kwargs):
    if not self.get_current_user():
      self.redirect(self.create_login_url(self.request.url))
      return
    try:
      return func(self, *args, **kwargs)
    except AuthorizationError:
      if (not self.is_current_user_gae_admin() or
          is_admin() or model.is_replica()):
        raise
      self.redirect(
          '/auth/bootstrap?r=%s' % urllib.quote_plus(self.request.path_qs))

  # Propagate reference to original function, mark function as decorated.
  wrapper.__wrapped__ = original
  wrapper.__auth_require = True

  return wrapper


def is_decorated(func):
  """Return True if |func| is decorated by @public or @require decorators."""
  return hasattr(func, '__auth_public') or hasattr(func, '__auth_require')
