# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

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
import json
import logging
import os
import threading
import time
import urllib

from google.appengine.api import oauth
from google.appengine.api import urlfetch
from google.appengine.ext import ndb
from google.appengine.ext.ndb import metadata
from google.appengine.runtime import apiproxy_errors

from components import utils

from . import config
from . import ipaddr
from . import model

# Part of public API of 'auth' component, exposed by this module.
__all__ = [
  'AuthenticationError',
  'AuthorizationError',
  'autologin',
  'disable_process_cache',
  'Error',
  'get_current_identity',
  'get_peer_identity',
  'get_peer_ip',
  'get_process_cache_expiration_sec',
  'get_secret',
  'is_admin',
  'is_group_member',
  'is_in_ip_whitelist',
  'is_superuser',
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


# The endpoint used to validate an access token on dev server.
TOKEN_INFO_ENDPOINT = 'https://www.googleapis.com/oauth2/v1/tokeninfo'

# OAuth2 client_id of the "API Explorer" web app.
API_EXPLORER_CLIENT_ID = '292824132082.apps.googleusercontent.com'


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
## AuthDB.


# Name of a secret. Can be service-local (scope == 'local') or global across
# all services (scope == 'global'). Used by 'get_secret' function.
SecretKey = collections.namedtuple('SecretKey', ['name', 'scope'])


# The representation of AuthGroup used by AuthDB, preprocessed for faster
# membership checks. We keep it in AuthDB in place of AuthGroup to reduce RAM
# usage.
CachedGroup = collections.namedtuple('CachedGroup', [
  'members',  # == set(m.to_bytes() for m in auth_group.members)
  'globs',
  'nested',
  'description',
  'owners',
  'created_ts',
  'created_by',
  'modified_ts',
  'modified_by',
])


class AuthDB(object):
  """A read only in-memory database of auth configuration of a service.

  Holds user groups, all secret keys (local and global) and OAuth2
  configuration.

  Each instance process holds AuthDB object in memory and shares it between all
  requests, occasionally refetching it from Datastore.
  """

  def __init__(
      self,
      replication_state=None,
      global_config=None,
      groups=None,
      secrets=None,
      ip_whitelist_assignments=None,
      ip_whitelists=None,
      entity_group_version=None):
    """
    Args:
      replication_state: instance of AuthReplicationState entity.
      global_config: instance of AuthGlobalConfig entity.
      groups: list of AuthGroup entities.
      secrets: list of AuthSecret entities ('local' and 'global' in same list).
      ip_whitelists: list of AuthIPWhitelist entities.
      ip_whitelist_assignments: AuthIPWhitelistAssignments entity.
      entity_group_version: version of AuthGlobalConfig entity group at the
          moment when entities were fetched from it.
    """
    self.replication_state = replication_state or model.AuthReplicationState()
    self.global_config = global_config or model.AuthGlobalConfig()
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

    # Preprocess groups for faster membership checks. Throw away original
    # entities to reduce memory usage.
    self.groups = {}
    for entity in (groups or []):
      self.groups[entity.key.string_id()] = CachedGroup(
          members=frozenset(m.to_bytes() for m in entity.members),
          globs=entity.globs or (),
          nested=entity.nested or (),
          description=entity.description,
          owners=entity.owners,
          created_ts=entity.created_ts,
          created_by=entity.created_by,
          modified_ts=entity.modified_ts,
          modified_by=entity.modified_by)

  @property
  def auth_db_rev(self):
    """Returns the revision number of groups database."""
    return self.replication_state.auth_db_rev

  @property
  def primary_id(self):
    """For services in Replica mode, GAE application ID of Primary."""
    return self.replication_state.primary_id

  @property
  def primary_url(self):
    """For services in Replica mode, root URL of Primary, i.e https://<host>."""
    return self.replication_state.primary_url

  @property
  def token_server_url(self):
    """URL of a token server to use to generate tokens, provided by Primary."""
    return self.global_config.token_server_url

  def is_group_member(self, group_name, identity):
    """Returns True if |identity| belongs to group |group_name|.

    Unknown groups are considered empty.
    """
    # Will be used when checking self.group_members_set sets.
    ident_as_bytes = identity.to_bytes()

    # While the code to add groups refuses to add cycle, this code ensures that
    # it doesn't go in a cycle by keeping track of the groups currently being
    # visited via |current| stack.
    current = []

    # Used to avoid revisiting same groups multiple times in case of
    # diamond-like graphs, e.g. A->B, A->C, B->D, C->D.
    visited = set()

    def is_member(group_name):
      # Wildcard group that matches all identities (including anonymous!).
      if group_name == model.GROUP_ALL:
        return True

      # An unknown group is empty.
      group_obj = self.groups.get(group_name)
      if not group_obj:
        logging.warning(
            'Querying unknown group: %s via %s', group_name, current)
        return False

      # In a group DAG a group can not reference any of its ancestors, since it
      # creates a cycle.
      if group_name in current:
        logging.warning(
            'Cycle in a group graph: %s via %s', group_name, current)
        return False

      # Explored this group already (and didn't find |identity| there) while
      # visiting some sibling branch? Can happen in diamond-like graphs.
      if group_name in visited:
        return False

      current.append(group_name)
      try:
        # Note that we don't include nested groups in GroupEssense.members sets
        # because it blows up memory usage pretty bad. We don't have very deep
        # nesting graphs, so checking nested groups separately is OK.
        if ident_as_bytes in group_obj.members:
          return True

        if any(glob.match(identity) for glob in group_obj.globs):
          return True

        return any(is_member(nested) for nested in group_obj.nested)
      finally:
        current.pop()
        visited.add(group_name)

    return is_member(group_name)

  def get_group(self, group_name):
    """Returns AuthGroup entity reconstructing it from the cache.

    It slightly differs from the original entity:
      - 'members' list is always sorted.
      - 'auth_db_rev' and 'auth_db_prev_rev' are not set.

    Returns:
      AuthGroup object or None if no such group.
    """
    g = self.groups.get(group_name)
    if not g:
      return None
    return model.AuthGroup(
        key=model.group_key(group_name),
        members=[model.Identity.from_bytes(m) for m in sorted(g.members)],
        globs=list(g.globs),
        nested=list(g.nested),
        description=g.description,
        owners=g.owners,
        created_ts=g.created_ts,
        created_by=g.created_by,
        modified_ts=g.modified_ts,
        modified_by=g.modified_by)

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
      if not group_obj:
        return set()
      return set(model.Identity.from_bytes(m) for m in group_obj.members)

    # Set of groups already added to 'listing'.
    visited = set()

    # Updated by visit_group. We keep it in the closure to avoid passing more
    # stuff through the thread stack.
    listing = set()

    def visit_group(group_name):
      # An unknown group is empty.
      group_obj = self.groups.get(group_name)
      if not group_obj:
        return

      if group_name in visited:
        return
      visited.add(group_name)

      listing.update(group_obj.members)
      for nested in group_obj.nested:
        visit_group(nested)

    visit_group(group_name)
    return set(model.Identity.from_bytes(m) for m in listing)

  def fetch_groups_with_member(self, ident):
    """Returns a set of group names that have given Identity as a member.

    This is expensive call, don't use it unless really necessary.
    """
    # TODO(vadimsh): This is currently very dumb and can probably be optimized.
    return {g for g in self.groups if self.is_group_member(g, ident)}

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

  def is_in_ip_whitelist(self, whitelist_name, ip):
    """Returns True if the given IP belongs to the given IP whitelist.

    Missing IP whitelists are considered empty.

    Args:
      whitelist_name: name of the IP whitelist (e.g. 'bots').
      ip: instance of ipaddr.IP.
    """
    whitelist = self.ip_whitelists.get(whitelist_name)
    if not whitelist:
      logging.error('Unknown IP whitelist: %s', whitelist_name)
      return False
    return whitelist.is_ip_whitelisted(ip)

  def verify_ip_whitelisted(self, identity, ip):
    """Verifies IP is in a whitelist assigned to the Identity.

    This check is used to restrict some callers to particular IP subnets as
    additional security measure.

    Raises AuthorizationError if identity has an IP whitelist assigned and given
    IP address doesn't belong to it.

    Args:
      identity: caller's identity.
      ip: instance of ipaddr.IP.
    """
    assert isinstance(identity, model.Identity), identity

    for assignment in self.ip_whitelist_assignments.assignments:
      if assignment.identity == identity:
        whitelist_name = assignment.ip_whitelist
        break
    else:
      return

    if not self.is_in_ip_whitelist(whitelist_name, ip):
      ip_as_str = ipaddr.ip_to_string(ip)
      logging.error(
          'IP is not whitelisted.\nIdentity: %s\nIP: %s\nWhitelist: %s',
          identity.to_bytes(), ip_as_str, whitelist_name)
      raise AuthorizationError('IP %s is not whitelisted' % ip_as_str)

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


################################################################################
## OAuth token check.


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
    except oauth.OAuthServiceFailureError as e:
      logging.warning(
          'oauth.OAuthServiceFailureError (%s): %s', e.__class__.__name__, e)
      # oauth library "caches" the error code in os.environ and retrying
      # oauth.get_client_id doesn't do anything. Clear this cache first, see
      # oauth_api.py, _maybe_call_get_oauth_user in GAE SDK.
      os.environ.pop('OAUTH_ERROR_CODE', None)
      continue
    except oauth.Error as e:
      # Next call to oauth.get_client_id() will trigger same error and it will
      # be handled for real.
      logging.warning('oauth.Error (%s): %s', e.__class__.__name__, e)
      return


def extract_oauth_caller_identity():
  """Extracts and validates Identity of a caller for the current request.

  Implemented on top of GAE OAuth2 API.

  Uses client_id whitelist fetched from the datastore to validate OAuth client
  used to build access_token. Also recognizes various types of service accounts
  and verifies that their client_id is what it should be. Service account's
  client_id doesn't have to be in client_id whitelist.

  Returns:
    (Identity, is_superuser).

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
      client_id == API_EXPLORER_CLIENT_ID or
      get_request_auth_db().is_allowed_oauth_client_id(client_id))

  if not good:
    raise AuthorizationError(
        'Unrecognized combination of email (%s) and client_id (%s). '
        'Is client_id whitelisted? Is it unrecognized service account?' %
        (email, client_id))
  try:
    ident = model.Identity(model.IDENTITY_USER, email)
  except ValueError:
    raise AuthenticationError('Unsupported user email: %s' % email)
  return ident, oauth.is_current_user_admin(oauth_scope)


def check_oauth_access_token(headers):
  """Verifies the access token of the current request.

  This function uses slightly different strategies for prod, dev and local
  environments:
    * In prod it always require real OAuth2 tokens, validated by GAE OAuth2 API.
    * On local devserver it uses URL Fetch and prod token info endpoint.
    * On '-dev' instances or on dev server it can also fallback to a custom
      token info endpoint, defined in AuthDevConfig datastore entity. This is
      useful to "stub" authentication when running integration or load tests.

  In addition to checking the correctness of OAuth token, this function also
  verifies that the client_id associated with the token is whitelisted in the
  auth config.

  The client_id check is skipped on the local devserver or when using custom
  token info endpoint (e.g. on '-dev' instances).

  Args:
    headers: a dict with request headers.

  Returns:
    Tuple (ident, is_superuser), where ident is an identity of the caller in
    case the request was successfully validated (always 'user:...', never
    anonymous), and is_superuser is true if the caller is GAE-level admin.

  Raises:
    AuthenticationError in case the access token is invalid.
    AuthorizationError in case the access token is not allowed.
  """
  header = headers.get('Authorization')
  if not header:
    raise AuthenticationError('No "Authorization" header')

  # Non-development instances always use real OAuth API.
  if not utils.is_local_dev_server() and not utils.is_dev():
    return extract_oauth_caller_identity()

  # OAuth2 library is mocked on dev server to return some nonsense. Use (slow,
  # but real) OAuth2 API endpoint instead to validate access_token. It is also
  # what Cloud Endpoints do on a local server.
  if utils.is_local_dev_server():
    # auth_call returns tuple (Identity, is_superuser). Superuser is always
    # False if not using native GAE OAuth API.
    auth_call = lambda: (
        dev_oauth_authentication(header, TOKEN_INFO_ENDPOINT), False)
  else:
    auth_call = extract_oauth_caller_identity

  # Do not fallback to custom endpoint if not configured. This call also has a
  # side effect of initializing AuthDevConfig entity in the datastore, to make
  # it editable in Datastore UI.
  cfg = model.get_dev_config()
  if not cfg.token_info_endpoint:
    return auth_call()

  # Try the real call first, then fallback to the custom validation endpoint.
  try:
    return auth_call()
  except AuthenticationError:
    ident = dev_oauth_authentication(header, cfg.token_info_endpoint, '.dev')
    logging.warning('Authenticated as dev account: %s', ident.to_bytes())
    return ident, False


def dev_oauth_authentication(header, token_info_endpoint, suffix=''):
  """OAuth2 based authentication via URL Fetch to the token info endpoint.

  This is slow and ignores client_id whitelist. Must be used only in
  a development environment.

  Returns:
    Identity of the caller in case the request was successfully validated.

  Raises:
    AuthenticationError in case access token is missing or invalid.
    AuthorizationError in case the token is not trusted.
  """
  assert utils.is_local_dev_server() or utils.is_dev()

  header = header.split(' ', 1)
  if len(header) != 2 or header[0] not in ('OAuth', 'Bearer'):
    raise AuthenticationError('Invalid authorization header')

  # Adapted from endpoints/users_id_tokens.py, _set_bearer_user_vars_local.
  logging.info('Using dev token info endpoint %s', token_info_endpoint)
  result = urlfetch.fetch(
      url='%s?%s' % (
          token_info_endpoint,
          urllib.urlencode({'access_token': header[1]})),
      follow_redirects=False,
      validate_certificate=True)
  if result.status_code != 200:
    try:
      error = json.loads(result.content)['error_description']
    except (KeyError, ValueError):
      error = repr(result.content)
    raise AuthenticationError('Failed to validate the token: %s' % error)

  token_info = json.loads(result.content)
  if 'email' not in token_info:
    raise AuthorizationError('Token doesn\'t include an email address')
  if not token_info.get('verified_email'):
    raise AuthorizationError('Token email isn\'t verified')

  email = token_info['email'] + suffix
  try:
    return model.Identity(model.IDENTITY_USER, email)
  except ValueError:
    raise AuthorizationError('Unsupported user email: %s' % email)


################################################################################
## RequestCache.


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
    self._peer_identity = None
    self._peer_ip = None
    self._is_superuser = None

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
    assert isinstance(current_identity, model.Identity), current_identity
    assert not self._current_identity
    self._current_identity = current_identity

  @property
  def peer_identity(self):
    return self._peer_identity or model.Anonymous

  @peer_identity.setter
  def peer_identity(self, peer_identity):
    """Records identity of whoever is making the request.

    It's an identity directly extracted from user credentials (ignoring
    delegation tokens).
    """
    assert isinstance(peer_identity, model.Identity), peer_identity
    assert not self._peer_identity
    self._peer_identity = peer_identity

  @property
  def peer_ip(self):
    return self._peer_ip

  @peer_ip.setter
  def peer_ip(self, peer_ip):
    assert isinstance(peer_ip, ipaddr.IP)
    assert not self._peer_ip
    self._peer_ip = peer_ip

  @property
  def is_superuser(self):
    return bool(self._is_superuser)

  @is_superuser.setter
  def is_superuser(self, value):
    assert self._is_superuser is None # haven't been set yet
    self._is_superuser = bool(value)

  def close(self):
    """Helps GC to collect garbage faster."""
    self._auth_db = None
    self._current_identity = None
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
    replication_state_future = model.replication_state_key().get_async()
    global_config_future = root_key.get_async()
    groups_future = model.AuthGroup.query(ancestor=root_key).fetch_async()
    secrets_future = model.AuthSecret.query(ancestor=root_key).fetch_async()

    # It's fine to block here as long as it's the last fetch.
    ip_whitelist_assignments, ip_whitelists = model.fetch_ip_whitelists()

    # Note that get_entity_group_version() uses same entity group (root_key)
    # internally and respects transactions. So all data fetched here does indeed
    # correspond to |current_version|.
    return AuthDB(
        replication_state=replication_state_future.get_result(),
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
      logging.info('Fetched AuthDB at rev %d', _auth_db.auth_db_rev)
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
    else:
      logging.info(
          'Updated cached AuthDB: rev %d->%d',
          known_auth_db.auth_db_rev, fresh_copy.auth_db_rev)
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
  """Returns True if |identity| (or current identity if None) is an admin.

  Admins are identities belonging to 'administrators' group
  (see model.ADMIN_GROUP). They have no relation to GAE notion of 'admin'.

  See 'is_superuser' for asserting GAE-level admin access.
  """
  return is_group_member(model.ADMIN_GROUP, identity)


def is_superuser():
  """Returns True if the current caller is GAE-level administrator.

  This works only for requests authenticated via GAE Users API or OAuth APIs.
  """
  return get_request_cache().is_superuser


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


def is_in_ip_whitelist(whitelist_name, ip):
  """Returns True if the given IP belongs to the given IP whitelist.

  Args:
    whitelist_name: name of the IP whitelist (e.g. 'bots').
    ip: instance of ipaddr.IP.
  """
  return get_request_cache().auth_db.is_in_ip_whitelist(whitelist_name, ip)


def verify_ip_whitelisted(identity, ip):
  """Verifies IP is in a whitelist assigned to the Identity.

  This check is used to restrict some callers to particular IP subnets as
  additional security measure.

  Raises AuthorizationError if identity has an IP whitelist assigned and given
  IP address doesn't belong to it.

  Args:
    identity: caller's identity.
    ip: instance of ipaddr.IP.
  """
  get_request_cache().auth_db.verify_ip_whitelisted(identity, ip)


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
      # Redirect to auth bootstrap page only if called by GAE-level admin
      # (only they are capable of running bootstrap), not already bootstrapped
      # (as approximated by is_admin returning False), and not on replica
      # (bootstrap works only on standalone or on primary).
      if not is_superuser() or is_admin() or model.is_replica():
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
