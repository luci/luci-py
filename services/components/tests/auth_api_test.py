#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

# Disable 'Access to a protected member', Unused argument', 'Unused variable'.
# pylint: disable=W0212,W0612,W0613


import Queue
import sys
import threading
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

import test_case
from components.auth import api
from components.auth import model


class AuthDBTest(test_case.TestCase):
  """Tests for AuthDB class."""

  def setUp(self):
    super(AuthDBTest, self).setUp()
    self.mock(api.logging, 'warning', lambda *_args: None)

  def test_is_group_member(self):
    # Test identity.
    joe = model.Identity(model.IDENTITY_USER, 'joe@example.com')

    # Group that includes joe via glob.
    with_glob = model.AuthGroup(id='WithGlob')
    with_glob.globs.append(
        model.IdentityGlob(model.IDENTITY_USER, '*@example.com'))

    # Group that includes joe via explicit listing.
    with_listing = model.AuthGroup(id='WithListing')
    with_listing.members.append(joe)

    # Group that includes joe via nested group.
    with_nesting = model.AuthGroup(id='WithNesting')
    with_nesting.nested.append('WithListing')

    # Creates AuthDB with given list of groups and then runs the check.
    is_member = (lambda groups, identity, group:
        api.AuthDB(groups=groups).is_group_member(identity, group))

    # Wildcard group includes everyone (even anonymous).
    self.assertTrue(is_member([], joe, '*'))
    self.assertTrue(is_member([], model.Anonymous, '*'))

    # An unknown group includes nobody.
    self.assertFalse(is_member([], joe, 'Missing'))
    self.assertFalse(is_member([], model.Anonymous, 'Missing'))

    # Globs are respected.
    self.assertTrue(is_member([with_glob], joe, 'WithGlob'))
    self.assertFalse(is_member([with_glob], model.Anonymous, 'WithGlob'))

    # Members lists are respected.
    self.assertTrue(is_member([with_listing], joe, 'WithListing'))
    self.assertFalse(is_member([with_listing], model.Anonymous, 'WithListing'))

    # Nested groups are respected.
    self.assertTrue(is_member([with_nesting, with_listing], joe, 'WithNesting'))
    self.assertFalse(
        is_member([with_nesting, with_listing], model.Anonymous, 'WithNesting'))

  def test_nested_groups_cycle(self):
    # Groups that nest each other.
    group1 = model.AuthGroup(id='Group1')
    group1.nested.append('Group2')
    group2 = model.AuthGroup(id='Group2')
    group2.nested.append('Group1')

    # Collect error messages.
    errors = []
    self.mock(api.logging, 'error', lambda *args: errors.append(args))

    # This should not hang, but produce error message.
    auth_db = api.AuthDB(groups=[group1, group2])
    self.assertFalse(
        auth_db.is_group_member(model.Anonymous, 'Group1'))
    self.assertEqual(1, len(errors))

  def test_get_groups(self):
    groups = [
      model.AuthGroup(id='Group1', members=[model.Anonymous]),
      model.AuthGroup(id='Group2'),
    ]
    auth_db = api.AuthDB(groups=groups)

    # No identity given -> all known groups.
    self.assertEqual(set(['Group1', 'Group2']), auth_db.get_groups())
    # With identity -> only groups that identity belongs to.
    self.assertEqual(set(['Group1']), auth_db.get_groups(model.Anonymous))

  def test_get_matching_rules(self):
    # Groups.
    joe = model.Identity(model.IDENTITY_USER, 'joe@example.com')
    groups = [
      model.AuthGroup(id='WithJoe', members=[joe]),
      model.AuthGroup(id='WithoutJoe'),
    ]

    # Rules.
    allow_joe_create = model.AccessRule(
        model.ALLOW_RULE, 'WithJoe', [model.CREATE], '^existing$')
    allow_others_create = model.AccessRule(
        model.ALLOW_RULE, 'WithoutJoe', [model.CREATE], '^existing$')
    allow_joe_update = model.AccessRule(
        model.ALLOW_RULE, 'WithJoe', [model.UPDATE], '^existing$')
    allow_others_update = model.AccessRule(
        model.ALLOW_RULE, 'WithoutJoe', [model.UPDATE], '^existing$')
    another_resource = model.AccessRule(
        model.ALLOW_RULE, 'WithoutJoe', [model.DELETE], '^another-.*$')

    rules = [
      allow_joe_create,
      allow_joe_update,
      allow_others_create,
      allow_others_update,
      another_resource,
    ]

    # AuthDB with groups and rules.
    auth_db = api.AuthDB(
        groups=groups,
        service_config=model.AuthServiceConfig(rules=rules))

    # If no query is given, returns all defined rules.
    self.assertEqual(rules, auth_db.get_matching_rules())
    # If full query is given and there's a matching rule, return only that rule.
    self.assertEqual(
        [allow_joe_create],
        auth_db.get_matching_rules(joe, model.CREATE, 'existing'))
    # If full query is given, and there is NO matching rule, return deny-all.
    self.assertEqual(
        [model.AccessRule(model.DENY_RULE, '*', model.ALLOWED_ACTIONS, '^.*$')],
        auth_db.get_matching_rules(joe, model.CREATE, 'non-existing'))

    # Only identity is given.
    self.assertEqual(
        [allow_joe_create, allow_joe_update],
        auth_db.get_matching_rules(identity=joe))
    # Only action is given.
    self.assertEqual(
        [allow_joe_create, allow_others_create],
        auth_db.get_matching_rules(action=model.CREATE))
    # Only resource is given.
    self.assertEqual(
        [another_resource],
        auth_db.get_matching_rules(resource='another-resource'))

  def test_has_permission(self):
    # Groups.
    joe = model.Identity(model.IDENTITY_USER, 'joe@example.com')
    bob = model.Identity(model.IDENTITY_USER, 'bob@example.com')
    tom = model.Identity(model.IDENTITY_USER, 'tom@example.com')
    ned = model.Identity(model.IDENTITY_USER, 'ned@example.com')
    groups = [
      model.AuthGroup(id='Read', members=[joe, bob]),
      model.AuthGroup(id='Write', members=[joe]),
      model.AuthGroup(id='Ninjas', members=[tom]),
      model.AuthGroup(id='Banned', members=[ned]),
    ]

    # Helpers to reduce amount of typing.
    def make_rule(kind, group, actions, resource):
      letter_to_action = {
        'C': model.CREATE,
        'D': model.DELETE,
        'R': model.READ,
        'U': model.UPDATE,
      }
      return model.AccessRule(
          kind, group, [letter_to_action[c] for c in actions], resource)

    def allow(group, actions, resource):
      return make_rule(model.ALLOW_RULE, group, actions, resource)

    def deny(group, actions, resource):
      return make_rule(model.DENY_RULE, group, actions, resource)

    # Rules.
    rules = [
      deny('Banned', 'CRUD', '^.*$'),
      allow('Ninjas', 'CRUD', '^.*$'),
      allow('Read', 'R', '^public/(.*)$'),
      allow('Write', 'CRUD', '^public/(.*)$'),
    ]

    # AuthDB with groups and rules.
    auth_db = api.AuthDB(
        groups=groups,
        service_config=model.AuthServiceConfig(rules=rules))

    # Joe's permission.
    self.assertEqual(set(['Read', 'Write']), auth_db.get_groups(joe))
    self.assertTrue(auth_db.has_permission(joe, model.READ, 'public/stuff'))
    self.assertTrue(auth_db.has_permission(joe, model.CREATE, 'public/stuff'))
    self.assertFalse(auth_db.has_permission(joe, model.READ, 'private/stuff'))

    # Bob's permission.
    self.assertEqual(set(['Read']), auth_db.get_groups(bob))
    self.assertTrue(auth_db.has_permission(bob, model.READ, 'public/stuff'))
    self.assertFalse(auth_db.has_permission(bob, model.CREATE, 'public/stuff'))
    self.assertFalse(auth_db.has_permission(bob, model.READ, 'private/stuff'))

    # Tom's permission.
    self.assertEqual(set(['Ninjas']), auth_db.get_groups(tom))
    self.assertTrue(auth_db.has_permission(tom, model.READ, 'public/stuff'))
    self.assertTrue(auth_db.has_permission(tom, model.CREATE, 'public/stuff'))
    self.assertTrue(auth_db.has_permission(tom, model.READ, 'private/stuff'))

    # Ned's permission.
    self.assertEqual(set(['Banned']), auth_db.get_groups(ned))
    self.assertFalse(auth_db.has_permission(ned, model.READ, 'public/stuff'))
    self.assertFalse(auth_db.has_permission(ned, model.CREATE, 'public/stuff'))
    self.assertFalse(auth_db.has_permission(ned, model.READ, 'private/stuff'))

    # Anonymous permissions.
    anon = model.Anonymous
    self.assertEqual(set(), auth_db.get_groups(anon))
    self.assertFalse(auth_db.has_permission(anon, model.READ, 'public/stuff'))
    self.assertFalse(auth_db.has_permission(anon, model.CREATE, 'public/stuff'))
    self.assertFalse(auth_db.has_permission(anon, model.READ, 'private/stuff'))

  def test_is_allowed_oauth_client_id(self):
    global_config = model.AuthGlobalConfig(
        oauth_client_id='1',
        oauth_additional_client_ids=['2', '3'])
    auth_db = api.AuthDB(global_config=global_config)
    self.assertFalse(auth_db.is_allowed_oauth_client_id(None))
    self.assertTrue(auth_db.is_allowed_oauth_client_id('1'))
    self.assertTrue(auth_db.is_allowed_oauth_client_id('2'))
    self.assertTrue(auth_db.is_allowed_oauth_client_id('3'))
    self.assertFalse(auth_db.is_allowed_oauth_client_id('4'))

  def test_fetch_auth_db_lazy_bootstrap(self):
    local_conf_key = ndb.Key(
        model.AuthServiceConfig, 'local', parent=model.ROOT_KEY)

    # Don't exist before the call.
    self.assertFalse(model.ROOT_KEY.get())
    self.assertFalse(local_conf_key.get())

    # Run bootstrap.
    api._lazy_bootstrap_ran = False
    api.fetch_auth_db()

    # Exist now.
    self.assertTrue(model.ROOT_KEY.get())
    self.assertTrue(local_conf_key.get())

    # 'Allow all' rule is set.
    self.assertEqual([model.AllowAllRule], local_conf_key.get().rules)

  def test_fetch_auth_db(self):
    # Create AuthGlobalConfig.
    global_config = model.AuthGlobalConfig(key=model.ROOT_KEY)
    global_config.oauth_client_id = '1'
    global_config.oauth_client_secret = 'secret'
    global_config.oauth_additional_client_ids = ['2', '3']
    global_config.put()

    # Create local AuthServiceConfig.
    service_config = model.AuthServiceConfig(id='local', parent=model.ROOT_KEY)
    service_config.rules = [
      model.AccessRule(model.ALLOW_RULE, '*', model.ALLOWED_ACTIONS, '^.*$'),
    ]
    service_config.put()

    # Create a bunch of (empty) groups.
    groups = [
      model.AuthGroup(id='Group A', parent=model.ROOT_KEY),
      model.AuthGroup(id='Group B', parent=model.ROOT_KEY),
    ]
    for group in groups:
      group.put()

    # And a bunch of secrets (local and global).
    local_secrets = [
        model.AuthSecret.bootstrap('local%d' % i, 'local') for i in (0, 1, 2)
    ]
    global_secrets = [
        model.AuthSecret.bootstrap('global%d' % i, 'global') for i in (0, 1, 2)
    ]

    # This all stuff should be fetched into AuthDB.
    auth_db = api.fetch_auth_db()
    self.assertEqual(global_config, auth_db.global_config)
    self.assertEqual(service_config, auth_db.service_config)
    self.assertEqual(
        set(g.key.id() for g in groups),
        set(auth_db.groups))
    self.assertEqual(
        set(s.key.id() for s in local_secrets),
        set(auth_db.secrets['local']))
    self.assertEqual(
        set(s.key.id() for s in global_secrets),
        set(auth_db.secrets['global']))

  def test_get_secret(self):
    # Make AuthDB with two secrets.
    local_secret = model.AuthSecret.bootstrap('local_secret', 'local')
    global_secret = model.AuthSecret.bootstrap('global_secret', 'global')
    auth_db = api.AuthDB(secrets=[local_secret, global_secret])

    # Ensure they are accessible via get_secret.
    self.assertEqual(
        local_secret.values,
        auth_db.get_secret(api.SecretKey('local_secret', 'local')))
    self.assertEqual(
        global_secret.values,
        auth_db.get_secret(api.SecretKey('global_secret', 'global')))

  def test_get_secret_bootstrap(self):
    # Mock AuthSecret.bootstrap to capture calls to it.
    original = api.model.AuthSecret.bootstrap
    calls = []
    @classmethod
    def mocked_bootstrap(cls, name, scope):
      calls.append((name, scope))
      result = original(name, scope)
      result.values = ['123']
      return result
    self.mock(api.model.AuthSecret, 'bootstrap', mocked_bootstrap)

    auth_db = api.AuthDB()
    got = auth_db.get_secret(api.SecretKey('local_secret', 'local'))
    self.assertEqual(['123'], got)
    self.assertEqual([('local_secret', 'local')], calls)

  def test_get_secret_bad_scope(self):
    with self.assertRaises(ValueError):
      api.AuthDB().get_secret(api.SecretKey('some', 'bad-scope'))


class TestAuthDBCache(test_case.TestCase):
  """Tests for process-global and request-local AuthDB cache."""

  def setUp(self):
    super(TestAuthDBCache, self).setUp()
    api.reset_local_state()

  def set_time(self, ts):
    """Mocks time.time() to return |ts|."""
    self.mock(api.time, 'time', lambda: ts)

  def set_fetched_auth_db(self, auth_db):
    """Mocks fetch_auth_db to return |auth_db|."""
    def mock_fetch_auth_db(known_version=None):
      if (known_version is not None and
          auth_db.entity_group_version == known_version):
        return None
      return auth_db
    self.mock(api, 'fetch_auth_db', mock_fetch_auth_db)

  def test_get_request_cache_different_threads(self):
    """Ensure get_request_cache() respects multiple threads."""
    # Runs in its own thread.
    def thread_proc():
      # get_request_cache() returns something meaningful.
      request_cache = api.get_request_cache()
      self.assertTrue(request_cache)
      # Returns same object in a context of a same request thread.
      self.assertTrue(api.get_request_cache() is request_cache)
      return request_cache

    # Launch two threads running 'thread_proc', wait for them to stop, collect
    # whatever they return.
    results_queue = Queue.Queue()
    threads = [
      threading.Thread(target=lambda: results_queue.put(thread_proc()))
      for _ in xrange(2)
    ]
    for t in threads:
      t.start()
    results = [results_queue.get() for _ in xrange(len(threads))]

    # Different threads use different RequestCache objects.
    self.assertTrue(results[0] is not results[1])

  def test_get_request_cache_different_requests(self):
    """Ensure get_request_cache() returns new object for a new request."""
    # Grab request cache for 'current' request.
    request_cache = api.get_request_cache()

    # Track calls to 'close'.
    close_calls = []
    self.mock(request_cache, 'close', lambda: close_calls.append(1))

    # Restart testbed, effectively emulating a new request on a same thread.
    self.testbed.deactivate()
    self.testbed.activate()

    # Should return a new instance of request cache now.
    self.assertTrue(api.get_request_cache() is not request_cache)
    # Old one should have been closed.
    self.assertEqual(1, len(close_calls))

  def test_get_process_auth_db_expiration(self):
    """Ensure get_process_auth_db() respects expiration."""
    # Prepare several instances of AuthDB to be used in mocks.
    auth_db_v0 = api.AuthDB(entity_group_version=0)
    auth_db_v1 = api.AuthDB(entity_group_version=1)

    # Fetch initial copy of AuthDB.
    self.set_time(0)
    self.set_fetched_auth_db(auth_db_v0)
    self.assertEqual(auth_db_v0, api.get_process_auth_db())

    # It doesn't expire for some time.
    self.set_time(api.PROCESS_CACHE_EXPIRATION_SEC - 1)
    self.set_fetched_auth_db(auth_db_v1)
    self.assertEqual(auth_db_v0, api.get_process_auth_db())

    # But eventually it does.
    self.set_time(api.PROCESS_CACHE_EXPIRATION_SEC + 1)
    self.set_fetched_auth_db(auth_db_v1)
    self.assertEqual(auth_db_v1, api.get_process_auth_db())

  def test_get_process_auth_db_known_version(self):
    """Ensure get_process_auth_db() respects entity group version."""
    # Prepare several instances of AuthDB to be used in mocks.
    auth_db_v0 = api.AuthDB(entity_group_version=0)
    auth_db_v0_again = api.AuthDB(entity_group_version=0)

    # Fetch initial copy of AuthDB.
    self.set_time(0)
    self.set_fetched_auth_db(auth_db_v0)
    self.assertEqual(auth_db_v0, api.get_process_auth_db())

    # Make cache expire, but setup fetch_auth_db to return a new instance of
    # AuthDB, but with same entity group version. Old known instance of AuthDB
    # should be reused.
    self.set_time(api.PROCESS_CACHE_EXPIRATION_SEC + 1)
    self.set_fetched_auth_db(auth_db_v0_again)
    self.assertTrue(api.get_process_auth_db() is auth_db_v0)

  def test_get_process_auth_db_multithreading(self):
    """Ensure get_process_auth_db() plays nice with multiple threads."""

    def run_in_thread(func):
      """Runs |func| in a parallel thread, returns future (as Queue)."""
      result = Queue.Queue()
      thread = threading.Thread(target=lambda: result.put(func()))
      thread.start()
      return result

    # Prepare several instances of AuthDB to be used in mocks.
    auth_db_v0 = api.AuthDB(entity_group_version=0)
    auth_db_v1 = api.AuthDB(entity_group_version=1)

    # Run initial fetch, should cache |auth_db_v0| in process cache.
    self.set_time(0)
    self.set_fetched_auth_db(auth_db_v0)
    self.assertEqual(auth_db_v0, api.get_process_auth_db())

    # Make process cache expire.
    self.set_time(api.PROCESS_CACHE_EXPIRATION_SEC + 1)

    # Start fetching AuthDB from another thread, at some point it will call
    # 'fetch_auth_db', and we pause the thread then and resume main thread.
    fetching_now = threading.Event()
    auth_db_queue = Queue.Queue()
    def mock_fetch_auth_db(**_kwargs):
      fetching_now.set()
      return auth_db_queue.get()
    self.mock(api, 'fetch_auth_db', mock_fetch_auth_db)
    future = run_in_thread(api.get_process_auth_db)

    # Wait for internal thread to call |fetch_auth_db|.
    fetching_now.wait()

    # Ok, now main thread is unblocked, while internal thread is blocking on a
    # artificially slow 'fetch_auth_db' call. Main thread can now try to get
    # AuthDB via get_process_auth_db(). It should get older stale copy right
    # away.
    self.assertEqual(auth_db_v0, api.get_process_auth_db())

    # Finish background 'fetch_auth_db' call by returning 'auth_db_v1'.
    # That's what internal thread should get as result of 'get_process_auth_db'.
    auth_db_queue.put(auth_db_v1)
    self.assertEqual(auth_db_v1, future.get())

    # Now main thread should get it as well.
    self.assertEqual(auth_db_v1, api.get_process_auth_db())

  def test_get_process_auth_db_exceptions(self):
    """Ensure get_process_auth_db() handles DB exceptions well."""
    # Prepare several instances of AuthDB to be used in mocks.
    auth_db_v0 = api.AuthDB(entity_group_version=0)
    auth_db_v1 = api.AuthDB(entity_group_version=1)

    # Fetch initial copy of AuthDB.
    self.set_time(0)
    self.set_fetched_auth_db(auth_db_v0)
    self.assertEqual(auth_db_v0, api.get_process_auth_db())

    # Make process cache expire.
    self.set_time(api.PROCESS_CACHE_EXPIRATION_SEC + 1)

    # Emulate an exception in fetch_auth_db.
    def mock_fetch_auth_db(*_kwargs):
      raise Exception('Boom!')
    self.mock(api, 'fetch_auth_db', mock_fetch_auth_db)

    # Capture calls to logging.exception.
    logger_calls = []
    self.mock(api.logging, 'exception', lambda *_args: logger_calls.append(1))

    # Should return older copy of auth_db_v0 and log the exception.
    self.assertEqual(auth_db_v0, api.get_process_auth_db())
    self.assertEqual(1, len(logger_calls))

    # Make fetch_auth_db to work again. Verify get_process_auth_db() works too.
    self.set_fetched_auth_db(auth_db_v1)
    self.assertEqual(auth_db_v1, api.get_process_auth_db())

  def test_get_request_auth_db(self):
    """Ensure get_request_auth_db() caches AuthDB in request cache."""
    # 'get_request_auth_db()' returns whatever get_process_auth_db() returns
    # when called for a first time.
    self.mock(api, 'get_process_auth_db', lambda: 'fake')
    self.assertEqual('fake', api.get_request_auth_db())

    # But then it caches it locally and reuses local copy, instead of calling
    # 'get_process_auth_db()' all the time.
    self.mock(api, 'get_process_auth_db', lambda: 'another-fake')
    self.assertEqual('fake', api.get_request_auth_db())

  def test_warmup(self):
    """Ensure api.warmup() fetches AuthDB into process-global cache."""
    self.assertFalse(api._auth_db)
    api.warmup()
    self.assertTrue(api._auth_db)


class ApiTest(test_case.TestCase):
  """Test for publicly exported API."""

  def mock_has_permission(self, permissions):
    """Setup a mock for api.has_permission that collects calls."""
    calls = []
    def mocked(action, resource):
      calls.append((action, resource))
      return permissions[(action, resource)]
    self.mock(api, 'has_permission', mocked)
    return calls

  def test_get_current_identity_unitialized(self):
    """If set_current_identity wasn't called raises an exception."""
    with self.assertRaises(api.UninitializedError):
      api.get_current_identity()

  def test_get_current_identity(self):
    """Ensure get_current_identity returns whatever was put in request cache."""
    api.get_request_cache().set_current_identity(model.Anonymous)
    self.assertEqual(model.Anonymous, api.get_current_identity())

  def test_has_permission_uninitialized(self):
    """If set_current_identity wasn't called raises an exception."""
    with self.assertRaises(api.UninitializedError):
      api.has_permission(model.READ, 'stuff')

  def test_has_permission(self):
    """Ensure has_permission uses AuthDB."""
    calls = []
    def mock_auth_db_has_permission(identity, action, resource):
      calls.append((identity, action, resource))
      return True
    auth_db = api.AuthDB()
    self.mock(auth_db, 'has_permission', mock_auth_db_has_permission)
    self.mock(api, 'get_request_auth_db', lambda: auth_db)

    # Ensure api.has_permission uses mocked AuthDB.has_permission.
    api.get_request_cache().set_current_identity(model.Anonymous)
    self.assertTrue(api.has_permission(model.READ, 'some'))
    self.assertEqual([(model.Anonymous, model.READ, 'some')], calls)

  def test_require_decorator_ok(self):
    """@require calls 'has_permissions' and then decorated function."""
    calls = self.mock_has_permission({(model.READ, 'some-resource'): True})

    @api.require(model.READ, 'some-resource')
    def allowed(*args, **kwargs):
      return (args, kwargs)
    self.assertEqual(
        [(model.READ, 'some-resource')], api.get_require_decorators(allowed))
    self.assertEqual(((1, 2), {'a': 3}), allowed(1, 2, a=3))
    self.assertEqual([(model.READ, 'some-resource')], calls)

  def test_require_decorator_fail(self):
    """@require raises exception and doesn't call decorated function."""
    calls = self.mock_has_permission({(model.DELETE, 'some-resource'): False})
    forbidden_calls = []

    @api.require(model.DELETE, 'some-resource')
    def forbidden():
      forbidden_calls.append(1)
    with self.assertRaises(api.AuthorizationError):
      forbidden()
    self.assertFalse(forbidden_calls)
    self.assertEqual([(model.DELETE, 'some-resource')], calls)

  def test_require_decorator_nesting_ok(self):
    """Permission checks are called in order."""
    calls = self.mock_has_permission({
      (model.READ, 'A/value'): True,
      (model.READ, 'B/value'): True,
    })

    @api.require(model.READ, 'A/{arg}')
    @api.require(model.READ, 'B/{arg}')
    def allowed(arg):
      return arg
    self.assertEqual(
        [(model.READ, 'A/{arg}'), (model.READ, 'B/{arg}')],
        api.get_require_decorators(allowed))
    self.assertEqual('value', allowed('value'))
    self.assertEqual([(model.READ, 'A/value'), (model.READ, 'B/value')], calls)

  def test_require_decorator_nesting_first_deny(self):
    """First deny raises AuthorizationError."""
    calls = self.mock_has_permission({
      (model.DELETE, 'A/value'): False,
      (model.READ, 'B/value'): True,
    })
    forbidden_calls = []

    @api.require(model.DELETE, 'A/{arg}')
    @api.require(model.READ, 'B/{arg}')
    def forbidden(arg):
      forbidden_calls.append(1)
    with self.assertRaises(api.AuthorizationError):
      forbidden('value')
    self.assertFalse(forbidden_calls)
    self.assertEqual([(model.DELETE, 'A/value')], calls)

  def test_require_decorator_nesting_non_first_deny(self):
    """Non-first deny also raises AuthorizationError."""
    calls = self.mock_has_permission({
      (model.READ, 'A/value'): True,
      (model.DELETE, 'B/value'): False,
    })
    forbidden_calls = []

    @api.require(model.READ, 'A/{arg}')
    @api.require(model.DELETE, 'B/{arg}')
    def forbidden(arg):
      forbidden_calls.append(1)
    with self.assertRaises(api.AuthorizationError):
      forbidden('value')
    self.assertFalse(forbidden_calls)
    self.assertEqual(
        [(model.READ, 'A/value'), (model.DELETE, 'B/value')], calls)

  def test_require_decorator_on_method(self):
    calls = self.mock_has_permission({(model.READ, 'value'): True})

    class Class(object):
      @api.require(model.READ, '{arg}')
      def method(self, arg):
        return (self, arg)

    obj = Class()
    self.assertEqual((obj, 'value'), obj.method('value'))
    self.assertEqual([(model.READ, 'value')], calls)

  def test_require_decorator_on_static_method(self):
    calls = self.mock_has_permission({(model.READ, 'value'): True})

    class Class(object):
      @staticmethod
      @api.require(model.READ, '{arg}')
      def static_method(arg):
        return arg

    obj = Class()
    self.assertEqual('value', Class.static_method('value'))
    self.assertEqual([(model.READ, 'value')], calls)

  def test_require_decorator_on_class_method(self):
    calls = self.mock_has_permission({(model.READ, 'value'): True})

    class Class(object):
      @classmethod
      @api.require(model.READ, '{arg}')
      def class_method(cls, arg):
        return (cls, arg)

    obj = Class()
    self.assertEqual((Class, 'value'), Class.class_method('value'))
    self.assertEqual([(model.READ, 'value')], calls)

  def test_require_decorator_ndb_nesting_require_first(self):
    calls = self.mock_has_permission({(model.READ, 'value'): True})

    @api.require(model.READ, '{arg}')
    @ndb.non_transactional
    def func(arg):
      return arg
    self.assertEqual([(model.READ, '{arg}')], api.get_require_decorators(func))
    self.assertEqual('value', func('value'))
    self.assertEqual([(model.READ, 'value')], calls)

  def test_require_decorator_ndb_nesting_require_last(self):
    calls = self.mock_has_permission({(model.READ, 'value'): True})

    @ndb.non_transactional
    @api.require(model.READ, '{arg}')
    def func(arg):
      return arg
    self.assertEqual([(model.READ, '{arg}')], api.get_require_decorators(func))
    self.assertEqual('value', func('value'))
    self.assertEqual([(model.READ, 'value')], calls)

  def test_public_then_require_fails(self):
    with self.assertRaises(TypeError):
      @api.public
      @api.require(model.READ, 'some')
      def func():
        pass

  def test_require_then_public_fails(self):
    with self.assertRaises(TypeError):
      @api.require(model.READ, 'some')
      @api.public
      def func():
        pass

  def test_is_decorated(self):
    self.assertTrue(api.is_decorated(api.public(lambda: None)))
    self.assertTrue(
        api.is_decorated(api.require(model.READ, 'some')(lambda: None)))

  def test_get_require_decorators_on_undecorated(self):
    self.assertEqual([], api.get_require_decorators(lambda: None))

  def test_get_require_decorators_on_public(self):
    self.assertEqual([], api.get_require_decorators(api.public(lambda: None)))


class TestResourceTemplateRenderer(test_case.TestCase):
  """Tests for get_template_renderer function."""

  def test_rejects_positional_format(self):
    with self.assertRaises(ValueError):
      api.get_template_renderer(lambda arg: None, '{0}')

  def test_unknown_vars(self):
    api.get_template_renderer(lambda arg: None, '{arg}')
    with self.assertRaises(TypeError):
      api.get_template_renderer(lambda another_arg: None, '{arg}')
    with self.assertRaises(TypeError):
      api.get_template_renderer(lambda *arg: None, '{arg}')
    with self.assertRaises(TypeError):
      api.get_template_renderer(lambda **kwargs: None, '{kwargs}')

  def test_no_args(self):
    renderer = api.get_template_renderer(lambda: None, 'not-a-template')
    self.assertEqual('not-a-template', renderer())

  def test_positional_args(self):
    renderer = api.get_template_renderer(lambda a, b, c: None, '{a}/{b}/{c}')
    self.assertEqual('1/2/3', renderer(1, 2, 3))

  def test_positional_and_default_args(self):
    renderer = api.get_template_renderer(lambda a, b, c=3: None, '{a}/{b}/{c}')
    self.assertEqual('1/2/3', renderer(1, 2))
    self.assertEqual('1/2/4', renderer(1, 2, 4))

  def test_keyword_args(self):
    renderer = api.get_template_renderer(lambda a, b, c: None, '{a}/{b}/{c}')
    self.assertEqual('1/2/3', renderer(a=1, b=2, c=3))

  def test_keyword_and_default_args(self):
    renderer = api.get_template_renderer(lambda a, b, c=3: None, '{a}/{b}/{c}')
    self.assertEqual('1/2/3', renderer(a=1, b=2))
    self.assertEqual('1/2/4', renderer(a=1, b=2, c=4))

  def test_positional_and_keyword_args(self):
    renderer = api.get_template_renderer(lambda a, b, c: None, '{a}/{b}/{c}')
    self.assertEqual('1/2/3', renderer(1, 2, c=3))

  def test_positional_and_keyword_and_default_args(self):
    renderer = api.get_template_renderer(lambda a, b, c=3: None, '{a}/{b}/{c}')
    self.assertEqual('1/2/3', renderer(1, b=2))
    self.assertEqual('1/2/4', renderer(1, b=2, c=4))

  def test_many_defaults(self):
    renderer = api.get_template_renderer(
        lambda a=1, b=2, c=3: None, '{a}/{b}/{c}')
    self.assertEqual('1/2/3', renderer())
    self.assertEqual('4/2/3', renderer(4))
    self.assertEqual('4/2/3', renderer(a=4))
    self.assertEqual('1/4/3', renderer(b=4))
    self.assertEqual('1/2/4', renderer(c=4))

  def test_extra_args(self):
    renderer = api.get_template_renderer(lambda a, b, c=3: None, '{c}')
    self.assertEqual('3', renderer(1, 2))
    self.assertEqual('4', renderer(1, 2, 4))
    self.assertEqual('4', renderer(1, 2, c=4))

  def test_missing_args(self):
    renderer = api.get_template_renderer(lambda a, b, c: None, '{a}/{b}/{c}')
    with self.assertRaises(TypeError):
      renderer(1, 2)

  def test_args_placeholder(self):
    renderer = api.get_template_renderer(lambda a, b, *args: None, '{a}/{b}')
    self.assertEqual('1/2', renderer(1, 2))
    self.assertEqual('1/2', renderer(1, 2, 3, 4, 5))
    self.assertEqual('1/2', renderer(1, b=2))

  def test_kwargs_placeholder(self):
    renderer = api.get_template_renderer(lambda a, b, **kwargs: None, '{a}/{b}')
    self.assertEqual('1/2', renderer(1, 2))
    self.assertEqual('1/2', renderer(1, 2, c=3, d=4))
    self.assertEqual('1/2', renderer(1, b=2))
    self.assertEqual('1/2', renderer(1, b=2, c=3, d=4))

  def test_positional_keyword_args_overlap(self):
    # Attempting to specify 'c' twice: as positional and as keyword arg.
    renderer = api.get_template_renderer(
        lambda a, b, c=3, d=4: None, '{a}/{b}/{c}/{d}')
    with self.assertRaises(TypeError):
      renderer(1, 2, 'x', c=3)

  def test_index_in_format_string(self):
    renderer = api.get_template_renderer(lambda a: None, '{a[0]}')
    self.assertEqual('1', renderer([1]))

  def test_attribute_in_format_string(self):
    renderer = api.get_template_renderer(lambda a: None, '{a.name}')
    self.assertEqual(
        'joe@example.com',
        renderer(model.Identity(model.IDENTITY_USER, 'joe@example.com')))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
