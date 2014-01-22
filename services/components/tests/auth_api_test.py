#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
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


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
