#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

import test_env
test_env.setup_test_env()

from components.auth import model
from support import test_case


class IdentityTest(test_case.TestCase):
  """Tests for Identity class."""

  def test_immutable(self):
    # Note that it's still possible to add new attributes to |ident|. To fix
    # this we'd have to add __slots__ = () to Identity and to BytesSerializable
    # (it inherits from). Since adding extra attributes to an instance doesn't
    # harm any expected behavior of Identity (like equality operator or
    # serialization) we ignore this hole in immutability.
    ident = model.Identity(model.IDENTITY_USER, 'joe@example.com')
    self.assertTrue(isinstance(ident, tuple))
    with self.assertRaises(AttributeError):
      ident.kind = model.IDENTITY_USER
    with self.assertRaises(AttributeError):
      ident.name = 'bob@example.com'

  def test_equality(self):
    # Identities are compared by values, not by reference.
    ident1 = model.Identity(model.IDENTITY_USER, 'joe@example.com')
    ident2 = model.Identity(model.IDENTITY_USER, 'joe@example.com')
    ident3 = model.Identity(model.IDENTITY_USER, 'bob@example.com')
    self.assertEqual(ident1, ident2)
    self.assertNotEqual(ident1, ident3)
    # Verify that adding extra attribute doesn't change equality relation.
    ident1.extra = 1
    ident2.extra = 2
    self.assertEqual(ident1, ident2)

  def test_validation(self):
    # Unicode with ASCII data is ok.
    ok_identities = (
      (unicode(model.IDENTITY_USER), 'joe@example.com'),
      (model.IDENTITY_USER, u'joe@example.com'),
    )
    for kind, name in ok_identities:
      ident = model.Identity(kind, name)
      # Should be 'str', not 'unicode'
      self.assertEqual(type(ident.kind), str)
      self.assertEqual(type(ident.name), str)
      # And data should match.
      self.assertEqual(kind, ident.kind)
      self.assertEqual(name, ident.name)

    # Nasty stuff.
    bad_identities = (
      ('unknown-kind', 'joe@example.com'),
      (model.IDENTITY_ANONYMOUS, 'not-anonymous'),
      (model.IDENTITY_BOT, 'bad bot name - spaces'),
      (model.IDENTITY_SERVICE, 'spaces everywhere'),
      (model.IDENTITY_USER, 'even here'),
      (model.IDENTITY_USER, u'\u043f\u0440\u0438\u0432\u0435\u0442')
    )
    for kind, name in bad_identities:
      with self.assertRaises(ValueError):
        model.Identity(kind, name)

  def test_serialization(self):
    # Identity object goes through serialize-deserialize process unchanged.
    good_cases = (
      model.Identity(model.IDENTITY_USER, 'joe@example.com'),
      model.Anonymous,
    )
    for case in good_cases:
      self.assertEqual(case, model.Identity.from_bytes(case.to_bytes()))

    # Malformed data causes ValueError.
    bad_cases = (
      '',
      'userjoe@example.com',
      'user:',
      ':joe@example.com',
      'user::joe@example.com',
    )
    for case in bad_cases:
      with self.assertRaises(ValueError):
        model.Identity.from_bytes(case)


class IdentityGlobTest(test_case.TestCase):
  """Tests for IdentityGlob class."""

  def test_immutable(self):
    # See comment in IdentityTest.test_immutable regarding existing hole in
    # immutability.
    glob = model.IdentityGlob(model.IDENTITY_USER, '*@example.com')
    self.assertTrue(isinstance(glob, tuple))
    with self.assertRaises(AttributeError):
      glob.kind = model.IDENTITY_USER
    with self.assertRaises(AttributeError):
      glob.pattern = '*@example.com'

  def test_equality(self):
    # IdentityGlobs are compared by values, not by reference.
    glob1 = model.IdentityGlob(model.IDENTITY_USER, '*@example.com')
    glob2 = model.IdentityGlob(model.IDENTITY_USER, '*@example.com')
    glob3 = model.IdentityGlob(model.IDENTITY_USER, '*-sub@example.com')
    self.assertEqual(glob1, glob2)
    self.assertNotEqual(glob1, glob3)
    # Verify that adding extra attribute doesn't change equality relation.
    glob1.extra = 1
    glob2.extra = 2
    self.assertEqual(glob1, glob2)

  def test_validation(self):
    # Unicode with ASCII data is ok.
    ok_globs = (
      (unicode(model.IDENTITY_USER), '*@example.com'),
      (model.IDENTITY_USER, u'*@example.com'),
    )
    for kind, pattern in ok_globs:
      glob = model.IdentityGlob(kind, pattern)
      # Should be 'str', not 'unicode'
      self.assertEqual(type(glob.kind), str)
      self.assertEqual(type(glob.pattern), str)
      # And data should match.
      self.assertEqual(kind, glob.kind)
      self.assertEqual(pattern, glob.pattern)

    # Nasty stuff.
    bad_globs = (
      ('unknown-kind', '*@example.com'),
      (model.IDENTITY_USER, ''),
      (model.IDENTITY_USER, u'\u043f\u0440\u0438\u0432\u0435\u0442')
    )
    for kind, pattern in bad_globs:
      with self.assertRaises(ValueError):
        model.IdentityGlob(kind, pattern)

  def test_serialization(self):
    # IdentityGlob object goes through serialize-deserialize process unchanged.
    glob = model.IdentityGlob(model.IDENTITY_USER, '*@example.com')
    self.assertEqual(glob, model.IdentityGlob.from_bytes(glob.to_bytes()))

    # Malformed data causes ValueError.
    bad_cases = (
      '',
      'user*@example.com',
      'user:',
      ':*@example.com',
    )
    for case in bad_cases:
      with self.assertRaises(ValueError):
        model.IdentityGlob.from_bytes(case)

  def test_match(self):
    glob = model.IdentityGlob(model.IDENTITY_USER, '*@example.com')
    self.assertTrue(
        glob.match(model.Identity(model.IDENTITY_USER, 'a@example.com')))
    self.assertFalse(
        glob.match(model.Identity(model.IDENTITY_BOT, 'a@example.com')))
    self.assertFalse(
        glob.match(model.Identity(model.IDENTITY_USER, 'a@test.com')))


class AccessRuleTest(test_case.TestCase):
  """Tests for AccessRule class."""

  def test_immutable(self):
    # See comment in IdentityTest.test_immutable regarding existing hole in
    # immutability.
    rule = model.AccessRule(model.ALLOW_RULE, 'Group', [model.READ], '^.*$')
    self.assertTrue(isinstance(rule, tuple))
    for attr in ('kind', 'group', 'actions', 'resource'):
      with self.assertRaises(AttributeError):
        setattr(rule, attr, 'value')

  def test_equality(self):
    rule1 = model.AccessRule(model.ALLOW_RULE, 'Group', [model.READ], '^.*$')
    rule2 = model.AccessRule(model.ALLOW_RULE, 'Group', [model.READ], '^.*$')
    rule3 = model.AccessRule(model.ALLOW_RULE, 'Group', [model.UPDATE], '^.*$')
    self.assertEqual(rule1, rule2)
    self.assertNotEqual(rule1, rule3)
    # Ensure |actions| is treated like a set (order is ignored).
    self.assertEqual(
        model.AccessRule(model.ALLOW_RULE, 'Group',
            [model.READ, model.UPDATE, model.READ], '^.*$'),
        model.AccessRule(model.ALLOW_RULE, 'Group',
            [model.UPDATE, model.READ], '^.*$'))

  def test_validation(self):
    # Good case.
    model.AccessRule(model.ALLOW_RULE, 'Group', [model.READ], '^.*$')

    # Bad cases, deviations from good case.
    bad_cases = (
      ('bad-kind', 'Group', [model.READ], '^.*$'),
      (model.ALLOW_RULE, '   Bad Group  ', [model.READ], '^.*$'),
      (model.ALLOW_RULE, 'Group', [], '^.*$'),
      (model.ALLOW_RULE, 'Group', ['BAD_ACTION'], '^.*$'),
      (model.ALLOW_RULE, 'Group', [model.READ], '^bad regexp****$'),
      (model.ALLOW_RULE, 'Group', [model.READ], '^nodollar'),
      (model.ALLOW_RULE, 'Group', [model.READ], 'nohat$'),
    )
    for kind, group, actions, regex in bad_cases:
      with self.assertRaises(ValueError):
        model.AccessRule(kind, group, actions, regex)

  def test_serialization(self):
    # Rule goes though serialization-deserialization unchanged.
    rule = model.AccessRule(model.ALLOW_RULE, 'Group', [model.READ], '^.*$')
    self.assertEqual(rule, model.AccessRule.from_jsonish(rule.to_jsonish()))

    # Missing keys are bad.
    with self.assertRaises(ValueError):
      model.AccessRule.from_jsonish(
          {'group': 'Group', 'kind': 'ALLOW', 'resource': '^.*$'})


class AuthSecretTest(test_case.TestCase):
  """Tests for AuthSecret class."""

  def setUp(self):
    super(AuthSecretTest, self).setUp()
    self.mock(model.logging, 'warning', lambda *_args: None)

  def test_bootstrap_works(self):
    # Creating it for a first time.
    ent1 = model.AuthSecret.bootstrap('test_secret', 'local', length=127)
    self.assertTrue(ent1)
    self.assertEqual(ent1.key.string_id(), 'test_secret')
    self.assertEqual(ent1.key.parent().string_id(), 'local')
    self.assertEqual(1, len(ent1.values))
    self.assertEqual(127, len(ent1.values[0]))
    # Getting same one.
    ent2 = model.AuthSecret.bootstrap('test_secret', 'local')
    self.assertEqual(ent1, ent2)

  def test_bad_key_scope(self):
    with self.assertRaises(ValueError):
      model.AuthSecret.bootstrap('test_secret', 'bad-scope')

  def test_update_keep_key(self):
    # Update, keeping old key around.
    ent = model.AuthSecret.bootstrap('test_secret', 'local')
    ident = model.Identity(model.IDENTITY_USER, 'joe@example.com')
    old_secret = ent.values[0]
    ent.update('new-secret', ident, keep_previous=True, retention=1)
    self.assertEqual(ent.values, ['new-secret', old_secret])
    self.assertEqual(ent.modified_by, ident)

  def test_update_forget_key(self):
    # Update, forgetting old key around.
    ent = model.AuthSecret.bootstrap('test_secret', 'local')
    ident = model.Identity(model.IDENTITY_USER, 'joe@example.com')
    ent.update('new-secret', ident, keep_previous=False, retention=1)
    self.assertEqual(ent.values, ['new-secret'])
    self.assertEqual(ent.modified_by, ident)

  def test_update_retention(self):
    ent = model.AuthSecret.bootstrap('test_secret', 'local')
    ident = model.Identity(model.IDENTITY_USER, 'joe@example.com')
    old_secret = ent.values[0]
    ent.update('new-secret-1', ident, keep_previous=True, retention=2)
    ent.update('new-secret-2', ident, keep_previous=True, retention=2)
    self.assertEqual(ent.values, ['new-secret-2', 'new-secret-1', old_secret])
    self.assertEqual(ent.modified_by, ident)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
