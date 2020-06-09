#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from proto import realms_config_pb2
from realms import permissions
from test_support import test_case


class BuilderTest(test_case.TestCase):

  def setUp(self):
    super(BuilderTest, self).setUp()
    self.builder = permissions.Builder('rev')
    self.permission = self.builder.permission
    self.include = self.builder.include
    self.role = self.builder.role

  def check(self, perms=None, roles=None):
    db = self.builder.finish()
    self.assertEquals(db.revision, 'rev')
    if perms is not None:
      self.assertEquals(sorted(db.permissions), perms)
    if roles is not None:
      self.assertEquals(
          db.roles,
          {n: permissions.Role(n, perms) for n, perms in roles.items()})

  def test_empty(self):
    self.check([], {})

  def test_permissions_only(self):
    self.permission('luci.dev.p1')
    self.permission('luci.dev.p2')
    self.permission('luci.dev.p1')  # redeclaration is ok
    self.check(perms=['luci.dev.p1', 'luci.dev.p2'])

  def test_bad_permission_name(self):
    with self.assertRaises(ValueError):
      self.permission('luci.dev')
    with self.assertRaises(ValueError):
      self.permission('luci.dev.something.something')

  def test_simple_role(self):
    self.role('role/dev.a', [
        self.permission('luci.dev.p1'),
        self.permission('luci.dev.p2'),
    ])
    self.check(
        perms=['luci.dev.p1', 'luci.dev.p2'],
        roles={'role/dev.a': ('luci.dev.p1', 'luci.dev.p2')})

  def test_complex_role(self):
    self.role('role/dev.a', [
        self.permission('luci.dev.p1'),
        self.permission('luci.dev.p2'),
    ])
    self.role('role/dev.b', [
        self.permission('luci.dev.p2'),
        self.permission('luci.dev.p3'),
        self.include('role/dev.a'),
    ])
    self.check(
        perms=['luci.dev.p1', 'luci.dev.p2', 'luci.dev.p3'],
        roles={
            'role/dev.a': ('luci.dev.p1', 'luci.dev.p2'),
            'role/dev.b': ('luci.dev.p1', 'luci.dev.p2', 'luci.dev.p3'),
        })

  def test_role_redeclaration(self):
    self.role('role/dev.a', [])
    with self.assertRaises(ValueError):
      self.role('role/dev.a', [])

  def test_bad_role_name(self):
    with self.assertRaises(ValueError):
      self.role('zzz/role', [])

  def test_referencing_undeclared_role(self):
    with self.assertRaises(ValueError):
      self.include('role/zzz')

  def test_non_idempotent_perm(self):
    self.permission('luci.dev.p1')
    self.permission('luci.dev.p1')
    with self.assertRaises(ValueError):
      self.permission('luci.dev.p1', internal=True)


class HardcodedDBTest(test_case.TestCase):

  def test_can_be_built(self):
    db = permissions.db()
    for b in db.implicit_root_bindings('proj'):
      self.assertIsInstance(b, realms_config_pb2.Binding)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
