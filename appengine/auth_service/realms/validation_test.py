#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from test_support import test_case

from components.config import validation as cfgvalidation

from proto import realms_config_pb2

from realms import permissions
from realms import validation


def test_db():
  b = permissions.Builder('auth-code-rev')
  b.role('role/dev.a', [
      b.permission('luci.dev.p1'),
      b.permission('luci.dev.p2'),
  ])
  b.role('role/dev.b', [
      b.permission('luci.dev.p2'),
      b.permission('luci.dev.p3'),
  ])
  b.role('role/luci.internal.int', [
      b.permission('luci.dev.int', internal=True),
  ])
  return b.finish()


class ValidationTest(test_case.TestCase):
  def call(self, fields, allow_internal=False):
    ctx = cfgvalidation.Context()
    val = validation.Validator(ctx, test_db(), allow_internal)
    val.validate(realms_config_pb2.RealmsCfg(**fields))
    return ctx

  def assert_valid(self, fields, allow_internal=False):
    ctx = self.call(fields, allow_internal)
    self.assertEqual(ctx.messages, [])

  def assert_has_error(self, err, fields):
    ctx = self.call(fields)
    if len(ctx.messages) != 1 or err not in ctx.messages[0].text:
      self.fail(
          'Expected to fail validation with %r only, but got:\n%s' %
          (err, '\n'.join(msg.text for msg in ctx.messages) or 'none'))

  def test_empty(self):
    self.assert_valid({})

  def test_happy_path_project_config(self):
    self.assert_valid({
        'custom_roles': [
            {
                'name': 'customRole/empty-is-ok',
            },
            {
                'name': 'customRole/extends-builtin',
                'extends': ['role/dev.a', 'role/dev.b'],
            },
            {
                'name': 'customRole/permissions-and-extension',
                'extends': [
                    'role/dev.a',
                    'customRole/extends-builtin',
                    'customRole/empty-is-ok',
                ],
                'permissions': [
                    'luci.dev.p1',
                    'luci.dev.p2',
                ],
            },
        ],
        'realms': [
            {
                'name': '@root',
                'bindings': [
                    {
                        'role': 'customRole/permissions-and-extension',
                        'principals': [
                            'group:aaa',
                            'user:a@example.com',
                            'project:zzz',
                        ],
                    },
                    {
                        'role': 'role/dev.a',
                        'principals': ['group:bbb'],
                    },
                ],
            },
            {
                'name': '@legacy',
                'extends': [
                    '@root',  # explicitly extending root is OK
                    'some-realm/a',
                    'some-realm/b',
                ],
            },
            {
                'name': 'some-realm/a',
                'extends': ['some-realm/b'],
                'bindings': [
                    {
                        'role': 'role/dev.a',
                        'principals': ['group:aaa'],
                    },
                ],
            },
            {
                'name': 'some-realm/b',
                'bindings': [
                    {
                        'role': 'role/dev.b',
                        'principals': ['group:bbb'],
                    },
                ],
            },
        ],
    })

  def test_happy_path_internal_config(self):
    self.assert_valid({
        'custom_roles': [
            {
                'name': 'customRole/with-int-perm',
                'permissions': [
                    'luci.dev.p1',  # public
                    'luci.dev.int'  # internal
                ],
            },
        ],
        'realms': [
            {
                'name': 'realm',
                'bindings': [
                    {
                        'role': 'role/dev.a',
                        'principals': ['group:aaa'],
                    },
                    {
                        'role': 'role/luci.internal.int',
                        'principals': ['group:aaa'],
                    },
                    {
                        'role': 'customRole/with-int-perm',
                        'principals': ['group:aaa'],
                    },
                ],
            },
        ],
    }, allow_internal=True)

  def test_custom_role_bad_name(self):
    self.assert_has_error('name should start with "customRole/"', {
        'custom_roles': [
            {'name': 'role/bad'},
        ],
    })

  def test_custom_role_dup(self):
    self.assert_has_error('was already defined', {
        'custom_roles': [
            {'name': 'customRole/some'},
            {'name': 'customRole/some'},
        ],
    })

  def test_custom_role_unknown_permission(self):
    self.assert_has_error('not defined in permissions DB', {
        'custom_roles': [
            {
                'name': 'customRole/some',
                'permissions': [
                    'luci.dev.p1',  # known
                    'luci.dev.unknown',
                ],
            },
        ],
    })

  def test_custom_role_internal_permission(self):
    self.assert_has_error('is internal', {
        'custom_roles': [
            {
                'name': 'customRole/some',
                'permissions': [
                    'luci.dev.p1',  # public
                    'luci.dev.int',
                ],
            },
        ],
    })

  def test_custom_role_unknown_builtin_role_ref(self):
    self.assert_has_error('not defined in permissions DB', {
        'custom_roles': [
            {
                'name': 'customRole/some',
                'extends': ['role/unknown'],
            },
        ],
    })

  def test_custom_role_unknown_custom_role_ref(self):
    self.assert_has_error('not defined in the realms config', {
        'custom_roles': [
            {
                'name': 'customRole/some',
                'extends': ['customRole/unknown'],
            },
        ],
    })

  def test_custom_role_unknown_role_ref_format(self):
    self.assert_has_error('bad role reference "foo/unknown"', {
        'custom_roles': [
            {
                'name': 'customRole/some',
            },
            {
                'name': 'customRole/another',
                'extends': ['foo/unknown'],
            },
        ],
    })

  def test_custom_role_dup_extends(self):
    self.assert_has_error('extended from more than once', {
        'custom_roles': [
            {
                'name': 'customRole/base',
            },
            {
                'name': 'customRole/another',
                'extends': ['customRole/base', 'customRole/base'],
            },
        ],
    })

  def test_custom_role_cycle(self):
    self.assert_has_error('cyclically extends itself', {
        'custom_roles': [
            {
                'name': 'customRole/roleA',
                'extends': ['customRole/roleB'],
            },
            {
                'name': 'customRole/roleB',
                'extends': ['customRole/roleA'],
            },
        ],
    })

  def test_realms_bad_name_re(self):
    self.assert_has_error('name must match', {
        'realms': [
            {'name': 'FORBIDDENCHARS'},
        ],
    })

  def test_realms_bad_name_special(self):
    self.assert_has_error('unknown special realm name', {
        'realms': [
            {'name': '@huh'},
        ],
    })

  def test_realms_dup(self):
    self.assert_has_error('was already defined', {
        'realms': [
            {'name': 'some'},
            {'name': 'some'},
        ],
    })

  def test_realms_root_realm_cant_extend(self):
    self.assert_has_error('the root realm must not use `extends`', {
        'realms': [
            {'name': 'some'},
            {'name': '@root', 'extends': ['some']},
        ],
    })

  def test_realms_extending_unknown(self):
    self.assert_has_error('extending from an undefined realm', {
        'realms': [
            {'name': 'some'},
            {'name': 'another', 'extends': ['some', 'unknown']},
        ],
    })

  def test_realms_dup_extends(self):
    self.assert_has_error('extended from more than once', {
        'realms': [
            {'name': 'some'},
            {'name': 'another', 'extends': ['some', 'some']},
        ],
    })

  def test_realms_cycle(self):
    self.assert_has_error('cyclically extends itself', {
        'realms': [
            {'name': 'a', 'extends': ['b']},
            {'name': 'b', 'extends': ['a']},
        ],
    })

  def test_bindings_bad_role_ref(self):
    self.assert_has_error('not defined in the realms config', {
        'realms': [
            {
                'name': 'a',
                'bindings': [
                    {'role': 'customRole/unknown'},
                ],
            },
        ],
    })

  def test_bindings_internal_role(self):
    self.assert_has_error('is internal', {
        'realms': [
            {
                'name': 'a',
                'bindings': [
                    {'role': 'role/luci.internal.system'},
                ],
            },
        ],
    })

  def test_bindings_bad_group_name(self):
    self.assert_has_error('invalid group name', {
        'realms': [
            {
                'name': 'a',
                'bindings': [
                    {'role': 'role/dev.a', 'principals': ['group:!!!!']},
                ],
            },
        ],
    })

  def test_bindings_bad_principal_format(self):
    self.assert_has_error('invalid principal format', {
        'realms': [
            {
                'name': 'a',
                'bindings': [
                    {'role': 'role/dev.a', 'principals': ['zzz']},
                ],
            },
        ],
    })



class FindCycleTest(test_case.TestCase):
  def test_no_cycles(self):
    self.assertIsNone(validation.find_cycle('A', {
        'A': ['B', 'C'],
        'B': ['C'],
        'C': [],
    }))

  def test_trivial_cycle(self):
    self.assertEqual(validation.find_cycle('A', {
        'A': ['A'],
    }), ['A', 'A'])

  def test_longer_cycle(self):
    self.assertEqual(validation.find_cycle('A', {
        'A': ['B'],
        'B': ['C'],
        'C': ['A'],
    }), ['A', 'B', 'C', 'A'])

  def test_cycle_that_doesnt_involve_start(self):
    self.assertIsNone(validation.find_cycle('A', {
        'A': ['B'],
        'B': ['C'],
        'C': ['B'],
    }))

  def test_many_cycles_but_only_one_has_start(self):
    self.assertEqual(validation.find_cycle('A', {
        'A': ['B'],
        'B': ['C'],
        'C': ['B', 'D'],
        'D': ['B', 'C', 'A'],
    }), ['A', 'B', 'C', 'D', 'A'])

  def test_diamon_no_cycles(self):
    self.assertIsNone(validation.find_cycle('A', {
        'A': ['B1', 'B2'],
        'B1': ['C'],
        'B2': ['C'],
        'C': [],
    }))

  def test_diamon_with_cycle(self):
    self.assertEqual(validation.find_cycle('A', {
        'A': ['B1', 'B2'],
        'B1': ['C'],
        'B2': ['C'],
        'C': ['A'],
    }), ['A', 'B1', 'C', 'A'])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
