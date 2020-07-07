#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.protobuf import json_format

from test_support import test_case

from proto import realms_config_pb2

from realms import permissions
from realms import rules


def test_db(implicit_root_bindings=False):
  b = permissions.Builder('auth-code-rev')
  b.role('role/dev.a', [
      b.permission('luci.dev.p1'),
      b.permission('luci.dev.p2'),
  ])
  b.role('role/dev.b', [
      b.permission('luci.dev.p2'),
      b.permission('luci.dev.p3'),
  ])
  b.role('role/dev.all', [
      b.include('role/dev.a'),
      b.include('role/dev.b'),
  ])
  b.role('role/dev.unused', [
      b.permission('luci.dev.p2'),
      b.permission('luci.dev.p3'),
      b.permission('luci.dev.p4'),
      b.permission('luci.dev.p5'),
      b.permission('luci.dev.unused'),
  ])
  b.role('role/implicitRoot', [
      b.permission('luci.dev.implicitRoot'),
  ])
  if implicit_root_bindings:
    b.implicit_root_bindings = lambda project_id: [
        realms_config_pb2.Binding(
            role='role/implicitRoot',
            principals=['project:'+project_id],
        ),
    ]
  return b.finish()


class RolesExpanderTest(test_case.TestCase):
  def test_builtin_roles(self):
    r = rules.RolesExpander(test_db().roles, [])
    self.assertEqual(r.role('role/dev.a'), (0, 1))
    self.assertEqual(r.role('role/dev.b'), (1, 2))
    perms, mapping = r.sorted_permissions()
    self.assertEqual(perms, ['luci.dev.p1', 'luci.dev.p2', 'luci.dev.p3'])
    self.assertEqual(mapping, [0, 1, 2])

  def test_custom_roles(self):
    r = rules.RolesExpander(test_db().roles, [
        realms_config_pb2.CustomRole(
            name='customRole/custom1',
            extends=['role/dev.a', 'customRole/custom2', 'customRole/custom3'],
            permissions=['luci.dev.p1', 'luci.dev.p4'],
        ),
        realms_config_pb2.CustomRole(
            name='customRole/custom2',
            extends=['customRole/custom3'],
            permissions=['luci.dev.p4'],
        ),
        realms_config_pb2.CustomRole(
            name='customRole/custom3',
            extends=['role/dev.b'],
            permissions=['luci.dev.p5'],
        ),
    ])
    self.assertEqual(r.role('customRole/custom1'), (0, 1, 2, 3, 4))
    self.assertEqual(r.role('customRole/custom2'), (1, 2, 3, 4))
    self.assertEqual(r.role('customRole/custom3'), (2, 3, 4))

    perms, mapping = r.sorted_permissions()
    self.assertEqual(perms, [
        'luci.dev.p1',
        'luci.dev.p2',
        'luci.dev.p3',
        'luci.dev.p4',
        'luci.dev.p5',
    ])
    self.assertEqual(mapping, [0, 3, 1, 4, 2])

    # Since eyeballing these numbers are hard, here's a somewhat redundant test.
    remap = lambda permset: {perms[mapping[idx]] for idx in permset}
    self.assertEqual(remap(r.role('customRole/custom1')), {
        'luci.dev.p1',
        'luci.dev.p2',
        'luci.dev.p3',
        'luci.dev.p4',
        'luci.dev.p5',
    })
    self.assertEqual(remap(r.role('customRole/custom2')), {
        'luci.dev.p2',
        'luci.dev.p3',
        'luci.dev.p4',
        'luci.dev.p5',
    })
    self.assertEqual(remap(r.role('customRole/custom3')), {
        'luci.dev.p2',
        'luci.dev.p3',
        'luci.dev.p5',
    })


def binding(role, *principals):
  return {'role': role, 'principals': principals}


class ExpandRealmsTest(test_case.TestCase):
  def expand(self, cfg, project_id='p', implicit_root_bindings=False):
    out = rules.expand_realms(
        test_db(implicit_root_bindings),
        project_id,
        realms_config_pb2.RealmsCfg(**cfg))
    return json_format.MessageToDict(out)

  def test_fails_validation(self):
    with self.assertRaises(ValueError):
      self.expand({
          'realms': [
              {'name': 'r1', 'extends': 'r2'},
              {'name': 'r2', 'extends': 'r1'},
          ],
      })

  def test_completely_empty(self):
    self.assertEqual(self.expand({}), {'realms': [{'name': 'p:@root'}]})

  def test_empty_realm(self):
    cfg = {
        'realms': [
            {'name': 'r2'},
            {'name': 'r1'},
        ],
    }
    self.assertEqual(self.expand(cfg), {'realms': [
        {'name': u'p:@root'},
        {'name': u'p:r1'},
        {'name': u'p:r2'},
    ]})

  def test_simple_bindings(self):
    cfg = {'realms': [
        {
            'name': 'r',
            'bindings': [
                binding('role/dev.a', 'group:gr1', 'group:gr3'),
                binding('role/dev.b', 'group:gr2', 'group:gr3'),
                binding('role/dev.all', 'group:gr4'),
            ],
        },
    ]}
    self.assertEqual(self.expand(cfg), {
        'permissions': [
            {'name': u'luci.dev.p1'},
            {'name': u'luci.dev.p2'},
            {'name': u'luci.dev.p3'},
        ],
        'realms': [
            {
                'name': u'p:@root',
            },
            {
                'name': u'p:r',
                'bindings': [
                    {
                        'permissions': [0, 1],
                        'principals': [u'group:gr1'],
                    },
                    {
                        'permissions': [0, 1, 2],
                        'principals': [u'group:gr3', u'group:gr4'],
                    },
                    {
                        'permissions': [1, 2],
                        'principals': [u'group:gr2'],
                    },
                ],
            },
        ],
    })

  def test_custom_root(self):
    cfg = {
        'realms': [
            {
                'name': '@root',
                'bindings': [binding('role/dev.all', 'group:gr4')],
            },
            {
                'name': 'r',
                'bindings': [
                    binding('role/dev.a', 'group:gr1', 'group:gr3'),
                    binding('role/dev.b', 'group:gr2', 'group:gr3'),
                ],
            },
        ],
    }
    self.assertEqual(self.expand(cfg), {
        'permissions': [
            {'name': u'luci.dev.p1'},
            {'name': u'luci.dev.p2'},
            {'name': u'luci.dev.p3'},
        ],
        'realms': [
            {
                'name': u'p:@root',
                'bindings': [
                    {
                        'permissions': [0, 1, 2],
                        'principals': [u'group:gr4'],
                    },
                ],
            },
            {
                'name': u'p:r',
                'bindings': [
                    {
                        'permissions': [0, 1],
                        'principals': [u'group:gr1'],
                    },
                    {
                        'permissions': [0, 1, 2],
                        'principals': [u'group:gr3', u'group:gr4'],
                    },
                    {
                        'permissions': [1, 2],
                        'principals': [u'group:gr2'],
                    },
                ],
            },
        ],
    })

  def test_realm_inheritance(self):
    cfg = {
        'realms': [
            {
                'name': '@root',
                'bindings': [binding('role/dev.all', 'group:gr4')],
            },
            {
                'name': 'r1',
                'bindings': [binding('role/dev.a', 'group:gr1', 'group:gr3')],
            },
            {
                'name': 'r2',
                'bindings': [binding('role/dev.b', 'group:gr2', 'group:gr3')],
                'extends': ['r1', '@root'],
            },
        ],
    }
    self.assertEqual(self.expand(cfg), {
        'permissions': [
            {'name': u'luci.dev.p1'},
            {'name': u'luci.dev.p2'},
            {'name': u'luci.dev.p3'},
        ],
        'realms': [
            {
                'name': u'p:@root',
                'bindings': [
                    {
                        'permissions': [0, 1, 2],
                        'principals': [u'group:gr4'],
                    },
                ],
            },
            {
                'name': u'p:r1',
                'bindings': [
                    {
                        'permissions': [0, 1],
                        'principals': [u'group:gr1', u'group:gr3'],
                    },
                    {
                        'permissions': [0, 1, 2],
                        'principals': [u'group:gr4'],
                    },
                ],
            },
            {
                'name': u'p:r2',
                'bindings': [
                    {
                        'permissions': [0, 1],
                        'principals': [u'group:gr1'],
                    },
                    {
                        'permissions': [0, 1, 2],
                        'principals': [u'group:gr3', u'group:gr4'],
                    },
                    {
                        'permissions': [1, 2],
                        'principals': [u'group:gr2'],
                    },
                ],
            },
        ],
    })

  def test_custom_roles(self):
    cfg = {
        'custom_roles': [
            {
                'name': 'customRole/r1',
                'extends': ['role/dev.a'],
                'permissions': ['luci.dev.p4'],
            },
            {
                'name': 'customRole/r2',
                'extends': ['customRole/r1', 'role/dev.b'],
            },
            {
                'name': 'customRole/r3',
                'permissions': ['luci.dev.p5'],
            },
        ],
        'realms': [
            {
                'name': 'r',
                'bindings': [
                    binding('customRole/r1', 'group:gr1', 'group:gr3'),
                    binding('customRole/r2', 'group:gr2', 'group:gr3'),
                    binding('customRole/r3', 'group:gr5'),
                ],
            },
        ],
    }
    self.assertEqual(self.expand(cfg), {
        'permissions': [
            {'name': u'luci.dev.p1'},
            {'name': u'luci.dev.p2'},
            {'name': u'luci.dev.p3'},
            {'name': u'luci.dev.p4'},
            {'name': u'luci.dev.p5'},
        ],
        'realms': [
            {
                'name': u'p:@root',
            },
            {
                'name': u'p:r',
                'bindings': [
                    {
                        'permissions': [0, 1, 2, 3],
                        'principals': [u'group:gr2', u'group:gr3'],
                    },
                    {
                        'permissions': [0, 1, 3],
                        'principals': [u'group:gr1'],
                    },
                    {
                        'permissions': [4],
                        'principals': [u'group:gr5'],
                    },
                ],
            },
        ],
    })

  def test_implicit_root_bindings_no_root(self):
    cfg = {
        'realms': [
            {
                'name': 'r',
                'bindings': [
                    binding('role/dev.a', 'group:gr'),
                ],
            },
        ],
    }
    self.assertEqual(self.expand(cfg, implicit_root_bindings=True), {
        'permissions': [
            {'name': u'luci.dev.implicitRoot'},
            {'name': u'luci.dev.p1'},
            {'name': u'luci.dev.p2'},
        ],
        'realms': [
            {
                'name': u'p:@root',
                'bindings': [
                    {
                        'permissions': [0],
                        'principals': [u'project:p'],
                    },
                ],
            },
            {
                'name': u'p:r',
                'bindings': [
                    {
                        'permissions': [0],
                        'principals': [u'project:p'],
                    },
                    {
                        'permissions': [1, 2],
                        'principals': [u'group:gr'],
                    },
                ],
            },
        ],
    })

  def test_implicit_root_bindings_with_root(self):
    cfg = {
        'realms': [
            {
                'name': '@root',
                'bindings': [
                    binding('role/dev.a', 'group:gr1'),
                ],
            },
            {
                'name': 'r',
                'bindings': [
                    binding('role/dev.a', 'group:gr2'),
                ],
            },
        ],
    }
    self.assertEqual(self.expand(cfg, implicit_root_bindings=True), {
        'permissions': [
            {'name': u'luci.dev.implicitRoot'},
            {'name': u'luci.dev.p1'},
            {'name': u'luci.dev.p2'},
        ],
        'realms': [
            {
                'name': u'p:@root',
                'bindings': [
                    {
                        'permissions': [0],
                        'principals': [u'project:p'],
                    },
                    {
                        'permissions': [1, 2],
                        'principals': [u'group:gr1'],
                    },
                ],
            },
            {
                'name': u'p:r',
                'bindings': [
                    {
                        'permissions': [0],
                        'principals': [u'project:p'],
                    },
                    {
                        'permissions': [1, 2],
                        'principals': [u'group:gr1', u'group:gr2'],
                    },
                ],
            },
        ],
    })

  def test_implicit_root_binding_in_internal(self):
    cfg = {
        'realms': [
            {
                'name': 'r',
                'bindings': [
                    binding('role/dev.a', 'group:gr'),
                ],
            },
        ],
    }
    realms = self.expand(
        cfg,
        project_id='@internal',
        implicit_root_bindings=True)  # should be ignored
    self.assertEqual(realms, {
        'permissions': [
            {'name': u'luci.dev.p1'},
            {'name': u'luci.dev.p2'},
        ],
        'realms': [
            {
                'name': u'@internal:@root',  # added empty @root
            },
            {
                'name': u'@internal:r',
                'bindings': [
                    {
                        'permissions': [0, 1],
                        'principals': [u'group:gr'],
                    },
                ],
            },
        ],
    })


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
