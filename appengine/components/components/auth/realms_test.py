#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import sys
import unittest

from test_support import test_env
test_env.setup_test_env()
from test_support import test_case

from components.auth import realms
from components.auth.proto import realms_pb2


def perms(*names):
  return [realms_pb2.Permission(name=name) for name in names]


def conds(*names):
  return [
      realms_pb2.Condition(
          restrict=realms_pb2.Condition.AttributeRestriction(
              attribute=name,
              values=['x', 'y', 'z'],
          ),
      ) for name in names
  ]


class MergeTest(test_case.TestCase):
  def test_empty(self):
    self.assertEqual(
        realms.merge([], {}),
        realms_pb2.Realms(api_version=realms.API_VERSION))

  def test_remaps_permissions(self):
    out = realms.merge(
        perms('luci.dev.p1', 'luci.dev.p2'),
        {
            'proj1': realms_pb2.Realms(
                permissions=perms('luci.dev.p2', 'luci.dev.z', 'luci.dev.p1'),
                realms=[
                    {
                        'name': 'proj1:@root',
                        'bindings': [
                            {
                                'permissions': [0, 1, 2],  # p2, z, p1
                                'principals': ['group:gr1'],
                            },
                            {
                                'permissions': [1],  # z, will be dropped
                                'principals': ['group:gr2'],
                            },
                            {
                                'permissions': [2],  # p1
                                'principals': ['group:gr3'],
                            },
                        ],
                    },
                ],
            ),
        },
    )
    self.assertEqual(out, realms_pb2.Realms(
        api_version=realms.API_VERSION,
        permissions=perms('luci.dev.p1', 'luci.dev.p2'),
        realms=[
            {
                'name': 'proj1:@root',
                'bindings': [
                    {
                        'permissions': [0],  # p1
                        'principals': ['group:gr3'],
                    },
                    {
                        'permissions': [0, 1],  # p1, p2
                        'principals': ['group:gr1'],
                    },
                ],
            },
        ],
    ))

  def test_remaps_conditions(self):
    out = realms.merge(
        perms('luci.dev.p1'),
        {
            'proj1': realms_pb2.Realms(
                permissions=perms('luci.dev.p1'),
                conditions=conds('a', 'b'),
                realms=[
                    {
                        'name': 'proj1:@root',
                        'bindings': [
                            {
                                'permissions': [0],
                                'conditions': [0, 1],
                                'principals': ['group:gr1'],
                            },
                        ],
                    },
                ],
            ),
            'proj2': realms_pb2.Realms(
                permissions=perms('luci.dev.p1'),
                conditions=conds('c', 'a', 'b'),
                realms=[
                    {
                        'name': 'proj2:@root',
                        'bindings': [
                            {
                                'permissions': [0],
                                'conditions': [0],  # c
                                'principals': ['group:gr2'],
                            },
                            {
                                'permissions': [0],
                                'conditions': [1],  # a
                                'principals': ['group:gr3'],
                            },
                            {
                                'permissions': [0],
                                'conditions': [0, 1, 2],  # c a b
                                'principals': ['group:gr4'],
                            },
                        ],
                    },
                ],
            ),
        },
    )
    self.assertEqual(out, realms_pb2.Realms(
        api_version=realms.API_VERSION,
        permissions=perms('luci.dev.p1'),
        conditions=conds('a', 'b', 'c'),
        realms=[
            {
                'name': 'proj1:@root',
                'bindings': [
                    {
                        'permissions': [0],
                        'conditions': [0, 1],
                        'principals': ['group:gr1'],
                    },
                ],
            },
            {
                'name': 'proj2:@root',
                'bindings': [
                    {
                        'permissions': [0],
                        'conditions': [0],  # a
                        'principals': ['group:gr3'],
                    },
                    {
                        'permissions': [0],
                        'conditions': [0, 1, 2],  # a b c == c a b
                        'principals': ['group:gr4'],
                    },
                    {
                        'permissions': [0],
                        'conditions': [2],  # c
                        'principals': ['group:gr2'],
                    },
                ],
            },
        ],
    ))

  def test_merge_multiple(self):
    out = realms.merge(
        perms('luci.dev.p1', 'luci.dev.p2', 'luci.dev.p3'),
        {
            'proj1': realms_pb2.Realms(
                permissions=perms('luci.dev.p1', 'luci.dev.p2'),
                realms=[
                    {
                        'name': 'proj1:@root',
                        'bindings': [
                            {
                                'permissions': [0, 1],  # p1, p2
                                'principals': ['group:gr1'],
                            },
                        ],
                        'data': {
                            'enforce_in_service': ['a'],
                        },
                    },
                ],
            ),
            'proj2': realms_pb2.Realms(
                permissions=perms('luci.dev.p2', 'luci.dev.p3'),
                realms=[
                    {
                        'name': 'proj2:@root',
                        'bindings': [
                            {
                                'permissions': [0, 1],  # p2, p3
                                'principals': ['group:gr2'],
                            },
                        ],
                    },
                ],
            ),
        },
    )
    self.assertEqual(out, realms_pb2.Realms(
        api_version=realms.API_VERSION,
        permissions=perms('luci.dev.p1', 'luci.dev.p2', 'luci.dev.p3'),
        realms=[
            {
                'name': 'proj1:@root',
                'bindings': [
                    {
                        'permissions': [0, 1],  # p1, p2
                        'principals': ['group:gr1'],
                    },
                ],
                'data': {
                    'enforce_in_service': ['a'],
                },
            },
            {
                'name': 'proj2:@root',
                'bindings': [
                    {
                        'permissions': [1, 2],  # p2, p3
                        'principals': ['group:gr2'],
                    },
                ],
            },
        ],
    ))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
