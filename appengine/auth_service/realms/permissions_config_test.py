# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env

test_env.setup_test_env()

from test_support import test_case

from components.auth.proto import realms_pb2

from proto import config_pb2, realms_config_pb2

from realms import permissions
from realms import permissions_config


class ConfigToDBTest(test_case.TestCase):
  def test_config_conversion_works(self):
    cfg_rev = 'rev1'
    cfg = config_pb2.PermissionsConfig(role=[
        config_pb2.PermissionsConfig.Role(
            name='role/test.admin',
            permissions=[
                realms_pb2.Permission(name='auth.test.view'),
                realms_pb2.Permission(name='auth.test.setTested', internal=True)
            ]),
        config_pb2.PermissionsConfig.Role(
            name='role/test.viewer',
            permissions=[realms_pb2.Permission(name='auth.test.view')])
    ],
                                       attribute=['attribute-a', 'attribute-b'])
    cfg_db = permissions_config.to_db(cfg_rev, cfg)

    expected_bindings = lambda project_id: [
        realms_config_pb2.Binding(
            role='role/luci.internal.system',
            principals=['project:' + project_id],
        ),
        realms_config_pb2.Binding(
            role='role/luci.internal.buildbucket.reader',
            principals=['group:buildbucket-internal-readers'],
        ),
        realms_config_pb2.Binding(
            role='role/luci.internal.resultdb.reader',
            principals=['group:resultdb-internal-readers'],
        ),
        realms_config_pb2.Binding(
            role='role/luci.internal.resultdb.invocationSubmittedSetter',
            principals=['group:resultdb-internal-invocation-submitters'],
        ),
    ]
    expected_db = permissions.DB(
        revision='permissions.cfg:rev1',
        permissions={
            'auth.test.view':
            realms_pb2.Permission(name='auth.test.view'),
            'auth.test.setTested':
            realms_pb2.Permission(name='auth.test.setTested', internal=True)
        },
        roles={
            'role/test.admin':
            permissions.Role(name='role/test.admin',
                             permissions=('auth.test.setTested',
                                          'auth.test.view')),
            'role/test.viewer':
            permissions.Role(name='role/test.viewer',
                             permissions=('auth.test.view', ))
        },
        attributes={'attribute-a', 'attribute-b'},
        implicit_root_bindings=expected_bindings)

    for field in permissions.DB._fields:
      actual = getattr(cfg_db, field)
      expected = getattr(expected_db, field)

      if field == 'implicit_root_bindings':
        # implicit_root_bindings should be a function, so compare the
        # outputs given the same input.
        proj_id = 'dummy-project-id'
        actual = actual(proj_id)
        expected = expected(proj_id)

      self.assertEqual(actual, expected)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
