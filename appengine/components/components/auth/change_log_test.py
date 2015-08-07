#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components.auth import change_log
from components.auth import model
from test_support import test_case


class MakeInitialSnapshotTest(test_case.TestCase):
  """Tests for ensure_initial_snapshot function."""

  def test_works(self):
    # Initial entities. Don't call 'record_revision' to imitate "old"
    # application without history related code.
    @ndb.transactional
    def make_auth_db():
      model.AuthGlobalConfig(key=model.root_key()).put()
      model.AuthIPWhitelistAssignments(
          key=model.ip_whitelist_assignments_key()).put()
      model.AuthGroup(key=model.group_key('A group')).put()
      model.AuthIPWhitelist(key=model.ip_whitelist_key('A whitelist')).put()
      model.replicate_auth_db()
    make_auth_db()

    # Bump auth_db once more to avoid hitting trivial case of "processing first
    # revision ever".
    auth_db_rev = ndb.transaction(model.replicate_auth_db)
    self.assertEqual(2, auth_db_rev)

    # Now do the work.
    change_log.ensure_initial_snapshot(auth_db_rev)

    # Generated new AuthDB rev with updated entities.
    self.assertEqual(3, model.get_auth_db_revision())

    # Check all *History entitites exist now.
    p = model.historical_revision_key(3)
    self.assertIsNotNone(
        ndb.Key('AuthGlobalConfigHistory', 'root', parent=p).get())
    self.assertIsNotNone(
        ndb.Key(
            'AuthIPWhitelistAssignmentsHistory', 'default', parent=p).get())
    self.assertIsNotNone(ndb.Key('AuthGroupHistory', 'A group', parent=p).get())
    self.assertIsNotNone(
        ndb.Key('AuthIPWhitelistHistory', 'A whitelist', parent=p).get())

    # Call again, should be noop (marker is set).
    change_log.ensure_initial_snapshot(3)
    self.assertEqual(3, model.get_auth_db_revision())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
