#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from google.appengine.ext import ndb

from server import errors
from support import test_case


class TestManagementTest(test_case.TestCase):
  def testSwarmErrorDeleteOldErrors(self):
    # Create error.
    error = errors.SwarmError(name='name', message='msg', info='info')
    error.put()
    self.assertEqual(1, errors.SwarmError.query().count())

    # First call shouldn't delete the error since its not stale yet.
    ndb.delete_multi(errors.QueryOldErrors())
    self.assertEqual(1, errors.SwarmError.query().count())

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=errors.SWARM_ERROR_TIME_TO_LIVE_DAYS - 1))
    self.mock(errors, '_utcnow', lambda: mock_now)

    ndb.delete_multi(errors.QueryOldErrors())
    self.assertEqual(1, errors.SwarmError.query().count())

    # Set the current time to the future.
    mock_now = (datetime.datetime.utcnow() + datetime.timedelta(
        days=errors.SWARM_ERROR_TIME_TO_LIVE_DAYS + 1))

    # Second call should remove the now stale error.
    ndb.delete_multi(errors.QueryOldErrors())
    self.assertEqual(0, errors.SwarmError.query().count())


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
