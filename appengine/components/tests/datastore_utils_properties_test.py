#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components.datastore_utils import properties
from support import test_case


class BP(ndb.Model):
  prop = properties.BytesComputedProperty(lambda _: '\x00')


class DJP(ndb.Model):
  prop = properties.DeterministicJsonProperty(json_type=dict)


class PropertiesTest(test_case.TestCase):
  def test_DeterministicJsonProperty(self):
    self.assertEqual({'a': 1}, DJP(prop={'a': 1}).prop)

    DJP(prop={'a': 1}).put()
    self.assertEqual({'a': 1}, DJP.query().get().prop)

    with self.assertRaises(TypeError):
      DJP(prop=[])

  def test_BytesComputedProperty(self):
    self.assertEqual('\x00', BP().prop)
    BP().put()
    self.assertEqual('\x00', BP.query().get().prop)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
