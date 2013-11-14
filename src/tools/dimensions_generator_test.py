#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unittest to exercise the code in dimensions_generator.py."""


import json
import logging
import os
import sys
import tempfile
import unittest


import mox  # pylint: disable=g-import-not-at-top

from tools import dimensions_generator


class DimensionsGeneratorTest(unittest.TestCase):
  def setUp(self):
    self._mox = mox.Mox()

  def tearDown(self):
    self._mox.UnsetStubs()

  def testDimensionsGeneratorPlatformWindows(self):
    self._mox.StubOutWithMock(sys, 'platform')
    sys.platform = 'win32'

    self._mox.ReplayAll()

    dimensions = dimensions_generator.DimensionsGenerator()
    machine_dimensions = dimensions.GetDimensions()
    self.assertTrue('dimensions' in machine_dimensions)
    self.assertTrue('os' in machine_dimensions['dimensions'])
    self.assertEqual(machine_dimensions['dimensions']['os'],
                     'Windows')

    self._mox.VerifyAll()

  def testDimensionsGeneratorPlatformMac(self):
    self._mox.StubOutWithMock(sys, 'platform')
    sys.platform = 'darwin'

    self._mox.ReplayAll()

    dimensions = dimensions_generator.DimensionsGenerator()
    machine_dimensions = dimensions.GetDimensions()
    self.assertTrue('dimensions' in machine_dimensions)
    self.assertTrue('os' in machine_dimensions['dimensions'])
    self.assertEqual(machine_dimensions['dimensions']['os'],
                     'Mac')

    self._mox.VerifyAll()

  def testDimensionsGeneratorUnknownPlatform(self):
    self._mox.StubOutWithMock(sys, 'platform')
    sys.platform = 'newos'

    self._mox.StubOutWithMock(logging, 'error')
    dimensions_generator.logging.error(mox.IgnoreArg(), mox.IgnoreArg())

    self._mox.ReplayAll()

    dimensions = dimensions_generator.DimensionsGenerator()
    self.assertEqual(dimensions.GetDimensions(), {})

    self._mox.VerifyAll()

  def testDimensionsGeneratorWriteToFileSuccess(self):
    temp_file = tempfile.NamedTemporaryFile()

    dimensions = dimensions_generator.DimensionsGenerator()
    self.assertTrue(dimensions.WriteDimensionsToFile(temp_file.name))

    dimensions_file = open(temp_file.name)
    self.assertEqual(json.load(dimensions_file),
                     dimensions.GetDimensions())

    dimensions_file.close()
    temp_file.close()

  def testDimensionsGeneratorWriteToFileFailure(self):
    # Python should be unable to open this file for writing since it is in
    # a directory that doesn't exist.
    invalid_filename = 'fake_directory/no_file'
    self.assertFalse(os.path.exists(os.path.dirname(invalid_filename)))

    dimensions = dimensions_generator.DimensionsGenerator()
    self.assertFalse(dimensions.WriteDimensionsToFile(invalid_filename))

    self.assertFalse(os.path.exists(invalid_filename))


if __name__ == '__main__':
  unittest.main()
