#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Test cases for the dimensions code."""

import unittest

from google.appengine.ext import testbed
from common import dimensions_utils


def AreDimensionsInMachineDimensions(dimensions, machine_dimensions):
  return (dimensions_utils.GenerateDimensionHash(dimensions) in
          dimensions_utils.GenerateAllDimensionHashes(machine_dimensions))


class TestDimensionsUtils(unittest.TestCase):
  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def testMatchDimensions(self):
    machine_dimensions = {'os': 'win32', 'lang': 'en', 'browser': ['ie', 'ff']}

    self.assertTrue(dimensions_utils.MatchDimensions({'os': 'win32'},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'os': ['win32']},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'os': 'win32',
                                                      'lang': 'en'},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'os': 'win32',
                                                      'lang': ['en']},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'os': 'win32',
                                                      'lang': 'en',
                                                      'browser': 'ie'},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'os': 'win32',
                                                      'lang': 'en',
                                                      'browser': 'ff'},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'os': 'win32',
                                                      'lang': 'en',
                                                      'browser': ['ie', 'ff']},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'lang': 'en',
                                                      'browser': ['ie', 'ff']},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'lang': 'en'},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'browser': ['ie', 'ff']},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'os': 'win32',
                                                      'browser': ['ie', 'ff']},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'browser': 'ff'},
                                                     machine_dimensions)[0])
    self.assertTrue(dimensions_utils.MatchDimensions({'browser': 'ie'},
                                                     machine_dimensions)[0])

    self.assertFalse(dimensions_utils.MatchDimensions({'bs': ['win32']},
                                                      machine_dimensions)[0])
    self.assertFalse(dimensions_utils.MatchDimensions({42: ['win32']},
                                                      machine_dimensions)[0])
    self.assertFalse(dimensions_utils.MatchDimensions({'os': 42},
                                                      machine_dimensions)[0])
    self.assertFalse(dimensions_utils.MatchDimensions({'os': 'win32',
                                                       'browser': ['ie', 42]},
                                                      machine_dimensions)[0])
    self.assertFalse(dimensions_utils.MatchDimensions({'os': 'win32',
                                                       'browser': ['chrome']},
                                                      machine_dimensions)[0])
    self.assertFalse(dimensions_utils.MatchDimensions({'os': 'mac'},
                                                      machine_dimensions)[0])

  def testGenerateCombinations(self):
    dimension = {'os': 'win-xp', 'lang': 'en'}
    expected_output = sorted([{}, {'os': ['win-xp']}, {'lang': ['en']},
                              {'os': ['win-xp'], 'lang': ['en']}])
    self.assertEqual(sorted(dimensions_utils.GenerateCombinations(dimension)),
                     expected_output)

    dimension = {'browser': ['ie', 'ff']}
    expected_output = sorted([{}, {'browser': ['ie']}, {'browser': ['ff']},
                              {'browser': ['ie', 'ff']}])
    self.assertEqual(sorted(dimensions_utils.GenerateCombinations(dimension)),
                     expected_output)

    dimension = {'os': 'win-xp', 'browser': ['ie']}
    expected_output = sorted([{}, {'os': ['win-xp']}, {'browser': ['ie']},
                              {'os': ['win-xp'], 'browser': ['ie']}])
    self.assertEqual(sorted(dimensions_utils.GenerateCombinations(dimension)),
                     expected_output)

    # If there are more dimensions than allowed we should not return anything.
    self.assertEqual(
        [],
        dimensions_utils.GenerateCombinations(
            {'browser':
             range(dimensions_utils.MAX_DIMENSIONS_PER_MACHINE + 1)}))

    # If there are excatly MAC_DIMENSIONS_PER_MACHINE, we should still be
    # able to get all the combinations.
    self.assertNotEqual(
        [],
        dimensions_utils.GenerateCombinations(
            {'browser':
             range(dimensions_utils.MAX_DIMENSIONS_PER_MACHINE)}))

  def testGenerateAllDimensionHashes(self):
    machine_dimensions = {'os': 'win32', 'lang': 'en',
                          'browser': ['ie', 'ff', 1]}

    self.assertTrue(AreDimensionsInMachineDimensions({'os': 'win32'},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'os': ['win32']},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'os': 'win32',
                                                      'lang': 'en'},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'os': 'win32',
                                                      'lang': ['en']},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'os': 'win32',
                                                      'lang': 'en',
                                                      'browser': 'ie'},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'os': 'win32',
                                                      'lang': 'en',
                                                      'browser': 'ff'},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'os': 'win32',
                                                      'lang': 'en',
                                                      'browser': ['ie', 'ff']},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'os': 'win32',
                                                      'lang': 'en',
                                                      'browser': [
                                                          'ie', 'ff', 1]},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'os': 'win32',
                                                      'browser': ['ie', 'ff']},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'lang': 'en',
                                                      'browser': ['ie', 'ff']},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'lang': 'en'},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'browser': ['ie', 'ff']},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'browser': ['ie']},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'browser': 'ff'},
                                                     machine_dimensions))
    self.assertTrue(AreDimensionsInMachineDimensions({'browser': 1},
                                                     machine_dimensions))

    self.assertFalse(AreDimensionsInMachineDimensions({'bs': ['win32']},
                                                      machine_dimensions))
    self.assertFalse(AreDimensionsInMachineDimensions({42: ['win32']},
                                                      machine_dimensions))
    self.assertFalse(AreDimensionsInMachineDimensions({'os': 42},
                                                      machine_dimensions))
    self.assertFalse(AreDimensionsInMachineDimensions({'os': 'win32',
                                                       'browser': ['ie', 42]},
                                                      machine_dimensions))
    self.assertFalse(AreDimensionsInMachineDimensions({'os': 'win32',
                                                       'browser': ['chrome']},
                                                      machine_dimensions))
    self.assertFalse(AreDimensionsInMachineDimensions({'os': 'win32-bonus'},
                                                      machine_dimensions))
    self.assertFalse(AreDimensionsInMachineDimensions({'os': 'win'},
                                                      machine_dimensions))
    self.assertFalse(AreDimensionsInMachineDimensions({'os': 'mac'},
                                                      machine_dimensions))

    machine_dimensions_with_extra = machine_dimensions.copy()
    machine_dimensions_with_extra['game'] = 'on'
    self.assertFalse(AreDimensionsInMachineDimensions(
        machine_dimensions_with_extra,
        machine_dimensions))

  def testGenerateAllDimensionHashesLarge(self):
    # This dimension generates a list too large to store in the memcache,
    # so ensure it doesn't crash by trying to.
    dimension = {'browser': range(15)}
    dimensions_utils.GenerateAllDimensionHashes(dimension)


if __name__ == '__main__':
  unittest.main()
