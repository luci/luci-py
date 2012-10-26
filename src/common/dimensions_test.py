#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Test cases for the dimensions code."""

import unittest

from common import dimensions


class TestDimensions(unittest.TestCase):

  def testMatchDimensions(self):
    machine_dimensions = {'os': 'win32', 'lang': 'en', 'browser': ['ie', 'ff']}

    self.assertTrue(dimensions.MatchDimensions({'os': 'win32'},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'os': ['win32']},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'os': 'win32',
                                                'lang': 'en'},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'os': 'win32',
                                                'lang': ['en']},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'os': 'win32',
                                                'lang': 'en',
                                                'browser': 'ie'},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'os': 'win32',
                                                'lang': 'en',
                                                'browser': 'ff'},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'os': 'win32',
                                                'lang': 'en',
                                                'browser': ['ie', 'ff']},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'lang': 'en',
                                                'browser': ['ie', 'ff']},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'lang': 'en'},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'browser': ['ie', 'ff']},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'os': 'win32',
                                                'browser': ['ie', 'ff']},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'browser': 'ff'},
                                               machine_dimensions)[0])
    self.assertTrue(dimensions.MatchDimensions({'browser': 'ie'},
                                               machine_dimensions)[0])

    self.assertFalse(dimensions.MatchDimensions({'bs': ['win32']},
                                                machine_dimensions)[0])
    self.assertFalse(dimensions.MatchDimensions({42: ['win32']},
                                                machine_dimensions)[0])
    self.assertFalse(dimensions.MatchDimensions({'os': 42},
                                                machine_dimensions)[0])
    self.assertFalse(dimensions.MatchDimensions({'os': 'win32',
                                                 'browser': ['ie', 42]},
                                                machine_dimensions)[0])
    self.assertFalse(dimensions.MatchDimensions({'os': 'win32',
                                                 'browser': ['chrome']},
                                                machine_dimensions)[0])
    self.assertFalse(dimensions.MatchDimensions({'os': 'mac'},
                                                machine_dimensions)[0])
