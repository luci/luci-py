#!/usr/bin/env python
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

# google.protobuf package requires some python package magic hacking.
from components import utils
utils.fix_protobuf_package()

from google.protobuf import field_mask_pb2

import field_masks
import test_proto_pb2


class ParsePathTests(unittest.TestCase):
  def parse(self, path):
    return field_masks._parse_path(path, test_proto_pb2.Msg.DESCRIPTOR)

  def test_str(self):
    actual = self.parse('str')
    expected = ('str',)
    self.assertEqual(actual, expected)

  def test_str_str(self):
    err_pattern = r'scalar field "str" cannot have subfields'
    with self.assertRaisesRegexp(ValueError, err_pattern):
      self.parse('str.str')

  def test_str_invalid_char(self):
    err_pattern = r'unexpected token "@"; expected a period'
    with self.assertRaisesRegexp(ValueError, err_pattern):
      self.parse('str@')

  def test_str_repeated(self):
    actual = self.parse('strs')
    expected = ('strs',)
    self.assertEqual(actual, expected)

  def test_str_repeated_trailing_star(self):
    actual = self.parse('strs.*')
    expected = ('strs', field_masks._STAR_SEG)
    self.assertEqual(actual, expected)

  def test_str_repeated_index(self):
    err_pattern = r'unexpected token "1", expected a star'
    with self.assertRaisesRegexp(ValueError, err_pattern):
      self.parse('strs.1')

  def test_map_num_key(self):
    actual = self.parse('map_num_str.1')
    expected = ('map_num_str', 1)
    self.assertEqual(actual, expected)

  def test_map_num_key_negative(self):
    actual = self.parse('map_num_str.-1')
    expected = ('map_num_str', -1)
    self.assertEqual(actual, expected)

  def test_map_num_key_invalid(self):
    with self.assertRaisesRegexp(ValueError, r'expected an integer'):
      self.parse('map_num_str.a')

  def test_map_num_key_invalid_with_correct_prefix(self):
    err_pattern = r'unexpected token "a"; expected a period'
    with self.assertRaisesRegexp(ValueError, err_pattern):
      self.parse('map_num_str.1a')

  def test_map_str_key_unquoted(self):
    actual = self.parse('map_str_num.a')
    expected = ('map_str_num', 'a')
    self.assertEqual(actual, expected)

  def test_map_str_key_unquoted_longer(self):
    actual = self.parse('map_str_num.ab')
    expected = ('map_str_num', 'ab')
    self.assertEqual(actual, expected)

  def test_map_str_key_quoted(self):
    actual = self.parse('map_str_num.`a`')
    expected = ('map_str_num', 'a')
    self.assertEqual(actual, expected)

  def test_map_str_key_quoted_with_period(self):
    actual = self.parse('map_str_num.`a.b`')
    expected = ('map_str_num', 'a.b')
    self.assertEqual(actual, expected)

  def test_map_str_key_star(self):
    actual = self.parse('map_str_num.*')
    expected = ('map_str_num', field_masks._STAR_SEG)
    self.assertEqual(actual, expected)

  def test_map_str_key_int(self):
    with self.assertRaisesRegexp(ValueError, r'expected a string'):
      self.parse('map_str_num.1')

  def test_map_bool_key_true(self):
    actual = self.parse('map_bool_str.true')
    expected = ('map_bool_str', True)
    self.assertEqual(actual, expected)

  def test_map_bool_key_false(self):
    actual = self.parse('map_bool_str.false')
    expected = ('map_bool_str', False)
    self.assertEqual(actual, expected)

  def test_map_bool_key_invalid(self):
    with self.assertRaisesRegexp(ValueError, r'expected true or false'):
      self.parse('map_bool_str.not-a-bool')

  def test_map_bool_key_star(self):
    actual = self.parse('map_bool_str.*')
    expected = ('map_bool_str', field_masks._STAR_SEG)
    self.assertEqual(actual, expected)

  def test_msg_str(self):
    actual = self.parse('msg.str')
    expected = ('msg', 'str')
    self.assertEqual(actual, expected)

  def test_msg_star(self):
    actual = self.parse('msg.*')
    expected = ('msg', field_masks._STAR_SEG)
    self.assertEqual(actual, expected)

  def test_msg_star_str(self):
    with self.assertRaisesRegexp(ValueError, r'expected end of string'):
      self.parse('msg.*.str')

  def test_msg_unexpected_field(self):
    with self.assertRaisesRegexp(ValueError, r'field "msg.x" does not exist'):
      self.parse('msg.x')

  def test_msg_unexpected_subfield(self):
    with self.assertRaisesRegexp(ValueError, r'"msg.msg.x" does not exist'):
      self.parse('msg.msg.x')

  def test_msg_msgs_str(self):
    actual = self.parse('msgs.*.str')
    expected = ('msgs', field_masks._STAR_SEG, 'str')
    self.assertEqual(actual, expected)

  def test_msg_map_num_str(self):
    actual = self.parse('msg.map_num_str.1')
    expected = ('msg', 'map_num_str', 1)
    self.assertEqual(actual, expected)

  def test_map_str_map_num(self):
    actual = self.parse('map_str_msg.num')
    expected = ('map_str_msg', 'num')
    self.assertEqual(actual, expected)

  def test_map_str_map_num_star(self):
    actual = self.parse('map_str_msg.*')
    expected = ('map_str_msg', field_masks._STAR_SEG)
    self.assertEqual(actual, expected)

  def test_map_str_map_num_star_str(self):
    actual = self.parse('map_str_msg.*.str')
    expected = ('map_str_msg', field_masks._STAR_SEG, 'str')
    self.assertEqual(actual, expected)

  def test_trailing_period(self):
    with self.assertRaisesRegexp(ValueError, r'unexpected end'):
      self.parse('str.')

  def test_trailing_period_period(self):
    with self.assertRaisesRegexp(ValueError, r'cannot start with a period'):
      self.parse('str..')


class NormalizePathsTests(unittest.TestCase):

  def test_empty(self):
    actual = field_masks._normalize_paths([])
    expected = set()
    self.assertEqual(actual, expected)

  def test_normal(self):
    actual = field_masks._normalize_paths([
        ('a',),
        ('b',),
    ])
    expected = {('a',), ('b',)}
    self.assertEqual(actual, expected)

  def test_redundancy_one_level(self):
    actual = field_masks._normalize_paths([
        ('a',),
        ('a', 'b'),
    ])
    expected = {('a',)}
    self.assertEqual(actual, expected)

  def test_redundancy_second_level(self):
    actual = field_masks._normalize_paths([
        ('a',),
        ('a', 'b', 'c'),
    ])
    expected = {('a',)}
    self.assertEqual(actual, expected)


class ParseFieldTreeTests(unittest.TestCase):
  def parse(self, paths):
    return field_masks.parse_field_tree(
        field_mask_pb2.FieldMask(paths=paths),
        test_proto_pb2.Msg.DESCRIPTOR)

  def test_empty(self):
    actual = self.parse([])
    expected = {}
    self.assertEqual(actual, expected)

  def test_str(self):
    actual = self.parse(['str'])
    expected = {'str': {}}
    self.assertEqual(actual, expected)

  def test_str_num(self):
    actual = self.parse(['str', 'num'])
    expected = {'str': {}, 'num': {}}
    self.assertEqual(actual, expected)

  def test_str_msg_num(self):
    actual = self.parse(['str', 'msg.num'])
    expected = {'str': {}, 'msg': {'num': {}}}
    self.assertEqual(actual, expected)

  def test_redunant(self):
    actual = self.parse(['msg', 'msg.num'])
    expected = {'msg': {}}
    self.assertEqual(actual, expected)

  def test_redunant_star(self):
    actual = self.parse(['msg.*', 'msg.msg.num'])
    expected = {'msg': {}}
    self.assertEqual(actual, expected)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
