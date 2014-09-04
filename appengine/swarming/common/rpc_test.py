#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import unittest

# pylint: disable=W0403
import rpc


class TestRPC(unittest.TestCase):
  def test_build(self):
    # Should accept empty args without an error.
    input_function = 'some function'
    input_args = None
    rpc_packet = rpc.BuildRPC(input_function, input_args)
    function, args = rpc.ParseRPC(rpc_packet)
    self.assertEqual(function, input_function)
    self.assertEqual(args, input_args)

    # Make sure the functions have reverse functionality of each other
    # with list as arguments.
    input_function = 'function name'
    input_args = ['123', 123, 'some text']
    rpc_packet = rpc.BuildRPC(input_function, input_args)
    function, args = rpc.ParseRPC(rpc_packet)
    self.assertEqual(function, input_function)
    self.assertEqual(args, input_args)

    # Make sure the functions have reverse functionality of each other
    # with string as only argument.
    input_function = 'function'
    input_args = 'some text'
    rpc_packet = rpc.BuildRPC(input_function, input_args)
    function, args = rpc.ParseRPC(rpc_packet)
    self.assertEqual(function, input_function)
    self.assertEqual(args, input_args)

  def test_parse_fail_container(self):
    with self.assertRaises(rpc.RPCError):
      rpc.ParseRPC(['function', 'args'])

  def test_parse_fail_name(self):
    with self.assertRaises(rpc.RPCError):
      rpc.ParseRPC({'args': [1, 2, 3]})

  def test_parse_fail_args(self):
    with self.assertRaises(rpc.RPCError):
      rpc.ParseRPC({'function': 'func'})

  def test_parse_fail_extra(self):
    with self.assertRaises(rpc.RPCError):
      rpc.ParseRPC({'function': 'func', 'args': None, 'extra_args': 'invalid'})

  def test_parse_fail_function_type(self):
    with self.assertRaises(rpc.RPCError):
      rpc.ParseRPC({'function': [1234], 'args': [1234]})


if __name__ == '__main__':
  unittest.main()
