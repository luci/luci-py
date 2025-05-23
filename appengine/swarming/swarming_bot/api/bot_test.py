#!/usr/bin/env vpython3
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import os
import struct
import sys
import unittest

import test_env_api
test_env_api.setup_test_env()

import bot


def make_bot(remote=None):
  return bot.Bot(remote or FakeRemote(),
                 {'dimensions': {
                     'id': ['bot1'],
                     'pool': ['private']
                 }}, 'https://localhost:1', 'base_dir')


class FakeRemote:
  post_bot_event_cb = None

  @property
  def bot_id(self):
    return 'bot1'

  def post_bot_event(self, event_type, message, attributes):
    if self.post_bot_event_cb:
      self.post_bot_event_cb(event_type, message, attributes)


class TestBot(unittest.TestCase):
  def test_get_pseudo_rand(self):
    # This test assumes little endian.
    # The following confirms the equivalent code in Bot.get_pseudo_rand():
    self.assertEqual(-1., round(struct.unpack('h', b'\x00\x80')[0] / 32768., 4))
    self.assertEqual(1., round(struct.unpack('h', b'\xff\x7f')[0] / 32768., 4))
    b = make_bot()
    self.assertEqual(-0.7782, b.get_pseudo_rand(1.))
    self.assertEqual(-0.0778, b.get_pseudo_rand(.1))

  def test_post_error(self):
    # Not looking at the actual stack since the file name is call dependent and
    # the line number will change as the code is modified.
    prefix = 'US has failed us\nCalling stack:\n  0  '
    calls = []

    def post_bot_event(event_type, message, attributes):
      try:
        self.assertEqual('bot_error', event_type)
        expected = {
            'dimensions': {
                'id': ['bot1'],
                'pool': ['private']
            },
            'state': {
                'rbe_instance': None,
            },
            'version': 'unknown',
        }
        self.assertEqual(expected, attributes)
        self.assertTrue(message.startswith(prefix), repr(message))
        calls.append(event_type)
      except Exception as e:
        calls.append(str(e))


    remote = FakeRemote()
    remote.post_bot_event_cb = post_bot_event
    make_bot(remote).post_error('US has failed us')
    self.assertEqual(['bot_error'], calls)

  def test_frame(self):
    stack = bot._make_stack()
    # Not looking at the actual stack since the file name is call dependent and
    # the line number will change as the code is modified.
    self.assertTrue(stack.startswith('  0  '), repr(stack))

  def test_bot(self):
    obj = make_bot()
    self.assertEqual({'id': ['bot1'], 'pool': ['private']}, obj.dimensions)
    self.assertEqual(os.path.join(obj.base_dir, 'swarming_bot.zip'),
                     obj.swarming_bot_zip)
    self.assertEqual('base_dir', obj.base_dir)

  def test_attribute_updates(self):
    obj = make_bot()

    with obj.mutate_internals() as mut:
      mut.update_bot_group_cfg({'dimensions': {'pool': ['A']}})
    self.assertEqual({'id': ['bot1'], 'pool': ['A']}, obj.dimensions)
    self.assertEqual({'rbe_instance': None}, obj.state)

    # Dimension in bot_group_cfg ('A') wins over custom one ('B').
    with obj.mutate_internals() as mut:
      mut.update_dimensions({'id': ['bot1'], 'foo': ['baz'], 'pool': ['B']})
    self.assertEqual({
        'id': ['bot1'],
        'foo': ['baz'],
        'pool': ['A']
    }, obj.dimensions)

  def test_lifecycle_callbacks(self):
    obj = make_bot()
    calls = []
    with obj.mutate_internals() as mut:
      mut.add_lifecycle_callback(lambda _bot, ev: calls.append('0 %s' % ev))
      mut.add_lifecycle_callback(lambda _bot, ev: calls.append('1 %s' % ev))
    obj.run_lifecycle_callbacks('exit')
    self.assertEqual(calls, ['1 exit', '0 exit'])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
