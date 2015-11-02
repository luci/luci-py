#!/usr/bin/env python
# Copyright 2015 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import sys
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(THIS_DIR))
from adb import parallel


class Foo(Exception):
  pass


class Test(unittest.TestCase):
  def test_0(self):
    self.assertEqual([], parallel.pmap(self.fail, []))

  def test_1(self):
    self.assertEqual([1], parallel.pmap(lambda x: x+1, [0]))

  def test_2(self):
    self.assertEqual([1, 2], parallel.pmap(lambda x: x+1, [0, 1]))

  def test_throw_1(self):
    def fn(_):
      raise Foo()
    with self.assertRaises(Foo):
      parallel.pmap(fn, [0])

  def test_throw_2(self):
    def fn(_):
      raise Foo()
    with self.assertRaises(Foo):
      parallel.pmap(fn, [0, 1])

  def test_locked(self):
    with parallel._POOL_LOCK:
      self.assertEqual([1, 2], parallel.pmap(lambda x: x+1, [0, 1]))

  def test_junk(self):
    parallel._QUEUE_OUT.put(0)
    self.assertEqual([1, 2], parallel.pmap(lambda x: x+1, [0, 1]))
    self.assertEqual(0, parallel._QUEUE_OUT.qsize())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None  # pragma: no cover
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
