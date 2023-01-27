#!/usr/bin/env vpython3
# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import threading
import unittest

import test_env_bot_code
test_env_bot_code.setup_test_env()

import clock


class FakeNow:
  def __init__(self):
    self._now = 12345.0

  def now(self):
    return self._now

  def advance(self, delta):
    self._now += max(delta, 0.0)


class FakeThreadingEvent:
  def __init__(self, fake_now):
    self.fake_now = fake_now
    self.signaled = False

  def is_set(self):
    return self.signaled

  def wait(self, timeout):
    # Emulate "random" sleeping inaccuracies by adding 5ms.
    assert timeout >= 0.0, timeout
    self.fake_now.advance(timeout + 0.005)
    return self.signaled

  def set(self):
    self.signaled = True


class TestClock(unittest.TestCase):
  def setUp(self):
    super(TestClock, self).setUp()
    self.fake_now = FakeNow()
    self.fake_stop = FakeThreadingEvent(self.fake_now)
    self.clock = clock.Clock(self.fake_stop)
    self.clock._now_impl = self.fake_now.now

  def test_sleep(self):
    t0 = self.clock.timer(0)
    t1 = self.clock.timer(1)
    t2 = self.clock.timer(2)

    self.assertTrue(t0.firing)
    self.assertFalse(t1.firing)
    self.assertFalse(t2.firing)

    self.clock.sleep(1)
    self.assertTrue(t0.firing)
    self.assertTrue(t1.firing)
    self.assertFalse(t2.firing)

    t1.reset(2)
    self.assertFalse(t1.firing)

    self.clock.sleep(1)
    self.assertTrue(t0.firing)
    self.assertFalse(t1.firing)
    self.assertTrue(t2.firing)

    self.clock.sleep(1)
    self.assertTrue(t0.firing)
    self.assertTrue(t1.firing)
    self.assertTrue(t2.firing)

  def test_negative_duration(self):
    t0 = self.clock.timer(-1)
    self.assertTrue(t0.firing)
    self.clock.sleep(-2)
    self.assertTrue(t0.firing)

  def test_wait_next_timer(self):
    t0 = self.clock.timer(0)
    t1 = self.clock.timer(1)

    self.assertEqual(self.clock.wait_next_timer(), t0)
    self.assertTrue(t0.firing)
    self.assertFalse(t1.firing)

    # Resetting => should stop firing, will fire in the future.
    t0.reset(2)
    self.assertFalse(t0.firing)

    self.assertEqual(self.clock.wait_next_timer(), t1)
    self.assertFalse(t0.firing)
    self.assertTrue(t1.firing)

    # Not resetting => getting the same firing timer back.
    self.assertEqual(self.clock.wait_next_timer(), t1)
    self.assertFalse(t0.firing)
    self.assertTrue(t1.firing)

    # Canceling the timer removes it from the active timers list.
    t1.cancel()
    self.assertEqual(self.clock.wait_next_timer(), t0)
    self.assertTrue(t0.firing)

    # Removing all timers => wait_next_timer just sleeps and returns None.
    t0.cancel()
    self.assertIsNone(self.clock.wait_next_timer())

  def test_canceled_timer(self):
    t = self.clock.timer(0)
    t.cancel()
    with self.assertRaises(clock.TimerCanceledException):
      _ = t.firing
    with self.assertRaises(clock.TimerCanceledException):
      t.reset(0.0)
    with self.assertRaises(clock.TimerCanceledException):
      t.cancel()


class TestClockUnmocked(unittest.TestCase):
  def test_works(self):
    e = threading.Event()
    c = clock.Clock(e)

    t0 = c.timer(0.01)
    t0_ticks = 0

    t1 = c.timer(0.05)
    t1_ticks = 0

    deadline = c.now() + 1
    while c.wait_next_timer():
      if t0.firing:
        t0_ticks += 1
        t0.reset(0.01)
      if t1.firing:
        t1_ticks += 1
        t1.reset(0.05)
      if c.now() > deadline:
        e.set()

    # Since this test is using real clock and real sleep, the number of ticks
    # can vary significantly across execution. But it should be within some
    # reasonable range.
    self.assertGreater(t0_ticks, 1)
    self.assertLess(t0_ticks, 1 / 0.01 + 1)
    self.assertGreater(t1_ticks, 1)
    self.assertLess(t1_ticks, 1 / 0.05 + 1)


if __name__ == '__main__':
  unittest.main()
