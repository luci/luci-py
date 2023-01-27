# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Helpers for counting time and sleeping in the bot main loop."""

import logging
import time


class TimerCanceledException(Exception):
  """Raised by Timer methods if the timer was already canceled."""

  def __init__(self):
    super().__init__('The timer was canceled already')


class Clock:
  """A monotonic clock that tracks timers and knows when to wake up.

  Its main purpose is to simplify doing concurrent work in a single threaded
  bot poll loop. Each concurrent activity maintains a timer and advances itself
  when it ticks.

  The implementation assumes a small number of active timers (Swarming bot needs
  at most 3).
  """

  def __init__(self, stop_event):
    """Initializes the clock.

    Arguments:
      stop_event: threading.Event signaled if we need to interrupt a sleep.
    """
    self._stop_event = stop_event
    self._timers = []
    self._now_impl = time.monotonic  # can be mocked in tests

  def now(self):
    """Returns the current monotonic time as a float number of seconds.

    The absolute value is meaningless, but a difference between two times
    produced by the same Clock instance can be used to measure time intervals.
    """
    return float(self._now_impl())

  def timer(self, duration):
    """Schedules a timer that fires `duration` seconds in the future.

    If `duration` is zero or negative, the returned timer will already be
    firing.

    After timer fires, it either needs to be reset or canceled.

    Returns:
      Timer.
    """
    t = Timer(self, self.now() + duration)
    self._timers.append(t)
    if len(self._timers) > 10:
      logging.warning('Too many pending timers: %d!', len(self._timers))
    return t

  def sleep(self, duration):
    """Unconditionally sleeps for at least `duration` seconds.

    Arguments:
      duration: how long to sleep. If zero or negative, will do nothing.

    Returns:
      True if slept, False if was interrupted by `stop_event` signal.
    """
    while duration > 0.0:
      start = self.now()
      try:
        self._stop_event.wait(duration)
      except IOError:
        # This can happen on signals in Python 3.4 and older.
        logging.exception('IOError while waiting in threading.Event')
      if self._stop_event.is_set():
        return False
      duration -= (self.now() - start)
    return True

  def wait_next_timer(self):
    """Sleeps until the next scheduled timer and returns it.

    If the current time is already past the earliest scheduled tick, doesn't
    actually sleep and returns immediately. The corresponding timer needs to
    be reset or canceled, otherwise wait_next_timer(...) will just keep
    returning it.

    If there are no ticks scheduled (which must never happen), logs an
    error, sleeps 30 sec (just to avoid busy looping) and returns None.

    If the sleep was interrupted by firing `stop_event`, returns None as well.
    """
    if not self._timers:
      logging.error('No active timers, sleeping for 30 sec')
      self.sleep(30.0)
      return None

    # We'll just scan the whole set, since it should be very small. If this
    # code ever needs to support a lot of timers, the unsorted list should be
    # replaced with a priority queue.
    earliest = min(self._timers, key=lambda t: t._when)

    # Sleep until this timer is firing. This loop accounts for potential
    # inaccuracies in sleep(...).
    while not earliest.firing:
      if not self.sleep(earliest._when - self.now()):
        return None

    return earliest


class Timer:
  """A timer scheduled via Clock.timer(...)."""

  def __init__(self, clock, when):
    self._clock = clock
    self._when = when

  @property
  def firing(self):
    """True if the timer's tick already happened.

    This will keep returning True until the timer is reset.
    """
    if not self._clock:
      raise TimerCanceledException()
    return self._clock.now() >= self._when

  def reset(self, duration):
    """Makes the timer tick `duration` seconds in the future.

    If there's already a pending scheduled tick of this timer, just moves it.

    If `duration` is zero or negative, the timer is immediately moved into
    firing state.
    """
    if not self._clock:
      raise TimerCanceledException()
    self._when = self._clock.now() + duration

  def cancel(self):
    """Cancels this timer. It should not be used after being canceled."""
    if not self._clock:
      raise TimerCanceledException()
    self._clock._timers.remove(self)
    self._clock = None
