# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utilities."""

import logging
import os
import signal
import subprocess
import sys


def exec_python(args):
  """Executes a python process, replacing the current process if possible.

  On Windows, it returns the child process code. The caller must exit at the
  earliest opportunity.
  """
  cmd = [sys.executable] + args
  if sys.platform not in ('cygwin', 'win32'):
    os.execv(cmd[0], cmd)
    return 1

  try:
    # On Windows, we cannot sanely exec() so shell out the child process
    # instead. But we need to forward any signal received that the bot may care
    # about. This means processes accumulate, sadly.
    # If stdin closes, it tells the child process that the parent process died.
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    def handler(sig, _):
      logging.info('Got signal %s', sig)
      # In general, sig == SIGBREAK(21) but we cannot send it, so send
      # CTRL_BREAK_EVENT(1) instead.
      proc.send_signal(signal.CTRL_BREAK_EVENT)
    old_handlers = {}
    try:
      for s in (signal.SIGBREAK, signal.SIGTERM):
        old_handlers[s] = signal.signal(s, handler)
      proc.wait()
      return proc.returncode
    finally:
      for s, h in old_handlers.iteritems():
        signal.signal(s, h)
  except Exception as e:
    logging.exception('failed to start: %s', e)
    # Swallow the exception.
    return 1
