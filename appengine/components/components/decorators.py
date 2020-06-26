# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Handlers decorators."""

import functools
import logging


def silence(*exceptions):
  """Eats the exceptions listed and log a warning instead of an error.

  Sets the error code to HTTP 500. This will cause taskqueue to be automatically
  retried.
  """
  def decorator(f):
    @functools.wraps(f)
    def hook(self, *args, **kwargs):
      try:
        return f(self, *args, **kwargs)
      except tuple(exceptions) as e:  # pylint: disable=catching-non-exception
        logging.warning('Silencing exception %s', e, exc_info=True)
        self.abort(429, 'Silencing exception')
    return hook
  return decorator


def require_cronjob(f):
  """Enforces cronjob."""
  @functools.wraps(f)
  def hook(self, *args, **kwargs):
    if self.request.headers.get('X-AppEngine-Cron') != 'true':
      self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
      msg = 'Only internal cron jobs can do this'
      logging.error(msg)
      self.abort(403, msg)
      return
    return f(self, *args, **kwargs)
  return hook


def require_taskqueue(*queue_names):
  """Enforces the task is run by specific task queue(s)."""
  assert all(isinstance(q, basestring) for q in queue_names), (
      map(type, queue_names), queue_names)
  queue_names = frozenset(queue_names)

  def decorator(f):
    @functools.wraps(f)
    def hook(self, *args, **kwargs):
      actual_name = self.request.headers.get('X-AppEngine-QueueName')
      if actual_name not in queue_names:
        self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
        msg = 'Only internal queue(s) %s can do this' % (queue_names,)
        if actual_name:
          msg += '; got %s' % actual_name
        logging.error(msg)
        self.abort(403, msg)
        return
      return f(self, *args, **kwargs)
    return hook
  return decorator
