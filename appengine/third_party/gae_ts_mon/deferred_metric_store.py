# Copyright 2015 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import logging
import threading

from infra_libs.ts_mon.common import metric_store


class FinalizeWithoutInitializeError(Exception):
  def __init__(self):
    super(FinalizeWithoutInitializeError, self).__init__(
        'finalize_context called before initialize_context in a thread')


class DeferredMetricStore(metric_store.MetricStore):
  def __init__(self, state, base_store, time_fn=None):
    super(DeferredMetricStore, self).__init__(state, time_fn=time_fn)

    self._base_store = base_store
    self._thread_local = threading.local()

  def initialize_context(self):
    self._thread_local.deferred = []

  def finalize_context(self):
    try:
      deferred = self._thread_local.deferred
    except AttributeError:
      raise FinalizeWithoutInitializeError()
    else:
      del self._thread_local.deferred
      self._thread_local.finalizing = True
      try:
        self._base_store.modify_multi(deferred)
      finally:
        self._thread_local.finalizing = False

  def update_metric_index(self):
    self._base_store.update_metric_index()

  def get(self, name, fields, default=None):
    return self._base_store.get(name, fields, default)

  def get_all(self):
    return self._base_store.get_all()

  def set(self, name, fields, value, enforce_ge=False):
    try:
      deferred = self._thread_local.deferred
    except AttributeError:
      if not getattr(
          self._thread_local, 'finalizing', False):  # pragma: no cover
        logging.warning(
            'DeferredMetricStore is used without a context.  Have you wrapped '
            'your WSGIApplication with gae_ts_mon.initialize?')
      self._base_store.set(name, fields, value, enforce_ge)
    else:
      deferred.append(
          metric_store.Modification(name, fields, 'set', (value, enforce_ge)))

  def incr(self, name, fields, delta, modify_fn=None):
    try:
      deferred = self._thread_local.deferred
    except AttributeError:
      if not getattr(
          self._thread_local, 'finalizing', False):  # pragma: no cover
        logging.warning(
            'DeferredMetricStore is used without a context.  Have you wrapped '
            'your WSGIApplication with gae_ts_mon.initialize?')
      self._base_store.incr(name, fields, delta, modify_fn)
    else:
      deferred.append(
          metric_store.Modification(name, fields, 'incr', (delta, modify_fn)))

  def reset_for_unittest(self, name=None):
    self._base_store.reset_for_unittest(name)

    try:
      deferred = self._thread_local.deferred
    except AttributeError:
      pass
    else:
      if name is None:
        self._thread_local.deferred = []
      else:
        self._thread_local.deferred = [x for x in deferred if x.name != name]
