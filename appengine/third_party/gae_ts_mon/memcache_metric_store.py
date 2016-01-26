# Copyright 2015 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import collections
import copy
import functools
import logging
import random
import threading

from google.appengine.api import memcache
from google.appengine.api import modules
from google.appengine.ext import ndb

from infra_libs.ts_mon.common import errors
from infra_libs.ts_mon.common import metric_store
from infra_libs.ts_mon.common import metrics


# Note: this metric is not registered in the index, since it's handled in
# a special way.
appengine_default_version = metrics.StringMetric(
    'appengine/default_version',
    description='Name of the version currently marked as default.')

cas_failures = metrics.CounterMetric('appengine/memcache_cas_failures')


class MetricIndexEntry(ndb.Model):
  name = ndb.StringProperty()
  job_name = ndb.StringProperty()
  metric = ndb.PickleProperty()
  registered = ndb.DateTimeProperty(auto_now=True)


class MemcacheMetricStore(metric_store.MetricStore):
  """A metric store that keeps values in App Engine's memcache."""

  CAS_RETRIES = 10
  BASE_NAMESPACE = 'ts_mon_py'
  SHARDS_PER_METRIC = 10

  METRICS_EXCLUDED_FROM_INDEX = (appengine_default_version,)
  METRIC_NAMES_EXCLUDED_FROM_INDEX = set(
      [x.name for x in METRICS_EXCLUDED_FROM_INDEX])

  def __init__(self, state, time_fn=None, report_module_versions=False):
    super(MemcacheMetricStore, self).__init__(state, time_fn=time_fn)

    self.report_module_versions = report_module_versions

    self._thread_local = threading.local()
    self.update_metric_index()

  def _namespace_for_job(self, job_name=None):
    if job_name is None:
      job_name = self._state.target.job_name
    return '%s_%s' % (self.BASE_NAMESPACE, job_name)

  def _target_key(self):
    return self._state.target.hostname

  def _client(self):
    """Returns a google.appengine.api.memcache.Client.

    A different memcache client will be returned for each thread.
    """
    try:
      return self._thread_local.client
    except AttributeError:
      self._thread_local.client = memcache.Client()
      return self._thread_local.client

  def _is_metric_sharded(self, metric):
    if isinstance(metric, (metrics.CounterMetric, metrics.CumulativeMetric)):
      return True
    if isinstance(metric, metrics.DistributionMetric) and metric.is_cumulative:
      return True
    return False

  def _random_shard(self, metric, select_shard=random.randint):
    if not self._is_metric_sharded(metric):
      return metric.name
    return '%s-%d' % (metric.name, select_shard(1, self.SHARDS_PER_METRIC))

  def _all_shards(self, metric):
    if not self._is_metric_sharded(metric):
      return [metric.name]
    return [
        '%s-%d' % (metric.name, shard)
        for shard in xrange(1, self.SHARDS_PER_METRIC + 1)]

  def update_metric_index(self):
    entities = [
        MetricIndexEntry(
            id='%s/%s' % (self._state.target.job_name, name),
            name=name,
            job_name=self._state.target.job_name,
            metric=metric)
        # Iterate over a copy of metrics in case another thread registers a
        # metric and modifies the dict.
        for name, metric in self._state.metrics.items()
        if metric not in self.METRICS_EXCLUDED_FROM_INDEX]
    ndb.put_multi(entities)

  def get(self, name, fields, default=None):
    if name not in self._state.metrics:
      return default

    keys = self._all_shards(self._state.metrics[name])
    entries = self._client().get_multi([(None, x) for x in keys],
                                       namespace=self._namespace_for_job())
    values = []
    for entry in entries.values():
      _, targets_values = entry
      values.append(targets_values.get(self._target_key(), {})
                                  .get(fields, default))

    if not values:
      return default
    if len(values) == 1:
      return values[0]
    if not all(isinstance(x, (int, float)) for x in values):
      raise TypeError(
          'get() is not supported on sharded cumulative distribution metrics')
    return sum(values)

  def get_all(self):
    if self.report_module_versions:
      for module_name in modules.get_modules():
        # 'hostname' is usually set to module version name, but default version
        # name is a global thing, not associated with any version.
        target = copy.copy(self._state.target)
        target.hostname = ''
        target.job_name = module_name
        fields_values = {
            appengine_default_version._normalize_fields(None):
                modules.get_default_version(module_name),
        }
        yield (target, appengine_default_version, 0, fields_values)

    client = self._client()
    target = copy.copy(self._state.target)

    # Fetch the metric index from datastore.
    all_entities = collections.defaultdict(dict)
    for entity in MetricIndexEntry.query():
      all_entities[entity.job_name][entity.name] = entity

    for job_name, entities in all_entities.iteritems():
      target.job_name = job_name

      # Create the list of keys to fetch.  Sharded metrics (counters) are
      # spread across multiple keys in memcache.
      keys = []
      shard_map = {}  # key -> (index, metric name)
      for name, entity in entities.iteritems():
        if self._is_metric_sharded(entity.metric):
          for i, sharded_key in enumerate(self._all_shards(entity.metric)):
            keys.append(sharded_key)
            shard_map[sharded_key] = (i, name)
        else:
          keys.append(name)

      # Fetch all the keys in this namespace.
      values = self._client().get_multi(
          keys, namespace=self._namespace_for_job(job_name))

      for key, (start_time, targets_values) in values.iteritems():
        for hostname, fields_values in targets_values.iteritems():
          target.hostname = hostname
          if key in shard_map:
            # This row is one shard of a sharded metric - put the shard number
            # in the task number.
            target.task_num, metric_name = shard_map[key]
          else:
            target.task_num, metric_name = 0, key

          yield (copy.copy(target), entities[metric_name].metric, start_time,
                 fields_values)

  def _apply_all(self, modifications, entry):
    """Applies all the modifications, in order, to a memcache entry.

    All modifications must be for the same metric."""

    if not modifications:  # pragma: no cover
      return entry

    # All modifications should be for the same metric.
    name = modifications[0].name

    if entry is None:
      entry = (self._start_time(name), {})

    _, targets = entry

    target_key = self._target_key()
    values = targets.setdefault(target_key, {})

    for mod in modifications:
      value = values.get(mod.fields, 0)

      if mod.mod_type == 'set':
        new_value, enforce_ge = mod.args
        if enforce_ge and new_value < value:
          raise errors.MonitoringDecreasingValueError(name, value, new_value)
        value = new_value
      elif mod.mod_type == 'incr':
        delta, modify_fn = mod.args
        if modify_fn is None:
          modify_fn = metric_store.default_modify_fn(name)
        value = modify_fn(value, delta)
      else:
        raise errors.UnknownModificationTypeError(mod.mod_type)

      values[mod.fields] = value

    return entry

  def _compare_and_set(self, modifications, namespace):
    client = self._client()

    # Metrics that we haven't updated yet.  Metrics are removed from this dict
    # when they're successfully updated - if there are any left they will be
    # retried 10 times until everything has been updated.
    # We might have more than one modification for a metric if different field
    # values were updated.
    remaining = collections.defaultdict(list)
    for modification in modifications:
      remaining[modification.name].append(modification)

    failed_keys_count = 0

    for _ in xrange(self.CAS_RETRIES):
      keys = []
      key_map = {}  # key -> metric name (for sharded metrics)

      for name in remaining:
        # Pick one of the shards to modify.
        key = self._random_shard(self._state.metrics[name])
        keys.append(key)
        key_map[key] = name

      # Get all existing entries.
      cas_mapping = {}  # key -> new value (for existing entries)
      add_mapping = {}  # key -> new value (for new entries)

      entries = client.get_multi(keys, for_cas=True, namespace=namespace)
      for key, entry in entries.iteritems():
        cas_mapping[key] = self._apply_all(remaining[key_map[key]], entry)

      for key in keys:
        if key not in entries:
          add_mapping[key] = self._apply_all(remaining[key_map[key]], None)

      # Add entries that weren't present before.
      failed_keys = []
      if add_mapping:
        failed_keys.extend(client.add_multi(add_mapping, namespace=namespace))

      # Compare-and-set entries that were present before.
      if cas_mapping:
        failed_keys.extend(client.cas_multi(cas_mapping, namespace=namespace))

      if not failed_keys:
        break
      failed_keys_count += len(failed_keys)

      # Retry only failed keys.
      still_remaining = {}
      for failed_key in failed_keys:
        name = key_map[failed_key]
        still_remaining[name] = remaining[name]
      remaining = still_remaining
    else:
      logging.warning(
          'Memcache compare-and-set failed %d times for keys %s in '
          'namespace %s',
          self.CAS_RETRIES, remaining.keys(), namespace)

    # Update the cas_failures metric with the number of failed keys, but don't
    # do so recursively.
    if (failed_keys_count and
        any(modification.name != cas_failures.name
            for modification in modifications)):
      cas_failures.increment_by(failed_keys_count)

  def _compare_and_set_metrics(self, modifications):
    if any(mod.name in self.METRIC_NAMES_EXCLUDED_FROM_INDEX
           for mod in modifications):
      raise errors.MonitoringError('Metric is magical, can\'t set it')

    self._compare_and_set(modifications, self._namespace_for_job())

  def set(self, name, fields, value, enforce_ge=False):
    self._compare_and_set_metrics([metric_store.Modification(
        name, fields, 'set', (value, enforce_ge))])

  def incr(self, name, fields, delta, modify_fn=None):
    self._compare_and_set_metrics([metric_store.Modification(
        name, fields, 'incr', (delta, modify_fn))])

  def modify_multi(self, modifications):
    self._compare_and_set_metrics(modifications)

  def reset_for_unittest(self, name=None):
    if name is None:
      self._client().delete_multi(self._state.metrics.keys(),
                                  namespace=self._namespace_for_job())
    else:
      self._client().delete(name, namespace=self._namespace_for_job())
