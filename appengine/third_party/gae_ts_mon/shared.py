# Copyright 2016 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import contextlib

from google.appengine.api import modules
from google.appengine.api import namespace_manager
from google.appengine.ext import ndb

from infra_libs.ts_mon.common import metrics

REGION = 'appengine'
PUBSUB_PROJECT = 'chrome-infra-mon-pubsub'
PUBSUB_TOPIC = 'monacq'
INSTANCE_NAMESPACE = 'ts_mon_instance_namespace'
# Duration of inactivity to consider an instance dead.
INSTANCE_EXPIRE_SEC = 30 * 60
INSTANCE_EXPECTED_TO_HAVE_TASK_NUM_SEC = 5 * 60
INTERNAL_CALLBACK_NAME = '__gae_ts_mon_callback'


appengine_default_version = metrics.StringMetric(
    'appengine/default_version',
    'Name of the version currently marked as default.',
    None)
started_counter = metrics.CounterMetric(
    'appengine/instances/started',
    'Count the number of GAE instance initializations.',
    None)
shutdown_counter = metrics.CounterMetric(
    'appengine/instances/shutdown',
    'Count the number of GAE instance shutdowns.',
    None)
expired_counter = metrics.CounterMetric(
    'appengine/instances/expired',
    'Count the number of GAE instance expirations due to inactivity.',
    None)


global_metrics = {}
global_metrics_callbacks = {}


def reset_for_unittest():
  global global_metrics
  global global_metrics_callbacks
  global_metrics = {}
  global_metrics_callbacks = {}


def register_global_metrics(metrics):
  """Declare metrics as global.

  Registering a metric as "global" simply means it will be reset every
  time the metric is sent. This allows any instance to send such a
  metric to a shared stream, e.g. by overriding target fields like
  task_num (instance ID), host_name (version) or job_name (module
  name).

  There is no "unregister". Multiple calls add up. It only needs to be
  called once, similar to gae_ts_mon.initialize().

  Args:
    metrics (iterable): a collection of Metric objects.
  """
  global_metrics.update({m.name: m for m in metrics})


def register_global_metrics_callback(name, callback):
  """Register a named function to compute global metrics values.

  There can only be one callback for a given name. Setting another
  callback with the same name will override the previous one. To disable
  a callback, set its function to None.

  Args:
    name (string): name of the callback.
    callback (function): this function will be called without arguments
      every minute from the gae_ts_mon cron job. It is intended to set the
      values of the global metrics.
  """
  if not callback:
    if name in global_metrics_callbacks:
      del global_metrics_callbacks[name]
  else:
    global_metrics_callbacks[name] = callback


class Instance(ndb.Model):
  """Used to map instances to small integers.

  Each instance "owns" an entity with the key <instance-id>.<version>.<module>.
  `task_num` is a mapping assigned by a cron job to the instance; -1=undefined.
  """

  @classmethod
  def _get_kind(cls):
    return 'TSMonInstance'

  task_num = ndb.IntegerProperty(default=-1)
  last_updated = ndb.DateTimeProperty(auto_now_add=True)


def instance_key_id():
  return '%s.%s.%s' % (
      modules.get_current_instance_id(),
      modules.get_current_version_name(),
      modules.get_current_module_name())


@contextlib.contextmanager
def instance_namespace_context():
  previous_namespace = namespace_manager.get_namespace()
  try:
    namespace_manager.set_namespace(INSTANCE_NAMESPACE)
    yield
  finally:
    namespace_manager.set_namespace(previous_namespace)


def get_instance_entity():
  with instance_namespace_context():
    return Instance.get_or_insert(instance_key_id())
