# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Metrics to track with ts_mon and event_mon."""

import gae_ts_mon

import instances


# Overrides to create app-global metrics.
GLOBAL_TARGET_FIELDS = {
  # Name of the module reporting the metric.
  'job_name': '',
  # Version of the app reporting the metric.
  'hostname': '',
  # ID of the instance reporting the metric.
  'task_num': 0,
}


GLOBAL_METRICS = {
    'instances': gae_ts_mon.GaugeMetric(
        'machine_provider/gce_backend/instances',
        description='Current count of the number of instances.',
    ),
}


config_valid = gae_ts_mon.BooleanMetric(
    'machine_provider/gce_backend/config/valid',
    description='Whether or not the current config is valid.',
)


def compute_global_metrics():
  orphaned, total = instances.count_instances()
  GLOBAL_METRICS['instances'].set(
      orphaned,
      fields={
          'orphaned': True,
      },
      target_fields=GLOBAL_TARGET_FIELDS,
  )
  GLOBAL_METRICS['instances'].set(
      total - orphaned,
      fields={
          'orphaned': False,
      },
      target_fields=GLOBAL_TARGET_FIELDS,
  )


def initialize():
  gae_ts_mon.register_global_metrics(GLOBAL_METRICS.values())
  gae_ts_mon.register_global_metrics_callback(
      'callback', compute_global_metrics)
