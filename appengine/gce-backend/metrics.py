# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Metrics to track with ts_mon and event_mon."""

import logging

import gae_event_mon
import gae_ts_mon

import instance_group_managers


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
        'Current count of the number of instances.',
        [gae_ts_mon.StringField('instance_template')]
    ),
}


config_valid = gae_ts_mon.BooleanMetric(
    'machine_provider/gce_backend/config/valid',
    'Whether or not the current config is valid.',
    [gae_ts_mon.StringField('config')],
)


def compute_global_metrics(): # pragma: no cover
  for name, count in instance_group_managers.count_instances().iteritems():
    logging.info('%s: %s', name, count)
    GLOBAL_METRICS['instances'].set(
        count,
        fields={
            'instance_template': name,
        },
        target_fields=GLOBAL_TARGET_FIELDS,
    )


def initialize(): # pragma: no cover
  gae_ts_mon.register_global_metrics(GLOBAL_METRICS.values())
  gae_ts_mon.register_global_metrics_callback(
      'callback', compute_global_metrics)


def send_machine_event(state, hostname): # pragma: no cover
  """Sends an event_mon event about a GCE instance.

  Args:
    state: gae_event_mon.ChromeInfraEvent.GCEBackendMachineState.
    hostname: Name of the GCE instance this event is for.
  """
  state = gae_event_mon.MachineProviderEvent.GCEBackendMachineState.Value(state)
  event = gae_event_mon.Event('POINT')
  event.proto.event_source.host_name = hostname
  event.proto.machine_provider_event.gce_backend_state = state
  logging.info('Sending event: %s', event.proto)
  event.send()
