# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Timeseries metrics."""

import gae_ts_mon

from components import auth

import config

_bytes_requested = gae_ts_mon.CounterMetric(
    'downloads/bytes', 'Bytes requested for download by clients.', [
        gae_ts_mon.StringField('client_name'),
        gae_ts_mon.StringField('client_email'),
        gae_ts_mon.StringField('download_source'),
    ])


def file_size(size):
  """Reports the size of a file fetched from GCS by allowed clients.

  If the client's requests are not allowed for monitoring, does nothing.

  Args:
    size: Size of the file in bytes.
  """
  ip = auth.get_peer_ip()
  for cfg in config.settings().client_monitoring_config:
    if auth.is_in_ip_whitelist(cfg.ip_whitelist, ip):
      _bytes_requested.increment_by(
          size,
          fields={
              'client_name': cfg.label,
              'client_email': auth.get_peer_identity().to_bytes(),
              'download_source': 'GCS'
          })
      return
