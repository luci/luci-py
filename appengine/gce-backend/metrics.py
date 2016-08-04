# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Metrics to track with ts_mon and event_mon."""

import gae_ts_mon


config_valid = gae_ts_mon.BooleanMetric(
    'machine_provider/gce_backend/config/valid',
    description='Whether or not the current config is valid.',
)
