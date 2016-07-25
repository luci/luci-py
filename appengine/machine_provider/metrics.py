# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Metrics to track with ts_mon and event_mon."""

import gae_ts_mon


lease_requests_deduped = gae_ts_mon.CounterMetric(
    'machine_provider/lease_requests/deduped',
    description='Number of lease requests deduplicated.',
)


lease_requests_fulfilled = gae_ts_mon.CounterMetric(
    'machine_provider/lease_requests/fulfilled',
    description='Number of lease requests fulfilled.',
)


lease_requests_received = gae_ts_mon.CounterMetric(
    'machine_provider/lease_requests/received',
    description='Number of lease requests received.',
)
