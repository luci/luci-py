# Copyright 2015 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import imp
import os
import sys

import infra_libs.httplib2_utils

from config import DjangoMiddleware
from config import initialize
from config import instrument_endpoint
from config import instrument_wsgi_application
from config import reset_for_unittest
from handlers import app

# The remaining lines are copied from infra_libs/ts_mon/__init__.py.
from infra_libs.ts_mon.common.distribution import Distribution
from infra_libs.ts_mon.common.distribution import FixedWidthBucketer
from infra_libs.ts_mon.common.distribution import GeometricBucketer

from infra_libs.ts_mon.common.errors import MonitoringError
from infra_libs.ts_mon.common.errors import MonitoringDecreasingValueError
from infra_libs.ts_mon.common.errors import MonitoringDuplicateRegistrationError
from infra_libs.ts_mon.common.errors import MonitoringIncrementUnsetValueError
from infra_libs.ts_mon.common.errors import MonitoringInvalidFieldTypeError
from infra_libs.ts_mon.common.errors import MonitoringInvalidValueTypeError
from infra_libs.ts_mon.common.errors import MonitoringTooManyFieldsError
from infra_libs.ts_mon.common.errors import MonitoringNoConfiguredMonitorError
from infra_libs.ts_mon.common.errors import MonitoringNoConfiguredTargetError

from infra_libs.ts_mon.common.helpers import ScopedIncrementCounter
from infra_libs.ts_mon.common.helpers import ScopedMeasureTime

from infra_libs.ts_mon.common.interface import close
from infra_libs.ts_mon.common.interface import flush
from infra_libs.ts_mon.common.interface import register_global_metrics
from infra_libs.ts_mon.common.interface import register_global_metrics_callback

from infra_libs.ts_mon.common.metrics import BooleanField
from infra_libs.ts_mon.common.metrics import IntegerField
from infra_libs.ts_mon.common.metrics import StringField

from infra_libs.ts_mon.common.metrics import BooleanMetric
from infra_libs.ts_mon.common.metrics import CounterMetric
from infra_libs.ts_mon.common.metrics import CumulativeDistributionMetric
from infra_libs.ts_mon.common.metrics import CumulativeMetric
from infra_libs.ts_mon.common.metrics import FloatMetric
from infra_libs.ts_mon.common.metrics import GaugeMetric
from infra_libs.ts_mon.common.metrics import MetricsDataUnits
from infra_libs.ts_mon.common.metrics import NonCumulativeDistributionMetric
from infra_libs.ts_mon.common.metrics import StringMetric

from infra_libs.ts_mon.common.targets import TaskTarget
from infra_libs.ts_mon.common.targets import DeviceTarget
