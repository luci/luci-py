#!/usr/bin/python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Unit tests for catalog.py."""

import collections
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import datastore_utils
from components import machine_provider
from test_support import test_case

import catalog
import instances
import models


class ExtractDimensionsTest(test_case.TestCase):
  """Tests for catalog.extract_dimensions."""

  def test_no_dimensions(self):
    """Ensures basic dimensions are returned when there are no others."""
    instance = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    )
    instance_template_revision = models.InstanceTemplateRevision(
    )
    expected_dimensions = {
        'backend': 'GCE',
        'hostname': 'instance-name',
    }

    self.assertEqual(
        catalog.extract_dimensions(instance, instance_template_revision),
        expected_dimensions,
    )

  def test_dimensions(self):
    """Ensures dimensions are returned."""
    instance = models.Instance(
        key=instances.get_instance_key(
            'base-name',
            'revision',
            'zone',
            'instance-name',
        ),
    )
    instance_template_revision = models.InstanceTemplateRevision(
        dimensions=machine_provider.Dimensions(
            os_family=machine_provider.OSFamily.LINUX,
        ),
        disk_size_gb=300,
        machine_type='n1-standard-8',
    )
    expected_dimensions = {
        'backend': 'GCE',
        'disk_size_gb': 300,
        'hostname': 'instance-name',
        'memory_gb': 30,
        'num_cpus': 8,
        'os_family': 'LINUX',
    }

    self.assertEqual(
        catalog.extract_dimensions(instance, instance_template_revision),
        expected_dimensions,
    )


if __name__ == '__main__':
  unittest.main()
