#!/usr/bin/python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Unit tests for config.py."""

import unittest

import test_env
test_env.setup_test_env()

from components import datastore_utils
from test_support import test_case

import config
from proto import config_pb2


class UpdateConfigTest(test_case.TestCase):
  """Tests for config.update_config."""

  def tearDown(self, *args, **kwargs):
    """Performs post-test case tear-down."""
    super(UpdateConfigTest, self).tearDown(*args, **kwargs)

    # Even though the datastore resets between test cases and
    # the underlying entity doesn't persist, the cache does.
    config.Configuration.clear_cache()

  def install_mock(
      self,
      revision=None,
      template_config=None,
      manager_config=None,
  ):
    """Installs a mock for config.config.get_self_config.

    Args:
      template_config: What to return when templates.cfg is requested. Defaults
        to an empty config_pb2.InstanceTemplateConfig instance.
      manager_config: What to return when managers.cfg is requested. Defaults
        to an empty config_pb2.InstanceGroupManagerConfig instance.
    """
    def get(_, path, **kwargs):
      self.assertIn(path, ('templates.cfg', 'managers.cfg'))
      if path == 'templates.cfg':
        proto = template_config or config_pb2.InstanceTemplateConfig()
      elif path == 'managers.cfg':
        proto = manager_config or config_pb2.InstanceGroupManagerConfig()
      return revision or 'mock-revision', proto
    self.mock(config.config, 'get', get)

  def test_empty_configs(self):
    """Ensures empty configs are successfully stored."""
    self.install_mock()

    config.update_config()
    self.failIf(config.Configuration.cached().template_config)
    self.failIf(config.Configuration.cached().manager_config)
    self.assertEqual(config.Configuration.cached().revision, 'mock-revision')

  def test_repeated_base_names(self):
    """Ensures duplicate base names reject the entire config."""
    template_config = config_pb2.InstanceTemplateConfig(
        templates=[
            config_pb2.InstanceTemplateConfig.InstanceTemplate(
                base_name='base-name-1',
            ),
            config_pb2.InstanceTemplateConfig.InstanceTemplate(
                base_name='base-name-2',
            ),
            config_pb2.InstanceTemplateConfig.InstanceTemplate(
                base_name='base-name-3',
            ),
            config_pb2.InstanceTemplateConfig.InstanceTemplate(
                base_name='base-name-4',
            ),
            config_pb2.InstanceTemplateConfig.InstanceTemplate(
                base_name='base-name-2',
            ),
            config_pb2.InstanceTemplateConfig.InstanceTemplate(
                base_name='base-name-3',
            ),
        ],
    )
    self.install_mock(template_config=template_config)

    config.update_config()
    self.failIf(config.Configuration.cached().template_config)
    self.failIf(config.Configuration.cached().manager_config)
    self.failIf(config.Configuration.cached().revision)

  def test_repeated_zone_different_base_name(self):
    """Ensures repeated zones in different base names are valid."""
    manager_config = config_pb2.InstanceGroupManagerConfig(
        managers=[
            config_pb2.InstanceGroupManagerConfig.InstanceGroupManager(
                template_base_name='base-name-1',
                zone='us-central1-a',
            ),
            config_pb2.InstanceGroupManagerConfig.InstanceGroupManager(
                template_base_name='base-name-2',
                zone='us-central1-a',
            ),
            config_pb2.InstanceGroupManagerConfig.InstanceGroupManager(
                template_base_name='base-name-3',
                zone='us-central1-a',
            ),
        ],
    )
    self.install_mock(manager_config=manager_config)

    config.update_config()
    self.failIf(config.Configuration.cached().template_config)
    self.failUnless(config.Configuration.cached().manager_config)
    self.assertEqual(config.Configuration.cached().revision, 'mock-revision')

  def test_repeated_zone_same_base_name(self):
    """Ensures repeated zones in a base name reject the entire config."""
    manager_config = config_pb2.InstanceGroupManagerConfig(
        managers=[
            config_pb2.InstanceGroupManagerConfig.InstanceGroupManager(
                template_base_name='base-name-1',
                zone='us-central1-a',
            ),
            config_pb2.InstanceGroupManagerConfig.InstanceGroupManager(
                template_base_name='base-name-2',
                zone='us-central1-b',
            ),
            config_pb2.InstanceGroupManagerConfig.InstanceGroupManager(
                template_base_name='base-name-1',
                zone='us-central1-a',
            ),
        ],
    )
    self.install_mock(manager_config=manager_config)

    config.update_config()
    self.failIf(config.Configuration.cached().template_config)
    self.failIf(config.Configuration.cached().manager_config)
    self.failIf(config.Configuration.cached().revision)

  def test_update_configs(self):
    """Ensures config is updated when revision changes."""
    manager_config = config_pb2.InstanceGroupManagerConfig(
        managers=[config_pb2.InstanceGroupManagerConfig.InstanceGroupManager()],
    )
    self.install_mock(revision='revision-1', manager_config=manager_config)

    config.update_config()
    self.failIf(config.Configuration.cached().template_config)
    self.failUnless(config.Configuration.cached().manager_config)
    self.assertEqual(config.Configuration.cached().revision, 'revision-1')

    template_config = config_pb2.InstanceTemplateConfig(
        templates=[config_pb2.InstanceTemplateConfig.InstanceTemplate()],
    )
    self.install_mock(revision='revision-2', template_config=template_config)

    config.update_config()
    self.failUnless(config.Configuration.cached().template_config)
    self.failIf(config.Configuration.cached().manager_config)
    self.assertEqual(config.Configuration.cached().revision, 'revision-2')


  def test_update_configs_same_revision(self):
    """Ensures config is not updated when revision doesn't change."""
    manager_config = config_pb2.InstanceGroupManagerConfig(
        managers=[config_pb2.InstanceGroupManagerConfig.InstanceGroupManager()],
    )
    self.install_mock(manager_config=manager_config)

    config.update_config()
    self.failIf(config.Configuration.cached().template_config)
    self.failUnless(config.Configuration.cached().manager_config)
    self.assertEqual(config.Configuration.cached().revision, 'mock-revision')

    template_config = config_pb2.InstanceTemplateConfig(
        templates=[config_pb2.InstanceTemplateConfig.InstanceTemplate()],
    )
    self.install_mock(template_config=template_config)

    config.update_config()
    self.failIf(config.Configuration.cached().template_config)
    self.failUnless(config.Configuration.cached().manager_config)
    self.assertEqual(config.Configuration.cached().revision, 'mock-revision')


if __name__ == '__main__':
  unittest.main()
