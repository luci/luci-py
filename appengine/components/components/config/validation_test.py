#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import re
import sys
import unittest
import yaml

from test_support import test_env
test_env.setup_test_env()

import mock

from components.config import test_config_pb2
from components.config import validation
from test_support import test_case


class ValidationTestCase(test_case.TestCase):
  def setUp(self):
    super(ValidationTestCase, self).setUp()
    self.rule_set = validation.RuleSet()

  def test_rule(self):
    validating_func = mock.Mock()
    rule = validation.rule(
        'projects/foo', 'bar.cfg', test_config_pb2.Config,
        rule_set=self.rule_set)
    rule(validating_func)

    self.rule_set.validate('projects/foo', 'bar.cfg', 'param: "valid config"')
    with self.assertRaises(ValueError):
      self.rule_set.validate('projects/foo', 'bar.cfg', 'wrong123')
    self.assertEqual(validating_func.call_count, 1)

    validating_func.side_effect = lambda cfg, ctx: ctx.error('bad')
    with self.assertRaises(ValueError):
      self.rule_set.validate('projects/foo', 'bar.cfg', 'param: "valid config"')

    # Different config_set/path
    self.rule_set.validate('projects/foo', 'baz.cfg', 'wrong')
    self.rule_set.validate('projects/baz', 'bar.cfg', 'wrong')

  def test_regex_pattern_and_no_dest_type(self):
    rule = validation.rule(
        config_set=re.compile('^projects/f[^/]+$').match,
        path=lambda p: p.endswith('.yaml'),
        rule_set=self.rule_set)
    def validate_yaml(cfg, ctx):
      try:
        yaml.safe_load(cfg)
      except Exception as ex:
        ctx.error(ex)
    rule(validate_yaml)

    self.rule_set.validate('projects/foo', 'bar.cfg', '}{')
    self.rule_set.validate('projects/bar', 'bar.yaml', '}{')
    self.rule_set.validate('projects/foo', 'bar.yaml', '{}')

    with self.assertRaises(ValueError):
      self.rule_set.validate('projects/foo', 'bar.yaml', '}{')

  def test_project_config_rule(self):
    validation.project_config_rule(
        'bar.cfg', test_config_pb2.Config,
        rule_set=self.rule_set)

    self.assertTrue(self.rule_set.is_defined_for('projects/foo', 'bar.cfg'))
    self.assertTrue(self.rule_set.is_defined_for('projects/baz', 'bar.cfg'))

    self.assertFalse(self.rule_set.is_defined_for('projects/x/huh', 'bar.cfg'))
    self.assertFalse(self.rule_set.is_defined_for('services/baz', 'bar.cfg'))
    self.assertFalse(self.rule_set.is_defined_for('projects/baz', 'notbar.cfg'))

  def test_ref_config_rule(self):
    validation.ref_config_rule(
        'bar.cfg', test_config_pb2.Config,
        rule_set=self.rule_set)

    self.assertTrue(
        self.rule_set.is_defined_for(
            'projects/baz/refs/heads/master', 'bar.cfg'))

    self.assertFalse(
        self.rule_set.is_defined_for(
            'projects/baz/refs/heads/master', 'nonbar.cfg'))
    self.assertFalse(self.rule_set.is_defined_for('services/foo', 'bar.cfg'))
    self.assertFalse(self.rule_set.is_defined_for('projects/baz', 'bar.cfg'))

  def test_remove_rule(self):
    rule = validation.rule(
        'projects/foo', 'bar.cfg', test_config_pb2.Config,
        rule_set=self.rule_set)

    with self.assertRaises(ValueError):
      self.rule_set.validate('projects/foo', 'bar.cfg', 'invalid config')

    rule.remove()
    self.rule_set.validate('projects/foo', 'bar.cfg', 'invalid config')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
