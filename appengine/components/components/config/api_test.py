#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64  # noqa: F401
import logging
import sys
import unittest

from test_support import test_env

test_env.setup_test_env()

import mock  # noqa: E402

from google.appengine.ext import ndb  # noqa: E402

from components import auth  # noqa: E402, F401
from components import config  # noqa: E402
from components.config import api  # noqa: E402, F401
from components.config import remote  # noqa: E402, F401
from components.config import test_config_pb2  # noqa: E402
from test_support import test_case  # noqa: E402


class ApiTestCase(test_case.TestCase):
  def setUp(self):
    super(ApiTestCase, self).setUp()
    self.provider = mock.Mock()
    provider_future = ndb.Future()
    provider_future.set_result(self.provider)
    self.mock(config.api, "_get_config_provider_async", lambda: provider_future)
    self.provider.get_async.return_value = ndb.Future()
    self.provider.get_async.return_value.set_result(
      ("deadbeef", test_config_pb2.Config(param="value"))
    )

  def test_get(self):
    revision, cfg = config.get(
      "services/foo", "bar.cfg", test_config_pb2.Config
    )
    self.assertEqual(revision, "deadbeef")
    self.assertEqual(cfg.param, "value")

  def test_get_self_config(self):
    revision, cfg = config.get_self_config("bar.cfg", test_config_pb2.Config)
    self.assertEqual(revision, "deadbeef")
    self.assertEqual(cfg.param, "value")

  def test_get_project_config(self):
    revision, cfg = config.get_project_config(
      "foo", "bar.cfg", test_config_pb2.Config
    )
    self.assertEqual(revision, "deadbeef")
    self.assertEqual(cfg.param, "value")

  def test_get_ref_config(self):
    revision, cfg = config.get_ref_config(
      "foo", "refs/x", "bar.cfg", test_config_pb2.Config
    )
    self.assertEqual(revision, "deadbeef")
    self.assertEqual(cfg, test_config_pb2.Config(param="value"))

  def test_get_projects(self):
    self.provider.get_projects_async.return_value = ndb.Future()
    self.provider.get_projects_async.return_value.set_result(
      [
        {
          "id": "chromium",
          "repo_type": "GITILES",
          "repo_url": "https://chromium.googlesource.com/chromium/src",
          "name": "Chromium browser",
        },
        {
          "id": "infra",
          "repo_type": "GITILES",
          "repo_url": "https://chromium.googlesource.com/infra/infra",
        },
      ]
    )
    projects = config.get_projects()
    self.assertEqual(
      projects,
      [
        config.Project(
          id="chromium",
          repo_type="GITILES",
          repo_url="https://chromium.googlesource.com/chromium/src",
          name="Chromium browser",
        ),
        config.Project(
          id="infra",
          repo_type="GITILES",
          repo_url="https://chromium.googlesource.com/infra/infra",
          name="",
        ),
      ],
    )

  def test_get_project_configs(self):
    self.provider.get_project_configs_async.return_value = ndb.Future()
    self.provider.get_project_configs_async.return_value.set_result(
      {
        "projects/chromium": ("deadbeef", 'param: "value"'),
        "projects/v8": ("aaaabbbb", 'param: "value2"'),
        "projects/skia": ("badcoffee", "invalid config"),
      }
    )

    actual = config.get_project_configs("bar.cfg", test_config_pb2.Config)
    self.assertIsInstance(actual["skia"][2], config.ConfigFormatError)
    expected = {
      "chromium": ("deadbeef", test_config_pb2.Config(param="value"), None),
      "v8": ("aaaabbbb", test_config_pb2.Config(param="value2"), None),
      "skia": ("badcoffee", None, actual["skia"][2]),
    }
    self.assertEqual(expected, actual)


if __name__ == "__main__":
  if "-v" in sys.argv:
    unittest.TestCase.maxDiff = None
  else:
    logging.basicConfig(level=logging.CRITICAL)
  unittest.main()
