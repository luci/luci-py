#!/usr/bin/env vpython
# Copyright 2025 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import sys
import unittest

import test_env

test_env.setup_test_env()

from server import string_pairs_serializer
from server import task_request


class TestStringPairsSerializer(unittest.TestCase):
  def test_write_task_properties(self):
    props = task_request.TaskProperties(
      idempotent=True,
      dimensions_data={
        "d1": ["v1", "v2"],
        "d2": ["a"],
        "pool": ["pool"],
        "id": ["botID"],
      },
      execution_timeout_secs=123,
      grace_period_secs=456,
      io_timeout_secs=789,
      command=["run", "a"],
      relative_cwd="./rel/cwd",
      env={"k1": "v1", "k2": "a"},
      env_prefixes={"p1": ["v1", "v2"], "p2": ["a"]},
      caches=[
        task_request.CacheEntry(name="n1", path="p1"),
        task_request.CacheEntry(name="n2", path="p2"),
      ],
      cas_input_root=task_request.CASReference(
        cas_instance="projects/chromium-swarm/instances/default_instance",
        digest=task_request.Digest(hash="cas-hash", size_bytes=1234),
      ),
      cipd_input=task_request.CipdInput(
        server="https://chrome-infra-packages.example.com",
        client_package=task_request.CipdPackage(
          package_name="client-package", version="client-version"
        ),
        packages=[
          task_request.CipdPackage(
            package_name="pkg1", version="ver1", path="path1"
          ),
          task_request.CipdPackage(
            package_name="pkg2", version="ver2", path="path2"
          ),
        ],
      ),
      outputs=["o1", "o2"],
      has_secret_bytes=True,
      containment=task_request.Containment(
        lower_priority=True,
        containment_type=task_request.ContainmentType.JOB_OBJECT,
        limit_processes=456,
        limit_total_committed_memory=789,
      ),
    )
    serializer = string_pairs_serializer.StringPairsSerializer()
    serializer.write_task_properties(props)

    expected = [
      string_pairs_serializer.StringPair(key="idempotent", value="true"),
      string_pairs_serializer.StringPair(key="relativeCwd", value="./rel/cwd"),
      string_pairs_serializer.StringPair(
        key="execution_timeout_secs", value="123"
      ),
      string_pairs_serializer.StringPair(key="grace_period_secs", value="456"),
      string_pairs_serializer.StringPair(key="io_timeout_secs", value="789"),
      string_pairs_serializer.StringPair(key="command.0", value="run"),
      string_pairs_serializer.StringPair(key="command.1", value="a"),
      string_pairs_serializer.StringPair(key="outputs.0", value="o1"),
      string_pairs_serializer.StringPair(key="outputs.1", value="o2"),
      string_pairs_serializer.StringPair(key="env.0.key", value="k1"),
      string_pairs_serializer.StringPair(key="env.0.value", value="v1"),
      string_pairs_serializer.StringPair(key="env.1.key", value="k2"),
      string_pairs_serializer.StringPair(key="env.1.value", value="a"),
      string_pairs_serializer.StringPair(key="env_prefixes.0.key", value="p1"),
      string_pairs_serializer.StringPair(
        key="env_prefixes.0.value.0", value="v1"
      ),
      string_pairs_serializer.StringPair(
        key="env_prefixes.0.value.1", value="v2"
      ),
      string_pairs_serializer.StringPair(key="env_prefixes.1.key", value="p2"),
      string_pairs_serializer.StringPair(
        key="env_prefixes.1.value.0", value="a"
      ),
      string_pairs_serializer.StringPair(key="dimensions.0.key", value="d1"),
      string_pairs_serializer.StringPair(
        key="dimensions.0.value.0", value="v1"
      ),
      string_pairs_serializer.StringPair(
        key="dimensions.0.value.1", value="v2"
      ),
      string_pairs_serializer.StringPair(key="dimensions.1.key", value="d2"),
      string_pairs_serializer.StringPair(key="dimensions.1.value.0", value="a"),
      string_pairs_serializer.StringPair(key="dimensions.2.key", value="id"),
      string_pairs_serializer.StringPair(
        key="dimensions.2.value.0", value="botID"
      ),
      string_pairs_serializer.StringPair(key="dimensions.3.key", value="pool"),
      string_pairs_serializer.StringPair(
        key="dimensions.3.value.0", value="pool"
      ),
      string_pairs_serializer.StringPair(key="caches.0.name", value="n1"),
      string_pairs_serializer.StringPair(key="caches.0.path", value="p1"),
      string_pairs_serializer.StringPair(key="caches.1.name", value="n2"),
      string_pairs_serializer.StringPair(key="caches.1.path", value="p2"),
      string_pairs_serializer.StringPair(
        key="cas_input_root.cas_instance",
        value="projects/chromium-swarm/instances/default_instance",
      ),
      string_pairs_serializer.StringPair(
        key="cas_input_root.digest.hash", value="cas-hash"
      ),
      string_pairs_serializer.StringPair(
        key="cas_input_root.digest.size_bytes", value="1234"
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.server",
        value="https://chrome-infra-packages.example.com",
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.client_package.package_name", value="client-package"
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.client_package.version", value="client-version"
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.client_package.path", value=""
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.packages.0.package_name", value="pkg1"
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.packages.0.version", value="ver1"
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.packages.0.path", value="path1"
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.packages.1.package_name", value="pkg2"
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.packages.1.version", value="ver2"
      ),
      string_pairs_serializer.StringPair(
        key="cipd_input.packages.1.path", value="path2"
      ),
      string_pairs_serializer.StringPair(
        key="containment.containment_type", value="3"
      ),
      string_pairs_serializer.StringPair(
        key="containment.lower_priority", value="true"
      ),
      string_pairs_serializer.StringPair(
        key="containment.limit_processes", value="456"
      ),
      string_pairs_serializer.StringPair(
        key="containment.limit_total_committed_memory", value="789"
      ),
    ]
    for i, pair in enumerate(serializer.pairs):
      self.assertEqual(pair.key, expected[i].key)
      self.assertEqual(pair.value, expected[i].value)

  def test_write_task_properties_empty(self):
    serializer = string_pairs_serializer.StringPairsSerializer()
    serializer.write_task_properties(None)
    self.assertEqual(serializer.pairs, [])

  def test_write_task_properties_partial(self):
    props = task_request.TaskProperties(
      dimensions_data={
        "d1": ["v1", "v2"],
        "pool": ["pool"],
      },
      env_prefixes={
        "p1": ["v2", "v1"],
      },
    )
    serializer = string_pairs_serializer.StringPairsSerializer()
    serializer.write_task_properties(props)

    expected = [
      string_pairs_serializer.StringPair(key="idempotent", value="false"),
      string_pairs_serializer.StringPair(key="relativeCwd", value=""),
      string_pairs_serializer.StringPair(
        key="execution_timeout_secs", value="0"
      ),
      # has default value as DEFAULT_GRACE_PERIOD_SECS
      string_pairs_serializer.StringPair(key="grace_period_secs", value="30"),
      string_pairs_serializer.StringPair(key="io_timeout_secs", value="0"),
      string_pairs_serializer.StringPair(key="env_prefixes.0.key", value="p1"),
      string_pairs_serializer.StringPair(
        key="env_prefixes.0.value.0", value="v2"
      ),
      string_pairs_serializer.StringPair(
        key="env_prefixes.0.value.1", value="v1"
      ),
      string_pairs_serializer.StringPair(key="dimensions.0.key", value="d1"),
      string_pairs_serializer.StringPair(
        key="dimensions.0.value.0", value="v1"
      ),
      string_pairs_serializer.StringPair(
        key="dimensions.0.value.1", value="v2"
      ),
      string_pairs_serializer.StringPair(key="dimensions.1.key", value="pool"),
      string_pairs_serializer.StringPair(
        key="dimensions.1.value.0", value="pool"
      ),
    ]
    for i, pair in enumerate(serializer.pairs):
      self.assertEqual(pair.key, expected[i].key)
      self.assertEqual(pair.value, expected[i].value)


if __name__ == "__main__":
  if "-v" in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
    level=logging.DEBUG if "-v" in sys.argv else logging.CRITICAL
  )
  unittest.main()
