#!/usr/bin/env vpython
# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import unittest

import test_env
test_env.setup_test_env()

from test_support import test_case
import replication


class ReplicationTest(test_case.TestCase):
  def test_sharding(self):
    shard_ids = replication.shard_authdb(123, '0123456789', max_size=3)
    self.assertEqual(len(shard_ids), 4)

    blob = replication.unshard_authdb(shard_ids)
    self.assertEqual(blob, '0123456789')


if __name__ == '__main__':
  unittest.main()
