#!/usr/bin/python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for handlers_cron.py."""

import json
import unittest

import test_env
test_env.setup_test_env()

from protorpc.remote import protojson
import webtest

from components import auth_testing
from test_support import test_case

import handlers_cron
import models
import rpc_messages


class LeaseRequestProcessorTest(test_case.TestCase):
  """Tests for handlers_cron.LeaseRequestProcessor."""

  def setUp(self):
    super(LeaseRequestProcessorTest, self).setUp()
    app = handlers_cron.create_cron_app()
    self.app = webtest.TestApp(app)

  def test_one_request_one_matching_machine_entry(self):
    request = rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        duration=1,
        request_id='fake-id',
    )
    models.LeaseRequest(
        deduplication_checksum=models.LeaseRequest.compute_deduplication_checksum(
            request,
        ),
        key=models.LeaseRequest.generate_key(
            auth_testing.DEFAULT_MOCKED_IDENTITY.to_bytes(),
            request,
        ),
        owner=auth_testing.DEFAULT_MOCKED_IDENTITY,
        request=request,
        response=rpc_messages.LeaseResponse(),
        state=models.LeaseRequestStates.UNTRIAGED,
    ).put()
    models.CatalogMachineEntry.create_and_put(
        rpc_messages.Dimensions(
            backend=rpc_messages.Backend.DUMMY,
            hostname='fake-host',
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        state=models.CatalogMachineEntryStates.AVAILABLE,
    )

    self.app.get(
        '/internal/cron/process-lease-requests',
        headers={'X-AppEngine-Cron': 'true'},
    )


if __name__ == '__main__':
  unittest.main()
