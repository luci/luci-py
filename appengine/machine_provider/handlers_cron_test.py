#!/usr/bin/python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for handlers_cron.py."""

import datetime
import json
import unittest

import test_env
test_env.setup_test_env()

from protorpc.remote import protojson
import webtest

from components import auth_testing
from components import utils
from components.machine_provider import rpc_messages
from test_support import test_case

import handlers_cron
import models


class LeaseRequestProcessorTest(test_case.TestCase):
  """Tests for handlers_cron.LeaseRequestProcessor."""

  def setUp(self):
    super(LeaseRequestProcessorTest, self).setUp()
    app = handlers_cron.create_cron_app()
    self.app = webtest.TestApp(app)
    self.mock(utils, 'enqueue_task', lambda *args, **kwargs: True)

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
        rpc_messages.Policies(),
        models.CatalogMachineEntryStates.AVAILABLE,
    )

    self.app.get(
        '/internal/cron/process-lease-requests',
        headers={'X-AppEngine-Cron': 'true'},
    )


class MachineReclamationProcessorTest(test_case.TestCase):
  """Tests for handlers_cron.MachineReclamationProcessor."""

  def setUp(self):
    super(MachineReclamationProcessorTest, self).setUp()
    app = handlers_cron.create_cron_app()
    self.app = webtest.TestApp(app)
    self.mock(utils, 'enqueue_task', lambda *args, **kwargs: True)

  def test_reclaim_immediately(self):
    request = rpc_messages.LeaseRequest(
        dimensions=rpc_messages.Dimensions(
            os_family=rpc_messages.OSFamily.LINUX,
        ),
        duration=0,
        request_id='fake-id',
    )
    lease = models.LeaseRequest(
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
    )
    dimensions = rpc_messages.Dimensions(
        backend=rpc_messages.Backend.DUMMY,
        hostname='fake-host',
        os_family=rpc_messages.OSFamily.LINUX,
    )
    machine = models.CatalogMachineEntry(
        dimensions=dimensions,
        key=models.CatalogMachineEntry.generate_key(dimensions),
        lease_id=lease.key.id(),
        lease_expiration_ts=datetime.datetime.fromtimestamp(0),
        state=models.CatalogMachineEntryStates.AVAILABLE,
    ).put()
    lease.machine_id = machine.id()
    lease.put()

    self.app.get(
        '/internal/cron/process-machine-reclamations',
        headers={'X-AppEngine-Cron': 'true'},
    )


if __name__ == '__main__':
  unittest.main()
