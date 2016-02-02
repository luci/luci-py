# Copyright 2016 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Lease management for machines leased from the Machine Provider.

Keeps a list of machine types which should be leased from the Machine Provider
and the list of machines of each type currently leased.

Swarming integration with Machine Provider
==========================================

handlers_backend.py contains a cron job which looks at each machine type and
ensures the number of active lease requests is equal to the target size
specified by the machine type.

A lease request is considered active as soon as it is initiated. It remains
active while the lease request is pending and even after it is fulfilled. A
lease request is considered no longer active once the request is denied or
the machine used to fulfill the request is reclaimed.

This scheme ensures that we never have more machines of a given type than we
want

Each machine type is stored as a MachineType entity which describes the machine
in terms of Machine Provider Dimensions (not to be confused with Swarming's own
bot dimensions) as well as a target size which describes the number of machines
of this type we want connected at once.

handlers_backend.py calls get_lease_requests (below) to generate a number of
lease requests equal to the target size minus the current number of active
leases, then performs the RPC to send these requests to the Machine Provider,
and finally it calls update_leases (below) with the results of the lease
request to associate the new active leases with the machine type.

In principle, the entire operation on a single machine type needs to happen
transactionally so we don't make duplicate requests and end up with more
machines than the target size wants. However, in practice we need the RPC
to be outside the transaction because it may be slow and it may need to be
retried, which could cause the entire transaction to time out without logging
the result of the call. Therefore the procedure is broken up into two
transactions with an RPC in between.

With the transaction broken into two, the choice of request ID is the only way
to prevents duplicate lease requests (e.g. if get_lease_requests runs twice
before update_leases gets to run). Machine Provider supports idempotent RPCs
so long as the client-generated request ID is the same.

Therefore, we ensure get_lease_requests always generates lease requests with
the same request ID until update_lease is called. This is done by keeping a
count of the number of requests generated so far. If the request count is n,
get_lease_requests will always generate requests with IDs n + 1, n + 2, ...
up to n + target size - current size, where current size is the number of
currently active lease requests.

update_lease takes the RPC result and increments the request count and the
current size by the number of requests.

TODO(smut): Consider request count overflow.

TODO(smut): Check on the status of active lease requests, purge them when
machines get reclaimed.
"""

import logging

from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

from components import machine_provider


class MachineLease(ndb.Model):
  """A lease request for a machine from the Machine Provider.

  Standalone MachineLease entities should not exist in the datastore.
  """
  # Request hash returned by the server for the request for this machine.
  request_hash = ndb.StringProperty(required=True)


class MachineType(ndb.Model):
  """A type of machine which should be leased from the Machine Provider.

  Key:
    id: A human-readable name for this machine type.
    kind: MachineType. Is a root entity.
  """
  # Current number of active leases.
  current_size = ndb.ComputedProperty(lambda self: len(self.leases))
  # Description of this machine type for humans.
  description = ndb.StringProperty(indexed=False)
  # Whether or not to attempt to lease machines of this type.
  enabled = ndb.BooleanProperty(default=True)
  # Duration to lease each machine for.
  lease_duration_secs = ndb.IntegerProperty(indexed=False)
  # List of active lease requests.
  leases = ndb.LocalStructuredProperty(MachineLease, repeated=True)
  # machine_provider.Dimensions describing the machine.
  mp_dimensions = msgprop.MessageProperty(
      machine_provider.Dimensions, indexed=False)
  # Last request number used.
  request_count = ndb.IntegerProperty(default=0, required=True)
  # Request ID base string.
  request_id_base = ndb.StringProperty(indexed=False, required=True)
  # Target number of machines of this type to have leased at once.
  target_size = ndb.IntegerProperty(indexed=False, required=True)


@ndb.transactional
def get_lease_requests(machine_type_key, swarming_server):
  """Returns a list of requests to lease machines up to the target.

  Args:
    machine_type_key: ndb.Key for a MachineType instance.
    swarming_server: URL for the Swarming server to connect to.

  Returns:
    A list of lease requests.
  """
  machine_type = machine_type_key.get()
  if not machine_type:
    logging.warning('MachineType no longer exists: %s', machine_type_key)
    return []
  if not machine_type.enabled:
    logging.warning('MachineType is not enabled: %s', machine_type.key.id())
    return []
  if machine_type.current_size >= machine_type.target_size:
    logging.info(
        'MachineType %s is at capacity: %d/%d',
        machine_type.key.id(),
        machine_type.current_size,
        machine_type.target_size,
    )
    return []

  lease_requests = []
  request_number = machine_type.request_count
  for _ in xrange(machine_type.target_size - machine_type.current_size):
    request_number += 1
    request_id = '%s-%d' % (machine_type.request_id_base, request_number)
    lease_requests.append(machine_provider.LeaseRequest(
        dimensions=machine_type.mp_dimensions,
        duration=machine_type.lease_duration_secs,
        on_lease=machine_provider.Instruction(swarming_server=swarming_server),
        request_id=request_id,
    ))

  return lease_requests


@ndb.transactional
def update_leases(machine_type_key, responses):
  """Updates the states of leases of the given machine types.

  Args:
    machine_type_key: ndb.Key for a MachineType instance.
    responses: machine_provider.BatchedLeaseResponse instance.
  """
  machine_type = machine_type_key.get()
  if not machine_type:
    logging.warning('MachineType no longer exists: %s', machine_type_key)
    return

  lease_requests = []

  # The request IDs should form a contiguous block starting at
  # machine_type.request_count + 1 (see get_lease_requests, above).
  request_ids = []
  for response in responses.get('responses', []):
    lease_requests.append(MachineLease(request_hash=response['request_hash']))
    request_ids.append(int(response['client_request_id'].rsplit('-', 1)[-1]))
  if request_ids:
    request_ids = sorted(request_ids)
    if machine_type.request_count + 1 == request_ids[0]:
      logging.info(
          'Advancing request_id for MachineType %s from %d to %d',
          machine_type.key.id(),
          machine_type.request_count,
          request_ids[-1],
      )
      machine_type.leases.extend(lease_requests)
      machine_type.request_count = request_ids[-1]
      machine_type.put()
    elif machine_type.request_count == request_ids[-1]:
      # We already processed these responses. We probably had a cron job
      # overrun which scheduled duplicate lease requests. Since the Machine
      # Provider makes lease requests idempotent, just ignore the results.
      logging.warning(
          'Not advancing request_id for MachineType %s from %d to %d',
          machine_type.key.id(),
          machine_type.request_count,
          request_ids[-1],
      )
    else:
      logging.error(
          'Unexpected request_id for MachineType %s: expected %d or %d, got %d',
          machine_type.key.id(),
          request_ids[0],
          request_ids[-1],
          machine_type.request_count,
      )
