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

handlers_backend.py calls generate_lease_requests (below) to generate a number
of lease requests equal to the target size minus the current number of active
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
to prevents duplicate lease requests (e.g. if generate_lease_requests runs twice
before update_leases gets to run). Machine Provider supports idempotent RPCs
so long as the client-generated request ID is the same.

Therefore, we ensure generate_lease_requests always generates lease requests
with the same request ID until update_lease is called. This is done by keeping a
count of the number of requests generated so far. If the request count is n,
generate_lease_requests will always generate requests with IDs n + 1, n + 2, ...
up to n + target size - current size, where current size is the number of
currently active lease requests.

update_lease takes the RPC result and updates the pending request entries.

TODO(smut): Consider request count overflow.
"""

import datetime
import logging

from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

from components import machine_provider
from components import utils
from server import bot_management


class MachineLease(ndb.Model):
  """A lease request for a machine from the Machine Provider.

  Standalone MachineLease entities should not exist in the datastore.
  """
  # Request ID used to generate this request.
  client_request_id = ndb.StringProperty(required=True)
  # Hostname of the machine currently allocated for this request.
  hostname = ndb.StringProperty()
  # Request hash returned by the server for the request for this machine.
  request_hash = ndb.StringProperty()
  # DateTime indicating lease expiration time.
  lease_expiration_ts = ndb.DateTimeProperty()


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
  # Number of bots pending deletion.
  num_pending_deletion = ndb.ComputedProperty(
      lambda self: len(self.pending_deletion))
  # List of hostnames whose leases have expired and should be deleted.
  pending_deletion = ndb.StringProperty(indexed=False, repeated=True)
  # Last request number used.
  request_count = ndb.IntegerProperty(default=0, required=True)
  # Request ID base string.
  request_id_base = ndb.StringProperty(indexed=False, required=True)
  # Target number of machines of this type to have leased at once.
  target_size = ndb.IntegerProperty(indexed=False, required=True)


def clean_up_bots():
  """Cleans up expired leases."""
  # Maximum number of in-flight ndb.Futures.
  MAX_IN_FLIGHT = 50

  bot_ids = []
  deleted = {}
  for machine_type in MachineType.query(MachineType.num_pending_deletion > 0):
    bot_ids.extend(machine_type.pending_deletion)
    deleted[machine_type.key] = machine_type.pending_deletion

  # Generate a few asynchronous requests at a time in order to
  # prevent having too many in-flight ndb.Futures at a time.
  futures = []
  while bot_ids:
    num_futures = len(futures)
    if num_futures < MAX_IN_FLIGHT:
      keys = [bot_management.get_info_key(bot_id)
              for bot_id in bot_ids[:MAX_IN_FLIGHT - num_futures]]
      bot_ids = bot_ids[MAX_IN_FLIGHT - num_futures:]
      futures.extend(ndb.delete_multi_async(keys))

    ndb.Future.wait_any(futures)
    futures = [future for future in futures if not future.done()]

  if futures:
    ndb.Future.wait_all(futures)

  # There should be relatively few MachineType entitites, so
  # just process them sequentially.
  # TODO(smut): Parallelize this.
  for machine_key, hostnames in deleted.iteritems():
    _clear_bots_pending_deletion(machine_key, hostnames)


@ndb.transactional
def _clear_bots_pending_deletion(machine_type_key, hostnames):
  """Clears the list of bots pending deletion.

  Args:
    machine_type_key: ndb.Key for a MachineType instance.
    hostnames: List of bots pending deletion.
  """
  machine_type = machine_type_key.get()
  if not machine_type:
    logging.warning('MachineType no longer exists: %s', machine_type_key.id())
    return

  num_pending_deletion = len(machine_type.pending_deletion)
  machine_type.pending_deletion = [
      host for host in machine_type.pending_deletion if host not in hostnames]
  if len(machine_type.pending_deletion) != num_pending_deletion:
    machine_type.put()


@ndb.transactional
def generate_lease_requests(machine_type_key, swarming_server):
  """Generates lease requests.

  The list includes new requests to lease machines up to the targeted
  size for the given machine type as well as requests to get the status
  of pending lease requests.

  Args:
    machine_type_key: ndb.Key for a MachineType instance.
    swarming_server: URL for the Swarming server to connect to.

  Returns:
    A list of lease requests.
  """
  machine_type = machine_type_key.get()
  if not machine_type:
    logging.warning('MachineType no longer exists: %s', machine_type_key.id())
    return []

  expired_requests = _clean_up_expired_leases(machine_type)
  lease_requests = _generate_lease_request_status_updates(
      machine_type, swarming_server)

  if not machine_type.enabled:
    logging.warning('MachineType is not enabled: %s\n', machine_type.key.id())
    return lease_requests
  if machine_type.current_size >= machine_type.target_size:
    logging.info(
        'MachineType %s is at capacity: %d/%d',
        machine_type.key.id(),
        machine_type.current_size,
        machine_type.target_size,
    )
    return lease_requests

  new_requests = _generate_lease_requests_for_new_machines(
      machine_type, swarming_server)

  if new_requests or expired_requests:
    machine_type.put()
    lease_requests.extend(new_requests)

  return lease_requests


def _clean_up_expired_leases(machine_type):
  """Cleans up expired leases.

  Prunes expired leases from machine_type.leases,
  but does not write the result to the datastore.

  Args:
    machine_type: MachineType instance.

  Returns:
    A list of leases that were removed.
  """
  active = []
  expired = []

  for request in machine_type.leases:
    if request.hostname and request.lease_expiration_ts <= utils.utcnow():
      logging.warning(
          'Request ID %s expired:\nHostname: %s\nExpiration: %s',
          request.client_request_id,
          request.hostname,
          request.lease_expiration_ts,
      )
      expired.append(request.hostname)
    else:
      active.append(request)

  machine_type.leases = active
  machine_type.pending_deletion.extend(expired)
  return expired


def _generate_lease_request_status_updates(machine_type, swarming_server):
  """Generates status update requests for pending lease requests.

  Args:
    machine_type: MachineType instance.
    swarming_server: URL for the Swarming server to connect to.

  Returns:
    A list of lease requests.
  """
  lease_requests = []
  for request in machine_type.leases:
    if not request.hostname:
      # We don't know the hostname yet, meaning this request is still pending.
      lease_requests.append(machine_provider.LeaseRequest(
          dimensions=machine_type.mp_dimensions,
          duration=machine_type.lease_duration_secs,
          on_lease=machine_provider.Instruction(
              swarming_server=swarming_server),
          request_id=request.client_request_id,
      ))
  return lease_requests


def _generate_lease_requests_for_new_machines(machine_type, swarming_server):
  """Generates requests to lease machines up to the target.

  Extends machine_type.leases by the number of new lease requests generated,
  but does not write the result to the datastore.

  Args:
    machine_type: MachineType instance.
    swarming_server: URL for the Swarming server to connect to.

  Returns:
    A list of lease requests.
  """
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
    machine_type.leases.append(MachineLease(client_request_id=request_id))
  machine_type.request_count = request_number
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

  lease_request_map = {
      request.client_request_id: request for request in machine_type.leases
  }
  for response in responses.get('responses', []):
    request_id = response['client_request_id']
    request = lease_request_map.get(request_id)
    if not request:
      logging.error('Unknown request ID: %s', request_id)
      continue

    if response.get('error'):
      error = machine_provider.LeaseRequestError.lookup_by_name(
          response['error'])
      if error in (
          machine_provider.LeaseRequestError.DEADLINE_EXCEEDED,
          machine_provider.LeaseRequestError.TRANSIENT_ERROR,
      ):
        # Retryable errors. Just try again later.
        logging.warning(
            'Request not processed, trying again later: %s', request_id)
      else:
        # TODO(smut): Handle specific errors.
        logging.warning(
            'Error %s for request ID %s',
            response['error'],
            request_id,
        )
        lease_request_map.pop(request_id)
    else:
      request.request_hash = response['request_hash']
      state = machine_provider.LeaseRequestState.lookup_by_name(
          response['state'])
      if state == machine_provider.LeaseRequestState.DENIED:
        logging.warning('Request ID denied: %s', request_id)
        lease_request_map.pop(request_id)
      elif state == machine_provider.LeaseRequestState.FULFILLED:
        if response.get('hostname'):
          logging.info(
              'Request ID %s fulfilled:\nHostname: %s\nExpiration: %s',
              request_id,
              response['hostname'],
              response['lease_expiration_ts'],
          )
          request.hostname = response['hostname']
          request.lease_expiration_ts = datetime.datetime.utcfromtimestamp(
              int(response['lease_expiration_ts']))
        else:
          # Lease expired. This shouldn't happen, because it means we had a
          # pending request which was fulfilled and expired before we were
          # able to check its status.
          logging.warning('Request ID fulfilled and expired: %s', request_id)
          lease_request_map.pop(request_id)
      else:
        # Lease request isn't processed yet. Just try again later.
        logging.info(
            'Request ID %s in state: %s', request_id, response['state'])

  machine_type.leases = sorted(
      lease_request_map.values(), key=lambda lease: lease.client_request_id)
  machine_type.put()
