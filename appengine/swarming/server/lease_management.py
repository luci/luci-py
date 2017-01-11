# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Lease management for machines leased from the Machine Provider.

Keeps a list of machine types which should be leased from the Machine Provider
and the list of machines of each type currently leased.

Swarming integration with Machine Provider
==========================================

handlers_backend.py contains a cron job which looks at each MachineType and
ensures there are at least as many MachineLeases in the datastore which refer
to that MachineType as the target_size in MachineType specifies by numbering
them 0 through target_size - 1 If there are MachineType entities numbered
target_size or greater which refer to that MachineType, those MachineLeases
are marked as drained.

Each MachineLease manages itself. A cron job in handlers_backend.py will trigger
self-management jobs for each entity. If there is no associated lease and the
MachineLease is not drained, issue a request to the Machine Provider for a
matching machine. If there is an associated request, check the status of that
request. If it is fulfilled, ensure the existence of a BotInfo entity (see
server/bot_management.py) corresponding to the machine provided for the lease.
Include the lease ID and lease_expiration_ts as fields in the BotInfo. If it
is expired, clear the associated lease. If there is no associated lease and
the MachineLease is drained, delete the MachineLease entity.

TODO(smut): If there is an associated request and the MachineLease is drained,
release the lease immediately (as long as the bot is not mid-task).
"""

import base64
import datetime
import json
import logging

from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop
from protorpc.remote import protojson

from components import machine_provider
from components import pubsub
from components import utils
from server import bot_groups_config
from server import bot_management
from server import task_request
from server import task_result
from server import task_pack
from server import task_scheduler


# Name of the topic the Machine Provider is authorized to publish
# lease information to.
PUBSUB_TOPIC = 'machine-provider'

# Name of the pull subscription to the Machine Provider topic.
PUBSUB_SUBSCRIPTION = 'machine-provider'


class MachineLease(ndb.Model):
  """A lease request for a machine from the Machine Provider.

  Key:
    id: A string in the form <machine type id>-<number>.
    kind: MachineLease. Is a root entity.
  """
  # Bot ID for the BotInfo created for this machine.
  bot_id = ndb.StringProperty(indexed=False)
  # Request ID used to generate this request.
  client_request_id = ndb.StringProperty(indexed=True)
  # Whether or not this MachineLease should issue lease requests.
  drained = ndb.BooleanProperty(indexed=True)
  # Number of seconds ahead of lease_expiration_ts to release leases.
  early_release_secs = ndb.IntegerProperty(indexed=False)
  # Hostname of the machine currently allocated for this request.
  hostname = ndb.StringProperty()
  # Duration to lease for.
  lease_duration_secs = ndb.IntegerProperty(indexed=False)
  # DateTime indicating lease expiration time.
  lease_expiration_ts = ndb.DateTimeProperty()
  # Lease ID assigned by Machine Provider.
  lease_id = ndb.StringProperty(indexed=False)
  # ndb.Key for the MachineType this MachineLease is created for.
  machine_type = ndb.KeyProperty()
  # machine_provider.Dimensions describing the machine.
  mp_dimensions = msgprop.MessageProperty(
      machine_provider.Dimensions, indexed=False)
  # Last request number used.
  request_count = ndb.IntegerProperty(default=0, required=True)
  # Task ID for the termination task scheduled for this machine.
  termination_task = ndb.StringProperty(indexed=False)


class MachineType(ndb.Model):
  """A type of machine which should be leased from the Machine Provider.

  Key:
    id: A human-readable name for this machine type.
    kind: MachineType. Is a root entity.
  """
  # Description of this machine type for humans.
  description = ndb.StringProperty(indexed=False)
  # Number of seconds ahead of lease_expiration_ts to release leases.
  early_release_secs = ndb.IntegerProperty(indexed=False)
  # Whether or not to attempt to lease machines of this type.
  enabled = ndb.BooleanProperty(default=True)
  # Duration to lease each machine for.
  lease_duration_secs = ndb.IntegerProperty(indexed=False)
  # machine_provider.Dimensions describing the machine.
  mp_dimensions = msgprop.MessageProperty(
      machine_provider.Dimensions, indexed=False)
  # Target number of machines of this type to have leased at once.
  target_size = ndb.IntegerProperty(indexed=False, required=True)


@ndb.transactional_tasklet
def ensure_entity_exists(machine_type, n):
  """Ensures the nth MachineLease for the given MachineType exists.

  Args:
    machine_type: MachineType entity.
    n: The MachineLease index.
  """
  key = ndb.Key(MachineLease, '%s-%s' % (machine_type.key.id(), n))
  machine_lease = yield key.get_async()

  # If there is no MachineLease in the datastore at all, create one.
  if not machine_lease:
    yield MachineLease(
        key=key,
        lease_duration_secs=machine_type.lease_duration_secs,
        early_release_secs=machine_type.early_release_secs,
        machine_type=machine_type.key,
        mp_dimensions=machine_type.mp_dimensions,
    ).put_async()
    return

  # If there is a MachineLease, we may need to update it if the MachineType's
  # lease properties have changed. It's only safe to update it if the current
  # lease is fulfilled (indicated by the presence of lease_expiration_ts) so
  # the changes only go into effect for the next lease request.
  if machine_lease.lease_expiration_ts:
    put = False

    if machine_lease.early_release_secs != machine_type.early_release_secs:
      machine_lease.early_release_secs = machine_type.early_release_secs
      put = True

    if machine_lease.lease_duration_secs != machine_type.lease_duration_secs:
      machine_lease.lease_duration_secs = machine_type.lease_duration_secs
      put = True

    if machine_lease.mp_dimensions != machine_type.mp_dimensions:
      machine_lease.mp_dimensions = machine_type.mp_dimensions
      put = True

    if put:
      yield machine_lease.put_async()


def machine_type_pb2_to_entity(pb2):
  """Creates a MachineType entity from the given bots_pb2.MachineType.

  Args:
    pb2: A proto.bots_pb2.MachineType proto.

  Returns:
    A MachineType entity.
  """
  return MachineType(
      id=pb2.name,
      description=pb2.description,
      early_release_secs=pb2.early_release_secs,
      enabled=True,
      lease_duration_secs=pb2.lease_duration_secs,
      mp_dimensions=protojson.decode_message(
          machine_provider.Dimensions,
          json.dumps(dict(pair.split(':', 1) for pair in pb2.mp_dimensions)),
      ),
      target_size=pb2.target_size,
  )


def ensure_entities_exist(max_concurrent=50):
  """Ensures MachineType entities are correct, and MachineLease entities exist.

  Updates MachineType entities based on the config and creates corresponding
  MachineLease entities.

  Args:
    max_concurrent: Maximum number of concurrent asynchronous requests.
  """
  # Generate a few asynchronous requests at a time in order to prevent having
  # too many in flight at a time.
  futures = []
  machine_types = bot_groups_config.fetch_machine_types().copy()

  for machine_type in MachineType.query(MachineType.enabled == True):
    # Check the MachineType in the datastore against its config.
    # If it no longer exists, just disable it here. If it exists but
    # doesn't match, update it.
    config = machine_types.pop(machine_type.key.id(), None)
    if not config:
      machine_type.enabled = False
      futures.append(machine_type.put_async())
      continue

    config = machine_type_pb2_to_entity(config)
    if machine_type != config:
      logging.info('Updating MachineType: %s', config)
      machine_type = config
      futures.append(machine_type.put_async())

    cursor = 0
    while cursor < machine_type.target_size:
      while len(futures) < max_concurrent and cursor < machine_type.target_size:
        futures.append(ensure_entity_exists(machine_type, cursor))
        cursor += 1
      ndb.Future.wait_any(futures)
      # We don't bother checking success or failure. If a transient error
      # like TransactionFailed or DeadlineExceeded is raised and an entity
      # is not created, we will just create it the next time this is called,
      # converging to the desired state eventually.
      futures = [future for future in futures if not future.done()]

  if machine_types:
    machine_types = machine_types.values()

  # Create MachineTypes that never existed before.
  # The next iteration of this cron job will create their MachineLeases.
  while machine_types:
    num_futures = len(futures)
    if num_futures < max_concurrent:
      futures.extend([
          machine_type_pb2_to_entity(machine_type).put_async()
          for machine_type in machine_types[:max_concurrent - num_futures]
      ])
      machine_types = machine_types[max_concurrent - num_futures:]
    ndb.Future.wait_any(futures)
    futures = [future for future in futures if not future.done()]

  if futures:
    ndb.Future.wait_all(futures)


def drain_excess(max_concurrent=50):
  """Marks MachineLeases beyond what is needed by their MachineType as drained.

  Args:
    max_concurrent: Maximum number of concurrent asynchronous requests.
  """
  futures = []

  for machine_type in MachineType.query():
    for machine_lease in MachineLease.query(
        MachineLease.machine_type == machine_type.key,
    ):
      try:
        index = int(machine_lease.key.id().rsplit('-', 1)[-1])
      except ValueError:
        logging.error(
            'MachineLease index could not be deciphered\n Key: %s',
            machine_lease.key,
        )
        continue
      # Drain MachineLeases where the MachineType is not enabled or the index
      # exceeds the target_size given by the MachineType. Since MachineLeases
      # are created in contiguous blocks, only indices 0 through target_size - 1
      # should exist.
      if not machine_type.enabled or index >= machine_type.target_size:
        if len(futures) == max_concurrent:
          ndb.Future.wait_any(futures)
          futures = [future for future in futures if not future.done()]
        machine_lease.drained = True
        futures.append(machine_lease.put_async())

  if futures:
    ndb.Future.wait_all(futures)


def schedule_lease_management():
  """Schedules task queues to process each MachineLease."""
  now = utils.utcnow()
  for machine_lease in MachineLease.query():
    # If there's no known bot_id, we're waiting on a lease so schedule the
    # management job to check on it. If there is a bot_id, then don't bother
    # scheduling the management job until it's time to release the machine.
    if (not machine_lease.bot_id
        or machine_lease.lease_expiration_ts <= now + datetime.timedelta(
            seconds=machine_lease.early_release_secs)):
      if not utils.enqueue_task(
          '/internal/taskqueue/machine-provider-manage',
          'machine-provider-manage',
          params={
              'key': machine_lease.key.urlsafe(),
          },
      ):
        logging.warning(
            'Failed to enqueue task for MachineLease: %s', machine_lease.key)


@ndb.transactional
def clear_lease_request(key, request_id):
  """Clears information about given lease request.

  Args:
    key: ndb.Key for a MachineLease entity.
    request_id: ID of the request to clear.
  """
  machine_lease = key.get()
  if not machine_lease:
    logging.error('MachineLease does not exist\nKey: %s', key)
    return

  if not machine_lease.client_request_id:
    return

  if request_id != machine_lease.client_request_id:
    # Already cleared and incremented?
    logging.warning(
        'Request ID mismatch for MachineLease: %s\nExpected: %s\nActual: %s',
        key,
        request_id,
        machine_lease.client_request_id,
    )
    return

  machine_lease.bot_id = None
  machine_lease.client_request_id = None
  machine_lease.hostname = None
  machine_lease.lease_expiration_ts = None
  machine_lease.lease_id = None
  machine_lease.termination_task = None
  machine_lease.put()


@ndb.transactional
def associate_termination_task(key, hostname, task_id):
  """Associates a termination task with the given lease request.

  Args:
    key: ndb.Key for a MachineLease entity.
    hostname: Hostname of the machine the termination task is for.
    task_id: ID for a termination task.
  """
  machine_lease = key.get()
  if not machine_lease:
    logging.error('MachineLease does not exist\nKey: %s', key)
    return

  if hostname != machine_lease.hostname:
    logging.error(
        'Hostname mismatch\nKey: %s\nExpected: %s\nActual: %s',
        key,
        hostname,
        machine_lease.hostname,
    )
    return

  if machine_lease.termination_task == task_id:
    return

  machine_lease.termination_task = task_id
  machine_lease.put()


@ndb.transactional
def log_lease_fulfillment(
    key, request_id, hostname, lease_expiration_ts, lease_id):
  """Logs lease fulfillment.

  Args:
    key: ndb.Key for a MachineLease entity.
    request_id: ID of the request being fulfilled.
    hostname: Hostname of the machine fulfilling the request.
    lease_expiration_ts: UTC seconds since epoch when the lease expires.
    lease_id: ID of the lease assigned by Machine Provider.
  """
  machine_lease = key.get()
  if not machine_lease:
    logging.error('MachineLease does not exist\nKey: %s', key)
    return

  if request_id != machine_lease.client_request_id:
    logging.error(
        'Request ID mismatch\nKey: %s\nExpected: %s\nActual: %s',
        key,
        request_id,
        machine_lease.client_request_id,
    )
    return

  if (hostname == machine_lease.hostname
      and lease_expiration_ts == machine_lease.lease_expiration_ts
      and lease_id == machine_lease.lease_id):
    return

  machine_lease.hostname = hostname
  machine_lease.lease_expiration_ts = datetime.datetime.utcfromtimestamp(
      lease_expiration_ts)
  machine_lease.lease_id = lease_id
  machine_lease.put()


@ndb.transactional
def update_client_request_id(key):
  """Sets the client request ID used to lease a machine.

  Args:
    key: ndb.Key for a MachineLease entity.
  """
  machine_lease = key.get()
  if not machine_lease:
    logging.error('MachineLease does not exist\nKey: %s', key)
    return

  if machine_lease.drained:
    logging.info('MachineLease is drained\nKey: %s', key)
    return

  if machine_lease.client_request_id:
    return

  machine_lease.request_count += 1
  machine_lease.client_request_id = '%s-%s-%s' % (
      machine_lease.machine_type.id(), key.id(), machine_lease.request_count)
  machine_lease.put()


@ndb.transactional
def delete_machine_lease(key):
  """Deletes the given MachineLease if it is drained and has no active lease.

  Args:
    key: ndb.Key for a MachineLease entity.
  """
  machine_lease = key.get()
  if not machine_lease:
    return

  if not machine_lease.drained:
    logging.warning('MachineLease not drained: %s', key)
    return

  if machine_lease.client_request_id:
    return

  key.delete()


@ndb.transactional
def associate_bot_id(key, bot_id):
  """Associates a bot with the given machine lease.

  Args:
    key: ndb.Key for a MachineLease entity.
    bot_id: ID for a bot.
  """
  machine_lease = key.get()
  if not machine_lease:
    logging.error('MachineLease does not exist\nKey: %s', key)
    return

  if machine_lease.bot_id == bot_id:
    return

  machine_lease.bot_id = bot_id
  machine_lease.put()


def ensure_bot_info_exists(machine_lease):
  """Ensures a BotInfo entity exists and has Machine Provider-related fields.

  Args:
    machine_lease: MachineLease instance.
  """
  if machine_lease.bot_id == machine_lease.hostname:
    return
  bot_info = bot_management.get_info_key(machine_lease.hostname).get()
  if not (
      bot_info
      and bot_info.lease_id
      and bot_info.lease_expiration_ts
      and bot_info.machine_type
  ):
    bot_management.bot_event(
        event_type='bot_leased',
        bot_id=machine_lease.hostname,
        external_ip=None,
        authenticated_as=None,
        dimensions=None,
        state=None,
        version=None,
        quarantined=False,
        task_id='',
        task_name=None,
        lease_id=machine_lease.lease_id,
        lease_expiration_ts=machine_lease.lease_expiration_ts,
        machine_type=machine_lease.machine_type.id(),
    )
  associate_bot_id(machine_lease.key, machine_lease.hostname)


def last_shutdown_ts(hostname):
  """Returns the time the given bot posted a final bot_shutdown event.

  The bot_shutdown event is only considered if it is the last recorded event.

  Args:
    hostname: Hostname of the machine.

  Returns:
    datetime.datetime or None if the last recorded event is not bot_shutdown.
  """
  bot_event = bot_management.get_events_query(hostname, True).get()
  if bot_event and bot_event.event_type == 'bot_shutdown':
    return bot_event.ts


def handle_termination_task(machine_lease):
  """Checks the state of the termination task, releasing the lease if completed.

  Args:
    machine_lease: MachineLease instance.
  """
  assert machine_lease.termination_task

  task_result_summary = task_pack.unpack_result_summary_key(
      machine_lease.termination_task).get()
  if task_result_summary.state == task_result.State.COMPLETED:
    # There is a race condition where the bot reports the termination task as
    # completed but hasn't exited yet. The last thing it does before exiting
    # is post a bot_shutdown event. Check for the presence of a bot_shutdown
    # event which occurred after the termination task was completed.
    shutdown_ts = last_shutdown_ts(machine_lease.hostname)
    if not shutdown_ts or shutdown_ts < task_result_summary.completed_ts:
      logging.info(
          'Machine terminated but not yet shut down:\nKey: %s',
          machine_lease.key,
      )
      return

    response = machine_provider.release_machine(
        machine_lease.client_request_id)
    if response.get('error'):
      error = machine_provider.LeaseReleaseRequestError.lookup_by_name(
          response['error'])
      if error not in (
          machine_provider.LeaseReleaseRequestError.ALREADY_RECLAIMED,
          machine_provider.LeaseReleaseRequestError.NOT_FOUND,
      ):
        logging.error(
            'Lease release failed\nKey: %s\nRequest ID: %s\nError: %s',
            machine_lease.key,
            response['client_request_id'],
            response['error'],
        )
        return
    logging.info('MachineLease released: %s', machine_lease.key)
    clear_lease_request(machine_lease.key, machine_lease.client_request_id)
    bot_management.get_info_key(machine_lease.hostname).delete()


def handle_early_release(machine_lease):
  """Handles the early release of a leased machine.

  Args:
    machine_lease: MachineLease instance.
  """
  if machine_lease.lease_expiration_ts <= utils.utcnow() + datetime.timedelta(
      seconds=machine_lease.early_release_secs):
    logging.info('MachineLease ready to be released: %s', machine_lease.key)
    task_result_summary = task_scheduler.schedule_request(
        task_request.create_termination_task(machine_lease.hostname, True),
        None,
        check_acls=False,
    )
    associate_termination_task(
        machine_lease.key, machine_lease.hostname, task_result_summary.task_id)


def manage_leased_machine(machine_lease):
  """Manages a leased machine.

  Args:
    machine_lease: MachineLease instance with client_request_id, hostname,
      lease_expiration_ts set.
  """
  assert machine_lease.client_request_id, machine_lease.key
  assert machine_lease.hostname, machine_lease.key
  assert machine_lease.lease_expiration_ts, machine_lease.key

  # Handle a newly leased machine.
  if not machine_lease.bot_id:
    ensure_bot_info_exists(machine_lease)

  # Handle an expired lease.
  if machine_lease.lease_expiration_ts <= utils.utcnow():
    logging.info('MachineLease expired: %s', machine_lease.key)
    clear_lease_request(machine_lease.key, machine_lease.client_request_id)
    bot_management.get_info_key(machine_lease.hostname).delete()
    return

  # Handle an active lease with a termination task scheduled.
  # TODO(smut): Check if the bot got terminated by some other termination task.
  if machine_lease.termination_task:
    handle_termination_task(machine_lease)
    return

  # Handle a lease ready for early release.
  if machine_lease.early_release_secs:
    handle_early_release(machine_lease)
    return


def handle_lease_request_error(machine_lease, response):
  """Handles an error in the lease request response from Machine Provider.

  Args:
    machine_lease: MachineLease instance.
    response: Response returned by components.machine_provider.lease_machine.
  """
  error = machine_provider.LeaseRequestError.lookup_by_name(response['error'])
  if error in (
      machine_provider.LeaseRequestError.DEADLINE_EXCEEDED,
      machine_provider.LeaseRequestError.TRANSIENT_ERROR,
  ):
    logging.warning(
        'Transient failure: %s\nRequest ID: %s\nError: %s',
        machine_lease.key,
        response['client_request_id'],
        response['error'],
    )
  else:
    logging.error(
        'Lease request failed\nKey: %s\nRequest ID: %s\nError: %s',
        machine_lease.key,
        response['client_request_id'],
        response['error'],
    )
    clear_lease_request(machine_lease.key, machine_lease.client_request_id)


def handle_lease_request_response(machine_lease, response):
  """Handles a successful lease request response from Machine Provider.

  Args:
    machine_lease: MachineLease instance.
    response: Response returned by components.machine_provider.lease_machine.
  """
  assert not response.get('error')
  state = machine_provider.LeaseRequestState.lookup_by_name(response['state'])
  if state == machine_provider.LeaseRequestState.FULFILLED:
    if not response.get('hostname'):
      # Lease has already expired. This shouldn't happen, but it indicates the
      # lease expired faster than we could tell it even got fulfilled.
      logging.error(
          'Request expired\nKey: %s\nRequest ID:%s\nExpired: %s',
          machine_lease.key,
          machine_lease.client_request_id,
          response['lease_expiration_ts'],
      )
      clear_lease_request(machine_lease.key, machine_lease.client_request_id)
    else:
      logging.info(
          'Request fulfilled: %s\nRequest ID: %s\nHostname: %s\nExpires: %s',
          machine_lease.key,
          machine_lease.client_request_id,
          response['hostname'],
          response['lease_expiration_ts'],
      )
      log_lease_fulfillment(
          machine_lease.key,
          machine_lease.client_request_id,
          response['hostname'],
          int(response['lease_expiration_ts']),
          response['request_hash'],
      )
  elif state == machine_provider.LeaseRequestState.DENIED:
    logging.warning(
        'Request denied: %s\nRequest ID: %s',
        machine_lease.key,
        machine_lease.client_request_id,
    )
    clear_lease_request(machine_lease.key, machine_lease.client_request_id)


def manage_pending_lease_request(machine_lease):
  """Manages a pending lease request.

  Args:
    machine_lease: MachineLease instance with client_request_id set.
  """
  assert machine_lease.client_request_id, machine_lease.key

  response = machine_provider.lease_machine(
      machine_provider.LeaseRequest(
          dimensions=machine_lease.mp_dimensions,
          # TODO(smut): Vary duration so machines don't expire all at once.
          duration=machine_lease.lease_duration_secs,
          on_lease=machine_provider.Instruction(
              swarming_server='https://%s' % (
                  app_identity.get_default_version_hostname())),
          request_id=machine_lease.client_request_id,
      ),
  )

  if response.get('error'):
    handle_lease_request_error(machine_lease, response)
    return

  handle_lease_request_response(machine_lease, response)


def manage_lease(key):
  """Manages a MachineLease.

  Args:
    key: ndb.Key for a MachineLease entity.
  """
  machine_lease = key.get()
  if not machine_lease:
    return

  # Manage a leased machine.
  if machine_lease.lease_expiration_ts:
    manage_leased_machine(machine_lease)
    return

  # Lease expiration time is unknown, so there must be no leased machine.
  assert not machine_lease.hostname, key
  assert not machine_lease.termination_task, key

  # Manage a pending lease request.
  if machine_lease.client_request_id:
    manage_pending_lease_request(machine_lease)
    return

  # Manage an uninitiated lease request.
  if not machine_lease.drained:
    update_client_request_id(key)
    return

  # Manage an uninitiated, drained lease request.
  delete_machine_lease(key)
