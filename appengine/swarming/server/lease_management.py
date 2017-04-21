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
import collections
import datetime
import json
import logging

from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop
from protorpc.remote import protojson

import ts_mon_metrics

from components import datastore_utils
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
  # DateTime indicating when the instruction to join the server was sent.
  instruction_ts = ndb.DateTimeProperty()
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
  # Base string to use as the request ID.
  request_id_base = ndb.StringProperty(indexed=False)
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


class MachineTypeUtilization(ndb.Model):
  """Utilization numbers for a MachineType.

  Key:
    id: Name of the MachineType these utilization numbers are associated with.
    kind: MachineTypeUtilization. Is a root entity.
  """
  # Number of busy bots created from this machine type.
  busy = ndb.IntegerProperty(indexed=False)
  # Number of idle bots created from this machine type.
  idle = ndb.IntegerProperty(indexed=False)
  # DateTime indicating when busy/idle numbers were last computed.
  last_updated_ts = ndb.DateTimeProperty()


@ndb.transactional_tasklet
def create_machine_lease(machine_lease_key, machine_type):
  """Creates a MachineLease from the given MachineType and MachineLease key.

  Args:
    machine_lease_key: ndb.Key for a MachineLease entity.
    machine_type: MachineType entity.
  """
  machine_lease = yield machine_lease_key.get_async()
  if machine_lease:
    return

  yield MachineLease(
      key=machine_lease_key,
      lease_duration_secs=machine_type.lease_duration_secs,
      early_release_secs=machine_type.early_release_secs,
      machine_type=machine_type.key,
      mp_dimensions=machine_type.mp_dimensions,
      # Deleting and recreating the MachineLease needs a unique base request ID,
      # otherwise it will hit old requests.
      request_id_base='%s-%s' % (machine_lease_key.id(), utils.time_time()),
  ).put_async()


@ndb.transactional_tasklet
def update_machine_lease(machine_lease_key, machine_type):
  """Updates the given MachineLease from the given MachineType.

  Args:
    machine_lease_key: ndb.Key for a MachineLease entity.
    machine_type: MachineType entity.
  """
  machine_lease = yield machine_lease_key.get_async()
  if not machine_lease:
    logging.error('MachineLease not found:\nKey: %s', machine_lease_key)
    return

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


@ndb.tasklet
def ensure_entity_exists(machine_type, n):
  """Ensures the nth MachineLease for the given MachineType exists.

  Args:
    machine_type: MachineType entity.
    n: The MachineLease index.
  """
  machine_lease_key = ndb.Key(
      MachineLease, '%s-%s' % (machine_type.key.id(), n))
  machine_lease = yield machine_lease_key.get_async()

  if not machine_lease:
    yield create_machine_lease(machine_lease_key, machine_type)
    return

  # If there is a MachineLease, we may need to update it if the MachineType's
  # lease properties have changed. It's only safe to update it if the current
  # lease is fulfilled (indicated by the presence of lease_expiration_ts) so
  # the changes only go into effect for the next lease request.
  if machine_lease.lease_expiration_ts and (
      machine_lease.early_release_secs != machine_type.early_release_secs
      or machine_lease.lease_duration_secs != machine_type.lease_duration_secs
      or machine_lease.mp_dimensions != machine_type.mp_dimensions
  ):
    yield update_machine_lease(machine_lease_key, machine_type)


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


def get_target_size(schedule, default, now=None):
  """Returns the current target size for the MachineType.

  Args:
    schedule: A proto.bots_pb2.Schedule proto.
    default: A default to return if now is not within any of config's intervals.
    now: datetime.datetime to use as the time to check what the MachineType's
      target size currently is. Defaults to use the current time if unspecified.

  Returns:
    Target size.
  """
  # Only daily schedules are supported right now.
  assert schedule.daily
  now = now or utils.utcnow()

  # The validator ensures the given time will fall in at most one interval,
  # because intervals are not allowed to intersect. So just search linearly
  # for a matching interval.
  # TODO(smut): Improve linear search if we end up with many intervals.
  for i in schedule.daily:
    # If the days of the week given by this interval do not include the current
    # day, move on to the next interval. If no days of the week are given by
    # this interval at all, then the interval applies every day.
    if i.days_of_the_week and now.weekday() not in i.days_of_the_week:
      continue

    # Get the start and end times of this interval relative to the current day.
    h, m = map(int, i.start.split(':'))
    start = datetime.datetime(now.year, now.month, now.day, h, m)
    h, m = map(int, i.end.split(':'))
    end = datetime.datetime(now.year, now.month, now.day, h, m)

    if start <= now <= end:
      return i.target_size

  return default


def ensure_entities_exist(max_concurrent=50):
  """Ensures MachineType entities are correct, and MachineLease entities exist.

  Updates MachineType entities based on the config and creates corresponding
  MachineLease entities.

  Args:
    max_concurrent: Maximum number of concurrent asynchronous requests.
  """
  now = utils.utcnow()
  # Seconds and microseconds are too granular for determining scheduling.
  now = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute)

  # Generate a few asynchronous requests at a time in order to prevent having
  # too many in flight at a time.
  futures = []
  machine_types = bot_groups_config.fetch_machine_types().copy()

  for machine_type in MachineType.query():
    # Check the MachineType in the datastore against its config.
    # If it no longer exists, just disable it here. If it exists but
    # doesn't match, update it.
    config = machine_types.pop(machine_type.key.id(), None)

    # If there is no config, disable the MachineType.
    if not config:
      if machine_type.enabled:
        machine_type.enabled = False
        futures.append(machine_type.put_async())
        logging.info('Disabling deleted MachineType: %s', machine_type)
      continue

    put = False

    # Handle scheduled config changes.
    if config.schedule and config.schedule.daily:
      target_size = get_target_size(
          config.schedule, config.target_size, now=now)
      if machine_type.target_size != target_size:
        logging.info(
            'Adjusting target_size (%s -> %s) for MachineType: %s',
            machine_type.target_size,
            target_size,
            machine_type,
        )
        machine_type.target_size = target_size
        put = True

    # If the MachineType does not match the config, update it. Copy the values
    # of certain fields so we can compare the MachineType to the config to check
    # for differences in all other fields.
    config = machine_type_pb2_to_entity(config)
    config.enabled = machine_type.enabled
    config.target_size = machine_type.target_size
    if machine_type != config:
      logging.info('Updating MachineType: %s', config)
      machine_type = config
      put = True

    # If there's anything to update, update it once here.
    if put:
      futures.append(machine_type.put_async())

    # If the MachineType isn't enabled, don't create MachineLease entities.
    if not machine_type.enabled:
      continue

    # Ensure the existence of MachineLease entities.
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

  # Create MachineTypes that never existed before.
  # The next iteration of this cron job will create their MachineLeases.
  if machine_types:
    machine_types = machine_types.values()

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
        or machine_lease.drained
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
  machine_lease.instruction_ts = None
  machine_lease.lease_expiration_ts = None
  machine_lease.lease_id = None
  machine_lease.termination_task = None
  machine_lease.put()


@ndb.transactional
def clear_termination_task(key, task_id):
  """Clears the termination task associated with the given lease request.

  Args:
    key: ndb.Key for a MachineLease entity.
    task_id: ID for a termination task.
  """
  machine_lease = key.get()
  if not machine_lease:
    logging.error('MachineLease does not exist\nKey: %s', key)
    return

  if not machine_lease.termination_task:
    return

  if task_id != machine_lease.termination_task:
    logging.error(
        'Task ID mismatch\nKey: %s\nExpected: %s\nActual: %s',
        key,
        task_id,
        machine_lease.task_id,
    )
    return

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

  if machine_lease.termination_task:
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
  machine_lease.client_request_id = '%s-%s' % (
      machine_lease.request_id_base, machine_lease.request_count)
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
    logging.info(
        'Creating BotEvent\nKey: %s\nHostname: %s\nBotInfo: %s',
        machine_lease.key,
        machine_lease.hostname,
        bot_info,
    )
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
    # Occasionally bot_management.bot_event fails to store the BotInfo so
    # verify presence of Machine Provider fields. See https://crbug.com/681224.
    bot_info = bot_management.get_info_key(machine_lease.hostname).get()
    if not (
        bot_info
        and bot_info.lease_id
        and bot_info.lease_expiration_ts
        and bot_info.machine_type
    ):
      # If associate_bot_id isn't called, cron will try again later.
      logging.error(
          'Failed to put BotInfo\nKey: %s\nHostname: %s\nBotInfo: %s',
          machine_lease.key,
          machine_lease.hostname,
          bot_info,
      )
      return
    logging.info(
        'Put BotInfo\nKey: %s\nHostname: %s\nBotInfo: %s',
        machine_lease.key,
        machine_lease.hostname,
        bot_info,
    )
  associate_bot_id(machine_lease.key, machine_lease.hostname)


@ndb.transactional
def associate_instruction_ts(key, instruction_ts):
  """Associates an instruction time with the given machine lease.

  Args:
    key: ndb.Key for a MachineLease entity.
    bot_id: ID for a bot.
  """
  machine_lease = key.get()
  if not machine_lease:
    logging.error('MachineLease does not exist\nKey: %s', key)
    return

  if machine_lease.instruction_ts:
    return

  machine_lease.instruction_ts = instruction_ts
  machine_lease.put()


def send_connection_instruction(machine_lease):
  """Sends an instruction to the given machine to connect to the server.

  Args:
    machine_lease: MachineLease instance.
  """
  now = utils.utcnow()
  response = machine_provider.instruct_machine(
      machine_lease.client_request_id,
      'https://%s' % app_identity.get_default_version_hostname(),
  )
  if not response:
    logging.error(
        'MachineLease instruction got empty response:\nKey: %s\nHostname: %s',
        machine_lease.key,
        machine_lease.hostname,
    )
  elif not response.get('error'):
    associate_instruction_ts(machine_lease.key, now)
  elif response['error'] == 'ALREADY_RECLAIMED':
    # Can happen if lease duration is very short or there is a significant delay
    # in creating the BotInfo or instructing the machine. Consider it an error.
    logging.error(
        'MachineLease expired before machine connected:\nKey: %s\nHostname: %s',
        machine_lease.key,
        machine_lease.hostname,
    )
    clear_lease_request(machine_lease.key, machine_lease.client_request_id)
  else:
    logging.warning(
        'MachineLease instruction error:\nKey: %s\nHostname: %s\nError: %s',
        machine_lease.key,
        machine_lease.hostname,
        response['error'],
    )


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
  if task_result_summary.state in task_result.State.STATES_EXCEPTIONAL:
    logging.info(
        'Termination failed:\nKey: %s\nHostname: %s\nTask ID: %s\nState: %s',
        machine_lease.key,
        machine_lease.hostname,
        machine_lease.termination_task,
        task_result.State.to_string(task_result_summary.state),
    )
    clear_termination_task(machine_lease.key, machine_lease.termination_task)
    return

  if task_result_summary.state == task_result.State.COMPLETED:
    # There is a race condition where the bot reports the termination task as
    # completed but hasn't exited yet. The last thing it does before exiting
    # is post a bot_shutdown event. Check for the presence of a bot_shutdown
    # event which occurred after the termination task was completed.
    shutdown_ts = last_shutdown_ts(machine_lease.hostname)
    if not shutdown_ts or shutdown_ts < task_result_summary.completed_ts:
      logging.info(
          'Machine terminated but not yet shut down:\nKey: %s\nHostname: %s',
          machine_lease.key,
          machine_lease.hostname,
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
    logging.info(
        'MachineLease released:\nKey%s\nHostname: %s',
        machine_lease.key,
        machine_lease.hostname,
    )
    clear_lease_request(machine_lease.key, machine_lease.client_request_id)
    bot_management.get_info_key(machine_lease.hostname).delete()


def handle_early_release(machine_lease):
  """Handles the early release of a leased machine.

  Args:
    machine_lease: MachineLease instance.
  """
  assert not machine_lease.termination_task, machine_lease.termination_task

  early_expiration_ts = machine_lease.lease_expiration_ts - datetime.timedelta(
      seconds=machine_lease.early_release_secs)

  if machine_lease.drained or early_expiration_ts <= utils.utcnow():
    logging.info(
        'MachineLease ready to be released:\nKey: %s\nHostname: %s',
        machine_lease.key,
        machine_lease.hostname,
    )
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

  # Once BotInfo is created, send the instruction to join the server.
  if not machine_lease.instruction_ts:
    send_connection_instruction(machine_lease)

  # Handle an expired lease.
  if machine_lease.lease_expiration_ts <= utils.utcnow():
    logging.info(
        'MachineLease expired:\nKey: %s\nHostname: %s',
        machine_lease.key,
        machine_lease.hostname,
    )
    clear_lease_request(machine_lease.key, machine_lease.client_request_id)
    bot_management.get_info_key(machine_lease.hostname).delete()
    return

  # Handle an active lease with a termination task scheduled.
  # TODO(smut): Check if the bot got terminated by some other termination task.
  if machine_lease.termination_task:
    logging.info(
        'MachineLease pending termination:\nKey: %s\nHostname: %s\nTask ID: %s',
        machine_lease.key,
        machine_lease.hostname,
        machine_lease.termination_task,
    )
    handle_termination_task(machine_lease)
    return

  # Handle a lease ready for early release.
  if machine_lease.early_release_secs or machine_lease.drained:
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

  logging.info(
      'Sending lease request: %s\nRequest ID: %s',
      machine_lease.key,
      machine_lease.client_request_id,
  )
  response = machine_provider.lease_machine(
      machine_provider.LeaseRequest(
          dimensions=machine_lease.mp_dimensions,
          # TODO(smut): Vary duration so machines don't expire all at once.
          duration=machine_lease.lease_duration_secs,
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


def compute_utilization(batch_size=50):
  """Computes bot utilization per machine type.

  Args:
    batch_size: Number of bots to query for at a time.
  """
  # Records entities we've seen already. Used for debug logging. The query is
  # returning more entities than expected on some servers.
  seen = {}
  # Maps machine types to [busy, idle] bot counts.
  machine_types = collections.defaultdict(lambda: [0, 0])
  now = utils.utcnow()
  q = bot_management.BotInfo.query()
  q = bot_management.filter_availability(q, False, False, now, None, True)
  cursor = ''
  while cursor is not None:
    bots, cursor = datastore_utils.fetch_page(q, batch_size, cursor)
    for bot in bots:
      if bot.key.parent().id() in seen:
        logging.warning(
            'Saw duplicate bot: %s\nPrevious task ID: %s\nCurrent task ID: %s',
            bot.key.parent().id(),
            seen[bot.key.parent().id()],
            bot.task_id,
        )
      seen[bot.key.parent().id()] = bot.task_id
      if bot.task_id:
        machine_types[bot.machine_type][0] += 1
      else:
        machine_types[bot.machine_type][1] += 1

  for machine_type, (busy, idle) in machine_types.iteritems():
    logging.info('Utilization for %s: %s/%s', machine_type, busy, busy + idle)
    MachineTypeUtilization(
        id=machine_type,
        busy=busy,
        idle=idle,
        last_updated_ts=now,
    ).put()
  # TODO(smut): Use this computation for autoscaling.


def set_global_metrics():
  """Set global Machine Provider-related ts_mon metrics."""
  # Consider utilization metrics over 2 minutes old to be outdated.
  outdated = utils.utcnow() - datetime.timedelta(minutes=2)
  for machine_type in MachineType.query():
    ts_mon_metrics.machine_types_target_size.set(
        machine_type.target_size,
        fields={
            'enabled': machine_type.enabled,
            'machine_type': machine_type.key.id(),
        },
        target_fields=ts_mon_metrics.TARGET_FIELDS,
    )
    utilization = ndb.Key(MachineTypeUtilization, machine_type.key.id()).get()
    if utilization and utilization.last_updated_ts > outdated:
      ts_mon_metrics.machine_types_actual_size.set(
          utilization.busy,
          fields={
              'busy': True,
              'machine_type': machine_type.key.id(),
          },
          target_fields=ts_mon_metrics.TARGET_FIELDS,
      )
      ts_mon_metrics.machine_types_actual_size.set(
          utilization.idle,
          fields={
              'busy': False,
              'machine_type': machine_type.key.id(),
          },
          target_fields=ts_mon_metrics.TARGET_FIELDS,
      )
