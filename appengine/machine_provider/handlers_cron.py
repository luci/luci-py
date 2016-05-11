# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Cron jobs for processing lease requests."""

import datetime
import time
import logging

from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from protorpc.remote import protojson
import webapp2

from components import decorators
from components import utils
from components.machine_provider import rpc_messages

import models


class Error(Exception):
  pass


class TaskEnqueuingError(Error):
  def __init__(self, queue_name):
    super(TaskEnqueuingError, self).__init__()
    self.queue_name = queue_name


def can_fulfill(entry, request):
  """Determines if the given CatalogEntry can fulfill the given LeaseRequest.

  Args:
    entry: A models.CatalogEntry instance.
    request: An rpc_messages.LeaseRequest instance.

  Returns:
    True if the given CatalogEntry can be used to fulfill the given
    LeaseRequest, otherwise False.
  """
  # For each dimension, check if the entry meets or exceeds the request.
  # For now, "exceeds" is defined as when the request leaves the dimension
  # unspecified/None, but the request has a non-None value. In the future,
  # "exceeds" may be defined per-dimension. E.g. in the future, an entry
  # with 16GB RAM may fulfill a request for 8GB RAM.
  for dimension in rpc_messages.Dimensions.all_fields():
    entry_value = entry.dimensions.get_assigned_value(dimension.name)
    request_value = request.dimensions.get_assigned_value(dimension.name)
    if request_value is not None and entry_value != request_value:
      # There is a mismatched dimension, and the requested dimension was
      # not None, which means the entry does not fulfill the request.
      return False
  return True


def get_dimension_filters(request):
  """Returns filters to match the requested dimensions in a CatalogEntry query.

  Args:
    request: An rpc_messages.LeaseRequest instance.

  Returns:
    A list of filters for a CatalogEntry query.
  """
  # See can_fulfill for more information.
  filters = []
  for dimension in rpc_messages.Dimensions.all_fields():
    entry_value = getattr(models.CatalogEntry.dimensions, dimension.name)
    request_value = request.dimensions.get_assigned_value(dimension.name)
    if request_value is not None:
      filters.append(entry_value == request_value)
  return filters


@ndb.transactional(xg=True)
def lease_machine(machine_key, lease):
  """Attempts to lease the given machine.

  Args:
    machine_key: ndb.Key for a model.CatalogMachineEntry instance.
    lease: model.LeaseRequest instance.

  Returns:
    True if the machine was leased, otherwise False.
  """
  machine = machine_key.get()
  lease = lease.key.get()
  logging.info('Attempting to lease matching CatalogMachineEntry:\n%s', machine)

  if not can_fulfill(machine, lease.request):
    logging.warning('CatalogMachineEntry no longer matches:\n%s', machine)
    return False
  if machine.state != models.CatalogMachineEntryStates.AVAILABLE:
    logging.warning('CatalogMachineEntry no longer available:\n%s', machine)
    return False
  if lease.response.state != rpc_messages.LeaseRequestState.UNTRIAGED:
    logging.warning('LeaseRequest no longer untriaged:\n%s', lease)
    return False

  logging.info('Leasing CatalogMachineEntry:\n%s', machine)
  lease.leased_ts = utils.utcnow()
  lease_expiration_ts = lease.leased_ts + datetime.timedelta(
      seconds=lease.request.duration,
  )
  lease.machine_id = machine.key.id()
  lease.response.hostname = machine.dimensions.hostname
  # datetime_to_timestamp returns microseconds, which are too fine grain.
  lease.response.lease_expiration_ts = utils.datetime_to_timestamp(
      lease_expiration_ts) / 1000 / 1000
  lease.response.state = rpc_messages.LeaseRequestState.FULFILLED
  machine.lease_id = lease.key.id()
  machine.lease_expiration_ts = lease_expiration_ts
  machine.state = models.CatalogMachineEntryStates.LEASED
  ndb.put_multi([lease, machine])
  params = {
      'policies': protojson.encode_message(machine.policies),
      'request_json': protojson.encode_message(lease.request),
      'response_json': protojson.encode_message(lease.response),
      'machine_project': machine.pubsub_topic_project,
      'machine_topic': machine.pubsub_topic,
  }
  if not utils.enqueue_task(
      '/internal/queues/fulfill-lease-request',
      'fulfill-lease-request',
      params=params,
      transactional=True,
  ):
    raise TaskEnqueuingError('fulfill-lease-request')
  return True


@ndb.transactional(xg=True)
def provide_capacity(capacity_key, lease):
  """Attempts to provide capacity for the given lease.

  Args:
    capacity_key: ndb.Key for a model.CatalogCapacityEntry instance.
    lease: model.LeaseRequest instance.

  Returns:
    True if the capacity is being provided, otherwise False.
  """
  capacity = capacity_key.get()
  lease = lease.key.get()
  logging.info(
      'Attempting to provide matching CatalogCapacityEntry:\n%s',
      capacity,
  )

  if not can_fulfill(capacity, lease.request):
    logging.warning('CatalogCapacityEntry no longer matches:\n%s', capacity)
    return False
  if capacity.count == 0:
    logging.warning('CatalogCapacityEntry no longer available:\n%s', capacity)
    return False
  if lease.response.state != rpc_messages.LeaseRequestState.UNTRIAGED:
    logging.warning('LeaseRequest no longer untriaged:\n%s', lease)
    return False

  logging.info('Preparing CatalogCapacityEntry:\n%s', capacity)
  capacity.count -= 0
  capacity.put()
  lease.response.state = rpc_messages.LeaseRequestState.PENDING
  lease.put()
  return True
  # TODO: Contact the backend to provision this capacity.


class LeaseRequestProcessor(webapp2.RequestHandler):
  """Worker for processing lease requests."""

  @decorators.require_cronjob
  def get(self):
    for lease in models.LeaseRequest.query_untriaged():
      logging.info('Processing untriaged LeaseRequest:\n%s', lease)
      filters = get_dimension_filters(lease.request)
      fulfilled = False

      # Prefer immediately available machines.
      for machine_key in models.CatalogMachineEntry.query_available(*filters):
        if lease_machine(machine_key, lease):
          fulfilled = True
          break

      if fulfilled:
        continue

      # Fall back on available capacity.
      for capacity_key in models.CatalogCapacityEntry.query_available(*filters):
        if provide_capacity(capacity_key, lease):
          break


@ndb.transactional(xg=True)
def reclaim_machine(machine_key, reclamation_ts):
  """Attempts to reclaim the given machine.

  Args:
    machine_key: ndb.Key for a model.CatalogMachineEntry instance.
    reclamation_ts: datetime.datetime instance indicating when the machine was
      reclaimed.

  Returns:
    True if the machine was reclaimed, else False.
  """
  machine = machine_key.get()
  logging.info('Attempting to reclaim CatalogMachineEntry:\n%s', machine)

  if machine.lease_expiration_ts is None:
    # This can reasonably happen if e.g. the lease was voluntarily given up.
    logging.warning('CatalogMachineEntry no longer leased:\n%s', machine)
    return False

  if reclamation_ts < machine.lease_expiration_ts:
    # This can reasonably happen if e.g. the lease duration was extended.
    logging.warning('CatalogMachineEntry no longer overdue:\n%s', machine)
    return False

  logging.info('Reclaiming CatalogMachineEntry:\n%s', machine)
  lease = models.LeaseRequest.get_by_id(machine.lease_id)
  hostname = lease.response.hostname
  lease.machine_id = None
  lease.response.hostname = None
  machine.lease_id = None
  machine.lease_expiration_ts = None

  policy = machine.policies.on_reclamation
  if policy == rpc_messages.MachineReclamationPolicy.DELETE:
    logging.info('Executing MachineReclamationPolicy: DELETE')
    lease.put()
    machine.key.delete()
  else:
    if policy == rpc_messages.MachineReclamationPolicy.MAKE_AVAILABLE:
      logging.info('Executing MachineReclamationPolicy: MAKE_AVAILABLE')
      machine.state = models.CatalogMachineEntryStates.AVAILABLE
    else:
      if policy != rpc_messages.MachineReclamationPolicy.RECLAIM:
        # Something is awry. Log an error, but still reclaim the machine.
        # Fall back on the RECLAIM policy because it notifies the backend and
        # prevents the machine from being leased out again, but keeps it in
        # the Catalog in case we want to examine it further.
        logging.error(
            'Unexpected MachineReclamationPolicy: %s\nDefaulting to RECLAIM',
            policy,
        )
      else:
        logging.info('Executing MachineReclamationPolicy: RECLAIM')
      machine.state = models.CatalogMachineEntryStates.RECLAIMED
    ndb.put_multi([lease, machine])

  params = {
      'hostname': hostname,
      'machine_project': machine.pubsub_topic_project,
      'machine_topic': machine.pubsub_topic,
      'policies': protojson.encode_message(machine.policies),
      'request_json': protojson.encode_message(lease.request),
      'response_json': protojson.encode_message(lease.response),
  }
  backend_attributes = {}
  for attribute in machine.policies.backend_attributes:
    backend_attributes[attribute.key] = attribute.value
  params['backend_attributes'] = utils.encode_to_json(backend_attributes)
  if lease.request.pubsub_topic:
    params['lessee_project'] = lease.request.pubsub_project
    params['lessee_topic'] = lease.request.pubsub_topic
  if not utils.enqueue_task(
      '/internal/queues/reclaim-machine',
      'reclaim-machine',
      params=params,
      transactional=True,
  ):
    raise TaskEnqueuingError('reclaim-machine')
  return True


class MachineReclamationProcessor(webapp2.RequestHandler):
  """Worker for processing machine reclamation."""

  @decorators.require_cronjob
  def get(self):
    min_ts = utils.timestamp_to_datetime(0)
    now = utils.utcnow()

    for machine_key in models.CatalogMachineEntry.query(
        models.CatalogMachineEntry.lease_expiration_ts < now,
        # Also filter out unassigned machines, i.e. CatalogMachineEntries
        # where lease_expiration_ts is None. None sorts before min_ts.
        models.CatalogMachineEntry.lease_expiration_ts > min_ts,
    ).fetch(keys_only=True):
      reclaim_machine(machine_key, now)


@ndb.transactional(xg=True)
def release_lease(lease_key):
  """Releases a lease on a machine.

  Args:
    lease_key: ndb.Key for a models.LeaseRequest entity.
  """
  lease = lease_key.get()
  if not lease:
    logging.warning('LeaseRequest not found: %s', lease_key)
    return
  if not lease.released:
    logging.warning('LeaseRequest not released:\n%s', lease)
    return

  lease.released = False
  if not lease.machine_id:
    logging.warning('LeaseRequest has no associated machine:\n%s', lease)
    lease.put()
    return

  machine = ndb.Key(models.CatalogMachineEntry, lease.machine_id).get()
  if not machine:
    logging.error('LeaseRequest has non-existent machine leased:\n%s', lease)
    lease.put()
    return

  # Just expire the lease now and let MachineReclamationProcessor handle it.
  logging.info('Expiring LeaseRequest:\n%s', lease)
  now = utils.utcnow()
  lease.response.lease_expiration_ts = utils.datetime_to_timestamp(
      now) / 1000 / 1000
  machine.lease_expiration_ts = now
  ndb.put_multi([lease, machine])


class LeaseReleaseProcessor(webapp2.RequestHandler):
  """Worker for processing voluntary lease releases."""

  @decorators.require_cronjob
  def get(self):
    for lease_key in models.LeaseRequest.query(
        models.LeaseRequest.released == True,
    ).fetch(keys_only=True):
      release_lease(lease_key)


@ndb.transactional
def create_subscription(machine_key):
  """Creates a Cloud Pub/Sub subscription for machine communication.

  Args:
    machine_key: ndb.Key for the machine whose subscription should be created.
  """
  machine = machine_key.get()
  logging.info('Attempting to subscribe CatalogMachineEntry:\n%s', machine)

  if not machine:
    logging.warning('CatalogMachineEntry no longer exists: %s', machine_key)
    return

  if machine.state != models.CatalogMachineEntryStates.NEW:
    logging.warning('CatalogMachineEntry no longer new:\n%s', machine)
    return

  if not machine.policies.machine_service_account:
    logging.warning(
        'CatalogMachineEntry has no machine service account:\n%s', machine)
    return

  if machine.pubsub_subscription:
    logging.info('CatalogMachineEntry already subscribed:\n%s', machine)
    return

  machine.pubsub_subscription = 'subscription-%s' % machine.key.id()
  machine.pubsub_topic = 'topic-%s' % machine.key.id()

  params = {
      'backend_project': machine.policies.backend_project,
      'backend_topic': machine.policies.backend_topic,
      'hostname': machine.dimensions.hostname,
      'machine_id': machine.key.id(),
      'machine_service_account': machine.policies.machine_service_account,
      'machine_subscription': machine.pubsub_subscription,
      'machine_subscription_project': machine.pubsub_subscription_project,
      'machine_topic': machine.pubsub_topic,
      'machine_topic_project': machine.pubsub_topic_project,
  }
  backend_attributes = {}
  for attribute in machine.policies.backend_attributes:
    backend_attributes[attribute.key] = attribute.value
  params['backend_attributes'] = utils.encode_to_json(backend_attributes)
  if utils.enqueue_task(
      '/internal/queues/subscribe-machine',
      'subscribe-machine',
      params=params,
      transactional=True,
  ):
    machine.state = models.CatalogMachineEntryStates.SUBSCRIBING
    machine.put()
  else:
    raise TaskEnqueuingError('subscribe-machine')


class NewMachineProcessor(webapp2.RequestHandler):
  """Worker for processing new machines."""

  @decorators.require_cronjob
  def get(self):
    for machine_key in models.CatalogMachineEntry.query(
        models.CatalogMachineEntry.state==models.CatalogMachineEntryStates.NEW,
    ).fetch(keys_only=True):
      create_subscription(machine_key)


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/process-lease-requests', LeaseRequestProcessor),
      ('/internal/cron/process-machine-reclamations', MachineReclamationProcessor),
      ('/internal/cron/process-lease-releases', LeaseReleaseProcessor),
      ('/internal/cron/process-new-machines', NewMachineProcessor),
  ])
