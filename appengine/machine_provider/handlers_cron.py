# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Cron jobs for processing lease requests."""

import datetime
import logging

from google.appengine.api import taskqueue
from google.appengine.ext import ndb
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
  if lease.state != models.LeaseRequestStates.UNTRIAGED:
    logging.warning('LeaseRequest no longer untriaged:\n%s', lease)
    return False

  logging.info('Leasing CatalogMachineEntry:\n%s', machine)
  lease.leased_ts = utils.utcnow()
  lease.machine_id = machine.key.id()
  lease.state = models.LeaseRequestStates.FULFILLED
  machine.lease_id = lease.key.id()
  machine.lease_expiration_ts = lease.leased_ts + datetime.timedelta(
      seconds=lease.request.duration,
  )
  machine.state = models.CatalogMachineEntryStates.LEASED
  ndb.put_multi([lease, machine])
  if not utils.enqueue_task(
      '/internal/queues/fulfill-lease-request',
      'fulfill-lease-request',
      params={
          'lease_id': lease.key.id(),
          'machine_id': machine.key.id(),
          'pubsub_project': lease.request.pubsub_project,
          'pubsub_topic': lease.request.pubsub_topic,
      },
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
  if lease.state != models.LeaseRequestStates.UNTRIAGED:
    logging.warning('LeaseRequest no longer untriaged:\n%s', lease)
    return False

  logging.info('Preparing CatalogCapacityEntry:\n%s', capacity)
  capacity.count -= 0
  capacity.put()
  lease.state = models.LeaseRequest.States.PENDING
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
  lease.machine_id = None
  machine.lease_id = None
  machine.lease_expiration_ts = None

  policy = machine.policies.on_reclamation
  if policy == rpc_messages.MachineReclamationPolicy.DELETE:
    lease.put()
    machine.key.delete()
  else:
    if policy == rpc_messages.MachineReclamationPolicy.MAKE_AVAILABLE:
      machine.state = models.CatalogMachineEntryStates.AVAILABLE
    else:
      if policy != rpc_messages.MachineReclamationPolicy.RECLAIM:
        # Something is awry. Log an error, but still reclaim the machine.
        # Fall back on the RECLAIM policy because it notifies the backend and
        # prevents the machine from being leased out again, but keeps it in
        # the Catalog in case we want to examine it further.
        logging.error(
            'Unexpected MachineReclamationPolicy: %s\nDefaulting to RECLAIM.',
            policy,
        )
      machine.state = models.CatalogMachineEntryStates.RECLAIMED
    ndb.put_multi([lease, machine])

  if not utils.enqueue_task(
      '/internal/queues/reclaim-machine',
      'reclaim-machine',
      params={
          'backend_project': machine.policies.pubsub_project,
          'backend_topic': machine.policies.pubsub_topic,
          'lease_id': lease.key.id(),
          'lessee_project': lease.request.pubsub_project,
          'lessee_topic': lease.request.pubsub_topic,
          'machine_id': machine.key.id(),
      },
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

    for machine in models.CatalogMachineEntry.query(
        models.CatalogMachineEntry.lease_expiration_ts < now,
        # Also filter out unassigned machines, i.e. CatalogMachineEntries
        # where lease_expiration_ts is None. None sorts before min_ts.
        models.CatalogMachineEntry.lease_expiration_ts > min_ts,
    ).fetch(keys_only=True):
      reclaim_machine(machine, now)


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/process-lease-requests', LeaseRequestProcessor),
      ('/internal/cron/process-machine-reclamations', MachineReclamationProcessor),
  ])
