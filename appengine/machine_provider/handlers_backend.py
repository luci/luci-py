# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Cron jobs for processing lease requests."""

import logging

from google.appengine.ext import ndb
import webapp2

from components import decorators

import models
import rpc_messages


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
  machine.state = models.CatalogMachineEntryStates.LEASED
  machine.put()
  lease.state = models.LeaseRequestStates.FULFILLED
  lease.put()
  return True
  # TODO: Notify the user his machine has been provided.


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


def create_backend_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/process-lease-requests', LeaseRequestProcessor),
  ])
