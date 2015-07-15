# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Cloud endpoints for the Machine Provider API."""

import hashlib
import json
import logging

import endpoints
from google.appengine.ext import ndb

from protorpc import protobuf
from protorpc import remote

from components import auth

import acl
import models
import rpc_messages


@auth.endpoints.api(name='catalog', version='v1')
class CatalogEndpoints(remote.Service):
  """Implements cloud endpoints for the Machine Provider Catalog."""

  @staticmethod
  def check_backend(request):
    """Checks that the given catalog manipulation request specifies a backend.

    Returns:
      rpc_messages.CatalogManipulationRequestError.UNSPECIFIED_BACKEND if the
      backend is unspecified and can't be inferred, otherwise None.
    """
    if acl.is_catalog_admin():
      # Catalog administrators may update any CatalogEntry, but the backend must
      # be specified because hostname uniqueness is enforced per-backend.
      if not request.dimensions.backend:
        logging.warning('Backend unspecified by administrator')
        return rpc_messages.CatalogManipulationRequestError.UNSPECIFIED_BACKEND
    elif acl.is_backend_service():
      # Backends may only update their own machines.
      current_backend = acl.get_current_backend()
      if request.dimensions.backend is None:
        request.dimensions.backend = current_backend
      if request.dimensions.backend != current_backend:
        logging.warning('Mismatched backend')
        return rpc_messages.CatalogManipulationRequestError.MISMATCHED_BACKEND

  @staticmethod
  def check_hostname(request):
    """Checks that the given catalog manipulation request specifies a hostname.

    Returns:
      rpc_messages.CatalogManipulationRequestError.UNSPECIFIED_HOSTNAME if the
      hostname is unspecified, otherwise None.
    """
    if not request.dimensions.hostname:
      logging.warning('Hostname unspecified')
      return rpc_messages.CatalogManipulationRequestError.UNSPECIFIED_HOSTNAME

  @auth.endpoints_method(
      rpc_messages.CatalogMachineAdditionRequest,
      rpc_messages.CatalogManipulationResponse,
  )
  @auth.require(acl.is_backend_service_or_catalog_admin)
  def add_machine(self, request):
    """Handles an incoming CatalogMachineAdditionRequest."""
    user = auth.get_current_identity().to_bytes()
    logging.info(
        'Received CatalogMachineAdditionRequest:\nUser: %s\n%s',
        user,
        request,
    )
    error = self.check_backend(request) or self.check_hostname(request)
    if error:
      return rpc_messages.CatalogManipulationResponse(error=error)
    return self._add_machine(request)

  @ndb.transactional
  def _add_machine(self, request):
    """Handles datastore operations for CatalogMachineAdditionRequests."""
    entry = models.CatalogMachineEntry.generate_key(request.dimensions).get()
    if entry:
      # Enforces per-backend hostname uniqueness.
      logging.info('Hostname reuse:\nOriginally used for: \n%s', entry)
      return rpc_messages.CatalogManipulationResponse(
          error=rpc_messages.CatalogManipulationRequestError.HOSTNAME_REUSE
      )
    models.CatalogMachineEntry.create_and_put(request.dimensions)
    return rpc_messages.CatalogManipulationResponse()

  @auth.endpoints_method(
      rpc_messages.CatalogMachineDeletionRequest,
      rpc_messages.CatalogManipulationResponse,
  )
  @auth.require(acl.is_backend_service_or_catalog_admin)
  def delete_machine(self, request):
    """Handles an incoming CatalogMachineDeletionRequest."""
    user = auth.get_current_identity().to_bytes()
    logging.info(
        'Received CatalogMachineDeletionRequest:\nUser: %s\n%s',
        user,
        request,
    )
    error = self.check_backend(request) or self.check_hostname(request)
    if error:
      return rpc_messages.CatalogManipulationResponse(error=error)
    return self._delete_machine(request)

  @ndb.transactional
  def _delete_machine(self, request):
    """Handles datastore operations for CatalogMachineDeletionRequests."""
    entry = models.CatalogMachineEntry.generate_key(request.dimensions).get()
    if not entry:
      logging.info('Catalog entry not found')
      return rpc_messages.CatalogManipulationResponse(
        error=rpc_messages.CatalogManipulationRequestError.ENTRY_NOT_FOUND,
      )
    entry.key.delete()
    return rpc_messages.CatalogManipulationResponse()

  @auth.endpoints_method(
      rpc_messages.CatalogCapacityModificationRequest,
      rpc_messages.CatalogManipulationResponse,
  )
  @auth.require(acl.is_backend_service_or_catalog_admin)
  def modify_capacity(self, request):
    """Handles an incoming CatalogCapacityModificationRequest."""
    user = auth.get_current_identity().to_bytes()
    logging.info(
        'Received CatalogCapacityModificationRequest:\nUser: %s\n%s',
        user,
        request,
    )
    error = self.check_backend(request)
    if error:
      return rpc_messages.CatalogManipulationResponse(error=error)
    return self._modify_capacity(request)

  @ndb.transactional
  def _modify_capacity(self, request):
    """Handles datastore operations for CatalogCapacityModificationRequests."""
    models.CatalogCapacityEntry.create_and_put(request.dimensions)
    return rpc_messages.CatalogManipulationResponse()


@auth.endpoints.api(name='machine_provider', version='v1')
class MachineProviderEndpoints(remote.Service):
  """Implements cloud endpoints for the Machine Provider."""

  @auth.endpoints_method(rpc_messages.LeaseRequest, rpc_messages.LeaseResponse)
  @auth.require(acl.can_issue_lease_requests)
  def lease(self, request):
    """Handles an incoming LeaseRequest."""
    # Hash the combination of client + client-generated request ID in order to
    # deduplicate responses on a per-client basis.
    user = auth.get_current_identity().to_bytes()
    request_hash = hashlib.sha1(
        '%s\0%s' % (user, request.request_id)
    ).hexdigest()
    logging.info(
        'Received LeaseRequest:\nUser: %s\nRequest hash: %s\n%s',
        user,
        request_hash,
        request,
    )
    duplicate = models.LeaseRequest.get_by_id(request_hash)
    deduplication_checksum = hashlib.sha1(
        protobuf.encode_message(request)
    ).hexdigest()
    if duplicate:
      # Found a duplicate request ID from the same user. Attempt deduplication.
      if deduplication_checksum == duplicate.deduplication_checksum:
        # The LeaseRequest RPC we just received matches the original.
        # We're safe to dedupe.
        logging.info(
            'Dropped duplicate LeaseRequest:\n%s', duplicate.response,
        )
        return duplicate.response
      else:
        logging.warning(
            'Request ID reuse:\nOriginally used for:\n%s',
            duplicate.request
        )
        return rpc_messages.LeaseResponse(
            error=rpc_messages.LeaseRequestError.REQUEST_ID_REUSE
        )
    else:
      logging.info('Storing LeaseRequest')
      response = rpc_messages.LeaseResponse()
      response.request_hash = request_hash
      models.LeaseRequest(
          deduplication_checksum=deduplication_checksum,
          id=request_hash,
          owner=auth.get_current_identity(),
          request=request,
          response=response,
          state=models.LeaseRequestStates.UNTRIAGED,
      ).put()
      logging.info('Sending LeaseResponse:\n%s', response)
      return response


def create_endpoints_app():
  return endpoints.api_server([CatalogEndpoints, MachineProviderEndpoints])
