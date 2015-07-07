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

  @auth.endpoints_method(
      rpc_messages.CatalogAdditionRequest,
      rpc_messages.CatalogManipulationResponse,
  )
  @auth.require(acl.is_backend_service_or_catalog_admin)
  @ndb.transactional
  def add(self, request):
    """Handles an incoming CatalogAdditionRequest."""
    user = auth.get_current_identity().to_bytes()
    logging.info(
        'Received CatalogAdditionRequest:\nUser: %s\n%s',
        user,
        request,
    )
    error = self.check_backend(request)
    if error:
      return rpc_messages.CatalogManipulationResponse(error=error)

    if request.dimensions.hostname:
      entry = models.CatalogEntry.get_by_id(
          models.CatalogEntry.compute_id(request.dimensions),
      )
      if entry:
        # Hostname, if specified, must be unique.
        logging.info('Hostname reuse:\nOriginally used for: \n%s', entry)
        return rpc_messages.CatalogManipulationResponse(
            error=rpc_messages.CatalogManipulationRequestError.HOSTNAME_REUSE
        )
    models.CatalogEntry.create_and_put(request.dimensions)
    return rpc_messages.CatalogManipulationResponse()

  @auth.endpoints_method(
      rpc_messages.CatalogDeletionRequest,
      rpc_messages.CatalogManipulationResponse,
  )
  @auth.require(acl.is_backend_service_or_catalog_admin)
  def delete(self, request):
    """Handles an incoming CatalogDeletionRequest."""
    user = auth.get_current_identity().to_bytes()
    logging.info(
        'Received CatalogDeletionRequest:\nUser: %s\n%s',
        user,
        request,
    )
    error = self.check_backend(request)
    if error:
      return rpc_messages.CatalogManipulationResponse(error=error)

    if request.dimensions.hostname:
      error = self.delete_transactionally(request)
      if error:
        return rpc_messages.CatalogManipulationResponse(error=error)
    else:
      entries = models.CatalogEntry.query_by_exact_dimensions(
          request.dimensions
      )
      if not entries:
        logging.info('Catalog entry not found')
        return rpc_messages.CatalogManipulationResponse(
            error=rpc_messages.CatalogManipulationRequestError.ENTRY_NOT_FOUND
        )
      entries[0].key.delete()
    return rpc_messages.CatalogManipulationResponse()

  @ndb.transactional
  def delete_transactionally(self, request):
    """Transactionally deletes a CatalogEntry by backend and hostname.

    Returns:
      rpc_messages.CatalogManipulationRequestError.ENTRY_NOT_FOUND if a matching
      CatalogEntry doesn't exist, otherwise None.
    """
    assert request.dimensions.backend is not None
    assert request.dimensions.hostname is not None
    entry = models.CatalogEntry.get_by_id(
        models.CatalogEntry.compute_id(request.dimensions)
    )
    if not entry or not entry.has_exact_dimensions(request.dimensions):
      logging.info('Catalog entry not found')
      return rpc_messages.CatalogManipulationRequestError.ENTRY_NOT_FOUND
    entry.key.delete()


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
