# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Cloud endpoints for the Machine Provider API."""

import hashlib
import json
import logging

import endpoints
from google.appengine import runtime
from google.appengine.api import app_identity
from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from protorpc import protobuf
from protorpc import remote

from components import auth
from components import pubsub
from components import utils
from components.machine_provider import rpc_messages

import acl
import models


PUBSUB_DEFAULT_PROJECT = app_identity.get_application_id()


@auth.endpoints_api(name='catalog', version='v1')
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
      return rpc_messages.CatalogManipulationResponse(
          error=error,
          machine_addition_request=request,
      )
    if not pubsub.validate_topic(request.policies.backend_topic):
      logging.warning(
          'Invalid topic for Cloud Pub/Sub: %s',
          request.policies.backend_topic,
      )
      return rpc_messages.CatalogManipulationResponse(
          error=rpc_messages.CatalogManipulationRequestError.INVALID_TOPIC,
          machine_addition_request=request,
      )
    if not request.policies.backend_project:
      logging.info(
          'Cloud Pub/Sub project unspecified, using default: %s',
          PUBSUB_DEFAULT_PROJECT,
      )
      request.policies.backend_project = PUBSUB_DEFAULT_PROJECT
    if request.policies.backend_project:
      error = None
      if not pubsub.validate_project(request.policies.backend_project):
        logging.warning(
            'Invalid project for Cloud Pub/Sub: %s',
            request.policies.backend_project,
        )
        error = rpc_messages.CatalogManipulationRequestError.INVALID_PROJECT
      elif not request.policies.backend_topic:
        logging.warning(
            'Cloud Pub/Sub project specified without specifying topic: %s',
            request.policies.backend_project,
        )
        error = rpc_messages.CatalogManipulationRequestError.UNSPECIFIED_TOPIC
      if error:
        return rpc_messages.CatalogManipulationResponse(
            error=error, machine_addition_request=request)
    return self._add_machine(request)

  @auth.endpoints_method(
      rpc_messages.CatalogMachineBatchAdditionRequest,
      rpc_messages.CatalogBatchManipulationResponse,
  )
  @auth.require(acl.is_backend_service_or_catalog_admin)
  def add_machines(self, request):
    """Handles an incoming CatalogMachineBatchAdditionRequest.

    Batches are intended to save on RPCs only. The batched requests will not
    execute transactionally.
    """
    user = auth.get_current_identity().to_bytes()
    logging.info(
        'Received CatalogMachineBatchAdditionRequest:\nUser: %s\n%s',
        user,
        request,
    )
    responses = []
    for request in request.requests:
      logging.info(
          'Processing CatalogMachineAdditionRequest:\n%s',
          request,
      )
      error = self.check_backend(request) or self.check_hostname(request)
      if error:
        responses.append(rpc_messages.CatalogManipulationResponse(
            error=error,
            machine_addition_request=request,
        ))
      else:
        responses.append(self._add_machine(request))
    return rpc_messages.CatalogBatchManipulationResponse(responses=responses)

  @ndb.transactional
  def _add_machine(self, request):
    """Handles datastore operations for CatalogMachineAdditionRequests."""
    entry = models.CatalogMachineEntry.generate_key(request.dimensions).get()
    if entry:
      # Enforces per-backend hostname uniqueness.
      logging.warning('Hostname reuse:\nOriginally used for: \n%s', entry)
      return rpc_messages.CatalogManipulationResponse(
          error=rpc_messages.CatalogManipulationRequestError.HOSTNAME_REUSE,
          machine_addition_request=request,
      )
    models.CatalogMachineEntry(
        key=models.CatalogMachineEntry.generate_key(request.dimensions),
        dimensions=request.dimensions,
        pubsub_subscription_project=PUBSUB_DEFAULT_PROJECT,
        pubsub_topic_project=PUBSUB_DEFAULT_PROJECT,
        policies=request.policies,
        state=models.CatalogMachineEntryStates.NEW,
    ).put()
    return rpc_messages.CatalogManipulationResponse(
        machine_addition_request=request,
    )

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
      return rpc_messages.CatalogManipulationResponse(
          error=error,
          machine_deletion_request=request,
      )
    return self._delete_machine(request)

  @ndb.transactional
  def _delete_machine(self, request):
    """Handles datastore operations for CatalogMachineDeletionRequests."""
    entry = models.CatalogMachineEntry.generate_key(request.dimensions).get()
    if not entry:
      logging.info('Catalog entry not found')
      return rpc_messages.CatalogManipulationResponse(
          error=rpc_messages.CatalogManipulationRequestError.ENTRY_NOT_FOUND,
          machine_deletion_request=request,
      )
    entry.key.delete()
    return rpc_messages.CatalogManipulationResponse(
        machine_deletion_request=request,
    )

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
      return rpc_messages.CatalogManipulationResponse(
          capacity_modification_request=request,
          error=error,
      )
    return self._modify_capacity(request)

  @ndb.transactional
  def _modify_capacity(self, request):
    """Handles datastore operations for CatalogCapacityModificationRequests."""
    models.CatalogCapacityEntry.create_and_put(
        request.dimensions,
        request.count,
    )
    return rpc_messages.CatalogManipulationResponse(
        capacity_modification_request=request,
    )


@auth.endpoints_api(name='machine_provider', version='v1')
class MachineProviderEndpoints(remote.Service):
  """Implements cloud endpoints for the Machine Provider."""

  @auth.endpoints_method(
      rpc_messages.BatchedLeaseRequest,
      rpc_messages.BatchedLeaseResponse,
  )
  @auth.require(acl.can_issue_lease_requests)
  def batched_lease(self, request):
    """Handles an incoming BatchedLeaseRequest.

    Batches are intended to save on RPCs only. The batched requests will not
    execute transactionally.
    """
    # To avoid having large batches timed out by AppEngine after 60 seconds
    # when some requests have been processed and others haven't, enforce a
    # smaller deadline on ourselves to process the entire batch.
    DEADLINE_SECS = 30
    start_time = utils.utcnow()
    user = auth.get_current_identity().to_bytes()
    logging.info('Received BatchedLeaseRequest:\nUser: %s\n%s', user, request)
    responses = []
    for request in request.requests:
      request_hash = models.LeaseRequest.generate_key(user, request).id()
      logging.info(
          'Processing LeaseRequest:\nRequest hash: %s\n%s',
          request_hash,
          request,
      )
      if (utils.utcnow() - start_time).seconds > DEADLINE_SECS:
        logging.warning(
          'BatchedLeaseRequest exceeded enforced deadline: %s', DEADLINE_SECS)
        responses.append(rpc_messages.LeaseResponse(
            client_request_id=request.request_id,
            error=rpc_messages.LeaseRequestError.DEADLINE_EXCEEDED,
            request_hash=request_hash,
        ))
      else:
        try:
          responses.append(self._lease(request, user, request_hash))
        except (
            datastore_errors.NotSavedError,
            datastore_errors.Timeout,
            runtime.apiproxy_errors.CancelledError,
            runtime.apiproxy_errors.DeadlineExceededError,
            runtime.apiproxy_errors.OverQuotaError,
        ) as e:
          logging.warning('Exception processing LeaseRequest:\n%s', e)
          responses.append(rpc_messages.LeaseResponse(
              client_request_id=request.request_id,
              error=rpc_messages.LeaseRequestError.TRANSIENT_ERROR,
              request_hash=request_hash,
          ))
    return rpc_messages.BatchedLeaseResponse(responses=responses)

  @auth.endpoints_method(rpc_messages.LeaseRequest, rpc_messages.LeaseResponse)
  @auth.require(acl.can_issue_lease_requests)
  def lease(self, request):
    """Handles an incoming LeaseRequest."""
    # Hash the combination of client + client-generated request ID in order to
    # deduplicate responses on a per-client basis.
    user = auth.get_current_identity().to_bytes()
    request_hash = models.LeaseRequest.generate_key(user, request).id()
    logging.info(
        'Received LeaseRequest:\nUser: %s\nRequest hash: %s\n%s',
        user,
        request_hash,
        request,
    )
    return self._lease(request, user, request_hash)

  def _lease(self, request, user, request_hash):
    """Handles an incoming LeaseRequest."""
    if request.pubsub_topic:
      if not pubsub.validate_topic(request.pubsub_topic):
        logging.warning(
            'Invalid topic for Cloud Pub/Sub: %s',
            request.pubsub_topic,
        )
        return rpc_messages.LeaseResponse(
            client_request_id=request.request_id,
            error=rpc_messages.LeaseRequestError.INVALID_TOPIC,
        )
      if not request.pubsub_project:
        logging.info(
            'Cloud Pub/Sub project unspecified, using default: %s',
            PUBSUB_DEFAULT_PROJECT,
        )
        request.pubsub_project = PUBSUB_DEFAULT_PROJECT
    if request.pubsub_project:
      if not pubsub.validate_project(request.pubsub_project):
        logging.warning(
            'Invalid project for Cloud Pub/Sub: %s',
            request.pubsub_topic,
        )
        return rpc_messages.LeaseResponse(
            client_request_id=request.request_id,
            error=rpc_messages.LeaseRequestError.INVALID_PROJECT,
        )
      elif not request.pubsub_topic:
        logging.warning(
            'Cloud Pub/Sub project specified without specifying topic: %s',
            request.pubsub_project,
        )
        return rpc_messages.LeaseResponse(
            client_request_id=request.request_id,
            error=rpc_messages.LeaseRequestError.UNSPECIFIED_TOPIC,
        )
    duplicate = models.LeaseRequest.get_by_id(request_hash)
    deduplication_checksum = models.LeaseRequest.compute_deduplication_checksum(
        request,
    )
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
            client_request_id=request.request_id,
            error=rpc_messages.LeaseRequestError.REQUEST_ID_REUSE,
        )
    else:
      logging.info('Storing LeaseRequest')
      response = rpc_messages.LeaseResponse(
          client_request_id=request.request_id,
          request_hash=request_hash,
          state=rpc_messages.LeaseRequestState.UNTRIAGED,
      )
      models.LeaseRequest(
          deduplication_checksum=deduplication_checksum,
          id=request_hash,
          owner=auth.get_current_identity(),
          request=request,
          response=response,
      ).put()
      logging.info('Sending LeaseResponse:\n%s', response)
      return response

  @auth.endpoints_method(
      rpc_messages.BatchedLeaseReleaseRequest,
      rpc_messages.BatchedLeaseReleaseResponse,
  )
  @auth.require(acl.can_issue_lease_requests)
  def batched_release(self, request):
    """Handles an incoming BatchedLeaseReleaseRequest.

    Batches are intended to save on RPCs only. The batched requests will not
    execute transactionally.
    """
    # TODO(smut): Dedupe common logic in batched RPC handling.
    DEADLINE_SECS = 30
    start_time = utils.utcnow()
    user = auth.get_current_identity().to_bytes()
    logging.info(
        'Received BatchedLeaseReleaseRequest:\nUser: %s\n%s', user, request)
    responses = []
    for request in request.requests:
      request_hash = models.LeaseRequest.generate_key(user, request).id()
      logging.info(
          'Processing LeaseReleaseRequest:\nRequest hash: %s\n%s',
          request_hash,
          request,
      )
      if (utils.utcnow() - start_time).seconds > DEADLINE_SECS:
        logging.warning(
          'BatchedLeaseReleaseRequest exceeded enforced deadline: %s',
          DEADLINE_SECS,
        )
        responses.append(rpc_messages.LeaseReleaseResponse(
            client_request_id=request.request_id,
            error=rpc_messages.LeaseReleaseRequestError.DEADLINE_EXCEEDED,
            request_hash=request_hash,
        ))
      else:
        try:
          responses.append(rpc_messages.LeaseReleaseResponse(
              client_request_id=request.request_id,
              error=self._release(request_hash),
              request_hash=request_hash,
          ))
        except (
            datastore_errors.NotSavedError,
            datastore_errors.Timeout,
            runtime.apiproxy_errors.CancelledError,
            runtime.apiproxy_errors.DeadlineExceededError,
            runtime.apiproxy_errors.OverQuotaError,
        ) as e:
          logging.warning('Exception processing LeaseReleaseRequest:\n%s', e)
          responses.append(rpc_messages.LeaseReleaseResponse(
              client_request_id=request.request_id,
              error=rpc_messages.LeaseReleaseRequestError.TRANSIENT_ERROR,
              request_hash=request_hash,
          ))
    return rpc_messages.BatchedLeaseReleaseResponse(responses=responses)

  @auth.endpoints_method(
      rpc_messages.LeaseReleaseRequest, rpc_messages.LeaseReleaseResponse)
  @auth.require(acl.can_issue_lease_requests)
  def release(self, request):
    """Handles an incoming LeaseReleaseRequest."""
    user = auth.get_current_identity().to_bytes()
    request_hash = models.LeaseRequest.generate_key(user, request).id()
    logging.info(
        'Received LeaseReleaseRequest:\nUser: %s\nLeaseRequest: %s\n%s',
        user,
        request_hash,
        request,
    )
    return rpc_messages.LeaseReleaseResponse(
        client_request_id=request.request_id,
        error=self._release(request_hash),
        request_hash=request_hash,
    )

  @staticmethod
  @ndb.transactional
  def _release(request_hash):
    """Releases a LeaseRequest.

    Args:
      request_hash: ID of a models.LeaseRequest entity in the datastore to
        release.

    Returns:
      rpc_messages.LeaseReleaseRequestError indicating an error that occurred,
      or None if there was no error and the lease was released successfully.
    """
    request = ndb.Key(models.LeaseRequest, request_hash).get()
    if not request:
      logging.warning(
          'LeaseReleaseRequest referred to non-existent LeaseRequest: %s',
          request_hash,
      )
      return rpc_messages.LeaseReleaseRequestError.NOT_FOUND
    if request.response.state != rpc_messages.LeaseRequestState.FULFILLED:
      logging.warning(
          'LeaseReleaseRequest referred to unfulfilled LeaseRequest: %s',
          request_hash,
      )
      return rpc_messages.LeaseReleaseRequestError.NOT_FULFILLED
      # TODO(smut): Cancel the request.
    if not request.machine_id:
      logging.warning(
          'LeaseReleaseRequest referred to already reclaimed LeaseRequest: %s',
          request_hash,
      )
      return rpc_messages.LeaseReleaseRequestError.ALREADY_RECLAIMED
    logging.info('Releasing LeaseRequest: %s', request_hash)
    request.released = True
    request.put()


def create_endpoints_app():
  return endpoints.api_server([CatalogEndpoints, MachineProviderEndpoints])
