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
        response = rpc_messages.LeaseResponse()
        response.error = rpc_messages.LeaseRequestError.REQUEST_ID_REUSE
        return response
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
  return endpoints.api_server([MachineProviderEndpoints])
