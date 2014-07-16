# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Replica side of Primary <-> Replica protocol."""

from google.appengine.api import app_identity
from google.appengine.api import urlfetch

from components import utils

from . import model
from . import tokens
from .proto import replication_pb2


# Messages for error codes in ServiceLinkResponse.
LINKING_ERRORS = {
  replication_pb2.ServiceLinkResponse.TRANSPORT_ERROR: 'Transport error,',
  replication_pb2.ServiceLinkResponse.BAD_TICKET: 'The link has expired.',
  replication_pb2.ServiceLinkResponse.AUTH_ERROR: 'Authentication error.',
}


class ProtocolError(Exception):
  """Raised when request to primary fails."""
  def __init__(self, status_code, msg):
    super(ProtocolError, self).__init__(msg)
    self.status_code = status_code


def decode_link_ticket(encoded):
  """Returns replication_pb2.ServiceLinkTicket given base64 encoded blob."""
  return replication_pb2.ServiceLinkTicket.FromString(
      tokens.base64_decode(encoded))


def become_replica(ticket, initiated_by):
  """Converts current service to a replica of a primary specified in a ticket.

  Args:
    ticket: replication_pb2.ServiceLinkTicket passed from a primary.
    initiated_by: Identity of a user that accepted linking request, for logging.

  Raises:
    ProtocolError in case the request to primary fails.
  """
  assert model.is_standalone()

  # On dev appserver emulate X-Appengine-Inbound-Appid header.
  headers = {'Content-Type': 'application/octet-stream'}
  protocol = 'https'
  if utils.is_local_dev_server():
    headers['X-Appengine-Inbound-Appid'] = app_identity.get_application_id()
    protocol = 'http'

  # Pass back the ticket for primary to verify it, tell the primary to use
  # default version hostname to talk to us.
  link_request = replication_pb2.ServiceLinkRequest()
  link_request.ticket = ticket.ticket
  link_request.replica_url = (
      '%s://%s' % (protocol, app_identity.get_default_version_hostname()))
  link_request.initiated_by = initiated_by.to_bytes()

  # Primary will look at X-Appengine-Inbound-Appid and compare it to what's in
  # the ticket.
  try:
    result = urlfetch.fetch(
        url='%s/auth_service/api/v1/internal/link_replica' % ticket.primary_url,
        payload=link_request.SerializeToString(),
        method='POST',
        headers=headers,
        follow_redirects=False,
        deadline=30,
        validate_certificate=True)
  except urlfetch.Error as exc:
    raise ProtocolError(
        replication_pb2.ServiceLinkResponse.TRANSPORT_ERROR,
        'URLFetch error (%s): %s' % (exc.__class__.__name__, exc))

  # Protobuf based protocol is not using HTTP codes (handler always replies with
  # HTTP 200, providing error details if needed in protobuf serialized body).
  # So any other status code here means there was a transport level error.
  if result.status_code != 200:
    raise ProtocolError(
        replication_pb2.ServiceLinkResponse.TRANSPORT_ERROR,
        'Request to the primary failed with HTTP %d.' % result.status_code)

  link_response = replication_pb2.ServiceLinkResponse.FromString(result.content)
  if link_response.status != replication_pb2.ServiceLinkResponse.SUCCESS:
    message = LINKING_ERRORS.get(
        link_response.status,
        'Request to the primary failed with status %d.' % link_response.status)
    raise ProtocolError(link_response.status, message)

  # TODO(vadimsh): Enable this once replication mechanism is implemented.
  # Become replica. Auth DB will be overwritten on a first push from Primary.
  # state = model.AuthReplicationState(
  #     key=model.REPLICATION_STATE_KEY,
  #     primary_id=ticket.primary_id,
  #     primary_url=ticket.primary_url)
  # state.put()
