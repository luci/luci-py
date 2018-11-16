# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Defines high level authorization function called for all incoming requests.

Lives in its own module to avoid introducing module dependency cycle between
api.py and delegation.py.
"""

from . import api
from . import delegation
from . import model


def check_request(
    ctx, peer_identity, peer_ip, auth_details,
    delegation_token, use_bots_ip_whitelist):
  """Prepares the request context, checking IP whitelist and delegation token.

  This is intended to be called by request processing middlewares right after
  they have authenticated the peer, and before they dispatch the request to the
  actual handler.

  It checks IP whitelist, and delegation token, and updates the request auth
  context accordingly, populating peer_identity, peer_ip, current_identity and
  auth_details fields.

  Args:
    ctx: instance of api.RequestCache to update.
    peer_identity: caller's identity.
    peer_ip: instance of ipaddr.IP.
    auth_details: api.AuthDetails tuple (or None) with additional auth info.
    delegation_token: the token from X-Delegation-Token-V1 header.
    use_bots_ip_whitelist: [DEPRECATED] if true, treat anonymous access from
      IPs in "<appid>-bots" whitelist as coming from "bot:whitelisted-ip"
      identity.

  Raises:
    api.AuthorizationError if identity has an IP whitelist assigned and given IP
    address doesn't belong to it.
    delegation.TransientError if there was a transient error checking the token.
  """
  auth_db = ctx.auth_db

  # Hack to allow pure IP-whitelist based authentication for bots, until they
  # are switched to use something better.
  #
  # TODO(vadimsh): Get rid of this. Blocked on killing IP whitelisted access
  # from Chrome Buildbot machines.
  if (use_bots_ip_whitelist and peer_identity.is_anonymous and
      auth_db.is_in_ip_whitelist(model.bots_ip_whitelist(), peer_ip, False)):
      peer_identity = model.IP_WHITELISTED_BOT_ID

  # Note: populating fields early is useful, since exception handlers may use
  # them for logging.
  ctx.peer_ip = peer_ip
  ctx.peer_identity = peer_identity

  # Verify the caller is allowed to make calls from the given IP. It raises
  # AuthorizationError if IP is not allowed.
  auth_db.verify_ip_whitelisted(peer_identity, peer_ip)

  # Parse the delegation token, if given, to deduce end-user identity. We clear
  # auth_details if the delegation is used, since it no longer applies to
  # the delegated identity.
  if delegation_token:
    try:
      ident, unwrapped_tok = delegation.check_bearer_delegation_token(
          delegation_token, peer_identity, auth_db)
      ctx.current_identity = ident
      ctx.delegation_token = unwrapped_tok
      ctx.auth_details = None
    except delegation.BadTokenError as exc:
      raise api.AuthorizationError('Bad delegation token: %s' % exc)
  else:
    ctx.current_identity = ctx.peer_identity
    ctx.auth_details = auth_details
