# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Implements authentication based on LUCI machine tokens.

LUCI machine tokens are short lived signed protobuf blobs that (among other
information) contain machines' FQDNs.

Each machine has a TLS certificate (and corresponding private key) it uses
to authenticate to LUCI token server when periodically refreshing machine
tokens. Other LUCI backends then simply verifies that the short lived machine
token was signed by the trusted LUCI token server key. That way all the
complexities of dealing with PKI (like checking for certificate revocation) are
implemented in a dedicated LUCI token server, not in the each individual
service.

See:
  * https://github.com/luci/luci-go/tree/master/appengine/cmd/tokenserver
  * https://github.com/luci/luci-go/tree/master/client/cmd/luci_machine_tokend
"""


def machine_authentication(request):  # pylint: disable=unused-argument
  """Implementation of the machine authentication.

  See components.auth.handler.AuthenticatingHandler.get_auth_methods for details
  of the expected interface.

  Args:
    request: webapp2.Request with the incoming request.

  Returns:
    auth.Identity with machine ID ("bot:<fqdn>") on success or None if there's
    no machine token header (which means this authentication method is not
    applicable).

  Raises:
    auth.AuthenticationError is machine token header is present, but the token
    is invalid.
  """
  # TODO(vadimsh): Implement. For now just do nothing, so that components.auth
  # falls back to IP-whitelist based authentication.
  return None
