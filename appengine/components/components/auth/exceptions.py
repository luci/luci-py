# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Token creation exceptions."""

__all__ = [
  "BadTokenError",
  "TransientError",
  "TokenCreationError",
  "TokenAuthorizationError",
  "NotFoundError",
]

class BadTokenError(Exception):
  """Raised on fatal errors (like bad signature). Results in 403 HTTP code."""

class TransientError(Exception):
  """Raised on errors that can go away with retry. Results in 500 HTTP code."""

class TokenCreationError(Exception):
  """Raised on token creation errors."""

class TokenAuthorizationError(TokenCreationError):
  """Raised on authorization error during token creation."""

class NotFoundError(TokenCreationError):
  """Raised when a something required to issue a token was missing."""
