# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Fast(er) base64 urlsafe encoding/decoding for internal use in auth component.

Borrowed from ndb's key.py, where it is claimed they are 3-4x faster than
urlsafe_b64....
"""

import base64


def encode(data):
  """Bytes str -> URL safe base64 with stripped '='."""
  if not isinstance(data, str):
    raise TypeError('Expecting str with binary data')
  urlsafe = base64.b64encode(data)
  return urlsafe.rstrip('=').replace('+', '-').replace('/', '_')


def decode(data):
  """URL safe base64 with stripped '=' -> bytes str."""
  if not isinstance(data, str):
    raise TypeError('Expecting str with base64 data')
  mod = len(data) % 4
  if mod:
    data += '=' * (4 - mod)
  return base64.b64decode(data.replace('-', '+').replace('_', '/'))
