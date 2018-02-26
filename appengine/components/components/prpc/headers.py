# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import re

from components.prpc import encoding


def _parse_media_type(media_type):
  if media_type is None:
    return encoding.Encoding.BINARY
  if media_type == 'application/prpc; encoding=binary':
    return encoding.Encoding.BINARY
  if media_type == 'application/prpc; encoding=json':
    return encoding.Encoding.JSON
  if media_type == 'application/json':
    return encoding.Encoding.JSON
  if media_type == 'application/prpc; encoding=text':
    return encoding.Encoding.TEXT
  raise ValueError('Invalid media type "%s"' % media_type)


def _parse_accept_header(value):
  # TODO(nodir,mknyszek): Correctly parse Accept header according to
  # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
  # (e.g. list multiple acceptable types, or have quality factors).
  if not value or value == '*/*':
    return encoding.Encoding.BINARY
  try:
    return _parse_media_type(value)
  except ValueError:
    raise ValueError('unsupported Accept header %r' % value)


def _parse_timeout(timeout):
  if timeout is None:
    return None
  header_re = r'^(?P<amount>\d+)(?P<units>[HMSmun])$'
  m = re.match(header_re, timeout)
  if m is None:
    raise ValueError('Incorrectly formatted timeout header')
  unit = m.group('units')
  if unit == 'H':
    multiplier = 60*60
  elif unit == 'M':
    multiplier = 60
  elif unit == 'S':
    multiplier = 1
  elif unit == 'm':
    multiplier = 0.001
  elif unit == 'u':
    multiplier = 1e-6
  elif unit == 'n':
    multiplier = 1e-9
  else:
    raise ValueError('Incorrectly formatted timeout header')
  seconds = int(m.group('amount')) * multiplier
  return seconds


def process_headers(context, headers):
  """Parses headers and sets up the context object.

  Args:
    context: a context.ServicerContext, which represents a handler's execution
        context.
    headers: the self.request.headers dictionary-like object from a
        webapp2.RequestHandler.

  Returns:
    content_type: an encoding.Encoding enum value for the incoming request.
    accept: an encoding.Encoding enum value for the outgoing response.

  Raises:
    ValueError: when the headers indicate invalid content types or don't parse.
  """

  content_type_header = headers.get('Content-Type')
  try:
    content_type = _parse_media_type(content_type_header)
  except ValueError:
    # TODO(nodir,mknyszek): Figure out why the development server is getting
    # the header with an underscore instead of a hyphen for some requests.
    content_type_header = headers.get('Content_Type')
    if content_type_header:
      content_type = _parse_media_type(content_type_header)
    else:
      raise

  accept = _parse_accept_header(headers.get('Accept'))
  timeout_header = headers.get('X-Prpc-Timeout')
  context.timeout = _parse_timeout(timeout_header)

  for header, value in headers.iteritems():
    header = header.lower()
    if header.startswith('x-prpc-'):
      continue
    if header.endswith('-bin'):
      try:
        value = base64.b64decode(value)
      except TypeError:
        raise ValueError('Received invalid base64 string in header %s' % header)
      header = header[:-len('-bin')]
    context.invocation_metadata.append((header, value))

  return content_type, accept
