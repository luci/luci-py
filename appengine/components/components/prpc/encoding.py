# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import logging
import six

from google.protobuf import json_format, text_format


_BASE64_ENCODING_ERROR = TypeError
if six.PY3:
  import binascii
  _BASE64_ENCODING_ERROR = binascii.Error


class Encoding(object):
  BINARY = (0, 'application/prpc; encoding=binary')
  JSON   = (1, 'application/json')
  TEXT   = (2, 'application/prpc; encoding=text')

  @staticmethod
  def media_type(encoding):
    return encoding[1]


def get_decoder(encoding):
  """Returns the appropriate decoder for content type.

  Args:
    encoding: A value from the Encoding enum.

  Returns:
    a callable which takes an encoded string and an empty protobuf message, and
        populates the given protobuf with data from the string. Each decoder
        may raise exceptions of its own based on incorrectly formatted data.
  """
  if encoding == Encoding.BINARY:
    return lambda string, proto: proto.ParseFromString(string)
  if encoding == Encoding.JSON:
    return json_format.Parse
  if encoding == Encoding.TEXT:
    return text_format.Merge
  assert False, 'Argument |encoding| was not a value of the Encoding enum.'


def get_encoder(encoding):
  """Returns the appropriate encoder for the Accept content type.

  Args:
    encoding: A value from the Encoding enum.

  Returns:
    a callable which takes an initialized protobuf message, and returns a string
        representing its data. Each encoder may raise exceptions of its own.
  """
  if encoding == Encoding.BINARY:
    return lambda proto: proto.SerializeToString()
  if encoding == Encoding.JSON:
    return lambda proto: ')]}\'\n' + json_format.MessageToJson(proto)
  if encoding == Encoding.TEXT:
    return lambda proto: text_format.MessageToString(proto, as_utf8=True)
  assert False, 'Argument |encoding| was not a value of the Encoding enum.'


def encode_bin_metadata(mutable_metadata_dict):
  """Encodes values to base64 if their key ends with the `-bin` suffix.

  This works regardless of the casing of the key, and performs any changes
  in-place.
  """
  for k, v in mutable_metadata_dict.iteritems():
    if k.lower().endswith('-bin'):
      mutable_metadata_dict[k] = base64.b64encode(v)


def decode_bin_metadata(mutable_metadata_dict):
  """Decodes values from base64 if their key ends with the `-bin` suffix.

  This works regardless of the casing of the key, and performs any changes
  in-place.
  """
  for k in (mutable_metadata_dict or {}):
    if not k.lower().endswith('-bin'):
      continue
    try:
      mutable_metadata_dict[k] = base64.b64decode(mutable_metadata_dict[k])
    except _BASE64_ENCODING_ERROR:
      raise ValueError('Metadata key %s not base64 encoded, val: %s' %
                       (k, mutable_metadata_dict[k]))
