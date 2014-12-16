# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Isolate Server frontend url handlers."""

import os
import re
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, 'third_party'))
sys.path.insert(0, os.path.join(BASE_DIR, 'components', 'third_party'))

import endpoints
from protorpc import message_types
from protorpc import messages
from protorpc import remote

from components import auth
from components import ereporter2
from handlers_api import MIN_SIZE_FOR_DIRECT_GS
from handlers_api import MIN_SIZE_FOR_GS
from handlers_api import PreUploadContentHandler
import model
import stats


### Request Types


class Digest(messages.Message):
  """ProtoRPC message containing digest information."""
  digest = messages.StringField(1)
  is_isolated = messages.BooleanField(2, default=False)
  size = messages.IntegerField(3)


class DigestCollection(messages.Message):
  """Endpoints request type analogous to the existing JSON post body."""
  items = messages.MessageField(Digest, 1, repeated=True)
  namespace = messages.StringField(4, default='default')
  digest_hash = messages.StringField(5, default='SHA-1')
  compression = messages.StringField(6, default='flate')


### Response Types


class UrlMessage(messages.Message):
  """Endpoints response type for a single URL or pair of URLs."""
  upload_url = messages.StringField(1, required=False)
  finalize_url = messages.StringField(2, required=False)


class UrlCollection(messages.Message):
  """Endpoints response type analogous to existing JSON response."""
  items = messages.MessageField(UrlMessage, 1, repeated=True)


### API


@auth.endpoints_api(name='isolateservice', version='v1')
class IsolateService(remote.Service):
  """Implement API methods corresponding to handlers in handlers_api."""

  @classmethod
  def check_entries_exist(cls, entries):
    """For now, just monkey patch into the existing handler.

    Arguments:
      entries: a DigestCollection to be posted

    Yields:
      entry_info, Boolean pairs, where Boolean indicates existence of the entry
    """
    for yielded_tuple in PreUploadContentHandler.check_entry_infos(
        entries, entries.namespace):
      yield yielded_tuple

  @classmethod
  def generate_urls(cls, _digest_element):
    """Generate upload and finalize URLs for a digest.

    Arguments:
      _digest_element: a single Digest from the request message

    Returns:
      a two-tuple of URL strings

    TODO(cmassaro): for real implement this
    """
    return (None, None)

  @auth.endpoints_method(DigestCollection, UrlCollection,
                         path='preupload', http_method='POST',
                         name='preuploader.preupload')
  def preupload(self, request):
    """Checks for entry's existence and generates upload URLs.

    Arguments:
      request: the DigestCollection to be posted

    Returns:
      the UrlCollection corresponding to the uploaded digests

    The response list is commensurate to the request's; each UrlMessage has
      * if an entry is missing: two URLs: the URL to upload a file
        to and the URL to call when the upload is done (can be null).
      * if the entry is already present: null URLs ('').

    UrlCollection([
        UrlMessage(
          upload_url = "<upload url>"
          finalize_url = "<finalize url>"
          )
        UrlMessage(
          upload_url = '')
        ...
        ])
    """
    response = UrlCollection(items=[])

    # check for namespace error
    if not re.match(r'^%s$' % model.NAMESPACE_RE, request.namespace):
      raise endpoints.BadRequestException(
          'Invalid namespace; allowed keys must pass regexp "%s"' %
          model.NAMESPACE_RE)
    for digest_element in request.items:
      # check for error conditions
      if not model.is_valid_hex(digest_element.digest):
        raise endpoints.BadRequestException('Invalid hex code: %s' %
                                            (digest_element.digest))

      # generate and append new URLs
      upload_url, finalize_url = self.generate_urls(digest_element)
      response.items.append(UrlMessage(upload_url=upload_url,
                                       finalize_url=finalize_url))

    # all is well; return the UrlCollection
    return response


def create_application():
  ereporter2.register_formatter()
  return endpoints.api_server([IsolateService])


app = create_application()
