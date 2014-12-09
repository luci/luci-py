# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Isolate Server frontend url handlers."""

import endpoints
from protorpc import message_types
from protorpc import messages
from protorpc import remote

from components import auth
from handlers_api import MIN_SIZE_FOR_DIRECT_GS
from handlers_api import MIN_SIZE_FOR_GS


### Request Types


class Digest(messages.Message):
  """Abstraction for a single element of the JSON post body list."""
  digest = messages.StringField(1)
  is_isolated = messages.BoolField(2)
  size = messages.IntField(3)


class DigestCollection(messages.Message):
  """Endpoints request type analogous to the existing JSON post body."""
  items = messages.MessageField(Digest, 1, repeated=True)


### Response Types


class UrlMessage(messages.Message):
  """Endpoints response type for a single URL or pair of URLs."""
  upload_url = messages.StringField(1, required=False)
  finalize_url = messages.StringField(2, required=False)


class UrlCollection(messages.Message):
  """Endpoints response type analogous to existing JSON response."""
  items = messages.MessageField(UrlMessage, 1, repeated=True)


### Utility


@auth.endpoints_api(name='isolateservice', version='v1')
class IsolateService(remote.Service):
  """Base class for handlers; implements authentication functionality.

  TODO(cmassaro): this should eventually be equipollent to the similarly-named
  handler in handlers_api.py.
  """

  @auth.endpoints_method(DigestCollection, UrlCollection,
                         path='preupload', http_method='POST',
                         name='digests.preupload')
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

    pass

