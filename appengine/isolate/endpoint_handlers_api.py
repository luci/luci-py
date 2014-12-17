# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Isolate Server frontend url handlers."""

import binascii
import datetime
import os
import re
import sys
import time
import urllib

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, 'third_party'))
sys.path.insert(0, os.path.join(BASE_DIR, 'components', 'third_party'))

import endpoints
from protorpc import message_types
from protorpc import messages
from protorpc import remote

from components import auth
from components import ereporter2
from components import utils
import config
import gcs
from handlers_api import MIN_SIZE_FOR_DIRECT_GS
from handlers_api import MIN_SIZE_FOR_GS
from handlers_api import StoreContentHandler
import model
import stats

from google.appengine.ext import ndb


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

  # Default expiration time for signed links.
  DEFAULT_LINK_EXPIRATION = datetime.timedelta(hours=4)

  _gs_url_signer = None

  @classmethod
  def check_entries_exist(cls, entries):
    """For now, just monkey patch into the existing handler.

    Arguments:
      entries: a DigestCollection to be posted

    Yields:
      digest, Boolean pairs, where Boolean indicates existence of the entry

    Raises:
      BadRequestException if any digest is not a valid hexadecimal number.
    """
    # Kick off all queries in parallel. Build mapping Future -> digest.
    futures = {}
    for digest in entries.items:
      # check for error conditions
      try:
        key = model.entry_key(entries.namespace.encode('utf-8'),
                              digest.digest.encode('utf-8'))
      except (AssertionError, ValueError) as error:
        raise endpoints.BadRequestException(error.message)
      else:
        futures[key.get_async(use_cache=False)] = digest

    # Pick first one that finishes and yield it, rinse, repeat.
    while futures:
      future = ndb.Future.wait_any(futures)
      # TODO(maruel): For items that were present, make sure
      # future.get_result().compressed_size == digest.size.
      yield futures.pop(future), bool(future.get_result())

  @classmethod
  def partition_collection(cls, entries):
    """Create DigestCollections for existent and new digests."""
    seen_unseen = [DigestCollection(
        items=[], namespace=entries.namespace, digest_hash=entries.digest_hash,
        compression=entries.compression) for _ in range(2)]
    for digest, exists in cls.check_entries_exist(entries):
      seen_unseen[exists].items.append(digest)
    return seen_unseen

  @classmethod
  def should_push_to_gs(cls, digest):
    """True to direct client to upload given EntryInfo directly to GS."""
    # Relatively small *.isolated files go through app engine to cache them.
    if digest.isolated and digest.size <= model.MAX_MEMCACHE_ISOLATED:
      return False
    # All other large enough files go through GS.
    return digest.size >= MIN_SIZE_FOR_DIRECT_GS

  @property
  def gs_url_signer(self):
    """On demand instance of CloudStorageURLSigner object."""
    if not self._gs_url_signer:
      settings = config.settings()
      self._gs_url_signer = gcs.URLSigner(
          settings.gs_bucket,
          settings.gs_client_id_email,
          settings.gs_private_key)
    return self._gs_url_signer

  def uri_for(self, *args, **kwargs):
    """Fake version for now."""
    return auth.AuthenticatingHandler.uri_for(*args, **kwargs)

  def generate_store_url(self, digest, namespace, http_verb,
                         uploaded_to_gs, expiration):
    """Generates a signed URL to /content-gs/store method.

    Arguments:
      digest: a Digest instance
      namespace: the original request's namespace
      http_verb: the HTTP API method to be called (GET, POST, ...)
      uploaded_to_gs: whether the digest is already uploaded
      expiration: absolute specification of timeout

    Returns:
      the URL base with parameters as a string
    """
    # Data that goes into request parameters and signature.
    expiration_ts = str(int(time.time() + expiration.total_seconds()))
    item_size = str(digest.size)
    is_isolated = str(int(digest.isolated))
    uploaded_to_gs = str(int(uploaded_to_gs))

    # Generate signature.
    sig = StoreContentHandler.generate_signature(
        config.settings().global_secret, http_verb, expiration_ts, namespace,
        digest.digest, item_size, is_isolated, uploaded_to_gs)

    # Bare full URL to /content-gs/store endpoint.
    url_base = self.uri_for(
        'store-gs', namespace=namespace, hash_key=digest.digest, _full=True)

    # Construct url with query parameters, reuse auth token.
    params = {
        'g': uploaded_to_gs,
        'i': is_isolated,
        's': item_size,
        'sig': sig,
        'token': self.request.get('token'),
        'x': expiration_ts,
    }
    return '%s?%s' % (url_base, urllib.urlencode(params))

  def generate_push_urls(self, digest, namespace):
    """Generates a pair of URLs to be used by clients to upload an item.

    The GS filename is exactly ContentEntry.key.id().

    URL's being generated are 'upload URL' and 'finalize URL'. Client uploads
    an item to upload URL (via PUT request) and then POST status of the upload
    to a finalize URL.

    Finalize URL may be optional (it's None in that case).

    Arguments:
      digest: a Digest instance
      namespace: the original request's namespace

    Returns:
      a pair of store URLs (may be None)
    """
    if self.should_push_to_gs(digest):
      # Store larger stuff in Google Storage.
      key = model.entry_key(namespace.encode('utf-8'),
                            digest.digest.encode('utf-8'))
      upload_url = self.gs_url_signer.get_upload_url(
          filename=key.id(),
          content_type='application/octet-stream',
          expiration=self.DEFAULT_LINK_EXPIRATION)
      finalize_url = self.generate_store_url(
          digest, namespace,
          http_verb='POST',
          uploaded_to_gs=True,
          expiration=self.DEFAULT_LINK_EXPIRATION)
    else:
      # Store smallish entries and *.isolated in Datastore directly.
      upload_url = self.generate_store_url(
          digest, namespace,
          http_verb='PUT',
          uploaded_to_gs=False,
          expiration=self.DEFAULT_LINK_EXPIRATION)
      finalize_url = None
    return upload_url, finalize_url

  @classmethod
  def generate_urls(cls, _digest_element):
    """Generate upload and finalize URLs for a digest.

    Arguments:
      _digest_element: a single Digest from the request message

    Returns:
      a two-tuple of URL strings

    TODO(cmassaro): phase this out once generate_push_urls is working
    """
    return (None, None)

  @classmethod
  def tag_existing(cls, collection):
    """Tag existing digests with new timestamp.

    TODO(cmassaro): make sure that taskqueue will interact properly

    Arguments:
      collection: a DigestCollection containing existing digests

    Returns:
      the enqueued task if there were existing entries; None otherwise
    """
    if collection.items:
      url = '/internal/taskqueue/tag/%s/%s' % (
          collection.namespace, utils.datetime_to_timestamp(utils.utcnow()))
      payload = ''.join(binascii.unhexlify(digest.digest)
                        for digest in collection.items)
      return utils.enqueue_task(url, 'tag', payload=payload)

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

    # check for existing elements
    new_digests, existing_digests = self.partition_collection(request)

    # process unseen elements
    for digest_element in new_digests.items:
      # check for error conditions
      if not model.is_valid_hex(digest_element.digest):
        raise endpoints.BadRequestException('Invalid hex code: %s' %
                                            (digest_element.digest))

      # generate and append new URLs
      upload_url, finalize_url = self.generate_push_urls(digest_element,
                                                         request.namespace)
      response.items.append(UrlMessage(upload_url=upload_url,
                                       finalize_url=finalize_url))

    # tag existing entities and return new ones
    self.tag_existing(existing_digests)
    return response


def create_application():
  ereporter2.register_formatter()
  return endpoints.api_server([IsolateService])


app = create_application()
