# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Isolate Server frontend url handlers."""

import binascii
import datetime
import hashlib
import hmac
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
import model
import stats

from google.appengine.ext import ndb


### Request Types


class Digest(messages.Message):
  """ProtoRPC message containing digest information."""
  digest = messages.StringField(1)
  is_isolated = messages.BooleanField(2, default=False)
  size = messages.IntegerField(3)


class Namespace(messages.Message):
  """Encapsulates namespace, compression, and hash algorithm."""
  namespace = messages.StringField(1, default='default')
  digest_hash = messages.StringField(2, default='SHA-1')
  compression = messages.StringField(3, default='flate')


class DigestCollection(messages.Message):
  """Endpoints request type analogous to the existing JSON post body."""
  items = messages.MessageField(Digest, 1, repeated=True)
  namespace = messages.MessageField(Namespace, 2, repeated=False)


class StorageRequest(messages.Message):
  """ProtoRPC message representing an entity to be added to the data store.

  TODO(cmassaro): worthwhile to make expiration_ts a DateTimeField?
  TODO(cmassaro): any required fields?
  TODO(cmassaro): how is data distinct from item.digest?
  """
  item = messages.MessageField(Digest, 1, repeated=False)
  data = messages.BytesField(2)
  upload_ticket = messages.StringField(3)
  namespace = messages.MessageField(Namespace, 4)


class FinalizeRequest(messages.Message):
  """Content storage request for large Google storage entities."""
  item = messages.MessageField(Digest, 1, repeated=False)
  upload_ticket = messages.StringField(2)
  namespace = messages.MessageField(Namespace, 3)


## Response Types


class PreuploadStatus(messages.Message):
  """Endpoints response type for a single URL or pair of URLs."""
  gs_upload_url = messages.StringField(1, required=False)
  upload_ticket = messages.StringField(2, required=False)


class UrlCollection(messages.Message):
  """Endpoints response type analogous to existing JSON response."""
  items = messages.MessageField(PreuploadStatus, 1, repeated=True)


class StorageResponse(messages.Message):
  """Endpoints response to report content storage status.

  TODO(cmassaro): determine what "entry" should contain; maybe a repeated field?
  """
  entry = messages.StringField(1)


### API


@auth.endpoints_api(name='isolateservice', version='v1')
class IsolateService(remote.Service):
  """Implement API methods corresponding to handlers in handlers_api."""

  # Default expiration time for signed links.
  DEFAULT_LINK_EXPIRATION = datetime.timedelta(hours=4)

  _gs_url_signer = None

  ### Endpoints Methods

  @auth.endpoints_method(DigestCollection, UrlCollection,
                         path='preuploader', http_method='POST',
                         name='preupload')
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
    if not re.match(r'^%s$' % model.NAMESPACE_RE, request.namespace.namespace):
      raise endpoints.BadRequestException(
          'Invalid namespace; allowed keys must pass regexp "%s"' %
          model.NAMESPACE_RE)

    # check for existing elements
    new_digests, existing_digests = self.partition_collection(request)

    # process unseen elements
    for digest_element in new_digests.items:
      # check for error conditions
      if not model.is_valid_hex(digest_element.digest):
        raise endpoints.BadRequestException(
            'Invalid hex code: %s' % (digest_element.digest))

      status = PreuploadStatus()
      if self.should_push_to_gs(digest_element):
        # Store larger stuff in Google Storage.
        key = model.entry_key(
            request.namespace.namespace, digest_element.digest)
        status.gs_upload_url = self.gs_url_signer.get_upload_url(
            filename=key.id(),
            content_type='application/octet-stream',
            expiration=self.DEFAULT_LINK_EXPIRATION)
      response.items.append(status)

    # tag existing entities and return new ones
    self.tag_existing(existing_digests)
    return response

  @auth.endpoints_method(StorageRequest, StorageResponse)
  def store_inline(self, request):
    """Replaces the StoreContentHandler's PUT method.

    TODO(cmassaro): message types, implementation

    Args:
      request: a StoreRequest

    Response:
      the StorageResponse containing information about the upload
    """
    pass

  @auth.endpoints_method(FinalizeRequest, StorageResponse)
  def finalize_gs_upload(self, request):
    """Replaces the StoreContentHandler's POST method.

    Args:
      request: a StoreRequest

    Response:
      the StorageResponse containing information about the upload
    """
    pass

  ### Utility

  @classmethod
  def check_entries_exist(cls, entries):
    """Assess which entities already exist in the datastore.

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
        key = model.entry_key(entries.namespace.namespace, digest.digest)
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
        items=[], namespace=entries.namespace) for _ in range(2)]
    for digest, exists in cls.check_entries_exist(entries):
      seen_unseen[exists].items.append(digest)
    return seen_unseen

  @classmethod
  def should_push_to_gs(cls, digest):
    """True to direct client to upload given EntryInfo directly to GS."""
    # Relatively small *.isolated files go through app engine to cache them.
    if digest.is_isolated and digest.size <= model.MAX_MEMCACHE_ISOLATED:
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

  def uri_for(self, *_args, **kwargs):
    """Builds an URI for the provided data.

    Currently looks like
    <hostname>/<api_method>/<namespace>/<hash_key>, e.g.
    localhost:80/store-gs/default/234987234987239487139847abcd1234e1fefeb1
    """
    name = 'content-gs/store'
    namespace = kwargs.pop('namespace')
    hex_digest = kwargs.pop('hash_key')
    url_list = [dict(self.request_state.headers)['host']]
    url_list.extend([name, namespace, hex_digest])
    return '/'.join(url_list)

  @classmethod
  def generate_signature(cls, secret_key, http_verb, expiration_ts, namespace,
                         hash_key, item_size, is_isolated, uploaded_to_gs):
    """Generates an HMAC-SHA1 signature for a given set of parameters.

    Used by preupload to sign store URLs and by store_content to validate them.

    All arguments should be in the form of strings.
    """
    data_to_sign = '\n'.join([
        http_verb,
        expiration_ts,
        namespace,
        hash_key,
        item_size,
        is_isolated,
        uploaded_to_gs,
    ])
    mac = hmac.new(secret_key, digestmod=hashlib.sha1)
    mac.update(data_to_sign)
    return mac.hexdigest()

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
    is_isolated = str(int(digest.is_isolated))
    uploaded_to_gs = str(int(uploaded_to_gs))

    # Generate signature.
    sig = self.generate_signature(
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
        # 'token': digest.get('token'),
        'x': expiration_ts,
    }
    # TODO(cmassaro): where can we get the auth token?
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

    TODO(cmassaro): cannibalize this logic for client
    """
    if self.should_push_to_gs(digest):
      # Store larger stuff in Google Storage.
      key = model.entry_key(namespace, digest.digest)
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
  def tag_existing(cls, collection):
    """Tag existing digests with new timestamp.

    Arguments:
      collection: a DigestCollection containing existing digests

    Returns:
      the enqueued task if there were existing entries; None otherwise
    """
    if collection.items:
      url = '/internal/taskqueue/tag/%s/%s' % (
          collection.namespace.namespace,
          utils.datetime_to_timestamp(utils.utcnow()))
      payload = ''.join(
          binascii.unhexlify(digest.digest) for digest in collection.items)
      return utils.enqueue_task(url, 'tag', payload=payload)


def create_application():
  ereporter2.register_formatter()
  return endpoints.api_server([IsolateService])


app = create_application()
