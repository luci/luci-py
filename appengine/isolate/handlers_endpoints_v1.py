# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Isolate Server frontend url handlers."""

import binascii
import datetime
import hashlib
import os
import re
import time
import zlib

from google.appengine import runtime
from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

import endpoints
from protorpc import message_types
from protorpc import messages
from protorpc import remote

from components import auth
from components import utils

import config
import gcs
from handlers_api import hash_content
from handlers_api import MIN_SIZE_FOR_DIRECT_GS
import model
import stats


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
  """ProtoRPC message representing an entity to be added to the data store."""
  upload_ticket = messages.StringField(1)
  content = messages.BytesField(2)


class FinalizeRequest(messages.Message):
  """Request to validate upload of large Google storage entities."""
  upload_ticket = messages.StringField(1)


class RetrieveRequest(messages.Message):
  """Request to retrieve content from memcache, datastore, or GS."""
  digest = messages.StringField(1, required=True)
  namespace = messages.MessageField(Namespace, 2)
  offset = messages.IntegerField(3, default=0)


### Response Types


class PreuploadStatus(messages.Message):
  """Endpoints response type for a single URL or pair of URLs."""
  gs_upload_url = messages.StringField(1)
  upload_ticket = messages.StringField(2)
  index = messages.IntegerField(3)


class UrlCollection(messages.Message):
  """Endpoints response type analogous to existing JSON response."""
  items = messages.MessageField(PreuploadStatus, 1, repeated=True)


class RetrievedContent(messages.Message):
  """Content retrieved from DB, or GS URL."""
  content = messages.BytesField(1)
  url = messages.StringField(2)


class PushPing(messages.Message):
  """Indicates whether data storage executed successfully."""
  ok = messages.BooleanField(1)


class ServerDetails(messages.Message):
  """Reports the current API version."""
  server_version = messages.StringField(1)


### Utility


# default expiration time for signed links
DEFAULT_LINK_EXPIRATION = datetime.timedelta(hours=4)


# messages for generating and validating upload tickets
UPLOAD_MESSAGES = ['datastore', 'gs']


class TokenSigner(auth.TokenKind):
  """Used to create upload tickets."""
  expiration_sec = DEFAULT_LINK_EXPIRATION.total_seconds()
  secret_key = auth.SecretKey('isolate_upload_token', scope='local')


@ndb.transactional
def store_and_enqueue_verify_task(entry, task_queue_host):
  entry.put()
  taskqueue.add(
      url='/internal/taskqueue/verify/%s' % entry.key.id(),
      params={'req': os.environ['REQUEST_LOG_ID']},
      queue_name='verify',
      headers={'Host': task_queue_host},
      transactional=True,
  )


def entry_key_or_error(namespace, digest):
  try:
    return model.get_entry_key(namespace, digest)
  except ValueError as error:
    raise endpoints.BadRequestException(error.message)


### API


@auth.endpoints_api(
    name='isolateservice', version='v1',
    description='Version 1 of Isolate Service')
class IsolateService(remote.Service):
  """Implements Isolate's API methods."""

  _gs_url_signer = None

  ### Endpoints Methods

  @auth.endpoints_method(DigestCollection, UrlCollection, http_method='POST')
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

    if len(request.items) > 1000:
      raise endpoints.BadRequestException(
          'Only up to 1000 items can be looked up at once')

    # check for existing elements
    new_digests, existing_digests = self.partition_collection(request)

    # process all elements; add an upload ticket for cache misses
    for index, digest_element in enumerate(request.items):
      # check for error conditions
      if not model.is_valid_hex(digest_element.digest):
        raise endpoints.BadRequestException(
            'Invalid hex code: %s' % (digest_element.digest))

      if digest_element in new_digests:
        # generate preupload ticket
        status = PreuploadStatus(
            index=index,
            upload_ticket=self.generate_ticket(
                digest_element, request.namespace))

        # generate GS upload URL if necessary
        if self.should_push_to_gs(digest_element):
          key = entry_key_or_error(
              request.namespace.namespace, digest_element.digest)
          status.gs_upload_url = self.gs_url_signer.get_upload_url(
              filename=key.id(),
              content_type='application/octet-stream',
              expiration=DEFAULT_LINK_EXPIRATION)

        response.items.append(status)

    # Tag existing entities and collect stats.
    self.tag_existing(DigestCollection(
        items=list(existing_digests), namespace=request.namespace))
    stats.add_entry(stats.LOOKUP, len(request.items), len(existing_digests))
    return response

  @auth.endpoints_method(StorageRequest, PushPing)
  def store_inline(self, request):
    """Stores relatively small entities in the datastore."""
    return self.storage_helper(request, False)

  @auth.endpoints_method(FinalizeRequest, PushPing)
  def finalize_gs_upload(self, request):
    """Informs client that large entities have been uploaded to GCS."""
    return self.storage_helper(request, True)

  @auth.endpoints_method(RetrieveRequest, RetrievedContent)
  def retrieve(self, request):
    """Retrieves content from a storage location."""
    content = None
    key = None
    offset = request.offset

    # try the memcache
    memcache_entry = memcache.get(
        request.digest, namespace='table_%s' % request.namespace.namespace)
    if memcache_entry is not None:
      content = memcache_entry
      found = 'memcache'

    # try ndb
    else:
      key = entry_key_or_error(request.namespace.namespace, request.digest)
      stored = key.get()
      if stored is None:
        raise endpoints.NotFoundException('Unable to retrieve the entry.')
      content = stored.content  # will be None if entity is in GCS
      found = 'inline'

    # Return and log stats here if something has been found.
    if content is not None:
      # make sure that offset is acceptable
      if offset < 0 or offset > len(content):
        raise endpoints.BadRequestException(
            'Invalid offset %d. Offset must be between 0 and content length.' %
            offset)
      stats.add_entry(stats.RETURN, len(content) - offset, found)
      return RetrievedContent(content=content[offset:])

    # The data is in GS; log stats and return the URL.
    if offset < 0 or offset > stored.compressed_size:
      raise endpoints.BadRequestException(
          'Invalid offset %d. Offset must be between 0 and content length.' %
          offset)
    stats.add_entry(
        stats.RETURN,
        stored.compressed_size - offset,
        'GS; %s' % stored.key.id())
    return RetrievedContent(url=self.gs_url_signer.get_download_url(
        filename=key.id(),
        expiration=DEFAULT_LINK_EXPIRATION))

  @auth.endpoints_method(message_types.VoidMessage, ServerDetails)
  def server_details(self, _request):
    return ServerDetails(server_version=utils.get_app_version())

  ### Utility

  def storage_helper(self, request, uploaded_to_gs):
    """Implement shared logic between store_inline and finalize_gs."""
    # validate token or error out
    if not request.upload_ticket:
      raise endpoints.BadRequestException(
          'Upload ticket was empty or not provided.')
    try:
      embedded = TokenSigner.validate(
          request.upload_ticket, UPLOAD_MESSAGES[uploaded_to_gs])
    except (auth.InvalidTokenError, ValueError) as error:
      raise endpoints.BadRequestException(
          'Ticket validation failed: %s' % error.message)

    # read data and convert types
    digest = embedded['d'].encode('utf-8')
    is_isolated = bool(int(embedded['i']))
    namespace = embedded['n']
    size = int(embedded['s'])

    # create a key
    key = entry_key_or_error(namespace, digest)

    # get content and compressed size
    if uploaded_to_gs:
      # ensure that file info is uploaded to GS first
      # TODO(cmassaro): address analogous TODO from handlers_api
      file_info = gcs.get_file_info(config.settings().gs_bucket, key.id())
      if not file_info:
        raise endpoints.BadRequestException(
            'File should be in Google Storage.\nFile: \'%s\' Size: %d.' % (
                key.id(), size))
      content = None
      compressed_size = file_info.size
    else:
      content = request.content
      compressed_size = len(content)

    # all is well; create an entry
    entry = model.new_content_entry(
        key=key,
        is_isolated=is_isolated,
        compressed_size=compressed_size,
        expanded_size=size,
        is_verified=not uploaded_to_gs,
        content=content,
    )

    # DB: assert that embedded content is the data sent by the request
    if not uploaded_to_gs:
      if (digest, size) != hash_content(content, namespace):
        raise endpoints.BadRequestException(
            'Embedded digest does not match provided data: '
            '(digest, size): (%r, %r); expected: %r' % (
                digest, size, hash_content(content, namespace)))
      entry.put()

    # GCS: enqueue verification task
    else:
      try:
        store_and_enqueue_verify_task(entry, utils.get_task_queue_host())
      except (
          datastore_errors.Error,
          runtime.apiproxy_errors.CancelledError,
          runtime.apiproxy_errors.DeadlineExceededError,
          runtime.apiproxy_errors.OverQuotaError,
          runtime.DeadlineExceededError,
          taskqueue.Error) as e:
        raise endpoints.InternalServerErrorException(
            'Unable to store the entity: %s.' % e.__class__.__name__)

    stats.add_entry(
        stats.STORE, entry.compressed_size,
        'GS; %s' % entry.key.id() if uploaded_to_gs else 'inline')
    return PushPing(ok=True)

  @classmethod
  def generate_ticket(cls, digest, namespace):
    """Generates an HMAC-SHA1 signature for a given set of parameters.

    Used by preupload to sign store URLs and by store_content to validate them.

    Args:
      digest: the Digest being uploaded
      namespace: the namespace associated with the original DigestCollection

    Returns:
      base64-encoded upload ticket
    """
    uploaded_to_gs = cls.should_push_to_gs(digest)

    # get a single dictionary containing important information
    embedded = {
        'd': digest.digest,
        'i': str(int(digest.is_isolated)),
        'n': namespace.namespace,
        's': str(digest.size),
    }
    message = UPLOAD_MESSAGES[uploaded_to_gs]
    try:
      result = TokenSigner.generate(message, embedded)
    except ValueError as error:
      raise endpoints.BadRequestException(
          'Ticket generation failed: %s' % error.message)
    return result

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
      key = entry_key_or_error(entries.namespace.namespace, digest.digest)
      futures[key.get_async(use_cache=False)] = digest

    # Pick first one that finishes and yield it, rinse, repeat.
    while futures:
      future = ndb.Future.wait_any(futures)
      # TODO(maruel): For items that were present, make sure
      # future.get_result().compressed_size == digest.size.
      yield futures.pop(future), bool(future.get_result())

  @classmethod
  def partition_collection(cls, entries):
    """Create sets of existent and new digests."""
    seen_unseen = [set(), set()]
    for digest, exists in cls.check_entries_exist(entries):
      seen_unseen[exists].add(digest)
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
