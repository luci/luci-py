# Copyright 2012 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Isolate Server frontend url handlers."""

import binascii
import collections
import datetime
import hashlib
import hmac
import json
import logging
import re
import time
import urllib
import zlib

import webapp2
from google.appengine import runtime
from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

import acl
import config
import gcs
import handlers_common
import map_reduce_jobs
import model
import stats
import stats_gviz
import template
from components import auth
from components import ereporter2
from components import utils


# Version of isolate protocol returned to clients in /handshake request.
ISOLATE_PROTOCOL_VERSION = '1.0'


# The minimum size, in bytes, an entry must be before it gets stored in Google
# Cloud Storage, otherwise it is stored as a blob property.
MIN_SIZE_FOR_GS = 501

# The minimum size, in bytes, for entry that get's uploaded directly to Google
# Cloud Storage, bypassing App engine layer.
# This effectively disable inline upload. This is because urlfetch is too flaky
# in practice so it is not worth the associated downtime.
MIN_SIZE_FOR_DIRECT_GS = MIN_SIZE_FOR_GS


### ACLs


# Names of groups.
ADMINS_GROUP = 'isolate-admin-access'
READERS_GROUP = 'isolate-read-access'
WRITERS_GROUP = 'isolate-write-access'


def isolate_admin():
  """Returns True if current user can administer isolate server."""
  return auth.is_group_member(ADMINS_GROUP) or auth.is_admin()


def isolate_writable():
  """Returns True if current user can write to isolate."""
  # Admins have access by default.
  return auth.is_group_member(WRITERS_GROUP) or isolate_admin()


def isolate_readable():
  """Returns True if current user can read from isolate."""
  # Anyone that can write can also read.
  return auth.is_group_member(READERS_GROUP) or isolate_writable()


def bootstrap_dev_server_acls():
  """Adds 127.0.0.1 as a whitelisted IP when testing."""
  assert utils.is_local_dev_server()

  # Add to IP whitelist.
  access_id = acl.ip_to_str('v4', 2130706433)
  acl.WhitelistedIP.get_or_insert(
      access_id,
      ip='127.0.0.1',
      comment='automatic because of running on dev server')

  # Add to Isolate groups.
  ident = auth.Identity(auth.IDENTITY_BOT, access_id)
  auth.bootstrap_group(READERS_GROUP, ident, 'Can read from Isolate')
  auth.bootstrap_group(WRITERS_GROUP, ident, 'Can write to Isolate')

  # Add a fake admin for local dev server.
  auth.bootstrap_group(
      auth.ADMIN_GROUP,
      auth.Identity(auth.IDENTITY_USER, 'test@example.com'),
      'Users that can manage groups')


### Utility


def hash_content(content, namespace):
  """Decompresses and hashes given |content|.

  Returns tuple (hex digest, expanded size).

  Raises ValueError in case of errors.
  """
  expanded_size = 0
  digest = model.get_hash_algo(namespace)
  try:
    for data in model.expand_content(namespace, [content]):
      expanded_size += len(data)
      digest.update(data)
      # Make sure the data is GC'ed.
      del data
    return digest.hexdigest(), expanded_size
  except zlib.error as e:
    raise ValueError('Data is corrupted: %s' % e)


### Restricted handlers


class RestrictedAdminUIHandler(auth.AuthenticatingHandler):
  """Root admin UI page."""

  @auth.require(isolate_admin)
  def get(self):
    self.response.write(template.render('isolate/restricted.html', {
        'xsrf_token': self.generate_xsrf_token(),
        'map_reduce_jobs': [
            {'id': job_id, 'name': job_def['name']}
            for job_id, job_def in map_reduce_jobs.MAP_REDUCE_JOBS.iteritems()
        ],
    }))


class RestrictedGoogleStorageConfig(auth.AuthenticatingHandler):
  """View and modify Google Storage config entries."""

  @auth.require(isolate_admin)
  def get(self):
    settings = config.settings()
    self.response.write(template.render('isolate/gs_config.html', {
        'gs_bucket': settings.gs_bucket,
        'gs_client_id_email': settings.gs_client_id_email,
        'gs_private_key': settings.gs_private_key,
        'xsrf_token': self.generate_xsrf_token(),
    }))

  @auth.require(isolate_admin)
  def post(self):
    settings = config.settings()
    settings.gs_bucket = self.request.get('gs_bucket')
    settings.gs_client_id_email = self.request.get('gs_client_id_email')
    settings.gs_private_key = self.request.get('gs_private_key')
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    try:
      # Ensure key is correct, it's easy to make a mistake when creating it.
      gcs.URLSigner.load_private_key(settings.gs_private_key)
    except Exception as exc:
      # TODO(maruel): Handling Exception is too generic. And add self.abort(400)
      self.response.write('Bad private key: %s' % exc)
      return
    # Store the settings.
    settings.put()
    self.response.write('Done!')


### Mapreduce related handlers


class RestrictedLaunchMapReduceJob(auth.AuthenticatingHandler):
  """Enqueues a task to start a map reduce job on the backend module.

  A tree of map reduce jobs inherits module and version of a handler that
  launched it. All UI handlers are executes by 'default' module. So to run a
  map reduce on a backend module one needs to pass a request to a task running
  on backend module.
  """

  @auth.require(isolate_admin)
  def post(self):
    job_id = self.request.get('job_id')
    assert job_id in map_reduce_jobs.MAP_REDUCE_JOBS
    # Do not use 'backend' module when running from dev appserver. Mapreduce
    # generates URLs that are incompatible with dev appserver URL routing when
    # using custom modules.
    success = handlers_common.enqueue_task(
        url='/internal/taskqueue/mapreduce/launch/%s' % job_id,
        queue_name=map_reduce_jobs.MAP_REDUCE_TASK_QUEUE,
        use_dedicated_module=not utils.is_local_dev_server())
    # New tasks should show up on the status page.
    if success:
      self.redirect('/internal/mapreduce/status')
    else:
      self.abort(500, 'Failed to launch the job')


### Non-restricted handlers


class ProtocolHandler(auth.AuthenticatingHandler):
  """Base class for request handlers that implement isolate protocol."""

  # Isolate protocol uses 'token' instead of 'xsrf_token'.
  xsrf_token_request_param = 'token'

  def send_json(self, body, http_code=200):
    """Serializes |body| into JSON and sends it as a response."""
    self.response.set_status(http_code)
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.write(utils.encode_to_json(body))

  def send_error(self, message, http_code=400):
    """Sends a error message and aborts the request, logs the error."""
    logging.error(message)
    self.abort(http_code, detail=message)

  def send_data(self, data, filename=None, offset=0):
    """Sends binary data as a response.

    If |offset| is zero, returns an entire |data| and sets HTTP code to 200.
    If |offset| is non-zero, returns a subrange of |data| with HTTP code
    set to 206 and 'Content-Range' header.
    If |offset| is outside of acceptable range, returns HTTP code 416.
    """
    # Bad offset? Return 416.
    if offset < 0 or offset >= len(data):
      return self.send_error(
          'Unacceptable offset.\nRequested offset is %d while file '
          'size is %d' % (offset, len(data)), http_code=416)

    # Common headers that are set regardless of |offset| value.
    if filename:
      self.response.headers['Content-Disposition'] = (
          'attachment; filename="%s"' % filename)
    self.response.headers['Content-Type'] = 'application/octet-stream'
    self.response.headers['Cache-Control'] = 'public, max-age=43200'

    if not offset:
      # Returning an entire file.
      self.response.set_status(200)
      self.response.out.write(data)
    else:
      # Returning a partial content, set Content-Range header.
      self.response.set_status(206)
      self.response.headers['Content-Range'] = (
          'bytes %d-%d/%d' % (offset, len(data) - 1, len(data)))
      self.response.out.write(data[offset:])

  @property
  def client_protocol_version(self):
    """Returns protocol version client provided during handshake, or None if not
    known.

    Valid only for POST or PUT requests for now.
    """
    # See HandshakeHandler, its where xsrf_token_data is generated.
    return self.xsrf_token_data.get('v')


class HandshakeHandler(ProtocolHandler):
  """Returns access token, version and capabilities of the server.

  Request body is a JSON dict:
    {
      "client_app_version": "0.2",
      "fetcher": true,
      "protocol_version": "1.0",
      "pusher": true,
    }

  Response body is a JSON dict:
    {
      "access_token": "......",
      "protocol_version": "1.0",
      "server_app_version": "138-193f1f3",
    }
    or
    {
      "error": "Some user friendly error text",
      "protocol_version": "1.0",
      "server_app_version": "138-193f1f3",
    }
  """

  # This handler is called to get XSRF token, there's nothing to enforce yet.
  xsrf_token_enforce_on = ()

  @auth.require(isolate_readable)
  def post(self):
    """Responds with access token and server version."""
    try:
      request = json.loads(self.request.body)
      client_protocol = str(request['protocol_version'])
      client_app_version = str(request['client_app_version'])
      pusher = request.get('pusher', True)
      fetcher = request.get('fetcher', True)
    except (ValueError, KeyError) as exc:
      return self.send_error(
          'Invalid body of /handshake call.\nError: %s.' % exc)

    # This access token will be used to validate each subsequent request.
    access_token = self.generate_xsrf_token({'v': client_protocol})

    # Log details of the handshake to the server log.
    logging_info = {
      'Access Id': auth.get_current_identity().to_bytes(),
      'Client app version': client_app_version,
      'Client is fetcher': fetcher,
      'Client is pusher': pusher,
      'Client protocol version': client_protocol,
      'Token': access_token,
    }
    logging.info(
        '\n'.join('%s: %s' % (k, logging_info[k])
        for k in sorted(logging_info)))

    # Send back the response.
    self.send_json(
        {
          'access_token': access_token,
          'protocol_version': ISOLATE_PROTOCOL_VERSION,
          'server_app_version': utils.get_app_version(),
        })


class PreUploadContentHandler(ProtocolHandler):
  """Checks for entries existence, generates upload URLs.

  Request body is a JSON list:
  [
      {
          "h": <hex digest>,
          "i": <1 for isolated file, 0 for rest of them>
          "s": <int entry size>,
      },
      ...
  ]

  Response is a JSON list of the same length where each item is either:
    * If an entry is missing: a list with two URLs - URL to upload a file to,
      and URL to call when upload is done (can be null).
    * If entry is already present: null.

  For instance:
  [
      ["<upload url>", "<finalize url>"],
      null,
      null,
      ["<upload url>", null],
      null,
      ...
  ]
  """

  # Default expiration time for signed links.
  DEFAULT_LINK_EXPIRATION = datetime.timedelta(hours=4)

  # Info about a requested entry:
  #   digest: hex string with digest
  #   size: uncompressed item size
  #   is_isolated: True if it's *.isolated file
  EntryInfo = collections.namedtuple(
      'EntryInfo', ['digest', 'size', 'is_isolated'])

  _gs_url_signer = None

  @staticmethod
  def parse_request(body, namespace):
    """Parses a request body into a list of EntryInfo objects."""
    hex_digest_size = model.get_hash_algo(namespace).digest_size * 2
    try:
      out = [
        PreUploadContentHandler.EntryInfo(
            str(m['h']), int(m['s']), bool(m['i']))
        for m in json.loads(body)
      ]
      for i in out:
        model.check_hash(i.digest, hex_digest_size)
      return out
    except (ValueError, KeyError, TypeError) as exc:
      raise ValueError('Bad body: %s' % exc)

  @staticmethod
  def check_entry_infos(entries, namespace):
    """Generator that checks for EntryInfo entries existence.

    Yields pairs (EntryInfo object, True if such entry exists in Datastore).
    """
    # Kick off all queries in parallel. Build mapping Future -> digest.
    futures = {}
    for entry_info in entries:
      key = model.entry_key(namespace, entry_info.digest)
      futures[key.get_async(use_cache=False)] = entry_info

    # Pick first one that finishes and yield it, rinse, repeat.
    while futures:
      future = ndb.Future.wait_any(futures)
      # TODO(maruel): For items that were present, make sure
      # future.get_result().compressed_size == entry_info.size.
      yield futures.pop(future), bool(future.get_result())

  @staticmethod
  def tag_entries(entries, namespace):
    """Enqueues a task to update the timestamp for given entries."""
    url = '/internal/taskqueue/tag/%s/%s' % (
        namespace, utils.datetime_to_timestamp(handlers_common.utcnow()))
    payload = ''.join(binascii.unhexlify(e.digest) for e in entries)
    return handlers_common.enqueue_task(url, 'tag', payload=payload)

  @staticmethod
  def should_push_to_gs(entry_info):
    """True to direct client to upload given EntryInfo directly to GS."""
    # Relatively small *.isolated files go through app engine to cache them.
    if (entry_info.is_isolated and
        entry_info.size <= model.MAX_MEMCACHE_ISOLATED):
      return False
    # All other large enough files go through GS.
    return entry_info.size >= MIN_SIZE_FOR_DIRECT_GS

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

  def generate_store_url(self, entry_info, namespace, http_verb, uploaded_to_gs,
                         expiration):
    """Generates a signed URL to /content-gs/store method.

    Arguments:
      entry_info: A EntryInfo instance.
    """
    # Data that goes into request parameters and signature.
    expiration_ts = str(int(time.time() + expiration.total_seconds()))
    item_size = str(entry_info.size)
    is_isolated = str(int(entry_info.is_isolated))
    uploaded_to_gs = str(int(uploaded_to_gs))

    # Generate signature.
    sig = StoreContentHandler.generate_signature(
        config.settings().global_secret, http_verb, expiration_ts, namespace,
        entry_info.digest, item_size, is_isolated, uploaded_to_gs)

    # Bare full URL to /content-gs/store endpoint.
    url_base = self.uri_for(
        'store-gs', namespace=namespace, hash_key=entry_info.digest, _full=True)

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

  def generate_push_urls(self, entry_info, namespace):
    """Generates a pair of URLs to be used by clients to upload an item.

    The GS filename is exactly ContentEntry.key.id().

    URL's being generated are 'upload URL' and 'finalize URL'. Client uploads
    an item to upload URL (via PUT request) and then POST status of the upload
    to a finalize URL.

    Finalize URL may be optional (it's None in that case).
    """
    if self.should_push_to_gs(entry_info):
      # Store larger stuff in Google Storage.
      key = model.entry_key(namespace, entry_info.digest)
      upload_url = self.gs_url_signer.get_upload_url(
          filename=key.id(),
          content_type='application/octet-stream',
          expiration=self.DEFAULT_LINK_EXPIRATION)
      finalize_url = self.generate_store_url(
          entry_info, namespace,
          http_verb='POST',
          uploaded_to_gs=True,
          expiration=self.DEFAULT_LINK_EXPIRATION)
    else:
      # Store smallish entries and *.isolated in Datastore directly.
      upload_url = self.generate_store_url(
          entry_info, namespace,
          http_verb='PUT',
          uploaded_to_gs=False,
          expiration=self.DEFAULT_LINK_EXPIRATION)
      finalize_url = None
    return upload_url, finalize_url

  @auth.require(isolate_writable)
  def post(self, namespace):
    """Reads body with items to upload and replies with URLs to upload to."""
    if not re.match(r'^%s$' % model.NAMESPACE_RE, namespace):
      self.send_error(
          'Invalid namespace; allowed keys must pass regexp "%s"' %
          model.NAMESPACE_RE)

    # Parse a body into list of EntryInfo objects.
    try:
      entries = self.parse_request(self.request.body, namespace)
    except ValueError as err:
      return self.send_error(
          'Bad /pre-upload request.\n(%s)\n%s' % (err, self.request.body[:200]))

    # Generate push_urls for missing entries.
    push_urls = {}
    existing = []
    for entry_info, exists in self.check_entry_infos(entries, namespace):
      if exists:
        existing.append(entry_info)
      else:
        push_urls[entry_info.digest] = self.generate_push_urls(
            entry_info, namespace)

    # Send back the response.
    self.send_json([push_urls.get(entry_info.digest) for entry_info in entries])

    # Log stats, enqueue tagging task that updates last access time.
    stats.add_entry(stats.LOOKUP, len(entries), len(existing))
    if existing:
      # Ignore errors in a call below. They happen when task queue service has
      # a bad time and doesn't accept tagging tasks. We don't want isolate
      # server's reliability to depend on task queue service health. An ignored
      # error here means there's a chance some entry might be deleted sooner
      # than it should.
      self.tag_entries(existing, namespace)


class RetrieveContentHandler(ProtocolHandler):
  """The handlers for retrieving contents by its SHA-1 hash |hash_key|.

  Can produce 5 types of responses:
    * HTTP 200: the content is in response body as octet-stream.
    * HTTP 206: partial content is in response body.
    * HTTP 302: http redirect to a file with the content.
    * HTTP 404: content is not available, response body is a error message.
    * HTTP 416: requested byte range can not be satisfied.
  """

  @auth.require(isolate_readable)
  def get(self, namespace, hash_key):  #pylint: disable=W0221
    # Parse 'Range' header if it's present to extract initial offset.
    # Only support single continuous range from some |offset| to the end.
    offset = 0
    range_header = self.request.headers.get('range')
    if range_header:
      match = re.match(r'bytes=(\d+)-', range_header)
      if not match:
        return self.send_error(
            'Unsupported byte range.\n\'%s\'.' % range_header, http_code=416)
      offset = int(match.group(1))

    memcache_entry = memcache.get(hash_key, namespace='table_%s' % namespace)
    if memcache_entry is not None:
      self.send_data(memcache_entry, filename=hash_key, offset=offset)
      stats.add_entry(stats.RETURN, len(memcache_entry) - offset, 'memcache')
      return

    entry = model.entry_key(namespace, hash_key).get()
    if not entry:
      return self.send_error('Unable to retrieve the entry.', http_code=404)

    if entry.content is not None:
      self.send_data(entry.content, filename=hash_key, offset=offset)
      stats.add_entry(stats.RETURN, len(entry.content) - offset, 'inline')
      return

    # Generate signed download URL.
    settings = config.settings()
    # TODO(maruel): The GS object may not exist anymore. Handle this.
    signer = gcs.URLSigner(settings.gs_bucket,
        settings.gs_client_id_email, settings.gs_private_key)
    # The entry key is the GS filepath.
    signed_url = signer.get_download_url(entry.key.id())

    # Redirect client to this URL. If 'Range' header is used, client will
    # correctly pass it to Google Storage to fetch only subrange of file,
    # so update stats accordingly.
    self.redirect(signed_url)
    stats.add_entry(
        stats.RETURN, entry.compressed_size - offset, 'GS; %s' % entry.key.id())


class StoreContentHandler(ProtocolHandler):
  """Creates ContentEntry Datastore entity for some uploaded file.

  Clients usually do not call this handler explicitly. Signed URL to it
  is returned in /pre-upload call.

  This handler is called in two ways:
    * As a POST request to finalize a file already uploaded to GS. Request
      body is empty in that case.
    * As a PUT request to upload an actual data and create ContentEntry in one
      call. Request body contains octet-stream with entry's data.

  In either case query parameters define details of new content entry:
    g - 1 if it was previously uploaded to GS.
    i - 1 if it its *.isolated file.
    s - size of the uncompressed file.
    x - URL signature expiration timestamp.
    sig - signature of request parameters, to verify they are not tampered with.

  Can produce 3 types of responses:
    * HTTP 200: success, entry is created or existed before, response body is
      a json dict with information about new entry.
    * HTTP 400: fatal error, retrying request won't fix it, response body is
      a error message.
    * HTTP 503: transient error, request should be retried by client, response
      body is a error message.

  In case of HTTP 200, body is a JSON dict:
  {
      'entry': {<details about the entry>}
  }
  """

  @staticmethod
  def generate_signature(secret_key, http_verb, expiration_ts, namespace,
                         hash_key, item_size, is_isolated, uploaded_to_gs):
    """Generates HMAC-SHA1 signature for given set of parameters.

    Used by PreUploadContentHandler to sign store URLs and by
    StoreContentHandler to validate them.

    All arguments should be in form of strings.
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

  @auth.require(isolate_writable)
  def post(self, namespace, hash_key):
    """POST is used when finalizing upload to GS."""
    return self.handle(namespace, hash_key)

  @auth.require(isolate_writable)
  def put(self, namespace, hash_key):
    """PUT is used when uploading directly to datastore via this handler."""
    return self.handle(namespace, hash_key)

  def handle(self, namespace, hash_key):
    """Handles this request."""
    # Extract relevant request parameters.
    expiration_ts = self.request.get('x')
    item_size = self.request.get('s')
    is_isolated = self.request.get('i')
    uploaded_to_gs = self.request.get('g')
    signature = self.request.get('sig')

    # Build correct signature.
    expected_sig = self.generate_signature(
        config.settings().global_secret, self.request.method, expiration_ts,
        namespace, hash_key, item_size, is_isolated, uploaded_to_gs)

    # Verify signature is correct.
    if not utils.constant_time_equals(signature, expected_sig):
      return self.send_error('Incorrect signature.')

    # Convert parameters from strings back to something useful.
    # It can't fail since matching signature means it was us who generated
    # this strings in a first place.
    expiration_ts = int(expiration_ts)
    item_size = int(item_size)
    is_isolated = bool(int(is_isolated))
    uploaded_to_gs = bool(int(uploaded_to_gs))

    # Verify signature is not yet expired.
    if time.time() > expiration_ts:
      return self.send_error('Expired signature.')

    if uploaded_to_gs:
      # GS upload finalization uses empty POST body.
      assert self.request.method == 'POST'
      if self.request.headers.get('content-length'):
        return self.send_error('Expecting empty POST.')
      content = None
    else:
      # Datastore upload uses PUT.
      assert self.request.method == 'PUT'
      if self.request.headers.get('content-length'):
        content = self.request.body
      else:
        content = ''

    # Info about corresponding GS entry (if it exists).
    gs_bucket = config.settings().gs_bucket
    key = model.entry_key(namespace, hash_key)

    # Verify the data while at it since it's already in memory but before
    # storing it in memcache and datastore.
    if content is not None:
      # Verify advertised hash matches the data.
      try:
        hex_digest, expanded_size = hash_content(content, namespace)
        if hex_digest != hash_key:
          raise ValueError(
              'Hash and data do not match, '
              '%d bytes (%d bytes expanded)' % (len(content), expanded_size))
        if expanded_size != item_size:
          raise ValueError(
              'Advertised data length (%d) and actual data length (%d) '
              'do not match' % (item_size, expanded_size))
      except ValueError as err:
        return self.send_error('Inline verification failed.\n%s' % err)
      # Successfully verified!
      compressed_size = len(content)
      needs_verification = False
    else:
      # Fetch size of the stored file.
      file_info = gcs.get_file_info(gs_bucket, key.id())
      if not file_info:
        # TODO(maruel): Do not fail yet. If the request got up to here, the file
        # is likely there but the service may have trouble fetching the metadata
        # from GS.
        return self.send_error(
            'File should be in Google Storage.\nFile: \'%s\' Size: %d.' %
            (key.id(), item_size))
      compressed_size = file_info.size
      needs_verification = True

    # Data is here and it's too large for DS, so put it in GS. It is likely
    # between MIN_SIZE_FOR_GS <= len(content) < MIN_SIZE_FOR_DIRECT_GS
    if content is not None and len(content) >= MIN_SIZE_FOR_GS:
      if not gcs.write_file(gs_bucket, key.id(), [content]):
        # Returns 503 so the client automatically retries.
        return self.send_error(
            'Unable to save the content to GS.', http_code=503)
      # It's now in GS.
      uploaded_to_gs = True

    # Can create entity now, everything appears to be legit.
    entry = model.new_content_entry(
        key=key,
        is_isolated=is_isolated,
        compressed_size=compressed_size,
        expanded_size=-1 if needs_verification else item_size,
        is_verified = not needs_verification)

    # If it's not in GS then put it inline.
    if not uploaded_to_gs:
      assert content is not None and len(content) < MIN_SIZE_FOR_GS
      entry.content = content

    # Start saving *.isolated into memcache iff its content is available and
    # it's not in Datastore: there's no point in saving inline blobs in memcache
    # because ndb already memcaches them.
    memcache_store_future = None
    if (content is not None and
        entry.content is None and
        entry.is_isolated and
        entry.compressed_size <= model.MAX_MEMCACHE_ISOLATED):
      memcache_store_future = model.save_in_memcache(
          namespace, hash_key, content, async=True)

    try:
      # If entry was already verified above (i.e. it is a small inline entry),
      # store it right away, possibly overriding existing entity. Most of
      # the time it is a new entry anyway (since clients try to upload only
      # new entries).
      if not needs_verification:
        entry.put()
      else:
        # For large entries (that require expensive verification) be more
        # careful and check that it is indeed a new entity. No need to do it in
        # transaction: a race condition would lead to redundant verification
        # task enqueued, no big deal.
        existing = entry.key.get()
        if existing:
          if existing.is_verified:
            logging.info('Entity exists and already verified')
          else:
            logging.info('Entity exists, but not yet verified')
        else:
          # New entity. Store it and enqueue verification task, transactionally.
          try:
            @ndb.transactional
            def store_and_enqueue_verify_task(entry, task_queue_host):
              entry.put()
              taskqueue.add(
                  url='/internal/taskqueue/verify/%s' % entry.key.id(),
                  queue_name='verify',
                  headers={'Host': task_queue_host},
                  transactional=True)
            store_and_enqueue_verify_task(entry, utils.get_task_queue_host())
          except (
              datastore_errors.Error,
              runtime.apiproxy_errors.CancelledError,
              runtime.apiproxy_errors.DeadlineExceededError,
              runtime.apiproxy_errors.OverQuotaError,
              runtime.DeadlineExceededError,
              taskqueue.Error) as e:
            return self.send_error(
                'Unable to store the entity: %s.' % e.__class__.__name__,
                http_code=503)

      # TODO(vadimsh): Fill in details about the entry, such as expiration time.
      self.send_json({'entry': {}})

      # Log stats.
      where = 'GS; ' + 'inline' if entry.content is not None else entry.key.id()
      stats.add_entry(stats.STORE, entry.compressed_size, where)

    finally:
      # Do not keep dangling futures. Note that error here is ignored,
      # memcache is just an optimization.
      if memcache_store_future:
        memcache_store_future.wait()


###


class RootHandler(webapp2.RequestHandler):
  """Tells the user to RTM."""
  def get(self):
    self.response.write(template.render('isolate/root.html'))


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    config.warmup()
    auth.warmup()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


def get_routes():
  # Namespace can be letters, numbers, '-', '.' and '_'.
  namespace = r'/<namespace:%s>' % model.NAMESPACE_RE
  # Do not enforce a length limit to support different hashing algorithm. This
  # should represent a valid hex value.
  hashkey = r'/<hash_key:[a-f0-9]{4,}>'
  # This means a complete key is required.
  namespace_key = namespace + hashkey

  return [
      # Administrative urls.
      webapp2.Route(
          r'/restricted', RestrictedAdminUIHandler),
      webapp2.Route(
          r'/restricted/whitelistip', acl.RestrictedWhitelistIPHandler),
      webapp2.Route(
          r'/restricted/gs_config', RestrictedGoogleStorageConfig),

      # Mapreduce related urls.
      webapp2.Route(
          r'/restricted/launch_map_reduce',
          RestrictedLaunchMapReduceJob),

      # The public API:
      webapp2.Route(
          r'/content-gs/handshake',
          HandshakeHandler),
      webapp2.Route(
          r'/content-gs/pre-upload/<namespace:.*>',
          PreUploadContentHandler),
      webapp2.Route(
          r'/content-gs/retrieve%s' % namespace_key,
          RetrieveContentHandler),
      webapp2.Route(
          r'/content-gs/store%s' % namespace_key,
          StoreContentHandler,
          name='store-gs'),

      # Public stats.
      webapp2.Route(r'/stats', stats_gviz.StatsHandler),
      webapp2.Route(
          r'/isolate/api/v1/stats/days', stats_gviz.StatsGvizDaysHandler),
      webapp2.Route(
          r'/isolate/api/v1/stats/hours', stats_gviz.StatsGvizHoursHandler),
      webapp2.Route(
          r'/isolate/api/v1/stats/minutes', stats_gviz.StatsGvizMinutesHandler),

      # AppEngine-specific url:
      webapp2.Route(r'/_ah/warmup', WarmupHandler),

      # Must be last.
      webapp2.Route(r'/', RootHandler),
  ]


def create_application(debug=False):
  """Creates the url router.

  The basic layouts is as follow:
  - /restricted/.* requires being an instance administrator.
  - /content/.* has the public HTTP API.
  - /stats/.* has statistics.
  """
  template.bootstrap()
  ereporter2.configure(
      lambda: config.settings().monitoring_recipients,
      handlers_common.should_ignore_error_record)

  routes = get_routes()
  routes.extend(ereporter2.get_frontend_routes())

  # Routes added to WSGIApplication only a dev mode.
  if utils.is_local_dev_server():
    routes.extend(gcs.URLSigner.switch_to_dev_mode())

  # Add some predefined groups when running on local dev server.
  if utils.is_local_dev_server():
    bootstrap_dev_server_acls()
  return webapp2.WSGIApplication(routes, debug=debug)
