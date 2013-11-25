# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Accesses files on Google Cloud Storage via Google Cloud Storage Client API.

References:
  https://developers.google.com/appengine/docs/python/googlecloudstorageclient/
  https://developers.google.com/storage/docs/accesscontrol#Signed-URLs
"""

import base64
import collections
import datetime
import logging
import time
import urllib

import Crypto.Hash.SHA256 as SHA256
import Crypto.PublicKey.RSA as RSA
import Crypto.Signature.PKCS1_v1_5 as PKCS1_v1_5

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=F0401
import webapp2
# pylint: enable=F0401

import cloudstorage

# Export some exceptions for users of this module.
# pylint: disable=W0611
from cloudstorage.errors import (
    AuthorizationError,
    FatalError,
    ForbiddenError,
    NotFoundError,
    TransientError)

import config


# The limit is 32 megs but it's a tad on the large side. Use 512kb chunks
# instead to not clog memory when there's multiple concurrent requests being
# served.
CHUNK_SIZE = 512 * 1024


# Return value for get_file_info call.
FileInfo = collections.namedtuple('FileInfo', ['size'])


def list_files(bucket, subdir=None, batch_size=100):
  """Yields filenames of files inside subdirectory of some bucket.

  It always lists directories recursively.

  Arguments:
    bucket: a bucket to list.
    subdir: subdirectory to list files from or None for an entire bucket.

  Yields:
    Files names relative to the bucket root directory.
  """
  # When listing an entire bucket, gcs expects /<bucket> without ending '/'.
  path_prefix = '/%s/%s' % (bucket, subdir) if subdir else '/%s' % bucket
  bucket_prefix = '/%s/' % bucket
  marker = None
  while True:
    stats = cloudstorage.listbucket(
        path_prefix=path_prefix,
        marker=marker,
        max_keys=batch_size)
    # |stats| is an iterable, need to iterate through it to figure out
    # whether it's empty or not.
    empty = True
    for stat in stats:
      empty = False
      assert stat.filename.startswith(bucket_prefix)
      yield stat.filename[len(bucket_prefix):]
      # Restart next listing from the last fetched file.
      marker = stat.filename
    # Last batch was empty -> listed all files.
    if empty:
      break


def delete_files(bucket, filenames):
  """Deletes multiple files stored in GS.

  Arguments:
    bucket: a bucket that contains the files.
    filenames: list of file paths to delete (relative to a bucket root).

  Returns:
    An empty list so this function can be used with functions that expect
    the RPC to return a Future.
  """
  # Sadly Google Cloud Storage client library doesn't support batch deletes,
  # so do it one by one.
  for filename in filenames:
    try:
      cloudstorage.delete('/%s/%s' % (bucket, filename))
    except cloudstorage.errors.NotFoundError:
      logging.warning(
          'Trying to delete a GS file that\'s not there: /%s/%s',
          bucket, filename)
  return []


def get_file_info(bucket, filename):
  """Returns information about stored file.

  Arguments:
    bucket: a bucket that contains the file.
    filename: path to a file relative to bucket root.

  Returns:
    FileInfo object or None if no such file.
  """
  try:
    stat = cloudstorage.stat('/%s/%s' % (bucket, filename))
    return FileInfo(size=stat.st_size)
  except cloudstorage.errors.NotFoundError:
    return None


def read_file(bucket, filename, chunk_size=CHUNK_SIZE):
  """Reads a file and yields its content in chunks of a given size.

  Arguments:
    bucket: a bucket that contains the file.
    filename: name of the file to read.
    chunk_size: maximum size of a chunk to read and yield.

  Yields:
    Chunks of a file (as str objects).
  """
  path = '/%s/%s' % (bucket, filename)
  bytes_read = 0
  data = None
  file_ref = None
  try:
    with cloudstorage.open(path, read_buffer_size=chunk_size) as file_ref:
      while True:
        data = file_ref.read(chunk_size)
        if not data:
          break
        bytes_read += len(data)
        yield data
        # Remove reference to a buffer so it can be GC'ed.
        data = None
  except Exception as exc:
    logging.warning(
        'Exception while reading \'%s\', read %d bytes: %s %s',
        path, bytes_read, exc.__class__.__name__, exc)
    raise
  finally:
    # Remove lingering references to |data| and |file_ref| so they get GC'ed
    # sooner. Otherwise this function's frame object keeps references to them,
    # A frame object is around as long as there are references to this
    # generator instance somewhere.
    data = None
    file_ref = None


def write_file(bucket, filename, content):
  """Stores the given content as a file in Google Storage.

  Overwrites a file if it exists.

  Arguments:
    bucket: a bucket to store a file to.
    filename: name of the file to write.
    content: iterable that produces chunks of a content to write.

  Returns:
    True if successfully written a file, False on error.
  """
  written = 0
  last_chunk_size = 0
  try:
    with cloudstorage.open('/%s/%s' % (bucket, filename), 'w') as f:
      for chunk in content:
        last_chunk_size = len(chunk)
        f.write(chunk)
        written += last_chunk_size
    return True
  except cloudstorage.errors.Error as exc:
    logging.error(
        'Failed to write to a GS file.\n'
        '\'/%s/%s\', wrote %d bytes, failed at writting %d bytes: %s %s',
        bucket, filename, written, last_chunk_size, exc.__class__.__name__, exc)
    # Delete an incomplete file.
    delete_files(bucket, [filename])
    return False


class URLSigner(object):
  """Object that can generated signed Google Storage URLs."""

  # Default expiration time for signed links.
  DEFAULT_EXPIRATION = datetime.timedelta(hours=4)

  # Google Storage URL template for a singed link.
  GS_URL = 'https://%(bucket)s.storage.googleapis.com/%(filename)s?%(query)s'

  # True if switched to a local dev mode.
  DEV_MODE_ENABLED = False

  @staticmethod
  def switch_to_dev_mode():
    """Enables GS mock for a local dev server.

    Returns:
      List of webapp2.Routes objects to add to the application.
    """
    assert config.is_local_dev_server(), 'Must not be run in production'
    if not URLSigner.DEV_MODE_ENABLED:
      # Replace GS_URL with a mocked one.
      URLSigner.GS_URL = (
          'http://%s/_gcs_mock/' % config.get_local_dev_server_host())
      URLSigner.GS_URL += '%(bucket)s/%(filename)s?%(query)s'
      URLSigner.DEV_MODE_ENABLED = True

    class LocalStorageHandler(webapp2.RequestHandler):
      """Handles requests to a mock GS implementation."""

      def get(self, bucket, filepath):
        """Read a file from a mocked GS, return 404 if not found."""
        try:
          with cloudstorage.open('/%s/%s' % (bucket, filepath), 'r') as f:
            self.response.out.write(f.read())
          self.response.headers['Content-Type'] = 'application/octet-stream'
        except cloudstorage.errors.NotFoundError:
          self.abort(404)

      def put(self, bucket, filepath):
        """Stores a file in a mocked GS."""
        with cloudstorage.open('/%s/%s' % (bucket, filepath), 'w') as f:
          f.write(self.request.body)

    endpoint = r'/_gcs_mock/<bucket:[a-z0-9\.\-_]+>/<filepath:.*>'
    return [webapp2.Route(endpoint, LocalStorageHandler)]

  def __init__(self, bucket, client_id, private_key):
    self.bucket = str(bucket)
    self.client_id = str(client_id)
    self.private_key = URLSigner.load_private_key(private_key)

  @staticmethod
  def load_private_key(private_key):
    """Converts base64 *.der private key into RSA key instance."""
    # Empty private key is ok in a dev mode.
    if URLSigner.DEV_MODE_ENABLED and not private_key:
      return None
    binary = base64.b64decode(private_key)
    return RSA.importKey(binary)

  def generate_signature(self, data_to_sign):
    """Signs |data_to_sign| with a private key and returns a signature."""
    # Signatures are not used in a dev mode.
    if self.DEV_MODE_ENABLED:
      return 'fakesig'
    # Sign it with RSA-SHA256.
    signer = PKCS1_v1_5.new(self.private_key)
    signature = base64.b64encode(signer.sign(SHA256.new(data_to_sign)))
    return signature

  def get_signed_url(self, filename, http_verb, expiration=DEFAULT_EXPIRATION,
                     content_type='', content_md5=''):
    """Returns signed URL that can be used by clients to access a file."""
    # Prepare data to sign.
    filename = str(filename)
    expires = str(int(time.time() + expiration.total_seconds()))
    data_to_sign = '\n'.join([
        http_verb,
        content_md5,
        content_type,
        expires,
        '/%s/%s' % (self.bucket, filename),
    ])
    # Construct final URL.
    query_params = urllib.urlencode([
        ('GoogleAccessId', self.client_id),
        ('Expires', expires),
        ('Signature', self.generate_signature(data_to_sign)),
    ])
    return self.GS_URL % {
        'bucket': self.bucket,
        'filename': filename,
        'query': query_params}

  def get_download_url(self, filename, expiration=DEFAULT_EXPIRATION):
    """Returns signed URL that can be used to download a file."""
    return self.get_signed_url(filename, 'GET', expiration=expiration)

  def get_upload_url(self, filename, expiration=DEFAULT_EXPIRATION,
                     content_type='', content_md5=''):
    """Returns signed URL that can be used to upload a file."""
    return self.get_signed_url(filename, 'PUT', expiration=expiration,
        content_type=content_type, content_md5=content_md5)
