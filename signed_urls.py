# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Code to generate signed Google Storage URLs.

See docs for more information:
  https://developers.google.com/storage/docs/accesscontrol#Signed-URLs
"""

import base64
import datetime
import time
import urllib

import Crypto.Hash.SHA256 as SHA256
import Crypto.PublicKey.RSA as RSA
import Crypto.Signature.PKCS1_v1_5 as PKCS1_v1_5


class CloudStorageURLSigner(object):
  """Object that can generated signed Google Storage URLs."""

  # Default expiration time for signed links.
  DEFAULT_EXPIRATION = datetime.timedelta(hours=4)

  def __init__(self, bucket, client_id, private_key):
    self.bucket = str(bucket)
    self.client_id = str(client_id)
    self.private_key = CloudStorageURLSigner.load_private_key(private_key)

  @staticmethod
  def load_private_key(private_key):
    """Converts base64 *.der private key into RSA key instance."""
    binary = base64.b64decode(private_key)
    return RSA.importKey(binary)

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

    # Sign it with RSA-SHA256.
    signer = PKCS1_v1_5.new(self.private_key)
    signature = base64.b64encode(signer.sign(SHA256.new(data_to_sign)))

    # Construct final URL.
    query_params = urllib.urlencode([
        ('GoogleAccessId', self.client_id),
        ('Expires', expires),
        ('Signature', signature),
    ])
    return 'https://%s.storage.googleapis.com/%s?%s' % (
        self.bucket, filename, query_params)

  def get_download_url(self, filename, expiration=DEFAULT_EXPIRATION):
    """Returns signed URL that can be used to download a file."""
    return self.get_signed_url(filename, 'GET', expiration=expiration)

  def get_upload_url(self, filename, expiration=DEFAULT_EXPIRATION,
                     content_type='', content_md5=''):
    """Returns signed URL that can be used to upload a file."""
    return self.get_signed_url(filename, 'PUT', expiration=expiration,
        content_type=content_type, content_md5=content_md5)
