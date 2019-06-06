# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Helpers for managing AuthDB dump in Google Cloud Storage."""

from components import net


class Error(Exception):
  """Raised on fatal errors when calling Google Storage."""


def is_authorized_reader(email):  # pylint: disable=unused-argument
  """True if the given user is allowed to fetch AuthDB Google Storage file."""
  # TODO(vadimsh): Implement.
  return False


def authorize_reader(email):  # pylint: disable=unused-argument
  """Allows the given user to fetch AuthDB Google Storage file."""
  # TODO(vadimsh): Implement.


def deauthorize_reader(email):  # pylint: disable=unused-argument
  """Revokes the authorization to fetch AuthDB Google Storage file."""
  # TODO(vadimsh): Implement.


def revoke_stale_authorization():
  """Removes authorization from accounts that no longer have access."""
  # TODO(vadimsh): Implement.


def upload_file(path, data, content_type):
  """Overwrites a file in GCS, makes it readable to all authorized readers.

  Doesn't use streaming uploads currently. Data is limited by URL Fetch request
  size (10 MB).

  Args:
    path: "<bucket>/<object>" string.
    data: buffer with data to upload.
    content_type:

  Raises:
    Error if Google Storage writes fail.
  """
  # See https://cloud.google.com/storage/docs/json_api/v1/how-tos/simple-upload.
  bucket, name = path.split('/', 1)
  try:
    # TODO(vadimsh): Set ACLs.
    net.request(
        url='https://www.googleapis.com/upload/storage/v1/b/%s/o' % bucket,
        method='POST',
        payload=data,
        params={'uploadType': 'media', 'name': name},
        headers={'Content-Type': content_type},
        scopes=['https://www.googleapis.com/auth/cloud-platform'],
        deadline=30)
  except net.Error as exc:
    raise Error(str(exc))
