# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Helpers for uploading AuthDB to Google Cloud Storage."""

from components import net


def upload_file(path, data, content_type):
  """Overwrites a file in GCS.

  Doesn't use streaming uploads currently. Data is limited by URL Fetch request
  size (10 MB).

  Args:
    path: "<bucket>/<object>" string.
    data: buffer with data to upload.
    content_type:

  Raises:
    net.Error if Google Storage writes fail.
  """
  # See https://cloud.google.com/storage/docs/json_api/v1/how-tos/simple-upload.
  bucket, name = path.split('/', 1)
  net.request(
      url='https://www.googleapis.com/upload/storage/v1/b/%s/o' % bucket,
      method='POST',
      payload=data,
      params={'uploadType': 'media', 'name': name},
      headers={'Content-Type': content_type},
      scopes=['https://www.googleapis.com/auth/cloud-platform'],
      deadline=30)
