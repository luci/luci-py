# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Discovery service implementation."""

from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool

import service_pb2
import service_prpc_pb2


class FileNotFound(Exception):
  """Raised when a proto file is not found."""

  def __init__(self, name):
    super(FileNotFound, self).__init__(
        'Proto file "%s" is not found in the default protobuf descriptor pool. '
        'Ensure all _pb2.py are imported.' % name)


class Discovery(object):
  DESCRIPTION = service_prpc_pb2.DiscoveryServiceDescription

  def __init__(self):
    self._response = service_pb2.DescribeResponse()
    self._files = set()

  def Describe(self, _request, _ctx):
    return self._response

  def add_service(self, service_description):
    pool = descriptor_pool.Default()

    def ensure_file(name):
      if name in self._files:
        return
      desc = pool.FindFileByName(name)
      if not desc:
        raise FileNotFound(name)

      proto = self._response.description.file.add()
      desc.CopyToProto(proto)
      for dep in proto.dependency:
        ensure_file(dep)

    full_name = service_description['descriptor'].name
    if service_description['package']:
      full_name = '%s.%s' % (service_description['package'], full_name)
    self._response.services.append(full_name)

    ensure_file(service_description['file'])
