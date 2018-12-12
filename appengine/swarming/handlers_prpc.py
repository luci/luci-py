# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines Swarming Server frontend pRPC handlers."""

from components import prpc

from proto import swarming_prpc_pb2 # pylint: disable=no-name-in-module


class SwarmingService(object):
  """Service implements the pRPC service in swarming.proto."""

  DESCRIPTION = swarming_prpc_pb2.SwarmingServiceDescription

  # TODO(maruel): Add implementation. https://crbug.com/913953


def get_routes():
  s = prpc.Server()
  s.add_service(SwarmingService())
  return s.get_routes()
