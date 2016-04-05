# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Dimensions for the Machine Provider."""

from protorpc import messages


class Backend(messages.Enum):
  """Lists valid backends."""
  DUMMY = 0
  GCE = 1


class OSFamily(messages.Enum):
  """Lists valid OS families."""
  LINUX = 1
  OSX = 2
  WINDOWS = 3


class Dimensions(messages.Message):
  """Represents the dimensions of a machine."""
  # The operating system family of this machine.
  os_family = messages.EnumField(OSFamily, 1)
  # The backend which should be used to spin up this machine. This should
  # generally be left unspecified so the Machine Provider selects the backend
  # on its own.
  backend = messages.EnumField(Backend, 2)
  # The hostname of this machine.
  hostname = messages.StringField(3)
  # The number of CPUs available to this machine.
  num_cpus = messages.IntegerField(4)
  # The amount of memory available to this machine.
  memory_gb = messages.FloatField(5)
  # The disk space available to this machine.
  disk_gb = messages.IntegerField(6)
