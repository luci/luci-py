# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Policies for machines in the Machine Provider Catalog."""

from protorpc import messages


class MachineReclamationPolicy(messages.Enum):
  """Lists valid machine reclamation policies."""
  # Make the machine available for lease again immediately.
  MAKE_AVAILABLE = 1
  # Keep the machine in the Catalog, but prevent it from being leased out.
  RECLAIM = 2
  # Delete the machine from the Catalog.
  DELETE = 3


class Policies(messages.Message):
  """Represents the policies for a machine."""
  # Cloud Pub/Sub topic name to communicate on regarding this machine.
  pubsub_topic = messages.StringField(1)
  # Cloud Pub/Sub project to communicate on regarding this machine.
  pubsub_project = messages.StringField(2)
  # Action the Machine Provider should take when reclaiming a machine
  # from a lessee.
  on_reclamation = messages.EnumField(
      MachineReclamationPolicy,
      3,
      default=MachineReclamationPolicy.MAKE_AVAILABLE,
  )
