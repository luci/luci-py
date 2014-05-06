# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Machine Stats.

The model of the Machine Stats, and various helper functions.
"""

import datetime
import json
import logging

from google.appengine.ext import ndb

from components import utils


# The number of hours that have to pass before a machine is considered dead.
MACHINE_DEATH_TIMEOUT = datetime.timedelta(hours=3)

# The amount of time that needs to pass before the last_seen field of
# MachineStats will update for a given machine, to prevent too many puts.
MACHINE_UPDATE_TIME = datetime.timedelta(hours=1)

# The dict of acceptable keys to sort MachineStats by, with the key as the key
# and the value as the human readable name.
ACCEPTABLE_SORTS = {
    'dimensions': 'Dimensions',
    'last_seen': 'Last Seen',
    'machine_id': 'Machine ID',
    'tag': 'Tag',
}


def utcnow():
  """To be mocked in tests."""
  return datetime.datetime.utcnow()


class MachineStats(ndb.Model):
  """A machine's stats."""
  # The tag of the machine polling.
  tag = ndb.StringProperty(default='')

  # The dimensions of the machine polling.
  dimensions = ndb.StringProperty(default='')

  # The last time the machine queried for work.
  # Don't use auto_now_add so we control exactly what the time is set to
  # (since we later need to compare this value, so we need to know if it was
  # made with .now() or .utcnow()).
  last_seen = ndb.DateTimeProperty(required=True)

  # The machine id, which is also the model's key.
  machine_id = ndb.ComputedProperty(lambda self: self.key.string_id())

  def _pre_put_hook(self):
    """Stores the creation time for this model."""
    if not self.last_seen:
      self.last_seen = utcnow()

  def to_dict(self):
    """Converts dimensions from json to dict."""
    out = super(MachineStats, self).to_dict()
    try:
      out['dimensions'] = json.loads(out['dimensions'] or '{}')
    except ValueError:
      # TODO(maruel): Disallow creating entities with bad 'dimensions' values.
      # For now, cope with the current entities.
      logging.error('Failed to encode %s', repr(out.get('dimensions')))
    return out


def RecordMachineQueriedForWork(machine_id, dimensions, machine_tag):
  """Records when a machine has queried for work.

  Args:
    machine_id: The machine id of the machine.
    dimensions: The machine dimensions.
    machine_tag: The tag identifier of the machine.
  """
  # TODO(maruel): Put the entities into an entity group.
  dimensions_str = utils.encode_to_json(dimensions)

  machine_stats = MachineStats.get_by_id(machine_id)
  if (machine_stats and
      (machine_stats.last_seen + MACHINE_UPDATE_TIME >= utcnow()) and
      machine_stats.dimensions == dimensions_str and
      machine_stats.tag == machine_tag):
    return
  if not machine_stats:
    machine_stats = MachineStats(id=machine_id)
  machine_stats.dimensions = dimensions_str
  machine_stats.last_seen = utcnow()
  machine_stats.tag = machine_tag
  machine_stats.put()


def DeleteMachineStats(key):
  """Delete the machine assignment referenced to by the given key.

  Args:
    key: The key of the machine assignment to delete.

  Returns:
    True if the key was valid and machine assignment was successfully deleted.
  """
  try:
    key = ndb.Key(MachineStats, key)
    if not key.get():
      logging.warning('No MachineStats has key: %s', str(key))
      return False
  except Exception:
    # All exceptions must be caught because some exceptions can only be caught
    # this way. See this bug report for more details:
    # https://code.google.com/p/appengine-ndb-experiment/issues/detail?id=143
    logging.error('Invalid MachineStats key given, %s', str(key))
    return False

  key.delete()
  return True


def GetAllMachines(sort_by='machine_id'):
  """Get the list of whitelisted machines.

  Args:
    sort_by: The string of the attribute to sort the machines by.

  Returns:
    An iterator of all machines whitelisted.
  """
  # If we receive an invalid sort_by parameter, just default to machine_id.
  if sort_by not in ACCEPTABLE_SORTS:
    sort_by = 'machine_id'

  return (machine for machine in MachineStats.gql('ORDER BY %s' % sort_by))


def GetMachineTag(machine_id):
  """Get the tag for a given machine id.

  Args:
    machine_id: The machine id to find the tag for

  Returns:
    The machine's tag, or None if the machine id isn't used.
  """
  machine = MachineStats.get_by_id(machine_id) if machine_id else None

  return machine.tag if machine else 'Unknown'
