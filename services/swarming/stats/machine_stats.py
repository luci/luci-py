# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Machine Stats.

The model of the Machine Stats, and various helper functions.
"""


import datetime
import logging

from google.appengine.api import app_identity
from google.appengine.ext import ndb

from server import admin_user


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
      self.last_seen = datetime.datetime.utcnow()

  @property
  def last_seen_str(self):
    """Returns a shorter version of self.last_seen as a str."""
    return self.last_seen.strftime('%Y-%m-%d %H:%M')


def FindDeadMachines():
  """Finds all dead machines.

  Returns:
    A list of the dead machines.
  """
  dead_machine_cutoff = (datetime.datetime.utcnow() - MACHINE_DEATH_TIMEOUT)

  return list(MachineStats.gql('WHERE last_seen < :1', dead_machine_cutoff))


def NotifyAdminsOfDeadMachines(dead_machines):
  """Notifies the admins of the dead_machines detected.

  Args:
    dead_machines: The list of the currently dead machines.

  Returns:
    True if the email was successfully sent.
  """
  death_list = (
      '  %s (%s)   %s' % (m.machine_id, m.tag, m.last_seen_str)
      for m in dead_machines)

  body = (
    'The following registered machines haven\'t been active in at least %s.\n'
    'They are assumed to be dead:\n'
    '%s\n') % (MACHINE_DEATH_TIMEOUT, '\n'.join(sorted(death_list)))

  subject = 'Dead Machines on %s' % app_identity.get_application_id()
  return admin_user.EmailAdmins(subject, body)


def RecordMachineQueriedForWork(machine_id, dimensions_str, machine_tag):
  """Records when a machine has queried for work.

  Args:
    machine_id: The machine id of the machine.
    dimensions_str: The string representation of the machines dimensions.
    machine_tag: The tag identifier of the machine.
  """
  machine_stats = MachineStats.get_or_insert(machine_id)

  if (machine_stats.dimensions != dimensions_str or
      (machine_stats.last_seen + MACHINE_UPDATE_TIME <
       datetime.datetime.utcnow()) or
      machine_stats.tag != machine_tag):
    machine_stats.dimensions = dimensions_str
    machine_stats.last_seen = datetime.datetime.utcnow()
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
      logging.error('No MachineStats has key: %s', str(key))
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
