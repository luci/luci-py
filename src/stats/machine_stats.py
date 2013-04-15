#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Machine Stats.

The model of the Machine Stats, and various helper functions.
"""


import datetime
import logging

from google.appengine.ext import db


class MachineStats(db.Model):
  """A machine's stats."""
  # The machine id of the polling machine.
  machine_id = db.StringProperty()

  # The tag of the machine polling.
  tag = db.StringProperty(default='')

  # The time of the assignment.
  assignment_time = db.DateTimeProperty(auto_now=True)


def RecordMachineAssignment(machine_id, machine_tag):
  """Records when a machine has a runner assigned.

  Args:
    machine_id: The machine id of the machine.
    machine_tag: The tag identifier of the machine.
  """
  machine_stats = MachineStats.gql('WHERE machine_id = :1',
                                   machine_id).get()

  # Check to see if we need to create the model.
  if machine_stats is None:
    machine_stats = MachineStats(machine_id=machine_id)

  if machine_tag is not None:
    machine_stats.tag = machine_tag
  machine_stats.assignment_time = datetime.datetime.now()
  machine_stats.put()


def DeleteMachineStats(key):
  """Delete the machine assignment referenced to by the given key.

  Args:
    key: The key of the machine assignment to delete.

  Returns:
    True if the key was valid and machine assignment was successfully deleted.
  """
  try:
    machine_stats = MachineStats.get(key)
  except (db.BadKeyError, db.BadArgumentError):
    logging.error('Invalid MachineStats key given, %s', str(key))
    return False

  if not machine_stats:
    logging.error('No MachineStats has key %s', str(key))
    return False

  machine_stats.delete()
  return True


def GetAllMachines(sort_by='machine_id'):
  """Get the list of whitelisted machines.

  Args:
    sort_by: The string of the attribute to sort the machines by.

  Returns:
    An iterator of all machines whitelisted.
  """
  # If we recieve an invalid sort_by parameter, just default to machine_id.
  if not sort_by in MachineStats.properties():
    sort_by = 'machine_id'

  return (machine for machine in MachineStats.gql('ORDER BY %s' % sort_by))


def GetMachineTag(machine_id):
  """Get the tag for a given machine id.

  Args:
    machine_id: The machine id to find the tag for

  Returns:
    The machine's tag, or None if the machine id isn't used.
  """
  machine = MachineStats.gql('WHERE machine_id = :1', machine_id).get()

  return machine.tag if machine else 'Unknown'
