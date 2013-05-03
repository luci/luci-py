#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Machine Stats.

The model of the Machine Stats, and various helper functions.
"""


import datetime
import logging

from google.appengine.api import app_identity
from google.appengine.api import mail
from google.appengine.ext import db
from server import admin_user


# The number of days that have to pass before a machine is considered dead.
MACHINE_TIMEOUT_IN_DAYS = 3

# The message to use for each dead machine.
_INDIVIDUAL_DEAD_MACHINE_MESSAGE = (
    'Machine %(machine_id)s(%(machine_tag)s) was last seen %(last_seen)s and '
    'is assumed to be dead.')

# The message body of the dead machine message to send admins.
_DEAD_MACHINE_MESSAGE_BODY = """Hello,

The following registered machines haven't been active in %(timeout)s days.

%(death_summary)s

Please revive the machines or remove them from the list of active machines.
"""


class MachineStats(db.Model):
  """A machine's stats."""
  # The machine id of the polling machine.
  machine_id = db.StringProperty(required=True)

  # The tag of the machine polling.
  tag = db.StringProperty(default='')

  # The last day the machine queried for work.
  last_seen = db.DateProperty(required=True)


def FindDeadMachines():
  """Find all dead machines.

  Returns:
    A list of the dead machines.
  """
  dead_machine_cutoff = (datetime.date.today() -
                         datetime.timedelta(days=MACHINE_TIMEOUT_IN_DAYS))

  return list(MachineStats.gql('WHERE last_seen < :1', dead_machine_cutoff))


def NotifyAdminsOfDeadMachines(dead_machines):
  """Notify the admins of the dead_machines detected.

  Args:
    dead_machines: The list of the currently dead machines.

  Returns:
    True if the email was successfully sent.
  """
  if admin_user.AdminUser.all().count() == 0:
    logging.error('No admins found, no one to notify of dead machines')
    return False

  death_summary = []
  for machine in dead_machines:
    death_summary.append(
        _INDIVIDUAL_DEAD_MACHINE_MESSAGE % {'machine_id': machine.machine_id,
                                            'machine_tag': machine.tag,
                                            'last_seen': machine.last_seen})

  app_id = app_identity.get_application_id()

  message = mail.EmailMessage()
  message.sender = 'dead_machine_detecter@%s.appspotmail.com' % app_id
  message.to = ','.join(admin.email for admin in admin_user.AdminUser.all())
  message.subject = 'Dead Machines Found on %s' % app_id

  message.body = _DEAD_MACHINE_MESSAGE_BODY % {
      'timeout': MACHINE_TIMEOUT_IN_DAYS,
      'death_summary': '\n'.join(death_summary)}

  message.send()

  return True


def RecordMachineQueriedForWork(machine_id, machine_tag):
  """Records when a machine has queried for work.

  Args:
    machine_id: The machine id of the machine.
    machine_tag: The tag identifier of the machine.
  """
  machine_stats = MachineStats.gql('WHERE machine_id = :1',
                                   machine_id).get()

  # Check to see if we need to create the model.
  if machine_stats is None:
    machine_stats = MachineStats(machine_id=machine_id,
                                 last_seen=datetime.date.today())

  if machine_tag is not None:
    machine_stats.tag = machine_tag

  if machine_stats.last_seen < datetime.date.today():
    machine_stats.last_seen = datetime.date.today()

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
  machine = db.GqlQuery('SELECT tag FROM MachineStats WHERE machine_id = :1 '
                        'LIMIT 1', machine_id).get()

  return machine.tag if machine else 'Unknown'
