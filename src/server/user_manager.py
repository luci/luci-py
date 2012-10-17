#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""User Manager.

The User Manager is responsible for handling user profiles and whitelisting.
"""


import logging

from google.appengine.ext import db


# TODO(user): Machine should not be whitelisted, but just
# authenticate themselves with valid accounts.
class MachineWhitelist(db.Model):
  # The IP of the machine to whitelist.
  ip = db.StringProperty()

  # An optional password (NOT necessarily equal to the actual user
  # account password) used to ensure requests coming from a remote machine
  # are indeed valid. Defaults to None.
  password = db.StringProperty()


def AddWhitelist(ip, password=None):
  """Adds the given ip to the whitelist.

  Args:
    ip: The ip to be added. Ignores duplicate ips regardless of the password.
    password: Optional password to associate with the machine.
  """
  # Find existing entries, if any.
  query = MachineWhitelist.gql('WHERE ip = :1', ip)

  # Sanity check.
  assert query.count() < 2

  # Ignore duplicate requests.
  if query.count() == 0:
    machine_whitelist = MachineWhitelist(ip=ip, password=password)
    machine_whitelist.put()
    logging.debug('Stored ip: %s', ip)
  else:
    logging.info('Ignored duplicate whitelist request for ip: %s', ip)


def DeleteWhitelist(ip):
  """Removes the given ip from the whitelist.

  Args:
    ip: The ip to be removed. Ignores non-existing ips.
  """
  # Find existing entries, if any.
  query = MachineWhitelist.gql('WHERE ip = :1', ip)

  # Sanity check.
  assert query.count() < 2

  # Ignore non-existing requests.
  if query.count() == 1:
    # Delete existing entry.
    white_list = query.get()
    white_list.delete()
    logging.debug('Removed ip: %s', ip)
  else:
    logging.info('Ignored missing remove whitelist request for ip: %s', ip)


def IsWhitelistedMachine(ip, password):
  """Return True if the given ip and password are whitelisted.

  Args:
    ip: IP of the client making the request.
    password: The password provided by the client making the request.

  Returns:
    True if the machine referenced is whitelisted.
  """
  whitelist = MachineWhitelist.gql(
      'WHERE ip = :1 AND password = :2', ip, password)

  return whitelist.count() == 1
