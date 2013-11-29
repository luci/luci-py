# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""User Manager.

The User Manager is responsible for handling user profiles and whitelisting.
"""


import logging

from google.appengine.ext import ndb


# TODO(user): Machine should not be whitelisted, but just
# authenticate themselves with valid accounts.
class MachineWhitelist(ndb.Model):
  # The IP of the machine to whitelist.
  ip = ndb.StringProperty()

  # An optional password (NOT necessarily equal to the actual user
  # account password) used to ensure requests coming from a remote machine
  # are indeed valid. Defaults to None.
  password = ndb.StringProperty()


def AddWhitelist(ip, password=None):
  """Adds the given ip to the whitelist.

  Args:
    ip: The ip to be added. Ignores duplicate ips regardless of the password.
    password: Optional password to associate with the machine.
  """
  if MachineWhitelist.query().filter(MachineWhitelist.ip == ip).count(1):
    # Ignore duplicate requests. Note that the password is silently ignored.
    logging.info('Ignored duplicate whitelist request for ip: %s', ip)
    return

  MachineWhitelist(ip=ip, password=password).put()
  logging.debug('Stored ip: %s', ip)


def DeleteWhitelist(ip):
  """Removes the given ip from the whitelist.

  Args:
    ip: The ip to be removed. Ignores non-existing ips.
  """
  entries = MachineWhitelist.query(
      default_options=ndb.QueryOptions(keys_only=True)).filter(
        MachineWhitelist.ip == ip).fetch()
  if not entries:
    logging.info('Ignored missing remove whitelist request for ip: %s', ip)
    return

  ndb.delete_multi(entries)
  logging.debug('Removed ip: %s', ip)


def IsWhitelistedMachine(ip, password):
  """Return True if the given ip and password are whitelisted.

  Args:
    ip: IP of the client making the request.
    password: The password provided by the client making the request.

  Returns:
    True if the machine referenced is whitelisted.
  """
  entries = MachineWhitelist.query().filter(
      MachineWhitelist.ip == ip).filter(
          MachineWhitelist.password == password).count(1)
  return bool(entries)
