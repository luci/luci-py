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


@ndb.tasklet
def AddWhitelist(ip):
  """Adds the given IP address to the whitelist."""
  res = yield MachineWhitelist.query().filter(
      MachineWhitelist.ip == ip).count_async(1)
  if not res:
    yield MachineWhitelist(ip=ip).put_async()
    logging.debug('Stored ip: %s', ip)
  raise ndb.Return(None)


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


def IsWhitelistedMachine(ip):
  """Return True if the given IP is whitelisted.

  Returns:
    True if the machine referenced is whitelisted.
  """
  entries = MachineWhitelist.query().filter(
      MachineWhitelist.ip == ip).count(1)
  return bool(entries)
