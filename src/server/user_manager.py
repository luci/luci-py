#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""User Manager.

The User Manager is responsible for handling user profiles and whitelisting.
"""


import logging

from google.appengine.ext import db


class UserProfile(db.Model):
  """A user profile.

  All TestRequest and TestRunner objects are associated with a specific user.
  A user has a whitelist of machine IPs that are allowed to interact with its
  data. A UserProfile can be retrieved using the user's email address as
  the key.

  A UserProfile has a 'whitelist' list of MachineWhitelist of authorized remote
  machines.
  A UserProfile has a 'test_requests' list of TestRequest of tests belonging
  to this user.
  """
  # The actual account of the user.
  user = db.UserProperty()

  def DeleteProfile(self):
    # Deletes the profile, its whitelists and test cases.
    for test_case in self.test_requests:
      test_case.delete()
    for whitelist in self.whitelist:
      whitelist.delete()
    self.delete()


class MachineWhitelist(db.Model):
  """A machine IP as part of a UserProfile whitelist."""
  # A reference to the user's profile.
  user_profile = db.ReferenceProperty(
      UserProfile, required=True, collection_name='whitelist')

  # The IP of the machine to whitelist.
  ip = db.StringProperty()

  # An optional password (NOT necessarily equal to the actual user
  # account password) used to ensure requests coming from a remote machine
  # are indeed valid. Defaults to None.
  password = db.StringProperty()


def ModifyUserProfileAddWhitelist(user, ip, password=None):
  """Adds the given ip to the whitelist of the current user.

  If a user profile doesn't already exist, one will be created first.
  This function is thread safe.

  Args:
    user: The AppEngine user modifying its whitelist.
    ip: The ip to be added. Ignores duplicate ips regardless of the password.
    password: Optional password to associate with the machine.

  Returns:
    True if request was valid.
  """
  if not user:
    logging.error('User not signed in? Security breach.')
    return False

  # Atomically create the user profile or use an existing one.
  # Handle normal transaction exceptions for get_or_insert.
  try:
    user_profile = UserProfile.get_or_insert(user.email(), user=user)
  except (db.TransactionFailedError, db.Timeout, db.InternalError) as e:
    # This is a low-priority request. Abort on any failures.
    logging.exception('User profile creation exception: %s', str(e))
    return False

  assert user_profile

  # Find existing entries, if any.
  query = user_profile.whitelist.filter('ip =', ip)

  # Sanity check.
  assert query.count() < 2

  # Ignore duplicate requests.
  if query.count() == 0:
    # Create a new entry.
    white_list = MachineWhitelist(
        user_profile=user_profile, ip=ip, password=password)
    white_list.put()
    logging.debug('Stored ip: %s', ip)
  else:
    logging.info('Ignored duplicate whitelist request for ip: %s', ip)

  return True


def ModifyUserProfileDelWhitelist(user, ip):
  """Removes the given ip from the whitelist of the current user.

  Args:
    user: The AppEngine user modifying its whitelist.
    ip: The ip to be removed. Ignores non-existing ips.

  Returns:
    True if request was valid.
  """
  if not user:
    logging.error('User not signed in? Security breach.')
    return False

  user_profile = GetUserProfile(user)
  if not user_profile:
    return False

  # Find existing entries, if any.
  query = user_profile.whitelist.filter('ip =', ip)

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

  return True


def FindUserWithWhitelistedIP(ip, password):
  """Finds and returns the user that has whitelisted the given IP.

  Args:
    ip: IP of the client making the request.
    password: The password provided by the client making the request.

  Returns:
    UserProfile of the user that has whitelisted the IP, None otherwise.
  """
  whitelist = MachineWhitelist.gql(
      'WHERE ip = :1 AND password = :2', ip, password)

  if whitelist.count() == 0:
    return None

  # Sanity check.
  assert whitelist.count() == 1

  return whitelist.get().user_profile


def GetUserProfile(user):
  """Return the user profile that the given user corresponds to.

  Args:
    user: The user to find the profile for.

  Returns:
    The user profile of the user, or None if it doesn't exist.
  """
  return UserProfile.gql('WHERE user = :1', user).get()


