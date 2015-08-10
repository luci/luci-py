# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Helper functions for working with ACLs."""

import logging

from components import auth

import rpc_messages


def is_logged_in():
  """Returns whether the current used is logged in."""
  if auth.get_current_identity().is_anonymous:
    logging.info('User is not logged in')
    return False
  return True


def is_catalog_admin():
  """Returns whether the current user is a catalog administrator."""
  # TODO: Implement a permissions model that allows admins to be set.
  return False


def is_backend_service():
  """Returns whether the current user is a recognized backend."""
  return is_logged_in()


def get_current_backend():
  """Returns the backend associated with the current user.

  The user must be a recognized Machine Provider backend.

  Returns:
    An rpc_messages.Backend instance representing the current user.
  """
  return rpc_messages.Backend.DUMMY


def is_backend_service_or_catalog_admin():
  return is_backend_service() or is_catalog_admin()


def can_issue_lease_requests():
  """Returns whether the current user may issue lease requests."""
  return is_logged_in()
