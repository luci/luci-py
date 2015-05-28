# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Helper function for working with ACLs."""

import logging

from components import auth


def can_issue_lease_requests():
  """Returns whether the current user may issue lease requests.

  Returns:
    True if the current user may issue lease requests, otherwise False.
  """
  if auth.get_current_identity().is_anonymous:
    logging.info('User is not logged in')
    return False
  return True
