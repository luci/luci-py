# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Access control groups for ereporter2."""

from components import auth


# Name of a group that lists users that receive ereporter2 reports.
RECIPIENTS_AUTH_GROUP = 'ereporter2-reports'

# Group that can view all ereporter2 reports without being a general auth admin.
# It can also silence reports.
VIEWERS_AUTH_GROUP = 'ereporter2-viewers'


def get_ereporter2_recipients():
  """Returns list of emails to send reports to."""
  return [x.name for x in auth.list_group(RECIPIENTS_AUTH_GROUP) if x.is_user]


def is_ereporter2_viewer():
  """True if current user is in recipients list, viewer list or is an admin."""
  if auth.is_admin() or auth.is_group_member(VIEWERS_AUTH_GROUP):
    return True
  ident = auth.get_current_identity()
  return ident.is_user and ident.name in get_ereporter2_recipients()


def is_ereporter2_editor():
  """Only auth admins or recipients can edit the silencing filters."""
  return auth.is_admin() or auth.is_group_member(RECIPIENTS_AUTH_GROUP)
