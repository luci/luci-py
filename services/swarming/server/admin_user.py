# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Basic admin helper function."""


import logging

from google.appengine.api import app_identity
from google.appengine.api import mail
from google.appengine.ext import db


class AdminUser(db.Model):
  """A email address of the admin to send the exception emails.

  If there isn't a valid instance of these then no emails are sent.
  """
  # The email to send the exception emails from.
  email = db.StringProperty()


def EmailAdmins(subject, body):
  """Emails the admins the given message and subject.

  Args:
    subject: The subject of the email.
    body: The body of the email.

  Returns:
    True if the email was sucessfully sent.
  """
  if AdminUser.all().count() == 0:
    logging.error('No admins found, no one to email')
    return False

  send_to = ','.join(admin.email for admin in AdminUser.all())
  server_name = app_identity.get_application_id()
  server_email = '%s <no_reply@%s.appspotmail.com>' % (server_name, server_name)

  mail.send_mail(sender=server_email, to=send_to, subject=subject, body=body)
  return True
