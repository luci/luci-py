#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Basic admin helper function."""



from google.appengine.ext import db


class AdminUser(db.Model):
  """A email address of the admin to send the exception emails.

  If there isn't a valid instance of these then no emails are sent.
  """
  # The email to send the exception emails from.
  email = db.StringProperty()
