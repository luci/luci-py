# Copyright (c) 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import binascii
import hashlib
import logging
import os
import time

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
import webapp2
from google.appengine.api import memcache
from google.appengine.api import users
from google.appengine.ext import db
# pylint: enable=E0611,F0401


# The domains that are allowed to access this application.
VALID_DOMAINS = (
    'chromium.org',
    'google.com',
)


def htmlwrap(text):
  """Wraps text in minimal HTML tags."""
  return '<html><body>%s</body></html>' % text


class User(db.Model):
  secret = db.ByteStringProperty(indexed=False)
  # For information purpose only.
  email = db.StringProperty(indexed=False)


class WhitelistedIP(db.Model):
  """Items where the IP address is allowed."""
  # The IP of the machine to whitelist. Can be either IPv4 or IPv6.
  ip = db.StringProperty()

  # Is only for maintenance purpose.
  comment = db.StringProperty(indexed=False)
  secret = db.ByteStringProperty(indexed=False)


class ACLRequestHandler(webapp2.RequestHandler):
  """Adds ACL to the request handler to ensure only valid users can use
  the handlers."""
  secret = None
  access_id = None
  is_user = None

  def dispatch(self):
    """Ensures that only users from valid domains can continue, and that users
    from invalid domains receive an error message."""
    current_user = users.get_current_user()
    if current_user:
      self.CheckUser(current_user)
    else:
      self.CheckIP(self.request.remote_addr)
    return webapp2.RequestHandler.dispatch(self)

  def CheckIP(self, ip):
    """Verifies if the IP is whitelisted."""
    self.access_id = ip
    self.secret = memcache.get(self.access_id, namespace='ip_token')
    if not self.secret:
      whitelisted = WhitelistedIP.all().filter('ip', self.access_id).get()
      if not whitelisted:
        logging.warning('Blocking IP %s', self.request.remote_addr)
        self.abort(401, detail='Please login first.')
      self.secret = whitelisted.secret
      if not self.secret:
        self.secret = os.urandom(8)
        whitelisted.secret = self.secret
        whitelisted.put()
      memcache.set(self.access_id, self.secret, namespace='ip_token')
    self.is_user = False

  def CheckUser(self, user):
    """Verifies if the user is whitelisted."""
    domain = user.email().partition('@')[2]
    if domain not in VALID_DOMAINS:
      logging.warning('Disallowing %s, invalid domain' % user.email())
      self.abort(403, detail='Invalid domain, %s' % domain)
    return self.CheckUserId(user.user_id() or user.email(), user.email())

  def CheckUserId(self, user_id, email):
    # user_id() is only set with Google accounts, fallback to the email address
    # otherwise.
    self.access_id = user_id
    self.secret = memcache.get(self.access_id, namespace='user_token')
    if not self.secret:
      user_obj = User.get_by_key_name(self.access_id)
      if not user_obj:
        if not email:
          logging.warning('Blocking user %s', user_id)
          self.abort(403, detail='Please login first.')
        user_obj = User.get_or_insert(
            self.access_id, secret=os.urandom(8), email=email)
      if not user_obj.secret:
        # Should not happen but cope with it.
        user_obj.secret = os.urandom(8)
        user_obj.put()
      self.secret = user_obj.secret
      memcache.set(self.access_id, self.secret, namespace='user_token')
    self.is_user = True

  def GetToken(self, offset, now):
    """Returns a valid token for the current user.

    |offset| is the offset versus current time of day, in hours. It should be 0
    or -1.
    """
    m = hashlib.sha1(self.secret)
    m.update(self.access_id)
    # Rotate every hour.
    this_hour = int(now / 3600.)
    m.update(str(this_hour + offset))
    # Keep 8 bytes of entropy.
    return m.hexdigest()[:16]

  def CheckToken(self):
    """Ensures the token is valid."""
    token = self.request.get('token')
    if not token:
      logging.info('Token was not provided')
      self.abort(403)

    now = time.time()
    token_0 = self.GetToken(0, now)
    if token != token_0:
      token_1 = self.GetToken(-1, now)
      if token != token_1:
        logging.info(
            'Token was invalid:\nGot %s\nExpected %s or %s\nAccessId: %s\n'
              'Secret: %s',
            token, token_0, token_1, self.access_id,
            binascii.hexlify(self.secret))
        self.abort(403)
    return token


class RestrictedWhitelistHandler(ACLRequestHandler):
  """Whitelists the current IP."""
  def get(self):
    # The user must authenticate with a user credential before being able to
    # whitelist the IP.
    self.response.out.write(htmlwrap(
      '<form name="whitelist" method="post">'
      'Comment: <input type="text" name="comment" /><br />'
      '<input type="hidden" name="token" value="%s" />'
      '<input type="submit" value="SUBMIT" />' % self.GetToken(0, time.time())))

  def post(self):
    self.CheckToken()
    ip = self.request.remote_addr
    comment = self.request.get('comment')
    item = WhitelistedIP.gql('WHERE ip = :1', ip).get()
    if item:
      item.comment = comment or item.comment
      item.put()
      self.response.out.write(htmlwrap('Already present: %s' % ip))
      return
    WhitelistedIP(ip=ip, comment=comment).put()
    self.response.out.write(htmlwrap('Success: %s' % ip))


def bootstrap():
  if os.environ['SERVER_SOFTWARE'].startswith('Development'):
    # Add example.com as a valid domain when testing.
    global VALID_DOMAINS
    VALID_DOMAINS = ('example.com',) + VALID_DOMAINS
