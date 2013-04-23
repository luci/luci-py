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


### Models


class User(db.Model):
  timestamp = db.DateTimeProperty(auto_now=True)

  secret = db.ByteStringProperty(indexed=False)
  # For information purpose only.
  email = db.StringProperty(indexed=False)


class WhitelistedIP(db.Model):
  """Items where the IP address is allowed."""
  # Logs who made the change.
  timestamp = db.DateTimeProperty(auto_now=True)
  who = db.UserProperty(auto_current_user=True)

  # The IP of the machine to whitelist. Can be either IPv4 or IPv6.
  ip = db.StringProperty()

  # Is only for maintenance purpose.
  comment = db.StringProperty(indexed=False)
  secret = db.ByteStringProperty(indexed=False)


class WhitelistedDomain(db.Model):
  """Domain from which users can use the isolate server.

  The key is the domain name, like 'example.com'.
  """
  # Logs who made the change.
  timestamp = db.DateTimeProperty(auto_now=True)
  who = db.UserProperty(auto_current_user=True)


### Utility


def htmlwrap(text):
  """Wraps text in minimal HTML tags."""
  return '<html><body>%s</body></html>' % text


def is_valid_domain(domain):
  """Returns True if the domain is valid.

  Deleting a domain by deleting the corresponding WhitelistedDomain requires
  clearing memcache.
  """
  if memcache.get(domain, namespace='whitelisted_domain'):
    return True
  if WhitelistedDomain.get_by_key_name(domain):
    memcache.set(domain, True, namespace='whitelisted_domain')
    return True
  # Do not save negative result.
  return False


### Handlers


class ACLRequestHandler(webapp2.RequestHandler):
  """Adds ACL to the request handler to ensure only valid users can use
  the handlers."""
  secret = None
  access_id = None
  is_user = None
  # Set to False if custom processing is required. In that case, a call to
  # self.enforce_valid_token() is required inside the post() handler.
  enforce_token_on_post = True
  # The value is cached on the request once calculated.
  token = None

  def dispatch(self):
    """Ensures that only users from valid domains can continue, and that users
    from invalid domains receive an error message."""
    current_user = users.get_current_user()
    if current_user:
      self.check_user(current_user)
    else:
      self.check_ip(self.request.remote_addr)
    if self.request.method == 'POST' and self.enforce_token_on_post:
      self.enforce_valid_token()
    return webapp2.RequestHandler.dispatch(self)

  def check_ip(self, ip):
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

  def check_user(self, user):
    """Verifies if the user is whitelisted."""
    domain = user.email().partition('@')[2]
    if not is_valid_domain(domain) and not users.is_current_user_admin():
      logging.warning('Disallowing %s, invalid domain' % user.email())
      self.abort(403, detail='Invalid domain, %s' % domain)
    return self.check_user_id(user.user_id() or user.email(), user.email())

  def check_user_id(self, user_id, email):
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

  def get_token(self, offset, now):
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

  def enforce_valid_token(self):
    """Ensures the token is valid."""
    token = self.request.get('token')
    if not token:
      logging.info('Token was not provided')
      self.abort(403)

    now = time.time()
    token_0 = self.get_token(0, now)
    if token != token_0:
      token_1 = self.get_token(-1, now)
      if token != token_1:
        logging.info(
            'Token was invalid:\nGot %s\nExpected %s or %s\nAccessId: %s\n'
              'Secret: %s',
            token, token_0, token_1, self.access_id,
            binascii.hexlify(self.secret))
        self.abort(403, detail='Invalid token.')
    self.token = token
    return token


class RestrictedWhitelistIPHandler(ACLRequestHandler):
  """Whitelists the current IP.

  This handler must have login:admin in app.yaml.
  """
  def get(self):
    # The user must authenticate with a user credential before being able to
    # whitelist the IP. This is done with login:admin.
    self.response.out.write(htmlwrap(
      '<form name="whitelist" method="post">'
      'Comment: <input type="text" name="comment" /><br />'
      '<input type="hidden" name="token" value="%s" />'
      '<input type="submit" value="SUBMIT" />' %
        self.get_token(0, time.time())))
    self.response.headers['Content-Type'] = 'text/html'

  def post(self):
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
    self.response.headers['Content-Type'] = 'text/html'


class RestrictedWhitelistDomainHandler(ACLRequestHandler):
  """Whitelists a domain.

  This handler must have login:admin in app.yaml.
  """
  def get(self):
    # The user must authenticate with a user credential before being able to
    # whitelist the IP. This is done with login:admin.
    self.response.out.write(htmlwrap(
      '<form name="whitelist" method="post">'
      'Domain: <input type="text" name="domain" /><br />'
      '<input type="hidden" name="token" value="%s" />'
      '<input type="submit" value="SUBMIT" />' %
        self.get_token(0, time.time())))
    self.response.headers['Content-Type'] = 'text/html'

  def post(self):
    domain = self.request.get('domain')
    if not is_valid_domain(domain):
      WhitelistedDomain(key_name=domain).put()
      self.response.out.write(htmlwrap('Success: %s' % domain))
    else:
      self.response.out.write(htmlwrap('Already present: %s' % domain))
    self.response.headers['Content-Type'] = 'text/html'


class GetTokenHandler(ACLRequestHandler):
  """Returns the token."""
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    token = self.get_token(0, time.time())
    self.response.out.write(token)
    logging.info(
        'Generated %s\nAccessId: %s\nSecret: %s',
        token, self.access_id, binascii.hexlify(self.secret))


def bootstrap():
  """Adds example.com as a valid domain when testing."""
  if os.environ['SERVER_SOFTWARE'].startswith('Development'):
    WhitelistedDomain.get_or_insert(key_name='example.com')
