# Copyright (c) 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import base64
import hashlib
import logging
import os
import re
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


class GlobalSecret(db.Model):
  """Secret."""
  secret = db.ByteStringProperty()


class WhitelistedIP(db.Model):
  """Items where the IP address is allowed.

  The key is the ip as returned by ip_to_str(*parse_ip(ip)).
  """
  # Logs who made the change.
  timestamp = db.DateTimeProperty(auto_now=True)
  who = db.UserProperty(auto_current_user=True)

  # This is used for sharing token. Use case: a slave are multiple HTTP proxies
  # which different public IP used in a round-robin fashion, so the slave looks
  # like a different IP at each request, but reuses the original token.
  group = db.StringProperty(indexed=False)

  # The textual representation of the IP of the machine to whitelist. Not used
  # in practice, just there since the canonical representation is hard to make
  # sense of.
  ip = db.StringProperty(indexed=False)

  # Is only for maintenance purpose.
  comment = db.StringProperty(indexed=False)


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


def parse_ip(ipstr):
  """Returns a long number representing the IP and its type, 'v4' or 'v6'.

  This works around potentially different representations of the same value,
  like 1.1.1.1 vs 1.01.1.1 or hex case difference in IPv6.
  """
  if '.' in ipstr:
    # IPv4.
    try:
      values = [int(i) for i in ipstr.split('.')]
    except ValueError:
      return None, None
    if len(values) != 4 or not all(0 <= i <= 255 for i in values):
      return None, None
    factor = 256
    iptype = 'v4'
  else:
    # IPv6.
    try:
      values = [int(i, 16) for i in ipstr.split(':')]
    except ValueError:
      return None, None
    if len(values) != 8 or not all(0 <= i <= 65535 for i in values):
      return None, None
    factor = 65536
    iptype = 'v6'
  value = 0L
  for i in values:
    value = value * factor + i
  return iptype, value


def ip_to_str(iptype, ipvalue):
  if not iptype:
    return None
  return '%s-%d' % (iptype, ipvalue)


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


def get_global_secret():
  """Returns the global secret to be used for the token."""
  secret = memcache.get('value', namespace='global_secret')
  if not secret:
    secret_obj = GlobalSecret.get_by_key_name('global')
    if not secret_obj:
      # First time.
      secret_obj = GlobalSecret(key_name='global', secret=os.urandom(16))
      secret_obj.put()
    secret = secret_obj.secret
    memcache.set('value', secret, namespace='global_secret')
  return secret


def gen_token(access_id, offset, now):
  """Returns a valid token for the access_id.

  |offset| is the offset versus current time of day, in hours. It should be 0
  or -1.
  """
  assert offset <= 0
  # Rotate every hour.
  this_hour = int(now / 3600.)
  timestamp = str(this_hour + offset)
  version = os.environ['CURRENT_VERSION_ID']
  secrets = (get_global_secret(), str(access_id), str(version), timestamp)
  hashed = hashlib.sha1('\0'.join(secrets)).digest()
  return base64.urlsafe_b64encode(hashed)[:16] + '-' + timestamp


def is_valid_token(provided_token, access_id, now):
  """Returns True if the provided token is valid."""
  token_0 = gen_token(access_id, 0, now)
  if provided_token != token_0:
    token_1 = gen_token(access_id, -1, now)
    if provided_token != token_1:
      logging.info(
          'Token was invalid:\nGot %s\nExpected %s or %s\nAccessId: %s',
          provided_token, token_0, token_1, access_id)
      return False
  return True


### Handlers


class ACLRequestHandler(webapp2.RequestHandler):
  """Adds ACL to the request handler to ensure only valid users can use
  the handlers."""
  # Set to the uniquely identifiable token, either the userid or the IP address.
  access_id = None
  # Set to False if custom processing is required. In that case, a call to
  # self.enforce_valid_token() is required inside the post() handler.
  enforce_token_on_post = True

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
    valid = memcache.get(ip, namespace='ip_token')
    if not valid:
      iptype, ipvalue = parse_ip(ip)
      whitelisted = WhitelistedIP.get_by_key_name(ip_to_str(iptype, ipvalue))
      if not whitelisted:
        logging.warning('Blocking IP %s', ip)
        self.abort(401, detail='Please login first.')
      if whitelisted.group:
        # Any member of of the group can impersonate others. This is to enable
        # support for slaves behind proxies with multiple IPs.
        self.access_id = whitelisted.group
      # Performance enhancement.
      memcache.set(ip, self.access_id, namespace='ip_token')
    else:
      self.access_id = valid

  def check_user(self, user):
    """Verifies if the user is whitelisted."""
    domain = user.email().partition('@')[2]
    if not is_valid_domain(domain) and not users.is_current_user_admin():
      logging.warning('Disallowing %s, invalid domain' % user.email())
      self.abort(403, detail='Invalid domain, %s' % domain)
    # user_id() is only set with Google accounts, fallback to the email address
    # otherwise.
    self.access_id = user.user_id() or user.email()

  def get_token(self, offset, now):
    return gen_token(self.access_id, offset, now)

  def enforce_valid_token(self):
    """Ensures the token is valid."""
    token = self.request.get('token')
    if not token:
      logging.info('Token was not provided')
      self.abort(403)
    if not is_valid_token(token, self.access_id, time.time()):
      self.abort(403, detail='Invalid token.')


class RestrictedWhitelistIPHandler(ACLRequestHandler):
  """Whitelists the current IP.

  This handler must have login:admin in app.yaml.
  """
  def get(self):
    # The user must authenticate with a user credential before being able to
    # whitelist the IP. This is done with login:admin.
    self.response.out.write(htmlwrap(
      ('<form name="whitelist" method="post">'
       'IP: <input type="text" name="ip" value="%s" /><br />'
       'Group: <input type="text" name="group" /><br />'
       'Comment: <input type="text" name="comment" /><br />'
       '<input type="hidden" name="token" value="%s" />'
       '<input type="submit" value="SUBMIT" />') %
          (self.request.remote_addr, self.get_token(0, time.time()))))
    self.response.headers['Content-Type'] = 'text/html'

  def post(self):
    comment = self.request.get('comment')
    group = self.request.get('group')
    ip = self.request.get('ip')
    key = ip_to_str(*parse_ip(ip))
    if not comment:
      self.abort(403, 'Comment is required.')
    if not key:
      self.abort(403, 'IP is invalid')
    item = WhitelistedIP.get_by_key_name(key)
    if item:
      item.comment = comment or item.comment
      item.group = group
      item.ip = ip
      item.put()
      self.response.out.write(htmlwrap('Already present: %s' % ip))
    else:
      WhitelistedIP(key_name=key, comment=comment, group=group, ip=ip).put()
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
    if not re.match(r'^[a-z\.\-]+$', domain):
      self.abort(403, 'Invalid domain format')
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
    logging.info('Generated %s\nAccessId: %s', token, self.access_id)


def bootstrap():
  """Adds example.com as a valid domain when testing."""
  if os.environ['SERVER_SOFTWARE'].startswith('Development'):
    WhitelistedDomain.get_or_insert(key_name='example.com')
    WhitelistedIP.get_or_insert(
        key_name=ip_to_str('v4', 2130706433),
        ip='127.0.0.1',
        comment='automatic because of running on dev server')
  get_global_secret()
