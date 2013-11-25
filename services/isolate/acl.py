# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
import datetime
import hashlib
import hmac
import itertools
import logging
import os
import re
import time

import webapp2
from google.appengine.api import app_identity
from google.appengine.api import users
from google.appengine.ext import ndb

import config
import template
import utils


### Models


class WhitelistedIP(ndb.Model):
  """Items where the IP address is allowed.

  The key is the ip as returned by ip_to_str(*parse_ip(ip)).
  """
  # Logs who made the change.
  timestamp = ndb.DateTimeProperty(auto_now=True)
  who = ndb.UserProperty(auto_current_user=True)

  # This is used for sharing token. Use case: a slave are multiple HTTP proxies
  # which different public IP used in a round-robin fashion, so the slave looks
  # like a different IP at each request, but reuses the original token.
  group = ndb.StringProperty(indexed=False)

  # The textual representation of the IP of the machine to whitelist. Not used
  # in practice, just there since the canonical representation is hard to make
  # sense of.
  ip = ndb.StringProperty(indexed=False)

  # Is only for maintenance purpose.
  comment = ndb.StringProperty(indexed=False)


class WhitelistedDomain(ndb.Model):
  """Domain from which users can use the isolate server.

  The key is the domain name, like 'example.com'.
  """
  # Logs who made the change.
  timestamp = ndb.DateTimeProperty(auto_now=True)
  who = ndb.UserProperty(auto_current_user=True)


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


def ipv4_to_int(ip):
  values = [int(i) for i in ip.split('.')]
  factor = 256
  value = 0L
  for i in values:
    value = value * factor + i
  return value


def int_to_ipv4(integer):
  values = []
  factor = 256
  for _ in range(4):
    values.append(integer % factor)
    integer = integer / factor
  return '.'.join(str(i) for i in reversed(values))


def expand_subnet(ip, mask):
  """Returns all the IP addressed comprised in a range."""
  if mask == 32:
    return [ip]
  bit = 1 << (32 - mask)
  return [int_to_ipv4(ipv4_to_int(ip) + r) for r in range(bit)]


### HMAC signatures and access tokens

# Hashing algorithm to use for HMAC signature.
HMAC_HASH_ALGO = hashlib.sha256
# How many bytes of HMAC to use as a signature.
HMAC_HASH_BYTES = 8

# How long token lives.
TOKEN_EXPIRATION_SEC = 3600 * 2


# Ensure consistency in parameters.
assert HMAC_HASH_ALGO().digest_size >= HMAC_HASH_BYTES


class InvalidTokenError(ValueError):
  """Raised by validate_token if token is broken or expired."""


def generate_hmac_signature(secret, strings):
  """Returns HMAC of a list of strings.

  Arguments:
    secret: secret key to sign with.
    strings: list of strings to sign.

  Returns:
    Signature (as str object with binary data) of length HMAC_HASH_BYTES.
  """
  # Ensure data is a non empty list of strings that do not contain '\0'.
  # Unicode strings are not allowed.
  assert secret
  assert strings and all(isinstance(x, str) and '\0' not in x for x in strings)
  mac = hmac.new(secret, digestmod=HMAC_HASH_ALGO)
  mac.update('\0'.join(strings))
  return mac.digest()[:HMAC_HASH_BYTES]


def generate_token(access_id, secret, expiration_sec, token_data=None):
  """Returns new token that expires after |expiration_sec| seconds.

  Arguments:
    access_id: identifies a client this token is issued to.
    secret: secret key to sign token with.
    expiration_sec: how long token will be valid, sec.
    token_data: an optional dict with string keys and values that will be put
                in the token. Keys starting with '_' are reserved for internal
                use. This data is publicly visible.

  Returns:
    Base64 encoded token string.
  """
  token_data = token_data or {}

  assert access_id
  assert secret
  assert expiration_sec > 0
  assert all(k and not k.startswith('_') for k in token_data)

  # Convert dict to a flat list of key-value pairs, append expiration timestamp.
  public_params = list(itertools.chain(*token_data.items()))
  public_params.extend(['_x', str(int(time.time() + expiration_sec))])

  # Append access_id and sign it with secret key.
  sig = generate_hmac_signature(secret, public_params + [str(access_id)])

  # Final token is base64 encoded public_params + sig.
  assert all('\0' not in x for x in public_params)
  assert len(sig) == HMAC_HASH_BYTES
  return base64.urlsafe_b64encode('\0'.join(public_params) + sig)


def validate_token(token, access_id, secret):
  """Checks token signature and expiration, decodes data embedded into it.

  The following holds:
    token = generate_token(secret, expiration_sec, token_data)
    assert validate_token(token, secret) == token_data

  Arguments:
    token: token produced by generate_token call.
    access_id: identifies a client this token should belong to.
    secret: secret used to sign the token.

  Returns:
    A dict with public data embedded into the token.

  Raises:
    InvalidTokenError if token is broken, tempered with or expired.
  """
  try:
    # Reverse actions performed in generate_token to encode the token.
    blob = base64.urlsafe_b64decode(str(token))
    public_params = blob[:-HMAC_HASH_BYTES].split('\0')
    provided_signature = blob[-HMAC_HASH_BYTES:]
    # Flat list of key-value pairs can't have odd length.
    if len(public_params) % 2:
      raise ValueError()
  except (ValueError, TypeError):
    raise InvalidTokenError('Bad token format: %s' % token)

  # Calculate a correct signature for given secret and public_params.
  sig = generate_hmac_signature(secret, public_params + [str(access_id)])
  # It should be equal to provided signature.
  if not utils.constant_time_equals(sig, provided_signature):
    raise InvalidTokenError('Token signature is invalid.')

  # At this point we're sure that token was generated by us. It still can be
  # expired though, so check it next.

  # Convert flat list of key-value pairs back to dict.
  public_params_dict = dict(
      public_params[i:i+2] for i in xrange(0, len(public_params), 2))

  # Ensure timestamp is there and has a valid format, also remove it from dict.
  try:
    expiration_ts = int(public_params_dict.pop('_x'))
  except (KeyError, ValueError):
    raise InvalidTokenError('Invalid timestamp format.')

  # Check timestamp for expiration.
  now = time.time()
  if now > expiration_ts:
    raise InvalidTokenError('Token expired %d sec ago.' % (now - expiration_ts))

  # Token is valid and non-expired.
  return public_params_dict


### Handlers


class ACLRequestHandler(webapp2.RequestHandler):
  """Adds ACL to the request handler to ensure only valid users can use
  the handlers."""
  # Set to the uniquely identifiable id, either the userid or the IP address.
  access_id = None
  # Set to False if custom processing is required. In that case, a call to
  # self.enforce_valid_token() is required inside the post()/put() handler.
  enforce_token = True
  # Token data dict embedded into token via 'generate_token'. Valid only for
  # POST or PUT requests if 'enforce_token' is True.
  token_data = None

  def dispatch(self):
    """Ensures that only users from valid domains can continue, and that users
    from invalid domains receive an error message."""
    current_user = users.get_current_user()
    if current_user:
      self.check_user(current_user)
    else:
      self.check_ip(self.request.remote_addr)
    self.token_data = {}
    if self.request.method in ('POST', 'PUT') and self.enforce_token:
      self.enforce_valid_token()
    return webapp2.RequestHandler.dispatch(self)

  def check_ip(self, ip):
    """Verifies if the IP is whitelisted."""
    self.access_id = ip
    iptype, ipvalue = parse_ip(ip)
    whitelisted = WhitelistedIP.get_by_id(ip_to_str(iptype, ipvalue))
    if not whitelisted:
      logging.warning('Blocking IP %s', ip)
      self.abort(401, detail='Please login first.')
    if whitelisted.group:
      # Any member of of the group can impersonate others. This is to enable
      # support for slaves behind proxies with multiple IPs.
      self.access_id = whitelisted.group

  def check_user(self, user):
    """Verifies if the user is whitelisted."""
    domain = user.email().partition('@')[2]
    if (not WhitelistedDomain.get_by_id(domain) and
        not users.is_current_user_admin()):
      logging.warning('Disallowing %s, invalid domain' % user.email())
      self.abort(403, detail='Invalid domain, %s' % domain)
    # user_id() is only set with Google accounts, fallback to the email address
    # otherwise.
    self.access_id = user.user_id() or user.email()

  def generate_token(self, token_data=None):
    """Returns new access token.

    Arguments:
      token_data: optional dict with string keys and values that will be
                  embedded into the token. It's later accessible as
                  self.token_data. It's publicly visible.
    """
    assert self.access_id is not None
    return generate_token(
        self.access_id,
        self._get_token_secret(),
        TOKEN_EXPIRATION_SEC,
        token_data)

  def enforce_valid_token(self):
    """Ensures the token is valid, populates self.token_data."""
    assert self.access_id is not None
    token = self.request.get('token')
    if not token:
      logging.info('Token was not provided')
      self.abort(403)
    try:
      self.token_data = validate_token(
          token,
          self.access_id,
          self._get_token_secret())
    except InvalidTokenError as err:
      logging.error('Invalid token.\n%s', err)
      self.abort(403, detail='Invalid token.')

  @staticmethod
  def _get_token_secret():
    """Returns secret key used to sign or validate a token."""
    app_id = app_identity.get_application_id()
    return 'token-secret-%s-%s' % (
        config.settings().global_secret,
        str(app_id))


class RestrictedWhitelistIPHandler(ACLRequestHandler):
  """Whitelists the current IP.

  This handler must have login:admin in app.yaml.
  """
  def get(self):
    # The user must authenticate with a user credential before being able to
    # whitelist the IP. This is done with login:admin.
    data = {
      'default_comment': '',
      'default_group': '',
      'default_ip': self.request.remote_addr,
      'note': '',
      'now': datetime.datetime.utcnow(),
      'token': self.generate_token(),
      'whitelistips': WhitelistedIP.query(),
    }
    self.response.out.write(template.get('whitelistip.html').render(data))
    self.response.headers['Content-Type'] = 'text/html'

  def post(self):
    comment = self.request.get('comment')
    group = self.request.get('group')
    ip = self.request.get('ip')
    if not comment:
      self.abort(403, 'Comment is required.')
    mask = 32
    if '/' in ip:
      ip, mask = ip.split('/', 1)
      mask = int(mask)

    if not all(ip_to_str(*parse_ip(i)) for i in expand_subnet(ip, mask)):
      self.abort(403, 'IP is invalid')

    note = []
    for i in expand_subnet(ip, mask):
      key = ip_to_str(*parse_ip(i))
      item = WhitelistedIP.get_by_id(key)
      item_comment = comment
      if mask != 32:
        item_comment += ' ' + self.request.get('ip')
      if item:
        item.comment = item_comment
        item.group = group
        item.ip = i
        item.put()
        note.append('Already present: %s' % i)
      else:
        WhitelistedIP(id=key, comment=item_comment, group=group, ip=i).put()
        note.append('Success: %s' % i)

    data = {
      'default_comment': self.request.get('comment'),
      'default_group': self.request.get('group'),
      'default_ip': self.request.get('ip'),
      'note': '<br>'.join(note),
      'now': datetime.datetime.utcnow(),
      'token': self.generate_token(),
      'whitelistips': WhitelistedIP.query(),
    }
    self.response.out.write(template.get('whitelistip.html').render(data))
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
        self.generate_token()))
    self.response.headers['Content-Type'] = 'text/html'

  def post(self):
    domain = self.request.get('domain')
    if not re.match(r'^[a-z\.\-]+$', domain):
      self.abort(403, 'Invalid domain format')
    # Do not use get_or_insert() right away so we know if the entity existed
    # before.
    if not WhitelistedDomain.get_by_id(domain):
      WhitelistedDomain.get_or_insert(domain)
      self.response.out.write(htmlwrap('Success: %s' % domain))
    else:
      self.response.out.write(htmlwrap('Already present: %s' % domain))
    self.response.headers['Content-Type'] = 'text/html'


class GetTokenHandler(ACLRequestHandler):
  """Returns the token."""
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    token = self.generate_token()
    self.response.out.write(token)
    logging.info('Generated %s\nAccessId: %s', token, self.access_id)


def bootstrap():
  """Adds example.com as a valid domain when testing."""
  if os.environ['SERVER_SOFTWARE'].startswith('Development'):
    WhitelistedDomain.get_or_insert('example.com')
    WhitelistedIP.get_or_insert(
        ip_to_str('v4', 2130706433),
        ip='127.0.0.1',
        comment='automatic because of running on dev server')
