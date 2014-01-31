# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os

from google.appengine.ext import ndb

from components import auth
import template


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


def whitelisted_ip_authentication(request):
  """Returns bot Identity if request comes from known IP, or None otherwise."""
  iptype, ipvalue = parse_ip(request.remote_addr)
  whitelisted = WhitelistedIP.get_by_id(ip_to_str(iptype, ipvalue))
  if not whitelisted:
    logging.warning('Access from unknown IP: %s', request.remote_addr)
    return None
  if whitelisted.group:
    # Any member of of the group can impersonate others. This is to enable
    # support for slaves behind proxies with multiple IPs.
    access_id = whitelisted.group
  else:
    access_id = ip_to_str(iptype, ipvalue)
  return auth.Identity(auth.IDENTITY_BOT, access_id)


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


### Handlers


class RestrictedWhitelistIPHandler(auth.AuthenticatingHandler):
  """Whitelists the current IP.

  This handler must have login:admin in app.yaml.
  """

  @auth.require(auth.READ, 'isolate/management')
  def get(self):
    # The user must authenticate with a user credential before being able to
    # whitelist the IP. This is done with login:admin.
    data = {
      'default_comment': '',
      'default_group': '',
      'default_ip': self.request.remote_addr,
      'note': '',
      'now': datetime.datetime.utcnow(),
      'xsrf_token': self.generate_xsrf_token(),
      'whitelistips': WhitelistedIP.query(),
    }
    self.response.out.write(template.get('whitelistip.html').render(data))
    self.response.headers['Content-Type'] = 'text/html'

  @auth.require(auth.UPDATE, 'isolate/management')
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
      'xsrf_token': self.generate_xsrf_token(),
      'whitelistips': WhitelistedIP.query(),
    }
    self.response.out.write(template.get('whitelistip.html').render(data))
    self.response.headers['Content-Type'] = 'text/html'


def bootstrap():
  """Adds 127.0.0.1 as a whitelisted IP when testing."""
  if os.environ['SERVER_SOFTWARE'].startswith('Development'):
    WhitelistedIP.get_or_insert(
        ip_to_str('v4', 2130706433),
        ip='127.0.0.1',
        comment='automatic because of running on dev server')
