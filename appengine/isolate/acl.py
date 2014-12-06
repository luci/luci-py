# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging

from google.appengine.ext import ndb

from components import auth
from components import utils


# Names of groups.
ADMINS_GROUP = 'isolate-admin-access'
READERS_GROUP = 'isolate-read-access'
WRITERS_GROUP = 'isolate-write-access'


### Models


class WhitelistedIP(ndb.Model):
  """Items where the IP address is allowed.

  The key is the ip as returned by _ip_to_str(*_parse_ip(ip)).
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


### Private stuff.


def _parse_ip(ipstr):
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
    assert ':' in ipstr, ipstr
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


def _ip_to_str(iptype, ipvalue):
  if not iptype:
    return None
  return '%s-%d' % (iptype, ipvalue)


def _ipv4_to_int(ip):
  values = [int(i) for i in ip.split('.')]
  factor = 256
  value = 0L
  for i in values:
    value = value * factor + i
  return value


def _int_to_ipv4(integer):
  values = []
  factor = 256
  for _ in range(4):
    values.append(integer % factor)
    integer = integer / factor
  return '.'.join(str(i) for i in reversed(values))


def _expand_subnet(ip, mask):
  """Returns all the IP addressed comprised in a range."""
  if mask == 32:
    return [ip]
  bit = 1 << (32 - mask)
  return [_int_to_ipv4(_ipv4_to_int(ip) + r) for r in range(bit)]


### Public API.


def whitelisted_ip_authentication(request):
  """Returns bot Identity if request comes from known IP, or None otherwise."""
  iptype, ipvalue = _parse_ip(request.remote_addr)
  whitelisted = WhitelistedIP.get_by_id(_ip_to_str(iptype, ipvalue))
  if not whitelisted:
    return None
  if whitelisted.group:
    # Any member of of the group can impersonate others. This is to enable
    # support for slaves behind proxies with multiple IPs.
    access_id = whitelisted.group
  else:
    access_id = _ip_to_str(iptype, ipvalue)
  return auth.Identity(auth.IDENTITY_BOT, access_id)


def isolate_admin():
  """Returns True if current user can administer isolate server."""
  return auth.is_group_member(ADMINS_GROUP) or auth.is_admin()


def isolate_writable():
  """Returns True if current user can write to isolate."""
  # Admins have access by default.
  return auth.is_group_member(WRITERS_GROUP) or isolate_admin()


def isolate_readable():
  """Returns True if current user can read from isolate."""
  # Anyone that can write can also read.
  return auth.is_group_member(READERS_GROUP) or isolate_writable()


def get_user_type():
  """Returns a string describing the current access control for the user."""
  if isolate_admin():
    return 'admin'
  if isolate_writable():
    return 'user'
  return 'unknown user'


def bootstrap():
  """Adds 127.0.0.1 as a whitelisted IP when testing."""
  if not utils.is_local_dev_server() or auth.is_replica():
    return

  bots = auth.bootstrap_loopback_ips()
  auth.bootstrap_group(READERS_GROUP, bots, 'Can read from Isolate')
  auth.bootstrap_group(WRITERS_GROUP, bots, 'Can write to Isolate')

  # Add a fake admin for local dev server.
  auth.bootstrap_group(
      auth.ADMIN_GROUP,
      [auth.Identity(auth.IDENTITY_USER, 'test@example.com')],
      'Users that can manage groups')


def add_whitelist(ip, group, comment):
  if not comment:
    raise ValueError('Comment is required.')
  original_ip = ip
  mask = 32
  if '/' in ip:
    ip, mask = ip.split('/', 1)
    mask = int(mask)

  keys = [(ip, _ip_to_str(*_parse_ip(ip))) for ip in _expand_subnet(ip, mask)]
  if not all(i[1] for i in keys):
    raise ValueError('IP is invalid')
  keys = [(i[0], ndb.Key(WhitelistedIP, i[1])) for i in keys]

  MAX_CHUNK = 250

  note = []
  while keys:
    chunk = keys[:MAX_CHUNK]
    keys = keys[MAX_CHUNK:]

    to_write = []
    for item, (ip, key) in zip(ndb.get_multi(i[1] for i in chunk), chunk):
      item_comment = comment
      if mask != 32:
        item_comment += ' ' + original_ip

      if (item and
          item.comment == item_comment and
          item.group == group and
          item.ip == ip):
        # Skip writing it.
        note.append('Already present: %s' % ip)
        continue

      to_write.append(
          WhitelistedIP(key=key, comment=item_comment, group=group, ip=ip))
      note.append('Success: %s' % ip)
    if to_write:
      ndb.put_multi(to_write)

  return note
