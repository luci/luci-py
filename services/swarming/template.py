# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Setups jinja2 environment."""

import datetime
import os
import re
import urllib

import jinja2

from google.appengine.api import users

from components import natsort
from components import utils


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

JINJA = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.join(ROOT_DIR, 'templates')),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)


# Registers library custom filters.


def datetimeformat(value, f='%Y-%m-%d %H:%M:%S'):
  if not value:
    return '--'
  return value.strftime(f)


def datetime_human(dt):
  """Converts a datetime.datetime to a user-friendly string."""
  if not dt:
    return '--'
  dt_date = dt.date()
  today = datetime.datetime.utcnow().date()
  if dt_date == today:
    return dt.strftime('Today, %H:%M:%S')
  if dt_date == today - datetime.timedelta(days=1):
    return dt.strftime('Yesterday, %H:%M:%S')
  return dt.strftime('%Y-%m-%d %H:%M:%S')


def natsorted(value):
  """Accepts None transparently."""
  return natsort.natsorted(value or [])


def timedeltaformat(value):
  """Formats a timedelta in a sane way. Ignores micro seconds, we're not that
  fast.
  """
  if not value:
    return '--'
  hours, remainder = divmod(int(round(value.total_seconds())), 3600)
  minutes, seconds = divmod(remainder, 60)
  if hours:
    return '%d:%02d:%02d' % (hours, minutes, seconds)
  # Always prefix minutes, even if 0, otherwise this looks weird. Revisit this
  # decision if bikeshedding is desired.
  return '%d:%02d' % (minutes, seconds)


def urlquote(s):
  # TODO(maruel): Remove once jinja is upgraded to 2.7.
  if isinstance(s, jinja2.Markup):
    s = s.unescape()
  return jinja2.Markup(urllib.quote_plus(s.encode('utf8')))


JINJA.filters['datetime_human'] = datetime_human
JINJA.filters['datetimeformat'] = datetimeformat
JINJA.filters['encode_to_json'] = utils.encode_to_json
JINJA.filters['natsort'] = natsorted
JINJA.filters['timedeltaformat'] = timedeltaformat
JINJA.filters['urlquote'] = urlquote


@utils.cache
def get_app_revision_url():
  """Returns URL of a git revision page for currently running app version.

  Works only for non-tainted versions uploaded with tools/update.py: app version
  should look like '162-efaec47'.

  Returns None if a version is tainted or has unexpected name.
  """
  rev = re.match(r'\d+-([a-f0-9]+)$', utils.get_app_version())
  template = 'https://code.google.com/p/swarming/source/detail?r=%s'
  return template % rev.group(1) if rev else None


def _get_defaults():
  """Returns parameters used by templates/base.html."""
  account = users.get_current_user()
  return {
    'app_version': utils.get_app_version(),
    'app_revision_url': get_app_revision_url(),
    'nickname': account.email() if account else None,
    'signin_link': users.create_login_url('/') if not account else None,
    'user_is_admin': users.is_current_user_admin(),
  }


def render(name, params):
  """Shorthand to render a template."""
  data = _get_defaults()
  data.update(params)
  return JINJA.get_template(name).render(data)
