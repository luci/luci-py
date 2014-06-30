# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Setups jinja2 environment."""

import os

from google.appengine.api import users

from components import utils
from components import template


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def _get_defaults():
  """Returns parameters used by templates/base.html."""
  account = users.get_current_user()
  return {
    'nickname': account.email() if account else None,
    'signin_link': users.create_login_url('/') if not account else None,
  }


def bootstrap():
  global_env = {
    'app_version': utils.get_app_version(),
    'app_revision_url': utils.get_app_revision_url(),
  }
  template.bootstrap(
      {'swarming': os.path.join(ROOT_DIR, 'templates')}, global_env)


def render(name, params=None):
  """Shorthand to render a template."""
  data = _get_defaults()
  data.update(params or {})
  return template.render(name, data)
