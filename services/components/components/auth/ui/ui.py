# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Auth management UI handlers."""

import jinja2
import json
import os
import webapp2

from google.appengine.api import users

from components import utils

from .. import api
from .. import handler
from .. import model


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
JINJA = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.join(ROOT_DIR, 'templates')),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)


# Top navigation bar links as tuples (id, title, url).
NAVBAR_TABS = (
  ('groups', 'Groups', '/auth/groups'),
  ('oauth_config', 'OAuth', '/auth/oauth_config'),
)


# Global static configuration set in 'configure_ui'.
_ui_app_name = 'Unknown'


def configure_ui(app_name):
  """Global configuration of some UI parameters."""
  global _ui_app_name
  _ui_app_name = app_name


def get_ui_routes():
  """Returns a list of webapp2 routes with auth REST API handlers."""
  return [
    webapp2.Route(r'/auth', MainHandler),
    webapp2.Route(r'/auth/bootstrap', BootstrapHandler, name='bootstrap'),
    webapp2.Route(r'/auth/groups', GroupsHandler),
    webapp2.Route(r'/auth/oauth_config', OAuthConfigHandler),
  ]


class UIHandler(handler.AuthenticatingHandler):
  """Renders Jinja templates extending base.html or base_minimal.html."""

  def reply(self, path, env=None, status=200):
    """Render template |path| to response using given environment.

    Optional keys from |env| that base.html uses:
      page_title: title of an HTML page.
      css_file: path to file with page specific styles, relative to static/css/.
      js_file: path to file with page specific Javascript code, relative to
          static/js. File should define global object named same as a file, i.e.
          'api.js' should define global object 'api' that incapsulates
          functionality implemented in the module.
      navbar_tab_id: id a navbar tab to highlight, one of ids in NAVBAR_TABS.

    Args:
      path: path to a template, relative to templates/.
      env: additional environment dict to use when rendering the template.
      status: HTTP status code to return.
    """
    # This goes to both Jinja2 env and Javascript config object.
    common = {
      'login_url': users.create_login_url(self.request.path),
      'logout_url': users.create_logout_url('/'),
      'xsrf_token': self.generate_xsrf_token(),
    }

    # This will be accessible from Javascript as global 'config' variable.
    js_config = {
      'identity': api.get_current_identity().to_bytes(),
    }
    js_config.update(common)

    # Jinja2 environment to use to render a template.
    full_env = {
      'app_name': _ui_app_name,
      'app_revision_url': utils.get_app_revision_url(),
      'app_version': utils.get_app_version(),
      'config': json.dumps(js_config),
      'identity': api.get_current_identity(),
      'navbar': NAVBAR_TABS,
    }
    full_env.update(common)
    full_env.update(env or {})

    # Render it.
    self.response.set_status(status)
    self.response.headers['Content-Type'] = 'text/html; charset=UTF-8'
    self.response.write(JINJA.get_template(path).render(full_env))

  def authentication_error(self, error):
    """Shows 'Access denied' page."""
    env = {
      'page_title': 'Access Denied',
      'error': error,
    }
    self.reply('access_denied.html', env=env, status=401)

  def authorization_error(self, error):
    """Redirects to login or shows 'Access Denied' page."""
    # Not authenticated -> redirect to login.
    if api.get_current_identity().is_anonymous:
      self.redirect(users.create_login_url(self.request.path))
      return

    # Admin group is empty -> redirect to bootstrap procedure to create it.
    if model.is_empty_group(model.ADMIN_GROUP):
      self.redirect_to('bootstrap')
      return

    # No access.
    env = {
      'page_title': 'Access Denied',
      'error': error,
    }
    self.reply('access_denied.html', env=env, status=403)


class MainHandler(UIHandler):
  """Redirects to first navbar tab."""
  @api.require(api.is_admin)
  def get(self):
    self.redirect(NAVBAR_TABS[0][2])


class BootstrapHandler(UIHandler):
  """Creates Administrators group (if necessary) and adds current caller to it.

  Requires Appengine level Admin access for its handlers, since Administrators
  group may not exist yet. Used to bootstrap a new service instance.
  """

  @api.require(users.is_current_user_admin)
  def get(self):
    self.reply(
        'bootstrap.html',
        env={
          'page_title': 'Bootstrap',
          'admin_group': model.ADMIN_GROUP,
        })

  @api.require(users.is_current_user_admin)
  def post(self):
    added = model.bootstrap_group(
        model.ADMIN_GROUP, api.get_current_identity(),
        'Users that can manage groups')
    self.reply(
        'bootstrap_done.html',
        env={
          'page_title': 'Bootstrap',
          'admin_group': model.ADMIN_GROUP,
          'added': added,
        })


class GroupsHandler(UIHandler):
  """Page with Groups management."""
  @api.require(api.is_admin)
  def get(self):
    env = {
      'js_file': 'groups.js',
      'navbar_tab_id': 'groups',
      'page_title': 'Groups',
    }
    self.reply('groups.html', env=env)


class OAuthConfigHandler(UIHandler):
  """Page with OAuth configuration."""
  @api.require(api.is_admin)
  def get(self):
    env = {
      'js_file': 'oauth_config.js',
      'navbar_tab_id': 'oauth_config',
      'page_title': 'OAuth Config',
    }
    self.reply('oauth_config.html', env=env)
