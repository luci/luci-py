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


# Directory with this file.
THIS_DIR = os.path.dirname(os.path.abspath(__file__))


# Global static configuration set in 'configure_ui'.
_ui_app_name = 'Unknown'
_ui_navbar_tabs = ()
_ui_extra_template_paths = ()


def configure_ui(app_name, ui_tabs=None, template_paths=None):
  """Modifies global configuration of Auth UI.

  Args:
    app_name: name of the service (visible in page headers, titles, etc.)
    ui_tabs: list of UINavbarTabHandler subclasses that define tabs to show, or
        None to show the standard set of tabs.
    template_paths: a list of additional paths to search Jinja templates in.
  """
  global _ui_app_name
  global _ui_navbar_tabs
  global _ui_extra_template_paths
  _ui_app_name = app_name
  if ui_tabs is not None:
    assert all(issubclass(cls, UINavbarTabHandler) for cls in ui_tabs)
    _ui_navbar_tabs = tuple(ui_tabs)
  _ui_extra_template_paths = tuple(template_paths or [])
  # Reset cached Jinja2 env to pick up new template paths.
  utils.clear_cache(get_jinja_env)


def get_ui_routes():
  """Returns a list of routes with auth UI handlers."""
  # Routes for registered navbar tabs.
  routes = [webapp2.Route(cls.navbar_tab_url, cls) for cls in _ui_navbar_tabs]
  # Routes for everything else.
  routes.extend([
    webapp2.Route(r'/auth', MainHandler),
    webapp2.Route(r'/auth/bootstrap', BootstrapHandler, name='bootstrap'),
  ])
  return routes


@utils.cache
def get_jinja_env():
  """Returns jinja2.Environment object that knows how to render templates."""
  template_paths = [os.path.join(THIS_DIR, 'templates')]
  template_paths.extend(_ui_extra_template_paths)
  return jinja2.Environment(
      loader=jinja2.FileSystemLoader(template_paths),
      extensions=['jinja2.ext.autoescape'],
      undefined=jinja2.StrictUndefined,
      autoescape=True)


class UIHandler(handler.AuthenticatingHandler):
  """Renders Jinja templates extending base.html or base_minimal.html."""

  def reply(self, path, env=None, status=200):
    """Render template |path| to response using given environment.

    Optional keys from |env| that base.html uses:
      css_file: URL to a file with page specific styles, relative to site root.
      js_file: URL to a file with page specific Javascript code, relative to
          site root. File should define global object named same as a filename,
          i.e. '/auth/static/js/api.js' should define global object 'api' that
          incapsulates functionality implemented in the module.
      navbar_tab_id: id a navbar tab to highlight.
      page_title: title of an HTML page.

    Args:
      path: path to a template, relative to templates/.
      env: additional environment dict to use when rendering the template.
      status: HTTP status code to return.
    """
    env = (env or {}).copy()
    env.setdefault('css_file', None)
    env.setdefault('js_file', None)
    env.setdefault('navbar_tab_id', None)
    env.setdefault('page_title', 'Untitled')

    # This goes to both Jinja2 env and Javascript config object.
    common = {
      'login_url': users.create_login_url(self.request.path),
      'logout_url': users.create_logout_url('/'),
      'xsrf_token': self.generate_xsrf_token(),
    }

    # Name of Javascript module with page code.
    js_module_name = None
    if env['js_file']:
      assert env['js_file'].endswith('.js')
      js_module_name = os.path.basename(env['js_file'])[:-3]

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
      'js_module_name': js_module_name,
      'navbar': [
        (cls.navbar_tab_id, cls.navbar_tab_title, cls.navbar_tab_url)
        for cls in _ui_navbar_tabs
      ],
    }
    full_env.update(common)
    full_env.update(env)

    # Render it.
    self.response.set_status(status)
    self.response.headers['Content-Type'] = 'text/html; charset=UTF-8'
    self.response.write(get_jinja_env().get_template(path).render(full_env))

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
    assert _ui_navbar_tabs
    self.redirect(_ui_navbar_tabs[0].navbar_tab_url)


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


class UINavbarTabHandler(UIHandler):
  """Handler for a navbar tab page."""
  # URL to the tab (relative to site root).
  nvabar_tab_url = None
  # ID of the tab, will be used in DOM.
  navbar_tab_id = None
  # Title of the tab, will be used in tab title and page title.
  navbar_tab_title = None
  # Relative URL to javascript file with tab's logic.
  js_file_url = None
  # Path to a Jinja2 template with tab's markup.
  template_file = None

  @api.require(api.is_admin)
  def get(self):
    """Renders page HTML to HTTP response stream."""
    env = {
      'js_file': self.js_file_url,
      'navbar_tab_id': self.navbar_tab_id,
      'page_title': self.navbar_tab_title,
    }
    self.reply(self.template_file, env=env)


################################################################################
## Default tabs.


class GroupsHandler(UINavbarTabHandler):
  """Page with Groups management."""
  navbar_tab_url = '/auth/groups'
  navbar_tab_id = 'groups'
  navbar_tab_title = 'Groups'
  js_file_url = '/auth/static/js/groups.js'
  template_file = 'groups.html'


class OAuthConfigHandler(UINavbarTabHandler):
  """Page with OAuth configuration."""
  navbar_tab_url = '/auth/oauth_config'
  navbar_tab_id = 'oauth_config'
  navbar_tab_title = 'OAuth'
  js_file_url = '/auth/static/js/oauth_config.js'
  template_file = 'oauth_config.html'


# Register them as default tabs.
_ui_navbar_tabs = (GroupsHandler, OAuthConfigHandler)
