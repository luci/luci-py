# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Auth management UI handlers."""

import functools
import json
import os
import re
import webapp2

from components import template
from components import utils

from . import acl
from . import rest_api

from .. import api
from .. import change_log
from .. import config
from .. import handler
from .. import model
from .. import openid
from .. import replication


# templates/.
TEMPLATES_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'templates')


# Global static configuration set in 'configure_ui'.
_ui_app_name = 'Unknown'
_ui_env_callback = None
_ui_navbar_tabs = ()


def configure_ui(app_name, ui_tabs=None, env_callback=None):
  """Modifies global configuration of Auth UI.

  Args:
    app_name: name of the service (visible in page headers, titles, etc.)
    ui_tabs: list of UINavbarTabHandler subclasses that define tabs to show, or
        None to show the standard set of tabs.
    env_callback: callable that returns a dict with additional environment for
        templates. Will be called each time a template is rendered with single
        argument: UIHandler instance. Used by auth_service.
  """
  global _ui_app_name
  global _ui_env_callback
  global _ui_navbar_tabs
  _ui_app_name = app_name
  _ui_env_callback = env_callback
  if ui_tabs is not None:
    assert all(issubclass(cls, UINavbarTabHandler) for cls in ui_tabs)
    _ui_navbar_tabs = tuple(ui_tabs)
  template.bootstrap({'auth': TEMPLATES_DIR})


def get_ui_routes():
  """Returns a list of routes with auth UI handlers."""
  # Routes for registered navbar tabs.
  routes = [webapp2.Route(cls.navbar_tab_url, cls) for cls in _ui_navbar_tabs]
  # Routes to OpenID login flow.
  if config.ensure_configured().USE_OPENID:
    routes.extend(openid.get_ui_routes())
  # Routes for everything else.
  routes.extend([
    webapp2.Route(r'/auth', MainHandler),
    webapp2.Route(r'/auth/bootstrap', BootstrapHandler, name='bootstrap'),
    webapp2.Route(r'/auth/link', LinkToPrimaryHandler),
  ])
  return routes


def forbid_ui_on_replica(method):
  """Decorator for methods that are not allowed to be called on Replica.

  If such method is called on a service in Replica mode, it would return
  HTTP 405 "Method Not Allowed".
  """
  @functools.wraps(method)
  def wrapper(self, *args, **kwargs):
    assert isinstance(self, webapp2.RequestHandler)
    if model.is_replica():
      primary_url = model.get_replication_state().primary_url
      self.abort(
          405,
          detail='Not allowed on a replica, see primary at %s' % primary_url)
    return method(self, *args, **kwargs)
  return wrapper


def redirect_ui_on_replica(method):
  """Decorator for methods that redirect to Primary when called on replica.

  If such method is called on a service in Replica mode, it would return
  HTTP 302 redirect to corresponding method on Primary.
  """
  @functools.wraps(method)
  def wrapper(self, *args, **kwargs):
    assert isinstance(self, webapp2.RequestHandler)
    assert self.request.method == 'GET'
    if model.is_replica():
      primary_url = model.get_replication_state().primary_url
      protocol = 'http://' if utils.is_local_dev_server() else 'https://'
      assert primary_url and primary_url.startswith(protocol), primary_url
      assert self.request.path_qs.startswith('/'), self.request.path_qs
      self.redirect(primary_url.rstrip('/') + self.request.path_qs, abort=True)
    return method(self, *args, **kwargs)
  return wrapper


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
    user = self.get_current_user()
    common = {
      'account_picture': user.picture() if user else None,
      'auth_service_config_locked': False, # overridden in auth_service
      'is_admin': api.is_admin(),
      'login_url': self.create_login_url(self.request.url),
      'logout_url': self.create_logout_url('/'),
      'using_gae_auth': self.auth_method == handler.gae_cookie_authentication,
      'xsrf_token': self.generate_xsrf_token(),
    }
    if _ui_env_callback:
      common.update(_ui_env_callback(self))

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
        if cls.is_visible()
      ],
    }
    full_env.update(common)
    full_env.update(env)

    # Render it.
    self.response.set_status(status)
    self.response.headers['Content-Type'] = 'text/html; charset=utf-8'
    self.response.write(template.render(path, full_env))

  def authentication_error(self, error):
    """Shows 'Access denied' page."""
    env = {
      'page_title': 'Access Denied',
      'error': error,
    }
    self.reply('auth/access_denied.html', env=env, status=401)

  def authorization_error(self, error):
    """Redirects to login or shows 'Access Denied' page."""
    # Not authenticated or used IP whitelist for auth -> redirect to login.
    # Bots doesn't use UI, and users should always use real accounts.
    ident = api.get_current_identity()
    if ident.is_anonymous or ident.is_bot:
      self.redirect(self.create_login_url(self.request.url))
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
    self.reply('auth/access_denied.html', env=env, status=403)


class MainHandler(UIHandler):
  """Redirects to first navbar tab."""
  @redirect_ui_on_replica
  @api.require(acl.has_access)
  def get(self):
    assert _ui_navbar_tabs
    self.redirect(_ui_navbar_tabs[0].navbar_tab_url)


class BootstrapHandler(UIHandler):
  """Creates Administrators group (if necessary) and adds current caller to it.

  Requires Appengine level Admin access for its handlers, since Administrators
  group may not exist yet. Used to bootstrap a new service instance.
  """

  @classmethod
  def get_auth_methods(cls, conf):
    # This method sets 'is_superuser' bit for GAE-level admins.
    return [handler.gae_cookie_authentication]

  @forbid_ui_on_replica
  @api.require(api.is_superuser)
  def get(self):
    env = {
      'page_title': 'Bootstrap',
      'admin_group': model.ADMIN_GROUP,
      'return_url': self.request.get('r') or '',
    }
    self.reply('auth/bootstrap.html', env)

  @forbid_ui_on_replica
  @api.require(api.is_superuser)
  def post(self):
    added = model.bootstrap_group(
        model.ADMIN_GROUP, [api.get_current_identity()],
        'Users that can manage groups')
    env = {
      'page_title': 'Bootstrap',
      'admin_group': model.ADMIN_GROUP,
      'added': added,
      'return_url': self.request.get('return_url') or '',
    }
    self.reply('auth/bootstrap_done.html', env)


class LinkToPrimaryHandler(UIHandler):
  """A page with confirmation of Primary <-> Replica linking request.

  URL to that page is generated by a Primary service.
  """

  def decode_link_ticket(self):
    """Extracts ServiceLinkTicket from 't' GET parameter."""
    try:
      return replication.decode_link_ticket(
          self.request.get('t').encode('ascii'))
    except (KeyError, ValueError):
      self.abort(400)
      return

  @classmethod
  def get_auth_methods(cls, conf):
    # This method sets 'is_superuser' bit for GAE-level admins.
    return [handler.gae_cookie_authentication]

  @forbid_ui_on_replica
  @api.require(api.is_superuser)
  def get(self):
    ticket = self.decode_link_ticket()
    env = {
      'generated_by': ticket.generated_by,
      'page_title': 'Switch',
      'primary_id': ticket.primary_id,
      'primary_url': ticket.primary_url,
    }
    self.reply('auth/linking.html', env)

  @forbid_ui_on_replica
  @api.require(api.is_superuser)
  def post(self):
    ticket = self.decode_link_ticket()
    success = True
    error_msg = None
    try:
      replication.become_replica(ticket, api.get_current_identity())
    except replication.ProtocolError as exc:
      success = False
      error_msg = exc.message
    env = {
      'error_msg': error_msg,
      'page_title': 'Switch',
      'primary_id': ticket.primary_id,
      'primary_url': ticket.primary_url,
      'success': success,
    }
    self.reply('auth/linking_done.html', env)


class UINavbarTabHandler(UIHandler):
  """Handler for a navbar tab page."""
  # URL to the tab (relative to site root).
  nvabar_tab_url = None
  # ID of the tab, will be used in DOM.
  navbar_tab_id = None
  # Title of the tab, will be used in tab title and page title.
  navbar_tab_title = None
  # Relative URL to CSS file with tab's styles.
  css_file = None
  # Relative URL to javascript file with tab's logic.
  js_file_url = None
  # Path to a Jinja2 template with tab's markup.
  template_file = None

  @redirect_ui_on_replica
  @api.require(acl.has_access)
  def get(self):
    """Renders page HTML to HTTP response stream."""
    env = {
      'css_file': self.css_file,
      'js_file': self.js_file_url,
      'navbar_tab_id': self.navbar_tab_id,
      'page_title': self.navbar_tab_title,
    }
    self.reply(self.template_file, env)

  @classmethod
  def is_visible(cls):
    """Subclasses may return False to hide the tab from tab bar."""
    return True


################################################################################
## Default tabs.


class GroupsHandler(UINavbarTabHandler):
  """Page with Groups management."""
  navbar_tab_url = '/auth/groups'
  navbar_tab_id = 'groups'
  navbar_tab_title = 'Groups'
  css_file = '/auth/static/css/groups.css'
  js_file_url = '/auth/static/js/groups.js'
  template_file = 'auth/groups.html'


class ChangeLogHandler(UINavbarTabHandler):
  """Page with a log of changes to some groups."""
  navbar_tab_url = '/auth/change_log'
  navbar_tab_id = 'change_log'
  navbar_tab_title = 'Change Log'
  js_file_url = '/auth/static/js/change_log.js'
  template_file = 'auth/change_log.html'

  @classmethod
  def is_visible(cls):
    # Hide 'Change Log' tab if there are no change log indexes in the datastore.
    # It happens on services that use components.auth, but do not modify
    # index.yaml. Don't try too hard to hide the log though. If user happes to
    # stumble on Change log page (e.g. by using direct URL), it handles
    # NeedIndexError gracefully (explaining how to configure indexes).
    return change_log.is_changle_log_indexed()


class OAuthConfigHandler(UINavbarTabHandler):
  """Page with OAuth configuration."""
  navbar_tab_url = '/auth/oauth_config'
  navbar_tab_id = 'oauth_config'
  navbar_tab_title = 'OAuth'
  js_file_url = '/auth/static/js/oauth_config.js'
  template_file = 'auth/oauth_config.html'


class IPWhitelistsHandler(UINavbarTabHandler):
  """Page with IP whitelists configuration."""
  navbar_tab_url = '/auth/ip_whitelists'
  navbar_tab_id = 'ip_whitelists'
  navbar_tab_title = 'IP Whitelists'
  js_file_url = '/auth/static/js/ip_whitelists.js'
  template_file = 'auth/ip_whitelists.html'


def pretty_json(d):
  return json.dumps(d, sort_keys=True, separators=(', ', ': '), indent=2)


class ApiDocHandler(UINavbarTabHandler):
  """Page with API documentation extracted from rest_api.py."""
  navbar_tab_url = '/auth/api'
  navbar_tab_id = 'api'
  navbar_tab_title = 'API'

  # These can be used as 'request_type' and 'response_type' in api_doc.
  doc_types = [
    {
      'name': 'Status',
      'doc': 'Outcome of some operation.',
      'example': pretty_json({'ok': True}),
    },
    {
      'name': 'Self info',
      'doc': 'Information about the requester.',
      'example': pretty_json({
        'identity': 'user:someone@example.com',
        'ip': '192.168.0.1',
      }),
    },
    {
      'name': 'Group',
      'doc': 'Represents a group, as stored in the database.',
      'example': pretty_json({
        'group': {
          'caller_can_modify': True,
          'created_by': 'user:someone@example.com',
          'created_ts': 1409250754978540,
          'description': 'Some free form description',
          'globs': ['user:*@example.com'],
          'members': ['user:a@example.com', 'anonymous:anonymous'],
          'modified_by': 'user:someone@example.com',
          'modified_ts': 1470871200558130,
          'name': 'Some group',
          'nested': ['Some nested group', 'Another nested group'],
          'owners': 'Owning group',
        },
      }),
    },
    {
      'name': 'Group listing',
      'doc':
        'All groups, along with their metadata. Does not include members '
        'listings.',
      'example': pretty_json({
        'groups': [
          {
            'caller_can_modify': True,
            'created_by': 'user:someone@example.com',
            'created_ts': 1409250754978540,
            'description': 'Some free form description',
            'modified_by': 'user:someone@example.com',
            'modified_ts': 1470871200558130,
            'name': 'Some group',
            'owners': 'Owning group',
          },
          {
            'caller_can_modify': True,
            'created_by': 'user:someone@example.com',
            'created_ts': 1409250754978540,
            'description': 'Another description',
            'modified_by': 'user:someone@example.com',
            'modified_ts': 1470871200558130,
            'name': 'Another group',
            'owners': 'Owning group',
          },
        ],
      }),
    },
  ]

  @redirect_ui_on_replica
  @api.require(acl.has_access)
  def get(self):
    """Extracts API doc for registered webapp2 API routes."""
    api_methods = []
    for route in rest_api.get_rest_api_routes():
      # Remove API parameter regexps from route template, they are mostly noise.
      simplified = re.sub(r'\:.*\>', '>', route.template)
      for doc in getattr(route.handler, 'api_doc', []):
        api_methods.append({
          'verb': doc['verb'],
          'path': simplified,
          'doc': doc['doc'],
          'request_type': doc.get('request_type'),
          'response_type': doc.get('response_type'),
        })
    env = {
      'navbar_tab_id': self.navbar_tab_id,
      'page_title': self.navbar_tab_title,
      'api_methods': api_methods,
      'doc_types': self.doc_types,
    }
    self.reply('auth/api.html', env)


# Register them as default tabs. Order is important.
_ui_navbar_tabs = (
  GroupsHandler,
  ChangeLogHandler,
  OAuthConfigHandler,
  IPWhitelistsHandler,
  ApiDocHandler,
)
