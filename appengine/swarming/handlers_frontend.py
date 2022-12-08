# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Main entry point for Swarming service.

This file contains the URL handlers for all the Swarming service URLs,
implemented using the webapp2 framework.
"""

import collections
import os

import webapp2

import handlers_bot
import handlers_endpoints
import template

from components import auth
from components import utils
from server import acl
from server import bot_code
from server import bot_groups_config
from server import config
from server import rbe


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


# Helper class for displaying the sort options in html templates.
SortOptions = collections.namedtuple('SortOptions', ['key', 'name'])


### is_admin pages.


class RestrictedConfigHandler(auth.AuthenticatingHandler):
  @auth.autologin
  @auth.require(acl.can_view_config)
  def get(self):
    # Template parameters schema matches settings_info() return value.
    self.response.write(template.render(
        'swarming/restricted_config.html', config.settings_info()))


### Redirectors.


class BotsListHandler(auth.AuthenticatingHandler):
  """Redirects to a list of known bots."""

  @auth.public
  def get(self):
    limit = int(self.request.get('limit', 100))

    dimensions = (
      l.strip() for l in self.request.get('dimensions', '').splitlines()
    )
    dimensions = [i for i in dimensions if i]

    new_ui_link = '/botlist?l=%d' % limit
    if dimensions:
      new_ui_link += '&f=' + '&f='.join(dimensions)

    self.redirect(new_ui_link)


class BotHandler(auth.AuthenticatingHandler):
  """Redirects to a page about the bot, including last tasks and events."""

  @auth.public
  def get(self, bot_id):
    self.redirect('/bot?id=%s' % bot_id)


class TasksHandler(auth.AuthenticatingHandler):
  """Redirects to a list of all task requests."""

  @auth.public
  def get(self):
    limit = int(self.request.get('limit', 100))
    task_tags = [
      line for line in self.request.get('task_tag', '').splitlines() if line
    ]

    new_ui_link = '/tasklist?l=%d' % limit
    if task_tags:
      new_ui_link += '&f=' + '&f='.join(task_tags)

    self.redirect(new_ui_link)


class TaskHandler(auth.AuthenticatingHandler):
  """Redirects to a page containing task request and result."""

  @auth.public
  def get(self, task_id):
    self.redirect('/task?id=%s' % task_id)


### Public pages.


class UIHandler(auth.AuthenticatingHandler):
  """Serves the landing page for the new UI of the requested page.

  This landing page is stamped with the OAuth 2.0 client id from the
  configuration.
  """
  @auth.public
  def get(self, page):
    page = page or 'swarming'

    params = {
      'client_id': config.settings().ui_client_id,
    }
    # Can cache for 1 week, because the only thing that would change in this
    # template is the oauth client id, which changes very infrequently.
    self.response.cache_control.no_cache = None
    self.response.cache_control.public = True
    self.response.cache_control.max_age = 604800
    try:
      self.response.write(template.render(
        'wcui/public_%s_index.html' % page, params))
    except template.TemplateNotFound:
      self.abort(404, 'Page %s not found.', page)

  def get_content_security_policy(self):
    # We use iframes to display pages at display_server_url_template. Need to
    # allow it in CSP.
    csp = super(UIHandler, self).get_content_security_policy()
    csp['frame-src'].append("'self'")
    tmpl = config.settings().display_server_url_template
    if tmpl:
      if not tmpl.startswith('/'):
        # We assume the template specifies '%s' in its last path component.
        # We strip it to get a "parent" path that we can put into CSP. Note that
        # allowing an entire display server domain is unnecessary wide.
        csp['frame-src'].append(tmpl[:tmpl.rfind('/')+1])
    extra = config.settings().extra_child_src_csp_url
    # Note that frame-src was once child-src, which was deprecated and support
    # was dropped by some browsers. See
    # https://bugs.chromium.org/p/chromium/issues/detail?id=839909
    csp['frame-src'].extend(extra)
    return csp


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    auth.warmup()
    bot_code.get_swarming_bot_zip(self.request.host_url)
    bot_groups_config.warmup()
    rbe.warmup()
    utils.get_module_version_list(None, None)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


class EmailHandler(webapp2.RequestHandler):
  """Blackhole any email sent."""
  def post(self, to):
    pass


def get_routes():
  routes = [
      ('/_ah/mail/<to:.+>', EmailHandler),
      ('/_ah/warmup', WarmupHandler),
  ]

  if not utils.should_disable_ui_routes():
    routes.extend([
        # Frontend pages. They return HTML.
        # Public pages.
        ('/<page:(botlist|tasklist|task|bot|)>', UIHandler),

        # This is for https://aip.dev/122#resource-uris
        ('/tasks/<task_id:[0-9a-fA-F]+>', TaskHandler),

        # These were the very old (pre-2016) links, so this redirects
        # them to the modern url style.
        ('/user/tasks', TasksHandler),
        ('/user/task/<task_id:[0-9a-fA-F]+>', TaskHandler),
        ('/restricted/bots', BotsListHandler),
        ('/restricted/bot/<bot_id:[^/]+>', BotHandler),

        # Admin pages.
        # TODO(maruel): Get rid of them.
        ('/restricted/config', RestrictedConfigHandler),
    ])

  return [webapp2.Route(*i) for i in routes]


def create_application(debug):
  routes = []
  routes.extend(get_routes())
  routes.extend(handlers_bot.get_routes())
  routes.extend(handlers_endpoints.get_routes())
  return webapp2.WSGIApplication(routes, debug=debug)
