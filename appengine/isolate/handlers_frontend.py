# Copyright 2012 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Isolate Server frontend url handlers."""

import datetime
import json

import webapp2

from google.appengine.api import memcache
from google.appengine.api import users

import acl
import config
import gcs
import handlers_api
import mapreduce_jobs
import model
import stats
import template
from components import auth
from components import stats_framework
from components import stats_framework_gviz
from components import utils
from gviz import gviz_api


# GViz data description.
_GVIZ_DESCRIPTION = {
  'failures': ('number', 'Failures'),
  'requests': ('number', 'Total'),
  'other_requests': ('number', 'Other'),
  'uploads': ('number', 'Uploads'),
  'uploads_bytes': ('number', 'Uploaded'),
  'downloads': ('number', 'Downloads'),
  'downloads_bytes': ('number', 'Downloaded'),
  'contains_requests': ('number', 'Lookups'),
  'contains_lookups': ('number', 'Items looked up'),
}

# Warning: modifying the order here requires updating templates/stats.html.
_GVIZ_COLUMNS_ORDER = (
  'key',
  'requests',
  'other_requests',
  'failures',
  'uploads',
  'downloads',
  'contains_requests',
  'uploads_bytes',
  'downloads_bytes',
  'contains_lookups',
)


### Restricted handlers


class RestrictedConfigHandler(auth.AuthenticatingHandler):
  @auth.autologin
  @auth.require(auth.is_admin)
  def get(self):
    self.common(None)

  @auth.require(auth.is_admin)
  def post(self):
    # Convert MultiDict into a dict.
    params = {
      k: self.request.params.getone(k) for k in self.request.params
      if k not in ('keyid', 'xsrf_token')
    }
    cfg = config.settings(fresh=True)
    keyid = int(self.request.get('keyid', '0'))
    if cfg.key.integer_id() != keyid:
      self.common('Update conflict %s != %s' % (cfg.key.integer_id(), keyid))
      return
    params['default_expiration'] = int(params['default_expiration'])
    cfg.populate(**params)
    try:
      # Ensure key is correct, it's easy to make a mistake when creating it.
      gcs.URLSigner.load_private_key(cfg.gs_private_key)
    except Exception as exc:
      # TODO(maruel): Handling Exception is too generic. And add self.abort(400)
      self.response.write('Bad private key: %s' % exc)
      return
    cfg.store()
    self.common('Settings updated')

  def common(self, note):
    params = {
      'cfg': config.settings(fresh=True),
      'note': note,
      'path': self.request.path,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.write(
        template.render('isolate/restricted_config.html', params))


### Mapreduce related handlers


class RestrictedLaunchMapReduceJob(auth.AuthenticatingHandler):
  """Enqueues a task to start a map reduce job on the backend module.

  A tree of map reduce jobs inherits module and version of a handler that
  launched it. All UI handlers are executes by 'default' module. So to run a
  map reduce on a backend module one needs to pass a request to a task running
  on backend module.
  """

  @auth.require(auth.is_admin)
  def post(self):
    job_id = self.request.get('job_id')
    assert job_id in mapreduce_jobs.MAPREDUCE_JOBS
    # Do not use 'backend' module when running from dev appserver. Mapreduce
    # generates URLs that are incompatible with dev appserver URL routing when
    # using custom modules.
    success = utils.enqueue_task(
        url='/internal/taskqueue/mapreduce/launch/%s' % job_id,
        queue_name=mapreduce_jobs.MAPREDUCE_TASK_QUEUE,
        use_dedicated_module=not utils.is_local_dev_server())
    # New tasks should show up on the status page.
    if success:
      self.redirect('/restricted/mapreduce/status')
    else:
      self.abort(500, 'Failed to launch the job')


### Non-restricted handlers


class BrowseHandler(auth.AuthenticatingHandler):
  @auth.autologin
  @auth.require(acl.isolate_readable)
  def get(self):
    namespace = self.request.get('namespace', 'default-gzip')
    # Support 'hash' for compatibility with old links. To remove eventually.
    digest = self.request.get('digest', '') or self.request.get('hash', '')
    content = None
    if digest and namespace:
      # TODO(maruel): Refactor into a function.
      memcache_entry = memcache.get(digest, namespace='table_%s' % namespace)
      if memcache_entry is not None:
        raw_data = memcache_entry
      else:
        try:
          key = model.get_entry_key(namespace, digest)
        except ValueError:
          self.abort(400, 'Invalid key')
        entity = key.get()
        if entity is None:
          self.abort(404, 'Unable to retrieve the entry')
        raw_data = entity.content
      if not raw_data:
        stream = gcs.read_file(config.settings().gs_bucket, key.id())
      else:
        stream = [raw_data]
      content = ''.join(model.expand_content(namespace, stream))
      if content.startswith('{'):
        # Try to format as JSON.
        try:
          content = json.dumps(
              json.loads(content), sort_keys=True, indent=2,
              separators=(',', ': '))
        except ValueError:
          pass
      content = content.decode('utf8', 'replace')
    params = {
      u'content': content,
      u'digest': unicode(digest),
      u'namespace': unicode(namespace),
      # TODO(maruel): Add back once Web UI authentication is switched to OAuth2.
      #'onload': 'update()' if digest else '',
      u'onload': '',
    }
    self.response.write(template.render('isolate/browse.html', params))


class StatsHandler(webapp2.RequestHandler):
  """Returns the statistics web page."""
  def get(self):
    """Presents nice recent statistics.

    It fetches data from the 'JSON' API.
    """
    # Preloads the data to save a complete request.
    resolution = self.request.params.get('resolution', 'hours')
    if resolution not in ('days', 'hours', 'minutes'):
      resolution = 'hours'
    duration = utils.get_request_as_int(self.request, 'duration', 120, 1, 1000)

    description = _GVIZ_DESCRIPTION.copy()
    description.update(stats_framework_gviz.get_description_key(resolution))
    table = stats_framework.get_stats(
        stats.STATS_HANDLER, resolution, None, duration, True)
    params = {
      'duration': duration,
      'initial_data': gviz_api.DataTable(description, table).ToJSon(
          columns_order=_GVIZ_COLUMNS_ORDER),
      'now': datetime.datetime.utcnow(),
      'resolution': resolution,
    }
    self.response.write(template.render('isolate/stats.html', params))


class StatsGvizHandlerBase(webapp2.RequestHandler):
  RESOLUTION = None

  def get(self):
    description = _GVIZ_DESCRIPTION.copy()
    description.update(
        stats_framework_gviz.get_description_key(self.RESOLUTION))
    try:
      stats_framework_gviz.get_json(
          self.request,
          self.response,
          stats.STATS_HANDLER,
          self.RESOLUTION,
          description,
          _GVIZ_COLUMNS_ORDER)
    except ValueError as e:
      self.abort(400, str(e))


class StatsGvizDaysHandler(StatsGvizHandlerBase):
  RESOLUTION = 'days'


class StatsGvizHoursHandler(StatsGvizHandlerBase):
  RESOLUTION = 'hours'


class StatsGvizMinutesHandler(StatsGvizHandlerBase):
  RESOLUTION = 'minutes'


###  Public pages.


class RootHandler(auth.AuthenticatingHandler):
  """Tells the user to RTM."""

  @auth.public
  def get(self):
    params = {
      'is_admin': auth.is_admin(),
      'is_user': acl.isolate_readable(),
      'mapreduce_jobs': [],
      'user_type': acl.get_user_type(),
    }
    if auth.is_admin():
      params['mapreduce_jobs'] = [
        {'id': job_id, 'name': job_def['job_name']}
        for job_id, job_def in mapreduce_jobs.MAPREDUCE_JOBS.iteritems()
      ]
      params['xsrf_token'] = self.generate_xsrf_token()
    self.response.write(template.render('isolate/root.html', params))


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    config.warmup()
    auth.warmup()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


class EmailHandler(webapp2.RequestHandler):
  """Blackhole any email sent."""
  def post(self, to):
    pass


def get_routes():
  return [
      # Administrative urls.
      webapp2.Route(r'/restricted/config', RestrictedConfigHandler),

      # Mapreduce related urls.
      webapp2.Route(
          r'/restricted/launch_mapreduce',
          RestrictedLaunchMapReduceJob),

      # User web pages.
      webapp2.Route(r'/browse', BrowseHandler),
      webapp2.Route(r'/stats', StatsHandler),
      webapp2.Route(r'/isolate/api/v1/stats/days', StatsGvizDaysHandler),
      webapp2.Route(r'/isolate/api/v1/stats/hours', StatsGvizHoursHandler),
      webapp2.Route(r'/isolate/api/v1/stats/minutes', StatsGvizMinutesHandler),
      webapp2.Route(r'/', RootHandler),

      # AppEngine-specific urls:
      webapp2.Route(r'/_ah/mail/<to:.+>', EmailHandler),
      webapp2.Route(r'/_ah/warmup', WarmupHandler),
  ]


def create_application(debug):
  """Creates the url router.

  The basic layouts is as follow:
  - /restricted/.* requires being an instance administrator.
  - /content/.* has the public HTTP API.
  - /stats/.* has statistics.
  """
  acl.bootstrap()
  template.bootstrap()

  routes = get_routes()
  routes.extend(handlers_api.get_routes())

  return webapp2.WSGIApplication(routes, debug=debug)
